// Travel-time map page logic. Standalone from index.html: reuses only the
// shared `/maas.js` helpers (`gql`) via globals, plus Leaflet from the same CDN
// the main app uses. Renders a continuous green(0)->red(maxSeconds) heatmap of
// the `travelTimeMap` GraphQL query onto a Leaflet canvas overlay.

// Travel-mode grid (mirrors index.html): rows = vehicle, columns = how the
// vehicle relates to transit. Each populated cell is one Mode enum value; null
// cells are impossible combos and render disabled. Headers toggle a whole
// row/column at once.
const MODE_COLS = [
  { col: 'SOLO', label: 'Solo' },
  { col: 'ON',   label: 'On transit' },
  { col: 'PARK', label: 'Park & ride' },
  { col: 'BOTH', label: 'Pickup' },
];
const MODE_ROWS = [
  { row: 'WALK', label: '🚶 Walk' },
  { row: 'BIKE', label: '🚲 Bike' },
  { row: 'CAR',  label: '🚗 Car' },
];
const MODE_GRID = {
  WALK: { SOLO: 'WALK', ON: 'WALK_TRANSIT',    PARK: null,              BOTH: null },
  BIKE: { SOLO: 'BIKE', ON: 'BIKE_ON_TRANSIT', PARK: 'BIKE_TO_TRANSIT', BOTH: 'BIKE_PICKUP' },
  CAR:  { SOLO: 'CAR',  ON: null,              PARK: 'CAR_DROP_OFF',    BOTH: 'CAR_PICKUP' },
};
const MODE_TITLES = {
  WALK: 'Walk — On foot the whole way',
  WALK_TRANSIT: 'Walk + transit — Walk to stops, take public transport',
  BIKE: 'Bike — Cycle door to door',
  BIKE_ON_TRANSIT: 'Bike on board — Bring your bike on the train',
  BIKE_TO_TRANSIT: 'Bike & Ride — Park your bike at the station',
  BIKE_PICKUP: 'Pickup — Walk to transit, your bike waiting at the destination station for the final leg',
  CAR: 'Car — Drive the whole way',
  CAR_DROP_OFF: 'Park & Ride — Drive to a station, park, take transit',
  CAR_PICKUP: 'Pickup — Walk to transit, a car waiting at the destination station for the final leg',
};
const DEFAULT_MODES = new Set(['WALK', 'WALK_TRANSIT']);

const TRAVEL_QUERY = `query T(
  $centerLat: Float!, $centerLng: Float!, $date: String, $time: String,
  $maxSeconds: Int!, $modes: [Mode], $aggregation: TravelAggregation,
  $windowEndTime: String, $gridStepM: Float
) {
  travelTimeMap(
    centerLat: $centerLat, centerLng: $centerLng, date: $date, time: $time,
    maxSeconds: $maxSeconds, modes: $modes, aggregation: $aggregation,
    windowEndTime: $windowEndTime, gridStepM: $gridStepM
  ) {
    maxSeconds
    centerLat
    centerLng
    cells { lat lng seconds }
  }
}`;

// Canvas opacity so the OSM basemap (streets, labels, landmarks) reads through
// the colour field. Applied as a CSS opacity on the whole overlay canvas, so it
// survives the zoom-animation transform (which only writes `transform`) and the
// post-zoom setPosition. This is the single translucency knob for the filled-
// square heatmap — tuned so the fill is clearly coloured yet the basemap shows.
const HEAT_OPACITY = 0.55;

// Per-cell square fill alpha. Kept fully opaque here so touching squares don't
// darken at their overlap; the whole-canvas HEAT_OPACITY provides translucency.
const CELL_ALPHA = 1;

// Collapse a plan's legs into a compact step list for the click-to-route popup.
// Consecutive walk legs merge; each transit leg becomes one "Line -> Headsign"
// step. Returns [{ kind:'walk', mins } | { kind:'transit', line, mode, to, color, textColor }].
// Pure (no DOM) so it's unit-testable.
export function summarizeLegs(legs) {
  const out = [];
  for (const leg of legs || []) {
    if (leg.__typename === 'PlanWalkLeg') {
      const mins = Math.round((leg.duration ?? Math.max(0, leg.end - leg.start)) / 60);
      const last = out[out.length - 1];
      if (last && last.kind === 'walk') last.mins += mins;      // merge adjacent walks
      else out.push({ kind: 'walk', mins });
    } else if (leg.__typename === 'PlanTransitLeg') {
      const route = leg.trip && leg.trip.route;
      out.push({
        kind: 'transit',
        line: (route && route.shortName) || (route && route.mode) || '?',
        mode: route && route.mode,
        to: (leg.to && leg.to.node && leg.to.node.name) || (leg.trip && leg.trip.headsign) || null,
        color: route && route.color,
        textColor: route && route.textColor,
      });
    }
  }
  return out;
}

// ── green -> yellow -> red ramp on a normalized t in [0,1] ──────────────────
// t=0 (near) green, t=0.5 yellow, t=1 (max) red. Returns [r,g,b].
export function ramp(t) {
  t = Math.max(0, Math.min(1, t));
  // Two-segment linear interpolation through green -> yellow -> red.
  const green  = [ 22, 163,  74]; // #16a34a
  const yellow = [234, 179,   8]; // #eab308
  const red    = [220,  38,  38]; // #dc2626
  let a, b, u;
  if (t < 0.5) { a = green;  b = yellow; u = t / 0.5; }
  else         { a = yellow; b = red;    u = (t - 0.5) / 0.5; }
  return [
    Math.round(a[0] + (b[0] - a[0]) * u),
    Math.round(a[1] + (b[1] - a[1]) * u),
    Math.round(a[2] + (b[2] - a[2]) * u),
  ];
}

// ── Grid reconstruction from the flat cell list ─────────────────────────────
// The backend samples cells on a REGULAR lat/lng lattice anchored at the centre.
// Reconstruct that 2-D grid so marching squares can run on it: collect the
// sorted unique lats and lngs (evenly spaced), map each cell to integer (i,j),
// and fill a `seconds` matrix with a +Infinity sentinel for unreachable/missing
// cells. Returns { lats, lngs, sec, rows, cols } (sec[i][j], i over lats asc).
export function buildGrid(cells) {
  const EPS = 1e-6; // coordinate-equality tolerance (lattice is far coarser)
  const uniq = (vals) => {
    const out = [];
    for (const v of [...vals].sort((a, b) => a - b)) {
      if (!out.length || Math.abs(v - out[out.length - 1]) > EPS) out.push(v);
    }
    return out;
  };
  const lats = uniq(cells.map((c) => c.lat));
  const lngs = uniq(cells.map((c) => c.lng));
  const rows = lats.length, cols = lngs.length;
  // Binary-search an evenly spaced axis for the index of `v`.
  const idxOf = (arr, v) => {
    let lo = 0, hi = arr.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (Math.abs(arr[mid] - v) <= EPS) return mid;
      if (arr[mid] < v) lo = mid + 1; else hi = mid - 1;
    }
    return -1;
  };
  const sec = Array.from({ length: rows }, () => new Array(cols).fill(Infinity));
  for (const c of cells) {
    const i = idxOf(lats, c.lat), j = idxOf(lngs, c.lng);
    if (i >= 0 && j >= 0) sec[i][j] = Math.min(sec[i][j], c.seconds);
  }
  return { lats, lngs, sec, rows, cols };
}

// Marching squares on the reconstructed grid at one iso-time `threshold`.
// Produces a list of line segments [{ a:{i,j fractional}, b:{…} }] in grid
// coordinates, where a fractional (i,j) is interpolated linearly by seconds
// along a cell edge. Cells with Infinity seconds are treated as strictly above
// the threshold (outside the reachable band), so the outer isochrone boundary
// is drawn where reachable meets unreachable. Returns segments; the caller maps
// grid coords -> pixels. Cheap: O(rows*cols) per threshold, few hundred cells.
export function marchingSquares(grid, threshold) {
  const { sec, rows, cols } = grid;
  const segs = [];
  if (rows < 2 || cols < 2) return segs;
  // Interpolate the fractional position where the iso-value crosses the edge
  // between two corners with seconds va (at t=0) and vb (at t=1).
  const cross = (va, vb) => {
    if (!isFinite(va) && !isFinite(vb)) return 0.5;
    if (!isFinite(va)) return 1; // crossing sits at the finite (vb) corner side
    if (!isFinite(vb)) return 0;
    if (va === vb) return 0.5;
    return (threshold - va) / (vb - va);
  };
  for (let i = 0; i < rows - 1; i++) {
    for (let j = 0; j < cols - 1; j++) {
      // Corner seconds: tl, tr, br, bl (i over lats asc, j over lngs asc).
      const tl = sec[i][j],     tr = sec[i][j + 1];
      const bl = sec[i + 1][j], br = sec[i + 1][j + 1];
      // Skip a fully-empty cell (no finite corner => nothing to contour).
      if (!isFinite(tl) && !isFinite(tr) && !isFinite(bl) && !isFinite(br)) continue;
      // Below/equal threshold = inside (bit set). Infinity is always outside.
      let code = 0;
      if (tl <= threshold) code |= 8;
      if (tr <= threshold) code |= 4;
      if (br <= threshold) code |= 2;
      if (bl <= threshold) code |= 1;
      if (code === 0 || code === 15) continue;
      // Edge crossing points, in fractional grid coords:
      //   top    edge tl->tr : i,       j + cross(tl,tr)
      //   right  edge tr->br : i + cross(tr,br), j+1
      //   bottom edge bl->br : i+1,     j + cross(bl,br)
      //   left   edge tl->bl : i + cross(tl,bl), j
      const top    = () => ({ i,                     j: j + cross(tl, tr) });
      const right  = () => ({ i: i + cross(tr, br),  j: j + 1 });
      const bottom = () => ({ i: i + 1,              j: j + cross(bl, br) });
      const left   = () => ({ i: i + cross(tl, bl),  j });
      const push = (a, b) => segs.push({ a, b });
      switch (code) {
        case 1:  push(left(), bottom()); break;
        case 2:  push(bottom(), right()); break;
        case 3:  push(left(), right()); break;
        case 4:  push(top(), right()); break;
        case 5:  push(top(), left()); push(bottom(), right()); break; // saddle
        case 6:  push(top(), bottom()); break;
        case 7:  push(top(), left()); break;
        case 8:  push(top(), left()); break;
        case 9:  push(top(), bottom()); break;
        case 10: push(top(), right()); push(bottom(), left()); break; // saddle
        case 11: push(top(), right()); break;
        case 12: push(left(), right()); break;
        case 13: push(bottom(), right()); break;
        case 14: push(left(), bottom()); break;
      }
    }
  }
  return segs;
}

// Iso-time threshold (seconds): a SINGLE contour at the reachability boundary
// (maxSeconds) to keep the map uncluttered — no intermediate rings. Empty when
// maxSeconds is non-positive (nothing to draw).
export function contourThresholds(maxSeconds) {
  return maxSeconds > 0 ? [maxSeconds] : [];
}

// Exposed-cell-edge outline: the outer boundary of the reachable-cell UNION,
// following the cell SQUARE edges (so it lines up exactly with the hard-clip
// fill, which also draws cell squares). For each reachable cell (finite seconds)
// emit one of its 4 square edges only when the neighbour in that direction is
// missing/unreachable/off-grid — the union of those edges is precisely the outer
// border (and any interior holes). Coordinates are fractional grid units: a cell
// centre is (i, j); its square spans ±0.5 in each axis. Returns segments
// { a:{i,j}, b:{i,j} } for the caller to map to pixels. Pure — unit-testable.
export function exposedEdges(grid) {
  const { sec, rows, cols } = grid || {};
  const out = [];
  if (!rows || !cols) return out;
  const reachable = (i, j) =>
    i >= 0 && i < rows && j >= 0 && j < cols && isFinite(sec[i][j]);
  for (let i = 0; i < rows; i++) {
    for (let j = 0; j < cols; j++) {
      if (!reachable(i, j)) continue;
      const t = i - 0.5, b = i + 0.5, l = j - 0.5, r = j + 0.5;
      // Neighbour above (smaller i) missing -> top edge exposed.
      if (!reachable(i - 1, j)) out.push({ a: { i: t, j: l }, b: { i: t, j: r } });
      // Neighbour below (larger i) missing -> bottom edge exposed.
      if (!reachable(i + 1, j)) out.push({ a: { i: b, j: l }, b: { i: b, j: r } });
      // Neighbour left (smaller j) missing -> left edge exposed.
      if (!reachable(i, j - 1)) out.push({ a: { i: t, j: l }, b: { i: b, j: l } });
      // Neighbour right (larger j) missing -> right edge exposed.
      if (!reachable(i, j + 1)) out.push({ a: { i: t, j: r }, b: { i: b, j: r } });
    }
  }
  return out;
}

// Sample the isochrone travel time (seconds) at an arbitrary lat/lng from the
// reconstructed grid, so a click reports EXACTLY what the heatmap colour shows
// (no re-routing). Bilinear interpolation over the 4 surrounding lattice cells
// when all are finite; otherwise fall back to the nearest finite corner. Returns
// a finite seconds value, or Infinity when no usable nearby cell exists (outside
// the sampled area / all four corners unreachable). Pure (no DOM) — unit-tested.
export function sampleSeconds(grid, lat, lng) {
  if (!grid) return Infinity;
  const { lats, lngs, sec, rows, cols } = grid;
  if (!rows || !cols) return Infinity;
  // Locate the bracketing lattice indices (lats/lngs are sorted ascending).
  const bracket = (arr, v) => {
    if (v <= arr[0]) return [0, 0, 0];
    if (v >= arr[arr.length - 1]) return [arr.length - 1, arr.length - 1, 0];
    let lo = 0, hi = arr.length - 1;
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1;
      if (arr[mid] <= v) lo = mid; else hi = mid;
    }
    const span = arr[hi] - arr[lo];
    const f = span > 0 ? (v - arr[lo]) / span : 0;
    return [lo, hi, f];
  };
  const [i0, i1, fi] = bracket(lats, lat);
  const [j0, j1, fj] = bracket(lngs, lng);
  const tl = sec[i0][j0], tr = sec[i0][j1];
  const bl = sec[i1][j0], br = sec[i1][j1];
  // Bilinear when every surrounding corner is reachable.
  if (isFinite(tl) && isFinite(tr) && isFinite(bl) && isFinite(br)) {
    const top = tl + (tr - tl) * fj;
    const bot = bl + (br - bl) * fj;
    return top + (bot - top) * fi;
  }
  // Fallback: nearest finite corner (by lattice fraction). If none finite, unreachable.
  const corners = [
    { v: tl, d: fi * fi + fj * fj },
    { v: tr, d: fi * fi + (1 - fj) * (1 - fj) },
    { v: bl, d: (1 - fi) * (1 - fi) + fj * fj },
    { v: br, d: (1 - fi) * (1 - fi) + (1 - fj) * (1 - fj) },
  ].filter((c) => isFinite(c.v));
  if (!corners.length) return Infinity;
  return corners.reduce((a, b) => (b.d < a.d ? b : a)).v;
}

// A Leaflet canvas overlay that paints each cell as a soft radial blob so the
// grid reads as a continuous field rather than scattered dots. The blob radius
// scales with the on-screen cell pitch so neighbouring cells overlap and blend.
function makeHeatLayer(L) {
  return L.Layer.extend({
    initialize(data) {
      this._setData(data);
    },
    // Cache the reconstructed grid alongside the raw cells so marching squares
    // runs once per dataset (not once per redraw/zoom).
    _setData(data) {
      this._data = data; // { cells, maxSeconds }
      this._grid = (data && data.cells && data.cells.length) ? buildGrid(data.cells) : null;
    },
    setData(data) {
      this._setData(data);
      if (this._map) this._reset();
    },
    onAdd(map) {
      this._map = map;
      const canvas = L.DomUtil.create('canvas', 'tt-heat-canvas');
      canvas.style.position = 'absolute';
      canvas.style.pointerEvents = 'none';
      // Translucent so the basemap reads through the colour field. This is a CSS
      // opacity on the whole canvas element, independent of the zoom-animation
      // transform, so it is never reset by setTransform/setPosition.
      canvas.style.opacity = String(HEAT_OPACITY);
      this._canvas = canvas;
      map.getPanes().overlayPane.appendChild(canvas);
      // Mark the canvas as zoom-animatable so Leaflet keeps it in the animated
      // overlayPane during a zoom gesture (parity with L.Canvas / L.ImageOverlay).
      if (L.DomUtil.addClass) L.DomUtil.addClass(canvas, 'leaflet-zoom-animated');
      map.on('moveend zoomend resize', this._reset, this);
      // Track the basemap smoothly through a zoom animation instead of snapping
      // only at zoomend: apply the same CSS transform L.Canvas uses so the heat
      // overlay scales+translates with the tiles for the whole animation.
      if (map.options.zoomAnimation && L.Browser.any3d) {
        map.on('zoomanim', this._onAnimZoom, this);
      }
      this._reset();
    },
    onRemove(map) {
      map.off('moveend zoomend resize', this._reset, this);
      map.off('zoomanim', this._onAnimZoom, this);
      if (this._canvas && this._canvas.parentNode) this._canvas.parentNode.removeChild(this._canvas);
      this._canvas = null;
      this._map = null;
    },
    // During a zoom animation Leaflet fires 'zoomanim' with the target
    // {zoom, center}. Replicate L.Canvas._onAnimZoom/_updateTransform: scale the
    // canvas by the zoom ratio and translate it so the latlng currently at the
    // canvas's top-left stays anchored under the animating basemap.
    _onAnimZoom(e) {
      const map = this._map, canvas = this._canvas;
      if (!map || !canvas || this._topLeftLatLng == null) return;
      const scale = map.getZoomScale(e.zoom, map.getZoom());
      // Where the canvas's top-left latlng lands in the target projection,
      // relative to the target pixel origin — same math as L.ImageOverlay._animateZoom.
      const offset = map._latLngToNewLayerPoint(this._topLeftLatLng, e.zoom, e.center);
      L.DomUtil.setTransform(canvas, offset, scale);
    },
    _reset() {
      const map = this._map, canvas = this._canvas;
      if (!map || !canvas) return;
      const size = map.getSize();
      const topLeft = map.containerPointToLayerPoint([0, 0]);
      // Clear any leftover zoom-animation transform so the post-zoom canvas is
      // positioned crisply by layer-point (setTransform and setPosition both write
      // the CSS transform; keep only the latter after the animation settles).
      L.DomUtil.setPosition(canvas, topLeft);
      // Remember the latlng at the canvas's top-left for the next zoom animation.
      this._topLeftLatLng = map.layerPointToLatLng(topLeft);
      const dpr = window.devicePixelRatio || 1;
      canvas.width = size.x * dpr;
      canvas.height = size.y * dpr;
      canvas.style.width = size.x + 'px';
      canvas.style.height = size.y + 'px';
      this._draw();
    },
    _draw() {
      const map = this._map, canvas = this._canvas, data = this._data;
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      if (!data || !data.cells || !data.cells.length) return;

      // Per-cell FILLED SQUARE heatmap: each reachable cell paints its own lattice
      // square (side = one lattice step in px), coloured by the green->red ramp on
      // seconds/maxSeconds at a fixed translucency so the basemap reads through.
      // Adjacent squares touch, so contiguous regions read solid; far transit
      // pockets read as separate patches (that IS the true reachability). No radial
      // blobs and no clip — a square never bleeds past its own cell.
      const stepPx = latticeStepPx(map, this._grid);
      const side = (isFinite(stepPx) && stepPx > 0) ? stepPx : 8; // robust fallback
      const half = side / 2;
      // Overlap by ~1px so anti-aliased seams between touching squares don't leave
      // faint hairline gaps in a contiguous region.
      const draw = side + 1;
      const maxS = data.maxSeconds || 1;

      ctx.globalCompositeOperation = 'source-over';
      for (const c of data.cells) {
        const p = map.latLngToContainerPoint([c.lat, c.lng]);
        const [r, g, b] = ramp(c.seconds / maxS);
        ctx.fillStyle = `rgba(${r},${g},${b},${CELL_ALPHA})`;
        ctx.fillRect(p.x - half, p.y - half, draw, draw);
      }

      // Subtle outer boundary only (thin, low-opacity) to crisp the reachable
      // edge without dominating the pockety fill. Interior edges are not drawn.
      this._drawBorder(ctx, map);
    },
    // Isochrone border: the exposed OUTER EDGES of the reachable-cell union
    // (exposedEdges), following the cell SQUARE edges so the line lines up
    // exactly with the hard-clip fill (not the center-to-center marching-squares
    // contour, which cut inside the clip). Each fractional grid coord (cell
    // centres at integer i/j, square edges at ±0.5) maps to a lat/lng — with a
    // half-step extrapolation past the lattice edges — then to a screen point, so
    // the border scales/pans and re-strokes crisply on every draw.
    _drawBorder(ctx, map) {
      const grid = this._grid;
      if (!grid || !grid.rows || !grid.cols) return;
      const segs = exposedEdges(grid);
      if (!segs.length) return;

      const lats = grid.lats, lngs = grid.lngs;
      // Interpolate/extrapolate a fractional lattice index to its coordinate,
      // using the local step (or the nearest step at the ends) so ±0.5 offsets
      // land half a cell beyond the outermost lattice lines.
      const coordAt = (arr, g) => {
        const n = arr.length;
        if (n === 1) {
          // Single line: use a tiny nominal step so a lone cell still has a box.
          return arr[0] + g * 1e-4;
        }
        const i0 = Math.max(0, Math.min(n - 2, Math.floor(g)));
        const step = arr[i0 + 1] - arr[i0];
        return arr[i0] + (g - i0) * step;
      };
      const at = (gi, gj) =>
        map.latLngToContainerPoint([coordAt(lats, gi), coordAt(lngs, gj)]);

      // Theme-aware, semi-opaque stroke that reads over any ramp colour in both
      // light and dark schemes (dark line on light basemap, light on dark).
      // Thin, low-opacity line: just enough to crisp the reachable edge without
      // dominating the (often pockety) filled squares. Light on dark, dark on light.
      const dark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
      ctx.save();
      ctx.lineWidth = 1;
      ctx.strokeStyle = dark ? 'rgba(235,238,243,0.35)' : 'rgba(28,32,37,0.3)';
      ctx.lineJoin = 'round';
      ctx.lineCap = 'round';
      ctx.beginPath();
      for (const s of segs) {
        const a = at(s.a.i, s.a.j), b = at(s.b.i, s.b.j);
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
      }
      ctx.stroke();
      ctx.restore();
    },
  });
}

// On-screen size (px) of ONE fixed lattice step at the current zoom. The backend
// samples on a regular lat/lng lattice; `buildGrid` sorts the unique lats/lngs,
// so the smallest consecutive gap IS the true lattice step in degrees even when
// interior rows/cols are omitted (sparse isochrone). Project that step from a
// real cell centre to a point offset by one step, and average the vertical +
// horizontal pixel distances. Independent of how sparse the reachable cells are,
// so the blob radius reflects the fixed grid step, not the inter-cell spread.
function latticeStepPx(map, grid) {
  if (!grid || !grid.lats.length || !grid.lngs.length) return 20;
  const minStep = (arr) => {
    let m = Infinity;
    for (let i = 1; i < arr.length; i++) {
      const d = arr[i] - arr[i - 1];
      if (d > 1e-9 && d < m) m = d;
    }
    return isFinite(m) ? m : 0;
  };
  const dlat = minStep(grid.lats);
  const dlng = minStep(grid.lngs);
  // Anchor at the grid's mid latlng so the projection is representative.
  const lat0 = grid.lats[grid.lats.length >> 1];
  const lng0 = grid.lngs[grid.lngs.length >> 1];
  const o = map.latLngToContainerPoint([lat0, lng0]);
  const steps = [];
  if (dlat > 0) {
    const p = map.latLngToContainerPoint([lat0 + dlat, lng0]);
    steps.push(Math.hypot(p.x - o.x, p.y - o.y));
  }
  if (dlng > 0) {
    const p = map.latLngToContainerPoint([lat0, lng0 + dlng]);
    steps.push(Math.hypot(p.x - o.x, p.y - o.y));
  }
  if (!steps.length) return 20;
  return steps.reduce((a, b) => a + b, 0) / steps.length;
}

// ── Page bootstrap ──────────────────────────────────────────────────────────
function initTravelMap(L) {
  const map = L.map('map').setView([50.85, 4.35], 12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap contributors', maxZoom: 19,
  }).addTo(map);

  // Leaflet sizes its container lazily; the mobile @media stack changes #map's
  // box, so recompute after initial layout settles and on every (debounced)
  // window resize / orientation change so the map always fills its box.
  const invalidate = () => map.invalidateSize();
  setTimeout(invalidate, 0);
  let resizeTimer = null;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(invalidate, 150);
  });

  const HeatLayer = makeHeatLayer(L);
  let heat = null;
  let centerMarker = null;
  let center = null;       // { lat, lng }
  let centerLabel = null;  // last resolved address label for `center`, if any
  // Start in the "restoring" (suppressed-write) state: control-setters run during
  // init (e.g. setAgg('BEST')) call syncUrlState, which would otherwise clobber an
  // incoming shared query string with defaults BEFORE loadFromUrl() reads it.
  // loadFromUrl()'s finally clears this once the incoming params are consumed, so
  // normal URL syncing resumes right after.
  let restoring = true;    // suppress URL writes until loadFromUrl consumes the query string

  // ── DOM refs ──
  const $ = (id) => document.getElementById(id);
  const centerInput = $('center-input');
  const addrDrop    = $('addr-drop');
  const dateInput   = $('date');
  const timeInput   = $('time');
  const maxInput    = $('max-min');
  const winInput    = $('win-min');
  const cellInput   = $('cell-m');
  const cellVal     = $('cell-val');
  const aggBest     = $('agg-best');
  const aggAvg      = $('agg-avg');
  const computeBtn  = $('compute');
  const statusEl    = $('status');
  const legendMax   = $('legend-max');
  const modesGrid   = $('modes-grid');

  // Default date/time = now.
  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  dateInput.value = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
  timeInput.value = `${pad(now.getHours())}:${pad(now.getMinutes())}`;

  // ── Mode grid ──
  // Reuses the same 3×4 vehicle × transit-relation matrix as the main page.
  // Each populated cell is a toggle button carrying its Mode enum id in
  // data-mode; row/column headers flip a whole line at once.
  const mkEl = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  };
  function setModeCells(cells, target) {
    cells.forEach((c) => c.classList.toggle('active', target));
    syncUrlState();
  }
  function buildModeGrid() {
    modesGrid.replaceChildren();
    modesGrid.appendChild(mkEl('span', 'mg-corner'));
    MODE_COLS.forEach(({ col, label }) => {
      const h = mkEl('button', 'mg-col', label);
      h.type = 'button';
      h.title = 'Toggle every ' + label + ' option';
      h.addEventListener('click', () => {
        const cells = [...modesGrid.querySelectorAll('.mode-cell:not(.disabled)[data-col="' + col + '"]')];
        setModeCells(cells, !cells.every((c) => c.classList.contains('active')));
      });
      modesGrid.appendChild(h);
    });
    MODE_ROWS.forEach(({ row, label }) => {
      const h = mkEl('button', 'mg-row', label);
      h.type = 'button';
      h.title = 'Toggle every ' + label.replace(/^\S+\s/, '') + ' option';
      h.addEventListener('click', () => {
        const cells = [...modesGrid.querySelectorAll('.mode-cell:not(.disabled)[data-row="' + row + '"]')];
        setModeCells(cells, !cells.every((c) => c.classList.contains('active')));
      });
      modesGrid.appendChild(h);
      MODE_COLS.forEach(({ col }) => {
        const mode = MODE_GRID[row][col];
        if (!mode) {
          modesGrid.appendChild(mkEl('span', 'mode-cell disabled'));
          return;
        }
        const cell = mkEl('button', 'mode-cell' + (DEFAULT_MODES.has(mode) ? ' active' : ''));
        cell.type = 'button';
        cell.dataset.mode = mode;
        cell.dataset.row = row;
        cell.dataset.col = col;
        cell.title = MODE_TITLES[mode];
        cell.addEventListener('click', () => setModeCells([cell], !cell.classList.contains('active')));
        modesGrid.appendChild(cell);
      });
    });
  }
  buildModeGrid();
  $('modes-all').addEventListener('click', () =>
    setModeCells([...modesGrid.querySelectorAll('.mode-cell:not(.disabled)')], true));
  $('modes-none').addEventListener('click', () =>
    setModeCells([...modesGrid.querySelectorAll('.mode-cell:not(.disabled)')], false));
  const selectedModes = () =>
    Array.from(modesGrid.querySelectorAll('.mode-cell.active')).map((c) => c.dataset.mode);

  // ── Aggregation toggle ──
  let aggregation = 'BEST';
  function setAgg(v) {
    aggregation = v;
    aggBest.classList.toggle('active', v === 'BEST');
    aggAvg.classList.toggle('active', v === 'AVERAGE');
    syncUrlState();
  }
  aggBest.addEventListener('click', () => setAgg('BEST'));
  aggAvg.addEventListener('click', () => setAgg('AVERAGE'));
  setAgg('BEST');

  // ── Plain inputs mirror straight into the URL on change ──
  // Window is a plain minutes number (like the main planner): the map samples
  // departures over [time, time+window] and aggregates per cell (BEST/AVERAGE);
  // 0 means a single departure.
  [dateInput, timeInput, maxInput, winInput].forEach((elm) =>
    elm.addEventListener('change', syncUrlState));

  // ── Cell size (grid step, metres) ──
  // Live-updates the numeric label while dragging; on release mirrors to the URL
  // and recomputes (a coarser/finer grid needs a fresh backend sample). Sent to
  // travelTimeMap as gridStepM (backend clamps 10–1000 with a max-cells cap).
  function syncCellLabel() { cellVal.textContent = cellInput.value; }
  cellInput.addEventListener('input', syncCellLabel);
  cellInput.addEventListener('change', () => {
    syncCellLabel();
    syncUrlState();
    if (center) compute();
  });

  // Departure time + window minutes -> absolute HH:MM window end for the backend
  // (which takes windowEndTime). Returns null for a single departure (window 0)
  // or when the end would cross midnight (single-departure fallback is fine).
  function windowEndTime() {
    const win = parseInt(winInput.value, 10);
    if (!(win > 0)) return null;
    const m = /^(\d{1,2}):(\d{2})$/.exec(timeInput.value || '');
    if (!m) return null;
    const total = parseInt(m[1], 10) * 60 + parseInt(m[2], 10) + win;
    if (total >= 24 * 60) return null; // crosses midnight -> single departure
    const hh = String(Math.floor(total / 60)).padStart(2, '0');
    const mm = String(total % 60).padStart(2, '0');
    return `${hh}:${mm}`;
  }

  // ── Center: map click, coord input, address search ──
  function setCenter(lat, lng, labelText) {
    center = { lat, lng };
    centerLabel = labelText || null;
    if (!centerMarker) {
      centerMarker = L.marker([lat, lng], { draggable: true }).addTo(map);
      centerMarker.on('dragend', () => {
        const p = centerMarker.getLatLng();
        setCenter(p.lat, p.lng);
      });
    } else {
      centerMarker.setLatLng([lat, lng]);
    }
    centerInput.value = labelText || `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
    syncUrlState();
  }

  // Map interaction split by button:
  //  · RIGHT-click (contextmenu) sets/moves the isochrone CENTRE and recomputes.
  //  · LEFT-click reads the travel time at the clicked point straight from the
  //    already-loaded isochrone cells (no re-routing) so the number ALWAYS
  //    matches the heatmap colour, and shows it in a small popup.
  map.on('contextmenu', (e) => {
    if (e.originalEvent) e.originalEvent.preventDefault();
    setCenter(e.latlng.lat, e.latlng.lng);
    compute();
    return false;
  });
  map.on('click', (e) => {
    if (!center) { setStatus('Right-click the map to set a center.', true); return; }
    showTimeAt(e.latlng.lat, e.latlng.lng);
  });

  // Address autocomplete: same GraphQL query the main app uses. Also accepts a
  // raw "lat, lng" coordinate pair typed directly.
  let addrSeq = 0, addrTimer = null;
  function closeDrop() { addrDrop.style.display = 'none'; }
  function tryParseCoords(raw) {
    const m = raw.trim().match(/^(-?\d+(?:\.\d+)?)\s*[, ]\s*(-?\d+(?:\.\d+)?)$/);
    if (!m) return null;
    const lat = parseFloat(m[1]), lng = parseFloat(m[2]);
    if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
    return { lat, lng };
  }
  centerInput.addEventListener('input', () => {
    const raw = centerInput.value;
    clearTimeout(addrTimer);
    const seq = ++addrSeq;
    if (raw.trim().length < 3 || tryParseCoords(raw)) { closeDrop(); return; }
    addrTimer = setTimeout(async () => {
      try {
        const flat = center ? center.lat : null, flng = center ? center.lng : null;
        const d = await gql(
          'query($q:String!,$flat:Float,$flng:Float){ searchAddresses(query:$q, limit:6, focusLat:$flat, focusLng:$flng){ id label lat lon } }',
          { q: raw, flat, flng },
        );
        if (seq !== addrSeq) return;
        const results = (d && d.searchAddresses) || [];
        addrDrop.textContent = '';
        if (!results.length) { closeDrop(); return; }
        results.forEach((a) => {
          const row = document.createElement('div');
          row.className = 'addr-row';
          row.textContent = a.label;
          row.addEventListener('mousedown', (ev) => {
            ev.preventDefault();
            setCenter(a.lat, a.lon, a.label);
            map.setView([a.lat, a.lon], Math.max(map.getZoom(), 13));
            closeDrop();
          });
          addrDrop.appendChild(row);
        });
        addrDrop.style.display = 'block';
      } catch (_e) { closeDrop(); }
    }, 250);
  });
  centerInput.addEventListener('blur', () => setTimeout(closeDrop, 160));
  centerInput.addEventListener('change', () => {
    const c = tryParseCoords(centerInput.value);
    if (c) { setCenter(c.lat, c.lng); map.setView([c.lat, c.lng], Math.max(map.getZoom(), 13)); }
  });

  // ── URL <-> state sync ───────────────────────────────────────────────────
  // Every control is reflected into the query string (via maas.js's shared
  // syncUrl/readUrl helpers, same replaceState pattern as the main planner) so
  // a view is bookmarkable/shareable. Param names reuse the planner's where
  // equivalent (date/time/modes); travel-map-only knobs get short new names.
  //   lat,lng   — center coordinate      max     — max minutes
  //   name      — resolved center label  agg     — best|avg
  //   window    — departure window (min)  cell    — grid step (m)
  //   z,mlat,mlng — map zoom + view center
  function syncUrlState() {
    if (restoring) return;
    const c = map.getCenter();
    const win = parseInt(winInput.value, 10);
    const cell = parseInt(cellInput.value, 10);
    syncUrl({
      lat:    center ? center.lat.toFixed(6) : null,
      lng:    center ? center.lng.toFixed(6) : null,
      name:   centerLabel || null,
      date:   dateInput.value || null,
      time:   timeInput.value || null,
      max:    parseInt(maxInput.value, 10) || null,
      modes:  selectedModes().join(',') || null,
      agg:    aggregation === 'AVERAGE' ? 'avg' : 'best',
      window: Number.isFinite(win) ? win : null,
      cell:   Number.isFinite(cell) ? cell : null,
      z:      map.getZoom(),
      mlat:   c.lat.toFixed(5),
      mlng:   c.lng.toFixed(5),
    });
  }

  // Debounced map-view persistence: a pan/zoom updates z/mlat/mlng without a
  // reload, mirroring the planner's replaceState-on-change approach.
  let moveTimer = null;
  map.on('moveend', () => {
    clearTimeout(moveTimer);
    moveTimer = setTimeout(syncUrlState, 200);
  });

  // Prefill every control from the query string, then (if a center is present)
  // run the query so a shared link opens straight to its computed map. Returns
  // whether a center was restored (so the caller can auto-compute).
  function loadFromUrl() {
    const p = readUrl();
    restoring = true;
    try {
      if (p.has('date')) dateInput.value = p.get('date');
      if (p.has('time')) timeInput.value = p.get('time');
      if (p.has('max'))  maxInput.value  = p.get('max');
      if (p.has('modes')) {
        const wanted = new Set(p.get('modes').split(',').filter(Boolean));
        modesGrid.querySelectorAll('.mode-cell[data-mode]').forEach((b) =>
          b.classList.toggle('active', wanted.has(b.dataset.mode)));
      }
      if (p.has('agg')) setAgg(p.get('agg') === 'avg' ? 'AVERAGE' : 'BEST');
      // Window minutes: round-trip the shared value; default 30 when absent.
      const win = parseInt(p.get('window'), 10);
      winInput.value = Number.isFinite(win) ? win : 30;
      // Cell size (grid step, m): round-trip; default 200 when absent.
      const cell = parseInt(p.get('cell'), 10);
      cellInput.value = Number.isFinite(cell) ? cell : 200;
      syncCellLabel();
      // Map view first, so setView's own zoom doesn't clobber a shared framing
      // when setCenter later nudges the marker (setCenter never moves the view).
      const z = parseInt(p.get('z'), 10);
      const mlat = parseFloat(p.get('mlat')), mlng = parseFloat(p.get('mlng'));
      if (!isNaN(mlat) && !isNaN(mlng) && !isNaN(z)) map.setView([mlat, mlng], z);

      const lat = parseFloat(p.get('lat')), lng = parseFloat(p.get('lng'));
      let hasCenter = false;
      if (!isNaN(lat) && !isNaN(lng)) {
        setCenter(lat, lng, p.get('name') || null);
        // No shared map view? Frame the center like an address pick.
        if (isNaN(mlat) || isNaN(mlng)) map.setView([lat, lng], Math.max(map.getZoom(), 13));
        hasCenter = true;
      }
      legendMax.textContent = fmtMinutes(parseInt(maxInput.value, 10) * 60);
      return hasCenter;
    } finally {
      restoring = false;
    }
  }

  // ── Compute ──
  async function compute() {
    if (!center) { setStatus('Pick a center first (click the map or search).', true); return; }
    const modes = selectedModes();
    if (!modes.length) { setStatus('Select at least one mode.', true); return; }
    const maxMin = parseInt(maxInput.value, 10);
    if (!(maxMin > 0)) { setStatus('Max time must be a positive number of minutes.', true); return; }
    const maxSeconds = maxMin * 60;
    const cellM = parseInt(cellInput.value, 10);

    const vars = {
      centerLat: center.lat,
      centerLng: center.lng,
      date: dateInput.value || null,
      time: timeInput.value || null,
      maxSeconds,
      modes,
      aggregation,
      windowEndTime: windowEndTime(), // null = single departure (window 0)
      gridStepM: Number.isFinite(cellM) ? cellM : null, // backend clamps 10–1000, default 200
    };

    syncUrlState(); // flush any not-yet-mirrored control state before running
    computeBtn.disabled = true;
    setStatus('Computing…');
    try {
      const t0 = performance.now();
      const d = await gql(TRAVEL_QUERY, vars);
      const m = d.travelTimeMap;
      const dt = ((performance.now() - t0) / 1000).toFixed(1);
      const data = { cells: m.cells, maxSeconds: m.maxSeconds };
      if (!heat) { heat = new HeatLayer(data); heat.addTo(map); }
      else heat.setData(data);
      legendMax.textContent = fmtMinutes(m.maxSeconds);
      setStatus(`${m.cells.length} cells reachable within ${fmtMinutes(m.maxSeconds)} · ${dt}s`);
    } catch (e) {
      setStatus('Error: ' + e.message, true);
    } finally {
      computeBtn.disabled = false;
    }
  }
  computeBtn.addEventListener('click', compute);

  // ── Click-to-time: how long to get to a clicked point ──────────────────────
  // No re-routing. Read the travel time straight from the loaded isochrone cells
  // (bilinear over the reconstructed grid) so the popup number ALWAYS equals the
  // heatmap colour there. Drop a marker at the click and show "≈ N min" (or "not
  // reachable within N min" when outside the sampled/reachable area). The marker
  // and any leftover overlay are cleared on the next click AND on popup close.
  let clickMarker = null;
  let clickLine = null; // reserved: any path a click draws (none in this version)
  function clearClick() {
    if (clickMarker) { map.removeLayer(clickMarker); clickMarker = null; }
    if (clickLine) { map.removeLayer(clickLine); clickLine = null; }
  }
  function showTimeAt(lat, lng) {
    clearClick();
    clickMarker = L.circleMarker([lat, lng], {
      radius: 6, color: '#fff', weight: 2, fillColor: '#ef4444', fillOpacity: 1,
    }).addTo(map);

    const grid = heat && heat._grid;
    const maxSeconds = (heat && heat._data && heat._data.maxSeconds) || 0;
    const secs = sampleSeconds(grid, lat, lng);
    const reachable = isFinite(secs) && (!maxSeconds || secs <= maxSeconds + 1);

    L.popup({ maxWidth: 220, className: 'tt-route-popup' })
      .setLatLng([lat, lng])
      .setContent(timePopupEl(reachable ? { secs } : { unreachable: maxSeconds }))
      .openOn(map);
  }
  // Clear the click marker/overlay whenever its popup is dismissed (X, click-off,
  // or Escape) so nothing lingers. Right-click's centre marker is untouched.
  map.on('popupclose', () => clearClick());

  // Build the click popup DOM (reachable time / not-reachable) with --ds-* styling.
  function timePopupEl(state) {
    const root = mkEl('div', 'tt-route');
    if (state.unreachable != null) {
      const n = Math.round(state.unreachable / 60);
      root.appendChild(mkEl('div', 'tt-route-status err',
        `Not reachable within ${n} min`));
      return root;
    }
    const head = mkEl('div', 'tt-route-head');
    head.appendChild(mkEl('span', 'tt-route-dur', '≈ ' + fmtMinutes(state.secs)));
    root.appendChild(head);
    root.appendChild(mkEl('div', 'tt-route-status', 'travel time from center'));
    return root;
  }

  function setStatus(msg, isErr) {
    statusEl.textContent = msg;
    statusEl.classList.toggle('err', !!isErr);
  }
  function fmtMinutes(secs) {
    const m = Math.round(secs / 60);
    return m + ' min';
  }

  legendMax.textContent = fmtMinutes(parseInt(maxInput.value, 10) * 60);

  // ── Share view (QR code + copyable link) ──────────────────────────────────
  // The URL already tracks state on every change (syncUrlState), so location.href
  // round-trips the current view via loadFromUrl on the target device. The QR is
  // rendered fully client-side (window.QRCode from qr.mjs; no network).
  (function initShare() {
    const btn = $('share-btn');
    const modal = $('share-modal');
    if (!btn || !modal) return;
    const closeBtn = $('share-close');
    const qrHost = $('share-qr');
    const urlInput = $('share-url');
    const copyBtn = $('share-copy');
    let copyTimer = null;

    function openShare() {
      syncUrlState();
      const url = location.href;
      urlInput.value = url;
      try {
        if (window.QRCode) qrHost.innerHTML = window.QRCode.svg(url, { border: 2 });
        else qrHost.textContent = 'QR unavailable.';
      } catch (_e) {
        qrHost.textContent = 'Link too long to encode as a QR code.';
      }
      modal.classList.add('open');
      btn.classList.add('active');
    }
    function closeShare() {
      modal.classList.remove('open');
      btn.classList.remove('active');
    }
    btn.addEventListener('click', () =>
      modal.classList.contains('open') ? closeShare() : openShare());
    closeBtn.addEventListener('click', closeShare);
    modal.addEventListener('click', (e) => { if (e.target === modal) closeShare(); });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && modal.classList.contains('open')) closeShare();
    });
    copyBtn.addEventListener('click', async () => {
      const text = urlInput.value;
      let ok = false;
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
          ok = true;
        }
      } catch (_) { /* fall through to legacy path */ }
      if (!ok) {
        urlInput.focus();
        urlInput.select();
        try { ok = document.execCommand('copy'); } catch (_) { ok = false; }
      }
      copyBtn.textContent = ok ? 'Copied' : 'Copy failed';
      copyBtn.classList.toggle('done', ok);
      clearTimeout(copyTimer);
      copyTimer = setTimeout(() => {
        copyBtn.textContent = 'Copy';
        copyBtn.classList.remove('done');
      }, 1600);
    });
  })();

  // Prefill from a shared link and, if it carried a center, compute immediately.
  if (loadFromUrl()) compute();
}

// Leaflet loads via a classic <script> tag; wait for window.L. Guarded so this
// module can be imported (pure helpers above) under node for unit tests without
// a DOM — the bootstrap only runs in a browser.
if (typeof window !== 'undefined') {
  if (window.L) initTravelMap(window.L);
  else window.addEventListener('load', () => initTravelMap(window.L));
}
