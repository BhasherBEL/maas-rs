'use strict';
// Shared MaaS utilities — included by index.html

const GRAPHQL_URL = '/graphql';

// ── GraphQL helper ────────────────────────────────────────────
async function gql(query, variables) {
  const res = await fetch(GRAPHQL_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (json.errors) throw new Error(json.errors.map(e => e.message).join('; '));
  return json.data;
}

// ── Formatting ────────────────────────────────────────────────
function fmtTime(secs) {
  if (secs == null) return '—';
  const h = Math.floor(secs / 3600) % 24;
  const m = Math.floor((secs % 3600) / 60);
  return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
}

// Times are seconds since midnight of the QUERY date, so a value >= 86400 lands
// on a following day. fmtTime keeps the correct wall-clock hour (e.g. 170520 →
// "23:12") but drops that day context; these helpers surface it.
function dayOffset(secs) {
  return secs == null ? 0 : Math.floor(secs / 86400);
}

// A styled superscript "+N" day marker element, or null for same-day times.
function mkDayMark(secs) {
  const off = dayOffset(secs);
  return off > 0 ? mkEl('sup', 'day-sup', '+' + off) : null;
}

// HH:MM plus a plain-text " (+N)" day suffix — for prose/tooltip strings that
// cannot host a DOM element. Same-day renders exactly like fmtTime.
function fmtTimeDay(secs) {
  if (secs == null) return '—';
  const off = dayOffset(secs);
  return fmtTime(secs) + (off > 0 ? ' (+' + off + ')' : '');
}

// mkEl carrying a time string plus an appended day marker when it crosses
// midnight. Same-day → byte-identical DOM to mkEl(tag, cls, fmtTime(secs)).
function mkTimeEl(tag, cls, secs) {
  const el = mkEl(tag, cls, fmtTime(secs));
  const mk = mkDayMark(secs);
  if (mk) el.appendChild(mk);
  return el;
}

// Build a leg-time-col cell that reflects realtime: non-RT legs show a plain
// (black) time; RT legs are green; an RT time that differs from schedule shows
// the scheduled time struck through with the realtime time in red.
function mkLegTime(secs, schedSecs, isRealtime) {
  const wrap = mkEl('div', 'leg-time-col');
  if (isRealtime && schedSecs != null && secs !== schedSecs) {
    wrap.classList.add('rt', 'rt-late');
    wrap.appendChild(mkTimeEl('span', 'lt-sched', schedSecs));
    wrap.appendChild(mkTimeEl('span', 'lt-rt', secs));
  } else if (isRealtime) {
    wrap.classList.add('rt', 'rt-ontime');
    wrap.appendChild(mkTimeEl('span', 'lt-rt', secs));
  } else {
    wrap.appendChild(mkTimeEl('span', 'lt-plain', secs));
  }
  return wrap;
}

function fmtTimeSec(secs) {
  if (secs == null) return '—';
  const h = Math.floor(secs / 3600) % 24;
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' + String(s).padStart(2, '0');
}

function fmtMins(secs) {
  const abs  = Math.abs(secs);
  const sign = secs < 0 ? '-' : '';
  const m    = Math.floor(abs / 60);
  const s    = abs % 60;
  return s === 0 ? `${sign}${m}m` : `${sign}${m}m${s}s`;
}

function fmtDuration(secs) {
  if (secs == null || isNaN(secs)) return '—';
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  return h > 0 ? h + 'h' + String(m).padStart(2, '0') : m + 'm';
}

function fmtDelta(secs) {
  if (!secs) return '±0m';
  const sign = secs > 0 ? '+' : '−';
  return sign + fmtDuration(Math.abs(secs));
}

// ── Mode helpers ──────────────────────────────────────────────
const _MODE_COLOR = {
  Bus: '#e65100', Tramway: '#558b2f', Subway: '#6a1b9a',
  Rail: '#1565c0', Ferry: '#00838f', Coach: '#5d4037',
  CableCar: '#827717', Gondola: '#4527a0', Funicular: '#bf360c',
  // Backend may also return uppercase
  BUS: '#e65100', TRAM: '#558b2f', METRO: '#6a1b9a',
  RAIL: '#1565c0', FERRY: '#00838f',
};
const _MODE_GLYPH = {
  Bus: '🚌', Subway: '🚇', Rail: '🚆', Tramway: '🚋', Ferry: '⛴',
  Coach: '🚍', CableCar: '🚠', Gondola: '🚡', Funicular: '🚞',
  Air: '✈', Taxi: '🚕', Other: '•',
};

function modeColor(mode) { return _MODE_COLOR[mode] || '#607d8b'; }
function modeGlyph(mode) { return _MODE_GLYPH[mode] || '•'; }

const _MODE_MARKER = {
  Bus: 'B', BUS: 'B', Subway: 'M', METRO: 'M', Rail: 'R', RAIL: 'R',
  Tramway: 'T', TRAM: 'T', Ferry: 'F', FERRY: 'F', Coach: 'C',
  CableCar: 'C', Gondola: 'G', Funicular: 'F', Air: 'A', Taxi: 'X',
};
function modeMarker(mode) { return _MODE_MARKER[mode] || (mode ? mode[0].toUpperCase() : '•'); }

function safeHex(c) {
  if (typeof c === 'string' && /^[0-9A-Fa-f]{6}$/.test(c)) return '#' + c;
  return null;
}

// Pick black/white text given a background hex (no #).
function contrastText(hex) {
  if (!hex || hex.length < 6) return '#fff';
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  // YIQ luminance
  const yiq = (r * 299 + g * 587 + b * 114) / 1000;
  return yiq >= 150 ? '#000' : '#fff';
}

// Resolve {bg, fg} from GTFS route info or fall back to mode color.
function routeColors(route) {
  if (!route) return { bg: '#607d8b', fg: '#fff' };
  if (route.color) {
    const bg = '#' + route.color;
    const fg = route.textColor ? '#' + route.textColor : contrastText(route.color);
    return { bg, fg };
  }
  const bg = modeColor(route.mode);
  return { bg, fg: '#fff' };
}

// ── DOM helpers ───────────────────────────────────────────────
function mkEl(tag, cls, text) {
  const el = document.createElement(tag);
  if (cls)          el.className   = cls;
  if (text != null) el.textContent = text;
  return el;
}

function mkSvg(d, size = 14) {
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('width', size);
  svg.setAttribute('height', size);
  svg.setAttribute('fill', 'currentColor');
  const path = document.createElementNS(ns, 'path');
  path.setAttribute('d', d);
  svg.appendChild(path);
  return svg;
}

// ── Transit stop list builder ─────────────────────────────────
// Returns [{name, lat, lon, arrival, departure}] for a transit leg.
function buildStopList(leg) {
  const stops = [];
  const fromNode = leg.from?.node;
  stops.push({
    name:      fromNode?.name ?? null,
    lat:       fromNode?.lat  ?? leg.geometry?.[0]?.lat,
    lon:       fromNode?.lon  ?? leg.geometry?.[0]?.lon,
    arrival:   null,
    departure: leg.from?.departure ?? leg.start,
    platform:  leg.from?.platform ?? null,
  });
  (leg.steps || []).forEach(step => {
    const node = step.place?.node;
    stops.push({
      name:      node?.name ?? null,
      lat:       node?.lat,
      lon:       node?.lon,
      arrival:   step.place?.arrival   ?? null,
      departure: step.place?.departure ?? null,
      platform:  step.place?.platform  ?? null,
    });
  });
  return stops;
}

// ── URL sync ──────────────────────────────────────────────────
// syncUrl({ key: value, ... }) — null/'' values are omitted
function syncUrl(params) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '' && v !== false) p.set(k, String(v));
  }
  const qs = p.toString();
  history.replaceState(null, '', qs ? '?' + qs : location.pathname);
}

function readUrl() { return new URLSearchParams(location.search); }

// ── Leaflet map markers ───────────────────────────────────────
function makePin(color) {
  return L.divIcon({
    html: `<div style="width:14px;height:14px;border-radius:50%;background:${color};border:2px solid #fff;box-shadow:0 0 3px rgba(0,0,0,.3)"></div>`,
    iconSize: [14, 14], iconAnchor: [7, 7], className: '',
  });
}
const PIN_ORIGIN = makePin('#22c55e');
const PIN_DEST   = makePin('#ef4444');

function makeStopDot(color, size = 10) {
  return L.divIcon({
    html: `<div style="width:${size}px;height:${size}px;border-radius:50%;background:#fff;border:2px solid ${color};box-shadow:0 0 2px rgba(0,0,0,.4)"></div>`,
    iconSize: [size + 4, size + 4], iconAnchor: [(size + 4) / 2, (size + 4) / 2], className: '',
  });
}

// ── Right-click context menu ──────────────────────────────────
function createContextMenu(map, onFrom, onTo) {
  const menu = mkEl('div');
  Object.assign(menu.style, {
    display: 'none', position: 'fixed', zIndex: '9999',
    background: '#fff', border: '1px solid #ddd', borderRadius: '6px',
    boxShadow: '0 4px 16px rgba(0,0,0,.13)', minWidth: '190px', overflow: 'hidden',
  });

  function menuBtn(label) {
    const b = mkEl('button');
    b.textContent = label;
    Object.assign(b.style, {
      display: 'block', width: '100%', padding: '10px 16px',
      textAlign: 'left', background: 'none', border: 'none',
      fontSize: '13px', cursor: 'pointer', color: '#1e293b',
    });
    b.addEventListener('mouseenter', () => { b.style.background = '#f1f5f9'; });
    b.addEventListener('mouseleave', () => { b.style.background = 'none'; });
    return b;
  }

  const btnFrom = menuBtn('Set as origin');
  const hr      = mkEl('hr');
  hr.style.cssText = 'margin:2px 0;border:none;border-top:1px solid #eee';
  const btnTo   = menuBtn('Set as destination');
  menu.append(btnFrom, hr, btnTo);
  document.body.appendChild(menu);

  let pending = null;
  function hide() { menu.style.display = 'none'; pending = null; }
  function show(x, y, latlng) {
    pending = latlng;
    const vw = window.innerWidth, vh = window.innerHeight;
    menu.style.left = Math.min(x, vw - 200) + 'px';
    menu.style.top  = Math.min(y, vh - 90)  + 'px';
    menu.style.display = 'block';
  }

  btnFrom.addEventListener('click', () => { if (pending) onFrom(pending); hide(); });
  btnTo  .addEventListener('click', () => { if (pending) onTo(pending);   hide(); });

  map.on('contextmenu', e => {
    e.originalEvent.preventDefault();
    show(e.originalEvent.clientX, e.originalEvent.clientY, e.latlng);
  });
  document.addEventListener('click',   e => { if (!menu.contains(e.target)) hide(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') hide(); });

  return { show, hide };
}

// ── Stop search widget ────────────────────────────────────────
let _stationsCache = null;
let _stationsPromise = null;

function _ensureStations() {
  if (_stationsPromise) return _stationsPromise;
  _stationsPromise = gql('{ gtfsStations { id name lat lon operators lines { mode shortName color textColor } } }')
    .then(d => { _stationsCache = d?.gtfsStations || []; })
    .catch(() => { _stationsCache = []; });
  return _stationsPromise;
}

function _operatorLabel(operators) {
  if (!operators || !operators.length) return '🚉';
  if (operators.length === 1) return operators[0];
  return operators[0] + ' +' + (operators.length - 1);
}

let _addressAttr = null;
let _addressAttrPromise = null;
function _ensureAttribution() {
  if (_addressAttrPromise) return _addressAttrPromise;
  _addressAttrPromise = gql('{ addressAttribution }')
    .then(d => { _addressAttr = d?.addressAttribution || ''; })
    .catch(() => { _addressAttr = ''; });
  return _addressAttrPromise;
}

function _focusArgs() {
  const m = typeof window !== 'undefined' && window.map;
  if (m && typeof m.getCenter === 'function') {
    try {
      const c = m.getCenter();
      if (c && Number.isFinite(c.lat) && Number.isFinite(c.lng)) {
        return { flat: c.lat, flng: c.lng };
      }
    } catch (e) {}
  }
  return { flat: null, flng: null };
}

// ── Station-search ranking ────────────────────────────────────
// Inlined here because maas.js is loaded as a CLASSIC <script> (see index.html),
// so it cannot `import`. The CANONICAL, unit-tested copy lives in
// static/js/station-rank.mjs (station-rank.test.mjs) — keep the two IN SYNC.
const STATION_RESULT_LIMIT = 25;   // ranked cap (was an unranked hard 8)
const _MATCH_TIER = { NONE: 0, SUBSTRING: 1, WORD_PREFIX: 2, PREFIX: 3, EXACT: 4 };

function _normStation(s) {
  if (s == null) return '';
  return String(s)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function _scoreStation(name, q) {
  const n = _normStation(name);
  if (!q || !n) return _MATCH_TIER.NONE;
  if (n === q) return _MATCH_TIER.EXACT;
  if (n.startsWith(q)) return _MATCH_TIER.PREFIX;
  if (n.split(' ').some(w => w.startsWith(q))) return _MATCH_TIER.WORD_PREFIX;
  if (n.includes(q)) return _MATCH_TIER.SUBSTRING;
  return _MATCH_TIER.NONE;
}

function _rankStations(stations, query, focus) {
  const q = _normStation(query);
  if (!q) return [];
  const scored = [];
  for (const s of stations || []) {
    const score = _scoreStation(s.name, q);
    if (score === _MATCH_TIER.NONE) continue;
    let dist2 = null;
    if (focus && Number.isFinite(focus.flat) && Number.isFinite(focus.flng)) {
      dist2 = (!Number.isFinite(s.lat) || !Number.isFinite(s.lon)) ? Infinity
        : (() => {
            const dLat = s.lat - focus.flat;
            const dLon = (s.lon - focus.flng) * Math.cos((focus.flat * Math.PI) / 180);
            return dLat * dLat + dLon * dLon;
          })();
    }
    scored.push({ s, score, nameLen: _normStation(s.name).length, dist2 });
  }
  scored.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    if (a.nameLen !== b.nameLen) return a.nameLen - b.nameLen;
    if (a.dist2 != null && b.dist2 != null && a.dist2 !== b.dist2) return a.dist2 - b.dist2;
    const ai = a.s.id != null ? String(a.s.id) : '';
    const bi = b.s.id != null ? String(b.s.id) : '';
    return ai.localeCompare(bi);
  });
  return scored.map(x => x.s);
}

function createStopSearch(parentEl, placeholder, onChange) {
  let selLat = null, selLng = null, selStationId = null;

  const wrapper = mkEl('div');
  wrapper.style.cssText = 'position:relative;width:100%';

  const input = mkEl('input');
  input.type        = 'text';
  input.placeholder = placeholder;
  input.autocomplete = 'off';
  input.className   = 'maas-input';

  const drop = mkEl('div', 'maas-dropdown');

  wrapper.appendChild(input);
  wrapper.appendChild(drop);
  parentEl.appendChild(wrapper);

  let highlighted = -1;
  let stationMatches = [];
  let addressResults = [];
  let addrSeq = 0;
  let addrTimer = null;

  function cancelAddr() { clearTimeout(addrTimer); addrSeq++; addressResults = []; }

  function closeDropdown() { drop.style.display = 'none'; highlighted = -1; }
  function getRows() { return Array.from(drop.querySelectorAll('.row')); }
  function setHighlight(idx) {
    const rows = getRows();
    rows.forEach((r, i) => { r.classList.toggle('hl', i === idx); });
    highlighted = idx;
  }

  function buildStationRow(s) {
    const row = mkEl('div', 'row');
    row.addEventListener('mouseenter', () => setHighlight(getRows().indexOf(row)));

    const head = mkEl('div', 'st-head');
    const name = mkEl('span', 'name', s.name);
    head.append(name);
    if (s.operators && s.operators.length) {
      const op = mkEl('span', 'op-tag', _operatorLabel(s.operators));
      op.title = s.operators.join(', ');
      head.append(op);
    }
    row.append(head);

    const lines = Array.isArray(s.lines) ? s.lines : [];
    const byMode = [];
    lines.forEach(l => {
      let g = byMode.find(x => x.mode === l.mode);
      if (!g) { g = { mode: l.mode, items: [] }; byMode.push(g); }
      g.items.push(l);
    });
    byMode.forEach(g => {
      const sub = mkEl('div', 'st-mode');
      const marker = mkEl('span', 'mode-marker', modeMarker(g.mode));
      marker.title = g.mode;
      sub.append(marker);
      const lineWrap = mkEl('div', 'mode-lines');
      g.items.forEach(l => {
        const badge = mkEl('span', 'line-badge', l.shortName || '?');
        const bg = safeHex(l.color);
        const fg = safeHex(l.textColor);
        if (bg) { badge.style.background = bg; badge.style.color = fg || '#fff'; badge.classList.add('colored'); }
        lineWrap.append(badge);
      });
      sub.append(lineWrap);
      row.append(sub);
    });

    row.addEventListener('mousedown', e => { e.preventDefault(); selectStop(s); });
    return row;
  }

  function buildAddressRow(a) {
    const row = mkEl('div', 'row addr-row');
    row.addEventListener('mouseenter', () => setHighlight(getRows().indexOf(row)));
    const head = mkEl('div', 'st-head');
    head.append(mkEl('span', 'addr-marker', '📍'));
    head.append(mkEl('span', 'name', a.label));
    row.append(head);
    row.addEventListener('mousedown', e => { e.preventDefault(); selectAddress(a); });
    return row;
  }

  function renderResults() {
    drop.textContent = '';
    highlighted = -1;
    const sts = stationMatches.slice(0, STATION_RESULT_LIMIT);
    const addrs = addressResults.slice(0, 6);
    if (!sts.length && !addrs.length) { closeDropdown(); return; }
    sts.forEach(s => drop.appendChild(buildStationRow(s)));
    if (addrs.length) {
      drop.appendChild(mkEl('div', 'group-head', 'Addresses'));
      addrs.forEach(a => drop.appendChild(buildAddressRow(a)));
      if (_addressAttr) drop.appendChild(mkEl('div', 'addr-attr', _addressAttr));
    }
    drop.style.display = 'block';
  }

  function selectStop(s) {
    cancelAddr();
    input.value = s.name;
    selLat = s.lat;
    selLng = s.lon;
    selStationId = s.id != null ? String(s.id) : null;
    closeDropdown();
    if (onChange) onChange({ name: s.name, lat: s.lat, lon: s.lon });
  }

  function selectAddress(a) {
    cancelAddr();
    input.value = a.label;
    selLat = a.lat;
    selLng = a.lon;
    selStationId = null;
    closeDropdown();
    if (onChange) onChange({ name: a.label, lat: a.lat, lon: a.lon });
  }

  input.addEventListener('input', async () => {
    addressResults = [];
    const raw0 = input.value;
    const raw = input.value.trim();
    const q = raw.toLowerCase();
    selLat = null; selLng = null; selStationId = null;
    if (q.length < 2) { cancelAddr(); closeDropdown(); return; }
    await _ensureStations();
    if (input.value !== raw0) return;
    stationMatches = _rankStations(_stationsCache || [], raw, _focusArgs());
    renderResults();
    clearTimeout(addrTimer);
    const seq = ++addrSeq;
    addrTimer = setTimeout(async () => {
      try {
        const f = _focusArgs();
        const d = await gql('query($q:String!,$flat:Float,$flng:Float){ searchAddresses(query:$q, limit:6, focusLat:$flat, focusLng:$flng){ id label lat lon } }', { q: raw, flat: f.flat, flng: f.flng });
        if (seq !== addrSeq) return;
        addressResults = d?.searchAddresses || [];
        renderResults();
      } catch (e) {
        if (seq === addrSeq) { addressResults = []; renderResults(); }
      }
    }, 250);
  });

  input.addEventListener('blur', () => setTimeout(closeDropdown, 160));

  input.addEventListener('keydown', e => {
    const rows = getRows();
    if (!rows.length) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); setHighlight(Math.min(highlighted + 1, rows.length - 1)); }
    if (e.key === 'ArrowUp')   { e.preventDefault(); setHighlight(Math.max(highlighted - 1, 0)); }
    if (e.key === 'Enter' && highlighted >= 0) {
      e.preventDefault();
      rows[highlighted].dispatchEvent(new MouseEvent('mousedown'));
    }
    if (e.key === 'Escape') closeDropdown();
  });

  _ensureStations();
  _ensureAttribution();

  return {
    el: input,
    setValue(name, lat, lon, stationId) { cancelAddr(); input.value = name; selLat = lat; selLng = lon; selStationId = stationId != null ? String(stationId) : null; },
    setCoords(lat, lon)      { cancelAddr(); input.value = lat.toFixed(5) + ', ' + lon.toFixed(5); selLat = lat; selLng = lon; selStationId = null; },
    clear()                  { cancelAddr(); input.value = ''; selLat = null; selLng = null; selStationId = null; },
    getName()                { return input.value; },
    getLat() { return selLat; },
    getLng() { return selLng; },
    getStationId() { return selStationId; },
  };
}
