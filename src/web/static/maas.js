'use strict';
// Shared MaaS utilities — included by both index.html and debug.html

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

// Build a leg-time-col cell that reflects realtime: non-RT legs show a plain
// (black) time; RT legs are green; an RT time that differs from schedule shows
// the scheduled time struck through with the realtime time in red.
function mkLegTime(secs, schedSecs, isRealtime) {
  const wrap = mkEl('div', 'leg-time-col');
  if (isRealtime && schedSecs != null && secs !== schedSecs) {
    wrap.classList.add('rt', 'rt-late');
    wrap.appendChild(mkEl('span', 'lt-sched', fmtTime(schedSecs)));
    wrap.appendChild(mkEl('span', 'lt-rt', fmtTime(secs)));
  } else if (isRealtime) {
    wrap.classList.add('rt', 'rt-ontime');
    wrap.appendChild(mkEl('span', 'lt-rt', fmtTime(secs)));
  } else {
    wrap.textContent = fmtTime(secs);
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
  });
  (leg.steps || []).forEach(step => {
    const node = step.place?.node;
    stops.push({
      name:      node?.name ?? null,
      lat:       node?.lat,
      lon:       node?.lon,
      arrival:   step.place?.arrival   ?? null,
      departure: step.place?.departure ?? null,
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
let _stopsCache = null;
let _stopsPromise = null;

function _ensureStops() {
  if (_stopsPromise) return _stopsPromise;
  _stopsPromise = gql('{ gtfsStops { name lat lon mode } }')
    .then(d => { _stopsCache = d?.gtfsStops || []; })
    .catch(() => { _stopsCache = []; });
  return _stopsPromise;
}

function createStopSearch(parentEl, placeholder, onChange) {
  let selLat = null, selLng = null;

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

  function closeDropdown() { drop.style.display = 'none'; highlighted = -1; }
  function getRows() { return Array.from(drop.children); }
  function setHighlight(idx) {
    const rows = getRows();
    rows.forEach((r, i) => { r.classList.toggle('hl', i === idx); });
    highlighted = idx;
  }

  function showDropdown(items) {
    drop.textContent = '';
    highlighted = -1;
    if (!items.length) { closeDropdown(); return; }
    items.slice(0, 12).forEach(s => {
      const row = mkEl('div', 'row');
      row.addEventListener('mouseenter', () => setHighlight(getRows().indexOf(row)));
      const badge = mkEl('span', 'badge', s.mode || '?');
      const name  = mkEl('span', 'name', s.name);
      row.append(badge, name);
      row.addEventListener('mousedown', e => { e.preventDefault(); selectStop(s); });
      drop.appendChild(row);
    });
    drop.style.display = 'block';
  }

  function selectStop(s) {
    input.value = s.name;
    selLat = s.lat;
    selLng = s.lon;
    closeDropdown();
    if (onChange) onChange({ name: s.name, lat: s.lat, lon: s.lon });
  }

  input.addEventListener('input', async () => {
    const q = input.value.trim().toLowerCase();
    selLat = null; selLng = null;
    if (q.length < 2) { closeDropdown(); return; }
    await _ensureStops();
    const matches = (_stopsCache || []).filter(s => s.name.toLowerCase().includes(q));
    showDropdown(matches);
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

  _ensureStops();

  return {
    el: input,
    setValue(name, lat, lon) { input.value = name; selLat = lat; selLng = lon; },
    setCoords(lat, lon)      { input.value = lat.toFixed(5) + ', ' + lon.toFixed(5); selLat = lat; selLng = lon; },
    clear()                  { input.value = ''; selLat = null; selLng = null; },
    getName()                { return input.value; },
    getLat() { return selLat; },
    getLng() { return selLng; },
  };
}
