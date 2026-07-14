// CANONICAL copy. maas.js inlines the same logic (`_normStation`,
// `_scoreStation`, `_rankStations`) — keep the two IN SYNC when editing either.
export const MATCH_TIER = Object.freeze({
  NONE: 0,
  SUBSTRING: 1,
  WORD_PREFIX: 2,
  PREFIX: 3,
  EXACT: 4,
});

// Applied SYMMETRICALLY to query and candidate name.
export function normalizeStationText(s) {
  if (s == null) return '';
  return String(s)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

export function scoreStationName(name, query) {
  const n = normalizeStationText(name);
  const q = normalizeStationText(query);
  if (!q || !n) return MATCH_TIER.NONE;
  if (n === q) return MATCH_TIER.EXACT;
  if (n.startsWith(q)) return MATCH_TIER.PREFIX;
  if (n.split(' ').some(w => w.startsWith(q))) return MATCH_TIER.WORD_PREFIX;
  if (n.includes(q)) return MATCH_TIER.SUBSTRING;
  return MATCH_TIER.NONE;
}

// Squared distance for tie-break ordering only; not a real distance.
function focusDist2(s, focus) {
  if (!focus) return null;
  const { flat, flng } = focus;
  if (!Number.isFinite(flat) || !Number.isFinite(flng)) return null;
  if (!Number.isFinite(s.lat) || !Number.isFinite(s.lng)) return Infinity;
  const dLat = s.lat - flat;
  const dLon = (s.lng - flng) * Math.cos((flat * Math.PI) / 180);
  return dLat * dLat + dLon * dLon;
}

function idKey(s) {
  return s && s.id != null ? String(s.id) : '';
}

export function rankStations(stations, query, focus) {
  const q = normalizeStationText(query);
  if (!q) return [];
  const scored = [];
  for (const s of stations || []) {
    const score = scoreStationName(s.name, q);
    if (score === MATCH_TIER.NONE) continue;
    scored.push({
      s,
      score,
      nameLen: normalizeStationText(s.name).length,
      dist2: focusDist2(s, focus),
    });
  }
  scored.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    if (a.nameLen !== b.nameLen) return a.nameLen - b.nameLen;
    if (a.dist2 != null && b.dist2 != null && a.dist2 !== b.dist2) {
      return a.dist2 - b.dist2;
    }
    return idKey(a.s).localeCompare(idKey(b.s));
  });
  return scored.map(x => Object.assign({ _score: x.score }, x.s));
}
