// Pure, testable station-search ranking. This is the CANONICAL copy, exercised
// by station-rank.test.mjs. maas.js is loaded as a classic <script> and cannot
// import ES modules, so it INLINES the same logic (`_normStation`,
// `_scoreStation`, `_rankStations`) — keep the two IN SYNC when editing either.
//
// The stop-search dropdown used to be a plain `name.includes(q)` substring
// filter, unranked and hard-capped at 8 in cache order — so a well-named station
// (e.g. the SNCB "Libramont") could sort below mid-word substring hits
// ("MALIBRAN") or fall outside the cap entirely. This ranks matches by quality
// so exact / prefix hits float to the top, deterministically.

// Match quality tiers (higher = better). Exposed so callers/tests can assert
// the ordering contract without magic numbers.
export const MATCH_TIER = Object.freeze({
  NONE: 0,
  SUBSTRING: 1,
  WORD_PREFIX: 2,
  PREFIX: 3,
  EXACT: 4,
});

// Lowercase, strip diacritics, fold punctuation to spaces, collapse runs of
// whitespace. Applied SYMMETRICALLY to both the query and each candidate name so
// "Libramont", "libramont" and "LIBRAMONT " all compare equal, and accents the
// user omits ("libramont" vs "Libràmont") still match.
export function normalizeStationText(s) {
  if (s == null) return '';
  return String(s)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')    // combining diacritical marks
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')        // punctuation → space
    .trim()
    .replace(/\s+/g, ' ');
}

// Score one candidate name against a query. Both are normalized internally, so
// callers may pass raw strings. Returns a MATCH_TIER value (0 = no match).
export function scoreStationName(name, query) {
  const n = normalizeStationText(name);
  const q = normalizeStationText(query);
  if (!q || !n) return MATCH_TIER.NONE;
  if (n === q) return MATCH_TIER.EXACT;
  if (n.startsWith(q)) return MATCH_TIER.PREFIX;
  // Whitespace-separated word starts with the query ("gare" matches
  // "Libramont Gare", "Bruxelles Gare Centrale").
  if (n.split(' ').some(w => w.startsWith(q))) return MATCH_TIER.WORD_PREFIX;
  if (n.includes(q)) return MATCH_TIER.SUBSTRING;
  return MATCH_TIER.NONE;
}

// Equirectangular approximation of squared distance — cheap, monotonic, and
// enough for a tie-break ordering (never surfaced as a real distance).
function focusDist2(s, focus) {
  if (!focus) return null;
  const { flat, flng } = focus;
  if (!Number.isFinite(flat) || !Number.isFinite(flng)) return null;
  if (!Number.isFinite(s.lat) || !Number.isFinite(s.lon)) return Infinity;
  const dLat = s.lat - flat;
  const dLon = (s.lon - flng) * Math.cos((flat * Math.PI) / 180);
  return dLat * dLat + dLon * dLon;
}

// Stable string compare on id for the final deterministic tie-break.
function idKey(s) {
  return s && s.id != null ? String(s.id) : '';
}

// Rank + filter stations for a query. Returns a new array of matching stations
// (score attached as `_score`), best first. Ordering, in priority:
//   1. match tier            (exact > prefix > word-prefix > substring)
//   2. shorter normalized name   (closer / less-qualified station hub)
//   3. geo distance to `focus`   (only when focus lat/lng are finite)
//   4. id                        (stable, deterministic final tie-break)
// `focus` is `{ flat, flng }` (the map centre) or null/undefined.
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
    if (a.score !== b.score) return b.score - a.score;          // tier desc
    if (a.nameLen !== b.nameLen) return a.nameLen - b.nameLen;  // shorter first
    if (a.dist2 != null && b.dist2 != null && a.dist2 !== b.dist2) {
      return a.dist2 - b.dist2;                                 // nearer first
    }
    return idKey(a.s).localeCompare(idKey(b.s));                // id, deterministic
  });
  return scored.map(x => Object.assign({ _score: x.score }, x.s));
}
