use std::collections::{BTreeMap, HashMap, HashSet};

use fst::automaton::Levenshtein;
use fst::{IntoStreamer, Set, Streamer};
use serde::{Deserialize, Serialize};

use super::geo::LatLng;

/// CC-BY 4.0 attribution string for the BeST Address open data. Surfaced via the
/// GraphQL API so clients can display the required credit.
pub const ADDRESS_ATTRIBUTION: &str =
    "Address data © FPS BOSA — BeSt Address, licensed under CC BY 4.0";

/// Fixed ratio applied to municipality-token match factors relative to a
/// street-token match: a street-name hit weighs strictly more than a
/// municipality-only hit (street weight ≥ muni weight). Multiplicative, so
/// `muni_exact = STREET_MUNI_WEIGHT_RATIO` and `muni_prefix =
/// STREET_MUNI_WEIGHT_RATIO * prefix_token_weight`.
const STREET_MUNI_WEIGHT_RATIO: f64 = 0.9;

/// Runtime tuning for [`AddressIndex::search`] proximity/relevance ranking. Held
/// on the index as a `#[serde(skip)]` field (so `address.bin`'s serialized layout
/// is unchanged — no schema bump), defaulted on load and overridden from
/// `config.yaml` (`default_routing.address_*`) at serve time.
///
/// Defaults encode the researched Stage-1 model:
/// - `geo_offset_km` 2.0 — full geo score within 2 km of the focus point.
/// - `geo_half_score_km` 5.0 — a candidate 5 km away keeps half its geo score;
///   the exponential scale is derived as `(half - offset)/ln(2)`.
/// - `geo_floor` 0.1 — a far but perfect text match is never fully buried.
/// - `prefix_token_weight` 0.6 — a prefix-only token match scores 0.6 of an exact.
/// - `house_number_boost` 1.5 — an exact house-number match outranks a prefix one.
///
/// Stage-3 typo tolerance (`fuzzy_*`). The exact/prefix pass runs first; a fuzzy
/// pass only widens the search when too few streets resolved, so precision and
/// latency on clean queries are unchanged:
/// - `fuzzy_trigger_k` 5 — run the fuzzy fallback only when fewer than this many
///   streets were covered by exact/prefix matching (a clean query that already
///   resolves ≥ k streets never triggers fuzzy, so its results are byte-identical).
/// - `fuzzy_min_len_1typo` 3 / `fuzzy_min_len_2typos` 8 — length gate on the query
///   token (char count): 1–2 chars ⇒ 0 edits (a short token is prefix intent, never
///   a typo), 3–7 ⇒ 1 edit, ≥8 ⇒ 2 edits. Bounds the candidate set and reflects that
///   longer words absorb more error.
/// - `fuzzy_token_weight` 0.4 — a token matched only via fuzzy scores below an exact
///   (1.0) or prefix (0.6) token, so corrected matches rank under literal ones.
///
/// Number tokens (house number / postcode) are NEVER fuzzed, and a fuzzy match is
/// only accepted when its first character equals the query token's (prefix_length=1:
/// users rarely mistype the first letter, and it bounds the automaton's matches).
#[derive(Debug, Clone, Copy)]
pub struct AddressSearchParams {
    pub geo_offset_km: f64,
    pub geo_half_score_km: f64,
    pub geo_floor: f64,
    pub prefix_token_weight: f64,
    pub house_number_boost: f64,
    pub fuzzy_trigger_k: usize,
    pub fuzzy_min_len_1typo: usize,
    pub fuzzy_min_len_2typos: usize,
    pub fuzzy_token_weight: f64,
}

impl Default for AddressSearchParams {
    fn default() -> Self {
        AddressSearchParams {
            geo_offset_km: 2.0,
            geo_half_score_km: 5.0,
            geo_floor: 0.1,
            prefix_token_weight: 0.6,
            house_number_boost: 1.5,
            fuzzy_trigger_k: 5,
            fuzzy_min_len_1typo: 3,
            fuzzy_min_len_2typos: 8,
            fuzzy_token_weight: 0.4,
        }
    }
}

impl AddressSearchParams {
    /// Exponential decay scale (km) such that the geo score halves at
    /// `geo_half_score_km`: `scale = (half - offset)/ln(2)`.
    fn geo_scale_km(&self) -> f64 {
        ((self.geo_half_score_km - self.geo_offset_km) / std::f64::consts::LN_2).max(f64::EPSILON)
    }

    /// Distance decay in `[geo_floor, 1.0]` for a candidate at `dist_km` from the
    /// focus point: full score within `geo_offset_km`, then exponential, floored.
    fn geo_decay(&self, dist_km: f64) -> f64 {
        let excess = (dist_km - self.geo_offset_km).max(0.0);
        (-excess / self.geo_scale_km()).exp().max(self.geo_floor)
    }

    /// Maximum edit distance allowed for a query token of `len` characters under
    /// the length gate: `len ≥ fuzzy_min_len_2typos` ⇒ 2, `len ≥ fuzzy_min_len_1typo`
    /// ⇒ 1, otherwise 0 (no fuzzy for very short tokens).
    fn max_edits(&self, len: usize) -> u32 {
        if len >= self.fuzzy_min_len_2typos {
            2
        } else if len >= self.fuzzy_min_len_1typo {
            1
        } else {
            0
        }
    }
}

/// One mailbox / apartment ("bus" / "bte") inside a building, collapsed into the
/// building's [`AddressRecord`] as metadata. `label` is the BeST `boxNumber` value
/// (e.g. `3`, `0023`, `A`). The coordinate is the box's own position when the
/// building's boxes diverge beyond `address_box_coord_epsilon_m`; when they all sit
/// within that epsilon the box coordinates are collapsed to the building centroid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressBox {
    pub label: String,
    pub lat: f64,
    pub lon: f64,
}

/// A geocoded building, keyed at build time by `(street, house_number)` so the
/// apartment/box rows BeST models as separate addresses collapse into one
/// candidate. `street`/`municipality`/`postal` are indices into the interned
/// tables on [`AddressIndex`]; `lat`/`lon` is the representative (centroid)
/// coordinate; `boxes` carries every box at this house number as metadata (empty
/// when the building has none).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddressRecord {
    pub id: String,
    pub street: u32,
    pub municipality: u32,
    pub postal: u32,
    pub house_number: String,
    pub lat: f64,
    pub lon: f64,
    pub boxes: Vec<AddressBox>,
}

/// A ranked search result with a display label and resolved fields.
#[derive(Debug, Clone, PartialEq)]
pub struct AddressHit {
    pub id: String,
    pub label: String,
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub house_number: String,
    pub postcode: String,
    pub municipality: String,
}

/// An interned street / municipality entity: a display name plus every language
/// spelling, all of which are searchable aliases pointing to the same id.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Named {
    pub display: String,
    pub aliases: Vec<String>,
}

/// In-memory address search index. The interned tables and compact rows are
/// serialized to `address.bin`; the token/prefix lookup structures are rebuilt
/// from them on load (so they are `#[serde(skip)]`).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct AddressIndex {
    streets: Vec<Named>,
    municipalities: Vec<Named>,
    postals: Vec<String>,
    records: Vec<AddressRecord>,

    /// Normalized word token → street ids (prefix-searchable via range scan).
    #[serde(skip)]
    street_tokens: BTreeMap<String, Vec<u32>>,
    /// Normalized word token → municipality ids.
    #[serde(skip)]
    muni_tokens: BTreeMap<String, Vec<u32>>,
    /// Street id → record ids.
    #[serde(skip)]
    street_records: Vec<Vec<u32>>,
    /// Street id → distinct municipality ids across its records.
    #[serde(skip)]
    street_munis: Vec<Vec<u32>>,
    /// Municipality id → street ids that have a record in it.
    #[serde(skip)]
    muni_streets: HashMap<u32, Vec<u32>>,

    /// FST over the unique `street_tokens` keys, for length-gated fuzzy lookup of
    /// misspelled street tokens. Rebuilt from the BTreeMap on load (so the
    /// serialized layout is unchanged — no schema bump).
    #[serde(skip)]
    street_token_fst: Option<Set<Vec<u8>>>,
    /// FST over the unique `muni_tokens` keys (fuzzy municipality lookup).
    #[serde(skip)]
    muni_token_fst: Option<Set<Vec<u8>>>,

    /// Proximity/relevance tuning. Rebuilt from defaults on load (so the
    /// serialized layout is unchanged) and overridden from config at serve time.
    #[serde(skip)]
    params: AddressSearchParams,
}

/// Lowercase, strip common French/Dutch/German accents, drop punctuation, and
/// collapse whitespace. Used for both indexing and querying so "stupid matching"
/// is accent- and case-insensitive.
pub fn normalize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = true;
    for ch in s.chars().flat_map(|c| c.to_lowercase()) {
        let mapped = match ch {
            'à' | 'á' | 'â' | 'ä' | 'ã' | 'å' => 'a',
            'ç' => 'c',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ñ' => 'n',
            'ò' | 'ó' | 'ô' | 'ö' | 'õ' => 'o',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ý' | 'ÿ' => 'y',
            'ß' => 's',
            c => c,
        };
        if mapped.is_ascii_alphanumeric() {
            out.push(mapped);
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

/// Recognized box-reference keywords (accent-folded, lowercased). A standalone
/// keyword takes the following token as its value; an attached keyword (`bus3`,
/// `bte3`, `boite3`) takes its alphanumeric suffix.
const BOX_KEYWORDS: [&str; 3] = ["bus", "bte", "boite"];

/// Maximum character length of a recognized box value. A real Belgian box value is
/// short (`3`, `0023`, `B12`); a long alphabetic suffix (`laan`, `straat`) is never
/// a box, so it gates keyword-prefixed street names out of the box grammar.
const MAX_BOX_VALUE_LEN: usize = 5;

/// Lowercase + accent-fold a single raw token *without* dropping the `/`, `:` and
/// `.` punctuation the box grammar relies on (full [`normalize`] eats them). Mirrors
/// the accent map of `normalize` so `boîte` folds to `boite`.
fn fold_box_token(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars().flat_map(|c| c.to_lowercase()) {
        let mapped = match ch {
            'à' | 'á' | 'â' | 'ä' | 'ã' | 'å' => 'a',
            'ç' => 'c',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ñ' => 'n',
            'ò' | 'ó' | 'ô' | 'ö' | 'õ' => 'o',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ý' | 'ÿ' => 'y',
            'ß' => 's',
            c => c,
        };
        out.push(mapped);
    }
    out
}

/// Strip the box value down to the searchable form: drop everything that is not
/// ASCII alphanumeric (so `:3`, `/3`, `n°3` all reduce to `3`), keeping case-folded
/// digits/letters.
fn clean_box_value(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// Whether `v` (already reduced via [`clean_box_value`]) has the shape of a real
/// box value: short (≤ [`MAX_BOX_VALUE_LEN`]) and either all digits (`3`, `0023`)
/// or a single leading letter optionally followed by digits (`A`, `B12`). A long
/// alphabetic suffix (`laan`, `straat`, `instein`) is never a box value.
fn is_box_value(v: &str) -> bool {
    let len = v.chars().count();
    if len == 0 || len > MAX_BOX_VALUE_LEN {
        return false;
    }
    if v.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let mut chars = v.chars();
    let first = chars.next().expect("non-empty");
    first.is_ascii_alphabetic() && chars.all(|c| c.is_ascii_digit())
}

/// Extract at most ONE box reference (`16 bus N`, `16 bte N`, `16 bN`, `16 /N`,
/// `16 boite N`, `16 bus: N`, …) from the raw query, returning the query with the
/// box span removed and the normalized box value. A box reference is recognized
/// only when BOTH hold: (1) POSITIONAL — it appears strictly after a numeric
/// house-number token (a token whose first character is a digit), so a leading
/// street word like "Buslaan", "boite" in "rue de la boite 5", or "b" in
/// "avenue b 12" is never swallowed; and (2) SHAPE — its value is box-shaped per
/// [`is_box_value`] (short, all-digits or a single letter then digits), so
/// keyword-prefixed streets ("buslaan" → "laan", "bteinstein" → "instein") are
/// rejected. A keyword with no box-shaped value is left untouched.
fn parse_box_reference(query: &str) -> (String, Option<String>) {
    let raw: Vec<&str> = query.split_whitespace().collect();
    let folded: Vec<String> = raw.iter().map(|t| fold_box_token(t)).collect();
    let house_before = |i: usize| -> bool {
        folded[..i]
            .iter()
            .any(|t| t.chars().next().is_some_and(|c| c.is_ascii_digit()))
    };
    let next_box_value = |i: usize| -> Option<String> {
        let nv = clean_box_value(folded.get(i + 1)?);
        is_box_value(&nv).then_some(nv)
    };

    for i in 0..raw.len() {
        if !house_before(i) {
            continue;
        }
        let tok = &folded[i];

        if let Some(rest) = tok.strip_prefix('/') {
            let v = clean_box_value(rest);
            if is_box_value(&v) {
                return (remove_indices(&raw, i, i), Some(v));
            }
            if v.is_empty() {
                if let Some(nv) = next_box_value(i) {
                    return (remove_indices(&raw, i, i + 1), Some(nv));
                }
            }
            continue;
        }

        let stripped = tok.trim_end_matches([':', '.']);
        if BOX_KEYWORDS.contains(&stripped) || stripped == "b" {
            if let Some(nv) = next_box_value(i) {
                return (remove_indices(&raw, i, i + 1), Some(nv));
            }
            continue;
        }

        for kw in BOX_KEYWORDS {
            if let Some(rest) = tok.strip_prefix(kw) {
                let v = clean_box_value(rest);
                if is_box_value(&v) {
                    return (remove_indices(&raw, i, i), Some(v));
                }
            }
        }
        if let Some(rest) = tok.strip_prefix('b') {
            let v = clean_box_value(rest);
            if is_box_value(&v) {
                return (remove_indices(&raw, i, i), Some(v));
            }
        }
    }
    (query.to_string(), None)
}

/// Re-join `raw` tokens with the inclusive index span `from..=to` removed.
fn remove_indices(raw: &[&str], from: usize, to: usize) -> String {
    raw.iter()
        .enumerate()
        .filter(|(i, _)| *i < from || *i > to)
        .map(|(_, t)| *t)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether a stored box `label` equals the query box value `q`, leading-zero /
/// numeric-insensitively: BeST stores box labels zero-padded (`0003`) but users
/// type `bus 3`. When both the normalized label and the query are all digits they
/// compare as numbers (leading zeros stripped); otherwise it is a plain
/// equality (so a non-numeric label like `A` keeps its exact behavior).
fn box_label_eq(label: &str, q: &str) -> bool {
    let l = normalize(label);
    if l == q {
        return true;
    }
    let numeric = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
    numeric(&l) && numeric(q) && l.trim_start_matches('0') == q.trim_start_matches('0')
}

fn push_unique(map: &mut BTreeMap<String, Vec<u32>>, token: String, id: u32) {
    let v = map.entry(token).or_default();
    if v.last() != Some(&id) {
        v.push(id);
    }
}

impl AddressIndex {
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn street_count(&self) -> usize {
        self.streets.len()
    }

    /// Override the proximity/relevance tuning (from `config.yaml` at serve time).
    pub fn set_search_params(&mut self, params: AddressSearchParams) {
        self.params = params;
    }

    /// Rebuild every `#[serde(skip)]` lookup structure from the interned tables
    /// and records. Called after construction and after deserialization.
    pub fn rebuild_indexes(&mut self) {
        self.street_tokens.clear();
        self.muni_tokens.clear();
        self.street_records = vec![Vec::new(); self.streets.len()];
        self.street_munis = vec![Vec::new(); self.streets.len()];
        self.muni_streets.clear();

        for (sid, s) in self.streets.iter().enumerate() {
            let sid = sid as u32;
            for alias in &s.aliases {
                for tok in normalize(alias).split_whitespace() {
                    push_unique(&mut self.street_tokens, tok.to_string(), sid);
                }
            }
        }
        for (mid, m) in self.municipalities.iter().enumerate() {
            let mid = mid as u32;
            for alias in &m.aliases {
                for tok in normalize(alias).split_whitespace() {
                    push_unique(&mut self.muni_tokens, tok.to_string(), mid);
                }
            }
        }
        for (rid, r) in self.records.iter().enumerate() {
            let rid = rid as u32;
            let s = r.street as usize;
            self.street_records[s].push(rid);
            if !self.street_munis[s].contains(&r.municipality) {
                self.street_munis[s].push(r.municipality);
                self.muni_streets
                    .entry(r.municipality)
                    .or_default()
                    .push(r.street);
            }
        }

        self.street_token_fst =
            Some(Set::from_iter(self.street_tokens.keys()).expect("street token FST"));
        self.muni_token_fst =
            Some(Set::from_iter(self.muni_tokens.keys()).expect("muni token FST"));
    }

    /// Length-gated fuzzy lookup: stream the token FST for keys within `max_edits`
    /// of `token`, keep only those whose first character equals the query token's
    /// (prefix_length=1 prune), and fold their entity ids together. `fst` uses
    /// standard Levenshtein (a transposition counts as 2 edits).
    fn fuzzy_ids(
        fst: Option<&Set<Vec<u8>>>,
        map: &BTreeMap<String, Vec<u32>>,
        token: &str,
        max_edits: u32,
    ) -> HashSet<u32> {
        let mut out = HashSet::new();
        let (Some(set), Ok(lev)) = (fst, Levenshtein::new(token, max_edits)) else {
            return out;
        };
        let first = token.chars().next();
        let mut stream = set.search(&lev).into_stream();
        while let Some(key) = stream.next() {
            let Ok(matched) = std::str::from_utf8(key) else {
                continue;
            };
            if matched.chars().next() != first {
                continue;
            }
            if let Some(ids) = map.get(matched) {
                out.extend(ids.iter().copied());
            }
        }
        out
    }

    fn prefix_ids(map: &BTreeMap<String, Vec<u32>>, token: &str) -> HashSet<u32> {
        let mut out = HashSet::new();
        for (k, ids) in map.range(token.to_string()..) {
            if !k.starts_with(token) {
                break;
            }
            out.extend(ids.iter().copied());
        }
        out
    }

    /// Per word-token text factor in `(0, 1]` for one candidate street/record:
    /// an exact alias token scores full (street 1.0, municipality
    /// `STREET_MUNI_WEIGHT_RATIO`), a prefix-only token scores `prefix_token_weight`
    /// of that, and a token matched only via the fuzzy fallback scores the lower
    /// `fuzzy_token_weight`. Takes the best of the street/municipality match for
    /// this token. Returns `None` when the token matches neither.
    #[allow(clippy::too_many_arguments)]
    fn token_factor(
        &self,
        token: &str,
        sid: u32,
        mid: u32,
        street_set: &HashSet<u32>,
        muni_set: &HashSet<u32>,
        fuzzy_street_set: &HashSet<u32>,
        fuzzy_muni_set: &HashSet<u32>,
    ) -> Option<f64> {
        let prefix = self.params.prefix_token_weight;
        let fuzzy = self.params.fuzzy_token_weight;
        let mut best: Option<f64> = None;
        if street_set.contains(&sid) {
            let exact = self
                .street_tokens
                .get(token)
                .is_some_and(|ids| ids.contains(&sid));
            best = Some(best.map_or(0.0, |b: f64| b).max(if exact { 1.0 } else { prefix }));
        } else if fuzzy_street_set.contains(&sid) {
            best = Some(best.map_or(0.0, |b: f64| b).max(fuzzy));
        }
        if muni_set.contains(&mid) {
            let exact = self
                .muni_tokens
                .get(token)
                .is_some_and(|ids| ids.contains(&mid));
            let f = STREET_MUNI_WEIGHT_RATIO * if exact { 1.0 } else { prefix };
            best = Some(best.map_or(f, |b| b.max(f)));
        } else if fuzzy_muni_set.contains(&mid) {
            let f = STREET_MUNI_WEIGHT_RATIO * fuzzy;
            best = Some(best.map_or(f, |b| b.max(f)));
        }
        best
    }

    /// Search the index. The query is normalized then split into word tokens
    /// (street / municipality names) and number tokens (house number / postcode).
    /// A street matches when every word token prefix-matches one of its aliases
    /// or one of its municipalities; number tokens then filter the house number
    /// or postcode.
    ///
    /// When the exact/prefix pass covers fewer than `fuzzy_trigger_k` streets, a
    /// length-gated, first-character-pruned fuzzy fallback widens the WORD tokens
    /// (never number tokens) so misspelled streets still resolve; fuzzy-only token
    /// matches score `fuzzy_token_weight` (< prefix < exact). A clean query that
    /// already covers ≥ `fuzzy_trigger_k` streets skips fuzzy entirely, so its
    /// results are unchanged.
    ///
    /// Candidates are ranked by `text_score * geo_decay` (higher first, then a
    /// deterministic tie-break on record id). `text_score` is the mean per-token
    /// factor (exact vs prefix vs fuzzy, street vs municipality) times the exact
    /// house-number boost; `geo_decay` is an exponential distance falloff toward
    /// `focus = (lat, lon)` (the map the user is viewing). `focus == None` ⇒
    /// `geo_decay = 1.0`, reducing to pure deterministic text ranking. Returns up
    /// to `limit` ranked hits.
    pub fn search(
        &self,
        query: &str,
        limit: usize,
        focus: Option<(f64, f64)>,
    ) -> Vec<AddressHit> {
        if limit == 0 {
            return Vec::new();
        }
        let (remainder, box_token) = parse_box_reference(query);
        let qn = normalize(&remainder);
        let tokens: Vec<&str> = qn.split_whitespace().collect();
        if tokens.is_empty() {
            return Vec::new();
        }

        let mut word_tokens: Vec<&str> = Vec::new();
        let mut number_tokens: Vec<&str> = Vec::new();
        for t in &tokens {
            if t.chars().any(|c| c.is_ascii_digit()) {
                number_tokens.push(t);
            } else {
                word_tokens.push(t);
            }
        }
        if word_tokens.is_empty() {
            return Vec::new();
        }

        let street_sets: Vec<HashSet<u32>> = word_tokens
            .iter()
            .map(|t| Self::prefix_ids(&self.street_tokens, t))
            .collect();
        let muni_sets: Vec<HashSet<u32>> = word_tokens
            .iter()
            .map(|t| Self::prefix_ids(&self.muni_tokens, t))
            .collect();

        let mut pool: HashSet<u32> = HashSet::new();
        for s in &street_sets {
            pool.extend(s.iter().copied());
        }
        for m in &muni_sets {
            for mid in m {
                if let Some(streets) = self.muni_streets.get(mid) {
                    pool.extend(streets.iter().copied());
                }
            }
        }

        let mut fuzzy_street_sets: Vec<HashSet<u32>> = vec![HashSet::new(); word_tokens.len()];
        let mut fuzzy_muni_sets: Vec<HashSet<u32>> = vec![HashSet::new(); word_tokens.len()];

        let covered = |sid: u32, fz_s: &[HashSet<u32>], fz_m: &[HashSet<u32>]| -> bool {
            let s = sid as usize;
            word_tokens.iter().enumerate().all(|(i, _)| {
                street_sets[i].contains(&sid)
                    || fz_s[i].contains(&sid)
                    || self
                        .street_munis[s]
                        .iter()
                        .any(|mid| muni_sets[i].contains(mid) || fz_m[i].contains(mid))
            })
        };

        let mut covered_count = 0usize;
        for &sid in &pool {
            if covered(sid, &fuzzy_street_sets, &fuzzy_muni_sets) {
                covered_count += 1;
                if covered_count >= self.params.fuzzy_trigger_k {
                    break;
                }
            }
        }

        if covered_count < self.params.fuzzy_trigger_k {
            for (i, t) in word_tokens.iter().enumerate() {
                let edits = self.params.max_edits(t.chars().count());
                if edits == 0 {
                    continue;
                }
                for id in Self::fuzzy_ids(
                    self.street_token_fst.as_ref(),
                    &self.street_tokens,
                    t,
                    edits,
                ) {
                    if !street_sets[i].contains(&id) {
                        fuzzy_street_sets[i].insert(id);
                    }
                }
                for id in
                    Self::fuzzy_ids(self.muni_token_fst.as_ref(), &self.muni_tokens, t, edits)
                {
                    if !muni_sets[i].contains(&id) {
                        fuzzy_muni_sets[i].insert(id);
                    }
                }
            }
            for s in &fuzzy_street_sets {
                pool.extend(s.iter().copied());
            }
            for m in &fuzzy_muni_sets {
                for mid in m {
                    if let Some(streets) = self.muni_streets.get(mid) {
                        pool.extend(streets.iter().copied());
                    }
                }
            }
        }

        let mut scored: Vec<(f64, u32)> = Vec::new();
        for &sid in &pool {
            let s = sid as usize;
            if !covered(sid, &fuzzy_street_sets, &fuzzy_muni_sets) {
                continue;
            }
            for &rid in &self.street_records[s] {
                let r = &self.records[rid as usize];
                let factors: Option<Vec<f64>> = word_tokens
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        self.token_factor(
                            t,
                            sid,
                            r.municipality,
                            &street_sets[i],
                            &muni_sets[i],
                            &fuzzy_street_sets[i],
                            &fuzzy_muni_sets[i],
                        )
                    })
                    .collect();
                let Some(factors) = factors else { continue };
                let hn = normalize(&r.house_number);
                let pc = &self.postals[r.postal as usize];
                let nums_ok = number_tokens
                    .iter()
                    .all(|n| hn == *n || hn.starts_with(n) || pc == n);
                if !nums_ok {
                    continue;
                }
                let base = factors.iter().sum::<f64>() / factors.len() as f64;
                let mut text_score = base;
                if number_tokens.iter().any(|n| hn == *n) {
                    text_score *= self.params.house_number_boost;
                }
                let geo_decay = match focus {
                    Some((flat, flon)) => {
                        let dist_km =
                            LatLng::distance(&[flat, flon], &[r.lat, r.lon]) / 1000.0;
                        self.params.geo_decay(dist_km)
                    }
                    None => 1.0,
                };
                scored.push((text_score * geo_decay, rid));
            }
        }

        if let Some(box_token) = &box_token {
            scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            scored.truncate(limit);
            return scored
                .into_iter()
                .map(|(_, rid)| self.hit_box(rid, box_token))
                .collect();
        }
        if !number_tokens.is_empty() {
            scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            scored.truncate(limit);
            return scored
                .into_iter()
                .map(|(_, rid)| self.hit_building(rid))
                .collect();
        }
        self.group_by_street(scored, limit)
    }

    /// Street-level collapse (no number token in the query): one hit per
    /// `(street, municipality)`, scored by the MAX of the group's building scores
    /// (preserving proximity/fuzzy ranking) with a deterministic record-id
    /// tie-break, the coordinate set to the centroid of the matched buildings.
    fn group_by_street(&self, scored: Vec<(f64, u32)>, limit: usize) -> Vec<AddressHit> {
        let mut groups: HashMap<(u32, u32), (f64, u32, f64, f64, usize)> = HashMap::new();
        for (score, rid) in scored {
            let r = &self.records[rid as usize];
            let e = groups
                .entry((r.street, r.municipality))
                .or_insert((f64::NEG_INFINITY, rid, 0.0, 0.0, 0));
            if score > e.0 || (score == e.0 && rid < e.1) {
                e.0 = score;
                e.1 = rid;
            }
            e.2 += r.lat;
            e.3 += r.lon;
            e.4 += 1;
        }
        let mut ranked: Vec<(f64, u32, f64, f64, usize)> = groups.into_values().collect();
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        ranked.truncate(limit);
        ranked
            .into_iter()
            .map(|(_, rid, sum_lat, sum_lon, n)| {
                let r = &self.records[rid as usize];
                let street = self.streets[r.street as usize].display.clone();
                let municipality = self.municipalities[r.municipality as usize].display.clone();
                let postcode = self.postals[r.postal as usize].clone();
                let label = format!("{street}, {postcode} {municipality}");
                AddressHit {
                    id: r.id.clone(),
                    label,
                    lat: sum_lat / n as f64,
                    lon: sum_lon / n as f64,
                    street,
                    house_number: String::new(),
                    postcode,
                    municipality,
                }
            })
            .collect()
    }

    /// Building-level hit (number token, no box): `"<Street> <house>, <pc> <muni>"`.
    fn hit_building(&self, rid: u32) -> AddressHit {
        let r = &self.records[rid as usize];
        let street = self.streets[r.street as usize].display.clone();
        let municipality = self.municipalities[r.municipality as usize].display.clone();
        let postcode = self.postals[r.postal as usize].clone();
        let label = format!("{street} {}, {postcode} {municipality}", r.house_number);
        AddressHit {
            id: r.id.clone(),
            label,
            lat: r.lat,
            lon: r.lon,
            street,
            house_number: r.house_number.clone(),
            postcode,
            municipality,
        }
    }

    /// Box-level hit: select the building's box whose label matches `box_token`
    /// (exact, else prefix); label `"<Street> <house> bus <box>, <pc> <muni>"` with
    /// the box's own coordinate. Falls back to the building when no box matches.
    fn hit_box(&self, rid: u32, box_token: &str) -> AddressHit {
        let r = &self.records[rid as usize];
        let exact = r.boxes.iter().find(|b| box_label_eq(&b.label, box_token));
        let chosen = exact.or_else(|| {
            r.boxes
                .iter()
                .find(|b| normalize(&b.label).starts_with(box_token))
        });
        let Some(b) = chosen else {
            return self.hit_building(rid);
        };
        let street = self.streets[r.street as usize].display.clone();
        let municipality = self.municipalities[r.municipality as usize].display.clone();
        let postcode = self.postals[r.postal as usize].clone();
        let label = format!(
            "{street} {} bus {}, {postcode} {municipality}",
            r.house_number, b.label
        );
        AddressHit {
            id: r.id.clone(),
            label,
            lat: b.lat,
            lon: b.lon,
            street,
            house_number: r.house_number.clone(),
            postcode,
            municipality,
        }
    }
}

/// One building accumulated during ingestion before it is finalized into an
/// [`AddressRecord`]: all raw-row coordinates (for the representative centroid and
/// the divergence test) plus each box's label and own coordinate.
struct PendingBuilding {
    id: String,
    street: u32,
    municipality: u32,
    postal: u32,
    house_number: String,
    coords: Vec<(f64, f64)>,
    boxes: Vec<(String, f64, f64)>,
}

/// Default representative-coordinate divergence epsilon (meters). Box coordinates
/// within this distance of the building centroid are collapsed to it; beyond it
/// each box keeps its own coordinate. Overridden from `config.yaml`
/// (`default_routing.address_box_coord_epsilon_m`) at build time.
pub const DEFAULT_BOX_COORD_EPSILON_M: f64 = 5.0;

/// Builder accumulating interned entities and records during ingestion, then
/// producing a ready-to-query [`AddressIndex`]. Raw address rows are aggregated by
/// `(street, house_number)` so the apartment/box rows BeST stores as separate
/// addresses collapse into one building-level record. The municipality is folded
/// into the key as well — a street id implies its municipality in the real feed, so
/// this is a no-op there, but it keeps a building from merging across municipalities
/// when test fixtures reuse one street id.
pub struct AddressIndexBuilder {
    streets: Vec<Named>,
    municipalities: Vec<Named>,
    postals: Vec<String>,
    street_ids: HashMap<String, u32>,
    muni_ids: HashMap<String, u32>,
    postal_ids: HashMap<String, u32>,
    buildings: Vec<PendingBuilding>,
    building_ids: HashMap<(u32, u32, String), usize>,
    box_coord_epsilon_m: f64,
}

impl Default for AddressIndexBuilder {
    fn default() -> Self {
        AddressIndexBuilder {
            streets: Vec::new(),
            municipalities: Vec::new(),
            postals: Vec::new(),
            street_ids: HashMap::new(),
            muni_ids: HashMap::new(),
            postal_ids: HashMap::new(),
            buildings: Vec::new(),
            building_ids: HashMap::new(),
            box_coord_epsilon_m: DEFAULT_BOX_COORD_EPSILON_M,
        }
    }
}

impl AddressIndexBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the representative-coordinate divergence epsilon (meters).
    pub fn set_box_coord_epsilon_m(&mut self, m: f64) {
        self.box_coord_epsilon_m = m;
    }

    pub fn intern_street(&mut self, key: &str, named: Named) -> u32 {
        if let Some(&id) = self.street_ids.get(key) {
            return id;
        }
        let id = self.streets.len() as u32;
        self.streets.push(named);
        self.street_ids.insert(key.to_string(), id);
        id
    }

    pub fn intern_municipality(&mut self, key: &str, named: Named) -> u32 {
        if let Some(&id) = self.muni_ids.get(key) {
            return id;
        }
        let id = self.municipalities.len() as u32;
        self.municipalities.push(named);
        self.muni_ids.insert(key.to_string(), id);
        id
    }

    pub fn intern_postal(&mut self, key: &str, code: String) -> u32 {
        if let Some(&id) = self.postal_ids.get(key) {
            return id;
        }
        let id = self.postals.len() as u32;
        self.postals.push(code);
        self.postal_ids.insert(key.to_string(), id);
        id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_record(
        &mut self,
        id: String,
        street: u32,
        municipality: u32,
        postal: u32,
        house_number: String,
        box_number: String,
        lat: f64,
        lon: f64,
    ) {
        let key = (street, municipality, house_number.clone());
        match self.building_ids.get(&key) {
            Some(&i) => {
                let b = &mut self.buildings[i];
                b.coords.push((lat, lon));
                if !box_number.is_empty() {
                    b.boxes.push((box_number, lat, lon));
                }
            }
            None => {
                let boxes = if box_number.is_empty() {
                    Vec::new()
                } else {
                    vec![(box_number, lat, lon)]
                };
                self.building_ids.insert(key, self.buildings.len());
                self.buildings.push(PendingBuilding {
                    id,
                    street,
                    municipality,
                    postal,
                    house_number,
                    coords: vec![(lat, lon)],
                    boxes,
                });
            }
        }
    }

    /// Finalize each pending building: representative coordinate = centroid of its
    /// raw-row coordinates; if every coordinate is within `box_coord_epsilon_m` of
    /// that centroid the box coordinates collapse to it, otherwise each box keeps
    /// its own coordinate (the box labels are always retained).
    pub fn finish(self) -> AddressIndex {
        let eps = self.box_coord_epsilon_m;
        let records = self
            .buildings
            .into_iter()
            .map(|b| {
                let n = b.coords.len() as f64;
                let lat = b.coords.iter().map(|c| c.0).sum::<f64>() / n;
                let lon = b.coords.iter().map(|c| c.1).sum::<f64>() / n;
                let convergent = b
                    .coords
                    .iter()
                    .all(|c| LatLng::distance(&[lat, lon], &[c.0, c.1]) <= eps);
                let boxes = b
                    .boxes
                    .into_iter()
                    .map(|(label, blat, blon)| {
                        if convergent {
                            AddressBox { label, lat, lon }
                        } else {
                            AddressBox {
                                label,
                                lat: blat,
                                lon: blon,
                            }
                        }
                    })
                    .collect();
                AddressRecord {
                    id: b.id,
                    street: b.street,
                    municipality: b.municipality,
                    postal: b.postal,
                    house_number: b.house_number,
                    lat,
                    lon,
                    boxes,
                }
            })
            .collect();
        let mut idx = AddressIndex {
            streets: self.streets,
            municipalities: self.municipalities,
            postals: self.postals,
            records,
            ..Default::default()
        };
        idx.rebuild_indexes();
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(display: &str, aliases: &[&str]) -> Named {
        Named {
            display: display.to_string(),
            aliases: aliases.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn sample() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi", "Wetstraat"]));
        let other = b.intern_street("S2", named("Avenue Louise", &["Avenue Louise", "Louizalaan"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles", "Brussel"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record(
            "A1".into(), loi, bxl, pc, "16".into(), String::new(), 50.846, 4.367,
        );
        b.push_record(
            "A2".into(), loi, bxl, pc, "200".into(), String::new(), 50.848, 4.378,
        );
        b.push_record(
            "A3".into(), other, bxl, pc, "10".into(), String::new(), 50.838, 4.362,
        );
        b.finish()
    }

    #[test]
    fn normalize_strips_accents_and_punctuation() {
        assert_eq!(normalize("  Rue de l'Église, "), "rue de l eglise");
        assert_eq!(normalize("Wetstraat"), "wetstraat");
    }

    #[test]
    fn prefix_search_returns_expected_address() {
        let idx = sample();
        let hits = idx.search("rue de la loi 16", 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "A1");
        assert_eq!(hits[0].label, "Rue de la Loi 16, 1000 Bruxelles");
        assert!((hits[0].lat - 50.846).abs() < 1e-9);
    }

    #[test]
    fn multilingual_alias_finds_same_record() {
        let idx = sample();
        let fr = idx.search("rue de la loi 16", 10, None);
        let nl = idx.search("wetstraat 16", 10, None);
        assert_eq!(fr.len(), 1);
        assert_eq!(nl.len(), 1);
        assert_eq!(fr[0].id, nl[0].id);
    }

    #[test]
    fn prefix_matches_partial_token_collapses_to_one_street() {
        let idx = sample();
        let hits = idx.search("wet", 10, None);
        assert_eq!(hits.len(), 1, "no number token ⇒ one street-level hit");
        assert_eq!(hits[0].street, "Rue de la Loi");
        assert_eq!(hits[0].label, "Rue de la Loi, 1000 Bruxelles");
        assert!(hits[0].house_number.is_empty());
    }

    #[test]
    fn municipality_token_narrows_match() {
        let idx = sample();
        let hits = idx.search("louise bruxelles", 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "A3");
    }

    #[test]
    fn non_match_returns_empty() {
        let idx = sample();
        assert!(idx.search("nonexistentstreetxyz", 10, None).is_empty());
        assert!(idx.search("12345", 10, None).is_empty());
        assert!(idx.search("", 10, None).is_empty());
    }

    #[test]
    fn limit_is_respected() {
        let idx = house_number_fixture();
        let unlimited = idx.search("rue de la loi 1", 10, None);
        assert_eq!(unlimited.len(), 2, "16 and 169 both prefix-match the number 1");
        let hits = idx.search("rue de la loi 1", 1, None);
        assert_eq!(hits.len(), 1, "the limit truncates the two building hits to one");
    }

    /// "Rue de la Loi 16" exists in both Brussels and Liège; ids B1/L1 let the
    /// proximity and geo-floor tests assert ordering by focus point.
    fn two_munis() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi", "Wetstraat"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles", "Brussel"]));
        let lie = b.intern_municipality("M2", named("Liège", &["Liège", "Luik"]));
        let pcb = b.intern_postal("P1", "1000".to_string());
        let pcl = b.intern_postal("P2", "4000".to_string());
        b.push_record("B1".into(), loi, bxl, pcb, "16".into(), String::new(), 50.846, 4.367);
        b.push_record("L1".into(), loi, lie, pcl, "16".into(), String::new(), 50.610, 5.500);
        b.finish()
    }

    #[test]
    fn proximity_ranks_nearest_focus_first() {
        let idx = two_munis();
        let near_bxl = idx.search("rue de la loi 16", 5, Some((50.846, 4.367)));
        assert_eq!(near_bxl[0].id, "B1");
        let near_lie = idx.search("rue de la loi 16", 5, Some((50.610, 5.500)));
        assert_eq!(near_lie[0].id, "L1");
    }

    #[test]
    fn geo_floor_keeps_far_exact_match() {
        let idx = two_munis();
        let hits = idx.search("rue de la loi 16", 5, Some((50.846, 4.367)));
        assert_eq!(hits.len(), 2, "the far Liège match is still returned, just lower");
        assert_eq!(hits[1].id, "L1");
    }

    #[test]
    fn no_focus_is_deterministic_text_ranking() {
        let idx = two_munis();
        let a = idx.search("rue de la loi 16", 5, None);
        let b = idx.search("rue de la loi 16", 5, None);
        assert_eq!(a.len(), 2);
        let ids_a: Vec<_> = a.iter().map(|h| h.id.clone()).collect();
        let ids_b: Vec<_> = b.iter().map(|h| h.id.clone()).collect();
        assert_eq!(ids_a, ids_b);
        assert_eq!(ids_a, vec!["B1", "L1"], "tie-break by record id is stable");
    }

    /// Two records on the same street at identical coords (equal geo_decay), one
    /// with the exact house number, one whose number only prefix-matches: the
    /// exact-number boost must rank the exact one first.
    fn house_number_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("H169".into(), loi, bxl, pc, "169".into(), String::new(), 50.846, 4.367);
        b.push_record("H16".into(), loi, bxl, pc, "16".into(), String::new(), 50.846, 4.367);
        b.finish()
    }

    #[test]
    fn exact_house_number_boost_ranks_first() {
        let idx = house_number_fixture();
        let hits = idx.search("rue de la loi 16", 5, None);
        assert_eq!(hits.len(), 2, "both 16 and 169 prefix-match the number token");
        assert_eq!(hits[0].id, "H16", "exact house number outranks prefix match");
    }

    /// A typo of "wetstraat" ("wetstrat", one deletion) is NOT a prefix of any
    /// alias, so it only resolves once the fuzzy fallback fires.
    #[test]
    fn typo_resolves_via_fuzzy() {
        let idx = sample();
        let hits = idx.search("wetstrat", 10, None);
        assert!(!hits.is_empty(), "the deletion typo must resolve via fuzzy");
        assert!(hits.iter().all(|h| h.street == "Rue de la Loi"));
    }

    /// With the fuzzy fallback disabled (trigger_k = 0 ⇒ never fire), the same
    /// deletion typo finds nothing, proving the typo resolves *only* via fuzzy.
    #[test]
    fn typo_needs_fuzzy_to_resolve() {
        let mut idx = sample();
        idx.set_search_params(AddressSearchParams {
            fuzzy_trigger_k: 0,
            ..AddressSearchParams::default()
        });
        assert!(idx.search("wetstrat", 10, None).is_empty());
    }

    /// A street whose single token is 17 chars long; a 2-edit typo of it resolves
    /// because the ≥8-char gate allows 2 edits.
    fn long_token_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let s = b.intern_street(
            "S1",
            named("Wetenschapsstraat", &["Wetenschapsstraat"]),
        );
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("L1".into(), s, bxl, pc, "10".into(), String::new(), 50.84, 4.36);
        b.finish()
    }

    #[test]
    fn two_edit_typo_resolves_for_long_token() {
        let idx = long_token_fixture();
        let hits = idx.search("wetenscapstraat", 10, None);
        assert!(
            !hits.is_empty(),
            "a 2-edit typo of a 17-char token resolves under the ≥8 gate"
        );
        assert!(hits.iter().all(|h| h.street == "Wetenschapsstraat"));
    }

    /// Five distinct "park*" streets share the prefix "park"; a clean "park" query
    /// covers all five (≥ trigger_k = 5) so the fuzzy fallback never fires — the
    /// 1-edit neighbour "parc" is therefore NOT pulled in. Raising trigger_k forces
    /// fuzzy on, which then does include "parc", proving the prefix-first gate.
    fn prefix_first_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        for (i, name) in [
            "Parkstraat",
            "Parklaan",
            "Parkweg",
            "Parkplein",
            "Parkdreef",
            "Parc",
        ]
        .iter()
        .enumerate()
        {
            let s = b.intern_street(&format!("S{i}"), named(name, &[name]));
            b.push_record(
                format!("R{i}"),
                s,
                bxl,
                pc,
                "1".into(),
                String::new(),
                50.84,
                4.36,
            );
        }
        b.finish()
    }

    #[test]
    fn prefix_first_does_not_overtrigger_fuzzy() {
        let idx = prefix_first_fixture();
        let hits = idx.search("park", 20, None);
        assert_eq!(hits.len(), 5, "only the five park* prefix matches");
        assert!(
            hits.iter().all(|h| h.street != "Parc"),
            "fuzzy must not fire when ≥ trigger_k streets already matched"
        );
    }

    #[test]
    fn raising_trigger_k_lets_fuzzy_pull_neighbour() {
        let mut idx = prefix_first_fixture();
        idx.set_search_params(AddressSearchParams {
            fuzzy_trigger_k: 10,
            ..AddressSearchParams::default()
        });
        let hits = idx.search("park", 20, None);
        assert!(
            hits.iter().any(|h| h.street == "Parc"),
            "with fuzzy forced on, the 1-edit neighbour is included"
        );
    }

    /// Tokens of length 2, 5, 7 and 8 on distinct streets with distinct first
    /// letters, to probe the length gate independently of prefix matching.
    fn length_gate_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        for (i, name) in ["ka", "blauw", "geelweg", "groenweg"].iter().enumerate() {
            let s = b.intern_street(&format!("S{i}"), named(name, &[name]));
            b.push_record(
                format!("R{i}"),
                s,
                bxl,
                pc,
                "1".into(),
                String::new(),
                50.84,
                4.36,
            );
        }
        b.finish()
    }

    #[test]
    fn length_gate_blocks_short_token() {
        let idx = length_gate_fixture();
        assert!(
            idx.search("ko", 10, None).is_empty(),
            "a 2-char token gets 0 edits — no fuzzy"
        );
    }

    #[test]
    fn length_gate_allows_one_edit_for_mid_token() {
        let idx = length_gate_fixture();
        let hits = idx.search("blouw", 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].street, "blauw", "5-char token allows 1 edit");
    }

    #[test]
    fn length_gate_caps_seven_char_token_at_one_edit() {
        let idx = length_gate_fixture();
        let hits = idx.search("gaalweg", 10, None);
        assert!(
            hits.iter().all(|h| h.street != "geelweg"),
            "a 2-edit typo of a 7-char token (gate = 1) must not resolve"
        );
    }

    #[test]
    fn length_gate_allows_two_edits_for_eight_char_token() {
        let idx = length_gate_fixture();
        let hits = idx.search("graenwag", 10, None);
        assert!(
            hits.iter().any(|h| h.street == "groenweg"),
            "an 8-char token allows 2 edits"
        );
    }

    #[test]
    fn first_char_typo_is_not_corrected() {
        let idx = sample();
        assert!(
            idx.search("aetstraat", 10, None).is_empty(),
            "prefix_length=1: a mistyped first character is never corrected"
        );
    }

    #[test]
    fn numbers_are_never_fuzzed() {
        let idx = sample();
        assert!(
            idx.search("wetstraat 15", 10, None).is_empty(),
            "house number 15 does not match a record with 16"
        );
        assert!(
            idx.search("wetstraat 9999", 10, None).is_empty(),
            "a wrong postcode token does not match"
        );
    }

    /// Two streets: "Wetstrat" (exact-matched by the query) and "Wetstraat"
    /// (fuzzy-matched, 1 edit), at identical coords. The exact match must rank
    /// above the fuzzy-only one (fuzzy_token_weight < prefix weight).
    fn ranking_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let a = b.intern_street("S1", named("Wetstrat", &["Wetstrat"]));
        let z = b.intern_street("S2", named("Wetstraat", &["Wetstraat"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("EX".into(), a, bxl, pc, "1".into(), String::new(), 50.84, 4.36);
        b.push_record("FZ".into(), z, bxl, pc, "1".into(), String::new(), 50.84, 4.36);
        b.finish()
    }

    #[test]
    fn exact_match_outranks_fuzzy_match() {
        let idx = ranking_fixture();
        let hits = idx.search("wetstrat", 10, None);
        assert_eq!(hits.len(), 2, "exact street and fuzzy neighbour both returned");
        assert_eq!(hits[0].id, "EX", "the exact token match ranks first");
        assert_eq!(hits[1].id, "FZ", "the fuzzy-only match ranks below it");
    }

    /// One building (Rue de la Loi 16) whose three apartment rows (bus 1/2/3) sit a
    /// metre or so apart — distinct coordinates, all within the 5 m epsilon — so the
    /// collapse must actively rewrite each box coordinate to the centroid.
    fn convergent_boxes() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi", "Wetstraat"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("A16-1".into(), loi, bxl, pc, "16".into(), "1".into(), 50.84600, 4.36700);
        b.push_record("A16-2".into(), loi, bxl, pc, "16".into(), "2".into(), 50.84601, 4.36701);
        b.push_record("A16-3".into(), loi, bxl, pc, "16".into(), "3".into(), 50.84602, 4.36702);
        b.finish()
    }

    /// One building (Rue de la Loi 16) whose three boxes sit > epsilon apart, so
    /// each box keeps its own coordinate and the building coordinate is the centroid.
    fn divergent_boxes() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi", "Wetstraat"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("D16-1".into(), loi, bxl, pc, "16".into(), "1".into(), 50.8460, 4.3670);
        b.push_record("D16-2".into(), loi, bxl, pc, "16".into(), "2".into(), 50.8470, 4.3680);
        b.push_record("D16-3".into(), loi, bxl, pc, "16".into(), "3".into(), 50.8480, 4.3690);
        b.finish()
    }

    #[test]
    fn apartments_collapse_to_one_building() {
        let idx = convergent_boxes();
        assert_eq!(idx.record_count(), 1, "three apartment rows ⇒ one building record");
        assert_eq!(idx.records[0].boxes.len(), 3, "all three boxes kept as metadata");

        let hits = idx.search("rue de la loi 16", 10, None);
        assert_eq!(hits.len(), 1, "one building hit, not one per apartment");
        assert_eq!(hits[0].label, "Rue de la Loi 16, 1000 Bruxelles");
        assert!(!hits[0].label.contains("bus"), "no box surfaced without a box token");
    }

    /// A street with house numbers 16/100/200 in Bruxelles plus the same street in
    /// Liège, to prove the no-number collapse and its per-municipality split.
    fn street_houses() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi", "Wetstraat"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let lie = b.intern_municipality("M2", named("Liège", &["Liège"]));
        let pcb = b.intern_postal("P1", "1000".to_string());
        let pcl = b.intern_postal("P2", "4000".to_string());
        b.push_record("B16".into(), loi, bxl, pcb, "16".into(), String::new(), 50.846, 4.367);
        b.push_record("B100".into(), loi, bxl, pcb, "100".into(), String::new(), 50.847, 4.368);
        b.push_record("B200".into(), loi, bxl, pcb, "200".into(), String::new(), 50.848, 4.369);
        b.push_record("L16".into(), loi, lie, pcl, "16".into(), String::new(), 50.610, 5.500);
        b.finish()
    }

    #[test]
    fn no_number_collapses_street_per_municipality() {
        let idx = street_houses();
        let hits = idx.search("rue de la loi", 10, None);
        assert_eq!(hits.len(), 2, "one street-level hit per municipality");
        let bxl = hits.iter().find(|h| h.municipality == "Bruxelles").unwrap();
        assert_eq!(bxl.label, "Rue de la Loi, 1000 Bruxelles");
        assert!(bxl.house_number.is_empty(), "street-level carries no house number");
        let centroid_lat = (50.846 + 50.847 + 50.848) / 3.0;
        assert!(
            (bxl.lat - centroid_lat).abs() < 1e-9,
            "coordinate is the centroid of the matched buildings"
        );
        assert!(hits.iter().any(|h| h.municipality == "Liège"));
    }

    #[test]
    fn distinct_house_numbers_appear_separately() {
        let idx = house_number_fixture();
        let hits = idx.search("rue de la loi 1", 10, None);
        assert_eq!(hits.len(), 2, "16 and 169 are distinct buildings");
        let ids: HashSet<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        assert!(ids.contains("H16") && ids.contains("H169"));
        assert!(hits.iter().all(|h| !h.house_number.is_empty()));
    }

    #[test]
    fn box_token_selects_matching_box() {
        let idx = divergent_boxes();
        for q in [
            "rue de la loi 16 bus 3",
            "rue de la loi 16 b3",
            "rue de la loi 16 /3",
        ] {
            let hits = idx.search(q, 10, None);
            assert_eq!(hits.len(), 1, "query {q:?}");
            assert_eq!(
                hits[0].label, "Rue de la Loi 16 bus 3, 1000 Bruxelles",
                "query {q:?}"
            );
            assert!((hits[0].lat - 50.8480).abs() < 1e-9, "box 3 own coord for {q:?}");
            assert!((hits[0].lon - 4.3690).abs() < 1e-9, "box 3 own coord for {q:?}");
        }
    }

    #[test]
    fn unmatched_box_token_falls_back_to_building() {
        let idx = divergent_boxes();
        let hits = idx.search("rue de la loi 16 bus 9", 10, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].label, "Rue de la Loi 16, 1000 Bruxelles",
            "no box 9 ⇒ building-level fallback"
        );
    }

    #[test]
    fn divergent_boxes_keep_own_coords_building_is_centroid() {
        let idx = divergent_boxes();
        let r = &idx.records[0];
        let centroid_lat = (50.8460 + 50.8470 + 50.8480) / 3.0;
        assert!((r.lat - centroid_lat).abs() < 1e-9, "building coord = centroid");
        let b3 = r.boxes.iter().find(|b| b.label == "3").unwrap();
        assert!((b3.lat - 50.8480).abs() < 1e-9, "divergent box keeps its own coord");
    }

    #[test]
    fn convergent_boxes_collapse_coords_to_building() {
        let idx = convergent_boxes();
        let r = &idx.records[0];
        assert!(
            r.boxes
                .iter()
                .all(|b| (b.lat - r.lat).abs() < 1e-12 && (b.lon - r.lon).abs() < 1e-12),
            "within-epsilon boxes collapse to the building coordinate"
        );
    }

    #[test]
    fn proximity_preserved_at_building_granularity() {
        let idx = two_munis();
        let near_bxl = idx.search("rue de la loi 16", 5, Some((50.846, 4.367)));
        assert_eq!(near_bxl[0].id, "B1", "nearest building ranks first");
        let near_lie = idx.search("rue de la loi 16", 5, Some((50.610, 5.500)));
        assert_eq!(near_lie[0].id, "L1");
    }

    #[test]
    fn fuzzy_preserved_at_building_granularity() {
        let idx = sample();
        let hits = idx.search("wetstrat 16", 10, None);
        assert!(!hits.is_empty(), "the deletion typo resolves with a number token");
        assert_eq!(hits[0].street, "Rue de la Loi");
        assert_eq!(hits[0].house_number, "16", "building-level hit, not street-level");
    }

    /// Two "Parc *" streets the query "parc" matches; one has a building right at
    /// the focus plus a far one, the other a single mid-distance building. With the
    /// group score = MAX of members, the street holding the focus-adjacent building
    /// must rank first — even though its mean (near + far) would lose to the other.
    fn street_group_proximity() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let royal = b.intern_street("S1", named("Parc Royal", &["Parc Royal"]));
        let sud = b.intern_street("S2", named("Parc Sud", &["Parc Sud"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("R-near".into(), royal, bxl, pc, "1".into(), String::new(), 50.846, 4.367);
        b.push_record("R-far".into(), royal, bxl, pc, "2".into(), String::new(), 51.500, 6.000);
        b.push_record("S-mid".into(), sud, bxl, pc, "1".into(), String::new(), 50.860, 4.400);
        b.finish()
    }

    #[test]
    fn street_group_ranks_by_max_member_proximity() {
        let idx = street_group_proximity();
        let hits = idx.search("parc", 10, Some((50.846, 4.367)));
        assert_eq!(hits.len(), 2, "two streets, one street-level hit each");
        assert_eq!(
            hits[0].street, "Parc Royal",
            "the street with the focus-adjacent building ranks first (group score = MAX)"
        );
        assert_eq!(hits[1].street, "Parc Sud");
    }

    #[test]
    fn box_keyword_does_not_swallow_street_token() {
        let idx = sample();
        let hits = idx.search("avenue louise", 10, None);
        assert_eq!(hits.len(), 1, "'louise' is not parsed as a box reference");
        assert_eq!(hits[0].street, "Avenue Louise");
    }

    /// Streets whose names begin with a box keyword (`bus`, `boite`, `bte`) or
    /// contain the keyword as a real word, plus a disambiguating sibling street, so
    /// the box parser must NOT eat the legitimate street/house tokens.
    fn eaten_token_fixture() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let bxl = b.intern_municipality("M1", named("Namur", &["Namur"]));
        let pc = b.intern_postal("P1", "5000".to_string());
        for (i, name) in ["Buslaan", "Busstraat", "Boiteux", "Bteinstein"]
            .iter()
            .enumerate()
        {
            let s = b.intern_street(&format!("W{i}"), named(name, &[name]));
            b.push_record(format!("W{i}"), s, bxl, pc, "1".into(), String::new(), 50.84, 4.36);
        }
        let boite = b.intern_street("SB", named("Rue de la Boite", &["Rue de la Boite"]));
        let loi = b.intern_street("SL", named("Rue de la Loi", &["Rue de la Loi"]));
        b.push_record("BOITE5".into(), boite, bxl, pc, "5".into(), String::new(), 50.84, 4.36);
        b.push_record("LOI5".into(), loi, bxl, pc, "5".into(), String::new(), 50.84, 4.36);
        let avb = b.intern_street("SAB", named("Avenue B", &["Avenue B"]));
        let ans = b.intern_street("SAN", named("Avenue Anspach", &["Avenue Anspach"]));
        b.push_record("AVB12".into(), avb, bxl, pc, "12".into(), String::new(), 50.84, 4.36);
        b.push_record("ANS12".into(), ans, bxl, pc, "12".into(), String::new(), 50.84, 4.36);
        b.finish()
    }

    #[test]
    fn box_parser_does_not_eat_keyword_prefixed_street() {
        let idx = eaten_token_fixture();
        for (q, street) in [
            ("buslaan", "Buslaan"),
            ("busstraat", "Busstraat"),
            ("boiteux", "Boiteux"),
            ("bteinstein", "Bteinstein"),
        ] {
            let hits = idx.search(q, 10, None);
            assert!(!hits.is_empty(), "query {q:?} must resolve to the street, not be eaten");
            assert_eq!(hits[0].street, street, "query {q:?}");
        }
    }

    #[test]
    fn box_parser_does_not_eat_street_word_before_number() {
        let idx = eaten_token_fixture();
        let hits = idx.search("rue de la boite 5", 10, None);
        assert_eq!(hits.len(), 1, "'boite' precedes the number ⇒ it is a street word");
        assert_eq!(hits[0].street, "Rue de la Boite");
        assert_eq!(hits[0].house_number, "5");
    }

    #[test]
    fn box_parser_does_not_eat_standalone_b_before_number() {
        let idx = eaten_token_fixture();
        let hits = idx.search("avenue b 12", 10, None);
        assert_eq!(hits.len(), 1, "'b' precedes the only number ⇒ it is a street word");
        assert_eq!(hits[0].street, "Avenue B");
        assert_eq!(hits[0].house_number, "12");
    }

    /// A building (Rue de la Loi 16) with a single zero-padded box label `0003`, to
    /// prove the box select matches the user-typed `bus 3` numeric-insensitively.
    fn zero_pad_box() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("Z16".into(), loi, bxl, pc, "16".into(), "0003".into(), 50.846, 4.367);
        b.finish()
    }

    #[test]
    fn box_select_is_leading_zero_insensitive() {
        let idx = zero_pad_box();
        for q in ["rue de la loi 16 bus 3", "rue de la loi 16 bus 0003"] {
            let hits = idx.search(q, 10, None);
            assert_eq!(hits.len(), 1, "query {q:?}");
            assert_eq!(
                hits[0].label, "Rue de la Loi 16 bus 0003, 1000 Bruxelles",
                "query {q:?} selects the zero-padded box"
            );
        }
    }

    /// A building with a pure-letter box label `A` — a legitimate single-letter box
    /// the shape gate must still accept.
    fn letter_box() -> AddressIndex {
        let mut b = AddressIndexBuilder::new();
        let loi = b.intern_street("S1", named("Rue de la Loi", &["Rue de la Loi"]));
        let bxl = b.intern_municipality("M1", named("Bruxelles", &["Bruxelles"]));
        let pc = b.intern_postal("P1", "1000".to_string());
        b.push_record("LA16".into(), loi, bxl, pc, "16".into(), "A".into(), 50.846, 4.367);
        b.finish()
    }

    #[test]
    fn letter_box_still_selected() {
        let idx = letter_box();
        for q in ["rue de la loi 16 bus A", "rue de la loi 16 bus a"] {
            let hits = idx.search(q, 10, None);
            assert_eq!(hits.len(), 1, "query {q:?}");
            assert_eq!(
                hits[0].label, "Rue de la Loi 16 bus A, 1000 Bruxelles",
                "query {q:?} selects the letter box"
            );
        }
    }
}
