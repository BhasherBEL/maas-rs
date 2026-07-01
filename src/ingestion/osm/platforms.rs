use std::collections::{HashMap, HashSet};

use kdtree::KdTree;
use osmpbf::{Element, ElementReader, RelMemberType};
use serde::{Deserialize, Serialize};

use crate::structures::{Connector, Graph, LatLng, NodeID};

/// Search radius (metres) around a GTFS station centroid when looking for the
/// matching OSM platform. Generous on purpose: SNCB platform stops collapse to
/// the station centroid, so the real platform geometry can sit a few hundred
/// metres away. Build-time matcher knob (the offset distribution reveals
/// clipping if it is too small).
pub const PLATFORM_MATCH_RADIUS_M: f64 = 400.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OsmPlatform {
    pub refs: Vec<String>,
    pub level: Option<f32>,
    pub centroid: LatLng,
    /// Graph node IDs of this platform way's polyline (Stage B1). Populated when
    /// the platform way is imported as routable foot edges; the connector-coverage
    /// measurement BFS starts here. Empty for platforms whose nodes never made it
    /// into the graph (old caches / synthetic test indices).
    #[serde(default)]
    pub node_ids: Vec<NodeID>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlatformIndex {
    platforms: Vec<OsmPlatform>,
    tree: KdTree<f64, usize, [f64; 2]>,
}

impl Default for PlatformIndex {
    fn default() -> Self {
        PlatformIndex {
            platforms: Vec::new(),
            tree: KdTree::new(2),
        }
    }
}

pub struct StopPlatformQuery<'a> {
    pub platform_code: Option<&'a str>,
    pub level_id: Option<&'a str>,
    pub station_centroid: LatLng,
    pub search_radius_m: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlatformMatch {
    ByNumber { platform: usize, dist_m: f64 },
    GeoNearest { platform: usize, dist_m: f64 },
    None,
}

impl PlatformIndex {
    pub fn from_platforms(platforms: Vec<OsmPlatform>) -> Self {
        let mut tree = KdTree::new(2);
        for (i, p) in platforms.iter().enumerate() {
            let _ = tree.add([p.centroid.latitude, p.centroid.longitude], i);
        }
        PlatformIndex { platforms, tree }
    }

    pub fn len(&self) -> usize {
        self.platforms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.platforms.is_empty()
    }

    pub fn platform(&self, i: usize) -> Option<&OsmPlatform> {
        self.platforms.get(i)
    }

    fn candidates(&self, c: LatLng, radius_m: f64) -> Vec<(usize, f64)> {
        let query = [c.latitude, c.longitude];
        let it = match self.tree.iter_nearest(&query, &LatLng::distance) {
            Ok(it) => it,
            Err(_) => return Vec::new(),
        };
        it.take_while(|(d, _)| *d <= radius_m)
            .map(|(d, &i)| (i, d))
            .collect()
    }

    /// Match a GTFS platform stop to an OSM platform. Prefers an OSM platform
    /// whose split `ref`/`local_ref` set contains the GTFS `platform_code`
    /// (resolves N:1 island platforms); falls back to the geo-nearest platform
    /// in range; returns `None` when the code is empty/absent or nothing is in
    /// range. `dist_m` is the centroid→platform offset in metres.
    pub fn match_platform(&self, q: &StopPlatformQuery) -> PlatformMatch {
        let code = match q.platform_code.map(|s| s.trim()).filter(|s| !s.is_empty()) {
            Some(c) => c,
            None => return PlatformMatch::None,
        };
        let cands = self.candidates(q.station_centroid, q.search_radius_m);
        if cands.is_empty() {
            return PlatformMatch::None;
        }
        for (i, d) in &cands {
            if self.platforms[*i].refs.iter().any(|r| r == code) {
                return PlatformMatch::ByNumber {
                    platform: *i,
                    dist_m: *d,
                };
            }
        }
        let (i, d) = cands[0];
        PlatformMatch::GeoNearest {
            platform: i,
            dist_m: d,
        }
    }
}

/// Returns `true` when tags describe a rail platform — safe to index for
/// rail-stop matching. Excludes bus/tram-only platforms so a TEC bus terminal
/// tagged `highway=bus_stop, public_transport=platform` cannot shadow a SNCB
/// rail platform with the same `local_ref`.
///
/// Include: `railway=platform` present, OR `train=yes`, OR
///   `public_transport=platform` with no bus/tram-only signal.
/// Exclude: `highway=bus_stop`, OR (`bus=yes`/`tram=yes`/`trolleybus=yes`
///   AND NOT `train=yes`/`railway=platform`).
fn is_rail_platform(tags: &[(&str, &str)]) -> bool {
    let has_railway_platform = tags.iter().any(|(k, v)| *k == "railway" && *v == "platform");
    let has_train_yes = tags.iter().any(|(k, v)| *k == "train" && *v == "yes");
    if has_railway_platform || has_train_yes {
        return true;
    }
    !tags.iter().any(|(k, v)| {
        (*k == "highway" && *v == "bus_stop")
            || (*k == "bus" && *v == "yes")
            || (*k == "tram" && *v == "yes")
            || (*k == "trolleybus" && *v == "yes")
    })
}

/// Parse a way's tags into `(refs, level)` when it is a platform way
/// (`railway=platform` or `public_transport=platform`). `refs` is the union of
/// `ref` and `local_ref`, each split on `;` (island platforms). Returns `None`
/// for non-platform ways and for bus/tram-only platforms (not rail).
pub fn parse_platform_tags(tags: &[(&str, &str)]) -> Option<(Vec<String>, Option<f32>)> {
    let is_platform = tags.iter().any(|(k, v)| {
        (*k == "railway" && *v == "platform") || (*k == "public_transport" && *v == "platform")
    });
    if !is_platform {
        return None;
    }
    if !is_rail_platform(tags) {
        return None;
    }

    let mut refs: Vec<String> = Vec::new();
    for key in ["ref", "local_ref"] {
        if let Some((_, v)) = tags.iter().find(|(k, _)| *k == key) {
            for part in v.split(';') {
                let t = part.trim();
                if !t.is_empty() && !refs.iter().any(|r| r == t) {
                    refs.push(t.to_string());
                }
            }
        }
    }

    let level = tags
        .iter()
        .find(|(k, _)| *k == "level")
        .and_then(|(_, v)| parse_level(v));

    Some((refs, level))
}

fn parse_level(v: &str) -> Option<f32> {
    v.split(';').next()?.trim().parse::<f32>().ok()
}

/// True when a way is a railway / public-transport platform (the ways Stage B1
/// imports as walkable foot edges; `validate_way` rejects them since they carry
/// no `highway`).
pub fn is_platform_way(tags: &[(&str, &str)]) -> bool {
    tags.iter().any(|(k, v)| {
        (*k == "railway" && *v == "platform") || (*k == "public_transport" && *v == "platform")
    })
}

/// Parse a way's `level` tag to a whole storey (`level=1` → `Some(1)`). Takes the
/// first component of a range (`0;1` → `0`); unparseable (`mezzanine`) → `None`.
/// Rounds to the nearest integer storey (half levels collapse).
pub fn parse_way_level(tags: &[(&str, &str)]) -> Option<i16> {
    let (_, v) = tags.iter().find(|(k, _)| *k == "level")?;
    let f = parse_level(v)?;
    Some(f.round() as i16)
}

/// Resolve the effective `highway` value for a way's tag slice.
///
/// Prefers the explicit `highway` tag. When `highway` is absent, falls back to
/// `virtual:highway` **only** for the foot-traversable pedestrian values that
/// [`crate::ingestion::osm::pbf`]'s `validate_way` already accepts:
/// `footway`, `steps`, `path`, `pedestrian`. This allows OSM ways tagged
/// `virtual:highway=footway` (e.g. platform stair-to-platform links at Bruges /
/// Berchem) to be imported and classified identically to a real `highway=`
/// way of the same value, without accidentally promoting non-pedestrian
/// `virtual:highway` values (e.g. `motorway`) to routable edges.
pub(crate) fn effective_highway<'a>(tags: &[(&str, &'a str)]) -> Option<&'a str> {
    if let Some(v) = tags.iter().find(|(k, _)| *k == "highway").map(|(_, v)| *v) {
        return Some(v);
    }
    tags.iter()
        .find(|(k, _)| *k == "virtual:highway")
        .map(|(_, v)| *v)
        .filter(|v| matches!(*v, "footway" | "steps" | "path" | "pedestrian"))
}

/// Classify a way as a pedestrian vertical [`Connector`] from its highway tag.
/// Stairs and elevators are unconditional; a ramp must be explicitly tagged
/// `ramp=yes` (a bare `incline` would mis-flag every sloped street). Returns
/// `None` for ordinary ways.
///
/// Recognises `virtual:highway=steps` / `virtual:highway=footway` (etc.) via
/// [`effective_highway`] so that OSM platform-stair connector ways tagged with
/// the `virtual:highway` namespace are classified correctly.
pub fn parse_connector(tags: &[(&str, &str)]) -> Option<Connector> {
    let get = |key: &str| tags.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
    match effective_highway(tags) {
        Some("steps") => return Some(Connector::Steps),
        Some("elevator") => return Some(Connector::Elevator),
        _ => {}
    }
    if get("elevator") == Some("yes") || get("conveying").is_some() {
        return Some(Connector::Elevator);
    }
    if get("ramp") == Some("yes") {
        return Some(Connector::Ramp);
    }
    None
}

/// Pedestrian cost model for a vertical [`Connector`], used by the Stage B1
/// connector-coverage measurement to report the *extra walk time* a connector
/// path adds. Stairs/ramps are slow (a meaningful penalty, not free walking);
/// an elevator is a flat wait. Tunable via `config.yaml`
/// (`default_routing.connector_cost`); the defaults below are the documented
/// fallbacks. NOTE: B1 does **not** charge this in routing — it is additive
/// metadata + a measurement statistic only.
#[derive(Debug, Clone, Copy)]
pub struct ConnectorCost {
    /// Effective walking speed on stairs (m/s along the step run). ~0.5 m/s is a
    /// typical stair descent/ascent pace — far slower than 1.2 m/s level walking.
    pub stairs_speed_mps: f64,
    /// Effective walking speed on a ramp (m/s) — slower than level but faster
    /// than stairs.
    pub ramp_speed_mps: f64,
    /// Fixed time (s) for an elevator: call + ride + doors, independent of run
    /// length.
    pub elevator_secs: f64,
    /// Stage B2a fixed penalty (s) added to the re-priced fallback connector that
    /// joins a relocated platform stop back to its original street snap node when no
    /// real mapped stairs/elevator exist. Charged ON TOP of pricing the geometric run
    /// as stairs, so a real mapped vertical connector (priced only by its own geometry)
    /// always undercuts this synthetic fallback.
    pub relocation_fallback_secs: f64,
}

impl Default for ConnectorCost {
    fn default() -> Self {
        ConnectorCost {
            stairs_speed_mps: 0.5,
            ramp_speed_mps: 0.9,
            elevator_secs: 45.0,
            relocation_fallback_secs: 60.0,
        }
    }
}

impl ConnectorCost {
    /// Seconds to traverse a connector of the given run `length_m`.
    pub fn seconds(&self, kind: Connector, length_m: f64) -> f64 {
        match kind {
            Connector::Steps => length_m / self.stairs_speed_mps,
            Connector::Ramp => length_m / self.ramp_speed_mps,
            Connector::Elevator => self.elevator_secs,
        }
    }

    /// Seconds for the Stage B2a synthetic fallback connector over a `run_m` run:
    /// the run priced as stairs plus the fixed `relocation_fallback_secs` penalty.
    /// Strictly greater than pricing the same run as real stairs, so genuine mapped
    /// stairs win whenever they exist.
    pub fn fallback_connector_secs(&self, run_m: f64) -> f64 {
        self.relocation_fallback_secs + self.seconds(Connector::Steps, run_m)
    }
}

fn centroid_of(ids: &[i64], coords: &HashMap<i64, (f64, f64)>) -> Option<LatLng> {
    let pts: Vec<(f64, f64)> = ids.iter().filter_map(|id| coords.get(id).copied()).collect();
    if pts.is_empty() {
        return None;
    }
    let n = pts.len() as f64;
    Some(LatLng {
        latitude: pts.iter().map(|p| p.0).sum::<f64>() / n,
        longitude: pts.iter().map(|p| p.1).sum::<f64>() / n,
    })
}

/// Build an [`OsmPlatform`] from a platform way's raw data (linear or closed/area).
/// `ids` are the OSM node refs (closed ways repeat first==last); `coords` supplies
/// lat/lon per OSM id; `resolve` maps an OSM id to its graph [`NodeID`]. Returns
/// `None` when no coord is available for any ref (e.g. the way is outside the
/// extract). Closed ways are handled identically to linear ones — the repeated
/// first/last node contributes a duplicate `node_id` entry which is harmless.
pub(crate) fn platform_from_way_data(
    ids: &[i64],
    refs: Vec<String>,
    level: Option<f32>,
    coords: &HashMap<i64, (f64, f64)>,
    resolve: impl Fn(i64) -> Option<NodeID>,
) -> Option<OsmPlatform> {
    let centroid = centroid_of(ids, coords)?;
    let node_ids: Vec<NodeID> = ids.iter().filter_map(|&id| resolve(id)).collect();
    Some(OsmPlatform { refs, level, centroid, node_ids })
}

/// Build an [`OsmPlatform`] from a platform OSM **node** (`public_transport=platform`
/// or `railway=platform` on a node). The node's own coordinate is the centroid;
/// `resolve` maps the OSM id to its graph [`NodeID`] (populated by
/// [`crate::ingestion::osm::load_pbf_file`] before this runs).
pub(crate) fn platform_from_node_data(
    osm_id: i64,
    lat: f64,
    lon: f64,
    refs: Vec<String>,
    level: Option<f32>,
    resolve: impl Fn(i64) -> Option<NodeID>,
) -> OsmPlatform {
    let centroid = LatLng { latitude: lat, longitude: lon };
    let node_ids: Vec<NodeID> = resolve(osm_id).into_iter().collect();
    OsmPlatform { refs, level, centroid, node_ids }
}

/// Build a [`PlatformIndex`] from an OSM PBF.
///
/// Indexes every `railway=platform` / `public_transport=platform` **way** (linear
/// or closed/area polygon) and every `public_transport=platform` / `railway=platform`
/// **node** carrying a `local_ref` or `ref`. Each entry retains its ref-set,
/// `level`, centroid coordinate, and — for Stage B1/B2a — the graph node IDs so
/// [`crate::ingestion::gtfs::relocate_matched_stop`] can relocate a GTFS stop onto
/// the platform. Platform way nodes (and platform-tagged OSM nodes) are added to
/// the graph in [`crate::ingestion::osm::load_pbf_file`] *before* this runs, so
/// `g.get_id` resolves them.
pub fn build_platform_index(osm_path: &str, g: &Graph) -> Result<PlatformIndex, osmpbf::Error> {
    // Pass 0: collect platform relations.  Tags (ref, level, railway/public_transport=platform)
    // live on the relation itself; member ways are typically untagged (classic multipolygon).
    // PBF ordering puts relations last, so a separate pre-scan is required.
    let mut platform_relations: Vec<(Vec<i64>, Vec<String>, Option<f32>)> = Vec::new();
    let mut relation_member_ways: HashSet<i64> = HashSet::new();

    ElementReader::from_path(osm_path)?.for_each(|el| {
        let Element::Relation(r) = el else { return };
        let tags: Vec<(&str, &str)> = r.tags().collect();
        if let Some((refs, level)) = parse_platform_tags(&tags) {
            let member_way_ids: Vec<i64> = r
                .members()
                .filter(|m| m.member_type == RelMemberType::Way)
                .map(|m| m.member_id)
                .collect();
            relation_member_ways.extend(member_way_ids.iter().copied());
            platform_relations.push((member_way_ids, refs, level));
        }
    })?;

    // Pass 1: collect platform ways, platform nodes, and node IDs for relation member ways.
    let mut platform_ways: Vec<(Vec<i64>, Vec<String>, Option<f32>)> = Vec::new();
    let mut platform_osm_nodes: Vec<(i64, f64, f64, Vec<String>, Option<f32>)> = Vec::new();
    let mut needed: HashSet<i64> = HashSet::new();
    // way_id → its node refs, for member ways of platform relations.
    let mut relation_way_nodes: HashMap<i64, Vec<i64>> = HashMap::new();

    ElementReader::from_path(osm_path)?.for_each(|el| match el {
        Element::Way(w) => {
            let tags: Vec<(&str, &str)> = w.tags().collect();
            if let Some((refs, level)) = parse_platform_tags(&tags) {
                let ids: Vec<i64> = w.refs().collect();
                needed.extend(ids.iter().copied());
                platform_ways.push((ids, refs, level));
            } else if relation_member_ways.contains(&w.id()) {
                let ids: Vec<i64> = w.refs().collect();
                needed.extend(ids.iter().copied());
                relation_way_nodes.insert(w.id(), ids);
            }
        }
        Element::DenseNode(n) => {
            if n.tags().any(|(k, v)| {
                (k == "railway" && v == "platform") || (k == "public_transport" && v == "platform")
            }) {
                let tags: Vec<(&str, &str)> = n.tags().collect();
                if is_rail_platform(&tags) {
                    if let Some((refs, level)) = parse_platform_tags(&tags) {
                        platform_osm_nodes.push((n.id(), n.lat(), n.lon(), refs, level));
                    }
                }
            }
        }
        Element::Node(n) => {
            if n.tags().any(|(k, v)| {
                (k == "railway" && v == "platform") || (k == "public_transport" && v == "platform")
            }) {
                let tags: Vec<(&str, &str)> = n.tags().collect();
                if is_rail_platform(&tags) {
                    if let Some((refs, level)) = parse_platform_tags(&tags) {
                        platform_osm_nodes.push((n.id(), n.lat(), n.lon(), refs, level));
                    }
                }
            }
        }
        _ => {}
    })?;

    // Pass 2: collect coords for all needed node IDs.
    let mut coords: HashMap<i64, (f64, f64)> = HashMap::new();
    ElementReader::from_path(osm_path)?.for_each(|el| match el {
        Element::DenseNode(n) if needed.contains(&n.id()) => {
            coords.insert(n.id(), (n.lat(), n.lon()));
        }
        Element::Node(n) if needed.contains(&n.id()) => {
            coords.insert(n.id(), (n.lat(), n.lon()));
        }
        _ => {}
    })?;

    let resolve = |id: i64| g.get_id(&format!("map#osm#{id}")).copied();

    let mut platforms: Vec<OsmPlatform> = Vec::new();
    for (ids, refs, level) in platform_ways {
        if let Some(p) = platform_from_way_data(&ids, refs, level, &coords, resolve) {
            platforms.push(p);
        }
    }
    for (osm_id, lat, lon, refs, level) in platform_osm_nodes {
        platforms.push(platform_from_node_data(osm_id, lat, lon, refs, level, resolve));
    }
    for (member_way_ids, refs, level) in platform_relations {
        let all_node_ids: Vec<i64> = member_way_ids
            .iter()
            .flat_map(|wid| {
                relation_way_nodes
                    .get(wid)
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
            })
            .copied()
            .collect();
        if let Some(p) = platform_from_way_data(&all_node_ids, refs, level, &coords, resolve) {
            platforms.push(p);
        }
    }

    let with_nodes = platforms.iter().filter(|p| !p.node_ids.is_empty()).count();
    tracing::info!(
        "platform index: {} OSM platforms parsed ({} with graph nodes)",
        platforms.len(),
        with_nodes
    );
    Ok(PlatformIndex::from_platforms(platforms))
}

/// `(count, mean, median, p90, max)` over a set of offsets (metres). Sorts in
/// place. Returns all-zero for an empty input.
pub fn offset_stats(v: &mut [f64]) -> (usize, f64, f64, f64, f64) {
    if v.is_empty() {
        return (0, 0.0, 0.0, 0.0, 0.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    let mean = v.iter().sum::<f64>() / n as f64;
    let median = v[n / 2];
    let p90_idx = ((n as f64 * 0.9).ceil() as usize).clamp(1, n) - 1;
    (n, mean, median, v[p90_idx], v[n - 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ll(lat: f64, lon: f64) -> LatLng {
        LatLng {
            latitude: lat,
            longitude: lon,
        }
    }

    #[test]
    fn parse_island_platform_tags() {
        let tags = [
            ("railway", "platform"),
            ("public_transport", "platform"),
            ("ref", "9;10"),
            ("level", "1"),
        ];
        let (refs, level) = parse_platform_tags(&tags).expect("is a platform");
        assert_eq!(refs, vec!["9".to_string(), "10".to_string()]);
        assert_eq!(level, Some(1.0));
    }

    #[test]
    fn parse_merges_ref_and_local_ref_dedup() {
        let tags = [
            ("railway", "platform"),
            ("ref", "1;2"),
            ("local_ref", "2;3"),
        ];
        let (refs, _) = parse_platform_tags(&tags).unwrap();
        assert_eq!(refs, vec!["1".to_string(), "2".to_string(), "3".to_string()]);
    }

    #[test]
    fn parse_level_range_takes_first_component() {
        let tags = [("railway", "platform"), ("level", "0;1")];
        let (_, level) = parse_platform_tags(&tags).unwrap();
        assert_eq!(level, Some(0.0));
    }

    #[test]
    fn parse_unparseable_level_is_none() {
        let tags = [("railway", "platform"), ("level", "mezzanine")];
        let (_, level) = parse_platform_tags(&tags).unwrap();
        assert_eq!(level, None);
    }

    #[test]
    fn parse_rejects_non_platform_way() {
        let tags = [("highway", "footway")];
        assert!(parse_platform_tags(&tags).is_none());
    }

    #[test]
    fn is_platform_way_railway_and_pt() {
        assert!(is_platform_way(&[("railway", "platform")]));
        assert!(is_platform_way(&[("public_transport", "platform")]));
        assert!(!is_platform_way(&[("highway", "steps")]));
        assert!(!is_platform_way(&[("railway", "rail")]));
    }

    #[test]
    fn parse_way_level_rounds_and_handles_range() {
        assert_eq!(parse_way_level(&[("level", "1")]), Some(1));
        assert_eq!(parse_way_level(&[("level", "-1")]), Some(-1));
        assert_eq!(parse_way_level(&[("level", "0;1")]), Some(0));
        assert_eq!(parse_way_level(&[("highway", "steps")]), None);
        assert_eq!(parse_way_level(&[("level", "mezzanine")]), None);
    }

    #[test]
    fn parse_connector_by_highway_tag() {
        assert_eq!(parse_connector(&[("highway", "steps")]), Some(Connector::Steps));
        assert_eq!(
            parse_connector(&[("highway", "elevator")]),
            Some(Connector::Elevator)
        );
        assert_eq!(
            parse_connector(&[("highway", "footway"), ("elevator", "yes")]),
            Some(Connector::Elevator)
        );
        assert_eq!(
            parse_connector(&[("highway", "footway"), ("ramp", "yes")]),
            Some(Connector::Ramp)
        );
        // A sloped ordinary street must NOT be a connector (would inflate coverage).
        assert_eq!(parse_connector(&[("highway", "residential"), ("incline", "5%")]), None);
        assert_eq!(parse_connector(&[("highway", "footway")]), None);
    }

    #[test]
    fn connector_cost_stairs_slower_than_elevator_constant() {
        let c = ConnectorCost::default();
        // 10 m of stairs at 0.5 m/s = 20 s; far slower than 10 m level walk (~8 s).
        assert!((c.seconds(Connector::Steps, 10.0) - 20.0).abs() < 1e-9);
        assert!((c.seconds(Connector::Ramp, 9.0) - 10.0).abs() < 1e-9);
        // Elevator is a fixed wait, independent of run length.
        assert_eq!(c.seconds(Connector::Elevator, 3.0), c.seconds(Connector::Elevator, 30.0));
    }

    #[test]
    fn connector_cost_configurable() {
        let c = ConnectorCost {
            stairs_speed_mps: 0.25,
            ramp_speed_mps: 1.0,
            elevator_secs: 60.0,
            relocation_fallback_secs: 30.0,
        };
        assert!((c.seconds(Connector::Steps, 10.0) - 40.0).abs() < 1e-9);
        assert_eq!(c.seconds(Connector::Elevator, 0.0), 60.0);
    }

    #[test]
    fn fallback_connector_costs_more_than_real_stairs() {
        let c = ConnectorCost::default();
        let run = 12.0;
        // The synthetic fallback prices the run as stairs PLUS the fixed penalty, so
        // it is strictly more expensive than a genuine mapped stairs run of equal length.
        assert!(c.fallback_connector_secs(run) > c.seconds(Connector::Steps, run));
        assert!(
            (c.fallback_connector_secs(run)
                - (c.relocation_fallback_secs + c.seconds(Connector::Steps, run)))
            .abs()
                < 1e-9
        );
    }

    fn island_index() -> PlatformIndex {
        PlatformIndex::from_platforms(vec![
            OsmPlatform {
                refs: vec!["9".into(), "10".into()],
                level: Some(1.0),
                centroid: ll(51.199, 4.433),
                node_ids: vec![],
            },
            OsmPlatform {
                refs: vec!["1".into(), "2".into()],
                level: Some(1.0),
                centroid: ll(51.200, 4.434),
                node_ids: vec![],
            },
        ])
    }

    #[test]
    fn match_island_platform_by_number() {
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: Some("9"),
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        match idx.match_platform(&q) {
            PlatformMatch::ByNumber { platform, dist_m } => {
                assert_eq!(platform, 0);
                let expected = ll(51.199, 4.432).dist(ll(51.199, 4.433));
                assert!((dist_m - expected).abs() < 1.0, "dist_m={dist_m} exp={expected}");
            }
            other => panic!("expected ByNumber, got {other:?}"),
        }
    }

    #[test]
    fn match_island_platform_n_to_1_both_tracks() {
        // platform_code 10 also maps to the same island way (refs 9;10).
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: Some("10"),
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(
            idx.match_platform(&q),
            PlatformMatch::ByNumber { platform: 0, .. }
        ));
    }

    #[test]
    fn match_empty_platform_code_is_none() {
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: Some(""),
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert_eq!(idx.match_platform(&q), PlatformMatch::None);
    }

    #[test]
    fn match_missing_platform_code_is_none() {
        // STIB-style: no platform_code column → None ⇒ matcher no-ops.
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: None,
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert_eq!(idx.match_platform(&q), PlatformMatch::None);
    }

    #[test]
    fn match_no_ref_falls_back_to_geo_nearest() {
        let idx = PlatformIndex::from_platforms(vec![OsmPlatform {
            refs: vec![],
            level: None,
            centroid: ll(51.199, 4.433),
            node_ids: vec![],
        }]);
        let q = StopPlatformQuery {
            platform_code: Some("9"),
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(
            idx.match_platform(&q),
            PlatformMatch::GeoNearest { platform: 0, .. }
        ));
    }

    #[test]
    fn match_out_of_radius_is_none() {
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: Some("9"),
            level_id: None,
            station_centroid: ll(52.0, 5.0),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert_eq!(idx.match_platform(&q), PlatformMatch::None);
    }

    #[test]
    fn offset_stats_known_distribution() {
        let mut v = vec![10.0, 20.0, 30.0, 40.0, 100.0];
        let (n, mean, median, p90, max) = offset_stats(&mut v);
        assert_eq!(n, 5);
        assert!((mean - 40.0).abs() < 1e-9);
        assert_eq!(median, 30.0);
        assert_eq!(p90, 100.0);
        assert_eq!(max, 100.0);
    }

    #[test]
    fn offset_stats_empty() {
        let mut v: Vec<f64> = vec![];
        assert_eq!(offset_stats(&mut v), (0, 0.0, 0.0, 0.0, 0.0));
    }

    #[test]
    fn area_closed_way_platform_builds_with_correct_refs_and_graph_node() {
        let tags = [
            ("railway", "platform"),
            ("ref", "9;10"),
            ("level", "1"),
            ("area", "yes"),
        ];
        let (refs, level) = parse_platform_tags(&tags).expect("area platform tags parse");
        assert_eq!(refs, vec!["9", "10"]);
        assert_eq!(level, Some(1.0));

        let ids = vec![101i64, 102, 103, 101];
        let mut coords = std::collections::HashMap::new();
        coords.insert(101i64, (51.199, 4.433));
        coords.insert(102i64, (51.200, 4.434));
        coords.insert(103i64, (51.200, 4.433));

        let plat = platform_from_way_data(&ids, refs, level, &coords, |id| match id {
            101 => Some(NodeID(0)),
            102 => Some(NodeID(1)),
            _ => None,
        })
        .expect("closed way has coords, must produce a platform");

        assert!(plat.refs.iter().any(|r| r == "9"), "ref-set must contain 9");
        assert!(plat.refs.iter().any(|r| r == "10"), "ref-set must contain 10");
        assert_eq!(plat.level, Some(1.0));
        assert!(!plat.node_ids.is_empty(), "area platform must expose at least one graph node");

        let idx = PlatformIndex::from_platforms(vec![plat]);
        let q = StopPlatformQuery {
            platform_code: Some("9"),
            level_id: None,
            station_centroid: ll(51.199, 4.432),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(idx.match_platform(&q), PlatformMatch::ByNumber { platform: 0, .. }));
    }

    #[test]
    fn platform_node_local_ref_builds_with_graph_node_and_matches() {
        let tags = [("public_transport", "platform"), ("local_ref", "3")];
        let (refs, level) = parse_platform_tags(&tags).expect("platform node tags parse");
        assert_eq!(refs, vec!["3"]);
        assert_eq!(level, None);

        let plat = platform_from_node_data(
            501,
            51.2,
            4.4,
            refs,
            level,
            |id| if id == 501 { Some(NodeID(7)) } else { None },
        );

        assert_eq!(plat.refs, vec!["3"]);
        assert_eq!(plat.node_ids, vec![NodeID(7)], "must expose the OSM node as its graph node");

        let idx = PlatformIndex::from_platforms(vec![plat]);
        let q = StopPlatformQuery {
            platform_code: Some("3"),
            level_id: None,
            station_centroid: ll(51.2, 4.4),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(idx.match_platform(&q), PlatformMatch::ByNumber { platform: 0, .. }));
    }

    #[test]
    fn linear_way_platform_no_regression() {
        let idx = island_index();
        let q = StopPlatformQuery {
            platform_code: Some("1"),
            level_id: None,
            station_centroid: ll(51.200, 4.434),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(idx.match_platform(&q), PlatformMatch::ByNumber { platform: 1, .. }));
    }

    #[test]
    fn platform_relation_two_member_ways_indexes_and_matches() {
        let tags = [("railway", "platform"), ("ref", "2;3"), ("level", "1")];
        let (refs, level) = parse_platform_tags(&tags).expect("relation tags parse as platform");
        assert_eq!(refs, vec!["2", "3"]);
        assert_eq!(level, Some(1.0));

        let all_node_ids = vec![201i64, 202, 203, 204];
        let mut coords = std::collections::HashMap::new();
        coords.insert(201i64, (51.200, 4.433));
        coords.insert(202i64, (51.201, 4.434));
        coords.insert(203i64, (51.201, 4.435));
        coords.insert(204i64, (51.200, 4.436));

        let plat = platform_from_way_data(
            &all_node_ids,
            refs,
            level,
            &coords,
            |id| match id {
                201 => Some(NodeID(20)),
                202 => Some(NodeID(21)),
                203 => Some(NodeID(22)),
                204 => Some(NodeID(23)),
                _ => None,
            },
        )
        .expect("relation platform must build when member-way nodes have coords");

        assert!(plat.refs.iter().any(|r| r == "2"), "ref-set must contain 2");
        assert!(plat.refs.iter().any(|r| r == "3"), "ref-set must contain 3");
        assert_eq!(plat.level, Some(1.0));
        assert_eq!(plat.node_ids.len(), 4, "all member-way nodes must be exposed");

        let idx = PlatformIndex::from_platforms(vec![plat]);
        let q = StopPlatformQuery {
            platform_code: Some("3"),
            level_id: None,
            station_centroid: ll(51.200, 4.434),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert!(matches!(
            idx.match_platform(&q),
            PlatformMatch::ByNumber { platform: 0, .. }
        ));
    }

    #[test]
    fn bus_stop_platform_excluded_from_index() {
        let tags = [
            ("public_transport", "platform"),
            ("highway", "bus_stop"),
            ("local_ref", "1"),
            ("name", "Namur Gare des Bus"),
            ("network", "TECN"),
        ];
        assert!(
            parse_platform_tags(&tags).is_none(),
            "highway=bus_stop platform must be excluded from the rail index"
        );
    }

    #[test]
    fn bus_yes_platform_excluded_from_index() {
        let tags = [("public_transport", "platform"), ("bus", "yes"), ("local_ref", "2")];
        assert!(
            parse_platform_tags(&tags).is_none(),
            "bus=yes platform (no rail signal) must be excluded"
        );
    }

    #[test]
    fn tram_platform_excluded_from_index() {
        let tags = [("public_transport", "platform"), ("tram", "yes"), ("local_ref", "1")];
        assert!(
            parse_platform_tags(&tags).is_none(),
            "tram=yes platform (no rail signal) must be excluded"
        );
    }

    #[test]
    fn trolleybus_platform_excluded_from_index() {
        let tags =
            [("public_transport", "platform"), ("trolleybus", "yes"), ("local_ref", "A")];
        assert!(
            parse_platform_tags(&tags).is_none(),
            "trolleybus=yes platform (no rail signal) must be excluded"
        );
    }

    #[test]
    fn railway_platform_with_bus_tag_still_included() {
        let tags = [("railway", "platform"), ("bus", "yes"), ("ref", "1")];
        let (refs, _) =
            parse_platform_tags(&tags).expect("railway=platform is an explicit rail signal");
        assert_eq!(refs, vec!["1"]);
    }

    #[test]
    fn train_yes_platform_included() {
        let tags = [("public_transport", "platform"), ("train", "yes"), ("local_ref", "3")];
        let (refs, _) = parse_platform_tags(&tags).expect("train=yes makes this a rail platform");
        assert_eq!(refs, vec!["3"]);
    }

    #[test]
    fn is_rail_platform_basic_cases() {
        assert!(is_rail_platform(&[("railway", "platform")]));
        assert!(is_rail_platform(&[("public_transport", "platform"), ("train", "yes")]));
        assert!(is_rail_platform(&[("public_transport", "platform")]));
        assert!(!is_rail_platform(&[
            ("public_transport", "platform"),
            ("highway", "bus_stop")
        ]));
        assert!(!is_rail_platform(&[("public_transport", "platform"), ("bus", "yes")]));
        assert!(!is_rail_platform(&[("public_transport", "platform"), ("tram", "yes")]));
        assert!(!is_rail_platform(&[("public_transport", "platform"), ("trolleybus", "yes")]));
        assert!(is_rail_platform(&[("railway", "platform"), ("bus", "yes")]));
    }

    #[test]
    fn match_platform_with_only_bus_platform_returns_none() {
        let bus_tags = [
            ("public_transport", "platform"),
            ("highway", "bus_stop"),
            ("local_ref", "1"),
        ];
        assert!(
            parse_platform_tags(&bus_tags).is_none(),
            "bus platform filtered at parse time → never enters index"
        );
        let idx = PlatformIndex::from_platforms(vec![]);
        let q = StopPlatformQuery {
            platform_code: Some("1"),
            level_id: None,
            station_centroid: ll(50.467, 4.865),
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        assert_eq!(
            idx.match_platform(&q),
            PlatformMatch::None,
            "empty index after bus-platform exclusion must return None"
        );
    }

    #[test]
    fn platform_relation_no_member_ways_skipped() {
        let tags = [("railway", "platform"), ("ref", "5")];
        let (refs, level) = parse_platform_tags(&tags).expect("parses");
        let all_node_ids: Vec<i64> = vec![];
        let coords = std::collections::HashMap::new();
        let result = platform_from_way_data(
            &all_node_ids,
            refs,
            level,
            &coords,
            |_| Some(NodeID(0)),
        );
        assert!(result.is_none(), "relation with no member-way coords must be skipped");
    }

    // --- effective_highway + parse_connector virtual:highway tests ---

    #[test]
    fn effective_highway_prefers_explicit_highway_tag() {
        assert_eq!(effective_highway(&[("highway", "footway")]), Some("footway"));
        assert_eq!(effective_highway(&[("highway", "steps")]), Some("steps"));
        assert_eq!(effective_highway(&[("highway", "residential")]), Some("residential"));
    }

    #[test]
    fn effective_highway_falls_back_to_virtual_highway_for_pedestrian_values() {
        assert_eq!(
            effective_highway(&[("virtual:highway", "footway")]),
            Some("footway"),
            "virtual:highway=footway must be treated as footway when highway is absent"
        );
        assert_eq!(
            effective_highway(&[("virtual:highway", "steps")]),
            Some("steps"),
            "virtual:highway=steps must be treated as steps when highway is absent"
        );
        assert_eq!(
            effective_highway(&[("virtual:highway", "path")]),
            Some("path"),
        );
        assert_eq!(
            effective_highway(&[("virtual:highway", "pedestrian")]),
            Some("pedestrian"),
        );
    }

    #[test]
    fn effective_highway_rejects_non_pedestrian_virtual_highway_values() {
        assert_eq!(
            effective_highway(&[("virtual:highway", "motorway")]),
            None,
            "virtual:highway=motorway must NOT be imported"
        );
        assert_eq!(effective_highway(&[("virtual:highway", "residential")]), None);
        assert_eq!(effective_highway(&[("virtual:highway", "cycleway")]), None);
        assert_eq!(effective_highway(&[("virtual:highway", "service")]), None);
        assert_eq!(effective_highway(&[]), None);
    }

    #[test]
    fn effective_highway_highway_wins_over_virtual_highway() {
        assert_eq!(
            effective_highway(&[("highway", "footway"), ("virtual:highway", "steps")]),
            Some("footway"),
            "explicit highway tag must always win over virtual:highway"
        );
        assert_eq!(
            effective_highway(&[("highway", "residential"), ("virtual:highway", "footway")]),
            Some("residential"),
        );
    }

    #[test]
    fn parse_connector_virtual_highway_steps_is_connector() {
        assert_eq!(
            parse_connector(&[("virtual:highway", "steps")]),
            Some(Connector::Steps),
            "virtual:highway=steps must be classified as a Steps connector"
        );
    }

    #[test]
    fn parse_connector_virtual_highway_footway_is_not_a_connector() {
        assert_eq!(
            parse_connector(&[("virtual:highway", "footway")]),
            None,
            "virtual:highway=footway is a plain walkable edge, not a connector"
        );
    }

    #[test]
    fn parse_connector_highway_wins_over_virtual_highway_for_connector() {
        // highway=footway beats virtual:highway=steps → footway is not a connector
        assert_eq!(
            parse_connector(&[("highway", "footway"), ("virtual:highway", "steps")]),
            None,
            "when highway=footway is present, virtual:highway=steps must be ignored"
        );
    }

}
