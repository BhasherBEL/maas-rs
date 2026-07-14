use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::{AddressIndex, Graph};

/// Magic prefix identifying a maas-rs cache file.
const MAGIC: &[u8; 4] = b"MAAS";
/// Bump when any OSM-side `Graph` field (nodes/edges/kdtree/id_mapper) changes layout.
/// v3: bike-route membership now propagated from OSM relations onto edges' `cycleroute`.
/// v4: `StreetEdgeData` gained a `var_gen` variance-generator field.
/// v5: `elev_delta` is now DEM-denoised per-way at ingestion (RDP smoothing), so
///     stale caches carry raw (noisy) ascent and must be rebuilt.
/// v6: `StreetEdgeData` gained a baked `surface_speed` bike speed factor.
/// v7: OSM view gained a `PlatformIndex` (Stage A platform matching) serialized
///     into `osm.bin` so the GTFS phase can match platform stops on `--update-gtfs`.
/// v8: Stage B1 — OSM view gained `node_levels` + `connector_edges` (level/connector
///     maps); `OsmPlatform` gained `node_ids`; platform ways are now imported as
///     walkable foot edges (so node/edge counts changed). GRAPH_SCHEMA is NOT bumped:
///     all of this is serde-skipped on `Graph` and lives only in the OSM view, mirroring
///     `PlatformIndex`; routing never reads it in B1, so graph.bin's layout is unchanged.
/// v9: `build_platform_index` now also indexes `public_transport=platform` /
///     `railway=platform` OSM **nodes** (with `local_ref`/`ref`); `load_pbf_file`
///     adds those nodes to the graph as unindexed entries. GRAPH stays 12 — these
///     unindexed nodes are serde-skipped and invisible to the routing core.
/// v10: `build_platform_index` now indexes `railway=platform` /
///     `public_transport=platform` OSM **relations** (multipolygon platforms where
///     member ways are untagged). `load_pbf_file` registers member-way nodes as
///     unindexed entries so the resolver can find them. Old osm.bin is missing
///     relation-derived platforms → rebuild needed. GRAPH stays 16: PlatformIndex
///     is OSM-view-only and unindexed nodes remain serde-skipped.
/// v11: `build_platform_index` now excludes bus/tram-only platforms from the index
///     (`highway=bus_stop` or `bus=yes`/`tram=yes`/`trolleybus=yes` without a rail
///     signal). Old osm.bin may contain bus-terminal nodes (e.g. Namur TECN Gare des
///     Bus) that falsely match SNCB rail platform codes → rebuild to purge them.
///     GRAPH stays 17: PlatformIndex is OSM-view-only.
/// v12: `validate_way` now treats `virtual:highway=footway/steps/path/pedestrian`
///     as a fallback when `highway` is absent, importing OSM platform-stair
///     connector ways that were previously silently dropped (e.g. Bruges/Berchem
///     stair-top-to-platform links tagged `virtual:highway=footway`). More ways
///     are imported → node/edge counts in osm.bin change → rebuild required.
///     GRAPH stays 17: no routing-core struct layout change.
/// v13: elevation moved from `build.elevation` into `build.inputs` as projection-tagged
///      `dem/*` ingestors (first-hit `DemSet`); baked per-edge `elev_delta` may change when
///      multiple DEMs layer, so osm.bin must rebuild.
/// v14: header layout changed: osm.bin now carries a 32-byte input+param fingerprint
///      after the version field (dependency-aware cache invalidation), so a v13 header is
///      unreadable and must rebuild.
pub const OSM_SCHEMA_VERSION: u32 = 14;
/// Bump when any `Graph`/`RaptorIndex` field changes layout (or, like v5, the baked
/// `elev_delta` edge values change meaning).
/// v7: `Graph` gained a serialized `contracted: Option<ContractedGraph>` (P3 node
///     contraction).
/// v8: `RaptorIndex` gained `transit_pattern_segment_timetables` (g-free transit-leg
///     reconstruction for the node-contraction drop).
/// v9: `RaptorIndex` gained `transit_stop_names` (g-free stop-name resolution for the
///     explain survey + plan nodes after the interior-node drop empties `g.nodes`).
/// v10: P3f cutover — node_contraction default ON, the interior-node arrays are DROPPED
///      at build/restore, so graph.bin carries empty `nodes`/`edges` + the contracted graph.
/// v11: overtaking-trips split — `build_raptor_index` now decomposes each pattern into
///      non-overtaking sub-routes so every per-stop departure column is monotonic, so the
///      built pattern set differs (old graph.bin holds unsplit, non-FIFO patterns).
/// v12: Stage B2a snap-relocation — Stage-A platform-matched stops are relocated onto their
///      matched OSM platform node and re-priced (boarding at the platform + a penalised
///      fallback connector to the original street snap node), so the stop anchors and foot
///      connector edges in graph.bin differ from v11. OSM_SCHEMA_VERSION stays 8: osm.bin is
///      serialized at the end of the OSM phase, before relocation runs in the GTFS phase, so
///      no relocated stop/edge/level ever enters it and its layout is unchanged.
/// v13: (was 13, now 14) Connector-cost baking — OSM stairs/elevator/ramp edge lengths are
///      rewritten at build time (before contraction) so `edge_secs` yields the correct
///      slower time instead of charging at flat walking speed. Super-edge segments in the
///      contracted graph now carry the baked lengths, so graph.bin content differs from v12.
/// v15: RaptorIndex carries transit_stop_platform_codes (parallel to names) so the plan/live
///      UI can show "Pl. N"; bump forces a rebuild to populate it (serde-default-compatible
///      load, but the existing graph.bin's array is empty until rebuilt).
/// v16: RaptorIndex carries transit_route_ids (raw GTFS route_id strings, parallel to
///      transit_routes) required for route-level realtime alert matching. Bump forces a rebuild
///      so the field is populated; old graphs silently skip route alerts until rebuilt.
/// v17: B2a platform relocation uses bounded foot Dijkstra to pick the cheapest reachable
///      platform node and suppresses the straight fallback connector when a real mapped path
///      exists. Relocation targets and edge counts change → rebuild required.
/// v18: RaptorIndex carries `transit_stations` (platforms grouped by GTFS `parent_station`
///      into deduped physical stations) plus `TransitStopData.parent_station`. Bump forces a
///      rebuild so the station index is populated; lookup maps are derived on load.
/// v19: `StationInfo` gains `modes` (per-station transport-type set), and the GTFS phase now
///      synthesizes `parent_station` for STIB/DeLijn orphan stops (radius-capped same-name
///      absorption), so the grouped station set + content differ. Rebuild required.
/// v20: `StationInfo` gains `lines` (distinct routes serving the station, each with mode +
///      hex colours), populated at build time for the per-mode line badges in autocomplete.
///      Rebuild required so the field is populated.
/// v21: elevation moved into `build.inputs` as projection-tagged `dem/*` ingestors; the baked
///      `elev_delta` values live in the serialized contracted graph, so graph.bin must rebuild
///      alongside the OSM_SCHEMA_VERSION 13 bump.
/// v22: header layout changed: graph.bin now carries a 32-byte input+param fingerprint after
///      the version field (dependency-aware cache invalidation), so a v21 header is unreadable
///      and must rebuild. The graph fingerprint embeds the osm fingerprint, so an OSM/DEM
///      change cascades to graph.bin; this bump also invalidates cch.bin via the XOR header.
pub const GRAPH_SCHEMA_VERSION: u32 = 22;

/// Bump when the persisted (`#[serde]`-non-skipped) fields of [`AddressIndex`] change
/// layout. Sibling cache `address.bin`, independent of the routing graph.
/// v1: initial BeST-Add address index (interned streets/municipalities/postals + rows).
/// v2: records are building-level (keyed by `(street, house_number)`); per-row box
///     numbers collapsed into a `boxes: Vec<AddressBox>` metadata list. Rebuild
///     required so apartment rows aggregate into one building candidate.
/// v3: ingestion drops the ~140 k un-geocoded `<pos>0 0</pos>` placeholder records
///     (and any out-of-Belgium coordinate); street-level collapse now uses the
///     component-wise median instead of the mean. Rebuild required so the poisoned
///     records leave the index and street centroids land on-street.
/// v4: header layout changed: address.bin now carries a 32-byte input+param fingerprint
///     after the version field (dependency-aware cache invalidation), so a v3 header is
///     unreadable and must rebuild.
pub const ADDRESS_SCHEMA_VERSION: u32 = 4;

/// Bump when the persisted `cch.bin` payload layout (the metric-independent nested-
/// dissection ORDER + vertex count) changes. Independent of the CCH *metric*, which is
/// re-customized from the live graph on every load. `cch.bin` is ALSO implicitly gated
/// by `GRAPH_SCHEMA_VERSION` (the order's vertex set is the built graph's junction
/// topology): the on-disk header stores `GRAPH_SCHEMA_VERSION ^ CCH_SCHEMA_VERSION` so a
/// graph-layout bump invalidates a stale order without a manual bump here.
/// v1: initial — stores `(n: u32, order: Vec<u32>)` for the foot-access CCH.
pub const CCH_SCHEMA_VERSION: u32 = 1;

/// Combined header version for `cch.bin`: mixes the CCH payload version with the graph
/// schema so any graph rebuild (which changes the junction set the order indexes)
/// invalidates a cached order automatically.
const CCH_HEADER_VERSION: u32 = CCH_SCHEMA_VERSION ^ GRAPH_SCHEMA_VERSION;

const HEADER_LEN: usize = 8;

/// MAGIC(4) + version(4) + fingerprint(32).
const HEADER_FP_LEN: usize = HEADER_LEN + 32;

pub type Fingerprint = [u8; 32];

/// A bad/absent magic-version header or a fingerprint mismatch; both signal "rebuild".
#[derive(Debug)]
pub struct StaleCache(pub String);

impl std::fmt::Display for StaleCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn with_header_fp(version: u32, fp: &Fingerprint, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_FP_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(fp);
    out.extend_from_slice(payload);
    out
}

fn split_header_fp<'a>(
    bytes: &'a [u8],
    expected_version: u32,
    expected_fp: &Fingerprint,
    path: &str,
) -> Result<&'a [u8], StaleCache> {
    if bytes.len() < HEADER_FP_LEN || &bytes[..4] != MAGIC {
        return Err(StaleCache(format!(
            "'{path}' is not a maas-rs cache file (missing header)"
        )));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != expected_version {
        return Err(StaleCache(format!(
            "'{path}' schema version mismatch (file={version}, expected={expected_version})"
        )));
    }
    if &bytes[HEADER_LEN..HEADER_FP_LEN] != expected_fp.as_slice() {
        return Err(StaleCache(format!(
            "'{path}' fingerprint mismatch (inputs or baked params changed)"
        )));
    }
    Ok(&bytes[HEADER_FP_LEN..])
}

/// Verifies only magic + version, IGNORING the fingerprint (for config-less diagnostics).
fn split_header_fp_any<'a>(
    bytes: &'a [u8],
    expected_version: u32,
    path: &str,
) -> Result<&'a [u8], String> {
    if bytes.len() < HEADER_FP_LEN || &bytes[..4] != MAGIC {
        return Err(format!("'{path}' is not a maas-rs cache file (missing header)"));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != expected_version {
        return Err(format!(
            "'{path}' schema version mismatch (file={version}, expected={expected_version})"
        ));
    }
    Ok(&bytes[HEADER_FP_LEN..])
}

/// Validate a cache header without reading the payload.
pub fn header_valid(path: &str, expected_version: u32, expected_fp: &Fingerprint) -> bool {
    let mut buf = [0u8; HEADER_FP_LEN];
    let Ok(mut f) = fs::File::open(path) else {
        return false;
    };
    if std::io::Read::read_exact(&mut f, &mut buf).is_err() {
        return false;
    }
    &buf[..4] == MAGIC
        && u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) == expected_version
        && &buf[HEADER_LEN..HEADER_FP_LEN] == expected_fp.as_slice()
}

fn with_header(version: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn split_header<'a>(bytes: &'a [u8], expected: u32, path: &str) -> Result<&'a [u8], String> {
    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(format!(
            "'{path}' is not a maas-rs cache file (missing header)"
        ));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != expected {
        return Err(format!(
            "'{path}' schema version mismatch (file={version}, expected={expected})"
        ));
    }
    Ok(&bytes[HEADER_LEN..])
}

pub fn save_graph(graph: &Graph, fp: &Fingerprint, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header_fp(GRAPH_SCHEMA_VERSION, fp, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save graph: {e}"))?;
    tracing::info!("graph saved to {path}");
    Ok(())
}

pub fn load_graph(path: &str, fp: &Fingerprint) -> Result<Graph, StaleCache> {
    tracing::info!("restoring graph from {path}…");
    let bytes = fs::read(path).map_err(|e| StaleCache(format!("Failed to read graph file: {e}")))?;
    let payload = split_header_fp(&bytes, GRAPH_SCHEMA_VERSION, fp, path)?;
    let mut graph: Graph = from_bytes(payload)
        .map_err(|e| StaleCache(format!("Failed to deserialize graph: {e}")))?;
    graph.raptor.validate().map_err(|e| {
        tracing::error!("{e}");
        StaleCache(e)
    })?;
    // Rebuild #[serde(skip)] runtime indices / spatial R-trees.
    graph.raptor.build_runtime_indices();
    graph.build_edge_index();
    if let Some(cg) = graph.contracted.as_mut() {
        cg.build_seg_index();
    }
    tracing::info!("graph restored from {path}");
    Ok(graph)
}

/// Verifies only the schema version, ignoring the fingerprint. NOT for the serving
/// path (fingerprint-gated); for config-less diagnostics only.
pub fn load_graph_unchecked(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read graph file: {e}"))?;
    let payload = split_header_fp_any(&bytes, GRAPH_SCHEMA_VERSION, path)?;
    let mut graph: Graph =
        from_bytes(payload).map_err(|e| format!("Failed to deserialize graph: {e}"))?;
    graph.raptor.validate()?;
    graph.raptor.build_runtime_indices();
    graph.build_edge_index();
    if let Some(cg) = graph.contracted.as_mut() {
        cg.build_seg_index();
    }
    Ok(graph)
}

pub fn load_osm_graph_unchecked(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read OSM graph file: {e}"))?;
    let payload = split_header_fp_any(&bytes, OSM_SCHEMA_VERSION, path)?;
    Graph::from_osm_postcard(payload)
}

pub fn save_osm_graph(graph: &Graph, fp: &Fingerprint, path: &str) -> Result<(), String> {
    let payload = graph.to_osm_postcard()?;
    let bytes = with_header_fp(OSM_SCHEMA_VERSION, fp, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save OSM graph: {e}"))?;
    tracing::info!("OSM graph saved to {path}");
    Ok(())
}

pub fn load_osm_graph(path: &str, fp: &Fingerprint) -> Result<Graph, StaleCache> {
    tracing::info!("restoring OSM graph from {path}…");
    let bytes =
        fs::read(path).map_err(|e| StaleCache(format!("Failed to read OSM graph file: {e}")))?;
    let payload = split_header_fp(&bytes, OSM_SCHEMA_VERSION, fp, path)?;
    let graph = Graph::from_osm_postcard(payload).map_err(StaleCache)?;
    tracing::info!("OSM graph restored from {path}");
    Ok(graph)
}

/// Sibling CCH-order path derived from the graph output, so distinct presets never
/// share a CCH file.
pub fn cch_cache_path(graph_output: &str) -> String {
    let p = std::path::Path::new(graph_output);
    let file = p
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    let leaf = cch_leaf_name(&file);
    match p.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => {
            dir.join(&leaf).to_string_lossy().into_owned()
        }
        _ => leaf,
    }
}

fn cch_leaf_name(graph_file: &str) -> String {
    if graph_file.is_empty() {
        return "cch.bin".to_string();
    }
    match graph_file.strip_prefix("graph") {
        Some(rest) => format!("cch{rest}"),
        None => format!("cch-{graph_file}"),
    }
}

/// Saves ONLY the metric-independent CCH order + vertex count; the rest is re-derived
/// from the live graph at load.
pub fn save_cch(order: &[u32], path: &str) -> Result<(), String> {
    let n = order.len() as u32;
    let payload = to_allocvec(&(n, order.to_vec()))
        .map_err(|e| format!("Failed to serialize CCH order: {e}"))?;
    let bytes = with_header(CCH_HEADER_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save CCH order: {e}"))?;
    tracing::info!("CCH order saved to {path} ({n} vertices)");
    Ok(())
}

pub fn load_cch(path: &str) -> Result<Vec<u32>, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read CCH file: {e}"))?;
    let payload = split_header(&bytes, CCH_HEADER_VERSION, path)?;
    let (n, order): (u32, Vec<u32>) =
        from_bytes(payload).map_err(|e| format!("Failed to deserialize CCH order: {e}"))?;
    if n as usize != order.len() {
        return Err(format!(
            "'{path}' CCH order corrupt (n={n}, order.len()={})",
            order.len()
        ));
    }
    Ok(order)
}

pub fn save_address_index(
    index: &AddressIndex,
    fp: &Fingerprint,
    path: &str,
) -> Result<(), String> {
    let payload =
        to_allocvec(index).map_err(|e| format!("Failed to serialize address index: {e}"))?;
    let bytes = with_header_fp(ADDRESS_SCHEMA_VERSION, fp, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save address index: {e}"))?;
    tracing::info!("address index saved to {path}");
    Ok(())
}

pub fn load_address_index(path: &str, fp: &Fingerprint) -> Result<AddressIndex, StaleCache> {
    let bytes =
        fs::read(path).map_err(|e| StaleCache(format!("Failed to read address index file: {e}")))?;
    let payload = split_header_fp(&bytes, ADDRESS_SCHEMA_VERSION, fp, path)?;
    let mut index: AddressIndex = from_bytes(payload)
        .map_err(|e| StaleCache(format!("Failed to deserialize address index: {e}")))?;
    index.rebuild_indexes();
    Ok(index)
}

/// Write-new, rotate-prev, atomic-rename so a crash always leaves a valid `<path>`
/// or `<path>.prev` for a later `--restore`.
pub fn save_graph_with_rollback(graph: &Graph, fp: &Fingerprint, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header_fp(GRAPH_SCHEMA_VERSION, fp, &payload);
    let new_path = format!("{path}.new");
    fs::write(&new_path, &bytes).map_err(|e| format!("Failed to write '{new_path}': {e}"))?;

    if fs::metadata(path).is_ok() {
        let prev_path = format!("{path}.prev");
        fs::rename(path, &prev_path)
            .map_err(|e| format!("Failed to rotate '{path}' to '{prev_path}': {e}"))?;
    }
    fs::rename(&new_path, path).map_err(|e| format!("Failed to publish '{path}': {e}"))?;
    tracing::info!("graph saved to {path} (previous kept as {path}.prev)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::Graph;

    const FP0: Fingerprint = [0u8; 32];
    const FP1: Fingerprint = [1u8; 32];

    #[test]
    fn rollback_save_rotates_previous() {
        let dir = std::env::temp_dir().join("maas_persist_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();
        let prev_s = format!("{path_s}.prev");
        let _ = std::fs::remove_file(&prev_s);

        save_graph_with_rollback(&Graph::new(), &FP0, path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());

        save_graph_with_rollback(&Graph::new(), &FP0, path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());
        assert!(std::path::Path::new(&prev_s).exists());

        assert!(load_graph(path_s, &FP0).is_ok());
    }

    #[test]
    fn load_graph_rebuilds_edge_index_for_snapping() {
        use crate::structures::{
            BikeAttrs, EdgeData, Endpoint, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            cost::VarGen,
        };
        let dir = std::env::temp_dir().join("maas_persist_edgeidx_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let mut g = Graph::new();
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.0085));
        let edge = |o, d| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: 607,
                foot: false,
                bike: true,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b));
        g.add_edge(b, edge(b, a));
        save_graph(&g, &FP0, path_s).unwrap();

        let loaded = load_graph(path_s, &FP0).unwrap();
        let (ep, _) = loaded
            .snap_to_edge(50.000, 4.00425, 300.0, |s| s.bike)
            .expect("loaded graph snaps onto the bike edge");
        assert!(
            matches!(ep, Endpoint::OnEdge { .. }),
            "edge index rebuilt on load"
        );
    }

    #[test]
    fn contracted_graph_survives_round_trip() {
        use crate::structures::{
            BikeAttrs, EdgeData, LatLng, NodeData, OsmNodeData, StreetEdgeData, cost::VarGen,
        };
        let dir = std::env::temp_dir().join("maas_persist_contracted_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let mut g = Graph::new();
        let coords = [
            ("a", 50.000, 4.0000),
            ("b", 50.000, 4.0010),
            ("c", 50.000, 4.0020),
            ("d", 50.000, 4.0030),
            ("e", 50.000, 4.0040),
        ];
        let ids: Vec<_> = coords.iter().map(|&(id, lat, lon)| g.add_node(mk(id, lat, lon))).collect();
        let edge = |o, d| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: 71,
                foot: true,
                bike: true,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        for w in ids.windows(2) {
            g.add_edge(w[0], edge(w[0], w[1]));
            g.add_edge(w[1], edge(w[1], w[0]));
        }
        g.build_raptor_index();

        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        assert!(cg.junction_count() >= 2, "endpoints a,e are junctions");
        g.contracted = Some(cg);

        save_graph(&g, &FP0, path_s).unwrap();
        let mut loaded = load_graph(path_s, &FP0).unwrap();
        let cg = loaded.contracted.take().expect("contracted survives the round trip");
        let entries = cg.walk_entries_arena(&loaded, 50.000, 4.0015, 100.0);
        assert!(!entries.is_empty(), "snap near an edge yields junction entries");
    }

    #[test]
    fn load_rejects_version_mismatch() {
        let dir = std::env::temp_dir().join("maas_persist_version_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let payload = to_allocvec(&Graph::new()).unwrap();
        let bytes = with_header_fp(GRAPH_SCHEMA_VERSION + 1, &FP0, &payload);
        std::fs::write(path_s, &bytes).unwrap();

        let err = load_graph(path_s, &FP0).unwrap_err();
        assert!(err.to_string().contains("version mismatch"), "got: {err}");
    }

    #[test]
    fn load_rejects_missing_header() {
        let dir = std::env::temp_dir().join("maas_persist_nohdr_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let payload = to_allocvec(&Graph::new()).unwrap();
        std::fs::write(path_s, &payload).unwrap();

        let err = load_graph(path_s, &FP0).unwrap_err();
        assert!(err.to_string().contains("missing header"), "got: {err}");
    }

    #[test]
    fn load_rejects_fingerprint_mismatch() {
        let dir = std::env::temp_dir().join("maas_persist_fp_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        save_graph(&Graph::new(), &FP0, path_s).unwrap();
        assert!(load_graph(path_s, &FP0).is_ok());
        let err = load_graph(path_s, &FP1).unwrap_err();
        assert!(err.to_string().contains("fingerprint mismatch"), "got: {err}");
    }

    #[test]
    fn osm_graph_round_trip_drops_raptor() {
        let dir = std::env::temp_dir().join("maas_persist_osm_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("osm.bin");
        let path_s = path.to_str().unwrap();

        save_osm_graph(&Graph::new(), &FP0, path_s).unwrap();
        let restored = load_osm_graph(path_s, &FP0).unwrap();
        assert_eq!(restored.node_count(), 0);
        assert_eq!(restored.raptor.transit_trips.len(), 0);
    }

    #[test]
    fn osm_graph_round_trip_preserves_platform_index() {
        use crate::ingestion::osm::{OsmPlatform, PlatformIndex};
        use crate::structures::LatLng;

        let dir = std::env::temp_dir().join("maas_persist_platform_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("osm.bin");
        let path_s = path.to_str().unwrap();

        let mut g = Graph::new();
        g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
            refs: vec!["9".into(), "10".into()],
            level: Some(1.0),
            centroid: LatLng {
                latitude: 51.199,
                longitude: 4.433,
            },
            node_ids: vec![crate::structures::NodeID(7)],
        }]));
        let mut nl = std::collections::HashMap::new();
        nl.insert(crate::structures::NodeID(7), 1i16);
        let mut ce = std::collections::HashMap::new();
        ce.insert(
            (crate::structures::NodeID(7), crate::structures::NodeID(8)),
            crate::structures::Connector::Steps,
        );
        g.set_osm_level_data(nl, ce);

        save_osm_graph(&g, &FP0, path_s).unwrap();
        let restored = load_osm_graph(path_s, &FP0).unwrap();
        let idx = restored.platform_index();
        assert_eq!(idx.len(), 1);
        let p = idx.platform(0).unwrap();
        assert_eq!(p.refs, vec!["9".to_string(), "10".to_string()]);
        assert_eq!(p.level, Some(1.0));
        assert!((p.centroid.latitude - 51.199).abs() < 1e-9);
        assert!((p.centroid.longitude - 4.433).abs() < 1e-9);
        assert_eq!(p.node_ids, vec![crate::structures::NodeID(7)]);
        assert_eq!(restored.node_level(crate::structures::NodeID(7)), Some(1));
        assert_eq!(
            restored.connector_kind(crate::structures::NodeID(7), crate::structures::NodeID(8)),
            Some(crate::structures::Connector::Steps)
        );
    }

    #[test]
    fn address_index_round_trip_rebuilds_search() {
        use crate::structures::{AddressIndexBuilder, Named};

        let dir = std::env::temp_dir().join("maas_persist_address_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("address.bin");
        let path_s = path.to_str().unwrap();

        let mut b = AddressIndexBuilder::new();
        let s = b.intern_street(
            "S1",
            Named {
                display: "Rue de la Loi".into(),
                aliases: vec!["Rue de la Loi".into(), "Wetstraat".into()],
            },
        );
        let m = b.intern_municipality(
            "M1",
            Named {
                display: "Bruxelles".into(),
                aliases: vec!["Bruxelles".into(), "Brussel".into()],
            },
        );
        let p = b.intern_postal("P1", "1000".into());
        b.push_record("A1".into(), s, m, p, "16".into(), String::new(), 50.846, 4.367);
        let idx = b.finish();

        save_address_index(&idx, &FP0, path_s).unwrap();
        let loaded = load_address_index(path_s, &FP0).unwrap();
        assert_eq!(loaded.search("rue de la loi 16", 5, None).len(), 1);
        assert_eq!(loaded.search("wetstraat 16", 5, None)[0].id, "A1");
    }

    #[test]
    fn cch_order_round_trip() {
        let dir = std::env::temp_dir().join("maas_persist_cch_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cch.bin");
        let path_s = path.to_str().unwrap();

        let order: Vec<u32> = vec![3, 0, 4, 1, 2];
        save_cch(&order, path_s).unwrap();
        let loaded = load_cch(path_s).unwrap();
        assert_eq!(loaded, order, "CCH order survives the round trip");
    }

    #[test]
    fn cch_load_rejects_version_mismatch() {
        let dir = std::env::temp_dir().join("maas_persist_cch_ver_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cch.bin");
        let path_s = path.to_str().unwrap();

        let payload = to_allocvec(&(2u32, vec![0u32, 1])).unwrap();
        let bytes = with_header(CCH_HEADER_VERSION.wrapping_add(1), &payload);
        std::fs::write(path_s, &bytes).unwrap();

        let err = load_cch(path_s).unwrap_err();
        assert!(err.contains("version mismatch"), "got: {err}");
    }

    #[test]
    fn cch_load_rejects_corrupt_count() {
        let dir = std::env::temp_dir().join("maas_persist_cch_corrupt_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cch.bin");
        let path_s = path.to_str().unwrap();

        let payload = to_allocvec(&(9u32, vec![0u32, 1, 2])).unwrap();
        let bytes = with_header(CCH_HEADER_VERSION, &payload);
        std::fs::write(path_s, &bytes).unwrap();

        let err = load_cch(path_s).unwrap_err();
        assert!(err.contains("corrupt"), "got: {err}");
    }

    #[test]
    fn cch_cache_path_is_sibling_of_graph() {
        assert_eq!(cch_cache_path("data/graph.bin"), "data/cch.bin");
        assert_eq!(cch_cache_path("graph.bin"), "cch.bin");
    }

    #[test]
    fn cch_cache_path_derives_distinct_leaf_per_output() {
        assert_eq!(cch_cache_path("graph.bin"), "cch.bin");
        assert_eq!(cch_cache_path("data/graph.bin"), "data/cch.bin");

        assert_eq!(cch_cache_path("graph.belgium.bin"), "cch.belgium.bin");
        assert_eq!(
            cch_cache_path("data/graph.belgium.bin"),
            "data/cch.belgium.bin"
        );

        assert_ne!(
            cch_cache_path("graph.bin"),
            cch_cache_path("graph.belgium.bin")
        );

        assert_eq!(cch_cache_path("data/custom.bin"), "data/cch-custom.bin");
        assert_ne!(
            cch_cache_path("data/custom.bin"),
            cch_cache_path("data/graph.bin")
        );
    }

    #[test]
    fn load_osm_rejects_graph_file() {
        let dir = std::env::temp_dir().join("maas_persist_xfmt_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        save_graph(&Graph::new(), &FP0, path_s).unwrap();
        let bytes = std::fs::read(path_s).unwrap();
        let bumped = with_header_fp(OSM_SCHEMA_VERSION + 99, &FP0, &bytes[HEADER_FP_LEN..]);
        std::fs::write(path_s, &bumped).unwrap();
        assert!(load_osm_graph(path_s, &FP0).is_err());
    }
}
