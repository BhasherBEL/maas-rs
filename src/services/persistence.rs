use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::Graph;

/// Magic prefix identifying a maas-rs cache file.
const MAGIC: &[u8; 4] = b"MAAS";
/// Bump when any OSM-side `Graph` field (nodes/edges/kdtree/id_mapper) changes layout.
/// v3: bike-route membership now propagated from OSM relations onto edges' `cycleroute`.
/// v4: `StreetEdgeData` gained a `var_gen` variance-generator field.
/// v5: `elev_delta` is now DEM-denoised per-way at ingestion (RDP smoothing), so
///     stale caches carry raw (noisy) ascent and must be rebuilt.
/// v6: `StreetEdgeData` gained a baked `surface_speed` bike speed factor.
pub const OSM_SCHEMA_VERSION: u32 = 6;
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
pub const GRAPH_SCHEMA_VERSION: u32 = 10;

const HEADER_LEN: usize = 8;

fn with_header(version: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Verify the magic + version header, returning the payload slice. Any mismatch
/// is an error so the caller can rebuild instead of deserializing stale bytes.
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

pub fn save_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header(GRAPH_SCHEMA_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save graph: {e}"))?;
    tracing::info!("graph saved to {path}");
    Ok(())
}

pub fn load_graph(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read graph file: {e}"))?;
    let payload = split_header(&bytes, GRAPH_SCHEMA_VERSION, path)?;
    let mut graph: Graph =
        from_bytes(payload).map_err(|e| format!("Failed to deserialize graph: {e}"))?;
    graph.raptor.validate().map_err(|e| {
        tracing::error!("{e}");
        e
    })?;
    // Rebuild #[serde(skip)] runtime indices (e.g. trip_id → TripId).
    graph.raptor.build_runtime_indices();
    // Rebuild the #[serde(skip)] spatial edge index for edge-aware snapping.
    graph.build_edge_index();
    // Rebuild the contracted graph's #[serde(skip)] segment R-tree (P3 node contraction).
    if let Some(cg) = graph.contracted.as_mut() {
        cg.build_seg_index();
    }
    tracing::info!("graph restored from {path}");
    Ok(graph)
}

/// Save the OSM network only (no `RaptorIndex`) to `path`, headered with
/// `OSM_SCHEMA_VERSION` so it can be reused across transit-only struct changes.
pub fn save_osm_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = graph.to_osm_postcard()?;
    let bytes = with_header(OSM_SCHEMA_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save OSM graph: {e}"))?;
    tracing::info!("OSM graph saved to {path}");
    Ok(())
}

/// Load an OSM-only cache into a `Graph` with an empty `RaptorIndex`.
pub fn load_osm_graph(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read OSM graph file: {e}"))?;
    let payload = split_header(&bytes, OSM_SCHEMA_VERSION, path)?;
    let graph = Graph::from_osm_postcard(payload)?;
    tracing::info!("OSM graph restored from {path}");
    Ok(graph)
}

/// Save `graph` to `path` while preserving the previous good copy.
/// 1. serialize to `<path>.new`, 2. rotate existing `<path>` → `<path>.prev`,
/// 3. atomically rename `<path>.new` → `<path>`. A crash between steps always
///    leaves a valid `<path>` or `<path>.prev` for a later `--restore`.
pub fn save_graph_with_rollback(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header(GRAPH_SCHEMA_VERSION, &payload);
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

    #[test]
    fn rollback_save_rotates_previous() {
        let dir = std::env::temp_dir().join("maas_persist_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();
        let prev_s = format!("{path_s}.prev");
        let _ = std::fs::remove_file(&prev_s);

        save_graph_with_rollback(&Graph::new(), path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());

        save_graph_with_rollback(&Graph::new(), path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());
        assert!(std::path::Path::new(&prev_s).exists());

        assert!(load_graph(path_s).is_ok());
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
        // Deliberately do NOT build the edge index before saving: it is #[serde(skip)]
        // and must be rebuilt on load.
        save_graph(&g, path_s).unwrap();

        let loaded = load_graph(path_s).unwrap();
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
        // A straight chain a-b-c-d-e: b,c,d are degree-2 interior pass-throughs that the
        // union contraction collapses into super-edges between junctions a and e.
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
        // build_raptor_index() populates transit_node_to_stop, which the contraction reads.
        g.build_raptor_index();
        g.set_node_contraction(true);

        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        assert!(cg.junction_count() >= 2, "endpoints a,e are junctions");
        g.contracted = Some(cg);

        save_graph(&g, path_s).unwrap();
        let mut loaded = load_graph(path_s).unwrap();
        // Move the contracted graph out so it can borrow `loaded` immutably below; this
        // also proves load_graph populated it (None ⇒ unwrap panics).
        let cg = loaded.contracted.take().expect("contracted survives the round trip");
        // load_graph rebuilt the serde-skipped seg_index; a coord near a chain edge
        // midpoint resolves to its bounding junctions.
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
        let bytes = with_header(GRAPH_SCHEMA_VERSION + 1, &payload);
        std::fs::write(path_s, &bytes).unwrap();

        let err = load_graph(path_s).unwrap_err();
        assert!(err.contains("version mismatch"), "got: {err}");
    }

    #[test]
    fn load_rejects_missing_header() {
        let dir = std::env::temp_dir().join("maas_persist_nohdr_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        // Legacy file: raw postcard, no header.
        let payload = to_allocvec(&Graph::new()).unwrap();
        std::fs::write(path_s, &payload).unwrap();

        let err = load_graph(path_s).unwrap_err();
        assert!(err.contains("missing header"), "got: {err}");
    }

    #[test]
    fn osm_graph_round_trip_drops_raptor() {
        let dir = std::env::temp_dir().join("maas_persist_osm_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("osm.bin");
        let path_s = path.to_str().unwrap();

        save_osm_graph(&Graph::new(), path_s).unwrap();
        let restored = load_osm_graph(path_s).unwrap();
        assert_eq!(restored.node_count(), 0);
        assert_eq!(restored.raptor.transit_trips.len(), 0);
    }

    #[test]
    fn load_osm_rejects_graph_file() {
        let dir = std::env::temp_dir().join("maas_persist_xfmt_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        // A full graph.bin must not be loadable as an OSM cache (version differs
        // only if the consts diverge, but the payloads are structurally distinct).
        save_graph(&Graph::new(), path_s).unwrap();
        // Force a version divergence to ensure the header guard triggers even
        // when both consts currently share a value.
        let bytes = std::fs::read(path_s).unwrap();
        let bumped = with_header(OSM_SCHEMA_VERSION + 99, &bytes[HEADER_LEN..]);
        std::fs::write(path_s, &bumped).unwrap();
        assert!(load_osm_graph(path_s).is_err());
    }
}
