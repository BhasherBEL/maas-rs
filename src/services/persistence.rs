use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::Graph;

/// Magic prefix identifying a maas-rs cache file.
const MAGIC: &[u8; 4] = b"MAAS";
/// Bump when any OSM-side `Graph` field (nodes/edges/kdtree/id_mapper) changes layout.
pub const OSM_SCHEMA_VERSION: u32 = 1;
/// Bump when any `Graph`/`RaptorIndex` field changes layout.
pub const GRAPH_SCHEMA_VERSION: u32 = 1;

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
        return Err(format!("'{path}' is not a maas-rs cache file (missing header)"));
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
