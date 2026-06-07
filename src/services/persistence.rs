use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::Graph;

pub fn save_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let bytes = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    fs::write(path, &bytes).map_err(|e| format!("Failed to save graph: {e}"))?;
    tracing::info!("graph saved to {path}");
    Ok(())
}

pub fn load_graph(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read graph file: {e}"))?;
    let mut graph: Graph = from_bytes(&bytes).map_err(|e| format!("Failed to deserialize graph: {e}"))?;
    graph.raptor.validate().map_err(|e| {
        tracing::error!("{e}");
        e
    })?;
    // Rebuild #[serde(skip)] runtime indices (e.g. trip_id → TripId).
    graph.raptor.build_runtime_indices();
    tracing::info!("graph restored from {path}");
    Ok(graph)
}

/// Save `graph` to `path` while preserving the previous good copy.
/// 1. serialize to `<path>.new`, 2. rotate existing `<path>` → `<path>.prev`,
/// 3. atomically rename `<path>.new` → `<path>`. A crash between steps always
/// leaves a valid `<path>` or `<path>.prev` for a later `--restore`.
pub fn save_graph_with_rollback(graph: &Graph, path: &str) -> Result<(), String> {
    let bytes = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
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
}
