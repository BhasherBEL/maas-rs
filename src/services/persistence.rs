use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::Graph;

pub fn save_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let bytes = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    fs::write(path, &bytes).map_err(|e| format!("Failed to save graph: {e}"))?;
    println!("Graph saved to {}", path);
    Ok(())
}

pub fn load_graph(path: &str) -> Result<Graph, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read graph file: {e}"))?;
    let res = from_bytes(&bytes).map_err(|e| format!("Failed to deserialize graph: {e}"));
    println!("Graph restored from {}", path);
    res
}
