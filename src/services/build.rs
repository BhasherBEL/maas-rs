use std::time::SystemTime;

use crate::{
    ingestion::{cache::resolve_path, gtfs::load_gtfs, osm},
    structures::{BuildConfig, Graph, Ingestor},
};

pub fn build_graph(config: BuildConfig) -> Option<Graph> {
    let mut g = Graph::new();

    let mut ordered: Vec<&Ingestor> = config.inputs.iter().collect();
    ordered.sort_by_key(|i| i.phase());

    for input in ordered {
        println!("Loading '{}'...", input.label());
        let before = SystemTime::now();

        let path = match resolve_path(input) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to resolve '{}': {e}", input.label());
                return None;
            }
        };

        let result = match input {
            Ingestor::OsmPbf(_) => osm::load_pbf_file(&path, &mut g).map_err(|e| e.to_string()),
            Ingestor::GtfsGeneric(_) => load_gtfs(&path, &mut g).map_err(|e| e.to_string()),
        };

        match result {
            Ok(_) => {
                if let Ok(elapsed) = before.elapsed() {
                    println!("Loaded '{}' in {}ms", input.label(), elapsed.as_millis());
                }
            }
            Err(e) => {
                eprintln!("Failed to ingest '{}': {e}", input.label());
                return None;
            }
        }
    }

    Some(g)
}
