use std::collections::HashMap;
use std::time::SystemTime;

use crate::{
    ingestion::{cache::resolve_path, gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib}, osm},
    structures::{BuildConfig, DelayCDF, Graph, Ingestor},
};

pub fn build_graph(config: BuildConfig) -> Option<Graph> {
    let mut g = Graph::new();

    let mut ordered: Vec<&Ingestor> = config.inputs.iter().collect();
    ordered.sort_by_key(|i| i.phase());

    for input in ordered {
        tracing::info!("loading '{}'...", input.label());
        let before = SystemTime::now();

        let path = match resolve_path(input) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to resolve '{}': {e}", input.label());
                return None;
            }
        };

        let result = match input {
            Ingestor::OsmPbf(_) => osm::load_pbf_file(&path, &mut g).map_err(|e| e.to_string()),
            Ingestor::GtfsGeneric(_) => load_gtfs(&path, &mut g).map_err(|e| e.to_string()),
            Ingestor::GtfsStib(_) => load_gtfs_stib(&path, &mut g).map_err(|e| e.to_string()),
            Ingestor::GtfsSncb(c) => {
                let osm_path = c.osm_url
                    .strip_prefix("path:")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| c.osm_url.clone());
                load_gtfs_sncb(&path, &osm_path, &mut g).map_err(|e| e.to_string())
            }
        };

        match result {
            Ok(_) => {
                if let Ok(elapsed) = before.elapsed() {
                    tracing::info!("loaded '{}' in {}ms", input.label(), elapsed.as_millis());
                }
            }
            Err(e) => {
                tracing::error!("failed to ingest '{}': {e}", input.label());
                return None;
            }
        }
    }

    tracing::info!("building RAPTOR index...");
    g.build_raptor_index();

    let models = config
        .delay_models
        .iter()
        .filter_map(|m| m.route_type().map(|rt| (rt, DelayCDF { bins: m.bins.clone() })))
        .collect::<HashMap<_, _>>();
    g.set_transit_delay_models(models);

    tracing::info!("build complete");
    Some(g)
}
