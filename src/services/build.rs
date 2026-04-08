use std::collections::HashMap;
use std::time::SystemTime;

use crate::{
    ingestion::{
        cache::resolve_path,
        gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib, prepare_sncb},
        osm,
    },
    structures::{BuildConfig, DelayCDF, Graph, Ingestor},
};

/// Run only phase-0 (OSM) ingestors, then call prepare on all non-OSM ingestors.
/// The returned graph is suitable for serialization to osm.bin.
pub fn build_osm_phase(config: &BuildConfig) -> Option<Graph> {
    let mut g = Graph::new();
    run_phase(config, &mut g, 0)?;
    for input in &config.inputs {
        if input.phase() != 0 {
            if let Err(e) = prepare_ingestor(input, &mut g) {
                tracing::warn!("prepare step for '{}' failed: {e}", input.label());
                // Non-fatal: GTFS phase will fall back to re-parsing
            }
        }
    }
    Some(g)
}

/// Run only phase-1+ (GTFS) ingestors on an existing graph, then finalize.
pub fn build_gtfs_phase(mut g: Graph, config: &BuildConfig) -> Option<Graph> {
    run_phase(config, &mut g, 1)?;
    finalize(g, config)
}

/// Convenience wrapper: full build without needing to manage osm.bin.
pub fn build_graph(config: BuildConfig) -> Option<Graph> {
    let g = build_osm_phase(&config)?;
    build_gtfs_phase(g, &config)
}

fn prepare_ingestor(input: &Ingestor, g: &mut Graph) -> Result<(), String> {
    match input {
        Ingestor::GtfsSncb(c) => {
            let osm_path = c.osm_url
                .strip_prefix("path:")
                .map(|s| s.to_string())
                .unwrap_or_else(|| c.osm_url.clone());
            prepare_sncb(&osm_path, g).map_err(|e| e.to_string())
        }
        _ => Ok(()),
    }
}

fn run_phase(config: &BuildConfig, g: &mut Graph, phase: u8) -> Option<()> {
    let ordered: Vec<&Ingestor> = config.inputs.iter()
        .filter(|i| i.phase() == phase)
        .collect();

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
            Ingestor::OsmPbf(_) => osm::load_pbf_file(&path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsGeneric(_) => load_gtfs(&path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsStib(_) => load_gtfs_stib(&path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsSncb(c) => {
                let osm_path = c.osm_url
                    .strip_prefix("path:")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| c.osm_url.clone());
                load_gtfs_sncb(&path, &osm_path, g).map_err(|e| e.to_string())
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
    Some(())
}

fn finalize(mut g: Graph, config: &BuildConfig) -> Option<Graph> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_config() -> BuildConfig {
        BuildConfig {
            inputs: vec![],
            output: "out.bin".into(),
            osm_output: "osm.bin".into(),
            delay_models: vec![],
        }
    }

    #[test]
    fn run_phase_empty_osm_succeeds() {
        let config = empty_config();
        let mut g = Graph::new();
        assert!(run_phase(&config, &mut g, 0).is_some());
    }

    #[test]
    fn run_phase_empty_gtfs_succeeds() {
        let config = empty_config();
        let mut g = Graph::new();
        assert!(run_phase(&config, &mut g, 1).is_some());
    }

    #[test]
    fn build_osm_phase_empty_config() {
        let config = empty_config();
        let g = build_osm_phase(&config);
        assert!(g.is_some());
        assert_eq!(g.unwrap().node_count(), 0);
    }

    #[test]
    fn build_gtfs_phase_empty_finalizes() {
        let config = empty_config();
        let g = Graph::new();
        let result = build_gtfs_phase(g, &config);
        assert!(result.is_some());
    }
}
