use std::collections::HashMap;
use std::time::SystemTime;

use crate::{
    ingestion::{
        cache::resolve_source,
        gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib, prepare_sncb},
        osm,
    },
    structures::{BuildConfig, DelayCDF, Graph, Ingestor, RoutingDefaultConfig},
};

/// Run only phase-0 (OSM) ingestors, then call prepare on all non-OSM ingestors.
/// The returned graph is suitable for serialization to osm.bin.
pub fn build_osm_phase(config: &BuildConfig, cache_dir: &str, force_download: bool) -> Option<Graph> {
    let mut g = Graph::new();
    run_phase(config, &mut g, 0, cache_dir, force_download)?;
    for input in &config.inputs {
        if input.phase() != 0
            && let Err(e) = prepare_ingestor(input, &mut g) {
                tracing::warn!("prepare step for '{}' failed: {e}", input.label());
                // Non-fatal: GTFS phase will fall back to re-parsing
            }
    }
    Some(g)
}

/// Run only phase-1+ (GTFS) ingestors on an existing graph, then finalize.
pub fn build_gtfs_phase(
    mut g: Graph,
    config: &BuildConfig,
    cache_dir: &str,
    force_download: bool,
) -> Option<Graph> {
    run_phase(config, &mut g, 1, cache_dir, force_download)?;
    finalize(g, config)
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

fn run_phase(
    config: &BuildConfig,
    g: &mut Graph,
    phase: u8,
    cache_dir: &str,
    force_download: bool,
) -> Option<()> {
    let ordered: Vec<&Ingestor> = config.inputs.iter()
        .filter(|i| i.phase() == phase)
        .collect();

    for input in ordered {
        tracing::info!("loading '{}'...", input.label());
        let before = SystemTime::now();

        let path = match resolve_source(input, cache_dir, force_download) {
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

/// Sorted list of non-OSM input labels from the build config.
/// Used to detect when a source is added/removed between restarts.
pub fn gtfs_input_labels(config: &BuildConfig) -> Vec<String> {
    let mut labels: Vec<String> = config
        .inputs
        .iter()
        .filter(|i| i.phase() != 0)
        .map(|i| i.label().to_string())
        .collect();
    labels.sort();
    labels
}

/// Apply config.yaml routing defaults onto a freshly built or restored graph.
/// Shared by `main` (startup) and the scheduler (after a hot rebuild).
pub fn apply_routing_defaults(g: &mut Graph, routing: &RoutingDefaultConfig) {
    if let Some(s) = routing.min_access_secs {
        g.set_min_access_secs(s);
    }
    if let Some(v) = routing.walking_speed_mps {
        g.set_walking_speed_mps(v);
    }
    if let Some(v) = routing.cycling_speed_mps {
        g.set_cycling_speed_mps(v);
    }
    if let Some(v) = routing.driving_speed_mps {
        g.set_driving_speed_mps(v);
    }
    if let Some(v) = routing.vehicle_access_secs {
        g.set_vehicle_access_secs(v);
    }
    if let Some(edges) = routing.reliability_bucket_edges.clone() {
        g.set_reliability_bucket_edges(edges);
    }
    if let Some(s) = routing.arrival_slack_secs {
        g.set_arrival_slack_secs(s);
    }
    if let Some(m) = routing.max_window_minutes {
        g.set_max_window_secs(m.saturating_mul(60));
    }
    if let Some(m) = routing.max_snap_distance_m {
        g.set_max_snap_distance_m(m);
    }
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
        assert!(run_phase(&config, &mut g, 0, "cache", false).is_some());
    }

    #[test]
    fn run_phase_empty_gtfs_succeeds() {
        let config = empty_config();
        let mut g = Graph::new();
        assert!(run_phase(&config, &mut g, 1, "cache", false).is_some());
    }

    #[test]
    fn build_osm_phase_empty_config() {
        let config = empty_config();
        let g = build_osm_phase(&config, "cache", false);
        assert!(g.is_some());
        assert_eq!(g.unwrap().node_count(), 0);
    }

    #[test]
    fn build_gtfs_phase_empty_finalizes() {
        let config = empty_config();
        let g = Graph::new();
        let result = build_gtfs_phase(g, &config, "cache", false);
        assert!(result.is_some());
    }
}
