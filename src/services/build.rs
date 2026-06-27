use std::collections::HashMap;
use std::time::SystemTime;

use crate::{
    ingestion::{
        cache::resolve_source,
        gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib, prepare_sncb},
        osm::{self, Dem},
    },
    structures::{BuildConfig, DelayCDF, Graph, Ingestor, RoutingDefaultConfig},
};

/// Run only phase-0 (OSM) ingestors, then call prepare on all non-OSM ingestors.
/// The returned graph is suitable for serialization to osm.bin.
pub fn build_osm_phase(
    config: &BuildConfig,
    cache_dir: &str,
    force_download: bool,
) -> Option<Graph> {
    let mut g = Graph::new();
    run_phase(config, &mut g, 0, cache_dir, force_download)?;
    for input in &config.inputs {
        if input.phase() != 0
            && let Err(e) = prepare_ingestor(input, &mut g)
        {
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
            let osm_path = c
                .osm_url
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
    let ordered: Vec<&Ingestor> = config
        .inputs
        .iter()
        .filter(|i| i.phase() == phase)
        .collect();

    // Load the elevation DEM once for this phase, only if it ingests OSM streets.
    let dem = if ordered.iter().any(|i| matches!(i, Ingestor::OsmPbf(_))) {
        config
            .elevation
            .as_deref()
            .map(|u| u.strip_prefix("path:").unwrap_or(u))
            .and_then(|p| match Dem::load(p) {
                Ok(d) => Some(d),
                Err(e) => {
                    tracing::warn!("elevation disabled: {e}");
                    None
                }
            })
    } else {
        None
    };

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
            Ingestor::OsmPbf(_) => {
                osm::load_pbf_file(
                    &path,
                    dem.as_ref(),
                    config.elevation_smoothing_epsilon,
                    &config.surface_speed_factors,
                    g,
                )
                .map_err(|e| e.to_string())
            }
            Ingestor::GtfsGeneric(_) => load_gtfs(&path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsStib(_) => load_gtfs_stib(&path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsSncb(c) => {
                let osm_path = c
                    .osm_url
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
        .filter_map(|m| {
            m.route_type().map(|rt| {
                (
                    rt,
                    DelayCDF {
                        bins: m.bins.clone(),
                    },
                )
            })
        })
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
    if let Some(bp) = routing.bike_profile {
        g.set_bike_profile(bp);
    }
    if let Some(st) = routing.street_time {
        g.set_street_time(st);
    }
    if let Some(v) = routing.driving_speed_mps {
        g.set_driving_speed_mps(v);
    }
    if let Some(v) = routing.vehicle_access_secs {
        g.set_vehicle_access_secs(v);
    }
    if let Some(v) = routing.vehicle_access_fraction {
        g.set_vehicle_access_fraction(v);
    }
    if let Some(v) = routing.vehicle_access_max_secs {
        g.set_vehicle_access_max_secs(v);
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
    if let Some(db) = routing.distance_budget {
        g.set_distance_budget(db);
    }
    if let Some(ep) = &routing.epsilon {
        g.set_epsilon(ep.to_epsilon());
    }
    if let Some(k) = routing.bike_bucket_cyc_k {
        g.set_bike_bucket_cyc_k(k);
    }
    if let Some(k) = routing.bike_bucket_dpl_k {
        g.set_bike_bucket_dpl_k(k);
    }
    if let Some(on) = routing.multiobj_contract {
        g.set_multiobj_contract(on);
    }
    if let Some(on) = routing.node_contraction {
        g.set_node_contraction(on);
    }
    if let Some(on) = routing.bike_select_dplus {
        g.raptor.set_bike_select_dplus(on);
    }
    if let Some(vm) = routing.variance_model {
        g.set_variance_model(vm);
    }
    if let Some(cw) = routing.cost_weights {
        g.set_cost_weights(cw);
    }
    if let Some(k) = routing.representatives_k {
        g.set_representatives_k(k);
    }
    if let Some(on) = routing.multiobj_street {
        g.set_multiobj_street(on);
    }
    if let Some(m) = routing.multiobj_street_max_len_m {
        g.set_multiobj_street_max_len_m(m);
    }
    if let Some(t) = routing.champion_time_tiebreak {
        g.set_champion_time_tiebreak(t);
    }
    if let Some(f) = routing.alt_max_share_factor {
        g.set_alt_max_share_factor(f);
    }
    if let Some(cv) = routing.systematic_cv {
        g.set_systematic_cv(cv);
    }
    if let Some(b) = routing.balance {
        g.set_balance(b);
    }
    // Cost-bake the contracted bike adjacency once the bike profile / cost params are in
    // place, so the multi-objective bike search traverses super-edges in O(1). Only when
    // enabled (default off ⇒ no extra build time or memory).
    if g.raptor.multiobj_contract {
        g.build_and_bake_bike_contracted();
    }
    // Build + persist the all-mode (union) contracted graph (P3 node contraction). On a
    // restore it is already present (deserialized from graph.bin, seg_index rebuilt in
    // load_graph), so the is_none() guard skips the rebuild — it only fires on a fresh
    // build. No routing change in T1; the full node/edge arrays are kept.
    if g.raptor.node_contraction && g.contracted.is_none() {
        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
    }
    // Cost-bake bike onto the union cg's super-edges so the bike search can run on it (T3).
    // Separate from the build above: `SuperEdge.baked` is serde-skipped, so a restored union
    // cg has `baked = None` and must be re-baked here on every startup (build AND restore).
    if g.raptor.node_contraction && g.contracted.is_some() {
        g.bake_bike_on_contracted_default();
    }
}

/// Enforce the node-contraction runtime invariant for a graph about to be served or
/// persisted. Call AFTER [`apply_routing_defaults`] (so `node_contraction` is the
/// effective value). When contraction is on, drop the interior-node arrays — the P3f
/// memory win; routing then runs entirely on `g.contracted`. Returns `Err` when the
/// graph was built contracted (interior arrays already dropped) but contraction is now
/// OFF: such a graph cannot serve full-graph routing and must be rebuilt from osm.bin
/// (a `graph.bin` schema match does not protect this reverse direction).
pub fn finalize_contraction(g: &mut Graph) -> Result<(), String> {
    if !g.raptor.node_contraction {
        if g.node_count() == 0 && g.contracted.is_some() {
            return Err(
                "graph.bin was built with node_contraction enabled (interior node arrays \
                 dropped); rebuild with `--build --save` to disable node_contraction"
                    .to_string(),
            );
        }
        return Ok(());
    }
    if g.contracted.is_some() {
        g.drop_full_node_arrays();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_config() -> BuildConfig {
        BuildConfig {
            inputs: vec![],
            output: "out.bin".into(),
            osm_output: "osm.bin".into(),
            elevation: None,
            elevation_smoothing_epsilon: 4.0,
            surface_speed_factors: Default::default(),
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
