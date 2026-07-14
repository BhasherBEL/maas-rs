use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::{
    ingestion::{
        address::bestadd::load_bestadd_zip,
        cache::{SourceLocation, download_to, resolve_source},
        gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib, prepare_sncb},
        osm::{self, Dem, DemSet, ElevationSource},
    },
    services::persistence::{
        cch_cache_path, load_address_index, load_cch, save_address_index, save_cch,
    },
    structures::{AddressIndex, BuildConfig, DelayCDF, Graph, Ingestor, RoutingDefaultConfig},
};

const ADDRESS_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

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
            // Non-fatal: GTFS phase will fall back to re-parsing.
            tracing::warn!("prepare step for '{}' failed: {e}", input.label());
        }
    }
    Some(g)
}

/// Run phase-1+ (GTFS) ingestors on an existing graph, then finalize.
///
/// `station_merge_radius_m` and the configured [`ConnectorCost`] MUST be set onto the
/// graph BEFORE ingestion: the orphan-absorption preprocessor reads the radius, and the
/// GTFS phase bakes fallback relocation connectors using `g.connector_cost()`. The same
/// connector cost is re-set idempotently later by `apply_connector_cost`.
pub fn build_gtfs_phase(
    mut g: Graph,
    config: &BuildConfig,
    cache_dir: &str,
    force_download: bool,
    station_merge_radius_m: Option<f64>,
    routing: &RoutingDefaultConfig,
) -> Option<Graph> {
    if let Some(r) = station_merge_radius_m {
        g.set_station_merge_radius_m(r);
    }
    g.set_connector_cost(resolve_connector_cost(routing));
    run_phase(config, &mut g, 1, cache_dir, force_download)?;
    finalize(g, config)
}

/// Bake the pedestrian connector cost into edge lengths so they survive contraction
/// and the serde-skip of `connector_edges`. Must run AFTER the OSM phase (so
/// `connector_edges` is populated) and BEFORE contraction (so lengths land in
/// super-edge segments). No-op when `connector_edges` is empty (restore path, tests).
pub fn apply_connector_cost(g: &mut Graph, routing: &RoutingDefaultConfig) {
    if routing.connector_cost.is_some() {
        g.set_connector_cost(resolve_connector_cost(routing));
    }
    g.bake_connector_lengths(g.connector_cost());
}

fn resolve_connector_cost(routing: &RoutingDefaultConfig) -> crate::ingestion::osm::ConnectorCost {
    let mut cost = crate::ingestion::osm::ConnectorCost::default();
    if let Some(c) = routing.connector_cost {
        if let Some(v) = c.stairs_speed_mps {
            cost.stairs_speed_mps = v;
        }
        if let Some(v) = c.ramp_speed_mps {
            cost.ramp_speed_mps = v;
        }
        if let Some(v) = c.elevator_secs {
            cost.elevator_secs = v;
        }
        if let Some(v) = c.relocation_fallback_secs {
            cost.relocation_fallback_secs = v;
        }
    }
    cost
}

fn file_age(path: &str) -> Option<Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}

fn file_newer(a: &str, b: &str) -> bool {
    let ma = std::fs::metadata(a).and_then(|m| m.modified());
    let mb = std::fs::metadata(b).and_then(|m| m.modified());
    matches!((ma, mb), (Ok(ta), Ok(tb)) if ta > tb)
}

/// Load the sibling Belgian address index, or build it from the configured
/// `address/bestadd` feed. Graph-independent and safely skippable: any failure
/// yields an empty index so the server still starts without address search.
pub fn load_or_build_address_index(
    config: &BuildConfig,
    cache_dir: &str,
    address_path: &str,
    box_coord_epsilon_m: f64,
    address_fp: &crate::services::persistence::Fingerprint,
) -> AddressIndex {
    let Some(input) = config
        .inputs
        .iter()
        .find(|i| i.address_kind().is_some())
    else {
        return AddressIndex::default();
    };

    let zip_path = match input.location() {
        Ok(SourceLocation::Local(p)) => {
            if Path::new(&p).exists() {
                p
            } else {
                tracing::warn!("address feed file '{p}' not found; skipping address index");
                return AddressIndex::default();
            }
        }
        Ok(SourceLocation::Remote(url)) => {
            let dest = format!("{cache_dir}/{}", input.cache_filename());
            if !Path::new(&dest).exists() {
                tracing::info!(
                    "address feed '{}' not cached; downloading '{url}' to '{dest}'",
                    input.label()
                );
                if let Err(e) = download_to(&url, input.headers(), &dest) {
                    tracing::warn!("address feed download failed; skipping address index: {e}");
                    return AddressIndex::default();
                }
            }
            if file_age(&dest).map(|a| a > ADDRESS_MAX_AGE).unwrap_or(false) {
                tracing::info!("address feed cache is stale; refreshing '{dest}'");
                if let Err(e) = download_to(&url, input.headers(), &dest) {
                    tracing::warn!("address feed refresh failed (keeping cached zip): {e}");
                }
            }
            dest
        }
        Err(e) => {
            tracing::warn!("address feed source invalid: {e}; skipping address index");
            return AddressIndex::default();
        }
    };

    // Reuse the cached index only when at least as new as the zip and its
    // fingerprint still matches.
    if Path::new(address_path).exists() && !file_newer(&zip_path, address_path) {
        match load_address_index(address_path, address_fp) {
            Ok(idx) => {
                tracing::info!(
                    "address index restored from {address_path} ({} records)",
                    idx.record_count()
                );
                return idx;
            }
            Err(e) => tracing::info!("rebuilding address index ({e})"),
        }
    }

    tracing::info!("building address index from '{zip_path}'...");
    match load_bestadd_zip(&zip_path, box_coord_epsilon_m) {
        Ok(idx) => {
            tracing::info!("address index built ({} records)", idx.record_count());
            if let Err(e) = save_address_index(&idx, address_fp, address_path) {
                tracing::warn!("failed to persist address index: {e}");
            }
            idx
        }
        Err(e) => {
            tracing::error!("failed to build address index: {e}");
            AddressIndex::default()
        }
    }
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

/// Cheap up-front validation before the (potentially ~10-minute) ingestion, so an
/// obvious misconfig fails in seconds: a missing local `path:` file, or a remote URL
/// that cannot be interpolated (unset secret). Does NOT touch the network. Returns the
/// first problem found.
fn preflight_inputs(inputs: &[&Ingestor]) -> Result<(), String> {
    for input in inputs {
        match input.location() {
            Ok(SourceLocation::Local(path)) => {
                if !Path::new(&path).exists() {
                    return Err(format!(
                        "input '{}' points at a local file that does not exist: '{path}'",
                        input.label()
                    ));
                }
            }
            Ok(SourceLocation::Remote(url)) => {
                if let Err(e) = crate::ingestion::secrets::interpolate(&url) {
                    return Err(format!(
                        "input '{}' has an unresolvable remote URL ({e}); set the missing \
                         variable/secret before building",
                        input.label()
                    ));
                }
                for v in input.headers().values() {
                    if let Err(e) = crate::ingestion::secrets::interpolate(v) {
                        return Err(format!(
                            "input '{}' has an unresolvable header value ({e}); set the missing \
                             variable/secret before building",
                            input.label()
                        ));
                    }
                }
            }
            Err(e) => return Err(format!("input '{}': {e}", input.label())),
        }
    }
    Ok(())
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

    if let Err(e) = preflight_inputs(&ordered) {
        tracing::error!("preflight failed: {e}");
        return None;
    }

    // Resolve every source once, reused for both DEM loading and ingestion.
    let resolved: Vec<Result<String, String>> = ordered
        .iter()
        .map(|input| resolve_source(input, cache_dir, force_download))
        .collect();

    // A DEM is only useful for OSM elevation sampling; skip it for a phase with no OSM.
    let has_osm = ordered
        .iter()
        .any(|i| matches!(i, Ingestor::OsmPbf(_)));

    let mut dems: Vec<Dem> = Vec::new();
    if has_osm {
        for (input, path) in ordered.iter().zip(&resolved) {
            let Some(projection) = input.dem_projection() else {
                continue;
            };
            let path = match path {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("elevation source '{}' unresolved: {e}", input.label());
                    continue;
                }
            };
            match Dem::load(path, projection) {
                Ok(d) => dems.push(d),
                Err(e) => tracing::warn!("elevation disabled for '{}': {e}", input.label()),
            }
        }
    }
    let dem_set = DemSet(dems);
    let dem: Option<&dyn ElevationSource> = if dem_set.0.is_empty() {
        None
    } else {
        Some(&dem_set)
    };

    for (input, resolved_path) in ordered.iter().zip(&resolved) {
        // DEM resolution failure is non-fatal (already warned above).
        if input.dem_projection().is_some() && resolved_path.is_err() {
            continue;
        }

        tracing::info!("loading '{}'...", input.label());
        let before = SystemTime::now();

        let path = match resolved_path {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to resolve '{}': {e}", input.label());
                return None;
            }
        };

        let result = match input {
            Ingestor::OsmPbf(_) => {
                osm::load_pbf_file(
                    path,
                    dem,
                    config.elevation_smoothing_epsilon,
                    &config.surface_speed_factors,
                    g,
                )
                .map_err(|e| e.to_string())
            }
            Ingestor::GtfsGeneric(_) => load_gtfs(path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsStib(_) => load_gtfs_stib(path, g).map_err(|e| e.to_string()),
            Ingestor::GtfsSncb(c) => {
                let osm_path = c
                    .osm_url
                    .strip_prefix("path:")
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| c.osm_url.clone());
                load_gtfs_sncb(path, &osm_path, g).map_err(|e| e.to_string())
            }
            Ingestor::AddressBestAdd(_) => Ok(()),
            Ingestor::DemBelgianLambert2008(_) => Ok(()),
        };

        match result {
            Ok(_) => {
                if let Ok(elapsed) = before.elapsed() {
                    tracing::info!("loaded '{}' in {}ms", input.label(), elapsed.as_millis());
                }
            }
            Err(e) => {
                tracing::error!(
                    "failed to ingest '{}' from '{path}': {e}. If '{path}' is a cached download, \
                     it may be corrupt or an HTML error page; delete it to force a re-download.",
                    input.label()
                );
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

/// Apply config.yaml routing defaults onto a freshly built or restored graph.
/// Shared by `main` (startup) and the scheduler (after a hot rebuild).
pub fn apply_routing_defaults(
    g: &mut Graph,
    routing: &RoutingDefaultConfig,
    graph_output: &str,
) {
    if let Some(s) = routing.min_access_secs {
        g.set_min_access_secs(s);
    }
    if let Some(v) = routing.walking_speed_mps {
        g.set_walking_speed_mps(v);
    }
    apply_connector_cost(g, routing);
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
    if let Some(v) = routing.unrestricted_transfers {
        g.set_unrestricted_transfers(v);
    }
    if let Some(v) = routing.use_cch_access {
        g.set_use_cch_access(v);
    }
    if let Some(v) = routing.profile_latency {
        g.set_profile_latency(v);
    }
    if let Some(m) = routing.max_window_minutes {
        g.set_max_window_secs(m.saturating_mul(60));
    }
    if let Some(v) = routing.travel_map_grid_step_m {
        g.set_travel_map_grid_step_m(v);
    }
    if let Some(v) = routing.travel_map_max_cells {
        g.set_travel_map_max_cells(v);
    }
    if let Some(v) = routing.travel_map_window_sample_secs {
        g.set_travel_map_window_sample_secs(v);
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
    if let Some(k) = routing.drive_bucket_var_k {
        g.set_drive_bucket_var_k(k);
    }
    if let Some(k) = routing.walk_bucket_surf_k {
        g.set_walk_bucket_surf_k(k);
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
    if let Some(f) = routing.alt_max_share_factor {
        g.set_alt_max_share_factor(f);
    }
    if let Some(cv) = routing.systematic_cv {
        g.set_systematic_cv(cv);
    }
    if let Some(b) = routing.balance {
        g.set_balance(b);
    }
    if let Some(fares) = &routing.fares {
        g.set_fare_model(fares.to_fare_model());
    }
    // On restore the contracted graph is already present, so this only fires on a fresh build.
    if g.contracted.is_none() {
        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
    }
    // `SuperEdge.baked` is serde-skipped, so re-bake bike on every startup (build AND restore).
    if g.contracted.is_some() {
        g.bake_bike_on_contracted_default();
    }

    if routing.prepare_cch_access.unwrap_or(true) && g.contracted.is_some() {
        prepare_cch_access(g, graph_output);
    }
}

/// Install the foot-access CCH on `g`, reusing the cached nested-dissection ORDER from
/// the sibling `cch.bin` when version/size-matching, else computing (~56 s) and caching
/// it. The CCH structure + walk-second metric are always rebuilt from the live graph so
/// the metric never goes stale. Requires `g.contracted` (checked by the caller).
fn prepare_cch_access(g: &mut Graph, graph_output: &str) {
    // Idempotent: apply_routing_defaults may run twice, so skip if already installed.
    if g.cch.is_some() {
        return;
    }
    let path = cch_cache_path(graph_output);
    let n = g.cch_vertex_count();
    let order = match load_cch(&path) {
        Ok(order) if order.len() == n => {
            tracing::info!("reusing cached CCH order from {path} ({n} vertices)");
            order
        }
        Ok(order) => {
            tracing::info!(
                "cached CCH order size mismatch (cached {}, graph {n}); recomputing",
                order.len()
            );
            let order = g.compute_cch_order();
            if let Err(e) = save_cch(&order, &path) {
                tracing::warn!("failed to save CCH order: {e}");
            }
            order
        }
        Err(e) => {
            tracing::info!("building CCH order ({e})");
            let order = g.compute_cch_order();
            if let Err(e) = save_cch(&order, &path) {
                tracing::warn!("failed to save CCH order: {e}");
            }
            order
        }
    };
    let cch = g.build_cch_access_with_order(&order);
    g.set_cch(cch);
}

/// Drop the interior-node arrays (the memory win) so routing runs entirely on
/// `g.contracted`. Call AFTER [`apply_routing_defaults`] (so `g.contracted` is built).
/// `Err` when no contracted graph is present: it must be rebuilt from osm.bin.
pub fn finalize_contraction(g: &mut Graph) -> Result<(), String> {
    if g.contracted.is_some() {
        g.drop_full_node_arrays();
        Ok(())
    } else {
        Err("graph has no contracted graph (cannot serve contraction-only routing); \
             rebuild with `--build --save`"
            .to_string())
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
            address_output: "address.bin".into(),
            cache_dir: None,
            elevation_smoothing_epsilon: 4.0,
            surface_speed_factors: Default::default(),
            delay_models: vec![],
        }
    }

    fn parse_inputs(yaml: &str) -> Vec<Ingestor> {
        #[derive(serde::Deserialize)]
        struct Wrap {
            inputs: Vec<Ingestor>,
        }
        let w: Wrap = serde_yaml_ng::from_str(yaml).unwrap();
        w.inputs
    }

    #[test]
    fn preflight_flags_missing_local_path() {
        let inputs = parse_inputs(
            "inputs:\n  - ingestor: osm/pbf\n    url: \"path:/no/such/file.pbf\"\n",
        );
        let refs: Vec<&Ingestor> = inputs.iter().collect();
        let err = preflight_inputs(&refs).unwrap_err();
        assert!(err.contains("/no/such/file.pbf"), "names the missing path: {err}");
        assert!(err.contains("does not exist"), "explains the problem: {err}");
    }

    #[test]
    fn preflight_flags_unresolvable_remote_url() {
        let inputs = parse_inputs(
            "inputs:\n  - ingestor: gtfs/generic\n    name: bus\n    \
             url: \"https://x/${MAAS_PREFLIGHT_UNSET_VAR_XYZ}/gtfs.zip\"\n",
        );
        let refs: Vec<&Ingestor> = inputs.iter().collect();
        let err = preflight_inputs(&refs).unwrap_err();
        assert!(err.contains("bus"), "names the feed: {err}");
        assert!(err.contains("unresolvable"), "explains the problem: {err}");
    }

    #[test]
    fn preflight_passes_for_existing_local_and_plain_remote() {
        let f = std::env::temp_dir().join(format!("maas_preflight_{}.pbf", std::process::id()));
        std::fs::write(&f, b"x").unwrap();
        let inputs = parse_inputs(&format!(
            "inputs:\n  - ingestor: osm/pbf\n    url: \"path:{}\"\n  \
             - ingestor: gtfs/generic\n    name: bus\n    url: \"https://x/gtfs.zip\"\n",
            f.display()
        ));
        let refs: Vec<&Ingestor> = inputs.iter().collect();
        assert!(preflight_inputs(&refs).is_ok());
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
        let result = build_gtfs_phase(g, &config, "cache", false, None, &RoutingDefaultConfig::default());
        assert!(result.is_some());
    }

    /// Regression: `relocation_fallback_secs` must reach the connector cost BEFORE the
    /// GTFS phase bakes fallback relocation connectors (phase entry, not later).
    #[test]
    fn gtfs_phase_applies_configured_fallback_secs() {
        use crate::structures::ConnectorCostConfig;
        let config = empty_config();
        let routing = RoutingDefaultConfig {
            connector_cost: Some(ConnectorCostConfig {
                stairs_speed_mps: None,
                ramp_speed_mps: None,
                elevator_secs: None,
                relocation_fallback_secs: Some(123.0),
            }),
            ..Default::default()
        };
        let g = build_gtfs_phase(Graph::new(), &config, "cache", false, None, &routing).unwrap();
        assert_eq!(g.connector_cost().relocation_fallback_secs, 123.0);
        let run_m = 10.0;
        assert_eq!(
            g.connector_cost().fallback_connector_secs(run_m),
            123.0 + g.connector_cost().seconds(crate::structures::Connector::Steps, run_m),
        );
    }
}
