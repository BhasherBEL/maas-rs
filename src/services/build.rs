use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::{
    ingestion::{
        bestadd::load_bestadd_zip,
        cache::{SourceLocation, download_to, resolve_source},
        gtfs::{load_gtfs, load_gtfs_sncb, load_gtfs_stib, prepare_sncb},
        osm::{self, Dem},
    },
    services::persistence::{
        cch_cache_path, load_address_index, load_cch, save_address_index, save_cch,
    },
    structures::{AddressIndex, BuildConfig, DelayCDF, Graph, Ingestor, RoutingDefaultConfig},
};

/// Re-download the address zip only when it is missing or older than this.
const ADDRESS_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

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
/// `station_merge_radius_m` (config `default_routing.station_merge_radius_m`) is
/// applied to the graph before ingestion so the per-provider orphan-absorption
/// preprocessor can read it; `None` keeps the compiled-in default.
pub fn build_gtfs_phase(
    mut g: Graph,
    config: &BuildConfig,
    cache_dir: &str,
    force_download: bool,
    station_merge_radius_m: Option<f64>,
) -> Option<Graph> {
    if let Some(r) = station_merge_radius_m {
        g.set_station_merge_radius_m(r);
    }
    run_phase(config, &mut g, 1, cache_dir, force_download)?;
    finalize(g, config)
}

/// Apply the configured pedestrian connector cost model onto the graph and bake
/// the costs into edge lengths so they survive contraction and serde-skip of
/// `connector_edges`. Must be called after the OSM phase (so `connector_edges` is
/// populated) and before contraction (so lengths land in super-edge segments).
/// Absent config fields keep the compiled-in defaults.
pub fn apply_connector_cost(g: &mut Graph, routing: &RoutingDefaultConfig) {
    if let Some(c) = routing.connector_cost {
        let mut cost = crate::ingestion::osm::ConnectorCost::default();
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
        g.set_connector_cost(cost);
    }
    // Bake connector-specific traversal costs into edge lengths so contraction
    // picks them up. Uses the connector_cost now set (defaults if no config override).
    // No-op when connector_edges is empty (restore path, tests without connectors).
    g.bake_connector_lengths(g.connector_cost());
}

fn file_age(path: &str) -> Option<Duration> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}

/// True when `a`'s mtime is strictly newer than `b`'s (both must exist).
fn file_newer(a: &str, b: &str) -> bool {
    let ma = std::fs::metadata(a).and_then(|m| m.modified());
    let mb = std::fs::metadata(b).and_then(|m| m.modified());
    matches!((ma, mb), (Ok(ta), Ok(tb)) if ta > tb)
}

/// Load the sibling Belgian address index, or build it from the configured
/// `best/add` feed. This is **graph-independent** and **safely skippable**:
///
/// * No `best/add` input configured ⇒ empty index (feature off).
/// * Source zip **absent** (remote) ⇒ downloaded once to `cache/<label>.zip` and
///   cached, then built; on download failure an empty index is returned gracefully
///   (the server still starts without address search). A present cache is never
///   re-downloaded.
/// * Source zip present and **fresh** (< 7 days) ⇒ reused as-is; a cached
///   `address.bin` at least as new as the zip is loaded without re-parsing.
/// * Source zip present but **stale** (> 7 days) ⇒ a best-effort weekly refresh
///   download runs (the operator opted in by providing the file); on failure the
///   cached zip is kept. The index is then (re)built and persisted to `address.bin`.
///
/// Never wired into the hourly auto-update scheduler (which force-downloads every
/// tick); the only download here is the opt-in stale refresh above.
pub fn load_or_build_address_index(
    config: &BuildConfig,
    cache_dir: &str,
    address_path: &str,
    box_coord_epsilon_m: f64,
) -> AddressIndex {
    let Some(input) = config
        .inputs
        .iter()
        .find(|i| matches!(i, Ingestor::BeStAdd(_)))
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
            let dest = format!("{cache_dir}/{}.zip", input.label());
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

    if Path::new(address_path).exists() && !file_newer(&zip_path, address_path) {
        match load_address_index(address_path) {
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
            if let Err(e) = save_address_index(&idx, address_path) {
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
            Ingestor::BeStAdd(_) => Ok(()),
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
        .filter(|i| i.phase() != 0 && !matches!(i, Ingestor::BeStAdd(_)))
        .map(|i| i.label().to_string())
        .collect();
    labels.sort();
    labels
}

/// Apply config.yaml routing defaults onto a freshly built or restored graph.
/// Shared by `main` (startup) and the scheduler (after a hot rebuild). `graph_output` is
/// the configured `graph.bin` path; the foot-access CCH order is cached in a sibling
/// `cch.bin` next to it (see [`prepare_cch_access`]).
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
    // Build + persist the all-mode (union) contracted graph. On a restore it is already
    // present (deserialized from graph.bin, seg_index rebuilt in load_graph), so the
    // is_none() guard skips the rebuild — it only fires on a fresh build.
    if g.contracted.is_none() {
        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
    }
    // Cost-bake bike onto the union cg's super-edges so the bike search can run on it.
    // Separate from the build above: `SuperEdge.baked` is serde-skipped, so a restored union
    // cg has `baked = None` and must be re-baked here on every startup (build AND restore).
    if g.contracted.is_some() {
        g.bake_bike_on_contracted_default();
    }

    // Foot-access CCH (serde-skipped runtime index). Behind `prepare_cch_access` (default
    // true) so the per-query `useCchAccess` flag has a live index to dispatch to; when
    // false the seam always falls back to the two-pass foot Dijkstra.
    if routing.prepare_cch_access.unwrap_or(true) && g.contracted.is_some() {
        prepare_cch_access(g, graph_output);
    }
}

/// Install the foot-access CCH on `g`, reusing the cached nested-dissection ORDER in the
/// sibling `cch.bin` when present + version/size-matching, else computing it (~56 s) and
/// caching it so later restarts skip that cost. The CCH structure + walk-second metric are
/// always (re)built from the live graph (~1.3 s), so the metric never goes stale. Requires
/// `g.contracted` (checked by the caller).
fn prepare_cch_access(g: &mut Graph, graph_output: &str) {
    // Idempotent: on the auto/no-flag path apply_routing_defaults runs twice (once inside
    // acquire_auto, once in main), so skip the redundant ~1.3 s rebuild if already installed.
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

/// Enforce the contraction runtime invariant for a graph about to be served or
/// persisted. Call AFTER [`apply_routing_defaults`] (so `g.contracted` is built). Drops
/// the interior-node arrays — the memory win; routing then runs entirely on
/// `g.contracted`. Returns `Err` when no contracted graph is present: such a graph cannot
/// serve contraction-only routing and must be rebuilt from osm.bin.
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
        let result = build_gtfs_phase(g, &config, "cache", false, None);
        assert!(result.is_some());
    }
}
