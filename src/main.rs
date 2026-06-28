use std::{env, sync::Arc};

use arc_swap::ArcSwap;
use chrono::Local;
use maas_rs::{
    ingestion::cache::{load_input_labels, save_input_labels, save_last_checked},
    logging,
    services::{
        build::{build_gtfs_phase, build_osm_phase, gtfs_input_labels},
        persistence::{
            load_graph, load_osm_graph, save_graph, save_graph_with_rollback, save_osm_graph,
        },
    },
    structures::Config,
    web::app,
};

#[tokio::main]
async fn main() {
    let config = match Config::load("config.yaml") {
        Ok(c) => c,
        Err(e) => {
            // Logging not yet initialised — write directly to stderr.
            eprintln!("Failed to load config.yaml: {e}");
            return;
        }
    };

    logging::init(&config.log_level);

    let cache_dir = config
        .auto_update
        .as_ref()
        .map(|a| a.cache_dir.clone())
        .unwrap_or_else(|| "cache".to_string());

    let args: Vec<String> = env::args().collect();

    let build_mode = args.contains(&"--build".to_string());
    let save_mode = args.contains(&"--save".to_string());
    let restore_mode = args.contains(&"--restore".to_string());
    let serve_mode = args.contains(&"--serve".to_string());
    let update_gtfs_mode = args.contains(&"--update-gtfs".to_string());

    let mode_count = [build_mode, restore_mode, update_gtfs_mode]
        .iter()
        .filter(|&&x| x)
        .count();
    if mode_count > 1 {
        tracing::error!("at most one of --build, --restore, or --update-gtfs may be set");
        return;
    }
    if save_mode && restore_mode {
        tracing::error!("--save requires --build or --update-gtfs");
        return;
    }

    // No explicit mode flag ⇒ self-healing auto path: restore the cached graph,
    // or rebuild it (reusing osm.bin when possible), then serve.
    let auto = mode_count == 0;

    let mut g = if auto {
        match acquire_auto(&config, &cache_dir) {
            Some(g) => g,
            None => return,
        }
    } else if build_mode {
        let osm_graph = match build_osm_phase(&config.build, &cache_dir, false) {
            Some(g) => g,
            None => {
                tracing::error!("OSM phase failed");
                return;
            }
        };

        if save_mode && let Err(e) = save_osm_graph(&osm_graph, &config.build.osm_output) {
            tracing::error!("{e}");
            return;
        }

        match build_gtfs_phase(osm_graph, &config.build, &cache_dir, false) {
            Some(g) => g,
            None => {
                tracing::error!("GTFS phase failed");
                return;
            }
        }
    } else if update_gtfs_mode {
        let osm_graph = match load_osm_graph(&config.build.osm_output) {
            Ok(g) => g,
            Err(_) => {
                tracing::error!(
                    "'{}' not found — run '--build --save' first",
                    config.build.osm_output
                );
                return;
            }
        };

        // Manual refresh: always pull fresh remote feeds.
        match build_gtfs_phase(osm_graph, &config.build, &cache_dir, true) {
            Some(g) => g,
            None => {
                tracing::error!("GTFS phase failed");
                return;
            }
        }
    } else {
        // --restore
        match load_graph(&config.build.output) {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("{e}");
                return;
            }
        }
    };

    // Apply config.yaml routing defaults (works for all modes). Must run BEFORE the save
    // below so any persisted artifact it builds (e.g. the contracted `g.contracted`)
    // is written into graph.bin rather than rebuilt in RAM on every restore.
    maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing);

    // Drop the interior-node arrays so the served graph (and any graph.bin saved below)
    // carries only the contracted structure. Errors if the loaded graph.bin has no
    // contracted graph (rebuild required).
    if let Err(e) = maas_rs::services::build::finalize_contraction(&mut g) {
        tracing::error!("{e}");
        return;
    }

    if save_mode && (build_mode || update_gtfs_mode) {
        if let Err(e) = save_graph(&g, &config.build.output) {
            tracing::error!("{e}");
            return;
        }
        if let Err(e) = save_last_checked(&cache_dir, Local::now()) {
            tracing::warn!("failed to persist last_checked: {e}");
        }
    }

    if !auto && !serve_mode {
        return;
    }

    let shared: maas_rs::services::scheduler::SharedGraph = Arc::new(ArcSwap::from_pointee(g));
    let config = Arc::new(config);
    let _ = app::server(shared, config).await;
}

/// Restore the cached graph if its version matches and the configured GTFS inputs
/// are unchanged, else rebuild reusing osm.bin when possible.
/// Returns `None` on a fatal build error.
fn acquire_auto(config: &Config, cache_dir: &str) -> Option<maas_rs::structures::Graph> {
    let current_labels = gtfs_input_labels(&config.build);

    match load_graph(&config.build.output) {
        Ok(mut g) => {
            let stored_labels = load_input_labels(cache_dir);
            if stored_labels == current_labels {
                // Apply defaults + finalize here so a cached graph.bin with no contracted
                // graph self-heals (rebuild) instead of serving broken; the caller
                // re-applies/finalizes idempotently.
                maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing);
                match maas_rs::services::build::finalize_contraction(&mut g) {
                    Ok(()) => return Some(g),
                    Err(e) => tracing::info!("cached graph unusable ({e}); rebuilding"),
                }
            } else {
                tracing::info!(
                    "GTFS inputs changed (was {:?}, now {:?}), rebuilding GTFS phase",
                    stored_labels,
                    current_labels,
                );
            }
        }
        Err(e) => tracing::info!("rebuilding graph ({e})"),
    }

    let osm = match load_osm_graph(&config.build.osm_output) {
        Ok(o) => {
            tracing::info!("reusing cached OSM network");
            o
        }
        Err(e) => {
            tracing::info!("rebuilding OSM network ({e})");
            let o = build_osm_phase(&config.build, cache_dir, false)?;
            if let Err(e) = save_osm_graph(&o, &config.build.osm_output) {
                tracing::error!("{e}");
            }
            o
        }
    };

    let mut g = build_gtfs_phase(osm, &config.build, cache_dir, false)?;
    // Apply routing defaults before saving so persisted artifacts (e.g. the contracted
    // `g.contracted`) land in graph.bin. The caller re-applies defaults (idempotent)
    // after this returns.
    maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing);
    // Drop interior arrays so the saved graph.bin is the contracted form.
    if let Err(e) = maas_rs::services::build::finalize_contraction(&mut g) {
        tracing::error!("{e}");
        return None;
    }
    if let Err(e) = save_graph_with_rollback(&g, &config.build.output) {
        tracing::error!("{e}");
    }
    if let Err(e) = save_last_checked(cache_dir, Local::now()) {
        tracing::warn!("failed to persist last_checked: {e}");
    }
    if let Err(e) = save_input_labels(cache_dir, &current_labels) {
        tracing::warn!("failed to persist input_labels: {e}");
    }
    Some(g)
}
