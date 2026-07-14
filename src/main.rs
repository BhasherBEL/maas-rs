use std::{env, process::ExitCode, sync::Arc};

use arc_swap::ArcSwap;
use chrono::Local;
use maas_rs::{
    cli::parse_config_path,
    ingestion::cache::save_last_checked,
    logging,
    services::{
        build::{build_gtfs_phase, build_osm_phase},
        fingerprint::{graph_fingerprint, osm_fingerprint},
        persistence::{
            load_osm_graph, save_graph, save_graph_with_rollback, save_osm_graph,
        },
        rebuild::plan_rebuild,
    },
    structures::Config,
    web::app,
};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();

    let config_path = match parse_config_path(&args) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config '{config_path}': {e}");
            return ExitCode::FAILURE;
        }
    };

    logging::init(&config.log_level);

    let cache_dir = config.cache_dir();

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
        return ExitCode::FAILURE;
    }
    if save_mode && restore_mode {
        tracing::error!("--save requires --build or --update-gtfs");
        return ExitCode::FAILURE;
    }

    let auto = mode_count == 0;

    let mut g = if auto {
        match acquire_auto(&config, &cache_dir) {
            Some(g) => g,
            None => return ExitCode::FAILURE,
        }
    } else if build_mode {
        let osm_graph = match build_osm_phase(&config.build, &cache_dir, false) {
            Some(g) => g,
            None => {
                tracing::error!("OSM phase failed");
                return ExitCode::FAILURE;
            }
        };

        if save_mode {
            let osm_fp = osm_fingerprint(&config, &cache_dir);
            if let Err(e) = save_osm_graph(&osm_graph, &osm_fp, &config.build.osm_output) {
                tracing::error!("{e}");
                return ExitCode::FAILURE;
            }
        }

        match build_gtfs_phase(
            osm_graph,
            &config.build,
            &cache_dir,
            false,
            config.default_routing.station_merge_radius_m,
            &config.default_routing,
        ) {
            Some(g) => g,
            None => {
                tracing::error!("GTFS phase failed");
                return ExitCode::FAILURE;
            }
        }
    } else if update_gtfs_mode {
        let osm_fp = osm_fingerprint(&config, &cache_dir);
        let osm_graph = match load_osm_graph(&config.build.osm_output, &osm_fp) {
            Ok(g) => g,
            Err(e) => {
                tracing::error!(
                    "'{}' unusable ({e}) — run '--build --save' first",
                    config.build.osm_output
                );
                return ExitCode::FAILURE;
            }
        };

        match build_gtfs_phase(
            osm_graph,
            &config.build,
            &cache_dir,
            true,
            config.default_routing.station_merge_radius_m,
            &config.default_routing,
        ) {
            Some(g) => g,
            None => {
                tracing::error!("GTFS phase failed");
                return ExitCode::FAILURE;
            }
        }
    } else {
        let graph_fp = graph_fingerprint(&config, &cache_dir);
        match maas_rs::services::persistence::load_graph(&config.build.output, &graph_fp) {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("{e}");
                return ExitCode::FAILURE;
            }
        }
    };

    // Apply config.yaml routing defaults (works for all modes). Must run BEFORE the save
    // below so any persisted artifact it builds (e.g. the contracted `g.contracted`)
    // is written into graph.bin rather than rebuilt in RAM on every restore.
    maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing, &config.build.output);

    // Drop the interior-node arrays so the served graph (and any graph.bin saved below)
    // carries only the contracted structure. Errors if the loaded graph.bin has no
    // contracted graph (rebuild required).
    if let Err(e) = maas_rs::services::build::finalize_contraction(&mut g) {
        tracing::error!("{e}");
        return ExitCode::FAILURE;
    }

    if save_mode && (build_mode || update_gtfs_mode) {
        let graph_fp = graph_fingerprint(&config, &cache_dir);
        if let Err(e) = save_graph(&g, &graph_fp, &config.build.output) {
            tracing::error!("{e}");
            return ExitCode::FAILURE;
        }
        if let Err(e) = save_last_checked(&cache_dir, Local::now()) {
            tracing::warn!("failed to persist last_checked: {e}");
        }
    }

    if !auto && !serve_mode {
        return ExitCode::SUCCESS;
    }

    let shared: maas_rs::services::scheduler::SharedGraph = Arc::new(ArcSwap::from_pointee(g));
    let config = Arc::new(config);
    if let Err(e) = app::server(shared, config).await {
        tracing::error!("server failed: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Restore the cached graph if its schema version + input/param fingerprint both
/// match, else rebuild granularly: reuse osm.bin when only the transit inputs/params
/// changed (rebuild just the GTFS phase), or rebuild osm too when OSM/DEM/osm-params
/// changed. Decisions come from the dependency-aware [`plan_rebuild`]; the cascade
/// (OSM change → graph invalid) is automatic because graph_fp embeds osm_fp.
/// Returns `None` on a fatal build error.
fn acquire_auto(config: &Config, cache_dir: &str) -> Option<maas_rs::structures::Graph> {
    let plan = plan_rebuild(config, cache_dir);

    if plan.graph_valid {
        match maas_rs::services::persistence::load_graph(&config.build.output, &plan.graph_fp) {
            Ok(mut g) => {
                // Apply defaults + finalize here so a cached graph.bin with no contracted
                // graph self-heals (rebuild) instead of serving broken; the caller
                // re-applies/finalizes idempotently.
                maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing, &config.build.output);
                match maas_rs::services::build::finalize_contraction(&mut g) {
                    Ok(()) => return Some(g),
                    Err(e) => tracing::info!("cached graph unusable ({e}); rebuilding"),
                }
            }
            Err(e) => tracing::info!("rebuilding graph ({e})"),
        }
    } else {
        tracing::info!("graph.bin inputs/params changed; rebuilding GTFS phase");
    }

    let osm = if plan.osm_valid {
        match load_osm_graph(&config.build.osm_output, &plan.osm_fp) {
            Ok(o) => {
                tracing::info!("reusing cached OSM network");
                o
            }
            Err(e) => rebuild_osm(config, cache_dir, &plan.osm_fp, &e.0)?,
        }
    } else {
        rebuild_osm(config, cache_dir, &plan.osm_fp, "OSM inputs/params changed")?
    };

    let mut g = build_gtfs_phase(
        osm,
        &config.build,
        cache_dir,
        false,
        config.default_routing.station_merge_radius_m,
        &config.default_routing,
    )?;
    // Apply routing defaults before saving so persisted artifacts (e.g. the contracted
    // `g.contracted`) land in graph.bin. The caller re-applies defaults (idempotent)
    // after this returns.
    maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing, &config.build.output);
    // Drop interior arrays so the saved graph.bin is the contracted form.
    if let Err(e) = maas_rs::services::build::finalize_contraction(&mut g) {
        tracing::error!("{e}");
        return None;
    }
    if let Err(e) = save_graph_with_rollback(&g, &plan.graph_fp, &config.build.output) {
        tracing::error!("{e}");
    }
    if let Err(e) = save_last_checked(cache_dir, Local::now()) {
        tracing::warn!("failed to persist last_checked: {e}");
    }
    Some(g)
}

/// Rebuild the OSM network from scratch and persist it under `osm_fp`.
fn rebuild_osm(
    config: &Config,
    cache_dir: &str,
    osm_fp: &maas_rs::services::persistence::Fingerprint,
    reason: &str,
) -> Option<maas_rs::structures::Graph> {
    tracing::info!("rebuilding OSM network ({reason})");
    let o = build_osm_phase(&config.build, cache_dir, false)?;
    if let Err(e) = save_osm_graph(&o, osm_fp, &config.build.osm_output) {
        tracing::error!("{e}");
    }
    Some(o)
}
