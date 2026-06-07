use std::{env, sync::Arc};

use arc_swap::ArcSwap;
use maas_rs::{
    logging,
    services::{
        build::{build_gtfs_phase, build_osm_phase},
        persistence::{load_graph, save_graph},
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
    if mode_count != 1 {
        tracing::error!("exactly one of --build, --restore, or --update-gtfs must be set");
        return;
    }
    if save_mode && restore_mode {
        tracing::error!("--save requires --build or --update-gtfs");
        return;
    }

    let mut g = if build_mode {
        let osm_graph = match build_osm_phase(&config.build, &cache_dir, false) {
            Some(g) => g,
            None => {
                tracing::error!("OSM phase failed");
                return;
            }
        };

        if save_mode {
            if let Err(e) = save_graph(&osm_graph, &config.build.osm_output) {
                tracing::error!("{e}");
                return;
            }
        }

        match build_gtfs_phase(osm_graph, &config.build, &cache_dir, false) {
            Some(g) => g,
            None => {
                tracing::error!("GTFS phase failed");
                return;
            }
        }
    } else if update_gtfs_mode {
        let osm_graph = match load_graph(&config.build.osm_output) {
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

    if save_mode && (build_mode || update_gtfs_mode) {
        if let Err(e) = save_graph(&g, &config.build.output) {
            tracing::error!("{e}");
            return;
        }
    }

    // Apply config.yaml routing defaults (works for all modes).
    maas_rs::services::build::apply_routing_defaults(&mut g, &config.default_routing);

    if !serve_mode {
        return;
    }

    let shared: maas_rs::services::scheduler::SharedGraph = Arc::new(ArcSwap::from_pointee(g));
    let config = Arc::new(config);
    let _ = app::server(shared, config).await;
}
