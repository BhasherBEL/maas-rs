use std::{env, sync::Arc};

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
        let osm_graph = match build_osm_phase(&config.build) {
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

        match build_gtfs_phase(osm_graph, &config.build) {
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

        match build_gtfs_phase(osm_graph, &config.build) {
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
    if let Some(s) = config.default_routing.min_access_secs {
        g.set_min_access_secs(s);
    }
    if let Some(v) = config.default_routing.walking_speed_mps {
        g.set_walking_speed_mps(v);
    }

    if !serve_mode {
        return;
    }

    let _ = app::server(Arc::new(g), &config.server).await;
}
