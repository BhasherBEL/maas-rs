use std::{env, sync::Arc};

use maas_rs::{
    logging,
    services::{
        build::build_graph,
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

    if build_mode && restore_mode || !build_mode && !restore_mode {
        tracing::error!("one of --build or --restore must be set (not both)");
        return;
    }
    if save_mode && !build_mode {
        tracing::error!("--save requires --build");
        return;
    }

    let mut g = if build_mode {
        let graph = match build_graph(config.build) {
            Some(g) => g,
            None => {
                tracing::error!("graph build failed");
                return;
            }
        };

        if save_mode {
            if let Err(e) = save_graph(&graph, "graph.bin") {
                tracing::error!("{e}");
                return;
            }
        }

        graph
    } else {
        match load_graph("graph.bin") {
            Ok(g) => g,
            Err(e) => {
                tracing::error!("{e}");
                return;
            }
        }
    };

    // Apply config.yaml routing defaults (works for both --build and --restore).
    if let Some(s) = config.default_routing.min_access_secs {
        g.set_min_access_secs(s);
    }

    if !serve_mode {
        return;
    }

    let _ = app::server(Arc::new(g)).await;
}
