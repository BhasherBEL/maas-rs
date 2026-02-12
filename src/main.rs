use std::env;

use otpand::{
    routing::routing::{RouteQuery, route},
    services::{
        build::build_graph,
        persistence::{load_graph, save_graph},
    },
    structures::Config,
};

fn main() {
    let config = match Config::load("config.yaml") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return;
        }
    };

    let args: Vec<String> = env::args().collect();

    let build_mode = args.contains(&"--build".to_string());
    let save_mode = args.contains(&"--save".to_string());
    let restore_mode = args.contains(&"--restore".to_string());
    let serve_mode = args.contains(&"--serve".to_string());

    if build_mode && restore_mode || !build_mode && !restore_mode {
        println!("One of --build or --restore must be enabled. Only one of them can be used");
        return;
    }
    if save_mode && !build_mode {
        println!("--save cannot be used without --build");
        return;
    }
    if serve_mode && args.len() < 6 {
        eprintln!(
            "Usage: {} <from_lat> <from_lng> <to_lat> <to_lng> --serve",
            args[0]
        );
        return;
    }

    let g = if build_mode {
        let graph = match build_graph(config.build) {
            Some(g) => g,
            None => {
                println!("Failed to build graph");
                return;
            }
        };

        if save_mode {
            if let Err(e) = save_graph(&graph, "graph.bin") {
                println!("{e}");
                return;
            }
        }

        graph
    } else {
        match load_graph("graph.bin") {
            Ok(g) => g,
            Err(e) => {
                println!("{e}");
                return;
            }
        }
    };

    if !serve_mode {
        return;
    }

    let from_lat: f64 = args[1].parse().expect("Invalid from_lat");
    let from_lng: f64 = args[2].parse().expect("Invalid from_lng");
    let to_lat: f64 = args[3].parse().expect("Invalid to_lat");
    let to_lng: f64 = args[4].parse().expect("Invalid to_lng");

    route(
        &g,
        &RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
        },
    );
}
