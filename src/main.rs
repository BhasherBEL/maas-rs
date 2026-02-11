use std::{env, time::SystemTime};

use chrono::NaiveDate;
use otpand::{
    ingestion::{
        gtfs::{date_to_days, load_gtfs},
        osm,
    },
    structures::{Graph, RoutingParameters},
};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        eprintln!("Usage: {} <from_lat> <from_lng> <to_lat> <to_lng>", args[0]);
        return;
    }

    let from_lat: f64 = args[1].parse().expect("Invalid from_lat");
    let from_lng: f64 = args[2].parse().expect("Invalid from_lng");
    let to_lat: f64 = args[3].parse().expect("Invalid to_lat");
    let to_lng: f64 = args[4].parse().expect("Invalid to_lng");

    let mut g = Graph::new();

    let before = SystemTime::now();
    match osm::load_pbf_file("data/brussels_capital_region-2026_01_24.osm.pbf", &mut g) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Failed to read file: {e}");
            return;
        }
    }
    match before.elapsed() {
        Ok(elapsed) => println!("Data loaded in in {}ms", elapsed.as_millis()),
        Err(e) => println!("Went backward ?? {}", e),
    }

    match load_gtfs("data/stib.zip", &mut g) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Faield to read GTFS: {}", e);
            return;
        }
    }

    match g.nearest_node_dist(from_lat, from_lng) {
        Some((a_dist, a_id)) => {
            println!(
                "Nearest node a: {} at {:.2}m (geo: {})",
                a_id.0,
                a_dist,
                g.get_node(*a_id).unwrap().loc()
            );
            match g.nearest_node_dist(to_lat, to_lng) {
                Some((b_dist, b_id)) => {
                    println!(
                        "Nearest node b: {} at {:.2}m (geo: {})",
                        b_id.0,
                        b_dist,
                        g.get_node(*b_id).unwrap().loc()
                    );
                    let before = SystemTime::now();

                    let from = *a_id;
                    let to = *b_id;
                    let time = 60 * 60 * 12;
                    let date = date_to_days(NaiveDate::from_ymd_opt(2026, 2, 10).unwrap());
                    let weekday = 1 << 2;
                    let params = RoutingParameters {
                        walking_speed: 5 * 278,
                        estimator_speed: 50 * 278,
                    };

                    g.a_star(from, to, time, date, weekday, params);
                    match before.elapsed() {
                        Ok(elapsed) => println!("Ran in {}ms", elapsed.as_millis()),
                        Err(e) => println!("Went backward ?? {}", e),
                    }
                }
                None => println!("No close node found"),
            }
        }
        None => println!("No close node found"),
    }
}
