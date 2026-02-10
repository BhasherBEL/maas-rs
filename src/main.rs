use std::time::SystemTime;

use chrono::NaiveDate;
use otpand::{
    ingestion::{
        gtfs::{date_to_days, load_gtfs},
        osm,
    },
    structures::{Graph, RoutingParameters},
};

fn main() {
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

    match g.nearest_node_dist(0.0, 0.0) {
        Some((a_dist, a_id)) => {
            println!(
                "Nearest node a: {} at {:.2}m (geo: {})",
                a_id.0,
                a_dist,
                g.get_node(*a_id).unwrap().loc()
            );
            match g.nearest_node_dist(0.0, 0.0) {
                // match g.nearest_node_dist(0.0, 0.0) {
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
                        walking_speed: 4 * 278,
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
