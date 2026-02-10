use std::time::SystemTime;

use otpand::{
    ingestion::{gtfs::load_gtfs, osm},
    structures::Graph,
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
                    g.a_star(*a_id, *b_id);
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
