use otpand::{ingestion::osm, structures::Graph};

fn main() {
    read();
}

fn read() {
    let mut g = Graph::new();

    println!("Graph created");

    match osm::load_pbf_file("data/brussels_capital_region-2026_01_24.osm.pbf", &mut g) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Failed to read file: {e}");
            return;
        }
    }

    println!("Data loaded");

    println!("Nodes: {}", g.node_count());
}
