use std::time::SystemTime;

use chrono::NaiveDate;

use crate::ingestion::gtfs::date_to_days;
use crate::structures::{Graph, RoutingParameters};

pub struct RouteQuery {
    pub from_lat: f64,
    pub from_lng: f64,
    pub to_lat: f64,
    pub to_lng: f64,
}

pub fn route(graph: &Graph, query: &RouteQuery) {
    let (_, a_id) = match graph.nearest_node_dist(query.from_lat, query.from_lng) {
        Some((a_dist, a_id)) => {
            println!(
                "Nearest node a: {} at {:.2}m (geo: {})",
                a_id.0,
                a_dist,
                graph.get_node(*a_id).unwrap().loc()
            );
            (a_dist, a_id)
        }
        None => {
            println!("No close node found");
            return;
        }
    };

    let (_, b_id) = match graph.nearest_node_dist(query.to_lat, query.to_lng) {
        Some((b_dist, b_id)) => {
            println!(
                "Nearest node b: {} at {:.2}m (geo: {})",
                b_id.0,
                b_dist,
                graph.get_node(*b_id).unwrap().loc()
            );
            (b_dist, b_id)
        }
        None => {
            println!("No close node found");
            return;
        }
    };

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

    graph.a_star(from, to, time, date, weekday, params);
    match before.elapsed() {
        Ok(elapsed) => println!("Ran in {}ms", elapsed.as_millis()),
        Err(e) => println!("Went backward ?? {}", e),
    }
}
