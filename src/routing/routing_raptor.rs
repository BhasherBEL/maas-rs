use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::ingestion::gtfs::date_to_days;
use crate::structures::Graph;
use crate::structures::plan::Plan;

pub struct RouteQuery {
    pub from_lat: f64,
    pub from_lng: f64,
    pub to_lat: f64,
    pub to_lng: f64,
    pub date: NaiveDate,
    pub time: NaiveTime,
    /// When `> 0`, run Range-RAPTOR over this window (seconds).
    pub window_minutes: Option<u32>,
    /// Per-query override for the minimum walk-radius used for access/egress
    /// stop discovery (seconds).  `None` → use the graph's configured default.
    pub min_access_secs: Option<u32>,
}

pub fn route(graph: &Graph, query: &RouteQuery) -> Result<Vec<Plan>, async_graphql::Error> {
    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    let origin = graph
        .nearest_node(query.from_lat, query.from_lng)
        .ok_or_else(|| async_graphql::Error::new("No node near departure"))?;

    let destination = graph
        .nearest_node(query.to_lat, query.to_lng)
        .ok_or_else(|| async_graphql::Error::new("No node near arrival"))?;

    let min_access = query.min_access_secs.unwrap_or(graph.min_access_secs);

    let plans = match query.window_minutes {
        Some(w) if w > 0 => {
            graph.raptor_range(origin, destination, time, w * 60, date, weekday, min_access)
        }
        _ => graph.raptor(origin, destination, time, date, weekday, min_access),
    };

    if plans.is_empty() {
        return Err(async_graphql::Error::new("No plan found"));
    }

    Ok(plans)
}
