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
}

pub fn route(graph: &Graph, query: &RouteQuery) -> Result<Vec<Plan>, async_graphql::Error> {
    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    let sources: Vec<(usize, u32)> = graph
        .nearby_stops(query.from_lat, query.from_lng)
        .into_iter()
        .map(|(stop, walk)| (stop, time + walk))
        .collect();

    if sources.is_empty() {
        return Err(async_graphql::Error::new("No transit stop near departure"));
    }

    let targets = graph.nearby_stops(query.to_lat, query.to_lng);

    if targets.is_empty() {
        return Err(async_graphql::Error::new("No transit stop near arrival"));
    }

    let plans = graph.raptor(&sources, &targets, time, date, weekday);

    if plans.is_empty() {
        return Err(async_graphql::Error::new("No plan found"));
    }

    Ok(plans)
}
