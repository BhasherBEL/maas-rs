use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::ingestion::gtfs::date_to_days;
use crate::structures::plan::Plan;
use crate::structures::{Graph, RoutingParameters};

pub struct RouteQuery {
    pub from_lat: f64,
    pub from_lng: f64,
    pub to_lat: f64,
    pub to_lng: f64,
    pub date: NaiveDate,
    pub time: NaiveTime,
}

pub fn route(
    graph: &Graph,
    query: &RouteQuery,
    params: RoutingParameters,
) -> Result<Plan, async_graphql::Error> {
    let (_, a_id) = graph
        .nearest_node_dist(query.from_lat, query.from_lng)
        .ok_or_else(|| async_graphql::Error::new("No node near departure"))?;

    let (_, b_id) = graph
        .nearest_node_dist(query.to_lat, query.to_lng)
        .ok_or_else(|| async_graphql::Error::new("No node near arrival"))?;

    let from = *a_id;
    let to = *b_id;

    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    graph.a_star(from, to, time, date, weekday, params)
}
