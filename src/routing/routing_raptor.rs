use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::ingestion::gtfs::date_to_days;
use crate::structures::{
    ActiveModes, Graph, Mode, RealtimeIndex, ReliabilityBuckets, valid_reliability_edges,
};
use crate::structures::plan::{ExplainResult, Plan};

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
    /// Per-query override for the arrival-slack (seconds). `None` → graph default.
    pub arrival_slack_secs: Option<u32>,
    /// Per-query override for reliability bucket edges. `None`/invalid → graph default.
    pub reliability_bucket_edges: Option<Vec<f32>>,
    /// Travel modes the router may use. `None` → `[WALK, WALK_TRANSIT]`
    /// (the historical behavior). Empty is rejected.
    pub modes: Option<Vec<Mode>>,
    /// Per-query bike cost profile. `None` → the graph's configured default.
    pub bike_profile: Option<crate::structures::BikeProfile>,
}

/// Effective bike cost profile for a query: the per-request override if present,
/// else the graph's configured default.
fn resolve_bike_profile(graph: &Graph, query: &RouteQuery) -> crate::structures::BikeProfile {
    query.bike_profile.unwrap_or(graph.raptor.bike_profile)
}

/// Resolves the effective buckets + slack for a query, honouring per-request overrides
/// (validated) and falling back to the graph's configured defaults.
fn resolve_tuning(
    graph: &Graph,
    query: &RouteQuery,
) -> Result<(ReliabilityBuckets, u32), async_graphql::Error> {
    let buckets = match &query.reliability_bucket_edges {
        Some(edges) if !valid_reliability_edges(edges) => {
            return Err(async_graphql::Error::new(
                "reliabilityBucketEdges must be sorted, strictly increasing, each in (0,1)",
            ));
        }
        Some(edges) => ReliabilityBuckets::new(edges),
        None => ReliabilityBuckets::new(&graph.raptor.reliability_bucket_edges),
    };
    let slack = query.arrival_slack_secs.unwrap_or(graph.raptor.arrival_slack_secs);
    Ok((buckets, slack))
}

/// Resolves the mode selection, rejecting an explicitly empty list.
fn resolve_modes(query: &RouteQuery) -> Result<ActiveModes, async_graphql::Error> {
    match &query.modes {
        None => Ok(ActiveModes::default()),
        Some(m) if m.is_empty() => {
            Err(async_graphql::Error::new("modes must not be empty"))
        }
        Some(m) => Ok(ActiveModes::new(m)),
    }
}

/// Range-RAPTOR window in seconds, clamped to the configured maximum.
fn effective_window_secs(window_minutes: u32, max_window_secs: u32) -> u32 {
    window_minutes.saturating_mul(60).min(max_window_secs)
}

/// Snap a query coordinate to the street network, rejecting coordinates that
/// land farther than the configured snap-distance guard.
fn snap_node(
    graph: &Graph,
    lat: f64,
    lng: f64,
    endpoint: &str,
) -> Result<crate::structures::NodeID, async_graphql::Error> {
    let (dist_m, node) = graph
        .nearest_node_dist(lat, lng)
        .ok_or_else(|| async_graphql::Error::new(format!("No node near {endpoint}")))?;
    let max = graph.raptor.max_snap_distance_m;
    if dist_m > max as f64 {
        return Err(async_graphql::Error::new(format!(
            "{endpoint} is too far from the network (nearest node {:.0} m away, max {} m)",
            dist_m, max
        )));
    }
    Ok(*node)
}

fn resolve_query_params(
    graph: &Graph,
    query: &RouteQuery,
) -> Result<(crate::structures::NodeID, crate::structures::NodeID, u32, u32, u8, u32), async_graphql::Error> {
    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    let origin = snap_node(graph, query.from_lat, query.from_lng, "departure")?;
    let destination = snap_node(graph, query.to_lat, query.to_lng, "arrival")?;

    let min_access = query.min_access_secs.unwrap_or(graph.raptor.min_access_secs);

    Ok((origin, destination, time, date, weekday, min_access))
}

pub fn route(
    graph: &Graph,
    query: &RouteQuery,
    rt: &RealtimeIndex,
) -> Result<Vec<Plan>, async_graphql::Error> {
    let (origin, destination, time, date, weekday, min_access) =
        resolve_query_params(graph, query)?;
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let am = resolve_modes(query)?;

    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query), graph.raptor.walking_speed_mps);
    let plans = match query.window_minutes {
        Some(w) if w > 0 => {
            let window = effective_window_secs(w, graph.raptor.max_window_secs);
            graph.raptor_range_tuned_rt_overnight_modes(origin, destination, time, window, date, weekday, min_access, &buckets, slack, rt, &am, &bike)
        }
        _ => graph.raptor_tuned_rt_overnight_modes(origin, destination, time, date, weekday, min_access, &buckets, slack, rt, &am, &bike),
    };

    if plans.is_empty() {
        return Err(async_graphql::Error::new("No plan found"));
    }

    Ok(plans)
}

/// Like `route`, but returns all intermediate candidates and access metadata.
/// Does NOT return an error for empty results — an empty result is itself a debug signal.
pub fn route_explain(
    graph: &Graph,
    query: &RouteQuery,
    rt: &RealtimeIndex,
) -> Result<ExplainResult, async_graphql::Error> {
    let (origin, destination, time, date, weekday, min_access) =
        resolve_query_params(graph, query)?;
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let am = resolve_modes(query)?;

    // Note: the explain path does not apply the overnight pass — it's a debug view
    // of a single RAPTOR run and overnight merging would complicate candidate provenance.
    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query), graph.raptor.walking_speed_mps);
    let result = match query.window_minutes {
        Some(w) if w > 0 => {
            let window = effective_window_secs(w, graph.raptor.max_window_secs);
            graph.raptor_range_explain_tuned_rt_modes(origin, destination, time, window, date, weekday, min_access, &buckets, slack, rt, &am, &bike)
        }
        _ => graph.raptor_explain_tuned_rt_modes(origin, destination, time, date, weekday, min_access, &buckets, slack, rt, &am, &bike),
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{LatLng, NodeData, OsmNodeData};

    fn graph_with_node_at(lat: f64, lon: f64) -> Graph {
        let mut g = Graph::new();
        g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "n1".to_string(),
            lat_lng: LatLng { latitude: lat, longitude: lon },
        }));
        g.build_raptor_index();
        g
    }

    fn query(from_lat: f64, from_lng: f64, to_lat: f64, to_lng: f64) -> RouteQuery {
        RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            reliability_bucket_edges: None,
            modes: None,
            bike_profile: None,
        }
    }

    #[test]
    fn effective_window_secs_clamps_to_max() {
        assert_eq!(effective_window_secs(30, 86_400), 1_800);
        assert_eq!(effective_window_secs(10_000, 86_400), 86_400);
        assert_eq!(effective_window_secs(1_440, 86_400), 86_400);
    }

    #[test]
    fn route_rejects_origin_snapping_too_far() {
        let graph = graph_with_node_at(50.85, 4.35);
        let q = query(48.85, 2.35, 50.85, 4.35);
        let err = route(&graph, &q, &RealtimeIndex::new()).unwrap_err();
        assert!(
            err.message.to_lowercase().contains("too far"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn route_rejects_destination_snapping_too_far() {
        let graph = graph_with_node_at(50.85, 4.35);
        let q = query(50.85, 4.35, 48.85, 2.35);
        let err = route(&graph, &q, &RealtimeIndex::new()).unwrap_err();
        assert!(
            err.message.to_lowercase().contains("too far"),
            "unexpected error: {}",
            err.message
        );
    }

    #[test]
    fn route_accepts_origin_within_snap_distance() {
        let graph = graph_with_node_at(50.85, 4.35);
        let q = query(50.851, 4.351, 50.85, 4.35);
        let res = route(&graph, &q, &RealtimeIndex::new());
        if let Err(e) = res {
            assert!(
                !e.message.to_lowercase().contains("too far"),
                "snap guard fired within range: {}",
                e.message
            );
        }
    }
}
