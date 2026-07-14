use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::ingestion::gtfs::date_to_days;
use crate::structures::plan::{ExplainResult, Plan};
use crate::structures::{
    ActiveModes, Graph, Mode, RealtimeIndex, ReliabilityBuckets,
    valid_reliability_edges,
};

pub struct RouteQuery {
    pub from_lat: f64,
    pub from_lng: f64,
    pub to_lat: f64,
    pub to_lng: f64,
    pub date: NaiveDate,
    pub time: NaiveTime,
    pub window_minutes: Option<u32>,
    pub min_access_secs: Option<u32>,
    pub arrival_slack_secs: Option<u32>,
    pub unrestricted_transfers: Option<bool>,
    pub use_cch_access: Option<bool>,
    pub reliability_bucket_edges: Option<Vec<f32>>,
    pub modes: Option<Vec<Mode>>,
    pub bike_profile: Option<crate::structures::BikeProfile>,
    pub terminal_deadline: bool,
    pub onboard_origin: Option<OnboardOrigin>,
    pub from_station_id: Option<String>,
    pub to_station_id: Option<String>,
    pub profile_latency: Option<bool>,
    pub fare_profile: Option<FareProfile>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FareProfile {
    pub category: crate::structures::cost::PassengerCategory,
    pub stib_subscription: bool,
    pub delijn_subscription: bool,
    pub tec_subscription: bool,
    pub sncb_subscription: bool,
    pub sncb_train_plus: bool,
    pub delijn_10_journey: bool,
    pub tec_6_journey: bool,
    pub travel_class: crate::structures::cost::TravelClass,
}

impl FareProfile {
    pub fn to_cost(&self) -> crate::structures::cost::FareProfile {
        crate::structures::cost::FareProfile {
            category: self.category,
            stib_subscription: self.stib_subscription,
            delijn_subscription: self.delijn_subscription,
            tec_subscription: self.tec_subscription,
            sncb_subscription: self.sncb_subscription,
            sncb_train_plus: self.sncb_train_plus,
            delijn_10_journey: self.delijn_10_journey,
            tec_6_journey: self.tec_6_journey,
            travel_class: self.travel_class,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OnboardOrigin {
    pub trip_id: String,
    pub from_stop_id: Option<String>,
    pub from_stop_seq: Option<u32>,
}

fn resolve_bike_profile(graph: &Graph, query: &RouteQuery) -> crate::structures::BikeProfile {
    query.bike_profile.unwrap_or(graph.raptor.bike_profile)
}

fn resolve_fare_profile(query: &RouteQuery) -> crate::structures::cost::FareProfile {
    query
        .fare_profile
        .map(|p| p.to_cost())
        .unwrap_or_default()
}

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
    let slack = query
        .arrival_slack_secs
        .unwrap_or(graph.raptor.arrival_slack_secs);
    Ok((buckets, slack))
}

fn resolve_modes(query: &RouteQuery) -> Result<ActiveModes, async_graphql::Error> {
    match &query.modes {
        None => Ok(ActiveModes::default()),
        Some(m) if m.is_empty() => Err(async_graphql::Error::new("modes must not be empty")),
        Some(m) => Ok(ActiveModes::new(m)),
    }
}

fn effective_window_secs(window_minutes: u32, max_window_secs: u32) -> u32 {
    window_minutes.saturating_mul(60).min(max_window_secs)
}

fn arena_snap_node(
    graph: &Graph,
    lat: f64,
    lng: f64,
    endpoint: &str,
) -> Result<(crate::structures::NodeID, crate::structures::LatLng), async_graphql::Error> {
    let Some(cg) = graph.contracted.as_ref() else {
        return Err(async_graphql::Error::new(format!("No node near {endpoint}")));
    };
    let radius = graph.raptor.edge_snap_radius_m;
    let (proj, dist_m) = cg
        .arena_snap_proj(lat, lng, radius, |s| s.foot)
        .ok_or_else(|| async_graphql::Error::new(format!("No node near {endpoint}")))?;
    let max = graph.raptor.max_snap_distance_m;
    if dist_m > max as f64 {
        return Err(async_graphql::Error::new(format!(
            "{endpoint} is too far from the network (nearest node {:.0} m away, max {} m)",
            dist_m, max
        )));
    }
    let junction = cg
        .foot_bounding_junction(graph, lat, lng, radius)
        .ok_or_else(|| async_graphql::Error::new(format!("No node near {endpoint}")))?;
    Ok((junction, proj))
}

use crate::structures::QueryEndpoints;

fn resolve_endpoint(
    graph: &Graph,
    lat: f64,
    lng: f64,
    station_id: Option<&str>,
    endpoint: &str,
) -> Result<
    (
        crate::structures::NodeID,
        crate::structures::LatLng,
        Option<Vec<usize>>,
    ),
    async_graphql::Error,
> {
    if let Some(id) = station_id
        && let Some((coord, platforms)) = graph.station_endpoint(id)
        && let Ok((node, _snapped)) = arena_snap_node(graph, coord.latitude, coord.longitude, endpoint)
    {
        return Ok((node, coord, Some(platforms)));
    }
    let (node, coord) = arena_snap_node(graph, lat, lng, endpoint)?;
    Ok((node, coord, None))
}

fn resolve_query_params(
    graph: &Graph,
    query: &RouteQuery,
) -> Result<
    (
        crate::structures::NodeID,
        crate::structures::NodeID,
        u32,
        u32,
        u8,
        u32,
        Option<QueryEndpoints>,
    ),
    async_graphql::Error,
> {
    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    let (origin, destination, endpoints) = {
        let (o, o_coord, o_station) = resolve_endpoint(
            graph,
            query.from_lat,
            query.from_lng,
            query.from_station_id.as_deref(),
            "departure",
        )?;
        let (d, d_coord, d_station) = resolve_endpoint(
            graph,
            query.to_lat,
            query.to_lng,
            query.to_station_id.as_deref(),
            "arrival",
        )?;
        (
            o,
            d,
            Some(QueryEndpoints {
                origin: o_coord,
                destination: d_coord,
                origin_station: o_station,
                destination_station: d_station,
            }),
        )
    };

    let min_access = query
        .min_access_secs
        .unwrap_or(graph.raptor.min_access_secs);

    Ok((origin, destination, time, date, weekday, min_access, endpoints))
}

fn route_onboard(
    graph: &Graph,
    query: &RouteQuery,
    onboard: &OnboardOrigin,
    rt: &RealtimeIndex,
) -> Result<Vec<Plan>, async_graphql::Error> {
    let time = query.time.num_seconds_from_midnight();
    let date = date_to_days(query.date);
    let weekday = 1u8 << query.date.weekday().num_days_from_monday();

    let (destination, d_coord) = arena_snap_node(graph, query.to_lat, query.to_lng, "arrival")?;
    let ep = QueryEndpoints {
        origin: d_coord,
        destination: d_coord,
        origin_station: None,
        destination_station: None,
    };

    let trip = graph
        .trip_index_of(&onboard.trip_id)
        .ok_or_else(|| async_graphql::Error::new(format!("Unknown trip_id {}", onboard.trip_id)))?;
    let from_stop = match &onboard.from_stop_id {
        Some(sid) => Some(graph.stop_index_of(sid).ok_or_else(|| {
            async_graphql::Error::new(format!("Unknown from_stop_id {sid}"))
        })?),
        None => None,
    };

    let (pattern, trip_within, current_pos) = graph
        .locate_onboard_trip(trip, from_stop, onboard.from_stop_seq, time, rt)
        .ok_or_else(|| {
            async_graphql::Error::new("Could not locate the onboard position (no downstream stops)")
        })?;

    let ride = graph.build_onboard_ride(pattern, trip_within, current_pos, rt);

    let (buckets, slack) = resolve_tuning(graph, query)?;
    let unrestricted = query
        .unrestricted_transfers
        .unwrap_or(graph.raptor.unrestricted_transfers);
    let use_cch = query
        .use_cch_access
        .unwrap_or(graph.raptor.use_cch_access);
    let egress_secs = query
        .min_access_secs
        .unwrap_or(graph.raptor.min_access_secs);
    let am = ActiveModes::new(&[Mode::WalkTransit]);

    let mut plans = graph.raptor_onboard_tuned_rt_modes_ep(
        &ride,
        destination,
        date,
        weekday,
        egress_secs,
        &buckets,
        slack,
        rt,
        &am,
        unrestricted,
        use_cch,
        Some(&ep),
    );

    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query));
    graph.enrich_street_legs(&mut plans, destination, destination, &bike, query.terminal_deadline);

    if plans.is_empty() {
        return Err(async_graphql::Error::new("No plan found"));
    }
    Ok(plans)
}

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn brussels_service_days(now_unix: i64) -> u32 {
    use chrono::TimeZone;
    match chrono_tz::Europe::Brussels.timestamp_opt(now_unix, 0).single() {
        Some(dt) => date_to_days(dt.date_naive()),
        None => 0,
    }
}

/// Realtime has no service-date dimension, so it applies only when the query's
/// service date equals the Brussels service date of `now` (keyed on `now`, not
/// `generated_at`, to keep the post-midnight window live).
fn gate_realtime<'a>(
    rt: &'a RealtimeIndex,
    empty: &'a RealtimeIndex,
    query_date_days: u32,
    now_unix: i64,
) -> &'a RealtimeIndex {
    if rt.is_empty() {
        return rt;
    }
    if now_unix.saturating_sub(rt.generated_at) > rt.max_age_secs() {
        return empty;
    }
    if query_date_days != brussels_service_days(now_unix) {
        return empty;
    }
    rt
}

pub fn route(
    graph: &Graph,
    query: &RouteQuery,
    rt: &RealtimeIndex,
) -> Result<Vec<Plan>, async_graphql::Error> {
    let empty = RealtimeIndex::new();
    let rt = gate_realtime(rt, &empty, date_to_days(query.date), now_unix_secs());
    if let Some(onboard) = &query.onboard_origin {
        return route_onboard(graph, query, onboard, rt);
    }
    let (origin, destination, time, date, weekday, min_access, endpoints) =
        resolve_query_params(graph, query)?;
    let ep = endpoints.as_ref();
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let unrestricted = query
        .unrestricted_transfers
        .unwrap_or(graph.raptor.unrestricted_transfers);
    let use_cch = query
        .use_cch_access
        .unwrap_or(graph.raptor.use_cch_access);
    let am = resolve_modes(query)?;

    let profiling = query
        .profile_latency
        .unwrap_or(graph.raptor.profile_latency);
    let profile_start = crate::structures::latency_profile::begin_query(profiling);

    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query));
    let fare_profile = resolve_fare_profile(query);
    let mut plans = match query.window_minutes {
        Some(w) if w > 0 => {
            let window = effective_window_secs(w, graph.raptor.max_window_secs);
            graph.raptor_range_tuned_rt_overnight_modes(
                origin,
                destination,
                time,
                window,
                date,
                weekday,
                min_access,
                &buckets,
                slack,
                unrestricted,
                use_cch,
                rt,
                &am,
                &bike,
                ep,
                fare_profile,
            )
        }
        _ => graph.raptor_tuned_rt_overnight_modes(
            origin,
            destination,
            time,
            date,
            weekday,
            min_access,
            &buckets,
            slack,
            unrestricted,
            use_cch,
            rt,
            &am,
            &bike,
            ep,
            fare_profile,
        ),
    };

    graph.enrich_street_legs(
        &mut plans,
        origin,
        destination,
        &bike,
        query.terminal_deadline,
    );

    if let Some(profile) = crate::structures::latency_profile::end_query(profile_start) {
        tracing::info!(target: "latency_profile", "{}", profile.report());
    }

    if plans.is_empty() {
        return Err(async_graphql::Error::new("No plan found"));
    }

    Ok(plans)
}

/// Unlike `route`, does NOT error on empty results (empty is itself a debug signal).
pub fn route_explain(
    graph: &Graph,
    query: &RouteQuery,
    rt: &RealtimeIndex,
) -> Result<ExplainResult, async_graphql::Error> {
    let empty = RealtimeIndex::new();
    let rt = gate_realtime(rt, &empty, date_to_days(query.date), now_unix_secs());
    let (origin, destination, time, date, weekday, min_access, endpoints) =
        resolve_query_params(graph, query)?;
    let ep = endpoints.as_ref();
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let unrestricted = query
        .unrestricted_transfers
        .unwrap_or(graph.raptor.unrestricted_transfers);
    let use_cch = query
        .use_cch_access
        .unwrap_or(graph.raptor.use_cch_access);
    let am = resolve_modes(query)?;

    // The explain path deliberately skips the overnight pass (it would complicate
    // candidate provenance).
    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query));
    let fare_profile = resolve_fare_profile(query);
    let mut result = match query.window_minutes {
        Some(w) if w > 0 => {
            let window = effective_window_secs(w, graph.raptor.max_window_secs);
            graph.raptor_range_explain_tuned_rt_modes(
                origin,
                destination,
                time,
                window,
                date,
                weekday,
                min_access,
                &buckets,
                slack,
                unrestricted,
                use_cch,
                rt,
                &am,
                &bike,
                ep,
                fare_profile,
            )
        }
        _ => graph.raptor_explain_tuned_rt_modes(
            origin,
            destination,
            time,
            date,
            weekday,
            min_access,
            &buckets,
            slack,
            unrestricted,
            use_cch,
            rt,
            &am,
            &bike,
            ep,
            fare_profile,
        ),
    };

    graph.enrich_street_legs(
        &mut result.plans,
        origin,
        destination,
        &bike,
        query.terminal_deadline,
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{LatLng, NodeData, NodeID, OsmNodeData};

    fn enable_contraction(g: &mut Graph) {
        use crate::structures::contraction::ContractedGraph;
        let mut cg = ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted_default();
    }

    fn graph_with_node_at(lat: f64, lon: f64) -> Graph {
        let mut g = Graph::new();
        let n1 = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "n1".to_string(),
            lat_lng: LatLng {
                latitude: lat,
                longitude: lon,
            },
        }));
        // Second node so the contracted seg index has a segment (isolated node = empty index).
        let n2 = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "n2".to_string(),
            lat_lng: LatLng {
                latitude: lat + 0.0001,
                longitude: lon + 0.0001,
            },
        }));
        street(&mut g, n1, n2, 15, true, true);
        street(&mut g, n2, n1, 15, true, true);
        g.build_raptor_index();
        // Whole-world snap radius so a far query hits the segment and yields "too far"
        // (not "no node near"); the default 300 m would silence the guard under test.
        g.raptor.edge_snap_radius_m = f64::MAX;
        enable_contraction(&mut g);
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
            unrestricted_transfers: None,
            use_cch_access: None,
            reliability_bucket_edges: None,
            modes: None,
            bike_profile: None,
            terminal_deadline: false,
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: None,
        }
    }

    use crate::ingestion::gtfs::TripId;

    /// Non-empty snapshot (one delay) so it does NOT hit the `is_empty()`
    /// short-circuit; the staleness/date checks are actually exercised.
    fn rt_snapshot(gen_unix: i64, ttl: i64) -> RealtimeIndex {
        RealtimeIndex::from_updates(gen_unix, [((TripId(1), 0), 60)], [])
            .with_max_age_secs(ttl)
    }

    fn brussels_unix(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> i64 {
        use chrono::TimeZone;
        chrono_tz::Europe::Brussels
            .with_ymd_and_hms(y, m, d, hh, mm, 0)
            .unwrap()
            .timestamp()
    }

    #[test]
    fn gate_fresh_today_applies_realtime() {
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let rt = rt_snapshot(now - 10, 600);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(std::ptr::eq(got, &rt), "fresh+today must apply the live index");
    }

    #[test]
    fn gate_sticky_only_index_is_inert_for_routing() {
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let mut sticky = std::collections::HashMap::new();
        sticky.insert((TripId(1), 0), (120, now));
        let rt = RealtimeIndex::new()
            .with_max_age_secs(600)
            .with_sticky_delays(sticky);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(got.is_empty(), "sticky-only snapshot is empty for routing");
        assert_eq!(got.delay(TripId(1), 0), 0, "routing accessor never sees sticky");
        assert_eq!(rt.delay_with_sticky(TripId(1), 0), 120);
    }

    #[test]
    fn gate_stale_index_is_ignored() {
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let rt = rt_snapshot(now - 1000, 600); // 1000s old, TTL 600s
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(std::ptr::eq(got, &empty), "stale index must be ignored");
    }

    #[test]
    fn gate_future_date_query_is_ignored() {
        let now = now_unix_secs();
        let tomorrow = brussels_service_days(now) + 1;
        let rt = rt_snapshot(now - 10, 600);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, tomorrow, now);
        assert!(std::ptr::eq(got, &empty), "future-date query must ignore realtime");
    }

    #[test]
    fn gate_after_midnight_window_keeps_realtime() {
        // `now` = 00:05 on D+1, snapshot from 23:58 on D (fresh). Keying the date on
        // `now` keeps realtime live across midnight; a `generated_at`-based date would
        // wrongly drop it.
        let now = brussels_unix(2026, 1, 15, 0, 5); // winter → no DST ambiguity
        let gen_unix = brussels_unix(2026, 1, 14, 23, 58);
        let now_days = brussels_service_days(now);
        let gen_days = brussels_service_days(gen_unix);
        assert_eq!(now_days, gen_days + 1, "the instants must straddle midnight");
        assert!(now - gen_unix < 600, "snapshot must still be within TTL");

        let rt = rt_snapshot(gen_unix, 600);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, now_days, now);
        assert!(
            std::ptr::eq(got, &rt),
            "post-midnight same-day query must keep realtime"
        );
    }

    #[test]
    fn gate_empty_index_short_circuits_regardless_of_clock() {
        let empty_in = RealtimeIndex::new(); // gen=0, ttl=0
        let empty_sub = RealtimeIndex::new();
        let now = now_unix_secs();
        let wrong_date = brussels_service_days(now) + 5;
        let got = gate_realtime(&empty_in, &empty_sub, wrong_date, now);
        assert!(
            std::ptr::eq(got, &empty_in),
            "empty index short-circuits to itself (schedule-only, byte-identical)"
        );
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

    #[test]
    fn direct_walk_plan_carries_multiobj_alternatives() {
        use crate::structures::cost::VarGen;
        use crate::structures::plan::PlanLeg;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, Mode, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.0001));
        let c = g.add_node(mk("c", 50.00001, 4.00005));
        g.build_raptor_index();
        g.raptor.set_bike_select_dplus(true);
        g.set_distance_budget(f64::INFINITY);
        let e = |o, d, len, s| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = s;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: len,
                foot: true,
                bike: true,
                car: false,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, e(a, b, 100, Surface::Unpaved));
        g.add_edge(a, e(a, c, 90, Surface::Paved));
        g.add_edge(c, e(c, b, 90, Surface::Paved));
        // Back-edge makes b a proper junction so both forward paths are findable.
        g.add_edge(b, e(b, a, 100, Surface::Unpaved));
        enable_contraction(&mut g);
        let q = RouteQuery {
            from_lat: 50.000,
            from_lng: 4.000,
            to_lat: 50.000,
            to_lng: 4.0001,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            unrestricted_transfers: None,
            use_cch_access: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Walk]),
            bike_profile: None,
            terminal_deadline: false,
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: None,
        };
        let plans = route(&g, &q, &RealtimeIndex::new()).unwrap();
        let walk = plans
            .iter()
            .find(|p| p.mode == Mode::Walk)
            .expect("a walk plan");
        let PlanLeg::Walk(leg) = &walk.legs[0] else {
            panic!()
        };
        assert!(
            leg.alternatives.len() >= 2,
            "direct walk plan carries multiobj alternatives"
        );
    }

    #[test]
    fn direct_bike_plan_has_alternatives() {
        use crate::structures::cost::VarGen;
        use crate::structures::plan::PlanLeg;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, Mode, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.0001));
        let c = g.add_node(mk("c", 50.00001, 4.00005));
        g.build_raptor_index();
        g.raptor.set_bike_select_dplus(true);
        g.set_distance_budget(f64::INFINITY);
        let e = |o, d, len, elev: i16| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: len,
                foot: true,
                bike: true,
                car: false,
                attrs: at,
                elev_delta: elev,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        // Climb trade-off: short direct edge climbs, long flat detour avoids it. Both
        // survive the 3-axis front as a faster-hillier vs flatter-slower pair.
        g.add_edge(a, e(a, b, 100, 8));
        g.add_edge(a, e(a, c, 400, 0));
        g.add_edge(c, e(c, b, 400, 0));
        // Back-edge makes b a proper junction so both forward paths are findable.
        g.add_edge(b, e(b, a, 100, -8));
        enable_contraction(&mut g);
        let q = RouteQuery {
            from_lat: 50.000,
            from_lng: 4.000,
            to_lat: 50.000,
            to_lng: 4.0001,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            unrestricted_transfers: None,
            use_cch_access: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Bike]),
            bike_profile: None,
            terminal_deadline: false,
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: None,
        };
        let plans = route(&g, &q, &RealtimeIndex::new()).unwrap();
        let bike = plans
            .iter()
            .find(|p| p.mode == Mode::Bike)
            .expect("a bike plan");
        let PlanLeg::Walk(leg) = &bike.legs[0] else {
            panic!("expected a walk leg in a bike plan")
        };
        assert!(
            leg.alternatives.len() >= 2,
            "bike legs are enriched with route alternatives (Phase B)"
        );
        assert_eq!(leg.street_mode, Mode::Bike, "stays a bike leg");
    }

    /// Coord-routed drop gate: route from raw lat/lng with contraction on, DROP the
    /// interior-node arrays, re-route the SAME coordinates; the full plans (including
    /// geometry) must be BYTE-IDENTICAL, proving arena snapping is g-free.
    #[test]
    fn coord_routed_drop_gate_identical() {
        let g = coord_drop_gate_graph(true);
        let q = coord_drop_gate_query();
        let dbg = |ps: &[Plan]| ps.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>();
        let before = route(&g, &q, &RealtimeIndex::new()).expect("pre-drop plans");

        let mut g = g;
        g.drop_full_node_arrays();

        let after = route(&g, &q, &RealtimeIndex::new()).expect("post-drop plans must not error");
        assert_eq!(
            dbg(&before),
            dbg(&after),
            "coord-routed plans must be byte-identical pre/post drop (arena snapping)"
        );
    }

    /// Chain a — i1 — i2 — i3 — b: i1..i3 are degree-2 interior (contracted away).
    /// `contract` ⇒ build + bake the union contracted graph.
    fn coord_drop_gate_graph(contract: bool) -> Graph {
        use crate::structures::contraction::ContractedGraph;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        use crate::structures::cost::VarGen;

        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let ids: Vec<_> = ["a", "i1", "i2", "i3", "b"]
            .iter()
            .enumerate()
            .map(|(k, name)| g.add_node(mk(name, 50.000, 4.000 + 0.0010 * k as f64)))
            .collect();
        g.build_raptor_index();
        g.raptor.set_bike_select_dplus(true);
        g.set_distance_budget(f64::INFINITY);
        let edge = |o: crate::structures::NodeID, d: crate::structures::NodeID| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: 100,
                foot: true,
                bike: true,
                car: true,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        for w in ids.windows(2) {
            g.add_edge(w[0], edge(w[0], w[1]));
            g.add_edge(w[1], edge(w[1], w[0]));
        }

        if contract {
            let mut cg = ContractedGraph::from_graph_union(&g);
            cg.build_seg_index();
            g.contracted = Some(cg);
            g.bake_bike_on_contracted_default();
        }
        g
    }

    fn coord_drop_gate_query() -> RouteQuery {
        RouteQuery {
            modes: Some(vec![Mode::Walk, Mode::Bike, Mode::Car]),
            ..query(50.0000, 4.0009, 50.0000, 4.0031)
        }
    }

    fn street(
        g: &mut Graph,
        o: NodeID,
        d: NodeID,
        len: usize,
        foot: bool,
        bike: bool,
    ) {
        use crate::structures::cost::VarGen;
        use crate::structures::{BikeAttrs, EdgeData, StreetEdgeData};
        g.add_edge(
            o,
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: len,
                foot,
                bike,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    }

}
