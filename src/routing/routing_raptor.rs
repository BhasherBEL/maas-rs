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
    /// When `> 0`, run Range-RAPTOR over this window (seconds).
    pub window_minutes: Option<u32>,
    /// Per-query override for the minimum walk-radius used for access/egress
    /// stop discovery (seconds).  `None` → use the graph's configured default.
    pub min_access_secs: Option<u32>,
    /// Per-query override for the arrival-slack (seconds). `None` → graph default.
    pub arrival_slack_secs: Option<u32>,
    /// Per-query override for MCR unrestricted (uncapped) inter-stop transfers.
    /// `None` → graph default (`graph.raptor.unrestricted_transfers`). Lets MCR on/off
    /// be A/B'd on the same binary + `graph.bin` without a rebuild.
    pub unrestricted_transfers: Option<bool>,
    /// Per-query override for exact CCH foot access/egress.
    /// `None` → graph default (`graph.raptor.use_cch_access`). Lets the CCH seam be
    /// A/B'd on the same binary + `graph.bin` without a rebuild.
    pub use_cch_access: Option<bool>,
    /// Per-query override for reliability bucket edges. `None`/invalid → graph default.
    pub reliability_bucket_edges: Option<Vec<f32>>,
    /// Travel modes the router may use. `None` → `[WALK, WALK_TRANSIT]`
    /// (the historical behavior). Empty is rejected.
    pub modes: Option<Vec<Mode>>,
    /// Per-query bike cost profile. `None` → the graph's configured default.
    pub bike_profile: Option<crate::structures::BikeProfile>,
    /// When true, direct walk/bike plans are built with `LegRole::Deadline`
    /// (variance-proxy axis active) rather than `LegRole::Neutral`.
    pub terminal_deadline: bool,
    /// When `Some`, route from a position ABOARD a transit trip (between stops)
    /// instead of `from_lat`/`from_lng` (which are then ignored). The destination
    /// stays the lat/lng `to_*`.
    pub onboard_origin: Option<OnboardOrigin>,
    /// When `Some` and resolvable, the origin is the chosen station: every member
    /// platform is reachable with zero access walk (no 50 m line, no access leg),
    /// overriding `from_lat`/`from_lng`. An unknown id falls back to the coordinate.
    pub from_station_id: Option<String>,
    /// As `from_station_id`, for the destination (zero-cost egress).
    pub to_station_id: Option<String>,
    /// When `Some(true)`, emit a per-phase wall-clock decomposition of this query
    /// (discovery/grid_alloc/forward/extract/backward, plus per-pass probe/range/
    /// departure counts) as one structured log line. Purely additive observability
    /// — never changes routing behavior or results. `None` → graph default
    /// (`graph.raptor.profile_latency`, itself defaulting to off).
    pub profile_latency: Option<bool>,
}

/// A position aboard a transit trip: the boarded GTFS `trip_id`, plus an optional
/// advisory current stop (id or stop sequence). When neither is given the current
/// position is the last pattern stop whose realtime departure is `<= now` (`time`).
#[derive(Clone, Debug)]
pub struct OnboardOrigin {
    pub trip_id: String,
    pub from_stop_id: Option<String>,
    pub from_stop_seq: Option<u32>,
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
    let slack = query
        .arrival_slack_secs
        .unwrap_or(graph.raptor.arrival_slack_secs);
    Ok((buckets, slack))
}

/// Resolves the mode selection, rejecting an explicitly empty list.
fn resolve_modes(query: &RouteQuery) -> Result<ActiveModes, async_graphql::Error> {
    match &query.modes {
        None => Ok(ActiveModes::default()),
        Some(m) if m.is_empty() => Err(async_graphql::Error::new("modes must not be empty")),
        Some(m) => Ok(ActiveModes::new(m)),
    }
}

/// Range-RAPTOR window in seconds, clamped to the configured maximum.
fn effective_window_secs(window_minutes: u32, max_window_secs: u32) -> u32 {
    window_minutes.saturating_mul(60).min(max_window_secs)
}

/// Arena snap of a query coordinate when contraction is on: the projected snap point +
/// a bounding-junction NodeID (stable identity; geometry/cost use the projection). Rejects
/// coordinates farther than the snap-distance guard, matching `snap_node`. `g` may have its
/// interior-node arrays dropped — this reads only the contracted segment R-tree.
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

/// Resolves one journey endpoint to its snap `NodeID`, geometry coordinate, and
/// optional zero-cost station platform set. When `station_id` resolves to a known
/// station with platforms, the station's representative coordinate is used for
/// snapping/geometry (so no spurious access line is drawn) and its platforms are
/// returned for zero-cost hub access/egress. An unknown id, or one that fails to
/// snap, falls back to the supplied coordinate (no station).
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
        // Use the station's own representative coordinate (not its street
        // projection) for the endpoint marker/geometry, so no spurious access line
        // is ever drawn to a chosen station.
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

/// Routes from a position ABOARD a transit trip to the lat/lng destination.
/// Seeds the boarded trip's downstream stops and re-plans onward, surfacing in
/// one shot: stay-on, alight-and-transfer, and alight-and-walk.
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

/// Wall-clock unix seconds; `0` if the clock is somehow before the epoch.
fn now_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Brussels-local calendar date of `now_unix`, as days since 2000-01-01 — the same
/// convention `date_to_days(query.date)` uses. GTFS service dates are Brussels-local,
/// so this is the service date the live snapshot is currently relevant to.
fn brussels_service_days(now_unix: i64) -> u32 {
    use chrono::TimeZone;
    match chrono_tz::Europe::Brussels.timestamp_opt(now_unix, 0).single() {
        Some(dt) => date_to_days(dt.date_naive()),
        None => 0,
    }
}

/// Consumer-boundary realtime gate. Returns the index routing should actually apply:
/// either the live snapshot or an inert empty index (`RealtimeIndex::new()`), so a
/// stale or wrong-date snapshot is never applied. The poller is never modified.
///
/// - **Already inert** (`rt.is_empty()`): returned as-is. The no-feed path is thus
///   byte-identical to schedule-only and never consults the clock.
/// - **Staleness (#2):** `now - generated_at > max_age_secs` → empty. A cycle where
///   every feed fails keeps the last good index in the poller with no TTL of its
///   own; this boundary supplies the config TTL (`realtime.index_max_age_secs`,
///   stamped on the snapshot) so hours-old delays/cancellations stop being applied.
/// - **Service date (#3):** realtime carries no service-date dimension (keyed
///   `(TripId, compact_stop)` only), so it may only be applied when the query's
///   calendar service date equals the Brussels service date of `now`. A query for
///   another day (tomorrow / last week) must not inherit today's delays or today's
///   cancellations.
///
/// Overnight nuance: `raptor_tuned_rt_overnight_modes` runs the after-midnight
/// (`date - 1`) sub-pass with the SAME index handed to the main pass. Because a
/// same-day query legitimately covers "now" — including just-after-midnight runs of
/// the previous service day — handing the live index to a same-day query is exactly
/// what serves that overnight sub-pass; we deliberately do NOT also accept
/// `date == now + 1` (that would leak realtime into tomorrow's MAIN pass, re-opening
/// #3). Keying the date on `now` (not on `generated_at`) keeps the immediate
/// post-midnight window live: a query dated D+1 at 00:05, against a still-fresh
/// snapshot generated at 23:58 on day D, matches `now`'s date D+1 and keeps its
/// realtime.
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

    // Flag read ONCE per query (never per-call/per-departure): resolves the
    // per-query override or the graph default, then arms the thread-local
    // profiler for the duration of this query only.
    let profiling = query
        .profile_latency
        .unwrap_or(graph.raptor.profile_latency);
    let profile_start = crate::structures::latency_profile::begin_query(profiling);

    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query));
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

/// Like `route`, but returns all intermediate candidates and access metadata.
/// Does NOT return an error for empty results — an empty result is itself a debug signal.
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

    // Note: the explain path does not apply the overnight pass — it's a debug view
    // of a single RAPTOR run and overnight merging would complicate candidate provenance.
    let bike = crate::structures::BikeCost::new(resolve_bike_profile(graph, query));
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
        // Add a second node a few metres away so the contracted seg index has a segment
        // to return (an isolated node produces an empty seg index).
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
        // Extend snap-search radius to the whole world so a far-away query (e.g. Paris)
        // still finds the segment and returns a large dist_m → "too far" distance error
        // rather than "no node near".  Default radius (300 m) would let the distant query
        // exit the R-tree before reaching any segment, silencing the guard we're testing.
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
        }
    }

    // --- Realtime consumer-boundary gate (bugs #2 stale-index, #3 wrong-date) ---

    use crate::ingestion::gtfs::TripId;

    /// A non-empty synthetic snapshot (one delay) generated at `gen_unix` with TTL
    /// `ttl`. Non-empty so it does NOT hit the `is_empty()` short-circuit — the
    /// staleness/date checks are actually exercised.
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
        // (a) FRESH snapshot + query for TODAY → the real index is applied
        // (returned handle is the snapshot itself). This is the common live case
        // and the byte-identity requirement: routing sees exactly the passed rt.
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let rt = rt_snapshot(now - 10, 600);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(std::ptr::eq(got, &rt), "fresh+today must apply the live index");
    }

    #[test]
    fn gate_sticky_only_index_is_inert_for_routing() {
        // A snapshot carrying ONLY sticky (tracked-journey) delays must look empty to
        // the routing gate: is_empty() short-circuits (returns the snapshot, which is
        // itself empty), so apply_realtime no-ops and planning is byte-identical to
        // no-feed. Retention lives solely for the live-refresh overlay.
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let mut sticky = std::collections::HashMap::new();
        sticky.insert((TripId(1), 0), (120, now));
        // Fresh generated_at + today, so the ONLY thing that could make it apply is a
        // non-empty index — which sticky must NOT provide.
        let rt = RealtimeIndex::new()
            .with_max_age_secs(600)
            .with_sticky_delays(sticky);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(got.is_empty(), "sticky-only snapshot is empty for routing");
        assert_eq!(got.delay(TripId(1), 0), 0, "routing accessor never sees sticky");
        // Sticky is still reachable via the live-refresh accessor on the same handle.
        assert_eq!(rt.delay_with_sticky(TripId(1), 0), 120);
    }

    #[test]
    fn gate_stale_index_is_ignored() {
        // (b) snapshot older than its TTL → ignored (empty substituted), even for a
        // today query. A feed outage must not serve hours-old delays/cancellations.
        let now = now_unix_secs();
        let today = brussels_service_days(now);
        let rt = rt_snapshot(now - 1000, 600); // 1000s old, TTL 600s
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, today, now);
        assert!(std::ptr::eq(got, &empty), "stale index must be ignored");
    }

    #[test]
    fn gate_future_date_query_is_ignored() {
        // (c) query for TOMORROW against a fresh snapshot → ignored: realtime has no
        // service-date dimension, so today's delays/cancellations must not apply to
        // a future day.
        let now = now_unix_secs();
        let tomorrow = brussels_service_days(now) + 1;
        let rt = rt_snapshot(now - 10, 600);
        let empty = RealtimeIndex::new();
        let got = gate_realtime(&rt, &empty, tomorrow, now);
        assert!(std::ptr::eq(got, &empty), "future-date query must ignore realtime");
    }

    #[test]
    fn gate_after_midnight_window_keeps_realtime() {
        // (d) The legitimate after-midnight window. `now` = 00:05 on day D+1,
        // snapshot generated at 23:58 on day D (7 min old, still fresh). The query
        // is dated D+1 (the current calendar day). Keying the date on `now` keeps
        // realtime live across the midnight boundary — a `generated_at`-based date
        // would (wrongly) drop it, since gen's date is D. The single applied index
        // also feeds RAPTOR's internal `date-1` overnight sub-pass.
        let now = brussels_unix(2026, 1, 15, 0, 5); // winter → no DST ambiguity
        let gen_unix = brussels_unix(2026, 1, 14, 23, 58);
        let now_days = brussels_service_days(now);
        let gen_days = brussels_service_days(gen_unix);
        assert_eq!(now_days, gen_days + 1, "the instants must straddle midnight");
        assert!(now - gen_unix < 600, "snapshot must still be within TTL");

        let rt = rt_snapshot(gen_unix, 600);
        let empty = RealtimeIndex::new();
        // Query dated D+1 (current calendar day) at/after midnight still gets rt.
        let got = gate_realtime(&rt, &empty, now_days, now);
        assert!(
            std::ptr::eq(got, &rt),
            "post-midnight same-day query must keep realtime"
        );
    }

    #[test]
    fn gate_empty_index_short_circuits_regardless_of_clock() {
        // Byte-identity guarantee for the no-feed path: an empty index is returned
        // as-is (never the substitute), and the clock/date are never consulted —
        // even a nominally "stale" or wrong-date call returns the same empty handle.
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
        // b has no outgoing edges in the one-directional test graph, so the contracted
        // graph builder skips it (k=0) and no super-edge reaches b.  A back-edge makes
        // b a proper junction so both forward paths (a→b direct, a→c→b) are findable.
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

    // Phase B (done): bike street legs are enriched like walk legs — the multi-objective
    // post-pass now runs for bike, so a direct bike plan surfaces route alternatives.
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
        // Climb trade-off (D+ is a bike front axis; Surface is display-only): the
        // short direct edge climbs, the long flat detour avoids the climb. Both
        // survive the 3-axis front as a faster-hillier vs flatter-slower pair.
        g.add_edge(a, e(a, b, 100, 8));
        g.add_edge(a, e(a, c, 400, 0));
        g.add_edge(c, e(c, b, 400, 0));
        // b has no outgoing edges in the one-directional test graph, so the contracted
        // graph builder skips it (k=0) and no super-edge reaches b.  A back-edge makes
        // b a proper junction so both forward paths (a→b direct, a→c→b) are findable.
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

    /// COORD-ROUTED drop gate (the snapping oracle): route from raw lat/lng with node
    /// contraction on, DROP the interior-node arrays, and re-route the SAME coordinates —
    /// the full plans (including endpoint geometry) must be BYTE-IDENTICAL. This fails
    /// today because snapping (`snap_node` → `nearest_node_dist`) reads the dropped g
    /// kdtree. It passes once snapping is arena-based and gated on `contracted.is_some()` so
    /// it snaps via the segment R-tree whether or not g is present — making the drop
    /// behaviorally a no-op (the same uniform-arena discipline as traversal/geometry/
    /// transit). Bike is baked so the bike-snap path is actually exercised.
    ///
    /// The completion oracle: when green, the full street-leg reconstruction/enrichment
    /// path (walk + bike, search + plan + alternatives + geometry) is g-free.
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

    /// Chain a — i1 — i2 — i3 — b: i1..i3 are degree-2 interior (contracted away),
    /// a and b are degree-1 junctions. A coordinate near i2 snaps mid-super-edge.
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
