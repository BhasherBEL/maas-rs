//! Degenerate next-day-fallback tests: when no same-day service reaches the
//! destination before midnight, the engine used to fabricate a "ride partway +
//! huge egress walk" plan. The fix surfaces the genuine next-day morning trip
//! (times shifted onto the next-day clock, `>= 86400`) via a Pareto-dominating
//! addition, gated so that a same-day-reachable OD gains no next-day plan.
//!
//! The graph is a single ~50 km corridor whose ONLY transit trip runs
//! 09:00->10:00 daily; the direct street walk arrives long after midnight.

use gtfs_structures::RouteType;
use maas_rs::{
    ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    },
    structures::{
        ActiveModes, BikeAttrs, BikeCost, BikeProfile, EdgeData, Graph, LatLng, NodeData, NodeID,
        OsmNodeData, QueryEndpoints, RealtimeIndex, ReliabilityBuckets, StreetEdgeData,
        TransitEdgeData, TransitStopData,
        cost::VarGen,
        plan::{Plan, PlanLeg},
        raptor::{Lookup, PatternInfo},
    },
};

fn osm_node(eid: &str, lat: f64, lon: f64) -> NodeData {
    NodeData::OsmNode(OsmNodeData {
        eid: eid.to_string(),
        lat_lng: LatLng { latitude: lat, longitude: lon },
    })
}

fn transit_stop(name: &str, lat: f64, lon: f64) -> NodeData {
    NodeData::TransitStop(TransitStopData {
        name: name.to_string(),
        lat_lng: LatLng { latitude: lat, longitude: lon },
        accessibility: gtfs_structures::Availability::Available,
        id: name.to_string(),
        platform_code: None,
        parent_station: None,
    })
}

fn street_edge(origin: NodeID, destination: NodeID, length_m: usize) -> EdgeData {
    EdgeData::Street(StreetEdgeData {
        origin,
        destination,
        length: length_m,
        partial: false,
        foot: true,
        bike: true,
        car: true,
        attrs: BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: VarGen::NONE,
    })
}

fn all_days_service() -> ServicePattern {
    ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }
}

fn enable_contraction(g: &mut Graph) {
    use maas_rs::structures::contraction::ContractedGraph;
    let mut cg = ContractedGraph::from_graph_union(g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();
}

/// Origin/dest ~50 km apart on one street edge, joined by a single daily
/// 09:00->10:00 transit trip. Direct walk arrives well after midnight.
fn corridor_graph() -> (Graph, NodeID, NodeID, LatLng, LatLng) {
    let mut g = Graph::new();
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.700)); // ~50 km east
    let stop_o = g.add_node(transit_stop("Origin Halt", 50.000, 4.0005));
    let stop_d = g.add_node(transit_stop("Dest Halt", 50.000, 4.6995));

    g.add_edge(osm_origin, street_edge(osm_origin, osm_dest, 50_000));
    g.add_edge(osm_dest, street_edge(osm_dest, osm_origin, 50_000));

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        let partial = |o: NodeID, d: NodeID| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                length: m,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(stop, partial(stop, osm));
        g.add_edge(osm, partial(osm, stop));
    };
    add_snap(&mut g, stop_o, osm_origin, 50);
    add_snap(&mut g, stop_d, osm_dest, 50);

    g.add_edge(
        stop_o,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_o,
            destination: stop_d,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 50_000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "R".into(),
        route_long_name: "Regio".into(),
        route_type: RouteType::Rail,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None,
        route_id: RouteId(0),
        service_id: ServiceId(0),
        bikes_allowed: None,
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: 9 * 3600,
        arrival: 10 * 3600,
        service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_o, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600,
            departure: 10 * 3600,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    let oll = LatLng { latitude: 50.000, longitude: 4.000 };
    let dll = LatLng { latitude: 50.000, longitude: 4.700 };
    (g, osm_origin, osm_dest, oll, dll)
}

#[allow(clippy::too_many_arguments)]
fn overnight_windowless(g: &Graph, o: NodeID, d: NodeID, oll: LatLng, dll: LatLng, time: u32) -> Vec<Plan> {
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = QueryEndpoints { origin: oll, destination: dll, origin_station: None, destination_station: None };
    g.raptor_tuned_rt_overnight_modes(
        o, d, time, 1000, 0x01, 300, &buckets, g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers, g.raptor.use_cch_access, &RealtimeIndex::new(),
        &ActiveModes::default(), &BikeCost::new(BikeProfile::default()), Some(&ep),
    )
}

#[allow(clippy::too_many_arguments)]
fn overnight_range(g: &Graph, o: NodeID, d: NodeID, oll: LatLng, dll: LatLng, time: u32, window: u32) -> Vec<Plan> {
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = QueryEndpoints { origin: oll, destination: dll, origin_station: None, destination_station: None };
    g.raptor_range_tuned_rt_overnight_modes(
        o, d, time, window, 1000, 0x01, 300, &buckets, g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers, g.raptor.use_cch_access, &RealtimeIndex::new(),
        &ActiveModes::default(), &BikeCost::new(BikeProfile::default()), Some(&ep),
    )
}

fn is_transit(p: &Plan) -> bool {
    p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_)))
}

/// (a) No same-day dest arrival before midnight + a next-day trip exists ->
/// next-day transit plan returned (start/end >= 86400), and it is a short ride,
/// not the ~50 km walk fabrication. Windowless / probe driver.
#[test]
fn next_day_fallback_surfaces_morning_trip_windowless() {
    let (g, o, d, oll, dll) = corridor_graph();
    let plans = overnight_windowless(&g, o, d, oll, dll, 19 * 3600 + 4 * 60);
    assert!(!plans.is_empty(), "expected at least a walk fallback");
    let next_day = plans.iter().find(|p| is_transit(p) && p.start >= 86400 && p.end >= 86400);
    assert!(
        next_day.is_some(),
        "expected a next-day transit plan (start/end >= 86400); got {:?}",
        plans.iter().map(|p| (p.start, p.end, is_transit(p))).collect::<Vec<_>>()
    );
    let p = next_day.unwrap();
    assert!(
        p.end.saturating_sub(p.start) < 6 * 3600,
        "next-day transit should be a short ride, not a huge egress walk"
    );
}

/// (a) Same, via the range/window driver with a 30-min window that does NOT cross
/// midnight (the pre-existing forward extension never fires) — only the new
/// degenerate fallback can surface the trip.
#[test]
fn next_day_fallback_surfaces_morning_trip_range() {
    let (g, o, d, oll, dll) = corridor_graph();
    let plans = overnight_range(&g, o, d, oll, dll, 19 * 3600 + 4 * 60, 1800);
    assert!(
        plans.iter().any(|p| is_transit(p) && p.start >= 86400 && p.end >= 86400),
        "range driver should surface the next-day transit plan; got {:?}",
        plans.iter().map(|p| (p.start, p.end, is_transit(p))).collect::<Vec<_>>()
    );
}

/// (b) Normal same-day-reachable OD -> no next-day plan added. Windowless driver.
#[test]
fn no_next_day_plan_when_same_day_reachable_windowless() {
    let (g, o, d, oll, dll) = corridor_graph();
    let plans = overnight_windowless(&g, o, d, oll, dll, 8 * 3600 + 30 * 60);
    assert!(plans.iter().any(is_transit), "expected a same-day transit plan");
    assert!(
        plans.iter().all(|p| p.start < 86400 && p.end < 86400),
        "no next-day plan should be added when reachable same-day; got {:?}",
        plans.iter().map(|p| (p.start, p.end)).collect::<Vec<_>>()
    );
}

/// (b) Same, via the range driver with a 60-min window covering the 09:00 trip.
#[test]
fn no_next_day_plan_when_same_day_reachable_range() {
    let (g, o, d, oll, dll) = corridor_graph();
    let plans = overnight_range(&g, o, d, oll, dll, 8 * 3600 + 30 * 60, 3600);
    assert!(plans.iter().any(is_transit), "expected a same-day transit plan");
    assert!(
        plans.iter().all(|p| p.start < 86400 && p.end < 86400),
        "no next-day plan should be added when reachable same-day; got {:?}",
        plans.iter().map(|p| (p.start, p.end)).collect::<Vec<_>>()
    );
}
