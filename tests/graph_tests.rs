/// Integration tests for the Graph data structure and its algorithms.
///
/// Key invariants to keep in mind when writing graph tests:
/// - `walk_dijkstra` accesses `transit_node_to_stop[node.0]` unconditionally, so
///   `build_raptor_index()` MUST be called before any test that uses it (or
///   `nearby_stops`).
/// - Weekday bitmask: Mon=0x01, Tue=0x02, Wed=0x04, Thu=0x08, Fri=0x10,
///   Sat=0x20, Sun=0x40.
/// - Times are seconds since midnight; dates are days since 2000-01-01.
use gtfs_structures::{Availability, RouteType};
use maas_rs::{
    ingestion::gtfs::{
        AgencyId, AgencyInfo, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime,
        TimetableSegment, TripId, TripInfo, TripSegment,
    },
    structures::{
        ActiveModes, BikeAttrs, BikeCost, BikeProfile, DelayCDF, EdgeData, Graph, HighwayClass,
        LatLng, Mode, NodeData, NodeID, OsmNodeData, RealtimeIndex, ReliabilityBuckets,
        StreetEdgeData, StreetProfile, Surface, TransitEdgeData, TransitStopData,
        plan::PlanLeg,
        raptor::{Lookup, PatternInfo},
    },
};
use std::collections::HashMap;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn osm_node(eid: &str, lat: f64, lon: f64) -> NodeData {
    NodeData::OsmNode(OsmNodeData {
        eid: eid.to_string(),
        lat_lng: LatLng {
            latitude: lat,
            longitude: lon,
        },
    })
}

fn transit_stop(name: &str, lat: f64, lon: f64) -> NodeData {
    NodeData::TransitStop(TransitStopData {
        name: name.to_string(),
        lat_lng: LatLng {
            latitude: lat,
            longitude: lon,
        },
        accessibility: Availability::Available,
        id: name.to_string(),
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
        car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
    })
}

/// Returns a simple 3-node street graph: A –100m– B –100m– C
/// Nodes are placed along a roughly horizontal line.
fn three_node_street_graph() -> (Graph, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001)); // ~80m east
    let c = g.add_node(osm_node("c", 50.000, 4.002)); // ~160m east
    g.add_edge(a, street_edge(a, b, 100));
    g.add_edge(b, street_edge(b, a, 100));
    g.add_edge(b, street_edge(b, c, 100));
    g.add_edge(c, street_edge(c, b, 100));
    (g, a, b, c)
}

/// Active-every-day service within date range [0, 9999].
fn all_days_service() -> ServicePattern {
    ServicePattern {
        days_of_week: 0x7F, // all 7 bits set
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }
}

// ── Graph construction ────────────────────────────────────────────────────────

#[test]
fn new_graph_is_empty() {
    let g = Graph::new();
    assert_eq!(g.node_count(), 0);
    assert_eq!(g.edge_count(), 0);
}

#[test]
fn add_osm_node_increments_count() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("n1", 50.0, 4.0));
    assert_eq!(g.node_count(), 1);
    assert_eq!(id, NodeID(0));
}

#[test]
fn add_multiple_nodes_assigns_sequential_ids() {
    let mut g = Graph::new();
    let id0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let id1 = g.add_node(osm_node("n1", 50.1, 4.1));
    let id2 = g.add_node(transit_stop("stop", 50.2, 4.2));
    assert_eq!(id0, NodeID(0));
    assert_eq!(id1, NodeID(1));
    assert_eq!(id2, NodeID(2));
    assert_eq!(g.node_count(), 3);
}

#[test]
fn get_node_returns_correct_data() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("myeid", 51.5, -0.1));
    let node = g.get_node(id).expect("node should exist");
    match node {
        NodeData::OsmNode(n) => {
            assert_eq!(n.eid, "myeid");
            assert!((n.lat_lng.latitude - 51.5).abs() < 1e-9);
            assert!((n.lat_lng.longitude - (-0.1)).abs() < 1e-9);
        }
        _ => panic!("Expected OsmNode"),
    }
}

#[test]
fn get_node_out_of_bounds_returns_none() {
    let g = Graph::new();
    assert!(g.get_node(NodeID(99)).is_none());
}

#[test]
fn get_id_finds_osm_node_by_eid() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("map#osm#42", 50.0, 4.0));
    assert_eq!(g.get_id("map#osm#42"), Some(&id));
}

#[test]
fn get_id_returns_none_for_unknown_eid() {
    let g = Graph::new();
    assert!(g.get_id("nonexistent").is_none());
}

#[test]
fn get_id_does_not_find_transit_stops() {
    let mut g = Graph::new();
    // Transit stops are not inserted into the id_mapper
    g.add_node(transit_stop("Central", 50.0, 4.0));
    assert!(g.get_id("Central").is_none());
}

#[test]
fn add_edge_increases_edge_count() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.0, 4.0));
    let b = g.add_node(osm_node("b", 50.0, 4.001));
    assert_eq!(g.edge_count(), 2); // edge_count() returns edges.len() == node count
    g.add_edge(a, street_edge(a, b, 100));
    // edge_count() returns edges Vec len (number of adjacency lists, not total edges)
    assert_eq!(g.edge_count(), 2);
}

// ── Spatial lookup ────────────────────────────────────────────────────────────

#[test]
fn nearest_node_returns_none_on_empty_graph() {
    let g = Graph::new();
    assert!(g.nearest_node(50.0, 4.0).is_none());
}

#[test]
fn nearest_node_finds_only_node() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("solo", 50.0, 4.0));
    assert_eq!(g.nearest_node(50.0, 4.0), Some(id));
}

#[test]
fn nearest_node_finds_closest_of_two() {
    let mut g = Graph::new();
    let near = g.add_node(osm_node("near", 50.000, 4.000));
    let _far = g.add_node(osm_node("far", 52.000, 6.000));
    // Query at a point close to "near"
    assert_eq!(g.nearest_node(50.001, 4.001), Some(near));
}

#[test]
fn nearest_node_dist_returns_none_on_empty_graph() {
    let g = Graph::new();
    assert!(g.nearest_node_dist(50.0, 4.0).is_none());
}

#[test]
fn nearest_node_dist_returns_distance() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("p", 50.0, 4.0));
    let (dist, found_id) = g.nearest_node_dist(50.0, 4.0).expect("should find node");
    assert_eq!(*found_id, id);
    assert!(dist < 1.0, "Same-point distance should be ~0, got {dist}");
}

#[test]
fn nearest_node_ignores_transit_stops() {
    // Only OsmNodes go into nodes_tree; TransitStops are not returned by nearest_node
    let mut g = Graph::new();
    g.add_node(transit_stop("stop", 50.0, 4.0));
    assert!(g.nearest_node(50.0, 4.0).is_none());
}

#[test]
fn nodes_distance_same_node_is_zero() {
    let mut g = Graph::new();
    let id = g.add_node(osm_node("a", 50.0, 4.0));
    assert_eq!(g.nodes_distance(id, id), 0);
}

#[test]
fn nodes_distance_close_nodes() {
    let mut g = Graph::new();
    // Two nodes ~111m apart in latitude (1/1000 degree ≈ 111m)
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.001, 4.000));
    let d = g.nodes_distance(a, b);
    // 0.001° lat ≈ 111m, scaled by 0.99 → ~110m
    assert!(d > 80 && d < 140, "Expected ~110m, got {d}");
}

// ── Transit data accessors ────────────────────────────────────────────────────

#[test]
fn get_trip_returns_none_on_empty_graph() {
    let g = Graph::new();
    assert!(g.get_trip(TripId(0)).is_none());
}

#[test]
fn get_trip_returns_inserted_trip() {
    let mut g = Graph::new();
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: Some("North".to_string()),
        route_id: RouteId(0),
        service_id: ServiceId(0),
        bikes_allowed: None,
    }]);
    let trip = g.get_trip(TripId(0)).expect("trip should exist");
    assert_eq!(trip.trip_headsign.as_deref(), Some("North"));
}

#[test]
fn get_route_returns_inserted_route() {
    let mut g = Graph::new();
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".to_string(),
        route_long_name: "Line One".to_string(),
        route_type: gtfs_structures::RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    let route = g.get_route(RouteId(0)).expect("route should exist");
    assert_eq!(route.route_short_name, "1");
}

#[test]
fn get_agency_returns_inserted_agency() {
    let mut g = Graph::new();
    g.add_transit_agencies(vec![AgencyInfo {
        name: "STIB".to_string(),
        url: "https://stib.be".to_string(),
        timezone: "Europe/Brussels".to_string(),
    }]);
    let agency = g.get_agency(AgencyId(0)).expect("agency should exist");
    assert_eq!(agency.name, "STIB");
}

// ── next_transit_departure ────────────────────────────────────────────────────

/// Builds a graph with 3 departures on a weekday service and returns
/// (graph, timetable_segment).
fn make_transit_graph() -> (Graph, TimetableSegment) {
    let mut g = Graph::new();

    // Service 0: active every day
    g.add_transit_services(vec![all_days_service()]);

    // 3 departures at 08:00, 10:00, 12:00
    let segments = vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600, // 08:00
            arrival: 8 * 3600 + 600,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 10 * 3600, // 10:00
            arrival: 10 * 3600 + 600,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 12 * 3600, // 12:00
            arrival: 12 * 3600 + 600,
            service_id: ServiceId(0),
        },
    ];

    let tt = TimetableSegment {
        start: 0,
        len: segments.len(),
    };
    g.add_transit_departures(segments);

    (g, tt)
}

#[test]
fn next_departure_before_first_returns_first() {
    let (g, tt) = make_transit_graph();
    let (idx, dep) = g
        .next_transit_departure(tt, 7 * 3600, 500, 0x7F)
        .expect("should find a departure");
    assert_eq!(idx, 0);
    assert_eq!(dep.departure, 8 * 3600);
}

#[test]
fn next_departure_at_exact_time_returns_that_departure() {
    let (g, tt) = make_transit_graph();
    let (idx, dep) = g
        .next_transit_departure(tt, 10 * 3600, 500, 0x7F)
        .expect("should find a departure");
    assert_eq!(idx, 1);
    assert_eq!(dep.departure, 10 * 3600);
}

#[test]
fn next_departure_between_two_returns_later_one() {
    let (g, tt) = make_transit_graph();
    let (idx, dep) = g
        .next_transit_departure(tt, 9 * 3600, 500, 0x7F)
        .expect("should find a departure");
    assert_eq!(idx, 1);
    assert_eq!(dep.departure, 10 * 3600);
}

#[test]
fn next_departure_after_last_returns_none() {
    let (g, tt) = make_transit_graph();
    assert!(g.next_transit_departure(tt, 13 * 3600, 500, 0x7F).is_none());
}

#[test]
fn next_departure_inactive_service_skips() {
    let mut g = Graph::new();
    // Service only active on Saturday (0x20)
    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x20,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: 9 * 3600,
        arrival: 9 * 3600 + 300,
        service_id: ServiceId(0),
    }]);
    let tt = TimetableSegment { start: 0, len: 1 };
    // Query on Monday (0x01) → no active service
    assert!(g.next_transit_departure(tt, 8 * 3600, 100, 0x01).is_none());
    // Query on Saturday (0x20) → finds the trip
    assert!(g.next_transit_departure(tt, 8 * 3600, 100, 0x20).is_some());
}

// ── previous_departures / next_departures ─────────────────────────────────────

#[test]
fn previous_departures_from_middle_yields_earlier_trips() {
    let (g, tt) = make_transit_graph();
    // Starting from index 2 (12:00), look for earlier active departures
    let prev: Vec<_> = g.previous_departures(tt, 500, 0x7F, 2).collect();
    assert_eq!(prev.len(), 2, "Expected 2 earlier departures");
    // Should be in reverse order: index 1 first, then 0
    assert_eq!(prev[0].0, 1);
    assert_eq!(prev[0].1.departure, 10 * 3600);
    assert_eq!(prev[1].0, 0);
    assert_eq!(prev[1].1.departure, 8 * 3600);
}

#[test]
fn previous_departures_from_first_yields_empty() {
    let (g, tt) = make_transit_graph();
    let prev: Vec<_> = g.previous_departures(tt, 500, 0x7F, 0).collect();
    assert!(prev.is_empty());
}

#[test]
fn next_departures_from_middle_yields_later_trips() {
    let (g, tt) = make_transit_graph();
    let next: Vec<_> = g.next_departures(tt, 500, 0x7F, 0).collect();
    assert_eq!(next.len(), 2);
    assert_eq!(next[0].0, 1);
    assert_eq!(next[0].1.departure, 10 * 3600);
    assert_eq!(next[1].0, 2);
    assert_eq!(next[1].1.departure, 12 * 3600);
}

#[test]
fn next_departures_from_last_yields_empty() {
    let (g, tt) = make_transit_graph();
    let next: Vec<_> = g.next_departures(tt, 500, 0x7F, 2).collect();
    assert!(next.is_empty());
}

#[test]
fn next_departures_filters_inactive_service() {
    let mut g = Graph::new();
    // Service 0: weekdays only (Mon–Fri)
    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x1F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600,
            arrival: 8 * 3600 + 600,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 10 * 3600,
            arrival: 10 * 3600 + 600,
            service_id: ServiceId(0),
        },
    ]);
    let tt = TimetableSegment { start: 0, len: 2 };
    // On Sunday (0x40), neither trip should appear
    let next: Vec<_> = g.next_departures(tt, 100, 0x40, 0).collect();
    assert!(next.is_empty(), "Expected no departures on Sunday");
}

// ── build_raptor_index + walk_dijkstra ────────────────────────────────────────

#[test]
fn raptor_index_compact_stop_mapping() {
    let mut g = Graph::new();
    let osm = g.add_node(osm_node("osm1", 50.0, 4.0));
    let stop = g.add_node(transit_stop("Stop A", 50.001, 4.001));
    g.build_raptor_index();

    // transit_node_to_stop[osm.0] should be u32::MAX (not a stop)
    // transit_node_to_stop[stop.0] should be 0 (first compact stop)
    // We verify indirectly via walk_dijkstra: starting from osm node should work
    let dist = g.walk_dijkstra(osm, 999999);
    assert!(dist.contains_key(&osm), "Origin should be in dist map");
    // Stop node is NOT walked through (transit stops halt Dijkstra)
    assert!(
        !dist.contains_key(&stop),
        "Transit stop should not be walked through"
    );
}

#[test]
fn walk_dijkstra_finds_connected_nodes() {
    let (mut g, a, b, c) = three_node_street_graph();
    g.build_raptor_index();

    let dist = g.walk_dijkstra(a, 99999);
    assert!(dist.contains_key(&a));
    assert!(dist.contains_key(&b));
    assert!(dist.contains_key(&c));
}

#[test]
fn walk_dijkstra_distances_are_ordered() {
    let (mut g, a, b, c) = three_node_street_graph();
    g.build_raptor_index();

    let dist = g.walk_dijkstra(a, 99999);
    let da = dist[&a];
    let db = dist[&b];
    let dc = dist[&c];
    assert_eq!(da, 0, "Origin distance should be 0");
    assert!(db > 0, "b should be reachable with positive cost");
    assert!(dc > db, "c should be further than b from a");
}

#[test]
fn walk_dijkstra_respects_max_seconds_cutoff() {
    let (mut g, a, b, c) = three_node_street_graph();
    g.build_raptor_index();

    // 100m at 1.2 m/s ≈ 83s. With max=90, b should be reachable but c should not.
    let dist = g.walk_dijkstra(a, 90);
    assert!(dist.contains_key(&b), "b (83s) should be within 90s cutoff");
    assert!(!dist.contains_key(&c), "c (166s) should exceed 90s cutoff");
}

#[test]
fn walk_dijkstra_isolated_node_not_reached() {
    let (mut g, a, _b, _c) = three_node_street_graph();
    let isolated = g.add_node(osm_node("iso", 55.0, 10.0)); // far away, no edges
    g.build_raptor_index();

    let dist = g.walk_dijkstra(a, 99999);
    assert!(
        !dist.contains_key(&isolated),
        "Isolated node should not be reachable"
    );
}

#[test]
fn walk_dijkstra_origin_always_in_result() {
    let (mut g, a, _, _) = three_node_street_graph();
    g.build_raptor_index();
    let dist = g.walk_dijkstra(a, 0);
    // Even with max_seconds=0, origin should be present at distance 0
    assert_eq!(dist[&a], 0);
}

// ── nearby_stops ──────────────────────────────────────────────────────────────

#[test]
fn nearby_stops_empty_when_no_transit_stops() {
    let (mut g, a, _, _) = three_node_street_graph();
    g.build_raptor_index();
    let stops = g.nearby_stops(a, 9999);
    assert!(stops.is_empty());
}

#[test]
fn nearby_stops_finds_connected_stop() {
    let mut g = Graph::new();
    // Street node connected to a transit stop
    let street = g.add_node(osm_node("s1", 50.000, 4.000));
    let stop = g.add_node(transit_stop("A", 50.000, 4.000));
    // Walk edge from street to stop
    g.add_edge(street, street_edge(street, stop, 50));
    g.build_raptor_index();

    let stops = g.nearby_stops(street, 9999);
    // The stop should be reachable; compact index 0 is the only stop
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0].0, 0); // compact stop index
}

// ── Street profiles (foot / bike) ─────────────────────────────────────────────

fn street_edge_flags(
    origin: NodeID,
    destination: NodeID,
    length_m: usize,
    foot: bool,
    bike: bool,
) -> EdgeData {
    EdgeData::Street(StreetEdgeData {
        origin,
        destination,
        length: length_m,
        partial: false,
        foot,
        bike,
        car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
    })
}

#[test]
fn bike_dijkstra_uses_bike_edges_at_bike_speed() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    g.add_edge(a, street_edge_flags(a, b, 420, false, true));
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Bike);
    // 420 m at 4.2 m/s = 100 s
    assert_eq!(dist[&b], 100);
}

#[test]
fn bike_dijkstra_falls_back_to_foot_edges_at_walk_speed() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    g.add_edge(a, street_edge_flags(a, b, 120, true, false));
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Bike);
    // foot-only edge pushed at walking speed: 120 m at 1.2 m/s = 100 s
    assert_eq!(dist[&b], 100);
}

#[test]
fn foot_dijkstra_ignores_bike_only_edges() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    g.add_edge(a, street_edge_flags(a, b, 100, false, true));
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Foot);
    assert!(!dist.contains_key(&b));
    // And the legacy wrapper behaves identically.
    let dist_legacy = g.walk_dijkstra(a, 99999);
    assert!(!dist_legacy.contains_key(&b));
}

#[test]
fn nearby_stops_bike_profile_reaches_farther() {
    let mut g = Graph::new();
    let street = g.add_node(osm_node("s1", 50.000, 4.000));
    let stop = g.add_node(transit_stop("A", 50.000, 4.001));
    // 504 m: 120 s by bike (4.2 m/s), 420 s on foot (1.2 m/s).
    g.add_edge(street, street_edge(street, stop, 504));
    g.build_raptor_index();

    let by_foot = g.nearby_stops_profile(street, 200, StreetProfile::Foot);
    let by_bike = g.nearby_stops_profile(street, 200, StreetProfile::Bike);
    assert!(by_foot.is_empty());
    assert_eq!(by_bike.len(), 1);
    assert_eq!(by_bike[0], (0, 120));
}

// ── RAPTOR transfer_risk ───────────────────────────────────────────────────────

/// Builds a minimal 2-route graph:
///   Bus  (route 0): stop_A → stop_B, departs 09:00, arrives 09:15
///   Tram (route 1): stop_C → stop_D, departs 09:30, arrives 09:45
///
/// Layout (all lat=50.000, varying lon):
///   osm_origin(4.000) — stop_A(4.001) ——— osm_ab(4.010) ——— osm_b(4.019)
///   stop_B(4.020) stop_C(4.022) ——— osm_cd(4.030) ——— osm_dest(4.041)
///   stop_D(4.040)
///
/// Distances:
///   stop_A to stop_B ≈ 1362 m  → OUTSIDE MAX_TRANSFER_DISTANCE_M (1000 m)
///   stop_B to stop_C ≈  143 m  → inside  MAX_TRANSFER_DISTANCE_M (1000 m)
///
/// This ensures round-0 apply_transfers cannot pre-walk to stop_B, so the only way
/// to reach stop_B is via the Bus in round 1. That guarantees labels_rt[1][stop_C]
/// is set to Some(Bus) after the B→C transfer, making the Tram leg's transfer_risk non-null.
fn two_route_raptor_graph() -> (Graph, NodeID, NodeID) {
    two_route_raptor_graph_with_bikes(None, None)
}

fn two_route_raptor_graph_with_bikes(
    bus_bikes: Option<bool>,
    tram_bikes: Option<bool>,
) -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    // OSM nodes (auto-added to nodes_tree for nearest_node lookup)
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_ab = g.add_node(osm_node("ab", 50.000, 4.010)); // mid A–B
    let osm_b = g.add_node(osm_node("b", 50.000, 4.019)); // near stop_B/C
    let osm_cd = g.add_node(osm_node("cd", 50.000, 4.030)); // mid C–D
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041)); // near stop_D

    // Transit stops (NOT added to nodes_tree)
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // bus board  (~72m from osm_origin)
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.020)); // bus alight (~72m from osm_b)
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.022)); // tram board (~215m from osm_b)
    let stop_d = g.add_node(transit_stop("Stop D", 50.000, 4.040)); // tram alight (~72m from osm_dest)

    // Street edges between OSM nodes
    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: m,
                partial: false,
                foot: true,
                bike: true,
                car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: m,
                partial: false,
                foot: true,
                bike: true,
                car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }),
        );
    };
    add_street(&mut g, osm_origin, osm_ab, 718); // 0.010° × 71695
    add_street(&mut g, osm_ab, osm_b, 645); // 0.009°
    add_street(&mut g, osm_b, osm_cd, 789); // 0.011°
    add_street(&mut g, osm_cd, osm_dest, 789); // 0.011°

    // Stop-to-OSM snap edges (simulating GTFS ingestion partial edges)
    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(
            stop,
            EdgeData::Street(StreetEdgeData {
                origin: stop,
                destination: osm,
                length: m,
                partial: true,
                foot: true,
                bike: false,
                car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }),
        );
        g.add_edge(
            osm,
            EdgeData::Street(StreetEdgeData {
                origin: osm,
                destination: stop,
                length: m,
                partial: true,
                foot: true,
                bike: false,
                car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }),
        );
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // nearest OSM to stop_A: osm_origin
    add_snap(&mut g, stop_b, osm_b, 72); // nearest OSM to stop_B: osm_b
    add_snap(&mut g, stop_c, osm_b, 215); // nearest OSM to stop_C: osm_b
    add_snap(&mut g, stop_d, osm_dest, 72); // nearest OSM to stop_D: osm_dest

    // Transit edges (required by reconstruct() for timetable_segment lookup)
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 1362,
        }),
    );
    g.add_edge(
        stop_c,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_c,
            destination: stop_d,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 1290,
        }),
    );

    // Service: active every day
    g.add_transit_services(vec![all_days_service()]); // ServiceId(0)

    // Routes
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "T".into(),
            route_long_name: "Tram T".into(),
            route_type: RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);

    // Trips
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: bus_bikes,
        }, // TripId(0) = bus
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: tram_bikes,
        }, // TripId(1) = tram
    ]);

    // Timetable: one TripSegment per hop
    //   index 0: bus hop A→B   dep 09:00 arr 09:15
    //   index 1: tram hop C→D  dep 09:30 arr 09:45
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1800,
            arrival: 9 * 3600 + 2700,
            service_id: ServiceId(0),
        },
    ]);

    // Pattern 0: Bus, stops [stop_A, stop_B], 1 trip
    // Column-major stop times: index = stop_pos * n_trips + trip_idx
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
        }); // stop_A, trip 0
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        }); // stop_B, trip 0
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    // Pattern 1: Tram, stops [stop_C, stop_D], 1 trip
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        }); // stop_C, trip 1
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
        }); // stop_D, trip 1
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();

    (g, osm_origin, osm_dest)
}

// ── Multi-state RAPTOR (bike modes) ───────────────────────────────────────────

/// Two express legs spanning ~10 km each, so transit genuinely beats direct
/// cycling (the precondition for any bike+transit plan to be Pareto-optimal):
///   Leg 1 (route 0): stop_P → stop_Q, dep 09:00, arr 09:08
///   Leg 2 (route 1): stop_R → stop_S, dep 09:15, arr 09:23
/// stop_Q/stop_R are 143 m apart (a footpath transfer). Streets run the whole
/// way (foot+bike), so direct cycling is possible but takes ~80 min.
fn express_two_leg_graph(
    leg1_bikes: Option<bool>,
    leg2_bikes: Option<bool>,
) -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_q = g.add_node(osm_node("q", 50.000, 4.139));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.281));

    let stop_p = g.add_node(transit_stop("Stop P", 50.000, 4.001));
    let stop_q = g.add_node(transit_stop("Stop Q", 50.000, 4.140));
    let stop_r = g.add_node(transit_stop("Stop R", 50.000, 4.142));
    let stop_s = g.add_node(transit_stop("Stop S", 50.000, 4.280));

    // Streets are car-navigable too (the `car` flag is inert for foot/bike
    // routing, so this leaves the walk/bike tests unchanged while enabling the
    // car-mode tests to drive the same network).
    let both = |g: &mut Graph, a: NodeID, b: NodeID, m: usize, foot: bool, bike: bool| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot, bike, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot, bike, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    both(&mut g, osm_o, osm_q, 9967, true, true);
    both(&mut g, osm_q, osm_d, 10182, true, true);
    both(&mut g, stop_p, osm_o, 72, true, false);
    both(&mut g, stop_q, osm_q, 72, true, false);
    both(&mut g, stop_r, osm_q, 215, true, false);
    both(&mut g, stop_s, osm_d, 72, true, false);

    g.add_edge(stop_p, EdgeData::Transit(TransitEdgeData {
        origin: stop_p, destination: stop_q, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 9967,
    }));
    g.add_edge(stop_r, EdgeData::Transit(TransitEdgeData {
        origin: stop_r, destination: stop_s, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 1, len: 1 }, length: 9895,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "X1".into(), route_long_name: "Express 1".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None,
        },
        RouteInfo {
            route_short_name: "X2".into(), route_long_name: "Express 2".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0),
            bikes_allowed: leg1_bikes,
        },
        TripInfo {
            trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0),
            bikes_allowed: leg2_bikes,
        },
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 480, service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600 + 900, arrival: 9 * 3600 + 1380, service_id: ServiceId(0),
        },
    ]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_p, stop_q]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600 });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 480,
            departure: 9 * 3600 + 480,
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_r, stop_s]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1380,
            departure: 9 * 3600 + 1380,
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 1 });
    }

    g.build_raptor_index();

    (g, osm_o, osm_d)
}

fn transit_leg_count(p: &maas_rs::structures::plan::Plan) -> usize {
    p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count()
}

fn street_modes(p: &maas_rs::structures::plan::Plan) -> Vec<Mode> {
    p.legs
        .iter()
        .filter_map(|l| match l {
            PlanLeg::Walk(w) => Some(w.street_mode),
            _ => None,
        })
        .collect()
}

#[test]
fn default_modes_match_legacy_raptor() {
    let (g, origin, dest) = two_route_raptor_graph();
    let legacy = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);
    let modes = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &ActiveModes::default());

    assert_eq!(legacy.len(), modes.len());
    for (a, b) in legacy.iter().zip(modes.iter()) {
        assert_eq!(a.start, b.start);
        assert_eq!(a.end, b.end);
        assert_eq!(a.legs.len(), b.legs.len());
        assert_eq!(b.mode, Mode::WalkTransit);
    }
}

#[test]
fn bike_on_transit_requires_bikes_allowed_chain() {
    // Leg 1 allows bikes, leg 2's bikes_allowed is unknown (= not allowed).
    let (g, origin, dest) = express_two_leg_graph(Some(true), None);
    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    for p in &plans {
        assert!(
            transit_leg_count(p) <= 1,
            "BIKE_ON_TRANSIT must not board the no-bikes tram (got {} transit legs)",
            transit_leg_count(p)
        );
    }
}

#[test]
fn bike_transit_drops_bike_between_legs() {
    // Leg 1 allows bikes, leg 2 does not: ride to stop P, bike on leg 1, drop it
    // at the transfer, continue on leg 2.
    let (g, origin, dest) = express_two_leg_graph(Some(true), None);
    let am = ActiveModes::new(&[Mode::BikeTransit]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let two_leg = plans
        .iter()
        .find(|p| transit_leg_count(p) == 2)
        .expect("BIKE_TRANSIT should still produce the 2-leg plan by dropping the bike");
    assert_eq!(two_leg.mode, Mode::BikeTransit);

    let sm = street_modes(two_leg);
    assert_eq!(
        sm.first().copied(),
        Some(Mode::Bike),
        "access leg should be ridden (street modes: {sm:?})"
    );
    assert_eq!(
        sm.last().copied(),
        Some(Mode::Walk),
        "egress after dropping the bike must be walked (street modes: {sm:?})"
    );
}

#[test]
fn bike_on_transit_rides_egress() {
    let (g, origin, dest) = express_two_leg_graph(Some(true), Some(true));
    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let two_leg = plans
        .iter()
        .find(|p| transit_leg_count(p) == 2)
        .expect("both legs allow bikes: BIKE_ON_TRANSIT should produce the 2-leg plan");
    assert_eq!(two_leg.mode, Mode::BikeOnTransit);

    let sm = street_modes(two_leg);
    assert_eq!(
        sm.first().copied(),
        Some(Mode::Bike),
        "access leg should be ridden (street modes: {sm:?})"
    );
    assert_eq!(
        sm.last().copied(),
        Some(Mode::Bike),
        "egress must keep the bike (street modes: {sm:?})"
    );
}

#[test]
fn bike_access_seeds_dropped_state() {
    // No trip allows bikes: the only bike-mode option is park & ride (bike to
    // the first stop, drop it there, transit unrestricted afterwards).
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::BikeTransit]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let transit_plan = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("park & ride plan expected even when no trip allows bikes");
    assert_eq!(transit_plan.mode, Mode::BikeTransit);
    assert_eq!(street_modes(transit_plan).first().copied(), Some(Mode::Bike));
}

#[test]
fn car_dijkstra_drives_car_edges_and_walks_foot_connectors() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    let c = g.add_node(osm_node("c", 50.001, 4.000));
    // a→b is a road (driven at car speed). a→c is a foot-only stop connector,
    // crossed at walking speed (park & walk the last bit).
    g.add_edge(a, EdgeData::Street(StreetEdgeData {
        origin: a, destination: b, length: 1100, partial: false, foot: true, bike: false, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
    }));
    g.add_edge(a, EdgeData::Street(StreetEdgeData {
        origin: a, destination: c, length: 120, partial: false, foot: true, bike: true, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
    }));
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Car);
    assert_eq!(dist[&b], 100, "1100 m at 11.0 m/s = 100 s by car");
    assert_eq!(dist[&c], 100, "120 m foot-only connector at 1.2 m/s = 100 s");
}

#[test]
fn transit_modes_never_emit_zero_transit_plans() {
    // With a wide bike access radius, bike-access + a stop-to-stop transfer +
    // bike-egress can reach the destination using NO transit. Such a degenerate
    // path is just a direct ride and must not be emitted as a BIKE_ON_TRANSIT
    // plan (it also dodges the direct-duration filter, since it has 0 transit).
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.008)); // ~570 m away
    let stop_a = g.add_node(transit_stop("A", 50.000, 4.0011));
    let stop_b = g.add_node(transit_stop("B", 50.000, 4.0071)); // ~430 m from A: transferable
    let stop_far = g.add_node(transit_stop("Far", 50.000, 4.050));

    let road = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    road(&mut g, osm_o, osm_d, 570);
    connector(&mut g, osm_o, stop_a);
    connector(&mut g, osm_d, stop_b);

    // A real (but useless here) transit route, so the mode has something to scan.
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_far, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 5000,
    }));
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(), route_long_name: "Express".into(),
        route_type: RouteType::Bus, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: Some(true),
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: 9 * 3600 + 600, arrival: 9 * 3600 + 900, service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_far]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 900, departure: 9 * 3600 + 900 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    g.build_raptor_index();

    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    // A range query: the degenerate 0-transit path departs later than the direct
    // bike, so it survives Pareto on the departure axis (single-departure
    // dominance would otherwise hide the bug).
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
    let plans = g.raptor_range_tuned_rt_modes(
        osm_o, osm_d, 9 * 3600, 1800, 0, 0x7F, 10 * 60, &buckets, 300, &RealtimeIndex::new(), &am,
        &BikeCost::new(BikeProfile::default()),
    );
    // Direct modes (Walk/Bike/Car) are legitimately 0-transit; transit-labelled
    // modes must use transit.
    let is_transit_mode = |m: Mode| {
        matches!(
            m,
            Mode::WalkTransit
                | Mode::BikeTransit
                | Mode::BikeToTransit
                | Mode::BikeOnTransit
                | Mode::CarDropOff
                | Mode::CarPickup
        )
    };
    for p in &plans {
        if is_transit_mode(p.mode) {
            assert!(
                transit_leg_count(p) >= 1,
                "a transit-mode plan must use transit; got {:?} with {} transit legs",
                p.mode, transit_leg_count(p)
            );
        }
    }
    assert!(
        plans.iter().any(|p| is_transit_mode(p.mode)) || !plans.is_empty(),
        "sanity: some plan returned"
    );
}

#[test]
fn car_drop_off_not_poisoned_when_car_reaches_destination() {
    // When the car-access radius is wide enough to also reach a stop near the
    // destination, that stop is an egress stop too. A round-0 "drove there"
    // label there must NOT suppress the genuine park&ride transit journey.
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.010)); // boarding area, ~1.1 km
    let osm_d = g.add_node(osm_node("d", 50.000, 4.100)); // destination, ~10 km on
    let stop_board = g.add_node(transit_stop("Board", 50.000, 4.0101));
    let stop_dest = g.add_node(transit_stop("Dest", 50.000, 4.1001)); // near dest

    let road = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    road(&mut g, osm_o, osm_b, 1100); // short drive to the boarding area
    road(&mut g, osm_b, osm_d, 9900); // road continues all the way to the dest
    connector(&mut g, osm_b, stop_board);
    connector(&mut g, osm_d, stop_dest);

    g.add_edge(stop_board, EdgeData::Transit(TransitEdgeData {
        origin: stop_board, destination: stop_dest, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 9900,
    }));
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(), route_long_name: "Express".into(),
        route_type: RouteType::Bus, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None,
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: 9 * 3600 + 600, arrival: 9 * 3600 + 1800, service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_board, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1800, departure: 9 * 3600 + 1800 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    g.build_raptor_index();

    let am = ActiveModes::new(&[Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 10 * 60, &am);
    assert!(
        plans.iter().any(|p| p.mode == Mode::CarDropOff && transit_leg_count(p) >= 1),
        "park&ride must survive even though the car can reach a near-destination stop; got {:?}",
        plans.iter().map(|p| (p.mode, transit_leg_count(p))).collect::<Vec<_>>()
    );
}

#[test]
fn car_drop_off_with_foot_only_connectors() {
    // Real-data topology: stops join the street network with foot-only connectors.
    // Park & ride must drive the road, walk the connector to board, transit, then
    // walk the egress connector.
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_p = g.add_node(osm_node("p", 50.000, 4.090)); // ~6.4 km east, by car
    let osm_d = g.add_node(osm_node("d", 50.000, 4.181));
    let stop_p = g.add_node(transit_stop("P", 50.000, 4.0901));
    let stop_q = g.add_node(transit_stop("Q", 50.000, 4.1809));

    let road = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    road(&mut g, osm_o, osm_p, 6400);
    road(&mut g, osm_p, osm_d, 6450);
    connector(&mut g, osm_p, stop_p); // foot-only, as gtfs builds it
    connector(&mut g, osm_d, stop_q);

    g.add_edge(stop_p, EdgeData::Transit(TransitEdgeData {
        origin: stop_p, destination: stop_q, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 6400,
    }));
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(), route_long_name: "Express".into(),
        route_type: RouteType::Bus, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None,
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: 9 * 3600 + 1000, arrival: 9 * 3600 + 1300, service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_p, stop_q]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1000, departure: 9 * 3600 + 1000 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1300, departure: 9 * 3600 + 1300 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    g.build_raptor_index();

    let am = ActiveModes::new(&[Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 10 * 60, &am);
    let pr = plans.iter().find(|p| transit_leg_count(p) >= 1);
    assert!(
        pr.is_some(),
        "park & ride must work with foot-only stop connectors; got {:?}",
        plans.iter().map(|p| (p.mode, p.legs.len())).collect::<Vec<_>>()
    );
    assert_eq!(pr.unwrap().mode, Mode::CarDropOff);
}

#[test]
fn car_drop_off_does_not_starve_walk_transit() {
    // Park & ride (CarDropOff) and plain walk+transit are co-selected. The car
    // drives to a far hub and reaches the destination ~20 min before the slower,
    // foot-reachable 2-leg transit journey. The car's fast arrival must NOT poison
    // the global arrival cutoff and prune the walk journey mid-search: a heavier
    // (burden-2) state may never starve a lighter (burden-0) one's exploration.
    //
    //   o --140m road--> near --2010m road--> far          (car drives o→far)
    //   near ~stop_near : walkable boarding for the slow 2-leg line
    //   far  ~stop_far  : car-only-reachable boarding for the fast 1-leg line
    //
    //   walk:  o -walk-> stop_near -P1-> stop_mid -P2-> stop_dest   (rounds 1+2, arr 9:50)
    //   car:   o -drive-> stop_far  -Q -> stop_dest                  (round 1,   arr 9:30)
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_near = g.add_node(osm_node("near", 50.000, 4.002)); // ~143 m, walkable
    let osm_far = g.add_node(osm_node("far", 50.000, 4.030)); // ~2.15 km by car
    let osm_d = g.add_node(osm_node("d", 50.000, 4.100)); // destination, transit-only
    let stop_near = g.add_node(transit_stop("Near", 50.000, 4.0021));
    let stop_far = g.add_node(transit_stop("Far", 50.000, 4.0301));
    let stop_mid = g.add_node(transit_stop("Mid", 50.000, 4.060)); // transfer hub
    let stop_dest = g.add_node(transit_stop("Dest", 50.000, 4.1001));

    let road = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    road(&mut g, osm_o, osm_near, 140); // short walk/drive to the local stop
    road(&mut g, osm_near, osm_far, 2010); // car continues to the far hub (foot too slow)
    connector(&mut g, osm_near, stop_near);
    connector(&mut g, osm_far, stop_far);
    connector(&mut g, osm_d, stop_dest);

    // Transit edges (one per boarded segment), mirroring the gtfs ingestion shape.
    g.add_edge(stop_near, EdgeData::Transit(TransitEdgeData {
        origin: stop_near, destination: stop_mid, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 4000,
    }));
    g.add_edge(stop_mid, EdgeData::Transit(TransitEdgeData {
        origin: stop_mid, destination: stop_dest, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 1, len: 1 }, length: 3000,
    }));
    g.add_edge(stop_far, EdgeData::Transit(TransitEdgeData {
        origin: stop_far, destination: stop_dest, route_id: RouteId(2),
        timetable_segment: TimetableSegment { start: 2, len: 1 }, length: 7000,
    }));
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo { route_short_name: "P1".into(), route_long_name: "Local 1".into(), route_type: RouteType::Bus, agency_id: AgencyId(0), route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "P2".into(), route_long_name: "Local 2".into(), route_type: RouteType::Bus, agency_id: AgencyId(0), route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "Q".into(), route_long_name: "Express".into(), route_type: RouteType::Bus, agency_id: AgencyId(0), route_color: None, route_text_color: None },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(2), service_id: ServiceId(0), bikes_allowed: None },
    ]);
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 600, arrival: 9 * 3600 + 1500, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 1800, arrival: 9 * 3600 + 3000, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 300, arrival: 9 * 3600 + 1800, service_id: ServiceId(0) },
    ]);
    {
        // Pattern 0: P1  stop_near → stop_mid   (dep 9:10, arr 9:25)
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_near, stop_mid]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1500, departure: 9 * 3600 + 1500 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });

        // Pattern 1: P2  stop_mid → stop_dest   (dep 9:30, arr 9:50)
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_mid, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1800, departure: 9 * 3600 + 1800 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 3000, departure: 9 * 3600 + 3000 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 1 });

        // Pattern 2: Q  stop_far → stop_dest    (dep 9:05, arr 9:30) — fast car line
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_far, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 300, departure: 9 * 3600 + 300 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1800, departure: 9 * 3600 + 1800 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(2), num_trips: 1 });
    }
    g.build_raptor_index();

    let am = ActiveModes::new(&[Mode::WalkTransit, Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 300, &am);

    let summary: Vec<_> = plans.iter().map(|p| (p.mode, transit_leg_count(p))).collect();
    assert!(
        plans.iter().any(|p| p.mode == Mode::WalkTransit && transit_leg_count(p) == 2),
        "walk+transit must survive even when a faster park&ride sets the cutoff; got {summary:?}"
    );
    assert!(
        plans.iter().any(|p| p.mode == Mode::CarDropOff && transit_leg_count(p) >= 1),
        "park & ride must still be offered alongside walk+transit; got {summary:?}"
    );
}

#[test]
fn car_cannot_resume_driving_after_walking() {
    // a --road--> b --foot only--> c --car-only road--> d
    // A car may drive a→b, then park and walk b→c, but it can NEVER pick the car
    // back up to drive c→d. So d must be unreachable by car.
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    let c = g.add_node(osm_node("c", 50.000, 4.002));
    let d = g.add_node(osm_node("d", 50.000, 4.003));
    let edge = |g: &mut Graph, x: NodeID, y: NodeID, foot: bool, car: bool| {
        g.add_edge(x, EdgeData::Street(StreetEdgeData {
            origin: x, destination: y, length: 110, partial: false, foot, bike: false, car, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    edge(&mut g, a, b, true, true);   // road
    edge(&mut g, b, c, true, false);  // foot-only connector (park & walk)
    edge(&mut g, c, d, false, true);  // car-only road
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Car);
    assert!(dist.contains_key(&b), "b reachable by car");
    assert!(dist.contains_key(&c), "c reachable by parking and walking");
    assert!(!dist.contains_key(&d), "a parked car cannot be resumed to drive c→d");
}

#[test]
fn car_dijkstra_reaches_stop_via_foot_connector() {
    // Real GTFS connects stops to the street network with foot-only edges. A car
    // must still reach the stop by driving the road, then walking the connector.
    let mut g = Graph::new();
    let o = g.add_node(osm_node("o", 50.000, 4.000));
    let p = g.add_node(osm_node("p", 50.000, 4.010));
    let stop = g.add_node(transit_stop("S", 50.000, 4.0101));
    // o→p road (car), p→stop foot-only connector (as gtfs ingestion builds it).
    g.add_edge(o, EdgeData::Street(StreetEdgeData {
        origin: o, destination: p, length: 1100, partial: false, foot: true, bike: false, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
    }));
    g.add_edge(p, EdgeData::Street(StreetEdgeData {
        origin: p, destination: stop, length: 12, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
    }));
    g.build_raptor_index();

    let near = g.nearby_stops_profile(o, 9600, StreetProfile::Car);
    assert!(
        !near.is_empty(),
        "car must reach the stop by driving then walking the foot connector: {near:?}"
    );
}

#[test]
fn foot_dijkstra_ignores_car_only_edges() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    g.add_edge(a, EdgeData::Street(StreetEdgeData {
        origin: a, destination: b, length: 100, partial: false, foot: false, bike: false, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
    }));
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Foot);
    assert!(!dist.contains_key(&b), "pedestrians must not use car-only roads");
}

#[test]
fn car_direct_drives_the_whole_way() {
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::Car]);
    let plans = g.raptor_modes(origin, dest, 9 * 3600, 0, 0x7F, 10 * 60, &am);

    assert_eq!(plans.len(), 1, "CAR alone should yield exactly the direct drive");
    assert_eq!(plans[0].mode, Mode::Car);
    assert_eq!(street_modes(&plans[0]), vec![Mode::Car]);
}

#[test]
fn car_drop_off_is_park_and_ride() {
    // Drive to the first station, park, ride transit, then walk to the door.
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::CarDropOff]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let pr = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("park & ride plan expected");
    assert_eq!(pr.mode, Mode::CarDropOff);
    let sm = street_modes(pr);
    assert_eq!(sm.first().copied(), Some(Mode::Car), "access must be driven ({sm:?})");
    assert_eq!(sm.last().copied(), Some(Mode::Walk), "egress must be walked ({sm:?})");
}

#[test]
fn car_pickup_is_kiss_and_ride() {
    // Walk to the first station, ride transit, then get picked up by car.
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::CarPickup]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let kr = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("kiss & ride plan expected");
    assert_eq!(kr.mode, Mode::CarPickup);
    let sm = street_modes(kr);
    assert_eq!(sm.first().copied(), Some(Mode::Walk), "access must be walked ({sm:?})");
    assert_eq!(sm.last().copied(), Some(Mode::Car), "egress must be driven ({sm:?})");
}

#[test]
fn raptor_second_transit_leg_has_transfer_risk() {
    let (g, origin, dest) = two_route_raptor_graph();

    // Depart at 08:00 on a Monday (date=0 = 2000-01-01 which is a Saturday, but
    // all_days_service has days_of_week=0x7F so every weekday mask passes)
    let plans = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);

    assert!(!plans.is_empty(), "Expected at least one plan");

    for (i, p) in plans.iter().enumerate() {
        let leg_summary: Vec<String> = p
            .legs
            .iter()
            .map(|l| match l {
                PlanLeg::Walk(w) => format!("Walk({}s)", w.duration),
                PlanLeg::Transit(t) => format!(
                    "Transit(risk={:?})",
                    t.transfer_risk.as_ref().map(|r| r.reliability)
                ),
            })
            .collect();
        eprintln!("Plan {}: {:?}", i, leg_summary);
    }

    // Find the plan that uses both transit routes (Bus + Tram)
    let two_leg = plans.iter().find(|p| {
        p.legs
            .iter()
            .filter(|l| matches!(l, PlanLeg::Transit(_)))
            .count()
            == 2
    });
    let plan = two_leg.expect("Expected a plan with 2 transit legs (Bus → transfer → Tram)");

    let transit: Vec<_> = plan
        .legs
        .iter()
        .filter_map(|l| {
            if let PlanLeg::Transit(t) = l {
                Some(t)
            } else {
                None
            }
        })
        .collect();

    assert!(
        transit[0].transfer_risk.is_none(),
        "First transit leg (Bus) should have no transfer risk — boarded from walk");
    
    assert!(
        transit[1].transfer_risk.is_some(),
        "Second transit leg (Tram) should have transfer risk — boarded after Bus transfer");

    // The first leg now records its downstream connection so its alternatives can
    // be scored for the outbound transfer onto the Tram.
    assert!(
        transit[0].following_route_type.is_some(),
        "First transit leg should know the following leg's route type");
    assert!(
        transit[0].following_margin_secs.is_some(),
        "First transit leg should record its outbound connection margin");
    assert!(
        transit[1].following_route_type.is_none(),
        "Last transit leg has no following connection");
}

#[test]
fn raptor_transfer_risk_reliability_is_one_without_delay_model() {
    let (g, origin, dest) = two_route_raptor_graph();
    let plans = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);

    let two_leg = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("Expected a 2-transit-leg plan");

    let tram_leg = two_leg
        .legs
        .iter()
        .filter_map(|l| {
            if let PlanLeg::Transit(t) = l {
                Some(t)
            } else {
                None
            }
        })
        .nth(1)
        .unwrap();

    let risk = tram_leg.transfer_risk.as_ref().unwrap();
    assert!(
        (risk.reliability - 1.0).abs() < 1e-6,
        "Without a delay model reliability should default to 1.0, got {}",
        risk.reliability
    );
    assert_eq!(
        risk.scheduled_departure,
        9 * 3600 + 1800,
        "scheduled_departure should be tram departure time"
    );
}

/// The transfer Bus → Tram convolves BOTH delay distributions: the feeder (Bus)
/// arrival and the boarding (Tram) departure. The reconstructed reliability must
/// equal `feeder.prob_on_time_vs(Some(board), margin)`, and the leg must carry its
/// own route type so the boarding distribution can be looked up.
#[test]
fn raptor_transfer_risk_merges_feeder_and_boarding_delays() {
    let (mut g, origin, dest) = two_route_raptor_graph();

    // Feeder (Bus) stair CDF and a boarding (Tram) model with heavy early mass, so
    // the convolution measurably differs from the feeder-only result at any margin.
    let bus = DelayCDF { bins: vec![(0, 0.1), (300, 0.4), (600, 0.6), (900, 0.8), (1200, 1.0)] };
    let tram = DelayCDF { bins: vec![(-600, 0.5), (0, 1.0)] };
    let mut models = HashMap::new();
    models.insert(RouteType::Bus, bus.clone());
    models.insert(RouteType::Tramway, tram.clone());
    g.set_transit_delay_models(models);

    let plans = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);
    let two_leg = plans
        .iter()
        .find(|p| {
            p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2
        })
        .expect("Expected a 2-transit-leg plan");

    let tram_leg = two_leg
        .legs
        .iter()
        .filter_map(|l| if let PlanLeg::Transit(t) = l { Some(t) } else { None })
        .nth(1)
        .unwrap();

    assert_eq!(
        tram_leg.route_type,
        Some(RouteType::Tramway),
        "Boarding leg must carry its own route type for the convolution lookup"
    );

    let risk = tram_leg.transfer_risk.as_ref().unwrap();
    let margin = risk.scheduled_departure as i32 - tram_leg.preceding_arrival.unwrap() as i32;

    let feeder_only = bus.prob_on_time(margin);
    let expected = bus.prob_on_time_vs(Some(&tram), margin);
    assert!(
        (expected - feeder_only).abs() > 1e-6,
        "test setup should exercise the convolution (margin {margin}): merged {expected} vs feeder-only {feeder_only}"
    );
    assert!(
        (risk.reliability - expected).abs() < 1e-6,
        "reliability {} should equal the two-delay convolution {expected} (margin {margin})",
        risk.reliability
    );
}

// ── Three-pass RAPTOR: backward tightening ────────────────────────────────────

/// Like `two_route_raptor_graph` but the Bus has TWO trips:
///   Trip 0: dep stop_A 08:00, arr stop_B 08:15  (early, unnecessary)
///   Trip 1: dep stop_A 09:00, arr stop_B 09:15  (later, still connects to tram)
/// The Tram still has one trip: dep stop_C 09:30, arr stop_D 09:45.
///
/// With forward-only RAPTOR the first transit leg boards at 08:00.
/// After three-pass backward tightening it should board at 09:00 instead,
/// so the user can depart home 1h later.
fn two_route_multi_trip_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_ab = g.add_node(osm_node("ab", 50.000, 4.010));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.019));
    let osm_cd = g.add_node(osm_node("cd", 50.000, 4.030));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.020));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.022));
    let stop_d = g.add_node(transit_stop("Stop D", 50.000, 4.040));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m,
            partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m,
            partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m,
            partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m,
            partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

    // Bus edge: timetable_segment has len=2 (two bus trips)
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 2 },
        length: 1362,
    }));
    // Tram edge: timetable_segment has len=1
    g.add_edge(stop_c, EdgeData::Transit(TransitEdgeData {
        origin: stop_c, destination: stop_d, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 2, len: 1 },
        length: 1290,
    }));

    g.add_transit_services(vec![all_days_service()]);

    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "1".into(), route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None,
        },
        RouteInfo {
            route_short_name: "T".into(), route_long_name: "Tram T".into(),
            route_type: RouteType::Tramway, agency_id: AgencyId(0),
            route_color: None, route_text_color: None,
        },
    ]);

    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // TripId(0) = bus 08:00
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // TripId(1) = bus 09:00
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None }, // TripId(2) = tram
    ]);

    // Timetable departures (absolute indices used by transit edges)
    // idx 0: bus trip 0 dep 08:00
    // idx 1: bus trip 1 dep 09:00
    // idx 2: tram dep 09:30
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 8 * 3600, arrival: 8 * 3600 + 900, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600, arrival: 9 * 3600 + 900, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 1800, arrival: 9 * 3600 + 2700, service_id: ServiceId(0) },
    ]);

    // Pattern 0: Bus, stops [stop_A, stop_B], 2 trips (sorted by departure)
    // Column-major: col 0 = stop_A times for trips 0,1; col 1 = stop_B times for trips 0,1
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });

        let sts = g.transit_pattern_stop_times_len();
        // stop_A col (2 entries): trip0 dep 08:00, trip1 dep 09:00
        g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600, departure: 8 * 3600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600 });
        // stop_B col (2 entries): trip0 arr 08:15, trip1 arr 09:15
        g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600 + 900, departure: 8 * 3600 + 900 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 900, departure: 9 * 3600 + 900 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });

        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 2 });
    }

    // Pattern 1: Tram, stops [stop_C, stop_D], 1 trip
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1800, departure: 9 * 3600 + 1800 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 2700, departure: 9 * 3600 + 2700 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 1 });
    }

    g.build_raptor_index();

    (g, osm_origin, osm_dest)
}

/// Verifies that three-pass RAPTOR tightens the first transit leg when a later
/// trip is available and still connects to the downstream leg.
///
/// Setup: Bus has two trips (08:00 and 09:00). Tram departs at 09:30.
/// User departs at 07:00.
/// Forward RAPTOR boards the bus at 08:00 (first available).
/// After backward tightening, the plan should use the 09:00 bus because it
/// still connects to the 09:30 tram (arrives stop_B at 09:15, ~179s walk to
/// stop_C, boards tram at 09:30).
#[test]
fn raptor_backward_tightening_shifts_first_leg_to_later_trip() {
    let (g, origin, dest) = two_route_multi_trip_graph();

    // Depart at 07:00 — both bus trips are reachable from forward pass
    let plans = g.raptor(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60);

    assert!(!plans.is_empty(), "Expected at least one plan");

    // Find the two-leg plan (Bus + Tram)
    let two_leg_plan = plans.iter().find(|p| {
        p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2
    }).expect("Expected a Bus+Tram plan");

    let bus_leg = two_leg_plan.legs.iter().find_map(|l| {
        if let PlanLeg::Transit(t) = l { Some(t) } else { None }
    }).unwrap();

    assert_eq!(
        bus_leg.start,
        9 * 3600,
        "Backward tightening should shift bus boarding to 09:00 (not 08:00); got {}s",
        bus_leg.start
    );
}

/// Realtime (differential): delaying *only* the tram trip at its alighting stop
/// shifts the plan's arrival by exactly that delay, while delaying an unrelated
/// trip leaves the arrival unchanged — proving the delay is applied per-trip, not
/// uniformly. (Compact stop indices follow node insertion order: A=0,B=1,C=2,D=3;
/// the tram is TripId(2), alighting at stop_D = compact 3.)
#[test]
fn raptor_realtime_delay_is_per_trip() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900);
    let base_end = base.iter().map(|p| p.end).min().unwrap();

    // Delay only the tram (TripId 2) at stop_D (compact 3) by 600s.
    let d: i32 = 600;
    let tram_delay = RealtimeIndex::from_delays(1, [((TripId(2), 3u32), d)]);
    let delayed =
        g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &tram_delay);
    let delayed_end = delayed.iter().map(|p| p.end).min().unwrap();
    assert_eq!(
        delayed_end,
        base_end + d as u32,
        "delaying the tram (the decisive last leg) must push arrival by {d}s \
         (base {base_end}, delayed {delayed_end})"
    );

    // Delaying a trip that is NOT on the chosen path leaves the arrival unchanged.
    let unrelated = RealtimeIndex::from_delays(1, [((TripId(0), 0u32), 600)]);
    let same =
        g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &unrelated);
    assert_eq!(same.iter().map(|p| p.end).min().unwrap(), base_end);
}

/// Realtime reaches the reconstructed leg: delaying the tram at its alighting
/// stop shifts that leg's `end` (effective) while `scheduled_end` keeps the
/// timetable value and `realtime` is flagged true; the un-delayed bus leg stays
/// scheduled with `realtime == false`.
#[test]
fn raptor_realtime_shows_on_leg_times() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    // Delay only the tram (TripId 2) at stop_D (compact 3) by 600s.
    let rt = RealtimeIndex::from_delays(1, [((TripId(2), 3u32), 600)]);
    let plans = g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);

    let plan = plans
        .iter()
        .find(|p| {
            p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2
        })
        .expect("a bus+tram plan");

    let mut saw_tram = false;
    let mut saw_bus = false;
    for leg in &plan.legs {
        if let PlanLeg::Transit(t) = leg {
            if t.trip_id == TripId(2) {
                saw_tram = true;
                assert!(t.realtime, "tram leg should be flagged realtime");
                assert_eq!(t.scheduled_end, 9 * 3600 + 2700, "tram scheduled arrival kept");
                assert_eq!(t.end, t.scheduled_end + 600, "tram effective arrival = scheduled + 600");
            } else {
                saw_bus = true;
                assert!(!t.realtime, "bus leg has no realtime data");
                assert_eq!(t.start, t.scheduled_start, "bus leg unshifted");
            }
        }
    }
    assert!(saw_tram && saw_bus, "expected both a tram and a bus leg");
}

/// STIB pointid → stop resolution: an exact stop_id match wins; otherwise every
/// platform-suffixed stop whose id is prefixed by the pointid is returned.
#[test]
fn stib_stop_indices_exact_and_prefix() {
    let mut g = Graph::new();
    g.raptor.transit_stop_ids =
        vec!["0470701".into(), "0470101".into(), "1234".into(), "0470".into()];
    g.raptor.build_runtime_indices();

    // Exact match takes priority (does not also pull in the prefixed platforms).
    assert_eq!(g.stib_stop_indices("0470"), vec![3]);
    // Exact match on a non-prefix id.
    assert_eq!(g.stib_stop_indices("1234"), vec![2]);
    // Prefix match: pointid with no exact id → all platform-suffixed stops.
    let mut pref = g.stib_stop_indices("04707");
    pref.sort();
    assert_eq!(pref, vec![0]);
    // Unknown point resolves to nothing.
    assert!(g.stib_stop_indices("9999").is_empty());
}

/// Realtime: a uniform delay applied to every trip at every stop must shift the
/// fastest plan's arrival by exactly that delay (walk legs are unaffected), and
/// an empty index must reproduce the schedule-only result.
#[test]
fn raptor_realtime_delay_shifts_arrival() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900);
    assert!(!base.is_empty(), "expected a baseline plan");
    let base_min_end = base.iter().map(|p| p.end).min().unwrap();

    // Empty index reproduces the baseline exactly.
    let empty = RealtimeIndex::new();
    let same = g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &empty);
    assert_eq!(same.iter().map(|p| p.end).min().unwrap(), base_min_end);

    // Delay every (trip, stop) by D seconds.
    let d: i32 = 300;
    let n_trips = g.get_transit_trips_size() as u32;
    let mut delays = Vec::new();
    for t in 0..n_trips {
        for stop in 0..64u32 {
            delays.push(((TripId(t), stop), d));
        }
    }
    let rt = RealtimeIndex::from_delays(1, delays);

    let delayed = g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    assert!(!delayed.is_empty(), "expected a plan under realtime delay");
    let rt_min_end = delayed.iter().map(|p| p.end).min().unwrap();

    assert_eq!(
        rt_min_end,
        base_min_end + d as u32,
        "uniform +{d}s realtime delay should push the fastest arrival by exactly {d}s \
         (base {base_min_end}, rt {rt_min_end})"
    );
}

/// Verifies that tightening still preserves a valid transfer: the bus arrives
/// at stop_B before the tram departs from stop_C (accounting for walk time).
#[test]
fn raptor_backward_tightening_preserves_valid_connection() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let plans = g.raptor(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60);

    let two_leg_plan = plans.iter().find(|p| {
        p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2
    }).expect("Expected a Bus+Tram plan");

    let transit_legs: Vec<_> = two_leg_plan.legs.iter().filter_map(|l| {
        if let PlanLeg::Transit(t) = l { Some(t) } else { None }
    }).collect();

    assert_eq!(transit_legs.len(), 2);
    // Bus arrives at 09:15, tram departs at 09:30 — connection is valid
    assert!(
        transit_legs[0].end <= transit_legs[1].start,
        "Bus end ({}) must be ≤ tram start ({})",
        transit_legs[0].end, transit_legs[1].start
    );
    assert_eq!(transit_legs[1].start, 9 * 3600 + 1800, "Tram should still depart at 09:30");
}

// ── Pattern shape storage ─────────────────────────────────────────────────────

#[test]
fn test_pattern_shape_stored_and_retrieved() {
    let mut g = Graph::new();
    let pts = vec![
        LatLng { latitude: 1.0, longitude: 1.0 },
        LatLng { latitude: 2.0, longitude: 2.0 },
        LatLng { latitude: 3.0, longitude: 3.0 },
        LatLng { latitude: 4.0, longitude: 4.0 },
        LatLng { latitude: 5.0, longitude: 5.0 },
    ];
    g.push_transit_pattern_shape(pts, vec![0u32, 4u32]);
    let (shape, idx) = g.get_pattern_shape(0).expect("should have shape for pattern 0");
    assert_eq!(shape.len(), 5);
    assert_eq!(idx, &[0u32, 4u32]);
}

#[test]
fn test_pattern_shape_empty_returns_none() {
    let mut g = Graph::new();
    g.push_transit_pattern_shape(vec![], vec![]);
    assert!(g.get_pattern_shape(0).is_none());
}

#[test]
fn test_pattern_shape_out_of_bounds_returns_none() {
    let g = Graph::new();
    assert!(g.get_pattern_shape(99).is_none());
}

// ── raptor_range ──────────────────────────────────────────────────────────────

/// Builds a single-route graph with N trips at 30-minute intervals.
///
/// Layout:
///   osm_origin (50.000, 4.000)   ─72 m─  stop_A (50.000, 4.001)
///                                          │  (bus, 6 trips every 30 min)
///   osm_dest   (50.000, 4.100)   ─72 m─  stop_B (50.000, 4.099)
///
/// Trip i departs stop_A at (09:00 + i*30 min), arrives stop_B 30 min later.
fn single_route_many_trips_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest   = g.add_node(osm_node("dest",   50.000, 4.100));
    let stop_a     = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // ~72 m from origin
    let stop_b     = g.add_node(transit_stop("Stop B", 50.000, 4.099)); // ~72 m from dest

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    // Connect origin ↔ dest via a long street (so walk-only is expensive)
    add_street(&mut g, osm_origin, osm_dest, 7200); // 7 200 m ≈ 1 h walk

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // 72 m / 1.2 m/s = 60 s walk
    add_snap(&mut g, stop_b, osm_dest,   72);

    // Transit edge (needed by reconstruct for timetable_segment lookup)
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b,
        route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 6 },
        length: 7000,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "42".into(),
        route_long_name: "Bus 42".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);

    // 6 trips: TripId 0..5
    g.add_transit_trips(
        (0..6u32).map(|_| TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }).collect(),
    );

    // TripSegments (one per trip, single A→B hop)
    let base = 9 * 3600u32; // 09:00
    g.add_transit_departures(
        (0..6u32).map(|i| TripSegment {
            trip_id: TripId(i),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: base + i * 1800,        // 09:00, 09:30, 10:00 …
            arrival:   base + i * 1800 + 1800, // arrives 30 min later
            service_id: ServiceId(0),
        }).collect(),
    );

    // Pattern 0: 2 stops × 6 trips, column-major stop times.
    // Column for stop_A (pos 0): indices 0..6 (trips 0..5 at stop_A)
    // Column for stop_B (pos 1): indices 6..12 (trips 0..5 at stop_B)
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 0..6u32 {
            g.push_transit_pattern_trip(TripId(i));
        }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 6 });

        let sts = g.transit_pattern_stop_times_len();
        // Stop A column (pos 0, 6 trips)
        for i in 0..6u32 {
            let t = base + i * 1800;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        // Stop B column (pos 1, 6 trips)
        for i in 0..6u32 {
            let t = base + i * 1800 + 1800;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 12 });

        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 6 });
    }

    g.build_raptor_index();
    (g, osm_origin, osm_dest)
}

#[test]
fn raptor_range_returns_multiple_plans_across_window() {
    let (g, origin, dest) = single_route_many_trips_graph();

    // Query at 09:00, 3-hour window — buses every 30 min → should get multiple plans.
    // collect_interesting_times caps at 5, so expect exactly 5.
    let plans = g.raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60);

    assert!(
        plans.len() > 1,
        "raptor_range should return multiple Pareto-optimal plans for a 3-hour window \
         with buses every 30 min, but got {} plan(s)",
        plans.len(),
    );

    // Each plan should have a different departure time (Pareto-distinct)
    let mut starts: Vec<u32> = plans.iter().map(|p| p.start).collect();
    starts.sort_unstable();
    starts.dedup();
    assert_eq!(
        starts.len(), plans.len(),
        "All plans should have distinct departure times; got starts={:?}",
        starts,
    );

    // Plans should be sorted by departure (ascending) or at least all within the window
    for p in &plans {
        assert!(
            p.start >= 9 * 3600,
            "Plan departs before query time: start={}",
            p.start
        );
        assert!(
            p.start < (9 + 3) * 3600,
            "Plan departs outside 3-hour window: start={}",
            p.start
        );
    }
}

#[test]
fn raptor_range_plans_are_pareto_optimal() {
    let (g, origin, dest) = single_route_many_trips_graph();
    let plans = g.raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60);

    // No plan should be dominated by another:
    // A dominates B iff tc_A <= tc_B && end_A <= end_B && start_A >= start_B (strict in ≥1)
    for (i, a) in plans.iter().enumerate() {
        let tc_a = a.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count().saturating_sub(1);
        for (j, b) in plans.iter().enumerate() {
            if i == j { continue; }
            let tc_b = b.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count().saturating_sub(1);
            let a_dominates_b = tc_a <= tc_b && a.end <= b.end && a.start >= b.start
                && (tc_a < tc_b || a.end < b.end || a.start > b.start);
            assert!(
                !a_dominates_b,
                "Plan {} (start={}, end={}, tc={}) dominates plan {} (start={}, end={}, tc={}) — Pareto filter is broken",
                i, a.start, a.end, tc_a, j, b.start, b.end, tc_b,
            );
        }
    }
}

/// `raptor_range` must be deterministic: the same query returns the exact same
/// ordered plan sequence on every call. Guards the parallel departure-time fan-out
/// — concurrent execution must not reorder, drop, or duplicate plans.
#[test]
fn raptor_range_is_deterministic_across_runs() {
    let (g, origin, dest) = single_route_many_trips_graph();
    let run = || -> Vec<(u32, u32)> {
        g.raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
            .iter()
            .map(|p| (p.start, p.end))
            .collect()
    };
    let a = run();
    let b = run();
    assert!(!a.is_empty(), "expected at least one plan");
    assert_eq!(a, b, "raptor_range must return an identical ordered plan sequence on repeat calls");
}

/// THE oracle gate for self-pruning rRAPTOR: the carried-grid, latest-first driver
/// (`raptor_range`) must produce the SAME 4-D Pareto set (departure, arrival,
/// transfers) as independent from-scratch passes (`raptor_range_independent`).
/// Extra keys in self-pruning ⇒ fabrication (FM-1); missing keys ⇒ over-pruning
/// (FM-2). Dense single route (many departures) stresses the departure×arrival core.
#[test]
fn self_pruning_range_equals_independent_single_route() {
    use std::collections::HashSet;
    let (g, origin, dest) = single_route_many_trips_graph();
    let key = |p: &maas_rs::structures::plan::Plan| {
        (p.start, p.end, p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count())
    };
    let sp: HashSet<_> = g
        .raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter().map(key).collect();
    let oracle: HashSet<_> = g
        .raptor_range_independent(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter().map(key).collect();
    assert!(!oracle.is_empty(), "oracle must produce plans");
    assert_eq!(sp, oracle, "self-pruning range != independent-passes (single route, 4-D key)");
}

/// Same oracle gate on a two-route graph that admits transfers, so transfer
/// preservation across departures is exercised (the only_nv class the 4-D contract
/// keeps and the 3-D contract would have dropped).
#[test]
fn self_pruning_range_equals_independent_two_route() {
    use std::collections::HashSet;
    let (g, origin, dest) = two_route_raptor_graph();
    let key = |p: &maas_rs::structures::plan::Plan| {
        (p.start, p.end, p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count())
    };
    let sp: HashSet<_> = g
        .raptor_range(origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter().map(key).collect();
    let oracle: HashSet<_> = g
        .raptor_range_independent(origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter().map(key).collect();
    assert!(!oracle.is_empty(), "oracle must produce plans");
    assert_eq!(sp, oracle, "self-pruning range != independent-passes (two route, 4-D key)");
}

/// Regression test: raptor_range must not discard the probe plan when
/// high-frequency dead-end patterns at the origin stop fill the entire
/// `collect_interesting_times` cap before the connecting pattern appears.
///
/// Layout:
///   osm_origin (50.000, 4.000) ─72m─ stop_A (50.000, 4.001)
///   osm_dest   (50.000, 4.100) ─72m─ stop_B (50.000, 4.099)
///                                     stop_C (50.000, 5.000)  ← dead-end, far from dest
///
/// Pattern 0 (dead-end): stop_A → stop_C, 5 trips every 5 min from 09:00.
///   These fill the first 5 slots in collect_interesting_times.
/// Pattern 1 (connecting): stop_A → stop_B, 3 trips at 09:30, 10:30, 11:30.
///   Without the fix, these are never tried (cap exhausted by pattern 0).
///
/// Expected: raptor_range returns ≥ 1 plan (connecting trips found).
#[test]
fn raptor_range_connecting_pattern_not_starved_by_dead_end_pattern() {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest   = g.add_node(osm_node("dest",   50.000, 4.100));
    let stop_a     = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // 72m / 60s from origin
    let stop_b     = g.add_node(transit_stop("Stop B", 50.000, 4.099)); // 72m / 60s from dest
    let stop_c     = g.add_node(transit_stop("Stop C", 50.000, 5.000)); // far from dest

    // Streets
    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    add_street(&mut g, osm_origin, osm_dest, 7200); // long direct walk (1 h)

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // 60s walk
    add_snap(&mut g, stop_b, osm_dest,   72); // 60s walk
    // stop_c has no snap edge to osm nodes (it's remote)

    // Transit edges (needed by reconstruct)
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_c,
        route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 5 },
        length: 80_000,
    }));
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b,
        route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 5, len: 3 },
        length: 7000,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo { route_short_name: "99".into(), route_long_name: "Dead-end".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "42".into(), route_long_name: "Connecting".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None },
    ]);

    // 5 dead-end trips (pattern 0) + 3 connecting trips (pattern 1)
    g.add_transit_trips(
        (0..8u32).map(|i| TripInfo {
            trip_headsign: None,
            route_id: if i < 5 { RouteId(0) } else { RouteId(1) },
            service_id: ServiceId(0),
            bikes_allowed: None,
        }).collect(),
    );

    // TripSegments (one per trip)
    let base = 9 * 3600u32;
    // Dead-end: 5 trips departing stop_A at 09:01, 09:02, 09:03, 09:04, 09:05.
    // earliest_at_stop = 09:00 + 60s walk = 09:01, so all 5 are within range.
    // Origin departure times = stop_A dep - 60s = 09:00, 09:01, 09:02, 09:03, 09:04.
    // These 5 fill collect_interesting_times' cap of 5 entirely, leaving no room
    // for the connecting pattern's trips (first at 09:30 → origin dep 09:29).
    let mut segs: Vec<TripSegment> = (0..5u32).map(|i| TripSegment {
        trip_id: TripId(i),
        origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: base + 60 + i * 60,         // 09:01, 09:02, 09:03, 09:04, 09:05
        arrival:   base + 60 + i * 60 + 3600,  // 60 min later at stop_C
        service_id: ServiceId(0),
    }).collect();
    // Connecting: 3 trips at 09:30, 10:30, 11:30 (stop_A → stop_B, 30 min)
    segs.extend((0..3u32).map(|i| TripSegment {
        trip_id: TripId(5 + i),
        origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: base + 1800 + i * 3600,        // 09:30, 10:30, 11:30
        arrival:   base + 1800 + i * 3600 + 1800, // 30 min later at stop_B
        service_id: ServiceId(0),
    }));
    g.add_transit_departures(segs);

    // Pattern 0 (dead-end): stop_A × stop_C, 5 trips, column-major
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_c]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 0..5u32 { g.push_transit_pattern_trip(TripId(i)); }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 5 });

        let sts = g.transit_pattern_stop_times_len();
        // stop_A column (pos 0): departures at 09:01, 09:02, 09:03, 09:04, 09:05
        for i in 0..5u32 {
            let t = base + 60 + i * 60;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        // stop_C column (pos 1)
        for i in 0..5u32 {
            let t = base + 60 + i * 60 + 3600;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 10 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 5 });
    }

    // Pattern 1 (connecting): stop_A × stop_B, 3 trips, column-major
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 5..8u32 { g.push_transit_pattern_trip(TripId(i)); }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 3 });

        let sts = g.transit_pattern_stop_times_len();
        // stop_A column (pos 0)
        for i in 0..3u32 {
            let t = base + 1800 + i * 3600;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        // stop_B column (pos 1)
        for i in 0..3u32 {
            let t = base + 1800 + i * 3600 + 1800;
            g.push_transit_pattern_stop_time(StopTime { arrival: t, departure: t });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 3 });
    }

    g.build_raptor_index();

    // 180-min window from 09:00, min_access=10 min.
    // Dead-end pattern fills all 5 departure slots (09:00..09:04 origin departure).
    // Connecting pattern's first trip (origin dep 09:29) is the 6th → currently missed.
    // The bug: all 5 RAPTOR runs return empty (dead-end), probe result is discarded,
    // raptor_range returns [] even though a valid connecting plan exists.
    let plans = g.raptor_range(osm_origin, osm_dest, base, 180 * 60, 0, 0x7F, 600);

    // The connecting pattern has trips at 09:30, 10:30, 11:30 from stop_A.
    // With only 5 interesting-time slots, all filled by dead-end departures
    // (09:01–09:05), RAPTOR never queries origin-departure times 10:29 or 11:29.
    // It accidentally finds the 09:30 connecting trip as the "first available"
    // in all 5 iterations, giving 1 deduplicated plan instead of 3.
    assert_eq!(
        plans.len(), 3,
        "raptor_range should return all 3 connecting trips (09:30, 10:30, 11:30) \
         from a 180-min window, but got {} plan(s). \
         Likely the dead-end pattern starved the interesting-times cap (bug).",
        plans.len(),
    );

    // All returned plans must actually reach the destination (end > start).
    for p in &plans {
        assert!(p.end > p.start, "plan end <= start: start={} end={}", p.start, p.end);
    }
}

/// Verifies that `with_access_search` doubles `access_secs` until it locates
/// all stops, and falls back to a walk-only plan when no transit exists.
/// Uses a two-node street-only graph so RAPTOR must double access_secs
/// past the walk time and return the walk fallback.
#[test]
fn access_search_doubles_until_walk_plan_returned() {
    let mut g = Graph::new();
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let n1 = g.add_node(osm_node("n1", 50.001, 4.0)); // ~111 m apart
    let dist = LatLng { latitude: 50.0, longitude: 4.0 }
        .dist(LatLng { latitude: 50.001, longitude: 4.0 }) as usize;
    g.add_edge(n0, street_edge(n0, n1, dist));
    g.add_edge(n1, street_edge(n1, n0, dist));
    g.build_raptor_index();

    // min_access_secs=1 forces many doublings before walk-only is reached.
    let plans = g.raptor(n0, n1, 0, 0, 0x7F, 1);

    assert_eq!(plans.len(), 1, "expected exactly one walk-only plan");
    assert_eq!(plans[0].legs.len(), 1);
    assert!(matches!(plans[0].legs[0], PlanLeg::Walk(_)), "single leg should be a walk");
}

// ── Pareto boarding fix (prefer later boarding stop on same trip) ─────────────

/// Route X: stop_A → stop_B → stop_C (single trip T).
/// Origin is near stop_B. Footpath B↔A exists (~180 m).
/// The user can board T at B directly (dep 10:02); the bug boards at A instead
/// (dep 10:00) via the backward footpath and produces a Walk(B→A) leg.
///
/// Layout (longitude only; lat fixed at 50.000):
///   osm_a (4.000) ─10m─ stop_A (4.000)
///   osm_a ────180m────── osm_origin (4.002) ─10m─ stop_B (4.002)
///   osm_origin ──7000m── osm_dest (4.100) ─10m─ stop_C (4.100)
fn backward_walk_graph() -> (Graph, NodeID, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_a      = g.add_node(osm_node("osm_a",  50.000, 4.000));
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.002));
    let osm_dest   = g.add_node(osm_node("dest",   50.000, 4.100));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.000));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.002));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.100));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    add_street(&mut g, osm_a, osm_origin, 180);
    add_street(&mut g, osm_origin, osm_dest, 7_000);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
            }));
        }
    };
    add_snap(&mut g, stop_a, osm_a,      10);
    add_snap(&mut g, stop_b, osm_origin, 10);
    add_snap(&mut g, stop_c, osm_dest,   10);

    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 },
        length: 180,
    }));
    g.add_edge(stop_b, EdgeData::Transit(TransitEdgeData {
        origin: stop_b, destination: stop_c, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 },
        length: 7_000,
    }));

    g.add_transit_services(vec![all_days_service()]);

    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(), route_long_name: "Route X".into(),
        route_type: RouteType::Bus, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);

    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None,
    }]);

    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 10 * 3600, arrival: 10 * 3600 + 120, service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 1, destination_stop_sequence: 2,
            departure: 10 * 3600 + 120, arrival: 10 * 3600 + 1200, service_id: ServiceId(0),
        },
    ]);

    // Pattern 0: stop_A → stop_B → stop_C, 1 trip (column-major stop-times)
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b, stop_c]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 10 * 3600,       departure: 10 * 3600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 10 * 3600 + 120, departure: 10 * 3600 + 120 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 10 * 3600 + 1200, departure: 10 * 3600 + 1200 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });

        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }

    g.build_raptor_index();

    (g, osm_origin, osm_dest, stop_a, stop_b)
}

/// Verifies that when a footpath exists from stop B back to stop A (A is earlier
/// on the same route), RAPTOR boards the trip at B — not at A via a backward walk.
/// The backward footpath B→A must NOT appear as a walk leg in any returned plan.
#[test]
fn raptor_no_backward_walk_same_trip() {
    let (g, origin, dest, stop_a, stop_b) = backward_walk_graph();

    // Start at 9:50 so the 10:00/10:02 trips at A/B are both reachable.
    let plans = g.raptor(origin, dest, 9 * 3600 + 600, 0, 0x7F, 30);

    assert!(!plans.is_empty(), "expected at least one plan");

    for plan in &plans {
        // No plan should walk to stop_A — that would be the backward detour.
        let backward_walk = plan.legs.iter().any(|leg| {
            matches!(leg, PlanLeg::Walk(w) if w.to.node_id == stop_a)
        });
        assert!(!backward_walk, "plan contains a backward walk to stop_A");

        // Every transit leg should board at stop_B (not stop_A).
        for leg in &plan.legs {
            if let PlanLeg::Transit(t) = leg {
                assert_ne!(
                    t.from.node_id, stop_a,
                    "transit leg boarded at stop_A — expected stop_B as boarding stop \
                     (from={:?}, to={:?})",
                    t.from.node_id, t.to.node_id
                );
                assert_eq!(
                    t.from.node_id, stop_b,
                    "transit leg should board at stop_B, got {:?}",
                    t.from.node_id
                );
            }
        }
    }
}

/// Verifies the walking Pareto criterion: among plans with the same arrival,
/// departure, and transfer count, the one with less walking should dominate.
/// Uses the backward_walk_graph: the corrected plan (board at B, no backward walk)
/// has less walking than the buggy plan (board at A via Walk B→A).
/// After both fixes the buggy plan is never produced, so there is exactly one plan
/// and it has no backward walk.
#[test]
fn raptor_pareto_less_walking_plan_survives() {
    let (g, origin, dest, stop_a, _stop_b) = backward_walk_graph();

    let plans = g.raptor(origin, dest, 9 * 3600 + 600, 0, 0x7F, 30);

    assert!(!plans.is_empty(), "expected at least one plan");

    // Verify no plan has a walk leg landing at stop_A (the backward walk).
    for plan in &plans {
        let has_backward_walk = plan.legs.iter().any(|leg| {
            matches!(leg, PlanLeg::Walk(w) if w.to.node_id == stop_a)
        });
        assert!(
            !has_backward_walk,
            "a plan with a backward Walk(→stop_A) survived the Pareto filter; \
             the less-walking plan should have dominated it"
        );
    }
}

/// `previous_departures` / `next_departures` must never panic on an index that
/// falls outside the timetable segment (regression: a backward-tightened leg could
/// pair a departure index from one segment with another segment's bounds, causing a
/// `usize` underflow and a slice-range panic that crashed the server).
#[test]
fn departures_out_of_segment_index_does_not_panic() {
    let mut g = Graph::new();
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 100, arrival: 200, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 300, arrival: 400, service_id: ServiceId(0) },
    ]);
    // Segment covers only index 1; querying with index 0 (< start) used to underflow.
    let tt = TimetableSegment { start: 1, len: 1 };
    let prev: Vec<_> = g.previous_departures(tt, 0, 0x7F, 0).collect();
    assert!(prev.is_empty(), "out-of-segment previous_departures should be empty, not panic");
    let next: Vec<_> = g.next_departures(tt, 0, 0x7F, 0).collect();
    assert!(next.is_empty(), "out-of-segment next_departures should be empty, not panic");
    // A valid in-segment index still works.
    let prev_ok: Vec<_> = g.previous_departures(TimetableSegment { start: 0, len: 2 }, 0, 0x7F, 1).collect();
    assert_eq!(prev_ok.len(), 1);
}

// ── Reliability-aware multi-criteria labels ───────────────────────────────────

/// Bus A→B (one trip, arr 09:15), walk B→C, then a Tram C→D with TWO trips:
///   tight: dep 09:20 → arr 09:35  (≈3 min after reaching C; risky under delay model)
///   safe:  dep 10:00 → arr 10:15  (≈40 min margin; reliable)
/// A Bus delay model makes the tight connection low-reliability and the safe one
/// reliable, so the two options differ on (arrival, reliability) — a trade-off.
fn reliability_tradeoff_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_ab = g.add_node(osm_node("ab", 50.000, 4.010));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.019));
    let osm_cd = g.add_node(osm_node("cd", 50.000, 4.030));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.020));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.022));
    let stop_d = g.add_node(transit_stop("Stop D", 50.000, 4.040));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 1362,
    }));
    g.add_edge(stop_c, EdgeData::Transit(TransitEdgeData {
        origin: stop_c, destination: stop_d, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 1, len: 2 }, length: 1290,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo { route_short_name: "1".into(), route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0), route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "T".into(), route_long_name: "Tram T".into(),
            route_type: RouteType::Tramway, agency_id: AgencyId(0), route_color: None, route_text_color: None },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // 0: bus
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None }, // 1: tram tight
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None }, // 2: tram safe
    ]);
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600, arrival: 9 * 3600 + 900, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 1200, arrival: 9 * 3600 + 2100, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 10 * 3600, arrival: 10 * 3600 + 900, service_id: ServiceId(0) },
    ]);

    // Pattern 0: Bus [A,B], 1 trip
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 900, departure: 9 * 3600 + 900 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }

    // Pattern 1: Tram [C,D], 2 trips (tight then safe)
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });
        let sts = g.transit_pattern_stop_times_len();
        // col C (dep): tight 09:20, safe 10:00
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1200, departure: 9 * 3600 + 1200 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 10 * 3600, departure: 10 * 3600 });
        // col D (arr): tight 09:35, safe 10:15
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 2100, departure: 9 * 3600 + 2100 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 10 * 3600 + 900, departure: 10 * 3600 + 900 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 2 });
    }

    g.build_raptor_index();

    // Bus delay model: small transfer margin ⇒ low on-time prob, large margin ⇒ certain.
    let mut models = HashMap::new();
    models.insert(
        RouteType::Bus,
        DelayCDF { bins: vec![(0, 0.05), (300, 0.5), (900, 0.9), (1800, 1.0)] },
    );
    g.set_transit_delay_models(models);

    (g, osm_origin, osm_dest)
}

/// With enough arrival slack, the multi-criteria core returns BOTH the fast-but-risky
/// plan (tight tram) and the slower-but-reliable plan (later tram). Without the
/// feature only the fastest would survive.
#[test]
fn raptor_returns_fast_risky_and_slow_safe() {
    let (g, origin, dest) = reliability_tradeoff_graph();
    let buckets = ReliabilityBuckets::default();

    // Generous slack so the later, safer tram is explored.
    let plans = g.raptor_tuned(origin, dest, 8 * 3600 + 1800, 0, 0x7F, 10 * 60, &buckets, 3600);

    // Worst transfer reliability per plan (1.0 if no risk), with its arrival time.
    let mut summary: Vec<(f32, u32)> = plans
        .iter()
        .map(|p| {
            let worst = p.legs.iter().filter_map(|l| match l {
                PlanLeg::Transit(t) => t.transfer_risk.as_ref().map(|r| r.reliability),
                _ => None,
            }).fold(1.0f32, f32::min);
            (worst, p.end)
        })
        .collect();
    summary.sort_by_key(|a| a.1);
    eprintln!("plans (worst_rel, arrive): {:?}", summary);

    let risky = summary.iter().find(|(r, _)| *r < 0.5);
    let safe = summary.iter().find(|(r, _)| *r >= 0.99);
    let risky = risky.expect("expected a fast low-reliability plan");
    let safe = safe.expect("expected a slow high-reliability alternative");
    assert!(
        safe.1 > risky.1,
        "the reliable alternative ({:?}) should arrive later than the risky one ({:?})",
        safe, risky
    );
}

/// Increasing arrival slack never removes plans — a wider explored band can only
/// add non-dominated alternatives. Guards the slack lever's monotonicity.
#[test]
fn raptor_more_slack_never_fewer_plans() {
    let (g, origin, dest) = reliability_tradeoff_graph();
    let buckets = ReliabilityBuckets::default();
    let few = g.raptor_tuned(origin, dest, 8 * 3600 + 1800, 0, 0x7F, 10 * 60, &buckets, 0).len();
    let many = g.raptor_tuned(origin, dest, 8 * 3600 + 1800, 0, 0x7F, 10 * 60, &buckets, 3600).len();
    assert!(many >= few, "more slack ({many}) should not yield fewer plans than less ({few})");
}

// ── Three-pass RAPTOR: tightening must not destroy transfer reliability ────────

/// Feeder (Bus, first leg) has three trips; the connecting Tram has one trip:
///   Bus trip 0: dep A 08:00, arr B 08:15  (huge margin to tram — unnecessary)
///   Bus trip 1: dep A 09:00, arr B 09:15  (large margin, still CERTAIN)
///   Bus trip 2: dep A 09:20, arr B 09:26  (tiny margin, low reliability)
///   Tram trip 3: dep C 09:30, arr D 09:45.
/// A Bus delay model makes the transfer reliability depend on the margin: large
/// margin ⇒ CERTAIN, tiny margin ⇒ low. Forward RAPTOR stores the destination
/// label in the CERTAIN bucket (computed from the earliest feeder arrival). Naive
/// backward tightening shifts the Bus to trip 2 (latest that merely *connects*),
/// collapsing the margin and demoting the plan out of its own reliability bucket.
fn feeder_tightening_reliability_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_ab = g.add_node(osm_node("ab", 50.000, 4.010));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.019));
    let osm_cd = g.add_node(osm_node("cd", 50.000, 4.030));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.020));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.022));
    let stop_d = g.add_node(transit_stop("Stop D", 50.000, 4.040));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, EdgeData::Street(StreetEdgeData {
            origin: a, destination: b, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m, partial: false, foot: true, bike: true, car: true, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m, partial: true, foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(), elev_delta: 0,
        }));
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 3 }, length: 1362,
    }));
    g.add_edge(stop_c, EdgeData::Transit(TransitEdgeData {
        origin: stop_c, destination: stop_d, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 3, len: 1 }, length: 1290,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo { route_short_name: "1".into(), route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0), route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "T".into(), route_long_name: "Tram T".into(),
            route_type: RouteType::Tramway, agency_id: AgencyId(0), route_color: None, route_text_color: None },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // 0: bus early
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // 1: bus safe
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None }, // 2: bus dangerous
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None }, // 3: tram
    ]);
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 8 * 3600, arrival: 8 * 3600 + 900, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600, arrival: 9 * 3600 + 900, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 1200, arrival: 9 * 3600 + 1560, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(3), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600 + 1800, arrival: 9 * 3600 + 2700, service_id: ServiceId(0) },
    ]);

    // Pattern 0: Bus [A,B], 3 trips
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 3 });
        let sts = g.transit_pattern_stop_times_len();
        // stop_A col (dep): 08:00, 09:00, 09:20
        g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600, departure: 8 * 3600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1200, departure: 9 * 3600 + 1200 });
        // stop_B col (arr): 08:15, 09:15, 09:26
        g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600 + 900, departure: 8 * 3600 + 900 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 900, departure: 9 * 3600 + 900 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1560, departure: 9 * 3600 + 1560 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 3 });
    }

    // Pattern 1: Tram [C,D], 1 trip
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(3));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1800, departure: 9 * 3600 + 1800 });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 2700, departure: 9 * 3600 + 2700 });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 1 });
    }

    g.build_raptor_index();

    // Bus delay model: tiny margin ⇒ low on-time prob, large margin ⇒ certain.
    let mut models = HashMap::new();
    models.insert(
        RouteType::Bus,
        DelayCDF { bins: vec![(60, 0.3), (600, 0.95), (1200, 1.0)] },
    );
    g.set_transit_delay_models(models);

    (g, osm_origin, osm_dest)
}

/// Backward tightening must not shift the feeder so late that it demotes the plan
/// below the reliability bucket the forward pass stored it in. The earliest feeder
/// gives a CERTAIN transfer; tightening to the latest *connecting* feeder collapses
/// the margin to a low-reliability transfer with the SAME arrival — strictly worse.
#[test]
fn tightening_preserves_transfer_reliability() {
    let (g, origin, dest) = feeder_tightening_reliability_graph();

    let plans = g.raptor(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "Expected at least one plan");

    let two_leg = plans
        .iter()
        .find(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2)
        .expect("Expected a Bus+Tram plan");

    let transit: Vec<_> = two_leg
        .legs
        .iter()
        .filter_map(|l| if let PlanLeg::Transit(t) = l { Some(t) } else { None })
        .collect();
    let bus = transit[0];
    let tram = transit[1];
    let rel = tram.transfer_risk.as_ref().expect("tram leg has transfer risk").reliability;

    eprintln!(
        "bus dep={}s arr={}s | tram dep={}s | transfer reliability={}",
        bus.start, bus.end, tram.start, rel
    );

    assert!(
        rel >= 0.80,
        "tightening collapsed the transfer to reliability {rel} (<0.80); the forward \
         pass scored this plan as reliable, so tightening must keep it reliable"
    );
    assert_eq!(
        bus.start,
        9 * 3600,
        "tightening should pick the safe-latest feeder (09:00): not the unnecessary \
         08:00 one, nor the reliability-collapsing 09:20 one; got dep {}s",
        bus.start
    );
}

#[test]
fn osm_only_cache_round_trip_preserves_network() {
    use maas_rs::services::persistence::{load_osm_graph, save_osm_graph};

    let (g, a, _b, _c) = three_node_street_graph();
    let dir = std::env::temp_dir().join("maas_osm_view_test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("osm.bin");
    let path_s = path.to_str().unwrap();

    save_osm_graph(&g, path_s).unwrap();
    let restored = load_osm_graph(path_s).unwrap();

    assert_eq!(restored.node_count(), g.node_count());
    assert_eq!(restored.get_id("a"), Some(&a));
    assert_eq!(restored.nearest_node(50.000, 4.000), Some(a));
    assert_eq!(restored.raptor.transit_trips.len(), 0);
}

/// Real-network oracle + benchmark for self-pruning rRAPTOR. Loads the prebuilt
/// `graph.bin` (Brussels: STIB + SNCB) and asserts the self-pruning range driver
/// returns the SAME 4-D Pareto set (departure, arrival, transfers, reliability
/// bucket) as the independent-passes oracle on dense real O/D where cross-departure
/// ties actually occur — the case toy graphs miss and where the prior attempt
/// failed. Also prints timings. Ignored by default (needs the 1.8 GB graph.bin):
///   cargo test --release --test graph_tests self_pruning_range_real_network -- --ignored --nocapture
#[test]
#[ignore]
fn self_pruning_range_real_network_equals_independent() {
    use maas_rs::services::persistence::load_graph;
    use std::collections::HashSet;
    use std::time::Instant;

    let g = load_graph("graph.bin").expect("load graph.bin");
    let buckets = ReliabilityBuckets::default();
    let date = 9657u32; // 2026-06-10, days since 2000-01-01
    let weekday = 0x7Fu8; // any service day; both paths use the same, so fair
    let start = 9 * 3600u32;

    let battery = [
        ("Schuman->Uccle", 50.843, 4.381, 50.800, 4.338),
        ("Bourse->Midi", 50.848, 4.349, 50.836, 4.336),
        // (Bxl->Antwerpen dropped from the fast loop: out of access radius, ~60s of
        //  widening for 0 plans. Re-add for a full sweep.)
    ];

    let key = |p: &maas_rs::structures::plan::Plan| {
        (
            p.start,
            p.end,
            p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count(),
            buckets.bucket(Graph::plan_reliability(p)),
        )
    };

    for window_min in [30u32, 60] {
        let window = window_min * 60;
        for (label, flat, flng, tlat, tlng) in battery {
            let o = g.nearest_node(flat, flng).expect("origin node");
            let d = g.nearest_node(tlat, tlng).expect("dest node");

            let t0 = Instant::now();
            let sp = g.raptor_range(o, d, start, window, date, weekday, 5 * 60);
            let sp_ms = t0.elapsed().as_millis();

            let t1 = Instant::now();
            let indep = g.raptor_range_independent(o, d, start, window, date, weekday, 5 * 60);
            let indep_ms = t1.elapsed().as_millis();

            let sp_keys: HashSet<_> = sp.iter().map(key).collect();
            let in_keys: HashSet<_> = indep.iter().map(key).collect();
            let only_sp: Vec<_> = sp_keys.difference(&in_keys).cloned().collect();
            let only_in: Vec<_> = in_keys.difference(&sp_keys).cloned().collect();

            // 3-D projection (drop reliability bucket): if a divergent key matches on
            // (start,end,transfers) across the two sets, the ONLY difference is the
            // reliability bucket — i.e. search-time vs post-tightening bucket mismatch.
            let sp3: HashSet<_> = sp_keys.iter().map(|k| (k.0, k.1, k.2)).collect();
            let in3: HashSet<_> = in_keys.iter().map(|k| (k.0, k.1, k.2)).collect();
            let only_in_bucket_only = only_in.iter().filter(|k| sp3.contains(&(k.0, k.1, k.2))).count();
            let only_sp_bucket_only = only_sp.iter().filter(|k| in3.contains(&(k.0, k.1, k.2))).count();

            println!(
                "[w={:>2}m] {:<16} sp {:>3}/{:>6}ms | indep {:>3}/{:>6}ms | {:.2}x | only_sp={} (bkt {}) only_in={} (bkt {})",
                window_min, label, sp.len(), sp_ms, indep.len(), indep_ms,
                indep_ms as f64 / sp_ms.max(1) as f64,
                only_sp.len(), only_sp_bucket_only, only_in.len(), only_in_bucket_only,
            );
            // Classify each only_in key: is it 4-D-dominated by some self-pruning key
            // (acceptable — sp's set still covers it) or a genuine missed Pareto point?
            // 4-D dom: tc_a<=tc_b && end_a<=end_b && start_a>=start_b && bkt_a>=bkt_b, strict in one.
            let dom = |a: &(u32, u32, usize, u8), b: &(u32, u32, usize, u8)| {
                a.2 <= b.2 && a.1 <= b.1 && a.0 >= b.0 && a.3 >= b.3
                    && (a.2 < b.2 || a.1 < b.1 || a.0 > b.0 || a.3 > b.3)
            };
            let genuine_miss: Vec<_> = only_in.iter()
                .filter(|k| !sp_keys.iter().any(|s| dom(s, k)))
                .collect();
            if !only_sp.is_empty() { println!("    only_sp: {only_sp:?}"); }
            if !only_in.is_empty() {
                println!("    only_in: {only_in:?}");
                println!("    genuine_miss (not dominated by any sp plan): {} -> {genuine_miss:?}", genuine_miss.len());
                // Dump legs of the first genuine-miss plan (from the independent set),
                // plus whether any independent plan itself dominates it (filter sanity).
                if let Some(&gm) = genuine_miss.first()
                    && let Some(p) = indep.iter().find(|p| key(p) == *gm) {
                        let self_dom = indep.iter().any(|q| key(q) != *gm && dom(&key(q), gm));
                        println!("    >>> MISS {gm:?} | dominated within indep set? {self_dom}");
                        for leg in &p.legs {
                            match leg {
                                PlanLeg::Transit(t) => println!(
                                    "        TRANSIT {}->{} dep={} arr={} rt={:?} rel={:?}",
                                    t.from.node_id.0, t.to.node_id.0, t.start, t.end, t.route_type,
                                    t.transfer_risk.as_ref().map(|r| r.reliability)),
                                PlanLeg::Walk(w) => println!(
                                    "        WALK    {}->{} {}s", w.from.node_id.0, w.to.node_id.0, w.duration),
                            }
                        }
                    }
            }
        }
    }
}

// ── Direct (no-transit) plans ─────────────────────────────────────────────────

#[test]
fn direct_bike_plan_uses_kinematic_time() {
    let (mut g, a, _, c) = three_node_street_graph();
    g.build_raptor_index();
    let am = ActiveModes::new(&[Mode::Bike]);
    let plans = g.raptor_modes(a, c, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].mode, Mode::Bike);
    // Direct bike now reports the kinematic ETA of the cost-optimal route: two
    // flat 100 m road edges (the chain a→b→c), each solved by the power model.
    let bc = BikeCost::new(BikeProfile::default());
    let edge100 = StreetEdgeData {
        origin: NodeID(0), destination: NodeID(1), length: 100,
        partial: false, foot: true, bike: true, car: true,
        attrs: BikeAttrs::road_default(), elev_delta: 0,
    };
    let expected = 2 * bc.edge_time(&edge100);
    assert_eq!(plans[0].end - plans[0].start, expected);
    assert_eq!(street_modes(&plans[0]), vec![Mode::Bike]);
}

#[test]
fn walk_and_bike_direct_both_returned_when_selected() {
    let (mut g, a, _, c) = three_node_street_graph();
    g.build_raptor_index();
    let am = ActiveModes::new(&[Mode::Walk, Mode::Bike]);
    let plans = g.raptor_modes(a, c, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    let modes: Vec<Mode> = plans.iter().map(|p| p.mode).collect();
    assert!(modes.contains(&Mode::Walk), "modes: {modes:?}");
    assert!(modes.contains(&Mode::Bike), "modes: {modes:?}");
}

#[test]
fn direct_bike_absent_with_default_modes() {
    let (mut g, a, _, c) = three_node_street_graph();
    g.build_raptor_index();
    let plans = g.raptor_modes(a, c, 8 * 3600, 0, 0x7F, 10 * 60, &ActiveModes::default());
    assert!(plans.iter().all(|p| p.mode != Mode::Bike));
}

/// When cycling the whole way beats every bike+transit combination, the only
/// bike-mode result is the direct ride — "no improvement → no transit plan".
#[test]
fn direct_bike_returned_when_transit_brings_no_improvement() {
    let (g, origin, dest) = two_route_raptor_graph_with_bikes(Some(true), Some(true));
    let am = ActiveModes::new(&[Mode::BikeTransit]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    assert!(
        plans.iter().any(|p| p.mode == Mode::Bike && transit_leg_count(p) == 0),
        "expected the direct ride, got: {:?}",
        plans.iter().map(|p| (p.mode, transit_leg_count(p))).collect::<Vec<_>>()
    );
}

/// Range soundness with bike states: the self-pruning range driver must return
/// the same Pareto set as independent from-scratch passes, with all modes on.
#[test]
fn raptor_range_modes_matches_independent_oracle() {
    let (g, origin, dest) = express_two_leg_graph(Some(true), None);
    let am = ActiveModes::new(&[Mode::WalkTransit, Mode::BikeTransit, Mode::BikeOnTransit]);
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let rt = RealtimeIndex::new();

    let pruned = g.raptor_range_tuned_rt_modes(
        origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60, &buckets, 900, &rt, &am,
        &BikeCost::new(BikeProfile::default()),
    );
    let indep = g.raptor_range_independent_rt_modes(
        origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60, &buckets, 900, &rt, &am,
    );

    let key = |p: &maas_rs::structures::plan::Plan| {
        (p.mode, p.start, p.end, transit_leg_count(p))
    };
    let mut a: Vec<_> = pruned.iter().map(key).collect();
    let mut b: Vec<_> = indep.iter().map(key).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "self-pruning range diverged from the independent oracle");
}

/// Bike modes flow through the explain (debug) path too: same plans, plus
/// candidate/stop instrumentation, without falling back to direct plans.
#[test]
fn raptor_explain_supports_bike_modes() {
    let (g, origin, dest) = express_two_leg_graph(Some(true), Some(true));
    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let res = g.raptor_explain_tuned_rt_modes(
        origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &buckets, 900,
        &RealtimeIndex::new(), &am,
        &BikeCost::new(BikeProfile::default()),
    );
    assert!(!res.access.fell_back_to_walk_only);
    assert!(res.plans.iter().any(|p| transit_leg_count(p) == 2));
    assert!(!res.stops_reached.is_empty());
}

// ── Bike cost-routing (Approach A: minimize weighted cost, carry kinematic time) ─

fn bike_attrs(hw: HighwayClass, isbike: bool, surface: Surface) -> BikeAttrs {
    let mut a = BikeAttrs::road_default();
    a.highway = hw;
    a.isbike = isbike;
    a.surface = surface;
    a
}

/// A cheap-but-long cycleway corridor and a costly-but-short unsafe primary both
/// reach the same stop. Cost-routing must take the cycleway, so the reported
/// access time matches the (longer) cycleway ride, not the short primary.
#[test]
fn bike_prefers_cycleway() {
    let mut g = Graph::new();
    let o = g.add_node(osm_node("o", 50.000, 4.000));
    let a = g.add_node(osm_node("a", 50.005, 4.005));
    let d = g.add_node(osm_node("d", 50.000, 4.010));
    let stop = g.add_node(transit_stop("S", 50.000, 4.0101));

    let cyc = bike_attrs(HighwayClass::Cycleway, true, Surface::Paved);
    let prim = bike_attrs(HighwayClass::Primary, false, Surface::Paved);
    let snap = BikeAttrs::road_default();

    let edge = |g: &mut Graph, from: NodeID, to: NodeID, len: usize, attrs: BikeAttrs| {
        for (o2, d2) in [(from, to), (to, from)] {
            g.add_edge(
                o2,
                EdgeData::Street(StreetEdgeData {
                    origin: o2,
                    destination: d2,
                    length: len,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: false,
                    attrs,
                    elev_delta: 0,
                }),
            );
        }
    };
    edge(&mut g, o, a, 600, cyc); // cycleway O–A–D = 1200 m, low cost
    edge(&mut g, a, d, 600, cyc);
    edge(&mut g, o, d, 715, prim); // unsafe primary O–D = 715 m, high cost
    edge(&mut g, d, stop, 8, snap); // foot connector to the platform
    g.build_raptor_index();

    let bc = BikeCost::new(BikeProfile::default());
    let mk = |len: usize, attrs: BikeAttrs| StreetEdgeData {
        origin: NodeID(0),
        destination: NodeID(1),
        length: len,
        partial: false,
        foot: true,
        bike: true,
        car: false,
        attrs,
        elev_delta: 0,
    };
    let t_cyc = bc.edge_time(&mk(600, cyc)) * 2 + bc.edge_time(&mk(8, snap));
    let t_prim = bc.edge_time(&mk(715, prim)) + bc.edge_time(&mk(8, snap));
    assert!(t_cyc > t_prim, "test setup: cycleway must be the slower corridor");

    let stops = g.bike_nearby_stops(o, 600, &bc);
    assert_eq!(stops.len(), 1, "exactly the one stop is reachable");
    let (_, secs) = stops[0];
    assert!(
        secs.abs_diff(t_cyc) <= 2,
        "access time {secs}s should match the cost-optimal cycleway ride {t_cyc}s"
    );
    assert!(
        secs > t_prim,
        "cost-routing took the short unsafe primary ({t_prim}s) instead of the cycleway"
    );

    // A profile that does not penalize unsafe roads and downweights the cycleway
    // base must instead take the short primary — proving the profile parameter
    // (the basis for the per-request override) actually steers route choice.
    let mut prof = BikeProfile::default();
    prof.avoid_unsafe = false;
    prof.stick_to_cycleroutes = false;
    prof.highway.primary = 1.0;
    let bc2 = BikeCost::new(prof);
    let stops2 = g.bike_nearby_stops(o, 600, &bc2);
    let (_, secs2) = stops2[0];
    assert!(
        secs2.abs_diff(t_prim) <= 2,
        "permissive profile should take the short primary ({t_prim}s), got {secs2}s"
    );
}
