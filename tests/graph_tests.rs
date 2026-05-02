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
        EdgeData, Graph, LatLng, NodeData, NodeID, OsmNodeData,
        StreetEdgeData, TransitEdgeData, TransitStopData,
        plan::PlanLeg,
        raptor::{Lookup, PatternInfo},
    },
};

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
                car: true,
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
                car: true,
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
                car: false,
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
                car: false,
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
            bikes_allowed: None,
        }, // TripId(0) = bus
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
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
            partial: false, foot: true, bike: true, car: true,
        }));
        g.add_edge(b, EdgeData::Street(StreetEdgeData {
            origin: b, destination: a, length: m,
            partial: false, foot: true, bike: true, car: true,
        }));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m,
            partial: true, foot: true, bike: false, car: false,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m,
            partial: true, foot: true, bike: false, car: false,
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
                partial: false, foot: true, bike: true, car: true,
            }));
        }
    };
    // Connect origin ↔ dest via a long street (so walk-only is expensive)
    add_street(&mut g, osm_origin, osm_dest, 7200); // 7 200 m ≈ 1 h walk

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: true, foot: true, bike: false, car: false,
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
                partial: false, foot: true, bike: true, car: true,
            }));
        }
    };
    add_street(&mut g, osm_origin, osm_dest, 7200); // long direct walk (1 h)

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(o, EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, length: m,
                partial: true, foot: true, bike: false, car: false,
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
