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
        AgencyId, AgencyInfo, GtfsProvider, RouteId, RouteInfo, ServiceId, ServicePattern,
        StopTime, TimetableSegment, TripId, TripInfo, TripSegment, preprocess_parent_stations,
    },
    routing::routing_raptor::{RouteQuery, route},
    structures::{
        ActiveModes, BikeAttrs, BikeCost, BikeProfile, DelayCDF, EdgeData, Endpoint, Graph,
        HighwayClass, LatLng, Mode, NodeData, NodeID, OnboardRide, OsmNodeData, QueryEndpoints,
        RealtimeIndex, ReliabilityBuckets, StreetEdgeData, StreetProfile, StreetTimeModel, Surface,
        TransitEdgeData, TransitStopData,
        cost::VarGen,
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
        platform_code: None,
        parent_station: None,
    })
}

fn transit_stop_parent(
    name: &str,
    id: &str,
    lat: f64,
    lon: f64,
    parent: Option<&str>,
) -> NodeData {
    NodeData::TransitStop(TransitStopData {
        name: name.to_string(),
        lat_lng: LatLng {
            latitude: lat,
            longitude: lon,
        },
        accessibility: Availability::Available,
        id: id.to_string(),
        platform_code: None,
        parent_station: parent.map(|s| s.to_string()),
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

fn enable_contraction(g: &mut Graph) {
    use maas_rs::structures::contraction::ContractedGraph;
    let mut cg = ContractedGraph::from_graph_union(g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();
}

/// `raptor_modes` carrying projected snap coordinates (`ep`), as production always
/// does — direct street legs are built g-free over the contracted graph. The caller
/// must have enabled contraction. Coordinates are the endpoints' own node positions.
#[allow(clippy::too_many_arguments)]
fn raptor_modes_ep(
    g: &Graph,
    origin: NodeID,
    destination: NodeID,
    origin_ll: LatLng,
    destination_ll: LatLng,
    start_time: u32,
    date: u32,
    weekday: u8,
    min_access_secs: u32,
    am: &ActiveModes,
) -> Vec<maas_rs::structures::plan::Plan> {
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = QueryEndpoints {
        origin: origin_ll,
        destination: destination_ll,
        origin_station: None,
        destination_station: None,
    };
    g.raptor_tuned_rt_modes_ep(
        origin,
        destination,
        start_time,
        date,
        weekday,
        min_access_secs,
        &buckets,
        g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &RealtimeIndex::new(),
        am,
        &BikeCost::new(BikeProfile::default()),
        Some(&ep),
        maas_rs::structures::cost::FareProfile::default(),
    )
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

// ── station dedup index ────────────────────────────────────────────────────────

#[test]
fn station_index_collapses_platforms_sharing_parent_station() {
    let mut g = Graph::new();
    g.add_node(transit_stop_parent("Gent P1", "p1", 51.000, 3.700, Some("Gent")));
    g.add_node(transit_stop_parent("Gent P2", "p2", 51.001, 3.701, Some("Gent")));
    g.add_node(transit_stop_parent("Gent P3", "p3", 51.002, 3.702, Some("Gent")));
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        1,
        "three platforms sharing parent_station collapse to one station"
    );
    let st = &g.raptor.transit_stations[0];
    assert_eq!(st.id, "Gent");
    let mut plats = st.platform_stop_indices.clone();
    plats.sort_unstable();
    assert_eq!(
        plats,
        vec![0, 1, 2],
        "the station holds ALL member platform compact indices"
    );

    let listed = g.gtfs_stations();
    assert_eq!(listed.len(), 1);
    let (id, _name, _lat, _lon, _ops, _modes, _lines, count) = &listed[0];
    assert_eq!(id, "Gent");
    assert_eq!(*count, 3, "gtfs_stations reports the member platform count");
}

fn gtfs_orphan(
    id: &str,
    name: &str,
    lat: f64,
    lon: f64,
) -> std::sync::Arc<gtfs_structures::Stop> {
    std::sync::Arc::new(gtfs_structures::Stop {
        id: id.to_string(),
        name: Some(name.to_string()),
        latitude: Some(lat),
        longitude: Some(lon),
        parent_station: None,
        ..Default::default()
    })
}

fn add_absorbed_stop(g: &mut Graph, stop: &gtfs_structures::Stop) {
    g.add_node(transit_stop_parent(
        stop.name.as_deref().unwrap(),
        &stop.id,
        stop.latitude.unwrap(),
        stop.longitude.unwrap(),
        stop.parent_station.as_deref(),
    ));
}

#[test]
fn orphan_absorbed_into_native_group_collapses_to_one_station() {
    let mut stops = HashMap::new();
    stops.insert(
        "plat12".to_string(),
        std::sync::Arc::new(gtfs_structures::Stop {
            id: "plat12".to_string(),
            name: Some("Merode".to_string()),
            latitude: Some(50.8330),
            longitude: Some(4.3920),
            parent_station: Some("12".to_string()),
            ..Default::default()
        }),
    );
    stops.insert(
        "surface".to_string(),
        gtfs_orphan("surface", "MERODE", 50.8331, 4.3920),
    );
    preprocess_parent_stations(GtfsProvider::Stib, &mut stops, 100.0);

    let mut g = Graph::new();
    add_absorbed_stop(&mut g, &stops["plat12"]);
    add_absorbed_stop(&mut g, &stops["surface"]);
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        1,
        "orphan within radius of a same-name native member joins its station"
    );
    let st = &g.raptor.transit_stations[0];
    assert_eq!(st.id, "12", "the merged station keeps the NATIVE parent id");
    assert_eq!(
        st.platform_stop_indices.len(),
        2,
        "both the native platform and the absorbed orphan are members"
    );
}

#[test]
fn display_harmonization_does_not_affect_grouping_or_ids() {
    let mut stib = HashMap::new();
    stib.insert("a".to_string(), gtfs_orphan("a", "MERODE", 50.8330, 4.3920));
    stib.insert("b".to_string(), gtfs_orphan("b", "MERODE", 50.8331, 4.3920));
    preprocess_parent_stations(GtfsProvider::Stib, &mut stib, 100.0);

    let mut g = Graph::new();
    add_absorbed_stop(&mut g, &stib["a"]);
    add_absorbed_stop(&mut g, &stib["b"]);
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        1,
        "same-name orphans collapse to one station regardless of letter case"
    );
    let st = &g.raptor.transit_stations[0];
    assert!(
        st.id.starts_with("maas:synth:"),
        "grouping id is the synthesized parent, not the display name"
    );
    assert_eq!(st.name, "Merode", "display name is harmonized to title case");
    assert!(
        g.raptor
            .transit_stop_names
            .iter()
            .all(|n| n == "Merode"),
        "plan-leg stop names are harmonized too"
    );
}

#[test]
fn cross_feed_same_name_orphans_do_not_merge() {
    let mut stib = HashMap::new();
    stib.insert(
        "1234".to_string(),
        gtfs_orphan("1234", "Markt", 50.8500, 4.3500),
    );
    stib.insert(
        "1235".to_string(),
        gtfs_orphan("1235", "markt", 50.8501, 4.3500),
    );
    preprocess_parent_stations(GtfsProvider::Stib, &mut stib, 100.0);

    let mut delijn = HashMap::new();
    delijn.insert(
        "gs:delijn:markt:1".to_string(),
        gtfs_orphan("gs:delijn:markt:1", "Markt", 51.2000, 4.4000),
    );
    delijn.insert(
        "gs:delijn:markt:2".to_string(),
        gtfs_orphan("gs:delijn:markt:2", "markt", 51.2001, 4.4000),
    );
    preprocess_parent_stations(GtfsProvider::Generic, &mut delijn, 100.0);

    let mut g = Graph::new();
    add_absorbed_stop(&mut g, &stib["1234"]);
    add_absorbed_stop(&mut g, &stib["1235"]);
    add_absorbed_stop(&mut g, &delijn["gs:delijn:markt:1"]);
    add_absorbed_stop(&mut g, &delijn["gs:delijn:markt:2"]);
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        2,
        "same-name orphan clusters from different feeds must stay two stations"
    );
    let mut ids: Vec<&str> = g
        .raptor
        .transit_stations
        .iter()
        .map(|s| s.id.as_str())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["maas:synth:1234", "maas:synth:gs:delijn:markt:1"]);
}

#[test]
fn station_index_keeps_empty_parent_stops_separate() {
    // Same display name, distinct stop_ids, nearby coords, NO parent_station:
    // they must remain two separate stations (no name/proximity merging).
    let mut g = Graph::new();
    g.add_node(transit_stop_parent("Central", "stop_a", 50.0000, 4.0000, None));
    g.add_node(transit_stop_parent("Central", "stop_b", 50.0005, 4.0005, None));
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        2,
        "stops without a parent_station stay separate even if same name / nearby"
    );
}

#[test]
fn station_index_normalizes_empty_parent_to_standalone() {
    // A stop whose `parent_station` is `Some("")` must be treated exactly like
    // `None`: it stays a standalone station keyed by its own stop_id. Locks the
    // empty-string → None normalization at the build site.
    let mut g = Graph::new();
    g.add_node(transit_stop_parent("Lonely", "lonely_id", 50.0, 4.0, Some("")));
    g.build_raptor_index();

    assert_eq!(
        g.raptor.transit_stations.len(),
        1,
        "an empty parent_station does not create a shared station"
    );
    assert_eq!(
        g.raptor.transit_stations[0].id, "lonely_id",
        "empty parent_station normalizes to the stop's own id"
    );
    assert!(
        g.station_platforms("").is_none(),
        "the empty string is never a usable station id"
    );
    assert!(g.station_platforms("lonely_id").is_some());
}

#[test]
fn station_platforms_returns_member_compact_indices() {
    let mut g = Graph::new();
    g.add_node(transit_stop_parent("Hub P1", "p1", 51.000, 3.700, Some("HUB")));
    g.add_node(transit_stop_parent("Hub P2", "p2", 51.001, 3.701, Some("HUB")));
    g.build_raptor_index();

    let mut plats = g.station_platforms("HUB").expect("known station id");
    plats.sort_unstable();
    assert_eq!(plats, vec![0, 1]);
    assert!(g.station_platforms("does-not-exist").is_none());
}

#[test]
fn station_operators_report_all_serving_agencies() {
    let mut g = Graph::new();
    // Two platforms of the same station, plus one standalone destination stop.
    let stop_a = g.add_node(transit_stop_parent("Hub A", "a", 51.000, 3.700, Some("HUB")));
    let stop_b = g.add_node(transit_stop_parent("Hub B", "b", 51.001, 3.701, Some("HUB")));
    let stop_c = g.add_node(transit_stop_parent("Dest", "c", 51.010, 3.710, None));

    g.add_transit_agencies(vec![
        AgencyInfo {
            name: "Agency Alpha".into(),
            url: String::new(),
            timezone: String::new(),
        },
        AgencyInfo {
            name: "Agency Beta".into(),
            url: String::new(),
            timezone: String::new(),
        },
    ]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "A".into(),
            route_long_name: "Route A".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "B".into(),
            route_long_name: "Route B".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(1),
            route_color: None,
            route_text_color: None,
        },
    ]);

    // Pattern 0 (Route A / Agency Alpha) serves platform A; pattern 1
    // (Route B / Agency Beta) serves platform B.
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_a, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_b, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(1),
        num_trips: 1,
    });

    g.build_raptor_index();

    let idx = g.raptor.station_id_to_index["HUB"];
    let st = &g.raptor.transit_stations[idx];
    assert_eq!(
        st.operators,
        vec!["Agency Alpha".to_string(), "Agency Beta".to_string()],
        "station served by two agencies reports BOTH operators (sorted)"
    );
}

#[test]
fn station_modes_report_all_member_route_types_deduped() {
    let mut g = Graph::new();
    let stop_a = g.add_node(transit_stop_parent("Hub A", "a", 51.000, 3.700, Some("HUB")));
    let stop_b = g.add_node(transit_stop_parent("Hub B", "b", 51.001, 3.701, Some("HUB")));
    let stop_c = g.add_node(transit_stop_parent("Bus Dest", "c", 51.010, 3.710, None));
    let stop_d = g.add_node(transit_stop_parent("Tram Dest", "d", 51.020, 3.720, None));

    g.add_transit_agencies(vec![AgencyInfo {
        name: "Agency".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "Bus".into(),
            route_long_name: "Bus Route".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "Tram".into(),
            route_long_name: "Tram Route".into(),
            route_type: RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_a, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_b, stop_d]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(1),
        num_trips: 1,
    });

    g.build_raptor_index();

    let idx = g.raptor.station_id_to_index["HUB"];
    assert_eq!(
        g.raptor.transit_stations[idx].modes,
        vec!["Bus".to_string(), "Tramway".to_string()],
        "merged station reports BOTH member modes (deduped, sorted)"
    );

    let bus_dest = g.raptor.station_id_to_index["c"];
    assert_eq!(
        g.raptor.transit_stations[bus_dest].modes,
        vec!["Bus".to_string()],
        "a single-mode station reports exactly one mode"
    );
}

#[test]
fn station_lines_dedup_color_and_sort() {
    let mut g = Graph::new();
    let stop_a = g.add_node(transit_stop_parent("Hub A", "a", 51.000, 3.700, Some("HUB")));
    let stop_b = g.add_node(transit_stop_parent("Hub B", "b", 51.001, 3.701, Some("HUB")));
    let dest = g.add_node(transit_stop_parent("Dest", "d", 51.010, 3.710, None));

    g.add_transit_agencies(vec![AgencyInfo {
        name: "Agency".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "5".into(),
            route_long_name: "Bus 5".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: Some((255, 0, 0)),
            route_text_color: Some((255, 255, 255)),
        },
        RouteInfo {
            route_short_name: "61".into(),
            route_long_name: "Bus 61".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "81".into(),
            route_long_name: "Tram 81".into(),
            route_type: RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: Some((0, 128, 0)),
            route_text_color: None,
        },
    ]);

    for (route_id, board) in [(0u32, stop_a), (0u32, stop_b), (1u32, stop_a), (2u32, stop_b)] {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[board, dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(route_id),
            num_trips: 1,
        });
    }

    g.build_raptor_index();

    let idx = g.raptor.station_id_to_index["HUB"];
    let lines = &g.raptor.transit_stations[idx].lines;

    let shape: Vec<(&str, &str, Option<&str>, Option<&str>)> = lines
        .iter()
        .map(|l| {
            (
                l.mode.as_str(),
                l.short_name.as_str(),
                l.color.as_deref(),
                l.text_color.as_deref(),
            )
        })
        .collect();

    assert_eq!(
        shape,
        vec![
            ("Tramway", "81", Some("008000"), None),
            ("Bus", "5", Some("FF0000"), Some("FFFFFF")),
            ("Bus", "61", None, None),
        ],
        "lines deduped (Bus 5 once despite two platforms), grouped Tram→Bus, \
         natural-sorted within mode (5 < 61), colours as hex / None; got {shape:?}"
    );
}

// ── zero-cost station-hub routing ───────────────────────────────────────────────

const HUB_ORIG: &str = "ORIG";
const HUB_DEST: &str = "DEST";

/// Like `two_route_raptor_graph`, but the origin and destination are physical
/// stations with TWO platforms each (collapsed by `parent_station`). The bus boards
/// at origin platform 1 and the tram alights at destination platform 1; the second
/// platform of each station is an additional non-boarding member. A mid-journey
/// footpath transfer (Stop B → Stop C) sits between the two transit legs.
fn station_hub_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_ab = g.add_node(osm_node("ab", 50.000, 4.010));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.019));
    let osm_cd = g.add_node(osm_node("cd", 50.000, 4.030));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));

    let stop_a1 = g.add_node(transit_stop_parent("Orig P1", "a1", 50.000, 4.001, Some(HUB_ORIG)));
    let stop_a2 = g.add_node(transit_stop_parent("Orig P2", "a2", 50.000, 4.0012, Some(HUB_ORIG)));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.020));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.022));
    let stop_d1 = g.add_node(transit_stop_parent("Dest P1", "d1", 50.000, 4.040, Some(HUB_DEST)));
    let stop_d2 = g.add_node(transit_stop_parent("Dest P2", "d2", 50.000, 4.0402, Some(HUB_DEST)));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, street_edge(a, b, m));
        g.add_edge(b, street_edge(b, a, m));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

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
    add_snap(&mut g, stop_a1, osm_origin, 72);
    add_snap(&mut g, stop_a2, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d1, osm_dest, 72);
    add_snap(&mut g, stop_d2, osm_dest, 72);

    g.add_edge(
        stop_a1,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a1,
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
            destination: stop_d1,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
    ]);
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

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a1, stop_b]);
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
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d1]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

fn station_query(from_station: Option<&str>, to_station: Option<&str>) -> RouteQuery {
    RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.041,
        date: chrono::NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
        time: chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
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
        from_station_id: from_station.map(|s| s.to_string()),
        to_station_id: to_station.map(|s| s.to_string()),
        profile_latency: None,
        fare_profile: None,
    }
}

fn leg_kinds(p: &maas_rs::structures::plan::Plan) -> Vec<&'static str> {
    p.legs
        .iter()
        .map(|l| match l {
            PlanLeg::Walk(_) => "Walk",
            PlanLeg::Transit(_) => "Transit",
        })
        .collect()
}

/// True iff some Walk leg sits BETWEEN two Transit legs (a mid-journey transfer).
fn has_mid_transfer_walk(p: &maas_rs::structures::plan::Plan) -> bool {
    let kinds = leg_kinds(p);
    (1..kinds.len().saturating_sub(1)).any(|i| {
        kinds[i] == "Walk"
            && kinds[..i].contains(&"Transit")
            && kinds[i + 1..].contains(&"Transit")
    })
}

#[test]
fn from_station_id_boards_with_zero_access_walk() {
    let (g, _osm_origin, _osm_dest) = station_hub_graph();
    let q = station_query(Some(HUB_ORIG), None);
    let plans = route(&g, &q, &RealtimeIndex::new()).expect("a plan from the station");

    // The property under test: a station ORIGIN boards directly with NO leading
    // access-walk leg. Assert it on the fastest transit-bearing plan. (With
    // provably-complete egress search the fastest plan here is a single ride plus
    // a long egress walk, which Pareto-dominates the fixture's 2-leg journey; the
    // mid-journey transfer-walk-survival property is covered separately by the
    // both-station tests below, which skip the wide egress pass.)
    let transit = plans
        .iter()
        .filter(|p| transit_leg_count(p) >= 1)
        .min_by_key(|p| p.end)
        .expect("a transit-bearing plan");

    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Transit(_))),
        "station origin must board directly (first leg Transit); got {:?}",
        leg_kinds(transit)
    );
}

#[test]
fn to_station_id_alights_with_zero_egress_walk() {
    let (g, _osm_origin, _osm_dest) = station_hub_graph();
    let q = station_query(None, Some(HUB_DEST));
    let plans = route(&g, &q, &RealtimeIndex::new()).expect("a plan to the station");

    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    // No trailing egress walk leg into the station.
    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Transit(_))),
        "station destination must alight directly (last leg Transit); got {:?}",
        leg_kinds(transit)
    );
    // Scoping proof: the coordinate ORIGIN still produces its access walk leg —
    // the zero-cost behaviour is confined to the chosen station endpoint.
    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Walk(_))),
        "coordinate origin must keep its access walk leg; got {:?}",
        leg_kinds(transit)
    );
}

#[test]
fn station_to_station_zero_cost_both_ends_keeps_transfer() {
    // The headline product action: both endpoints are chosen stations. Every
    // platform of each is zero-cost, so the journey is purely transit end-to-end —
    // no access walk, no egress walk — while the mid-journey transfer walk survives.
    let (g, _osm_origin, _osm_dest) = station_hub_graph();
    let q = station_query(Some(HUB_ORIG), Some(HUB_DEST));
    let plans = route(&g, &q, &RealtimeIndex::new()).expect("a station-to-station plan");

    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Transit(_))),
        "no access walk at the origin station; got {:?}",
        leg_kinds(transit)
    );
    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Transit(_))),
        "no egress walk at the destination station; got {:?}",
        leg_kinds(transit)
    );
    assert!(
        has_mid_transfer_walk(transit),
        "mid-journey transfer walk leg must survive both zero-cost endpoints; got {:?}",
        leg_kinds(transit)
    );
}

#[test]
fn unknown_station_id_falls_back_to_coordinate() {
    let (g, _osm_origin, _osm_dest) = station_hub_graph();
    // Both an unknown origin station and unknown destination station: must not
    // panic and must route as if only the coordinates were given.
    let q = station_query(Some("does-not-exist"), Some("nope"));
    let plans = route(&g, &q, &RealtimeIndex::new())
        .expect("unknown station ids fall back to coordinates and still route");

    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    // Coordinate behaviour restored on both ends: access AND egress walk legs present.
    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Walk(_))),
        "unknown origin station falls back to coordinate access walk; got {:?}",
        leg_kinds(transit)
    );
    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Walk(_))),
        "unknown destination station falls back to coordinate egress walk; got {:?}",
        leg_kinds(transit)
    );
}

/// A station whose chosen member platforms are NOT the stops the optimal journey
/// physically touches. The bus boards at a non-member stop `ox` (reachable from an
/// origin-station platform only via a footpath) and alights at a non-member stop
/// `dx` (reachable to a destination-station platform only via a footpath). The
/// origin and destination cluster osm graphs are disconnected, so no direct walk
/// plan exists and the single bus trip is the only journey.
fn station_offset_arrival_graph() -> Graph {
    let mut g = Graph::new();

    let osm_o = g.add_node(osm_node("osm_o", 50.000, 4.001));
    let osm_m = g.add_node(osm_node("osm_m", 50.000, 4.0505));

    let stop_a1 = g.add_node(transit_stop_parent("Orig P1", "a1", 50.000, 4.000, Some(HUB_ORIG)));
    let stop_a2 = g.add_node(transit_stop_parent("Orig P2", "a2", 50.000, 4.0003, Some(HUB_ORIG)));
    let stop_ox = g.add_node(transit_stop("Board X", 50.000, 4.0015));
    let stop_dx = g.add_node(transit_stop("Alight X", 50.000, 4.050));
    let stop_d1 = g.add_node(transit_stop_parent("Dest P1", "d1", 50.000, 4.051, Some(HUB_DEST)));
    let stop_d2 = g.add_node(transit_stop_parent("Dest P2", "d2", 50.000, 4.0512, Some(HUB_DEST)));

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
    add_snap(&mut g, stop_a1, osm_o, 72);
    add_snap(&mut g, stop_a2, osm_o, 72);
    add_snap(&mut g, stop_ox, osm_o, 180);
    add_snap(&mut g, stop_dx, osm_m, 72);
    add_snap(&mut g, stop_d1, osm_m, 180);
    add_snap(&mut g, stop_d2, osm_m, 185);

    g.add_edge(
        stop_ox,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_ox,
            destination: stop_dx,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 3500,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus 1".into(),
        route_type: RouteType::Bus,
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
        arrival: 9 * 3600 + 900,
        service_id: ServiceId(0),
    }]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_ox, stop_dx]);
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
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

fn offset_station_query() -> RouteQuery {
    RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.051,
        date: chrono::NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
        time: chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
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
        from_station_id: Some(HUB_ORIG.to_string()),
        to_station_id: Some(HUB_DEST.to_string()),
        profile_latency: None,
        fare_profile: None,
    }
}

fn intra_member_terminal_graph() -> Graph {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("osm_origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("osm_dest", 50.000, 4.040));

    let stop_board = g.add_node(transit_stop("Board", 50.000, 4.001));
    let d_far = g.add_node(transit_stop_parent("Dest Far", "d_far", 50.000, 4.040, Some(HUB_DEST)));
    let d_arr = g.add_node(transit_stop_parent("Dest Arr", "d_arr", 50.000, 4.040, Some(HUB_DEST)));

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
    add_snap(&mut g, stop_board, osm_origin, 72);
    add_snap(&mut g, d_far, osm_dest, 0);
    add_snap(&mut g, d_arr, osm_dest, 0);

    g.add_edge(
        stop_board,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_board,
            destination: d_arr,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 3500,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus 1".into(),
        route_type: RouteType::Bus,
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
        arrival: 9 * 3600 + 900,
        service_id: ServiceId(0),
    }]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_board, d_arr]);
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
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

fn intra_member_terminal_query() -> RouteQuery {
    RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.040,
        date: chrono::NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
        time: chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
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
        to_station_id: Some(HUB_DEST.to_string()),
        profile_latency: None,
        fare_profile: None,
    }
}

fn intra_member_origin_graph() -> Graph {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("osm_origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("osm_dest", 50.000, 4.040));

    let a_far = g.add_node(transit_stop_parent("Orig Far", "a_far", 50.000, 4.000, Some(HUB_ORIG)));
    let a_board =
        g.add_node(transit_stop_parent("Orig Board", "a_board", 50.000, 4.000, Some(HUB_ORIG)));
    let stop_alight = g.add_node(transit_stop("Alight", 50.000, 4.039));

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
    add_snap(&mut g, a_far, osm_origin, 0);
    add_snap(&mut g, a_board, osm_origin, 0);
    add_snap(&mut g, stop_alight, osm_dest, 72);

    g.add_edge(
        a_board,
        EdgeData::Transit(TransitEdgeData {
            origin: a_board,
            destination: stop_alight,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 3500,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus 1".into(),
        route_type: RouteType::Bus,
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
        arrival: 9 * 3600 + 900,
        service_id: ServiceId(0),
    }]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[a_board, stop_alight]);
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
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

fn intra_member_origin_query() -> RouteQuery {
    RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.040,
        date: chrono::NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
        time: chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
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
        from_station_id: Some(HUB_ORIG.to_string()),
        to_station_id: None,
        profile_latency: None,
        fare_profile: None,
    }
}

#[test]
fn to_station_intra_member_no_trailing_transfer_walk() {
    let g = intra_member_terminal_graph();
    let plans = route(&g, &intra_member_terminal_query(), &RealtimeIndex::new())
        .expect("a plan to the station");
    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Transit(_))),
        "a transit arrival at a member platform must terminate the plan with no \
         intra-member transfer walk; got {:?}",
        leg_kinds(transit)
    );
    assert_eq!(
        transit.end,
        9 * 3600 + 900,
        "arrival is the transit arrival at the platform, not a transfer-extended time"
    );
}

#[test]
fn to_station_cross_boundary_egress_walk_preserved() {
    let g = station_offset_arrival_graph();
    let plans = route(&g, &offset_station_query(), &RealtimeIndex::new())
        .expect("a station-to-station plan");
    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    let last = transit.legs.last().expect("at least one leg");
    let walk = match last {
        PlanLeg::Walk(w) => w,
        _ => panic!(
            "cross-boundary arrival must keep its egress walk leg; got {:?}",
            leg_kinds(transit)
        ),
    };
    assert!(
        walk.duration > 0,
        "the preserved cross-boundary egress walk has a real positive duration"
    );
    assert_eq!(
        transit.end, walk.end,
        "the arrival time includes the preserved egress walk"
    );
}

#[test]
fn from_station_intra_member_no_leading_transfer_walk() {
    let g = intra_member_origin_graph();
    let plans = route(&g, &intra_member_origin_query(), &RealtimeIndex::new())
        .expect("a plan from the station");
    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Transit(_))),
        "a transit boarding at a member platform must start the plan with no \
         intra-member transfer walk; got {:?}",
        leg_kinds(transit)
    );
}

#[test]
fn station_to_station_mid_journey_transfer_walk_preserved() {
    let (g, _osm_origin, _osm_dest) = station_hub_graph();
    let q = station_query(Some(HUB_ORIG), Some(HUB_DEST));
    let plans = route(&g, &q, &RealtimeIndex::new()).expect("a station-to-station plan");

    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Transit(_))),
        "no access walk at the origin station; got {:?}",
        leg_kinds(transit)
    );
    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Transit(_))),
        "no egress walk at the destination station; got {:?}",
        leg_kinds(transit)
    );
    assert!(
        has_mid_transfer_walk(transit),
        "mid-journey transfer walk leg must be preserved; got {:?}",
        leg_kinds(transit)
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
        car: false,
        attrs: BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: VarGen::NONE,
    })
}

fn street_edge_full(
    origin: NodeID,
    destination: NodeID,
    length_m: usize,
    foot: bool,
    bike: bool,
    car: bool,
) -> StreetEdgeData {
    StreetEdgeData {
        origin,
        destination,
        length: length_m,
        partial: false,
        foot,
        bike,
        car,
        attrs: BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: VarGen::NONE,
    }
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
                car: true,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
        ..Default::default()
        }); // stop_A, trip 0
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        ..Default::default()
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
        ..Default::default()
        }); // stop_C, trip 1
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
        ..Default::default()
        }); // stop_D, trip 1
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    (g, osm_origin, osm_dest)
}

// ── Brupass (Appendix A.3) end-to-end ─────────────────────────────────────────

/// A Brussels flat zone (Agglomeration::Brussels) box covering the two-route test
/// graph's stops (all at lat 50.000, lng ~4.00-4.04), so every boarding is in-zone.
fn brussels_zone_over_two_route() -> maas_rs::structures::cost::AgglomerationZone {
    use maas_rs::structures::LatLng;
    use maas_rs::structures::cost::{Agglomeration, AgglomerationZone};
    AgglomerationZone {
        zone: Agglomeration::Brussels,
        polygon: vec![
            LatLng { latitude: 49.95, longitude: 3.95 },
            LatLng { latitude: 49.95, longitude: 4.10 },
            LatLng { latitude: 50.05, longitude: 4.10 },
            LatLng { latitude: 50.05, longitude: 3.95 },
        ],
        reference: None,
    }
}

/// The two-route graph, re-wired so route 0 is STIB (agency 0) and route 1 is
/// De Lijn (agency 1), with a Brussels flat zone over all stops and a fare model
/// carrying STIB (2.40), De Lijn (3.00) and a Brupass placeholder (`brupass_cents`).
/// A STIB→De Lijn journey uses TWO in-zone operators.
fn two_operator_brussels_graph(brupass_cents: u32) -> (Graph, NodeID, NodeID) {
    use maas_rs::structures::cost::{
        FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel, TimeWindowOperator,
    };
    let (mut g, origin, dest) = two_route_raptor_graph();
    // Two agencies; re-point the tram route (1) to De Lijn so the journey spans two
    // distinct in-zone operators.
    g.raptor.transit_agencies = vec![
        AgencyInfo { name: "STIB".into(), url: String::new(), timezone: String::new() },
        AgencyInfo { name: "De Lijn".into(), url: String::new(), timezone: String::new() },
    ];
    g.raptor.transit_routes[1].agency_id = AgencyId(1);

    let model = FareModel {
        enabled: true,
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![
            OperatorFare {
                name: "STIB".into(),
                model: OperatorModel::TimeWindowFlat {
                    ticket_cents: 240,
                    card_cents: None,
                    validity_secs: 5400,
                    operator: TimeWindowOperator::Stib,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            },
            OperatorFare {
                name: "De Lijn".into(),
                model: OperatorModel::TimeWindowFlat {
                    ticket_cents: 300,
                    card_cents: Some(220),
                    validity_secs: 3600,
                    operator: TimeWindowOperator::Delijn,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            },
        ],
        agglomerations: vec![brussels_zone_over_two_route()],
        brupass_cents,
        brupass_validity_secs: 3600,
    };
    g.set_fare_model(model);
    (g, origin, dest)
}

/// Route a two-operator (STIB→De Lijn) in-zone journey with the given fare profile
/// and return the minimum `known_euros` across the returned two-transit plans.
fn min_two_transit_price(
    g: &Graph,
    origin: NodeID,
    dest: NodeID,
    profile: maas_rs::structures::cost::FareProfile,
) -> f64 {
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let plans = g.raptor_tuned_rt_modes_ep(
        origin,
        dest,
        8 * 3600 + 3000,
        0,
        0x7F,
        10 * 60,
        &buckets,
        g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &RealtimeIndex::new(),
        &ActiveModes::default(),
        &BikeCost::new(BikeProfile::default()),
        None,
        profile,
    );
    plans
        .iter()
        .filter(|p| transit_leg_count(p) == 2)
        .filter_map(|p| p.price.as_ref().map(|pr| pr.known_euros))
        .fold(f64::INFINITY, f64::min)
}

/// The two-operator (STIB→De Lijn) Brussels graph but with ALL stops moved OUT of
/// the Brussels zone (empty `agglomerations`), so no in-zone Brupass cap can fire.
/// Used to assert the plain two-ticket baseline.
fn two_operator_no_zone_graph() -> (Graph, NodeID, NodeID) {
    let (mut g, origin, dest) = two_operator_brussels_graph(260);
    let mut model = g.raptor.fare_model.clone();
    model.agglomerations = Vec::new(); // no Brussels zone → no in-zone boardings
    g.set_fare_model(model);
    (g, origin, dest)
}

#[test]
fn brupass_cap_applies_automatically_for_two_operator_in_zone_journey() {
    // Brupass is NOT a user option: it is an automatic cap. STIB (2.40) + De Lijn
    // (3.00) = 5.40 for two separate tickets; because both boardings are in the
    // Brussels zone on 2 DISTINCT operators, and the 2.60 Brupass is cheaper, the
    // in-zone multi-operator sum is capped at 2.60. With no Brussels zone the same
    // journey pays 5.40 (nothing to cap).
    let profile = maas_rs::structures::cost::FareProfile::default();

    let (g_zone, o, d) = two_operator_brussels_graph(260);
    let capped = min_two_transit_price(&g_zone, o, d, profile);
    assert!(
        (capped - 2.60).abs() < 1e-9,
        "one Brupass caps both in-zone operators at 2.60, got {capped}"
    );

    let (g_nozone, o2, d2) = two_operator_no_zone_graph();
    let baseline = min_two_transit_price(&g_nozone, o2, d2, profile);
    assert!((baseline - 5.40).abs() < 1e-9, "two separate tickets cost 5.40, got {baseline}");
    assert!(capped < baseline, "Brupass cap must be cheaper for a 2-operator in-zone trip");
}

/// The cheapest two-transit plan's fare breakdown for a profile.
fn min_two_transit_breakdown(
    g: &Graph,
    origin: NodeID,
    dest: NodeID,
    profile: maas_rs::structures::cost::FareProfile,
) -> Vec<maas_rs::structures::plan::FareBreakdownItem> {
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let plans = g.raptor_tuned_rt_modes_ep(
        origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60, &buckets,
        g.raptor.arrival_slack_secs, g.raptor.unrestricted_transfers, g.raptor.use_cch_access,
        &RealtimeIndex::new(), &ActiveModes::default(), &BikeCost::new(BikeProfile::default()),
        None, profile,
    );
    plans
        .iter()
        .filter(|p| transit_leg_count(p) == 2)
        .filter_map(|p| p.price.as_ref())
        .min_by(|a, b| a.capped_euros.partial_cmp(&b.capped_euros).unwrap())
        .map(|pr| pr.breakdown.clone())
        .unwrap_or_default()
}

#[test]
fn breakdown_two_separate_tickets_when_brupass_dearer() {
    // Brupass dearer than two tickets (10.00 > 5.40): the cap does NOT fire, so the
    // breakdown keeps the two individual paid items summing to 5.40 (no Brupass item).
    let (g, origin, dest) = two_operator_brussels_graph(1000);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    assert!(!items.iter().any(|i| i.operator == "Brupass"), "no Brupass item: {items:?}");
    let paid: Vec<_> = items.iter().filter(|i| i.euros > 0.0).collect();
    assert_eq!(paid.len(), 2, "two separate paid tickets: {items:?}");
    let sum: f64 = items.iter().map(|i| i.euros).sum();
    assert!((sum - 5.40).abs() < 1e-9, "breakdown sums to 5.40, got {sum}");
    // Operators are named.
    assert!(items.iter().any(|i| i.operator == "STIB"));
    assert!(items.iter().any(|i| i.operator == "De Lijn"));
}

#[test]
fn breakdown_brupass_one_item_covered_legs_annotated() {
    // Brupass cap fires automatically: ONE Brupass item (2.60) replaces both in-zone
    // operators' tickets; the replaced legs become €0 items with coverage "Brupass".
    // The sum equals the capped total (2.60).
    let (g, origin, dest) = two_operator_brussels_graph(260);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    let brupass_items: Vec<_> = items.iter().filter(|i| i.operator == "Brupass").collect();
    assert_eq!(brupass_items.len(), 1, "exactly one Brupass item: {items:?}");
    assert!((brupass_items[0].euros - 2.60).abs() < 1e-9, "Brupass costs 2.60");
    assert!(brupass_items[0].coverage.is_none(), "the Brupass item itself is paid (coverage None)");
    // At least one replaced leg annotated with the Brupass coverage reason.
    assert!(
        items.iter().any(|i| i.coverage.as_deref() == Some("Brupass") && i.euros == 0.0),
        "a replaced in-zone leg is annotated: {items:?}"
    );
    let sum: f64 = items.iter().map(|i| i.euros).sum();
    assert!((sum - 2.60).abs() < 1e-9, "breakdown sums to the Brupass price 2.60, got {sum}");
}

#[test]
fn breakdown_one_stib_ticket_across_windowed_transfer() {
    // Both STIB boardings share one 90-min ticket: ONE paid item (2.10) plus a covered
    // within-window item; the sum equals capped (2.10).
    let (g, origin, dest) = two_route_stib_graph();
    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 2)
        .and_then(|p| p.price.as_ref())
        .expect("a priced two-transit plan");
    let paid: Vec<_> = priced.breakdown.iter().filter(|i| i.euros > 0.0).collect();
    assert_eq!(paid.len(), 1, "one STIB ticket bought: {:?}", priced.breakdown);
    assert!((paid[0].euros - 2.10).abs() < 1e-9);
    assert!(
        priced.breakdown.iter().any(|i| i.coverage.is_some() && i.euros == 0.0),
        "the second windowed board is covered by the same ticket: {:?}",
        priced.breakdown
    );
    let sum: f64 = priced.breakdown.iter().map(|i| i.euros).sum();
    assert!((sum - 2.10).abs() < 1e-9, "breakdown sums to 2.10, got {sum}");
}

#[test]
fn brupass_single_operator_in_zone_unchanged() {
    // Only ONE distinct operator in-zone (both boardings STIB): the Brupass cap needs
    // 2+ distinct in-zone operators, so it does NOT fire. The plan prices on the plain
    // STIB rule — one 90-min ticket shared across the windowed transfer (2.40) — and
    // carries no Brupass item, even with a cheap (2.60) Brupass configured.
    let (mut g, origin, dest) = two_operator_brussels_graph(260);
    // Re-point route 1 back to STIB (agency 0) so both legs are the same operator, then
    // rebuild the fare lookup so route 1 charges the STIB model (not the stale De Lijn).
    g.raptor.transit_routes[1].agency_id = AgencyId(0);
    let model = g.raptor.fare_model.clone();
    g.set_fare_model(model);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let min = min_two_transit_price(&g, origin, dest, profile);
    // Two STIB boardings: one 90-min ticket if the transfer is within the window (2.40),
    // otherwise two STIB tickets (4.80). Either way NO Brupass (single operator in-zone).
    assert!(
        (min - 2.40).abs() < 1e-9 || (min - 4.80).abs() < 1e-9,
        "single-operator in-zone journey stays on STIB tickets, no Brupass, got {min}"
    );
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    assert!(!items.iter().any(|i| i.operator == "Brupass"), "no Brupass for one operator: {items:?}");
}

#[test]
fn brupass_dearer_not_forced_over_cheaper_tickets() {
    // If the Brupass price is HIGHER than the two individual in-zone tickets (10.00 >
    // 5.40), the cap must NOT fire: the plan keeps the two-ticket total (5.40).
    let (g, origin, dest) = two_operator_brussels_graph(1000); // 10.00 EUR Brupass
    let profile = maas_rs::structures::cost::FareProfile::default();
    let min = min_two_transit_price(&g, origin, dest, profile);
    assert!(
        (min - 5.40).abs() < 1e-9,
        "a dearer Brupass must not displace the cheaper two-ticket plan, got {min}"
    );
}

#[test]
fn brupass_ignores_subscription_covered_leg() {
    // A subscription makes an operator's legs €0 (coverage set). Those legs are NOT
    // counted toward the Brupass multi-operator sum and are NOT replaced. With a STIB
    // subscription, only De Lijn (3.00) is a PAID in-zone operator → a single distinct
    // paid operator → the Brupass cap does NOT fire; the total is just 3.00.
    let (g, origin, dest) = two_operator_brussels_graph(260);
    let profile = maas_rs::structures::cost::FareProfile {
        stib_subscription: true,
        ..Default::default()
    };
    let min = min_two_transit_price(&g, origin, dest, profile);
    assert!(
        (min - 3.00).abs() < 1e-9,
        "subscription STIB leg is free and uncounted; only De Lijn (3.00) is paid, got {min}"
    );
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    assert!(!items.iter().any(|i| i.operator == "Brupass"), "no Brupass with one paid operator: {items:?}");
    // The STIB leg is present as a €0 subscription-covered item, untouched by Brupass.
    assert!(
        items.iter().any(|i| i.operator == "STIB" && i.euros == 0.0
            && i.coverage.as_deref() == Some("STIB subscription")),
        "STIB subscription item preserved (not Brupass-covered): {items:?}"
    );
}

// ── Transit pricing (fares) end-to-end ────────────────────────────────────────

/// A STIB `time_window_flat` fare model: 2.10 EUR ticket, 90-minute window.
fn stib_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
        enabled: true,
        // Disable euro bucketing so the exact 210-cent ticket price is asserted.
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![OperatorFare {
            name: "STIB".into(),
            model: OperatorModel::TimeWindowFlat {
                ticket_cents: 210,
                card_cents: None,
                validity_secs: 5400,
                operator: maas_rs::structures::cost::TimeWindowOperator::Stib,
            },
            express_route_names: Vec::new(),
            express_route_prefixes: Vec::new(),
            express_single_cents: 0,
            express_card6_cents: 0,
            express_card6_reduced_cents: 0,
            airport_station_names: Vec::new(),
        }],
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

/// The two-route graph (bus A→B 09:00, tram C→D 09:30) with agency 0 named STIB,
/// so both boardings are STIB. The bus and tram board 30 min apart (< 90-min
/// window), so the whole journey costs ONE ticket. `set_fare_model` runs after
/// `build_raptor_index`, matching production ordering (`apply_routing_defaults`).
fn two_route_stib_graph() -> (Graph, NodeID, NodeID) {
    let (mut g, origin, dest) = two_route_raptor_graph();
    g.add_transit_agencies(vec![AgencyInfo {
        name: "STIB".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.set_fare_model(stib_fare_model());
    (g, origin, dest)
}

#[test]
fn fares_charge_one_stib_ticket_across_a_windowed_transfer() {
    let (g, origin, dest) = two_route_stib_graph();
    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    // The two-transit STIB journey (bus + tram): both boardings share one ticket
    // because they fall within the 90-minute window, so known price is 2.10 EUR.
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 2)
        .expect("a two-transit bus+tram plan");
    let price = priced.price.as_ref().expect("fares enabled ⇒ Plan.price populated");
    assert!(
        (price.known_euros - 2.10).abs() < 1e-9,
        "one STIB ticket for both windowed boardings; got {}",
        price.known_euros
    );
    assert_eq!(price.capped_euros, price.known_euros, "cap == known this increment");
    assert!(
        price.unknown_operators.is_empty(),
        "both legs are modeled (STIB), so no unknown operators"
    );
}

#[test]
fn fares_disabled_yields_no_price_and_same_plans() {
    // Byte-identity of the disabled path: the SAME graph without fares enabled must
    // return the same set of plans (by mode/start/end/leg-count) and no Plan.price.
    let (with_fares, origin, dest) = two_route_stib_graph();
    let (without_fares, o2, d2) = two_route_raptor_graph(); // no agency, fares off
    let start = 8 * 3600 + 3000;

    let on = with_fares.raptor(origin, dest, start, 0, 0x7F, 10 * 60);
    let off = without_fares.raptor(o2, d2, start, 0, 0x7F, 10 * 60);

    // The fares-off graph must carry no price at all.
    assert!(
        off.iter().all(|p| p.price.is_none()),
        "disabled fares surface no Plan.price"
    );
    // The plan STRUCTURE (mode/start/end/leg-count) is identical with fares on vs off
    // — the price axis adds a field but does not change which plans are returned here.
    let sig = |ps: &[maas_rs::structures::plan::Plan]| {
        let mut v: Vec<_> = ps
            .iter()
            .map(|p| (p.mode, p.start, p.end, p.legs.len()))
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        sig(&on),
        sig(&off),
        "enabling fares must not change which plans are returned (only add price)"
    );
}

// ── SNCB per-km fares (Increment 2) ───────────────────────────────────────────

fn sncb_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
        enabled: true,
        // No euro bucketing so exact base + per-km cents are asserted.
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![OperatorFare {
            name: "SNCB".into(),
            model: OperatorModel::DistanceBasePerKm {
                tariff: sncb_test_tariff(),
                rules: sncb_test_rules(),
                airport_od_cents: 0,
            },
            express_route_names: Vec::new(),
            express_route_prefixes: Vec::new(),
            express_single_cents: 0,
            express_card6_cents: 0,
            express_card6_reduced_cents: 0,
            airport_station_names: Vec::new(),
        }],
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

/// SNCB test tariff: the EXACT published 2026 2nd-class BRACKETED model
/// (a = 0.168546 EUR/km, b = 1.451226 EUR, floor 2.6151 EUR = 262 c, min 3 km,
/// cap ≥116 km → 118 km). The end-to-end tests compute their expected fare by
/// calling `sncb_test_tariff().fare_cents(d_km)`, so they assert the routing plumbs
/// the right railway distance into the tariff rather than hardcoding a formula.
fn sncb_test_tariff() -> maas_rs::structures::cost::DistanceTariff {
    maas_rs::structures::cost::DistanceTariff::Bracketed {
        a_cents_per_km: 16.8546,
        b_cents: 145.1226,
        floor_cents: 262,
        min_km: 3,
        cap_from_km: 116,
        cap_km: 118,
        first_class_thresholds: [36, 51],
        first_class_coeffs: [1.40, 1.50, 1.60],
        first_class_round_thresholds: [2500, 5000],
        first_class_round_grids: [10, 50, 100],
    }
}

/// SNCB time rules with no peak windows/discounts (so fare tests assert the raw
/// base+per-km, matching the pre-time-bucket expectations).
fn sncb_test_rules() -> maas_rs::structures::cost::SncbTimeRules {
    maas_rs::structures::cost::SncbTimeRules {
        peak_windows: [(0, 0); 2],
        n_peak_windows: 0,
        weekend_discount_adult: 0.0,
        weekend_discount_reduced: 0.0,
        train_plus_offpeak_discount: 0.0,
        train_plus_peak_cap_adult: u32::MAX,
        train_plus_peak_cap_reduced: u32::MAX,
    }
}

/// A single SNCB pattern over 3 stops laid on a straight rail chain, with a
/// railway topology whose nodes coincide with the stop coordinates. Returns the
/// graph (fare model installed) plus the expected railway metres between the
/// first stop and stops 1 and 2 (cumulative).
fn sncb_three_stop_graph() -> Graph {
    let mut g = Graph::new();
    // Three SNCB stations along a meridian. ~1113 m per 0.01 deg latitude.
    let s0 = g.add_node(transit_stop("Gare 0", 50.00, 4.00));
    let s1 = g.add_node(transit_stop("Gare 1", 50.10, 4.00));
    let s2 = g.add_node(transit_stop("Gare 2", 50.30, 4.00));

    g.add_transit_agencies(vec![AgencyInfo {
        name: "SNCB".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "IC".into(),
        route_long_name: "InterCity".into(),
        route_type: RouteType::Rail,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[s0, s1, s2]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });
    g.build_raptor_index();

    // Railway topology: a straight chain whose nodes sit exactly on the stops, so
    // snapping is exact and railway distance == the stop-to-stop haversine.
    let rail: Vec<(f64, f64)> = vec![(50.00, 4.00), (50.10, 4.00), (50.30, 4.00)];
    let d01 = LatLng { latitude: 50.00, longitude: 4.00 }
        .dist(LatLng { latitude: 50.10, longitude: 4.00 }) as u32;
    let d12 = LatLng { latitude: 50.10, longitude: 4.00 }
        .dist(LatLng { latitude: 50.30, longitude: 4.00 }) as u32;
    let adj = vec![
        vec![(1usize, d01)],
        vec![(0usize, d01), (2usize, d12)],
        vec![(1usize, d12)],
    ];
    g.store_railway_graph(rail, adj);

    // Installing the fare model triggers the SNCB railway-km precompute.
    g.set_fare_model(sncb_fare_model());
    g
}

#[test]
fn sncb_railway_km_precompute_is_cumulative_and_monotonic() {
    let g = sncb_three_stop_graph();
    let cum = &g.raptor.sncb_pattern_cum_railway_m[0];
    assert_eq!(cum.len(), 3, "one cumulative entry per pattern stop");
    assert_eq!(cum[0], 0.0, "cumulative distance starts at zero");
    // Monotonic non-decreasing.
    assert!(cum[1] >= cum[0] && cum[2] >= cum[1], "cumulative array is monotonic");

    // Compare against the direct stop-to-stop railway distances (== haversine here,
    // since the rail nodes coincide with the stops).
    let d01 = LatLng { latitude: 50.00, longitude: 4.00 }
        .dist(LatLng { latitude: 50.10, longitude: 4.00 });
    let d12 = LatLng { latitude: 50.10, longitude: 4.00 }
        .dist(LatLng { latitude: 50.30, longitude: 4.00 });
    assert!((cum[1] - d01).abs() < 5.0, "cum[1] ≈ rail d(0,1)");
    assert!((cum[2] - (d01 + d12)).abs() < 5.0, "cum[2] ≈ rail d(0,1)+d(1,2)");
}

#[test]
fn sncb_railway_km_falls_back_to_haversine_on_disconnected_rail() {
    // Same stops, but a railway topology with NO edges: every segment is
    // unreachable over rail, so the precompute must fall back to the haversine
    // straight line between the two stop coordinates rather than panic.
    let mut g = Graph::new();
    let s0 = g.add_node(transit_stop("Gare 0", 50.00, 4.00));
    let s1 = g.add_node(transit_stop("Gare 1", 50.10, 4.00));
    g.add_transit_agencies(vec![AgencyInfo {
        name: "SNCB".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "IC".into(),
        route_long_name: "InterCity".into(),
        route_type: RouteType::Rail,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[s0, s1]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });
    g.build_raptor_index();
    // Two isolated rail nodes at the stops: no adjacency edges.
    g.store_railway_graph(vec![(50.00, 4.00), (50.10, 4.00)], vec![vec![], vec![]]);
    g.set_fare_model(sncb_fare_model());

    let cum = &g.raptor.sncb_pattern_cum_railway_m[0];
    let hav = LatLng { latitude: 50.00, longitude: 4.00 }
        .dist(LatLng { latitude: 50.10, longitude: 4.00 });
    assert!(
        (cum[1] - hav).abs() < 5.0,
        "disconnected rail falls back to haversine; got {} vs {}",
        cum[1],
        hav
    );
}

#[test]
fn sncb_precompute_skipped_when_fares_disabled() {
    let mut g = sncb_three_stop_graph();
    // Re-install a disabled fare model: the precompute must clear itself and do
    // no rail-Dijkstra work (byte-identical disabled path).
    g.set_fare_model(maas_rs::structures::cost::FareModel::default());
    assert!(
        g.raptor.sncb_pattern_cum_railway_m.is_empty(),
        "disabled fares skip the SNCB railway-km precompute entirely"
    );
}

/// A routable single-SNCB-train graph: one pattern over three stations A→B→C on a
/// straight rail chain, so a rider boards once at A and alights at C, riding two
/// hops on the same ticket. Returns `(graph, origin_osm, dest_osm, railway_m_AC)`
/// where `railway_m_AC` is the total railway distance A→C used by the per-km fare.
fn sncb_routable_graph() -> (Graph, NodeID, NodeID, f64) {
    let mut g = Graph::new();

    // OSM access/egress nodes near the boarding and alighting stations.
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));

    // Three SNCB stations along a meridian.
    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Gare B", 50.100, 4.001));
    let stop_c = g.add_node(transit_stop("Gare C", 50.300, 4.001));

    // Snap edges origin↔A and dest↔C (foot access/egress).
    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_c, osm_dest, 72);

    // Transit edges A→B and B→C on the same route.
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 11_100,
    }));
    g.add_edge(stop_b, EdgeData::Transit(TransitEdgeData {
        origin: stop_b, destination: stop_c, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 1, len: 1 }, length: 22_200,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_agencies(vec![AgencyInfo {
        name: "SNCB".into(), url: String::new(), timezone: String::new(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "IC".into(), route_long_name: "InterCity".into(),
        route_type: RouteType::Rail, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0),
        bikes_allowed: None,
    }]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 600, service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 1, destination_stop_sequence: 2,
            departure: 9 * 3600 + 600, arrival: 9 * 3600 + 1200, service_id: ServiceId(0),
        },
    ]);

    // One pattern: [A, B, C], one trip, column-major stop times.
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_a, stop_b, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });
    let ts = g.transit_pattern_trips_len();
    g.push_transit_pattern_trip(TripId(0));
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
    let sts = g.transit_pattern_stop_times_len();
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600, ..Default::default() });
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600, ..Default::default() });
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1200, departure: 9 * 3600 + 1200, ..Default::default() });
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });
    g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });

    g.build_raptor_index();
    enable_contraction(&mut g);

    // Rail chain coincident with the stops so railway distance == haversine.
    let coords = [(50.000, 4.001), (50.100, 4.001), (50.300, 4.001)];
    let rail: Vec<(f64, f64)> = coords.to_vec();
    let d01 = LatLng { latitude: coords[0].0, longitude: coords[0].1 }
        .dist(LatLng { latitude: coords[1].0, longitude: coords[1].1 });
    let d12 = LatLng { latitude: coords[1].0, longitude: coords[1].1 }
        .dist(LatLng { latitude: coords[2].0, longitude: coords[2].1 });
    let adj = vec![
        vec![(1usize, d01 as u32)],
        vec![(0usize, d01 as u32), (2usize, d12 as u32)],
        vec![(1usize, d12 as u32)],
    ];
    g.store_railway_graph(rail, adj);
    g.set_fare_model(sncb_fare_model());

    (g, osm_origin, osm_dest, d01 + d12)
}

#[test]
fn sncb_end_to_end_charges_base_plus_per_km() {
    let (g, origin, dest, railway_m) = sncb_routable_graph();
    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 1)
        .expect("a single-SNCB-train plan A→C");
    let price = priced.price.as_ref().expect("fares enabled ⇒ Plan.price populated");

    // Expected: the bracketed tariff of the full A→C railway distance.
    let expected = sncb_test_tariff().fare_cents(railway_m / 1000.0) as f64 / 100.0;
    assert!(
        (price.known_euros - expected).abs() < 0.02,
        "SNCB price should be the bracketed tariff of the railway distance: got {} expected ~{}",
        price.known_euros,
        expected
    );
    assert!(
        price.unknown_operators.is_empty(),
        "SNCB is modeled, so no unknown operators"
    );
}

/// The `sncb_routable_graph`, but with a Brussels flat zone covering stops B and
/// C (both at lat 50.10 / 50.30) so the B→C railway segment is collapsed to 0 km.
/// Returns `(graph, origin, dest, railway_m_AB)` where `railway_m_AB` is the only
/// chargeable railway distance (A→B); B→C is intra-zone and adds nothing.
fn sncb_zoned_routable_graph() -> (Graph, NodeID, NodeID, f64) {
    use maas_rs::structures::LatLng;
    use maas_rs::structures::cost::{
        Agglomeration, AgglomerationZone, FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel,
    };
    let (mut g, origin, dest, _railway_ac) = sncb_routable_graph();
    // A zone box covering B (50.10) and C (50.30) but NOT A (50.00).
    let zone = AgglomerationZone {
        zone: Agglomeration::Brussels,
        polygon: vec![
            LatLng { latitude: 50.05, longitude: 3.90 },
            LatLng { latitude: 50.05, longitude: 4.10 },
            LatLng { latitude: 50.40, longitude: 4.10 },
            LatLng { latitude: 50.40, longitude: 3.90 },
        ],
        reference: None,
    };
    let model = FareModel {
        enabled: true,
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![OperatorFare {
            name: "SNCB".into(),
            model: OperatorModel::DistanceBasePerKm {
                tariff: sncb_test_tariff(),
                rules: sncb_test_rules(),
                airport_od_cents: 0,
            },
            express_route_names: Vec::new(),
            express_route_prefixes: Vec::new(),
            express_single_cents: 0,
            express_card6_cents: 0,
            express_card6_reduced_cents: 0,
            airport_station_names: Vec::new(),
        }],
        agglomerations: vec![zone],
        ..FareModel::default()
    };
    g.set_fare_model(model);
    // Only A→B is chargeable now (B, C are both in-zone).
    let coords = [(50.000, 4.001), (50.100, 4.001)];
    let d_ab = LatLng { latitude: coords[0].0, longitude: coords[0].1 }
        .dist(LatLng { latitude: coords[1].0, longitude: coords[1].1 });
    (g, origin, dest, d_ab)
}

#[test]
fn sncb_end_to_end_zone_to_station_is_fixed() {
    use maas_rs::structures::cost::Agglomeration;
    // A(free) → C(Brussels): the corrected spec (Appendix A.2) prices the SNCB fare
    // as base + per-km × the FIXED zone-to-station distance from A to the Brussels
    // reference node, NOT the pattern-dependent along-path distance. This exercises
    // the full routing/pricing path; the exact distance is read from the graph's own
    // reference-node table so the test is robust to the ref-station choice.
    let (g, origin, dest, _railway_m_ab) = sncb_zoned_routable_graph();
    assert_eq!(g.raptor.sncb_stop_zone[0], Agglomeration::None, "A outside zone");
    assert_eq!(g.raptor.sncb_stop_zone[1], Agglomeration::Brussels, "B in zone");
    assert_eq!(g.raptor.sncb_stop_zone[2], Agglomeration::Brussels, "C in zone");

    // The fixed zone-to-station fare distance A(0) → any Brussels station, via the
    // graph's own zone-collapse lookup (Brussels board B(1) → free A(0), symmetric).
    let d_fixed = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
    assert!(d_fixed > 0.0, "A→Brussels has a real fixed distance");

    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 1)
        .expect("a single-SNCB-train plan A→C");
    let price = priced.price.as_ref().expect("fares enabled ⇒ Plan.price populated");

    // The bracketed tariff of the fixed zone-to-station distance.
    let expected = sncb_test_tariff().fare_cents(d_fixed / 1000.0) as f64 / 100.0;
    assert!(
        (price.known_euros - expected).abs() < 0.02,
        "zoned SNCB price is the bracketed tariff of the fixed zone distance: got {} expected ~{}",
        price.known_euros,
        expected
    );
    // Regression guard: the reference distance is a real multi-km rail distance, so
    // the priced fare must be clearly ABOVE base (the live bug collapsed it to base).
    assert!(
        price.known_euros > 2.60,
        "zone->station fare must exceed base (ref distance must be non-zero): got {}",
        price.known_euros
    );
}

// ── SNCB airport special-OD (fixed 7.90 override) ─────────────────────────────

/// An SNCB fare model with the fixed airport special-OD override wired: base 2.50,
/// per-km 0.11, airport OD 7.90, station-name token "Airport". A ride whose board
/// OR alight stop name contains "AIRPORT" prices at the flat 7.90, not base+per-km.
fn sncb_airport_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
        enabled: true,
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![OperatorFare {
            name: "SNCB".into(),
            model: OperatorModel::DistanceBasePerKm {
                tariff: sncb_test_tariff(),
                rules: sncb_test_rules(),
                airport_od_cents: 790,
            },
            express_route_names: Vec::new(),
            express_route_prefixes: Vec::new(),
            express_single_cents: 0,
            express_card6_cents: 0,
            express_card6_reduced_cents: 0,
            // Config compiles these uppercased; the tagger uppercases the stop name.
            airport_station_names: vec!["AIRPORT".into()],
        }],
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

/// The `sncb_routable_graph` topology, but station C is the airport station
/// ("Brussels Airport-Zaventem") and the airport fare model is installed, so an
/// A→C SNCB ride is an airport OD. Returns `(graph, origin_osm, dest_osm)`.
fn sncb_airport_routable_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));
    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Gare B", 50.100, 4.001));
    // The airport station: its name contains the "Airport" token.
    let stop_c = g.add_node(transit_stop("Brussels Airport-Zaventem", 50.300, 4.001));

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_c, osm_dest, 72);

    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 11_100,
    }));
    g.add_edge(stop_b, EdgeData::Transit(TransitEdgeData {
        origin: stop_b, destination: stop_c, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 1, len: 1 }, length: 22_200,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_agencies(vec![AgencyInfo {
        name: "SNCB".into(), url: String::new(), timezone: String::new(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "IC".into(), route_long_name: "InterCity".into(),
        route_type: RouteType::Rail, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0),
        bikes_allowed: None,
    }]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 600, service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(0), origin_stop_sequence: 1, destination_stop_sequence: 2,
            departure: 9 * 3600 + 600, arrival: 9 * 3600 + 1200, service_id: ServiceId(0),
        },
    ]);

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_a, stop_b, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });
    let ts = g.transit_pattern_trips_len();
    g.push_transit_pattern_trip(TripId(0));
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
    let sts = g.transit_pattern_stop_times_len();
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600, ..Default::default() });
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600, ..Default::default() });
    g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1200, departure: 9 * 3600 + 1200, ..Default::default() });
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });
    g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });

    g.build_raptor_index();
    enable_contraction(&mut g);

    let coords = [(50.000, 4.001), (50.100, 4.001), (50.300, 4.001)];
    let rail: Vec<(f64, f64)> = coords.to_vec();
    let d01 = LatLng { latitude: coords[0].0, longitude: coords[0].1 }
        .dist(LatLng { latitude: coords[1].0, longitude: coords[1].1 });
    let d12 = LatLng { latitude: coords[1].0, longitude: coords[1].1 }
        .dist(LatLng { latitude: coords[2].0, longitude: coords[2].1 });
    let adj = vec![
        vec![(1usize, d01 as u32)],
        vec![(0usize, d01 as u32), (2usize, d12 as u32)],
        vec![(1usize, d12 as u32)],
    ];
    g.store_railway_graph(rail, adj);
    g.set_fare_model(sncb_airport_fare_model());

    (g, osm_origin, osm_dest)
}

#[test]
fn sncb_airport_stop_is_tagged_by_name_token() {
    let (g, _o, _d) = sncb_airport_routable_graph();
    // Only the airport-named station (compact stop 2) is tagged; A and B are not.
    assert!(!g.raptor.sncb_airport_stop.is_empty(), "airport tags built when fares on");
    assert!(!g.raptor.sncb_airport_stop[0], "Gare A is not an airport");
    assert!(!g.raptor.sncb_airport_stop[1], "Gare B is not an airport");
    assert!(g.raptor.sncb_airport_stop[2], "Brussels Airport-Zaventem is tagged");
}

#[test]
fn sncb_airport_od_prices_fixed_7_90_end_to_end() {
    // An SNCB journey A→(airport) prices at the fixed 7.90 special-OD fare, NOT the
    // bracketed distance tariff (which would be ~6.80 for the ~33 km ride). This
    // proves the override is wired into routing, not just the fare engine.
    let (g, origin, dest) = sncb_airport_routable_graph();
    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 1)
        .expect("a single-SNCB-train plan A→airport");
    let price = priced.price.as_ref().expect("fares enabled ⇒ Plan.price populated");
    assert!(
        (price.known_euros - 7.90).abs() < 1e-9,
        "airport OD must be the fixed 7.90, got {}",
        price.known_euros
    );
    assert!(price.unknown_operators.is_empty(), "SNCB is modeled");
}

#[test]
fn sncb_airport_stops_cleared_when_fares_disabled() {
    let (mut g, _o, _d) = sncb_airport_routable_graph();
    g.set_fare_model(maas_rs::structures::cost::FareModel::default());
    assert!(
        g.raptor.sncb_airport_stop.is_empty(),
        "disabled fares clear the airport tags (zero work)"
    );
}

// ── Defect A: carried fare credit must survive dominance at a shared hub ───────

/// A combined STIB + SNCB fare model (STIB 2.10/90min, SNCB the bracketed 2026
/// 2nd-class distance tariff), bucketing disabled so exact cents are asserted.
fn stib_sncb_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
        enabled: true,
        known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
        operators: vec![
            OperatorFare {
                name: "STIB".into(),
                model: OperatorModel::TimeWindowFlat {
                    ticket_cents: 210,
                    card_cents: None,
                    validity_secs: 5400,
                    operator: maas_rs::structures::cost::TimeWindowOperator::Stib,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            },
            OperatorFare {
                name: "SNCB".into(),
                model: OperatorModel::DistanceBasePerKm {
                    tariff: sncb_test_tariff(),
                    rules: sncb_test_rules(),
                    airport_od_cents: 0,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            },
        ],
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

/// Two competing access paths to a shared hub H, then a shared SNCB continuation
/// H→D. Both access options reach H at the SAME time on the SAME round so they
/// collide in the H Pareto cell:
///   - SNCB access:  A →(rail 10 km)→ H   → known 250 + 110 = 360, sncb_active.
///   - STIB access:  P →(flat)→        H   → known 210, no SNCB credit.
/// The cheaper STIB label (210 < 360) would prune the SNCB label at H if carried
/// fare state were ignored (defect A). But the pruned SNCB label keeps a paid
/// SNCB base credit, so continuing H →(rail 10 km)→ D re-uses it (no second base):
///   - SNCB-contiguous plan finishes 250 + 110 + 110 = 470  (single base).
///   - STIB-then-SNCB plan finishes  210 + 250 + 110 = 570  (second base paid).
/// The engine must RETAIN the 470 plan. Returns `(graph, origin, dest)`.
fn shared_hub_two_access_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    // OSM access nodes. Origin sits between the SNCB station A and the STIB stop P
    // so both are reachable on foot; H and D each get an egress/transfer OSM node.
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_hub = g.add_node(osm_node("hub", 50.150, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));

    // SNCB stations along a meridian: A (board), H (hub), D (dest). ~1113 m / 0.01°.
    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_h = g.add_node(transit_stop("Gare H", 50.100, 4.001));
    let stop_d = g.add_node(transit_stop("Gare D", 50.300, 4.001));
    // STIB stop P (access) near the origin. Both the SNCB rail leg and the STIB
    // leg TERMINATE at the same physical hub stop `stop_h`, so the two access
    // labels land in the SAME Pareto cell and genuinely compete on dominance.
    let stop_p = g.add_node(transit_stop("Arret P", 50.000, 3.999));

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        g.add_edge(stop, EdgeData::Street(StreetEdgeData {
            origin: stop, destination: osm, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
        g.add_edge(osm, EdgeData::Street(StreetEdgeData {
            origin: osm, destination: stop, length: m, partial: true,
            foot: true, bike: false, car: false, attrs: BikeAttrs::road_default(),
            elev_delta: 0, surface_speed: 100, var_gen: VarGen::NONE,
        }));
    };
    // Origin can reach both the SNCB station A and the STIB stop P.
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_p, osm_origin, 72);
    // Hub: the shared SNCB/STIB stop H snaps to the hub OSM node.
    add_snap(&mut g, stop_h, osm_hub, 72);
    add_snap(&mut g, stop_d, osm_dest, 72);

    // Transit edges (needed for reconstruct's timetable lookup). Route ids:
    //   0 = SNCB A→H, 1 = SNCB H→D, 2 = STIB P→H.
    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_h, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 11_100,
    }));
    g.add_edge(stop_h, EdgeData::Transit(TransitEdgeData {
        origin: stop_h, destination: stop_d, route_id: RouteId(1),
        timetable_segment: TimetableSegment { start: 1, len: 1 }, length: 22_200,
    }));
    g.add_edge(stop_p, EdgeData::Transit(TransitEdgeData {
        origin: stop_p, destination: stop_h, route_id: RouteId(2),
        timetable_segment: TimetableSegment { start: 2, len: 1 }, length: 11_100,
    }));

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_agencies(vec![
        AgencyInfo { name: "SNCB".into(), url: String::new(), timezone: String::new() },
        AgencyInfo { name: "STIB".into(), url: String::new(), timezone: String::new() },
    ]);
    g.add_transit_routes(vec![
        RouteInfo { route_short_name: "IC1".into(), route_long_name: "A-H".into(),
            route_type: RouteType::Rail, agency_id: AgencyId(0),
            route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "IC2".into(), route_long_name: "H-D".into(),
            route_type: RouteType::Rail, agency_id: AgencyId(0),
            route_color: None, route_text_color: None },
        RouteInfo { route_short_name: "M".into(), route_long_name: "P-H".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(1),
            route_color: None, route_text_color: None },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(2), service_id: ServiceId(0), bikes_allowed: None },
    ]);
    // Timings: both accesses arrive H at 09:10; the H→D train departs 09:12.
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 600, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600 + 720, arrival: 9 * 3600 + 1320, service_id: ServiceId(0) },
        // STIB arrives H at 09:09 — strictly EARLIER than the SNCB access (09:10),
        // so the cheaper STIB label genuinely dominates the SNCB label on the
        // (arrival ↓, bucket ↑) axis and, absent the credit buy-back, would prune it.
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 540, service_id: ServiceId(0) },
    ]);

    // Pattern 0: SNCB A→H.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_h]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600, ..Default::default() });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    // Pattern 1: SNCB H→D.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_h, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 720, departure: 9 * 3600 + 720, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 1320, departure: 9 * 3600 + 1320, ..Default::default() });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 1 });
    }
    // Pattern 2: STIB P→H (terminates at the shared hub stop H).
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_p, stop_h]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 540, departure: 9 * 3600 + 540, ..Default::default() });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(2), num_trips: 1 });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    // Rail chain A-H-D coincident with the SNCB stops (railway == haversine).
    let coords = [(50.000, 4.001), (50.100, 4.001), (50.300, 4.001)];
    let rail: Vec<(f64, f64)> = coords.to_vec();
    let d_ah = LatLng { latitude: coords[0].0, longitude: coords[0].1 }
        .dist(LatLng { latitude: coords[1].0, longitude: coords[1].1 });
    let d_hd = LatLng { latitude: coords[1].0, longitude: coords[1].1 }
        .dist(LatLng { latitude: coords[2].0, longitude: coords[2].1 });
    let adj = vec![
        vec![(1usize, d_ah as u32)],
        vec![(0usize, d_ah as u32), (2usize, d_hd as u32)],
        vec![(1usize, d_hd as u32)],
    ];
    g.store_railway_graph(rail, adj);
    g.set_fare_model(stib_sncb_fare_model());

    (g, osm_origin, osm_dest)
}

#[test]
fn demoted_price_shared_hub_plan_set_is_price_blind() {
    // The DEMOTION property on the former "defect A" OD: price no longer influences
    // the search, so a strictly-earlier-arriving STIB label at the shared hub H
    // dominates the SNCB label on (arrival ↓, bucket ↑) exactly as it would with
    // fares off. The returned plan set (mode/start/end/leg-count) must therefore be
    // IDENTICAL with fares enabled vs disabled — the cheaper single-base SNCB plan is
    // no longer surfaced by an in-search price axis (that was the ~9% drop the data
    // justified). Any plan that IS returned is priced correctly post-hoc.
    let (mut g, origin, dest) = shared_hub_two_access_graph();
    let with = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 30 * 60);
    // Same graph, fares off: turn the master switch off (topology unchanged).
    g.raptor.fare_model.enabled = false;
    let without = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 30 * 60);

    let sig = |ps: &[maas_rs::structures::plan::Plan]| {
        let mut v: Vec<_> = ps.iter().map(|p| (p.mode, p.start, p.end, p.legs.len())).collect();
        v.sort();
        v
    };
    assert_eq!(
        sig(&with),
        sig(&without),
        "price is demoted: the search is price-blind, so the plan set is identical on/off"
    );
    // Fares-off carries no price; fares-on annotates every returned plan post-hoc.
    assert!(without.iter().all(|p| p.price.is_none()), "fares off ⇒ no Plan.price");

    // Whatever two-transit plan surfaces (the earlier STIB-then-SNCB one) is priced
    // correctly post-hoc: STIB 2.10 + one SNCB ticket of the H→D distance.
    let h = LatLng { latitude: 50.100, longitude: 4.001 };
    let d = LatLng { latitude: 50.300, longitude: 4.001 };
    let hd_km = h.dist(d) / 1000.0;
    let expected = 2.10 + sncb_test_tariff().fare_cents(hd_km) as f64 / 100.0;
    let priced_two = with
        .iter()
        .filter(|p| transit_leg_count(p) == 2)
        .filter_map(|p| p.price.as_ref().map(|pr| pr.known_euros))
        .next();
    if let Some(price) = priced_two {
        assert!(
            (price - expected).abs() < 0.05,
            "the returned two-transit plan is priced correctly post-hoc: got {price}, expected {expected}"
        );
    }
}

// ── Unrestricted (MCR) inter-stop transfers ───────────────────────────────────

/// A graph whose only good itinerary requires a >1 km FOOT transfer between two
/// stops that the capped table (`MAX_TRANSFER_DISTANCE_M` = 1000 m) cannot link.
///
/// Three foot-ISOLATED islands, bridged only by transit, so the >1 km hop can only ever
/// be a MID-JOURNEY transfer — never an access or egress walk (which Stage-1
/// completeness would otherwise widen to reach any nearby stop, and which would let a
/// bus+long-egress shortcut dominate the two-leg plan):
///   Island A (origin):  osm_o(4.000) — stop_A(4.001)↦osm_o(50m)
///   Island B (mid):      osm_b(4.100) ─1434m─ osm_c(4.120);  stop_B↦osm_b(50m), stop_C↦osm_c(50m)
///   Island C (dest):     osm_d(4.140) ─50m─ osm_dest(4.1407); stop_D↦osm_d(50m)
///
///   Bus  (route 0): stop_A → stop_B   dep 09:10 arr 09:15  (bridges A→B)
///   Tram (route 1): stop_C → stop_D   dep 09:53 arr 10:03  (bridges B→C island)
///
/// stop_B ↔ stop_C is ~1434 m straight-line and ~1534 m over the street network —
/// OUTSIDE the 1000 m cap on both the KD-tree neighbour radius and the walk-Dijkstra
/// budget, so the precomputed table has no B↔C entry. The tram (hence the destination)
/// is reachable ONLY by walking B→C, which requires the live MCR transfer search.
fn long_walk_transfer_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.100));
    let osm_c = g.add_node(osm_node("c", 50.000, 4.120));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.140));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.1407));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.100));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.120));
    let stop_d = g.add_node(transit_stop("Stop D", 50.000, 4.140));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, street_edge(a, b, m));
        g.add_edge(b, street_edge(b, a, m));
    };
    // Only the mid island's B→C span and the dest island's short link are walkable;
    // the three islands are otherwise foot-disconnected (transit is the only bridge).
    add_street(&mut g, osm_b, osm_c, 1434); // the >1 km B→C span
    add_street(&mut g, osm_d, osm_dest, 50);

    // Stop→OSM snap edges (partial, foot-only), mirroring GTFS ingestion.
    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        let mk = |o: NodeID, d: NodeID| {
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
        g.add_edge(stop, mk(stop, osm));
        g.add_edge(osm, mk(osm, stop));
    };
    add_snap(&mut g, stop_a, osm_o, 50);
    add_snap(&mut g, stop_b, osm_b, 50);
    add_snap(&mut g, stop_c, osm_c, 50);
    add_snap(&mut g, stop_d, osm_d, 50);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 1434,
        }),
    );
    g.add_edge(
        stop_c,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_c,
            destination: stop_d,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 1434,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
    ]);

    // Bus dep 09:10 arr 09:15; tram dep 09:53 arr 10:03. After the bus reaches
    // stop_B at 33300 the ~1534 m walk (~1278 s) lands at stop_C ~34578 < 35580 dep.
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 600,
            arrival: 9 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 3180,
            arrival: 9 * 3600 + 3780,
            service_id: ServiceId(0),
        },
    ]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 3180,
            departure: 9 * 3600 + 3180,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 3780,
            departure: 9 * 3600 + 3780,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_o, osm_dest)
}

/// The capped transfer table drops any B↔C link beyond 1 km, so a journey that must
/// walk >1 km between two stops is invisible with the flag OFF but discovered by the
/// live MCR multi-source Dijkstra with it ON. Proves the feature does something:
/// ON yields a bus+walk+tram plan whose middle transfer leg is >1000 m; OFF yields no
/// such two-transit plan.
#[test]
fn unrestricted_transfers_find_long_inter_stop_walk() {
    let (mut g, origin, dest) = long_walk_transfer_graph();
    let start = 9 * 3600; // 09:00

    // Flag OFF (default): the capped table cannot link stop_B → stop_C.
    let plans_off = g.raptor(origin, dest, start, 0, 0x7F, 10 * 60);
    assert!(
        plans_off.iter().all(|p| transit_leg_count(p) < 2),
        "with capped transfers no two-transit (bus+tram) plan should exist; got legs {:?}",
        plans_off.iter().map(leg_kinds).collect::<Vec<_>>()
    );

    // Flag ON: MCR resolves the >1 km B→C foot transfer live.
    g.set_unrestricted_transfers(true);
    let plans_on = g.raptor(origin, dest, start, 0, 0x7F, 10 * 60);

    let long_transfer_plan = plans_on.iter().find(|p| {
        transit_leg_count(p) == 2
            && p.legs.iter().any(|l| match l {
                PlanLeg::Walk(w) => w.length > 1000,
                _ => false,
            })
    });
    assert!(
        long_transfer_plan.is_some(),
        "MCR should surface a bus+walk+tram plan with a >1000 m transfer leg; got {:?}",
        plans_on.iter().map(leg_kinds).collect::<Vec<_>>()
    );

    // And that transfer walk really is the >1 km hop between the two transit legs.
    let plan = long_transfer_plan.unwrap();
    let transfer = plan
        .legs
        .iter()
        .filter_map(|l| match l {
            PlanLeg::Walk(w) => Some(w),
            _ => None,
        })
        .max_by_key(|w| w.length)
        .expect("a walk leg");
    assert!(
        transfer.length > 1000,
        "longest walk leg should be the >1 km transfer; got {} m",
        transfer.length
    );
    // Reconstruction must materialize the real street polyline for the transfer leg
    // (from `trace.from_stop`, via `street_path_geom`) — not just a duration×speed
    // length. A non-empty geometry proves the leg is drawable end-to-end.
    assert!(
        !transfer.geometry.is_empty(),
        "the >1 km transfer leg must carry a reconstructed street geometry"
    );
    // The ON plan must also reach the destination strictly no later than any OFF plan.
    let best_off = plans_off.iter().map(|p| p.end).min();
    if let Some(best_off) = best_off {
        assert!(
            plan.end <= best_off,
            "the transit plan (end {}) should not be slower than the best flag-off plan (end {})",
            plan.end,
            best_off
        );
    }
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
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: m,
                partial: false,
                foot,
                bike,
                car: true,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: m,
                partial: false,
                foot,
                bike,
                car: true,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    both(&mut g, osm_o, osm_q, 9967, true, true);
    both(&mut g, osm_q, osm_d, 10182, true, true);
    both(&mut g, stop_p, osm_o, 72, true, false);
    both(&mut g, stop_q, osm_q, 72, true, false);
    both(&mut g, stop_r, osm_q, 215, true, false);
    both(&mut g, stop_s, osm_d, 72, true, false);

    // Junction-breaking stubs: osm_o and osm_d each have exactly 2 unique
    // street-graph neighbours (one road junction + one transit stop), so the
    // contracted graph would mark them as interior pass-throughs.  Car and bike
    // contracted routing require junction origins, so give each endpoint a
    // one-node dead-end that raises its degree to 3, making it a junction.
    let osm_o_stub = g.add_node(osm_node("o_stub", 50.001, 4.000));
    let osm_d_stub = g.add_node(osm_node("d_stub", 50.001, 4.281));
    both(&mut g, osm_o, osm_o_stub, 1, true, true);
    both(&mut g, osm_d, osm_d_stub, 1, true, true);

    g.add_edge(
        stop_p,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_p,
            destination: stop_q,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 9967,
        }),
    );
    g.add_edge(
        stop_r,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_r,
            destination: stop_s,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 9895,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "X1".into(),
            route_long_name: "Express 1".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "X2".into(),
            route_long_name: "Express 2".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: leg1_bikes,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: leg2_bikes,
        },
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 480,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 900,
            arrival: 9 * 3600 + 1380,
            service_id: ServiceId(0),
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 480,
            departure: 9 * 3600 + 480,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
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
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1380,
            departure: 9 * 3600 + 1380,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    (g, osm_o, osm_d)
}

fn transit_leg_count(p: &maas_rs::structures::plan::Plan) -> usize {
    p.legs
        .iter()
        .filter(|l| matches!(l, PlanLeg::Transit(_)))
        .count()
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
    let modes = g.raptor_modes(
        origin,
        dest,
        8 * 3600,
        0,
        0x7F,
        10 * 60,
        &ActiveModes::default(),
    );

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
    assert_eq!(
        street_modes(transit_plan).first().copied(),
        Some(Mode::Bike)
    );
}

#[test]
fn car_dijkstra_drives_car_edges_and_walks_foot_connectors() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    let c = g.add_node(osm_node("c", 50.001, 4.000));
    // a→b is a road (driven at car speed). a→c is a foot-only stop connector,
    // crossed at walking speed (park & walk the last bit).
    g.add_edge(
        a,
        EdgeData::Street(StreetEdgeData {
            origin: a,
            destination: b,
            length: 1100,
            partial: false,
            foot: true,
            bike: false,
            car: true,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }),
    );
    g.add_edge(
        a,
        EdgeData::Street(StreetEdgeData {
            origin: a,
            destination: c,
            length: 120,
            partial: false,
            foot: true,
            bike: true,
            car: false,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }),
    );
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Car);
    assert_eq!(dist[&b], 100, "1100 m at 11.0 m/s = 100 s by car");
    assert_eq!(
        dist[&c], 100,
        "120 m foot-only connector at 1.2 m/s = 100 s"
    );
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
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: m,
                partial: false,
                foot: true,
                bike: true,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    road(&mut g, osm_o, osm_d, 570);
    connector(&mut g, osm_o, stop_a);
    connector(&mut g, osm_d, stop_b);

    // A real (but useless here) transit route, so the mode has something to scan.
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_far,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 5000,
        }),
    );
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Express".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None,
        route_id: RouteId(0),
        service_id: ServiceId(0),
        bikes_allowed: Some(true),
    }]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: 9 * 3600 + 600,
        arrival: 9 * 3600 + 900,
        service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_far]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    g.build_raptor_index();
    enable_contraction(&mut g);

    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    // A range query: the degenerate 0-transit path departs later than the direct
    // bike, so it survives Pareto on the departure axis (single-departure
    // dominance would otherwise hide the bug).
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
    let plans = g.raptor_range_tuned_rt_modes(
        osm_o,
        osm_d,
        9 * 3600,
        1800,
        0,
        0x7F,
        10 * 60,
        &buckets,
        300,
        &RealtimeIndex::new(),
        &am,
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
                p.mode,
                transit_leg_count(p)
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    road(&mut g, osm_o, osm_b, 1100); // short drive to the boarding area
    road(&mut g, osm_b, osm_d, 9900); // road continues all the way to the dest
    connector(&mut g, osm_b, stop_board);
    connector(&mut g, osm_d, stop_dest);

    g.add_edge(
        stop_board,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_board,
            destination: stop_dest,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 9900,
        }),
    );
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Express".into(),
        route_type: RouteType::Bus,
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
        departure: 9 * 3600 + 600,
        arrival: 9 * 3600 + 1800,
        service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_board, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    g.build_raptor_index();
    enable_contraction(&mut g);

    let am = ActiveModes::new(&[Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 10 * 60, &am);
    assert!(
        plans
            .iter()
            .any(|p| p.mode == Mode::CarDropOff && transit_leg_count(p) >= 1),
        "park&ride must survive even though the car can reach a near-destination stop; got {:?}",
        plans
            .iter()
            .map(|p| (p.mode, transit_leg_count(p)))
            .collect::<Vec<_>>()
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    road(&mut g, osm_o, osm_p, 6400);
    road(&mut g, osm_p, osm_d, 6450);
    connector(&mut g, osm_p, stop_p); // foot-only, as gtfs builds it
    connector(&mut g, osm_d, stop_q);

    g.add_edge(
        stop_p,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_p,
            destination: stop_q,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 6400,
        }),
    );
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Express".into(),
        route_type: RouteType::Bus,
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
        departure: 9 * 3600 + 1000,
        arrival: 9 * 3600 + 1300,
        service_id: ServiceId(0),
    }]);
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_p, stop_q]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1000,
            departure: 9 * 3600 + 1000,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1300,
            departure: 9 * 3600 + 1300,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    g.build_raptor_index();
    enable_contraction(&mut g);

    let am = ActiveModes::new(&[Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 10 * 60, &am);
    let pr = plans.iter().find(|p| transit_leg_count(p) >= 1);
    assert!(
        pr.is_some(),
        "park & ride must work with foot-only stop connectors; got {:?}",
        plans
            .iter()
            .map(|p| (p.mode, p.legs.len()))
            .collect::<Vec<_>>()
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: 12,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    road(&mut g, osm_o, osm_near, 140); // short walk/drive to the local stop
    road(&mut g, osm_near, osm_far, 2010); // car continues to the far hub (foot too slow)
    connector(&mut g, osm_near, stop_near);
    connector(&mut g, osm_far, stop_far);
    connector(&mut g, osm_d, stop_dest);

    // Transit edges (one per boarded segment), mirroring the gtfs ingestion shape.
    g.add_edge(
        stop_near,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_near,
            destination: stop_mid,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 4000,
        }),
    );
    g.add_edge(
        stop_mid,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_mid,
            destination: stop_dest,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 3000,
        }),
    );
    g.add_edge(
        stop_far,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_far,
            destination: stop_dest,
            route_id: RouteId(2),
            timetable_segment: TimetableSegment { start: 2, len: 1 },
            length: 7000,
        }),
    );
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "P1".into(),
            route_long_name: "Local 1".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "P2".into(),
            route_long_name: "Local 2".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "Q".into(),
            route_long_name: "Express".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(2),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 600,
            arrival: 9 * 3600 + 1500,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1800,
            arrival: 9 * 3600 + 3000,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 300,
            arrival: 9 * 3600 + 1800,
            service_id: ServiceId(0),
        },
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1500,
            departure: 9 * 3600 + 1500,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });

        // Pattern 1: P2  stop_mid → stop_dest   (dep 9:30, arr 9:50)
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_mid, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 3000,
            departure: 9 * 3600 + 3000,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });

        // Pattern 2: Q  stop_far → stop_dest    (dep 9:05, arr 9:30) — fast car line
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_far, stop_dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 300,
            departure: 9 * 3600 + 300,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(2),
            num_trips: 1,
        });
    }
    g.build_raptor_index();
    enable_contraction(&mut g);

    let am = ActiveModes::new(&[Mode::WalkTransit, Mode::CarDropOff]);
    let plans = g.raptor_modes(osm_o, osm_d, 9 * 3600, 0, 0x7F, 300, &am);

    let summary: Vec<_> = plans
        .iter()
        .map(|p| (p.mode, transit_leg_count(p)))
        .collect();
    assert!(
        plans
            .iter()
            .any(|p| p.mode == Mode::WalkTransit && transit_leg_count(p) == 2),
        "walk+transit must survive even when a faster park&ride sets the cutoff; got {summary:?}"
    );
    assert!(
        plans
            .iter()
            .any(|p| p.mode == Mode::CarDropOff && transit_leg_count(p) >= 1),
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
        g.add_edge(
            x,
            EdgeData::Street(StreetEdgeData {
                origin: x,
                destination: y,
                length: 110,
                partial: false,
                foot,
                bike: false,
                car,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    edge(&mut g, a, b, true, true); // road
    edge(&mut g, b, c, true, false); // foot-only connector (park & walk)
    edge(&mut g, c, d, false, true); // car-only road
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Car);
    assert!(dist.contains_key(&b), "b reachable by car");
    assert!(dist.contains_key(&c), "c reachable by parking and walking");
    assert!(
        !dist.contains_key(&d),
        "a parked car cannot be resumed to drive c→d"
    );
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
    g.add_edge(
        o,
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: p,
            length: 1100,
            partial: false,
            foot: true,
            bike: false,
            car: true,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }),
    );
    g.add_edge(
        p,
        EdgeData::Street(StreetEdgeData {
            origin: p,
            destination: stop,
            length: 12,
            partial: true,
            foot: true,
            bike: false,
            car: false,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }),
    );
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
    g.add_edge(
        a,
        EdgeData::Street(StreetEdgeData {
            origin: a,
            destination: b,
            length: 100,
            partial: false,
            foot: false,
            bike: false,
            car: true,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }),
    );
    g.build_raptor_index();

    let dist = g.street_dijkstra(a, 99999, StreetProfile::Foot);
    assert!(
        !dist.contains_key(&b),
        "pedestrians must not use car-only roads"
    );
}

#[test]
fn car_direct_drives_the_whole_way() {
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::Car]);
    let plans = g.raptor_modes(origin, dest, 9 * 3600, 0, 0x7F, 10 * 60, &am);

    assert_eq!(
        plans.len(),
        1,
        "CAR alone should yield exactly the direct drive"
    );
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
    assert_eq!(
        sm.first().copied(),
        Some(Mode::Car),
        "access must be driven ({sm:?})"
    );
    assert_eq!(
        sm.last().copied(),
        Some(Mode::Walk),
        "egress must be walked ({sm:?})"
    );
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
    assert_eq!(
        sm.first().copied(),
        Some(Mode::Walk),
        "access must be walked ({sm:?})"
    );
    assert_eq!(
        sm.last().copied(),
        Some(Mode::Car),
        "egress must be driven ({sm:?})"
    );
}

#[test]
fn bike_pickup_is_walk_first_bike_last() {
    // Bike mirror of kiss & ride: walk to the first station, ride transit, then a
    // bike waiting at the destination station carries the final leg. `BikePickup`
    // must be the exact mirror of `CarPickup` (walk access, vehicle egress).
    let (g, origin, dest) = express_two_leg_graph(None, None);
    let am = ActiveModes::new(&[Mode::BikePickup]);
    let plans = g.raptor_modes(origin, dest, 8 * 3600 + 3300, 0, 0x7F, 10 * 60, &am);

    let pr = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("bike pickup plan expected");
    assert_eq!(pr.mode, Mode::BikePickup);
    let sm = street_modes(pr);
    assert_eq!(
        sm.first().copied(),
        Some(Mode::Walk),
        "access must be walked ({sm:?})"
    );
    assert_eq!(
        sm.last().copied(),
        Some(Mode::Bike),
        "egress must be ridden — real bike egress, not a walk fallback ({sm:?})"
    );
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
        "First transit leg (Bus) should have no transfer risk — boarded from walk"
    );

    assert!(
        transit[1].transfer_risk.is_some(),
        "Second transit leg (Tram) should have transfer risk — boarded after Bus transfer"
    );

    // The first leg now records its downstream connection so its alternatives can
    // be scored for the outbound transfer onto the Tram.
    assert!(
        transit[0].following_route_type.is_some(),
        "First transit leg should know the following leg's route type"
    );
    assert!(
        transit[0].following_margin_secs.is_some(),
        "First transit leg should record its outbound connection margin"
    );
    assert!(
        transit[1].following_route_type.is_none(),
        "Last transit leg has no following connection"
    );
}

#[test]
fn raptor_transit_leg_carries_scheduled_step_times_and_endpoint_fills() {
    use maas_rs::structures::plan::PlanLegStep;

    let (g, origin, dest) = two_route_raptor_graph();
    let plans = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "expected at least one plan");

    let mut checked_transit = 0usize;
    for p in &plans {
        for leg in &p.legs {
            let PlanLeg::Transit(t) = leg else { continue };
            checked_transit += 1;

            // B2: both endpoint dwell fields are now populated (were None).
            assert!(t.from.arrival.is_some(), "from.arrival must be filled");
            assert!(t.to.departure.is_some(), "to.departure must be filled");

            for step in &t.steps {
                let PlanLegStep::Transit(s) = step else { continue };
                // B1: every step carries its scheduled arrival.
                assert!(
                    s.scheduled_arrival.is_some(),
                    "each transit step must carry scheduled_arrival"
                );
                // Without realtime, scheduled == effective (place) exactly.
                assert_eq!(
                    s.scheduled_arrival, s.place.arrival,
                    "scheduled_arrival must mirror place.arrival with no realtime"
                );
                assert_eq!(
                    s.scheduled_departure, s.place.departure,
                    "scheduled_departure must mirror place.departure with no realtime"
                );
            }
        }
    }
    assert!(checked_transit > 0, "expected at least one transit leg to check");
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
    let bus = DelayCDF {
        bins: vec![(0, 0.1), (300, 0.4), (600, 0.6), (900, 0.8), (1200, 1.0)],
    };
    let tram = DelayCDF {
        bins: vec![(-600, 0.5), (0, 1.0)],
    };
    let mut models = HashMap::new();
    models.insert(RouteType::Bus, bus.clone());
    models.insert(RouteType::Tramway, tram.clone());
    g.set_transit_delay_models(models);

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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

    // Bus edge: timetable_segment has len=2 (two bus trips)
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 2 },
            length: 1362,
        }),
    );
    // Tram edge: timetable_segment has len=1
    g.add_edge(
        stop_c,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_c,
            destination: stop_d,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 2, len: 1 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);

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

    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(0) = bus 08:00
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(1) = bus 09:00
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(2) = tram
    ]);

    // Timetable departures (absolute indices used by transit edges)
    // idx 0: bus trip 0 dep 08:00
    // idx 1: bus trip 1 dep 09:00
    // idx 2: tram dep 09:30
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600,
            arrival: 8 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1800,
            arrival: 9 * 3600 + 2700,
            service_id: ServiceId(0),
        },
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600,
            departure: 8 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
        ..Default::default()
        });
        // stop_B col (2 entries): trip0 arr 08:15, trip1 arr 09:15
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600 + 900,
            departure: 8 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 2,
        });
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

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
    let two_leg_plan = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("Expected a Bus+Tram plan");

    let bus_leg = two_leg_plan
        .legs
        .iter()
        .find_map(|l| {
            if let PlanLeg::Transit(t) = l {
                Some(t)
            } else {
                None
            }
        })
        .unwrap();

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
    let delayed = g.raptor_tuned_rt(
        origin,
        dest,
        7 * 3600,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        &tram_delay,
    );
    let delayed_end = delayed.iter().map(|p| p.end).min().unwrap();
    assert_eq!(
        delayed_end,
        base_end + d as u32,
        "delaying the tram (the decisive last leg) must push arrival by {d}s \
         (base {base_end}, delayed {delayed_end})"
    );

    // Delaying a trip that is NOT on the chosen path leaves the arrival unchanged.
    let unrelated = RealtimeIndex::from_delays(1, [((TripId(0), 0u32), 600)]);
    let same = g.raptor_tuned_rt(
        origin,
        dest,
        7 * 3600,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        &unrelated,
    );
    assert_eq!(same.iter().map(|p| p.end).min().unwrap(), base_end);
}

/// Realtime SKIPPED stop (bug 6 routing consumption): the tram (TripId 2) is the
/// only transit that reaches the destination (it alights at stop_D = compact 3).
/// Marking the tram as SKIPPING stop_D means the router may not alight it there,
/// so no transit plan survives — only a walk-only plan remains. The schedule-only
/// baseline (empty, inert index) does produce the transit plan, proving the skip
/// guard is what removed it.
#[test]
fn raptor_skipped_stop_is_not_used_for_alighting() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
    fn has_transit(plans: &[maas_rs::structures::plan::Plan]) -> bool {
        plans
            .iter()
            .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
    }

    // Baseline via the SAME entry point with an empty (inert) index, isolating the
    // skip as the only difference. It reaches the destination by tram.
    let empty = RealtimeIndex::new();
    let base = g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &empty);
    assert!(
        has_transit(&base),
        "baseline must reach the destination by transit (the tram)"
    );

    // SKIP the tram at its alighting stop: the trip no longer serves stop_D, so the
    // router cannot alight there and no transit plan can reach the destination.
    let mut skip = std::collections::HashSet::new();
    skip.insert((TripId(2), 3u32));
    let rt = RealtimeIndex::new().with_skipped(skip);
    let skipped = g.raptor_tuned_rt(
        origin,
        dest,
        7 * 3600,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        &rt,
    );
    assert!(
        !has_transit(&skipped),
        "skipping the tram's alighting stop must leave no transit plan (the tram was \
         the only transit reaching the destination)"
    );
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
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("a bus+tram plan");

    let mut saw_tram = false;
    let mut saw_bus = false;
    for leg in &plan.legs {
        if let PlanLeg::Transit(t) = leg {
            if t.trip_id == TripId(2) {
                saw_tram = true;
                assert!(t.realtime, "tram leg should be flagged realtime");
                assert_eq!(
                    t.scheduled_end,
                    9 * 3600 + 2700,
                    "tram scheduled arrival kept"
                );
                assert_eq!(
                    t.end,
                    t.scheduled_end + 600,
                    "tram effective arrival = scheduled + 600"
                );
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
    g.raptor.transit_stop_ids = vec![
        "0470701".into(),
        "0470101".into(),
        "1234".into(),
        "0470".into(),
    ];
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
    let same = g.raptor_tuned_rt(
        origin,
        dest,
        7 * 3600,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        &empty,
    );
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

    let two_leg_plan = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("Expected a Bus+Tram plan");

    let transit_legs: Vec<_> = two_leg_plan
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

    assert_eq!(transit_legs.len(), 2);
    // Bus arrives at 09:15, tram departs at 09:30 — connection is valid
    assert!(
        transit_legs[0].end <= transit_legs[1].start,
        "Bus end ({}) must be ≤ tram start ({})",
        transit_legs[0].end,
        transit_legs[1].start
    );
    assert_eq!(
        transit_legs[1].start,
        9 * 3600 + 1800,
        "Tram should still depart at 09:30"
    );
}

// ── S1 chain-sweep tightening: over-credit safety ─────────────────────────────

/// Bus (A→B, three trips) + tram (C→D, one trip) with a short B→C transfer, where
/// the third bus trip arrives too late to make the tram. The forward pass boards
/// the first bus; tightening should shift it to the LATEST bus that still connects
/// (09:00), never the connection-breaking 09:20 bus.
fn over_tighten_break_graph() -> (Graph, NodeID, NodeID) {
    over_tighten_break_graph_perm(true)
}

/// Like `over_tighten_break_graph`, but the 09:00 bus (T1 — the latest bus that
/// still connects, hence the trip tightening would swap leg-0 onto) can be made
/// un-boardable at stop A (`t1_board = false`, GTFS pickup_type == 1). That is the
/// Bug #5 trap: the schedule-only oracle would re-time leg-0 onto this un-boardable
/// trip; the permission-aware oracle must instead keep the boardable 08:00 bus.
fn over_tighten_break_graph_perm(t1_board: bool) -> (Graph, NodeID, NodeID) {
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

    let street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: true,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    street(&mut g, osm_origin, osm_ab, 718);
    street(&mut g, osm_ab, osm_b, 645);
    street(&mut g, osm_b, osm_cd, 789);
    street(&mut g, osm_cd, osm_dest, 789);

    let snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    snap(&mut g, stop_a, osm_origin, 72);
    snap(&mut g, stop_b, osm_b, 72);
    snap(&mut g, stop_c, osm_b, 215);
    snap(&mut g, stop_d, osm_dest, 72);

    // Bus: 3 trips (timetable segment len=3).
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 3 },
            length: 1362,
        }),
    );
    // Tram: 1 trip.
    g.add_edge(
        stop_c,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_c,
            destination: stop_d,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 3, len: 1 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 0: bus 08:00
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 1: bus 09:00 (latest that still connects)
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 2: bus 09:20 (arrives 09:35 — too late for the 09:30 tram)
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 3: tram 09:30
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600,
            arrival: 8 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1200,
            arrival: 9 * 3600 + 2100,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(3),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1800,
            arrival: 9 * 3600 + 2700,
            service_id: ServiceId(0),
        },
    ]);

    // Pattern 0: Bus [A,B], 3 trips (column-major).
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
        // stop_A column (departures). The middle trip (T1, 09:00) is the swap
        // candidate; suppress its boarding permission when `t1_board` is false.
        for (idx, dep) in [8 * 3600, 9 * 3600, 9 * 3600 + 1200].into_iter().enumerate() {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: dep,
                departure: dep,
                board_allowed: idx != 1 || t1_board,
                ..Default::default()
            });
        }
        // stop_B column (arrivals)
        for arr in [8 * 3600 + 900, 9 * 3600 + 900, 9 * 3600 + 2100] {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: arr,
                departure: arr,
                ..Default::default()
            });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 3,
        });
    }
    // Pattern 1: Tram [C,D], 1 trip.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(3));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

/// Smallest transfer margin (seconds) between consecutive transit legs of a plan:
/// `next.start - (prev.end + intervening walk)`. Negative ⇒ time-inconsistent.
fn min_transfer_margin(legs: &[PlanLeg]) -> i32 {
    let mut worst = i32::MAX;
    let mut prev_end: Option<u32> = None;
    let mut walk = 0u32;
    for l in legs {
        match l {
            PlanLeg::Transit(t) => {
                if let Some(e) = prev_end {
                    worst = worst.min(t.start as i32 - (e + walk) as i32);
                }
                prev_end = Some(t.end);
                walk = 0;
            }
            PlanLeg::Walk(w) => {
                if prev_end.is_some() {
                    walk += w.duration;
                }
            }
        }
    }
    worst
}

/// S1 correctness gate: an over-generous alighting bound (the shape the legacy
/// backward pass can produce when its network-wide view over-credits a leg via a
/// parallel line it does not actually ride) re-times the first bus onto the 09:20
/// trip that CANNOT make the tram — a negative transfer margin. `chain_bounds`,
/// derived from the plan's own fixed legs, produces a bound that keeps the plan
/// time-consistent, and the debug-build assertion catches the bad bound.
#[test]
fn chain_bounds_reject_over_credit_that_lambda_would_accept() {
    let (g, origin, dest) = over_tighten_break_graph();
    let date = 0;
    let weekday = 0x7F;

    let plans = g.raptor(origin, dest, 7 * 3600, date, weekday, 10 * 60);
    let plan = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("a bus+tram plan");
    let legs = plan.legs.clone();

    // Baseline: the delivered (chain-tightened) plan is time-consistent.
    assert!(
        min_transfer_margin(&legs) >= 0,
        "baseline plan must be consistent"
    );

    // Target = arrival at the last transit leg's alighting stop.
    let (target_stop, target) = plan
        .legs
        .iter()
        .rev()
        .find_map(|l| match l {
            PlanLeg::Transit(t) => {
                Some((g.compact_stop_of_node(t.to.node_id).unwrap(), t.end))
            }
            _ => None,
        })
        .unwrap();

    let chain = g.chain_bounds_pub(&legs, target_stop, target, date, weekday, &RealtimeIndex::new());
    assert_eq!(chain.len(), 2, "two transit legs");

    // On this single-line graph the chain reproduces the backward pass exactly.
    let lambda =
        g.bounds_from_lambda_pub(&legs, target_stop, target, 2, date, weekday, &RealtimeIndex::new());
    assert_eq!(
        chain, lambda,
        "chain must reproduce the backward pass on a single-line plan"
    );

    // Simulate the legacy over-credit: inflate leg-0's bound past the connection
    // (to the 09:20 bus arrival, 09:35). Tighten the LAMBDA-style bound → the
    // first leg jumps to the 09:20 bus and the tram is missed (negative margin).
    let mut over = chain.clone();
    over[0] = 9 * 3600 + 2100; // 09:35 — the connection-breaking bus arrival
    let mut broken = legs.clone();
    g.tighten_with_bounds_pub(&mut broken, &over, date, weekday, &RealtimeIndex::new(), false, false);
    assert!(
        min_transfer_margin(&broken) < 0,
        "an over-credited bound must break the tram connection (got margin {})",
        min_transfer_margin(&broken)
    );

    // The chain bound keeps the plan consistent (debug_check on ⇒ also exercises
    // the negative-margin debug assertion, which must NOT fire).
    let mut kept = legs.clone();
    g.tighten_with_bounds_pub(&mut kept, &chain, date, weekday, &RealtimeIndex::new(), false, true);
    assert!(
        min_transfer_margin(&kept) >= 0,
        "chain_bounds must keep the plan consistent (got margin {})",
        min_transfer_margin(&kept)
    );

    // The debug assertion actively rejects the bad bound in debug builds.
    #[cfg(debug_assertions)]
    {
        let g2 = &g;
        let legs2 = legs.clone();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut b = legs2;
            g2.tighten_with_bounds_pub(&mut b, &over, date, weekday, &RealtimeIndex::new(), false, true);
        }));
        assert!(
            caught.is_err(),
            "debug_check must panic on an over-credited (negative-margin) bound"
        );
    }
}

/// Bug #5 S1 invariant (end-to-end): when the latest-connecting bus (09:00, T1)
/// forbids boarding at stop A, the live chain-sweep tightening path (which runs
/// `tighten_with_bounds` with `debug_check = true`) must NOT re-time leg-0 onto
/// that un-boardable trip. The delivered bus+tram plan must keep the boardable
/// 08:00 bus, stay time-consistent (margin ≥ 0), and never fire the S1 debug
/// assertion. Pre-fix the schedule-only oracle selects the un-boardable 09:00 bus
/// (start re-timed to 09:00 → an itinerary the passenger cannot board).
#[test]
fn tightening_never_retimes_onto_unboardable_trip() {
    let (g, origin, dest) = over_tighten_break_graph_perm(false); // T1 (09:00) un-boardable at A
    let date = 0;
    let weekday = 0x7F;

    // Live path: default (chain) tighten mode, debug_check = true. A pre-fix run
    // would either emit the un-boardable plan or (with a downstream break) trip
    // the S1 assert; post-fix it must return a valid, consistent plan.
    let plans = g.raptor(origin, dest, 7 * 3600, date, weekday, 10 * 60);
    let plan = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("a bus+tram plan");
    let legs = plan.legs.clone();

    assert!(
        min_transfer_margin(&legs) >= 0,
        "delivered plan must stay time-consistent (got margin {})",
        min_transfer_margin(&legs)
    );

    let bus = legs
        .iter()
        .find_map(|l| match l {
            PlanLeg::Transit(t) => Some(t),
            _ => None,
        })
        .expect("a first transit (bus) leg");
    assert_eq!(
        bus.start,
        8 * 3600,
        "tightening must keep the boardable 08:00 bus, never re-time onto the \
         un-boardable 09:00 bus (start={})",
        bus.start
    );
}

/// The `tighten_long_transfers` flag: by default the chain sweep leaves an
/// off-table (> MAX_TRANSFER_DISTANCE_M) transfer untightened — bound 0, exactly
/// as lambda's capped reverse footpath does — so the two are byte-identical. With
/// the flag on, the chain tightens that leg using the plan's own reconstructed
/// walk (the opt-in accuracy improvement).
#[test]
fn tighten_long_transfers_flag_gates_off_table_bound() {
    let (mut g, origin, dest) = long_walk_transfer_graph();
    g.set_unrestricted_transfers(true); // surface the >1 km B→C transfer plan
    let date = 0;
    let weekday = 0x7F;

    let plans = g.raptor(origin, dest, 9 * 3600, date, weekday, 10 * 60);
    let plan = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
                && p.legs
                    .iter()
                    .any(|l| matches!(l, PlanLeg::Walk(w) if w.length > 1000))
        })
        .expect("a bus + >1km walk + tram plan");
    let legs = plan.legs.clone();

    let (target_stop, target) = plan
        .legs
        .iter()
        .rev()
        .find_map(|l| match l {
            PlanLeg::Transit(t) => Some((g.compact_stop_of_node(t.to.node_id).unwrap(), t.end)),
            _ => None,
        })
        .unwrap();

    // Default (flag off): chain no-ops the long transfer ⇒ identical to lambda.
    let chain_off =
        g.chain_bounds_pub(&legs, target_stop, target, date, weekday, &RealtimeIndex::new());
    let lambda =
        g.bounds_from_lambda_pub(&legs, target_stop, target, 2, date, weekday, &RealtimeIndex::new());
    assert_eq!(
        chain_off, lambda,
        "default chain must reproduce lambda on a long-transfer plan"
    );
    assert_eq!(
        chain_off[0], 0,
        "the feeder leg before a >1km transfer is left untightened by default"
    );

    // Flag on: the same leg now gets a real (non-zero) tightened bound.
    g.set_tighten_long_transfers(true);
    let chain_on =
        g.chain_bounds_pub(&legs, target_stop, target, date, weekday, &RealtimeIndex::new());
    assert!(
        chain_on[0] > 0,
        "flag on must tighten the long-transfer feeder leg (got {})",
        chain_on[0]
    );

    // Tightening with the flag-on bound stays time-consistent.
    let mut tightened = legs.clone();
    g.tighten_with_bounds_pub(&mut tightened, &chain_on, date, weekday, &RealtimeIndex::new(), false, true);
    assert!(
        min_transfer_margin(&tightened) >= 0,
        "flag-on long-transfer tightening must stay consistent"
    );
}

// ── Pattern shape storage ─────────────────────────────────────────────────────

#[test]
fn test_pattern_shape_stored_and_retrieved() {
    let mut g = Graph::new();
    let pts = vec![
        LatLng {
            latitude: 1.0,
            longitude: 1.0,
        },
        LatLng {
            latitude: 2.0,
            longitude: 2.0,
        },
        LatLng {
            latitude: 3.0,
            longitude: 3.0,
        },
        LatLng {
            latitude: 4.0,
            longitude: 4.0,
        },
        LatLng {
            latitude: 5.0,
            longitude: 5.0,
        },
    ];
    g.push_transit_pattern_shape(pts, vec![0u32, 4u32]);
    let (shape, idx) = g
        .get_pattern_shape(0)
        .expect("should have shape for pattern 0");
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
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // ~72 m from origin
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099)); // ~72 m from dest

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: true,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    // Connect origin ↔ dest via a long street (so walk-only is expensive)
    add_street(&mut g, osm_origin, osm_dest, 7200); // 7 200 m ≈ 1 h walk

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // 72 m / 1.2 m/s = 60 s walk
    add_snap(&mut g, stop_b, osm_dest, 72);

    // Transit edge (needed by reconstruct for timetable_segment lookup)
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 6 },
            length: 7000,
        }),
    );

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
        (0..6u32)
            .map(|_| TripInfo {
                trip_headsign: None,
                route_id: RouteId(0),
                service_id: ServiceId(0),
                bikes_allowed: None,
            })
            .collect(),
    );

    // TripSegments (one per trip, single A→B hop)
    let base = 9 * 3600u32; // 09:00
    g.add_transit_departures(
        (0..6u32)
            .map(|i| TripSegment {
                trip_id: TripId(i),
                origin_stop_sequence: 0,
                destination_stop_sequence: 1,
                departure: base + i * 1800,      // 09:00, 09:30, 10:00 …
                arrival: base + i * 1800 + 1800, // arrives 30 min later
                service_id: ServiceId(0),
            })
            .collect(),
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
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        // Stop B column (pos 1, 6 trips)
        for i in 0..6u32 {
            let t = base + i * 1800 + 1800;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        g.push_transit_idx_pattern_stop_times(Lookup {
            start: sts,
            len: 12,
        });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 6,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
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
        starts.len(),
        plans.len(),
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
        let tc_a = a
            .legs
            .iter()
            .filter(|l| matches!(l, PlanLeg::Transit(_)))
            .count()
            .saturating_sub(1);
        for (j, b) in plans.iter().enumerate() {
            if i == j {
                continue;
            }
            let tc_b = b
                .legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1);
            let a_dominates_b = tc_a <= tc_b
                && a.end <= b.end
                && a.start >= b.start
                && (tc_a < tc_b || a.end < b.end || a.start > b.start);
            assert!(
                !a_dominates_b,
                "Plan {} (start={}, end={}, tc={}) dominates plan {} (start={}, end={}, tc={}) — Pareto filter is broken",
                i, a.start, a.end, tc_a, j, b.start, b.end, tc_b,
            );
        }
    }
}

/// Reproduces the negative access-walk bug: a pattern's per-stop departure column is
/// NOT monotonic (real GTFS admits overtaking trips), which defeats the
/// `partition_point` boarding cutoff in `scan_route`.
///
/// Layout at stop A (access):
///   Pattern P (route 0), A-departure column [32100, 32200, 32300, 32400, 32500,
///     32600, 32700, 32000]. Trips 0..5 are active; trip 6 (32700) is INACTIVE;
///     trip 7 (32000) is active and overtaking — it departs early and arrives B
///     earliest (33500). The column is ascending up to index 6, then drops.
///   Pattern Q (route 1), one active trip departing A at 32700. It supplies the
///     "interesting departure" slot whose access seed reaches A at exactly 32700.
///
/// At the pass with `min_dep = 32700`, `partition_point(dep < 32700)` returns 6.
/// The boarding loop iterates 6..8: trip 6 is skipped (inactive), then trip 7
/// (departed 32000, long gone) is boarded — a trip the passenger cannot catch.
/// Trip 7 arrives B earliest, so it is reconstructed and tagged with a departure
/// (~32700 − access walk) LATER than its own first boarding (32000): a negative
/// access walk. The fix in `scan_route` (`trip_dep < min_dep` guard) rejects it.
fn overtaking_pattern_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // ~72 m from origin
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099)); // ~72 m from dest

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize, partial: bool| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial,
                    foot: true,
                    bike: false,
                    car: false,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_street(&mut g, osm_origin, osm_dest, 7200, false); // long walk-only path
    add_street(&mut g, stop_a, osm_origin, 72, true); // ~60 s access walk
    add_street(&mut g, stop_b, osm_dest, 72, true);

    // Pattern P (route 0) uses departures 0..8; pattern Q (route 1) uses departure 8.
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 8 },
            length: 7000,
        }),
    );
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 8, len: 1 },
            length: 7000,
        }),
    );

    // Service 0 = every day (active); service 1 = no days (always inactive).
    g.add_transit_services(vec![
        all_days_service(),
        ServicePattern {
            days_of_week: 0,
            start_date: 0,
            end_date: 9999,
            added_dates: vec![],
            removed_dates: vec![],
        },
    ]);
    let route = |n: &str| RouteInfo {
        route_short_name: n.into(),
        route_long_name: format!("Bus {n}"),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    };
    g.add_transit_routes(vec![route("P"), route("Q")]);

    // 9 trips. Trips 0..7 belong to pattern P (route 0); trip 6 is on the inactive
    // service. Trip 8 belongs to pattern Q (route 1).
    g.add_transit_trips(
        (0..9u32)
            .map(|i| TripInfo {
                trip_headsign: None,
                route_id: if i == 8 { RouteId(1) } else { RouteId(0) },
                service_id: if i == 6 { ServiceId(1) } else { ServiceId(0) },
                bikes_allowed: None,
            })
            .collect(),
    );

    // Column-major stop times. Pattern P: A-departures non-monotonic (trip 7
    // overtakes); trip 7 arrives B earliest. Pattern Q: a single A-departure at 32700.
    let p_dep_a = [32100u32, 32200, 32300, 32400, 32500, 32600, 32700, 32000];
    let p_arr_b = [34200u32, 34200, 34200, 34200, 34200, 34200, 34200, 33500];
    let q_dep_a = [32700u32];
    let q_arr_b = [34000u32];

    let mut deps: Vec<TripSegment> = Vec::new();
    for (i, (&d, &a)) in p_dep_a.iter().zip(p_arr_b.iter()).enumerate() {
        deps.push(TripSegment {
            trip_id: TripId(i as u32),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: d,
            arrival: a,
            service_id: if i == 6 { ServiceId(1) } else { ServiceId(0) },
        });
    }
    deps.push(TripSegment {
        trip_id: TripId(8),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: q_dep_a[0],
        arrival: q_arr_b[0],
        service_id: ServiceId(0),
    });
    g.add_transit_departures(deps);

    let mut push_pattern =
        |g: &mut Graph, route: RouteId, trip_ids: &[u32], dep_a: &[u32], arr_b: &[u32]| {
            let n = trip_ids.len();
            let ss = g.transit_pattern_stops_len();
            g.extend_transit_pattern_stops(&[stop_a, stop_b]);
            g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

            let ts = g.transit_pattern_trips_len();
            for &t in trip_ids {
                g.push_transit_pattern_trip(TripId(t));
            }
            g.push_transit_idx_pattern_trips(Lookup { start: ts, len: n });

            let sts = g.transit_pattern_stop_times_len();
            for &t in dep_a {
                g.push_transit_pattern_stop_time(StopTime {
                    arrival: t,
                    departure: t,
                ..Default::default()
        });
            }
            for &t in arr_b {
                g.push_transit_pattern_stop_time(StopTime {
                    arrival: t,
                    departure: t,
                ..Default::default()
        });
            }
            g.push_transit_idx_pattern_stop_times(Lookup {
                start: sts,
                len: 2 * n,
            });

            g.push_transit_pattern(PatternInfo {
                route,
                num_trips: n as u32,
            });
        };
    push_pattern(&mut g, RouteId(0), &[0, 1, 2, 3, 4, 5, 6, 7], &p_dep_a, &p_arr_b);
    push_pattern(&mut g, RouteId(1), &[8], &q_dep_a, &q_arr_b);

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

/// Regression: an overtaking (non-monotonic) departure column must never let a range
/// pass board a trip that has already left, which fabricated a plan tagged with a
/// departure LATER than its own first boarding — surfacing as a negative access-walk
/// duration. Every returned plan must satisfy: `plan.start <= first transit
/// departure`, a monotonic timeline (`leg[i].end <= leg[i+1].start`), and (after
/// street enrichment) a non-negative access-walk duration (`end >= start`).
#[test]
fn raptor_range_overtaking_no_infeasible_departure_tag() {
    let (g, origin, dest) = overtaking_pattern_graph();

    // Query 08:50, 60-min window so the interesting departures include the slot whose
    // access seed reaches A at 32700 (pattern Q), from which trip 7 (gone at 32000)
    // must be unreachable.
    let mut plans = g.raptor_range(origin, dest, 31800, 60 * 60, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "expected at least one plan");

    for (pi, p) in plans.iter().enumerate() {
        let first_transit_dep = p.legs.iter().find_map(|l| match l {
            PlanLeg::Transit(t) => Some(t.start),
            _ => None,
        });
        if let Some(board) = first_transit_dep {
            assert!(
                p.start <= board,
                "plan {pi}: tagged departure {} is LATER than its first boarding {board} \
                 (infeasible) — start={} legs={:?}",
                p.start,
                p.start,
                p.legs
                    .iter()
                    .map(|l| match l {
                        PlanLeg::Walk(w) => ("walk", w.start, w.end),
                        PlanLeg::Transit(t) => ("transit", t.start, t.end),
                    })
                    .collect::<Vec<_>>(),
            );
        }
        // Monotonic timeline: no leg may start before the previous one ends.
        let bounds: Vec<(u32, u32)> = p
            .legs
            .iter()
            .map(|l| match l {
                PlanLeg::Walk(w) => (w.start, w.end),
                PlanLeg::Transit(t) => (t.start, t.end),
            })
            .collect();
        for w in bounds.windows(2) {
            assert!(
                w[0].1 <= w[1].0,
                "plan {pi}: non-monotonic timeline — leg ends {} but next starts {} (bounds={:?})",
                w[0].1,
                w[1].0,
                bounds,
            );
        }
    }

    // After street enrichment (the real serving path), the access walk must have a
    // non-negative duration.
    let bike = BikeCost::new(BikeProfile::default());
    g.enrich_street_legs(&mut plans, origin, dest, &bike, false);
    for (pi, p) in plans.iter().enumerate() {
        if let Some(PlanLeg::Walk(w)) = p.legs.first() {
            assert!(
                w.end >= w.start,
                "plan {pi}: access walk has negative duration start={} end={}",
                w.start,
                w.end,
            );
        }
    }
}

/// Builds a single pattern A→B→C whose MID-stop (B) departure column is
/// non-monotonic because an express (trip 2) overtakes the stopping trips between
/// A and B — exactly what real ingestion produces (stop-0/A column stays sorted).
///
///   A-departures (seq 0, sorted):    [32000, 32010, 32020, 32030, 32040]
///   B-departures (seq 1, OVERTAKEN): [32500, 32510, 32200, 32530, 32540]
///   C-arrivals   (seq 2):            [32600, 32610, 32300, 32900, 32910]
///
/// A passenger accesses Stop B directly (Stop A is >1 km away, beyond the transfer
/// radius, and every A-departure is already in the past), boarding at the mid stop.
/// With `min_dep ≈ 32460` at B, `partition_point(dep < 32460)` over the
/// non-monotonic column binary-searches to index 3 (the express at index 2 reads
/// 32200 < min_dep and pushes `lo` right), so the boarding loop scans only trips
/// 3,4 (depart B at 32530/32540 → reach C at 32900/32910) and SKIPS the feasible
/// optimal trips 0,1 (depart B at 32500/32510 → reach C at 32600/32610). The
/// overtaking-split fix puts the express in its own sub-route, restoring monotonic
/// columns so `partition_point` is valid and the optimal trip is found.
fn overtaking_midstop_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.050));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.000)); // ~3.5 km from origin
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.051)); // ~72 m from origin
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.099)); // ~72 m from dest

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize, partial: bool| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial,
                    foot: true,
                    bike: false,
                    car: false,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_street(&mut g, osm_origin, osm_dest, 7200, false); // long walk-only fallback
    add_street(&mut g, stop_b, osm_origin, 72, true); // ~60 s access walk to B
    add_street(&mut g, stop_c, osm_dest, 72, true); // ~60 s egress walk from C

    let a_dep = [32000u32, 32010, 32020, 32030, 32040];
    let b_arr = [32500u32, 32510, 32200, 32530, 32540];
    let c_arr = [32600u32, 32610, 32300, 32900, 32910];

    // Transit departure segments (A→B then B→C), sorted by departure as ingestion does.
    let mut ab: Vec<TripSegment> = (0..5)
        .map(|i| TripSegment {
            trip_id: TripId(i as u32),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: a_dep[i],
            arrival: b_arr[i],
            service_id: ServiceId(0),
        })
        .collect();
    ab.sort_unstable_by_key(|s| s.departure);
    let mut bc: Vec<TripSegment> = (0..5)
        .map(|i| TripSegment {
            trip_id: TripId(i as u32),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: b_arr[i],
            arrival: c_arr[i],
            service_id: ServiceId(0),
        })
        .collect();
    bc.sort_unstable_by_key(|s| s.departure);
    g.add_transit_departures(ab);
    g.add_transit_departures(bc);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 5 },
            length: 7000,
        }),
    );
    g.add_edge(
        stop_b,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_b,
            destination: stop_c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 5, len: 5 },
            length: 7000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "P".into(),
        route_long_name: "Bus P".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(
        (0..5u32)
            .map(|_| TripInfo {
                trip_headsign: None,
                route_id: RouteId(0),
                service_id: ServiceId(0),
                bikes_allowed: None,
            })
            .collect(),
    );

    // Single pattern [A, B, C] with all 5 trips; column-major stop times.
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[stop_a, stop_b, stop_c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });

    let ts = g.transit_pattern_trips_len();
    for t in 0..5u32 {
        g.push_transit_pattern_trip(TripId(t));
    }
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 5 });

    let sts = g.transit_pattern_stop_times_len();
    let stop_cols = [&a_dep, &b_arr, &c_arr];
    for col in stop_cols {
        for &t in col.iter() {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
    }
    g.push_transit_idx_pattern_stop_times(Lookup {
        start: sts,
        len: 3 * 5,
    });

    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 5,
    });

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

/// Optimality regression for OVERTAKING trips. Within one pattern an express
/// overtakes the stopping trips between stop 0 and a mid stop, so the mid-stop
/// departure column is non-monotonic and `scan_route`'s `partition_point` boarding
/// cutoff (which assumes a sorted column) skips the feasible optimal trip. The
/// build-time overtaking split must restore the FIFO precondition so the optimal
/// trip is found. Before the fix this boards trip 3 (reaches C at 32900); after it
/// boards trip 0/1 (reaches C at 32600/32610).
#[test]
fn raptor_overtaking_midstop_finds_optimal_trip() {
    let (g, origin, dest) = overtaking_midstop_graph();

    // Depart 08:53:20; ~60 s access walk reaches Stop B at ≈32460, so trips 0,1
    // (depart B at 32500/32510) are catchable and optimal, trips 3,4 are worse.
    let plans = g.raptor(origin, dest, 32400, 0, 0x7F, 60);

    let best_transit_end = plans
        .iter()
        .filter_map(|p| {
            p.legs.iter().find_map(|l| match l {
                PlanLeg::Transit(t) => Some(t.end),
                _ => None,
            })
        })
        .min()
        .expect("a transit plan reaching Stop C must exist");

    assert!(
        best_transit_end <= 32610,
        "overtaking under-exploration: best transit arrival at C is {best_transit_end}, \
         expected ≤ 32610 (the express overtakes between A and B, so partition_point \
         must not skip the feasible optimal trips 0/1)",
    );
}

/// Structural invariant of the overtaking split: after `build_raptor_index`, EVERY
/// route's per-stop departure column must be non-decreasing, so `partition_point`
/// is valid everywhere. Verified over the built index of a graph that contains an
/// overtaking pattern (which the split must have decomposed into >1 sub-route).
#[test]
fn build_raptor_index_yields_monotonic_departure_columns() {
    let (g, _origin, _dest) = overtaking_midstop_graph();

    let r = &g.raptor;
    let mut split_into_multiple = false;
    for p in 0..r.transit_patterns.len() {
        let n_stops = r.transit_idx_pattern_stops[p].of(&r.transit_pattern_stops).len();
        let n_trips = r.transit_patterns[p].num_trips as usize;
        let times = r.transit_idx_pattern_stop_times[p].of(&r.transit_pattern_stop_times);
        // A 3-stop A→B→C pattern with all 5 trips would stay single if unsplit;
        // the express forces a 2nd sub-route, so no route keeps all 5 trips.
        split_into_multiple |= n_trips < 5;
        for s in 0..n_stops {
            for t in 1..n_trips {
                let prev = times[s * n_trips + (t - 1)].departure;
                let cur = times[s * n_trips + t].departure;
                assert!(
                    prev <= cur,
                    "route {p} stop {s}: departure column non-monotonic ({prev} > {cur})",
                );
            }
        }
    }
    assert!(
        split_into_multiple,
        "the overtaking pattern must have been split into multiple sub-routes",
    );
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
    assert_eq!(
        a, b,
        "raptor_range must return an identical ordered plan sequence on repeat calls"
    );
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
        (
            p.start,
            p.end,
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count(),
        )
    };
    let sp: HashSet<_> = g
        .raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter()
        .map(key)
        .collect();
    let oracle: HashSet<_> = g
        .raptor_range_independent(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter()
        .map(key)
        .collect();
    assert!(!oracle.is_empty(), "oracle must produce plans");
    assert_eq!(
        sp, oracle,
        "self-pruning range != independent-passes (single route, 4-D key)"
    );
}

/// Same oracle gate on a two-route graph that admits transfers, so transfer
/// preservation across departures is exercised (the only_nv class the 4-D contract
/// keeps and the 3-D contract would have dropped).
#[test]
fn self_pruning_range_equals_independent_two_route() {
    use std::collections::HashSet;
    let (g, origin, dest) = two_route_raptor_graph();
    let key = |p: &maas_rs::structures::plan::Plan| {
        (
            p.start,
            p.end,
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count(),
        )
    };
    let sp: HashSet<_> = g
        .raptor_range(origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter()
        .map(key)
        .collect();
    let oracle: HashSet<_> = g
        .raptor_range_independent(origin, dest, 8 * 3600, 180 * 60, 0, 0x7F, 10 * 60)
        .iter()
        .map(key)
        .collect();
    assert!(!oracle.is_empty(), "oracle must produce plans");
    assert_eq!(
        sp, oracle,
        "self-pruning range != independent-passes (two route, 4-D key)"
    );
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
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // 72m / 60s from origin
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099)); // 72m / 60s from dest
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 5.000)); // far from dest

    // Streets
    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: true,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_street(&mut g, osm_origin, osm_dest, 7200); // long direct walk (1 h)

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // 60s walk
    add_snap(&mut g, stop_b, osm_dest, 72); // 60s walk
    // stop_c has no snap edge to osm nodes (it's remote)

    // Transit edges (needed by reconstruct)
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 5 },
            length: 80_000,
        }),
    );
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 5, len: 3 },
            length: 7000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "99".into(),
            route_long_name: "Dead-end".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "42".into(),
            route_long_name: "Connecting".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);

    // 5 dead-end trips (pattern 0) + 3 connecting trips (pattern 1)
    g.add_transit_trips(
        (0..8u32)
            .map(|i| TripInfo {
                trip_headsign: None,
                route_id: if i < 5 { RouteId(0) } else { RouteId(1) },
                service_id: ServiceId(0),
                bikes_allowed: None,
            })
            .collect(),
    );

    // TripSegments (one per trip)
    let base = 9 * 3600u32;
    // Dead-end: 5 trips departing stop_A at 09:01, 09:02, 09:03, 09:04, 09:05.
    // earliest_at_stop = 09:00 + 60s walk = 09:01, so all 5 are within range.
    // Origin departure times = stop_A dep - 60s = 09:00, 09:01, 09:02, 09:03, 09:04.
    // These 5 fill collect_interesting_times' cap of 5 entirely, leaving no room
    // for the connecting pattern's trips (first at 09:30 → origin dep 09:29).
    let mut segs: Vec<TripSegment> = (0..5u32)
        .map(|i| TripSegment {
            trip_id: TripId(i),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: base + 60 + i * 60, // 09:01, 09:02, 09:03, 09:04, 09:05
            arrival: base + 60 + i * 60 + 3600, // 60 min later at stop_C
            service_id: ServiceId(0),
        })
        .collect();
    // Connecting: 3 trips at 09:30, 10:30, 11:30 (stop_A → stop_B, 30 min)
    segs.extend((0..3u32).map(|i| TripSegment {
        trip_id: TripId(5 + i),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: base + 1800 + i * 3600, // 09:30, 10:30, 11:30
        arrival: base + 1800 + i * 3600 + 1800, // 30 min later at stop_B
        service_id: ServiceId(0),
    }));
    g.add_transit_departures(segs);

    // Pattern 0 (dead-end): stop_A × stop_C, 5 trips, column-major
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_c]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 0..5u32 {
            g.push_transit_pattern_trip(TripId(i));
        }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 5 });

        let sts = g.transit_pattern_stop_times_len();
        // stop_A column (pos 0): departures at 09:01, 09:02, 09:03, 09:04, 09:05
        for i in 0..5u32 {
            let t = base + 60 + i * 60;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        // stop_C column (pos 1)
        for i in 0..5u32 {
            let t = base + 60 + i * 60 + 3600;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        g.push_transit_idx_pattern_stop_times(Lookup {
            start: sts,
            len: 10,
        });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 5,
        });
    }

    // Pattern 1 (connecting): stop_A × stop_B, 3 trips, column-major
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 5..8u32 {
            g.push_transit_pattern_trip(TripId(i));
        }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 3 });

        let sts = g.transit_pattern_stop_times_len();
        // stop_A column (pos 0)
        for i in 0..3u32 {
            let t = base + 1800 + i * 3600;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        // stop_B column (pos 1)
        for i in 0..3u32 {
            let t = base + 1800 + i * 3600 + 1800;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 3,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

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
        plans.len(),
        3,
        "raptor_range should return all 3 connecting trips (09:30, 10:30, 11:30) \
         from a 180-min window, but got {} plan(s). \
         Likely the dead-end pattern starved the interesting-times cap (bug).",
        plans.len(),
    );

    // All returned plans must actually reach the destination (end > start).
    for p in &plans {
        assert!(
            p.end > p.start,
            "plan end <= start: start={} end={}",
            p.start,
            p.end
        );
    }
}

/// Regression test for the range-query PROBE-GATE bug.
///
/// The range driver used to run a full single-departure probe at `start_time`
/// and bail with `vec![]` when it was empty — treating "no plan at start_time"
/// as "no plan at any window departure". That is unsound: extract's per-bucket
/// suppression anchors to the round-0 walk-chain label (access + footpath
/// transfer to the destination stop), whose arrival shifts with departure time,
/// while the transit arrival is a step function. So the probe at `start_time`
/// can be empty (the far-future bus is dominated by the walk-chain at that
/// departure) even though a later window departure boards the bus successfully.
///
/// Layout (all foot speeds 1.2 m/s):
///   osm_origin (50.000, 4.000) ─60s─ stop_A (50.000, 4.001)
///   osm_dest   (50.000, 4.005) ─60s─ stop_B (50.000, 4.004)
///   direct street osm_origin↔osm_dest = 360 m (300 s walk)
///   ⇒ auto footpath transfer A→B ≈ 360 s (300 s street + 60 s snap), < 1000 m.
/// One bus stop_A→stop_B, single trip departing stop_A at 09:10 (D=33000),
/// 120 s ride (r < transfer, so it survives at its own departure).
///
/// At start_time 09:00 (S=32400, access a=60, transfer T=360): the round-0
/// walk-chain reaches stop_B at S+a+T=32820; the bus arrives 33120 ≥ that, so
/// it is suppressed → probe empty → the OLD gate fired → walk-only fallback.
/// At the windowed departure t*=D−a=32940 the walk-chain reaches stop_B at
/// 33360 while the bus arrives 33120 < that, so the bus is a valid transit plan.
///
/// Expected AFTER the fix: `raptor_range` returns a plan containing a Transit
/// leg (the bus). BEFORE the fix it returns only the walk-only fallback.
#[test]
fn raptor_range_probe_gate_does_not_drop_windowed_transit_plan() {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.005));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001)); // 72 m / 60 s from origin
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.004)); // 72 m / 60 s from dest

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: true,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    // 360 m direct walk (300 s) — short enough that the A→B footpath transfer
    // (≈360 s) stays under the 1000 m transfer radius.
    add_street(&mut g, osm_origin, osm_dest, 360);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72); // 60 s walk
    add_snap(&mut g, stop_b, osm_dest, 72); // 60 s walk

    // Single bus stop_A → stop_B.
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 300,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus".into(),
        route_type: RouteType::Bus,
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

    let d_dep = 33000u32; // 09:10 departure from stop_A
    let d_arr = d_dep + 120; // 120 s ride → arrive stop_B 33120
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: d_dep,
        arrival: d_arr,
        service_id: ServiceId(0),
    }]);

    // Pattern 0: stop_A × stop_B, 1 trip, column-major.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: d_dep,
            departure: d_dep,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: d_arr,
            departure: d_arr,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    // 60-min window from 09:00, min_access 10 min.
    let plans = g.raptor_range(osm_origin, osm_dest, 32400, 60 * 60, 0, 0x7F, 600);

    let has_transit = plans
        .iter()
        .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))));
    assert!(
        has_transit,
        "raptor_range must return the windowed bus plan (a Transit leg), not just \
         the walk-only fallback. The start_time probe is empty (the bus is dominated \
         by the walk-chain at 09:00), but the 09:10 bus is valid at its own departure. \
         Got {} plan(s), none with a Transit leg.",
        plans.len(),
    );
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
    let dist = LatLng {
        latitude: 50.0,
        longitude: 4.0,
    }
    .dist(LatLng {
        latitude: 50.001,
        longitude: 4.0,
    }) as usize;
    g.add_edge(n0, street_edge(n0, n1, dist));
    g.add_edge(n1, street_edge(n1, n0, dist));
    g.build_raptor_index();
    enable_contraction(&mut g);

    // min_access_secs=1 forces many doublings before walk-only is reached.
    let plans = g.raptor(n0, n1, 0, 0, 0x7F, 1);

    assert_eq!(plans.len(), 1, "expected exactly one walk-only plan");
    assert_eq!(plans[0].legs.len(), 1);
    assert!(
        matches!(plans[0].legs[0], PlanLeg::Walk(_)),
        "single leg should be a walk"
    );
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

    let osm_a = g.add_node(osm_node("osm_a", 50.000, 4.000));
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.002));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.000));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.002));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.100));

    let add_street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: true,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_street(&mut g, osm_a, osm_origin, 180);
    add_street(&mut g, osm_origin, osm_dest, 7_000);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    add_snap(&mut g, stop_a, osm_a, 10);
    add_snap(&mut g, stop_b, osm_origin, 10);
    add_snap(&mut g, stop_c, osm_dest, 10);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 180,
        }),
    );
    g.add_edge(
        stop_b,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_b,
            destination: stop_c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 7_000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);

    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Route X".into(),
        route_type: RouteType::Bus,
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

    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 10 * 3600,
            arrival: 10 * 3600 + 120,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: 10 * 3600 + 120,
            arrival: 10 * 3600 + 1200,
            service_id: ServiceId(0),
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600,
            departure: 10 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600 + 120,
            departure: 10 * 3600 + 120,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600 + 1200,
            departure: 10 * 3600 + 1200,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

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
        let backward_walk = plan
            .legs
            .iter()
            .any(|leg| matches!(leg, PlanLeg::Walk(w) if w.to.node_id == stop_a));
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
        let has_backward_walk = plan
            .legs
            .iter()
            .any(|leg| matches!(leg, PlanLeg::Walk(w) if w.to.node_id == stop_a));
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
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 100,
            arrival: 200,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 300,
            arrival: 400,
            service_id: ServiceId(0),
        },
    ]);
    // Segment covers only index 1; querying with index 0 (< start) used to underflow.
    let tt = TimetableSegment { start: 1, len: 1 };
    let prev: Vec<_> = g.previous_departures(tt, 0, 0x7F, 0).collect();
    assert!(
        prev.is_empty(),
        "out-of-segment previous_departures should be empty, not panic"
    );
    let next: Vec<_> = g.next_departures(tt, 0, 0x7F, 0).collect();
    assert!(
        next.is_empty(),
        "out-of-segment next_departures should be empty, not panic"
    );
    // A valid in-segment index still works.
    let prev_ok: Vec<_> = g
        .previous_departures(TimetableSegment { start: 0, len: 2 }, 0, 0x7F, 1)
        .collect();
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

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
            timetable_segment: TimetableSegment { start: 1, len: 2 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 0: bus
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 1: tram tight
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 2: tram safe
    ]);
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
            departure: 9 * 3600 + 1200,
            arrival: 9 * 3600 + 2100,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 10 * 3600,
            arrival: 10 * 3600 + 900,
            service_id: ServiceId(0),
        },
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1200,
            departure: 9 * 3600 + 1200,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600,
            departure: 10 * 3600,
        ..Default::default()
        });
        // col D (arr): tight 09:35, safe 10:15
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2100,
            departure: 9 * 3600 + 2100,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 10 * 3600 + 900,
            departure: 10 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 2,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    // Bus delay model: small transfer margin ⇒ low on-time prob, large margin ⇒ certain.
    let mut models = HashMap::new();
    models.insert(
        RouteType::Bus,
        DelayCDF {
            bins: vec![(0, 0.05), (300, 0.5), (900, 0.9), (1800, 1.0)],
        },
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
    let plans = g.raptor_tuned(
        origin,
        dest,
        8 * 3600 + 1800,
        0,
        0x7F,
        10 * 60,
        &buckets,
        3600,
    );

    // Worst transfer reliability per plan (1.0 if no risk), with its arrival time.
    let mut summary: Vec<(f32, u32)> = plans
        .iter()
        .map(|p| {
            let worst = p
                .legs
                .iter()
                .filter_map(|l| match l {
                    PlanLeg::Transit(t) => t.transfer_risk.as_ref().map(|r| r.reliability),
                    _ => None,
                })
                .fold(1.0f32, f32::min);
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
        safe,
        risky
    );
}

/// Increasing arrival slack never removes plans — a wider explored band can only
/// add non-dominated alternatives. Guards the slack lever's monotonicity.
#[test]
fn raptor_more_slack_never_fewer_plans() {
    let (g, origin, dest) = reliability_tradeoff_graph();
    let buckets = ReliabilityBuckets::default();
    let few = g
        .raptor_tuned(origin, dest, 8 * 3600 + 1800, 0, 0x7F, 10 * 60, &buckets, 0)
        .len();
    let many = g
        .raptor_tuned(
            origin,
            dest,
            8 * 3600 + 1800,
            0,
            0x7F,
            10 * 60,
            &buckets,
            3600,
        )
        .len();
    assert!(
        many >= few,
        "more slack ({many}) should not yield fewer plans than less ({few})"
    );
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
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
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 3 },
            length: 1362,
        }),
    );
    g.add_edge(
        stop_c,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_c,
            destination: stop_d,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 3, len: 1 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 0: bus early
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 1: bus safe
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 2: bus dangerous
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // 3: tram
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600,
            arrival: 8 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 900,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1200,
            arrival: 9 * 3600 + 1560,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(3),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600 + 1800,
            arrival: 9 * 3600 + 2700,
            service_id: ServiceId(0),
        },
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600,
            departure: 8 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1200,
            departure: 9 * 3600 + 1200,
        ..Default::default()
        });
        // stop_B col (arr): 08:15, 09:15, 09:26
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600 + 900,
            departure: 8 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 900,
            departure: 9 * 3600 + 900,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1560,
            departure: 9 * 3600 + 1560,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 3,
        });
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 2700,
            departure: 9 * 3600 + 2700,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    // Bus delay model: tiny margin ⇒ low on-time prob, large margin ⇒ certain.
    let mut models = HashMap::new();
    models.insert(
        RouteType::Bus,
        DelayCDF {
            bins: vec![(60, 0.3), (600, 0.95), (1200, 1.0)],
        },
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
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 2
        })
        .expect("Expected a Bus+Tram plan");

    let transit: Vec<_> = two_leg
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
    let bus = transit[0];
    let tram = transit[1];
    let rel = tram
        .transfer_risk
        .as_ref()
        .expect("tram leg has transfer risk")
        .reliability;

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
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count(),
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
            let only_in_bucket_only = only_in
                .iter()
                .filter(|k| sp3.contains(&(k.0, k.1, k.2)))
                .count();
            let only_sp_bucket_only = only_sp
                .iter()
                .filter(|k| in3.contains(&(k.0, k.1, k.2)))
                .count();

            println!(
                "[w={:>2}m] {:<16} sp {:>3}/{:>6}ms | indep {:>3}/{:>6}ms | {:.2}x | only_sp={} (bkt {}) only_in={} (bkt {})",
                window_min,
                label,
                sp.len(),
                sp_ms,
                indep.len(),
                indep_ms,
                indep_ms as f64 / sp_ms.max(1) as f64,
                only_sp.len(),
                only_sp_bucket_only,
                only_in.len(),
                only_in_bucket_only,
            );
            // Classify each only_in key: is it 4-D-dominated by some self-pruning key
            // (acceptable — sp's set still covers it) or a genuine missed Pareto point?
            // 4-D dom: tc_a<=tc_b && end_a<=end_b && start_a>=start_b && bkt_a>=bkt_b, strict in one.
            let dom = |a: &(u32, u32, usize, u8), b: &(u32, u32, usize, u8)| {
                a.2 <= b.2
                    && a.1 <= b.1
                    && a.0 >= b.0
                    && a.3 >= b.3
                    && (a.2 < b.2 || a.1 < b.1 || a.0 > b.0 || a.3 > b.3)
            };
            let genuine_miss: Vec<_> = only_in
                .iter()
                .filter(|k| !sp_keys.iter().any(|s| dom(s, k)))
                .collect();
            if !only_sp.is_empty() {
                println!("    only_sp: {only_sp:?}");
            }
            if !only_in.is_empty() {
                println!("    only_in: {only_in:?}");
                println!(
                    "    genuine_miss (not dominated by any sp plan): {} -> {genuine_miss:?}",
                    genuine_miss.len()
                );
                // Dump legs of the first genuine-miss plan (from the independent set),
                // plus whether any independent plan itself dominates it (filter sanity).
                if let Some(&gm) = genuine_miss.first()
                    && let Some(p) = indep.iter().find(|p| key(p) == *gm)
                {
                    let self_dom = indep.iter().any(|q| key(q) != *gm && dom(&key(q), gm));
                    println!("    >>> MISS {gm:?} | dominated within indep set? {self_dom}");
                    for leg in &p.legs {
                        match leg {
                            PlanLeg::Transit(t) => println!(
                                "        TRANSIT {}->{} dep={} arr={} rt={:?} rel={:?}",
                                t.from.node_id.0,
                                t.to.node_id.0,
                                t.start,
                                t.end,
                                t.route_type,
                                t.transfer_risk.as_ref().map(|r| r.reliability)
                            ),
                            PlanLeg::Walk(w) => println!(
                                "        WALK    {}->{} {}s",
                                w.from.node_id.0, w.to.node_id.0, w.duration
                            ),
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
    enable_contraction(&mut g);
    let am = ActiveModes::new(&[Mode::Bike]);
    let a_ll = LatLng { latitude: 50.000, longitude: 4.000 };
    let c_ll = LatLng { latitude: 50.000, longitude: 4.002 };
    let plans = raptor_modes_ep(&g, a, c, a_ll, c_ll, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].mode, Mode::Bike);
    // Direct bike now reports the kinematic ETA of the cost-optimal route: two
    // flat 100 m road edges (the chain a→b→c), each solved by the power model.
    let bc = BikeCost::new(BikeProfile::default());
    let edge100 = StreetEdgeData {
        origin: NodeID(0),
        destination: NodeID(1),
        length: 100,
        partial: false,
        foot: true,
        bike: true,
        car: true,
        attrs: BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: VarGen::NONE,
    };
    let expected = 2 * bc.edge_time(&edge100);
    assert_eq!(plans[0].end - plans[0].start, expected);
    assert_eq!(street_modes(&plans[0]), vec![Mode::Bike]);
}

#[test]
fn walk_and_bike_direct_both_returned_when_selected() {
    let (mut g, a, _, c) = three_node_street_graph();
    g.build_raptor_index();
    enable_contraction(&mut g);
    let am = ActiveModes::new(&[Mode::Walk, Mode::Bike]);
    let a_ll = LatLng { latitude: 50.000, longitude: 4.000 };
    let c_ll = LatLng { latitude: 50.000, longitude: 4.002 };
    let plans = raptor_modes_ep(&g, a, c, a_ll, c_ll, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    let modes: Vec<Mode> = plans.iter().map(|p| p.mode).collect();
    assert!(modes.contains(&Mode::Walk), "modes: {modes:?}");
    assert!(modes.contains(&Mode::Bike), "modes: {modes:?}");
}

#[test]
fn direct_bike_absent_with_default_modes() {
    let (mut g, a, _, c) = three_node_street_graph();
    g.build_raptor_index();
    enable_contraction(&mut g);
    let plans = g.raptor_modes(a, c, 8 * 3600, 0, 0x7F, 10 * 60, &ActiveModes::default());
    assert!(plans.iter().all(|p| p.mode != Mode::Bike));
}

/// When cycling the whole way beats every bike+transit combination, the only
/// bike-mode result is the direct ride — "no improvement → no transit plan".
#[test]
fn direct_bike_returned_when_transit_brings_no_improvement() {
    let (mut g, origin, dest) = two_route_raptor_graph_with_bikes(Some(true), Some(true));
    enable_contraction(&mut g);
    let am = ActiveModes::new(&[Mode::BikeTransit]);
    let origin_ll = LatLng { latitude: 50.000, longitude: 4.000 };
    let dest_ll = LatLng { latitude: 50.000, longitude: 4.041 };
    let plans = raptor_modes_ep(&g, origin, dest, origin_ll, dest_ll, 8 * 3600, 0, 0x7F, 10 * 60, &am);

    assert!(
        plans
            .iter()
            .any(|p| p.mode == Mode::Bike && transit_leg_count(p) == 0),
        "expected the direct ride, got: {:?}",
        plans
            .iter()
            .map(|p| (p.mode, transit_leg_count(p)))
            .collect::<Vec<_>>()
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
        origin,
        dest,
        8 * 3600,
        180 * 60,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        &rt,
        &am,
        &BikeCost::new(BikeProfile::default()),
    );
    let indep = g.raptor_range_independent_rt_modes(
        origin,
        dest,
        8 * 3600,
        180 * 60,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &rt,
        &am,
    );

    let key = |p: &maas_rs::structures::plan::Plan| (p.mode, p.start, p.end, transit_leg_count(p));
    let mut a: Vec<_> = pruned.iter().map(key).collect();
    let mut b: Vec<_> = indep.iter().map(key).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(
        a, b,
        "self-pruning range diverged from the independent oracle"
    );
}

/// Bike modes flow through the explain (debug) path too: same plans, plus
/// candidate/stop instrumentation, without falling back to direct plans.
#[test]
fn raptor_explain_supports_bike_modes() {
    let (g, origin, dest) = express_two_leg_graph(Some(true), Some(true));
    let am = ActiveModes::new(&[Mode::BikeOnTransit]);
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let res = g.raptor_explain_tuned_rt_modes(
        origin,
        dest,
        8 * 3600 + 3300,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &RealtimeIndex::new(),
        &am,
        &BikeCost::new(BikeProfile::default()),
        None,
        maas_rs::structures::cost::FareProfile::default(),
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
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    edge(&mut g, o, a, 600, cyc); // cycleway O–A–D = 1200 m, low cost
    edge(&mut g, a, d, 600, cyc);
    edge(&mut g, o, d, 715, prim); // unsafe primary O–D = 715 m, high cost
    edge(&mut g, d, stop, 8, snap); // foot connector to the platform

    // Junction-breaking stub: `o` has exactly 2 unique street-graph neighbours
    // (`a` via cycleway and `d` via primary) so the contracted graph would mark
    // it as an interior pass-through. bike_dijkstra_union requires a junction
    // origin, so add a degree-1 dead-end to raise `o` to degree 3.
    let o_stub = g.add_node(osm_node("o_stub", 50.001, 4.000));
    edge(&mut g, o, o_stub, 1, snap);

    g.build_raptor_index();
    enable_contraction(&mut g);

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
        surface_speed: 100,
        var_gen: VarGen::NONE,
    };
    let t_cyc = bc.edge_time(&mk(600, cyc)) * 2 + bc.edge_time(&mk(8, snap));
    let t_prim = bc.edge_time(&mk(715, prim)) + bc.edge_time(&mk(8, snap));
    assert!(
        t_cyc > t_prim,
        "test setup: cycleway must be the slower corridor"
    );

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

// ── Edge-aware snapping ────────────────────────────────────────────────────────

#[test]
fn snap_to_edge_projects_onto_long_edge_not_nearest_node() {
    // A long straight edge a–b, with an off-segment node c near the midpoint.
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.004)); // ~286 m east
    let c = g.add_node(osm_node("c", 50.0002, 4.002)); // ~22 m N of mid-edge
    g.add_edge(a, street_edge(a, b, 286));
    g.add_edge(b, street_edge(b, a, 286));
    g.add_edge(c, street_edge(c, a, 200));
    g.add_edge(a, street_edge(a, c, 200));
    g.build_edge_index();

    let (plat, plon) = (50.000, 4.002); // on the a–b line, midway

    // Node snapping picks the off-segment node c.
    assert_eq!(g.nearest_node(plat, plon), Some(c));

    // Edge snapping projects onto the a–b edge.
    let (ep, perp) = g
        .snap_to_edge(plat, plon, 400.0, |s| s.bike)
        .expect("edge found");
    match ep {
        Endpoint::OnEdge {
            a: ea,
            b: eb,
            dist_a,
            dist_b,
            ..
        } => {
            assert!(perp < 2.0, "perp {perp}");
            assert!(
                (ea == a && eb == b) || (ea == b && eb == a),
                "endpoints {ea:?},{eb:?}"
            );
            assert_eq!(dist_a + dist_b, 286, "offsets sum to edge length");
            assert!(
                (130..=156).contains(&dist_a),
                "midpoint offset ~143: {dist_a}"
            );
            assert!(
                (130..=156).contains(&dist_b),
                "midpoint offset ~143: {dist_b}"
            );
        }
        _ => panic!("expected OnEdge"),
    }
}

#[test]
fn snap_to_edge_finds_long_edge_with_far_endpoints() {
    // A long BIKE-only edge a–b whose endpoints are ~600 m apart, with the only
    // nearby node a FOOT-only stub near the edge midpoint — NOT incident to a or b.
    // The old node-KD-tree scan never inspects a–b (its endpoints are out of
    // radius and the near node touches neither), so it must miss the bike edge.
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.0085)); // ~607 m east
    let f = g.add_node(osm_node("f", 50.0001, 4.00425)); // ~11 m N of mid-edge
    let stub = g.add_node(osm_node("stub", 50.0010, 4.00425)); // off to the north
    g.add_edge(a, street_edge_flags(a, b, 607, false, true));
    g.add_edge(b, street_edge_flags(b, a, 607, false, true));
    g.add_edge(f, street_edge_flags(f, stub, 100, true, false));
    g.add_edge(stub, street_edge_flags(stub, f, 100, true, false));
    g.build_edge_index();

    let (plat, plon) = (50.000, 4.00425); // bike-edge midpoint

    // Node snapping picks the off-segment foot node f.
    assert_eq!(g.nearest_node(plat, plon), Some(f));

    // Bike-edge snapping must project onto a–b even though its endpoints are far.
    let (ep, perp) = g
        .snap_to_edge(plat, plon, 300.0, |s| s.bike)
        .expect("bike edge found");
    match ep {
        Endpoint::OnEdge {
            a: ea,
            b: eb,
            dist_a,
            dist_b,
            ..
        } => {
            assert!(perp < 15.0, "perp {perp}");
            assert!(
                (ea == a && eb == b) || (ea == b && eb == a),
                "endpoints {ea:?},{eb:?}"
            );
            assert_eq!(dist_a + dist_b, 607, "offsets sum to edge length");
            assert!(
                (280..=327).contains(&dist_a),
                "midpoint offset ~303: {dist_a}"
            );
        }
        _ => panic!("expected OnEdge"),
    }
}

#[test]
fn snap_to_edge_is_mode_aware_walk_car() {
    // Three parallel long edges near the query: a FOOT-only, a CAR-only, a BIKE-only.
    let mut g = Graph::new();
    let fa = g.add_node(osm_node("fa", 50.0010, 4.000));
    let fb = g.add_node(osm_node("fb", 50.0010, 4.006));
    let ca = g.add_node(osm_node("ca", 50.0000, 4.000));
    let cb = g.add_node(osm_node("cb", 50.0000, 4.006));
    let ba = g.add_node(osm_node("ba", 49.9990, 4.000));
    let bb = g.add_node(osm_node("bb", 49.9990, 4.006));
    g.add_edge(fa, EdgeData::Street(street_edge_full(fa, fb, 430, true, false, false)));
    g.add_edge(fb, EdgeData::Street(street_edge_full(fb, fa, 430, true, false, false)));
    g.add_edge(ca, EdgeData::Street(street_edge_full(ca, cb, 430, false, false, true)));
    g.add_edge(cb, EdgeData::Street(street_edge_full(cb, ca, 430, false, false, true)));
    g.add_edge(ba, EdgeData::Street(street_edge_full(ba, bb, 430, false, true, false)));
    g.add_edge(bb, EdgeData::Street(street_edge_full(bb, ba, 430, false, true, false)));
    g.build_edge_index();

    let (plat, plon) = (50.0005, 4.003); // between the foot and car rows

    let pick = |usable: fn(&StreetEdgeData) -> bool| {
        let (ep, _) = g.snap_to_edge(plat, plon, 300.0, usable).expect("edge");
        match ep {
            Endpoint::OnEdge { a, b, .. } => (a, b),
            _ => panic!("expected OnEdge"),
        }
    };

    let (wa, wb) = pick(|s| s.foot);
    assert!(
        (wa == fa && wb == fb) || (wa == fb && wb == fa),
        "walk→foot edge, got {wa:?},{wb:?}"
    );
    let (xa, xb) = pick(|s| s.car);
    assert!(
        (xa == ca && xb == cb) || (xa == cb && xb == ca),
        "car→car edge, got {xa:?},{xb:?}"
    );
    let (ya, yb) = pick(|s| s.bike);
    assert!(
        (ya == ba && yb == bb) || (ya == bb && yb == ba),
        "bike→bike edge, got {ya:?},{yb:?}"
    );
}

// ── Transit access/egress multiobj integration ────────────────────────────────

/// Graph with TWO distinct walk routes origin→stop_a (short unpaved vs. long paved)
/// AND two distinct walk routes stop_b→destination (same pattern), plus a single
/// transit trip stop_a→stop_b. This ensures `multiobj_leg_options` finds ≥1 option
/// on each access/egress leg so the non-empty branch in `extract_with_debug` runs.
fn multiobj_transit_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let via_acc = g.add_node(osm_node("via_acc", 50.001, 4.004));
    let hub_a = g.add_node(osm_node("hub_a", 50.000, 4.008));
    let hub_b = g.add_node(osm_node("hub_b", 50.000, 4.090));
    let via_egr = g.add_node(osm_node("via_egr", 50.001, 4.094));
    let destination = g.add_node(osm_node("dest", 50.000, 4.098));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.0081));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.0901));

    let mk_foot = |o: NodeID, d: NodeID, len: usize, surface: Surface| {
        let mut at = BikeAttrs::road_default();
        at.surface = surface;
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: len,
            partial: false,
            foot: true,
            bike: true,
            car: false,
            attrs: at,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        })
    };
    let bidirectional = |g: &mut Graph, a: NodeID, b: NodeID, len: usize, surface: Surface| {
        g.add_edge(a, mk_foot(a, b, len, surface));
        g.add_edge(b, mk_foot(b, a, len, surface));
    };

    bidirectional(&mut g, origin, hub_a, 580, Surface::Unpaved);
    bidirectional(&mut g, origin, via_acc, 420, Surface::Paved);
    bidirectional(&mut g, via_acc, hub_a, 420, Surface::Paved);

    let connector = |g: &mut Graph, a: NodeID, b: NodeID| {
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                length: 8,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        g.add_edge(
            b,
            EdgeData::Street(StreetEdgeData {
                origin: b,
                destination: a,
                length: 8,
                partial: true,
                foot: true,
                bike: false,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
    };
    connector(&mut g, hub_a, stop_a);

    bidirectional(&mut g, hub_b, destination, 580, Surface::Unpaved);
    bidirectional(&mut g, hub_b, via_egr, 420, Surface::Paved);
    bidirectional(&mut g, via_egr, destination, 420, Surface::Paved);
    connector(&mut g, hub_b, stop_b);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 5900,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "M".into(),
        route_long_name: "Metro M".into(),
        route_type: RouteType::Subway,
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
        departure: 9 * 3600 + 600,
        arrival: 9 * 3600 + 600 + 480,
        service_id: ServiceId(0),
    }]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600 + 480,
            departure: 9 * 3600 + 600 + 480,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.set_distance_budget(f64::INFINITY);
    g.build_raptor_index();
    enable_contraction(&mut g);

    (g, origin, destination)
}

#[test]
fn transit_access_egress_multiobj_alternatives_and_leave_by() {
    let (g, _origin, _destination) = multiobj_transit_graph();

    use chrono::{NaiveDate, NaiveTime};
    let q = RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.098,
        date: NaiveDate::from_ymd_opt(2026, 6, 23).unwrap(),
        time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        window_minutes: None,
        min_access_secs: None,
        arrival_slack_secs: None,
        unrestricted_transfers: None,
        use_cch_access: None,
        reliability_bucket_edges: None,
        modes: Some(vec![Mode::WalkTransit]),
        bike_profile: None,
        terminal_deadline: false,
        onboard_origin: None,
        from_station_id: None,
        to_station_id: None,
        profile_latency: None,
        fare_profile: None,
    };
    let plans = route(&g, &q, &RealtimeIndex::new()).expect("route should succeed");

    let transit_plan = plans
        .iter()
        .find(|p| p.mode == Mode::WalkTransit && transit_leg_count(p) >= 1)
        .expect("expected at least one WalkTransit plan with a transit leg");

    let access_leg = transit_plan.legs.iter().find_map(|l| match l {
        PlanLeg::Walk(w) if w.leave_by.is_some() => Some(w),
        _ => None,
    });
    let access_leg = access_leg.expect("transit plan must have an access walk leg with leave_by");

    assert!(
        !access_leg.alternatives.is_empty(),
        "access leg must have multiobj alternatives (got empty — scalar fallback ran instead)"
    );
    assert!(
        access_leg.leave_by.is_some(),
        "access leg must carry a leave_by deadline"
    );

    let egress_leg = transit_plan.legs.iter().rev().find_map(|l| match l {
        PlanLeg::Walk(w) if w.leave_by.is_none() && !w.alternatives.is_empty() => Some(w),
        _ => None,
    });
    assert!(
        egress_leg.is_some(),
        "transit plan must have an egress walk leg with multiobj alternatives"
    );
}

// ── P3 node-contraction T2: flag-on == flag-off end-to-end ───────────────────

/// Builds a small transit graph whose street network has long degree-2 chains
/// between the transit-stop junctions, so the union contraction genuinely
/// collapses interior nodes. Two bus stops (A, B) sit on OSM junctions joined by
/// a 6-segment foot/bike/car chain; `origin` and `dest` junctions hang off each
/// stop by their own short chains. A single bus trip runs A→B.
fn contraction_t2_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let origin = g.add_node(osm_node("origin", 50.000, 4.0000));
    let j_a = g.add_node(osm_node("jA", 50.000, 4.0030));
    let j_b = g.add_node(osm_node("jB", 50.000, 4.0300));
    let dest = g.add_node(osm_node("dest", 50.000, 4.0330));

    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.0031));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.0299));

    let bidir = |g: &mut Graph, a: NodeID, b: NodeID, m: usize, car: bool| {
        let mk = |o: NodeID, d: NodeID| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                length: m,
                partial: false,
                foot: true,
                bike: true,
                car,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, mk(a, b));
        g.add_edge(b, mk(b, a));
    };

    // A degree-2 chain of `n` interior OSM nodes from `a` to `b`. `car` gates car
    // passability of the whole chain (the middle A→B chain is foot/bike-only so a
    // CarDropOff plan — drive→stop_A, bus→stop_B, walk→dest — survives instead of
    // being dominated by an all-the-way direct drive).
    let chain = |g: &mut Graph,
                 a: NodeID,
                 b: NodeID,
                 lon_a: f64,
                 lon_b: f64,
                 n: usize,
                 tag: &str,
                 car: bool| {
        let lat = 50.000;
        let mut prev = a;
        for i in 0..n {
            let f = (i + 1) as f64 / (n + 1) as f64;
            let lon = lon_a + (lon_b - lon_a) * f;
            let nid = g.add_node(osm_node(&format!("{tag}_{i}"), lat, lon));
            bidir(g, prev, nid, 100, car);
            prev = nid;
        }
        bidir(g, prev, b, 100, car);
    };

    chain(&mut g, origin, j_a, 4.0000, 4.0030, 2, "oa", true);
    chain(&mut g, j_a, j_b, 4.0030, 4.0300, 6, "ab", false);
    chain(&mut g, j_b, dest, 4.0300, 4.0330, 2, "bd", true);

    // Stop snap edges (partial, foot-only) onto their junctions.
    let snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        let mut e = |o: NodeID, d: NodeID| {
            g.add_edge(
                o,
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
                }),
            );
        };
        e(stop, osm);
        e(osm, stop);
    };
    snap(&mut g, stop_a, j_a, 8);
    snap(&mut g, stop_b, j_b, 8);

    // One bus trip A→B.
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 1900,
        }),
    );
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus 1".into(),
        route_type: RouteType::Bus,
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
        arrival: 9 * 3600 + 600,
        service_id: ServiceId(0),
    }]);
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
        ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
        ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    (g, origin, dest)
}

/// T4.3 the FINAL g-free proof: route a plan, DROP the interior-node arrays
/// (`drop_full_node_arrays` — the P3f memory win), then re-route the SAME query and
/// assert the plan is non-vacuous and BYTE-IDENTICAL to the pre-drop one. Only a
/// drop-then-route test can catch a lingering `g.nodes`/`g.edges` read that the flag-
/// on==flag-off gates (which keep `g`) cannot.
///
/// SCOPE (honest): **JUNCTION endpoints** via `raptor_modes`/`raptor`, covering direct
/// Walk, walk-transit, and car-drop-off→transit:
///   - JUNCTION endpoints (not `route()`/coords): the snapping path is NOT yet g-free
///     (coord-threading is deferred — `snap_node` still reads the g kdtree), so a
///     coord-routed post-drop query would error by design. Junctions hit the g-free
///     `node_walk_entries` junction branch + `junction_coord` geometry.
///   - Transit legs ARE now g-free: stop coords come from `node_loc` (contracted
///     `junction_coord`) and the `timetable_segment` from the precomputed
///     `transit_pattern_segment_timetables` side-table — so transit-leg reconstruction
///     and `previous_departures` no longer read `g`. This gate proves traversal +
///     geometry + reconstruction are g-free for every junction-routed mode.
#[test]
fn t4_drop_g_then_route_identical() {
    use maas_rs::structures::contraction::ContractedGraph;

    let (mut g, origin, dest) = contraction_t2_graph();
    // `origin`/`dest` are degree-1 chain ends ⇒ junctions; routing from them avoids the
    // (not-yet-g-free) snapping path.
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);

    assert!(
        g.contracted.as_ref().unwrap().junction_of[origin.0] != u32::MAX,
        "origin must be a junction"
    );

    use maas_rs::structures::plan::Plan;
    let dbg = |ps: &[Plan]| ps.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>();

    // Plans BEFORE the drop (contracted, g still present): direct Walk, walk-transit
    // (RAPTOR core + transit-leg reconstruction), and car-drop-off (drive→board→ride,
    // exercising the `previous_departures` timetable lookup in transit.rs).
    let walk = ActiveModes::new(&[Mode::Walk]);
    let before_walk = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &walk);
    let before_transit = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);
    let car = ActiveModes::new(&[Mode::CarDropOff]);
    let before_car = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &car);
    assert!(!before_walk.is_empty(), "pre-drop: expected a Walk plan");
    assert!(
        before_transit.iter().any(|p| transit_leg_count(p) >= 1),
        "pre-drop: expected a transit plan"
    );
    assert!(
        before_car.iter().any(|p| p.mode == Mode::CarDropOff && transit_leg_count(p) >= 1),
        "pre-drop: expected a car-drop-off→transit plan"
    );

    // THE IRREVERSIBLE STEP: free the interior-node arrays.
    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "node arrays must be empty after the drop");

    // Re-route the SAME queries against the dropped graph — must not panic, must match.
    let after_walk = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &walk);
    let after_transit = g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60);
    let after_car = g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &car);
    assert_eq!(
        dbg(&before_walk),
        dbg(&after_walk),
        "post-drop Walk plan must equal the pre-drop contracted plan"
    );
    assert_eq!(
        dbg(&before_transit),
        dbg(&after_transit),
        "post-drop walk-transit plan must equal pre-drop (g-free transit-leg reconstruction)"
    );
    assert_eq!(
        dbg(&before_car),
        dbg(&after_car),
        "post-drop car-drop-off→transit plan must equal pre-drop (g-free previous_departures)"
    );
}

/// EXPLAIN drop gate: the `raptorExplain` survey (`stops_reached` + per-leg path
/// geometry) and plan stop-name/coord resolution read `g.nodes` for transit-stop
/// names + coordinates. After `drop_full_node_arrays()` empties `g.nodes`, those reads
/// panic (the live-server crash this fix targets). With the serialized
/// `transit_stop_names` + `node_loc`, `route_explain` must (a) not panic post-drop and
/// (b) be byte-identical pre/post drop. The literal-name assertion below additionally
/// guards a *consistently-wrong* names field (which equality alone would pass).
#[test]
fn t4_explain_drop_gate_identical() {
    use maas_rs::routing::routing_raptor::route_explain;
    use maas_rs::structures::contraction::ContractedGraph;
    use maas_rs::structures::plan::PlanNode;
    use chrono::{NaiveDate, NaiveTime};

    let (mut g, _origin, _dest) = contraction_t2_graph();
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();

    // Stop A / Stop B are the 5th/6th nodes added in `contraction_t2_graph`.
    let (stop_a, stop_b) = (NodeID(4), NodeID(5));

    // Coord query spanning origin→dest so RAPTOR reaches the stops (walk-transit).
    let q = RouteQuery {
        from_lat: 50.000,
        from_lng: 4.0000,
        to_lat: 50.000,
        to_lng: 4.0330,
        date: NaiveDate::from_ymd_opt(2026, 6, 23).unwrap(),
        time: NaiveTime::from_hms_opt(8, 50, 0).unwrap(),
        window_minutes: None,
        min_access_secs: Some(600),
        arrival_slack_secs: None,
        unrestricted_transfers: None,
        use_cch_access: None,
        reliability_bucket_edges: None,
        modes: Some(vec![Mode::Walk, Mode::WalkTransit]),
        bike_profile: None,
        terminal_deadline: false,
        onboard_origin: None,
        from_station_id: None,
        to_station_id: None,
        profile_latency: None,
        fare_profile: None,
    };

    let before = route_explain(&g, &q, &RealtimeIndex::new()).expect("pre-drop explain");
    assert!(
        !before.stops_reached.is_empty(),
        "explain must reach the bus stops pre-drop"
    );
    // Names must come through populated (not the consistently-wrong empty string).
    assert!(
        before.stops_reached.iter().any(|s| s.name == "Stop A"),
        "stops_reached must carry the literal stop name pre-drop"
    );

    // Plan-node resolution (the GraphQL render layer) for a transit stop.
    let node_before = PlanNode::from_node_id(&g, stop_a).expect("plan node pre-drop");
    assert!(
        format!("{node_before:?}").contains("Stop A"),
        "PlanNode must carry the transit-stop name pre-drop"
    );

    // THE IRREVERSIBLE STEP.
    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "arrays dropped");

    let after = route_explain(&g, &q, &RealtimeIndex::new())
        .expect("post-drop explain must not panic/error");
    assert_eq!(
        format!("{:?}", before.stops_reached),
        format!("{:?}", after.stops_reached),
        "explain stops_reached must be byte-identical pre/post drop"
    );
    assert_eq!(
        before.plans.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>(),
        after.plans.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>(),
        "explain plans must be byte-identical pre/post drop"
    );

    // Direct plan-node assertions post-drop: name from `transit_stop_names`, coord from
    // `node_loc` (stops are junctions). Equality + literal-name catches a silent vanish.
    for (stop, want) in [(stop_a, "Stop A"), (stop_b, "Stop B")] {
        let node_after = PlanNode::from_node_id(&g, stop).expect("plan node post-drop");
        assert!(
            format!("{node_after:?}").contains(want),
            "post-drop PlanNode must carry {want} (g-free transit_stop_names)"
        );
    }
    assert_eq!(
        format!("{node_before:?}"),
        format!("{:?}", PlanNode::from_node_id(&g, stop_a).unwrap()),
        "PlanNode must be byte-identical pre/post drop"
    );

    // `gtfsStops` is a separate live GraphQL query (not on the explain call graph), so
    // it needs its own post-drop assertion: it must not panic and must carry both stop
    // names with their coordinates from `transit_stop_names` + `node_loc`.
    let stops = g.gtfs_stops();
    assert!(
        stops.iter().any(|(_, name, lat, _, _)| name == "Stop A" && *lat != 0.0),
        "gtfs_stops must carry Stop A name + coord post-drop (g-free)"
    );
    assert!(
        stops.iter().any(|(_, name, _, _, _)| name == "Stop B"),
        "gtfs_stops must carry Stop B name post-drop (g-free)"
    );
}

/// ENUMERATION (exploratory, ignored): drop `g`, then attempt EVERY junction-routed
/// mode inside `catch_unwind` so every remaining `g.nodes`/`g.edges` read surfaces at
/// once as a panic with its location — converting the "which paths still read g?"
/// unknown into a complete list before the mechanical g-free work. Prints a table; does
/// not assert. Run: `cargo test --test graph_tests t4_enumerate_g_reads -- --ignored --nocapture`.
#[test]
#[ignore]
fn t4_enumerate_g_reads_after_drop() {
    use maas_rs::structures::contraction::ContractedGraph;
    use std::panic::{self, AssertUnwindSafe};

    let (mut g, origin, dest) = contraction_t2_graph();
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "arrays dropped");

    // Silence the default panic printer so the table is readable; capture the location.
    use std::sync::{Arc, Mutex};
    let loc: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let loc_h = loc.clone();
    panic::set_hook(Box::new(move |info| {
        if let Some(l) = info.location() {
            *loc_h.lock().unwrap() = format!("{}:{}", l.file(), l.line());
        }
    }));
    let run = |label: &str, f: &dyn Fn() -> usize| {
        let res = panic::catch_unwind(AssertUnwindSafe(f));
        match res {
            Ok(n) => println!("  OK    {label:24} -> {n} plan(s)"),
            Err(e) => {
                let msg = e
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| e.downcast_ref::<&str>().copied())
                    .unwrap_or("<non-string panic>");
                println!("  PANIC {label:24} -> {msg}  @ {}", loc.lock().unwrap());
            }
        }
    };

    println!("=== g-read enumeration (junction-routed, post-drop) ===");
    for (label, am) in [
        ("walk", ActiveModes::new(&[Mode::Walk])),
        ("bike", ActiveModes::new(&[Mode::Bike])),
        ("car", ActiveModes::new(&[Mode::Car])),
    ] {
        run(label, &|| {
            g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &am).len()
        });
    }
    run("walk-transit (raptor)", &|| {
        g.raptor(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60).len()
    });
    run("car-drop-off", &|| {
        let am = ActiveModes::new(&[Mode::CarDropOff]);
        g.raptor_modes(origin, dest, 8 * 3600, 0, 0x7F, 10 * 60, &am).len()
    });
    let _ = panic::take_hook();
}

/// DROP GATE for transit plans: loads the real `graph_on.bin`, mirrors production
/// startup (flag on + re-bake), routes the cutover OD pairs ONCE with g present
/// (baseline), then `drop_full_node_arrays()` and routes them AGAIN — asserting no
/// panic AND byte-identical plans. This is the oracle for re-enabling the P3f memory
/// drop: it exercises real transit access/egress walk-leg ENRICHMENT (the surface the
/// junction-endpoint oracles never reach), which still reads `g` via `path_edges`.
///   cargo test --release --test graph_tests transit_enrich_drop_gate -- --ignored --nocapture
#[test]
#[ignore]
fn transit_enrich_drop_gate() {
    use chrono::{NaiveDate, NaiveTime};
    use maas_rs::routing::routing_raptor::{route, RouteQuery};
    use maas_rs::services::persistence::load_graph;
    use maas_rs::structures::{Config, RealtimeIndex};

    use maas_rs::structures::Mode;

    let mut g = load_graph("graph_on.bin").expect("graph_on.bin");
    let config = Config::load("config.yaml").expect("config.yaml");
    maas_rs::services::build::apply_routing_defaults(
        &mut g,
        &config.default_routing,
        &config.build.output,
    );

    let rt = RealtimeIndex::new();
    let date = NaiveDate::from_ymd_opt(2026, 6, 26).unwrap();
    let time = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
    let ods = [
        (50.846, 4.352, 50.881, 4.717),
        (50.880, 4.702, 50.846, 4.352),
        (50.860, 4.360, 50.900, 4.480),
        (50.821, 4.392, 50.901, 4.484),
        (51.035, 3.710, 51.210, 4.416),
    ];
    let q = |fl, fg, tl, tg, modes: Option<Vec<Mode>>| RouteQuery {
        from_lat: fl, from_lng: fg, to_lat: tl, to_lng: tg,
        date, time,
        window_minutes: None, min_access_secs: None, arrival_slack_secs: None, unrestricted_transfers: None, use_cch_access: None,
        reliability_bucket_edges: None, modes, bike_profile: None,
        terminal_deadline: false,
        onboard_origin: None,
        from_station_id: None,
        to_station_id: None,
        profile_latency: None,
        fare_profile: None,
    };

    let before: Vec<_> = ods
        .iter()
        .map(|&(fl, fg, tl, tg)| route(&g, &q(fl, fg, tl, tg, None), &rt).expect("pre-drop route"))
        .collect();

    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "g dropped");

    for (i, &(fl, fg, tl, tg)) in ods.iter().enumerate() {
        let after = route(&g, &q(fl, fg, tl, tg, None), &rt).expect("post-drop route must not error");
        assert_eq!(
            after.len(), before[i].len(),
            "OD {}: plan count changed post-drop ({} -> {})", i + 1, before[i].len(), after.len()
        );
        for (b, a) in before[i].iter().zip(&after) {
            assert_eq!(a.end, b.end, "OD {}: arrival changed post-drop", i + 1);
            assert_eq!(a.legs.len(), b.legs.len(), "OD {}: leg count changed post-drop", i + 1);
        }
        eprintln!("OD {}: {} plans identical pre/post drop", i + 1, after.len());
    }

    // Multi-mode no-panic check post-drop: bike-to-transit and park&ride exercise the
    // bike/car access + enrichment paths the default walk-transit query never touches.
    for (label, modes) in [
        ("bike-to-transit", vec![Mode::WalkTransit, Mode::BikeToTransit]),
        ("park&ride", vec![Mode::WalkTransit, Mode::CarDropOff]),
    ] {
        for (i, &(fl, fg, tl, tg)) in ods.iter().enumerate() {
            let _ = route(&g, &q(fl, fg, tl, tg, Some(modes.clone())), &rt)
                .unwrap_or_else(|e| panic!("post-drop {label} OD {} errored: {e:?}", i + 1));
        }
        eprintln!("{label}: 5 ODs routed post-drop, no panic");
    }
}

/// Master synthetic oracle: proves the entire served query surface (all 6 modes,
/// route_explain, gtfs_stops) is g-free after drop_full_node_arrays() on the
/// contracted synthetic fixture. Runs in-suite without graph_on.bin.
///
/// GATE RESULT (2026-06-26): Walk/Bike/Car/WalkTransit/CarDropOff are byte-identical
/// pre/post drop. BikeToTransit diverges (plan end 33262→32879): the bike-access-leg
/// enrichment still reads g.edges via path_edges. Task 3 fixes this path.
#[test]
fn all_modes_drop_gate_identical() {
    use chrono::{NaiveDate, NaiveTime};
    use maas_rs::routing::routing_raptor::route_explain;
    use maas_rs::structures::contraction::ContractedGraph;

    let (mut g, _origin, _dest) = contraction_t2_graph();
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();

    let rt = RealtimeIndex::new();
    let date = NaiveDate::from_ymd_opt(2026, 6, 23).unwrap();
    let time = NaiveTime::from_hms_opt(8, 50, 0).unwrap();

    let q = |modes: Option<Vec<Mode>>| RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.033,
        date,
        time,
        window_minutes: None,
        min_access_secs: Some(600),
        arrival_slack_secs: None,
        unrestricted_transfers: None,
        use_cch_access: None,
        reliability_bucket_edges: None,
        modes,
        bike_profile: None,
        terminal_deadline: false,
        onboard_origin: None,
        from_station_id: None,
        to_station_id: None,
        profile_latency: None,
        fare_profile: None,
    };

    let all_modes = [
        vec![Mode::Walk],
        vec![Mode::Bike],
        vec![Mode::Car],
        vec![Mode::WalkTransit],
        vec![Mode::BikeToTransit],
        vec![Mode::CarDropOff],
    ];

    let before: Vec<Vec<_>> = all_modes
        .iter()
        .map(|m| route(&g, &q(Some(m.clone())), &rt).expect("pre-drop route must succeed"))
        .collect();
    let before_explain = route_explain(&g, &q(None), &rt);
    let before_stops = g.gtfs_stops();

    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "node arrays must be empty after drop");

    fn leg_geometry(leg: &PlanLeg) -> &Vec<maas_rs::structures::plan::PlanCoordinate> {
        match leg {
            PlanLeg::Transit(l) => &l.geometry,
            PlanLeg::Walk(l) => &l.geometry,
        }
    }

    let mut diverged: Vec<String> = Vec::new();
    for (i, m) in all_modes.iter().enumerate() {
        let after = route(&g, &q(Some(m.clone())), &rt)
            .unwrap_or_else(|e| panic!("mode {m:?} errored post-drop: {e:?}"));
        if after.len() != before[i].len() {
            diverged.push(format!("{m:?} plan count {} -> {}", before[i].len(), after.len()));
            continue;
        }
        for (p, (b, a)) in before[i].iter().zip(&after).enumerate() {
            if a.end != b.end {
                diverged.push(format!("{m:?} plan {p} end {} -> {}", b.end, a.end));
            }
            if a.legs.len() != b.legs.len() {
                diverged.push(format!(
                    "{m:?} plan {p} leg count {} -> {}",
                    b.legs.len(),
                    a.legs.len()
                ));
            } else {
                for (j, (bl, al)) in b.legs.iter().zip(&a.legs).enumerate() {
                    let bg = leg_geometry(bl);
                    let ag = leg_geometry(al);
                    if format!("{bg:?}") != format!("{ag:?}") {
                        diverged.push(format!("{m:?} plan {p} leg {j} geometry differs"));
                    }
                }
            }
        }
    }

    let after_explain = route_explain(&g, &q(None), &rt);
    if format!("{:?}", before_explain) != format!("{:?}", after_explain) {
        diverged.push("route_explain diverged".to_string());
    }

    let after_stops = g.gtfs_stops();
    if format!("{:?}", before_stops) != format!("{:?}", after_stops) {
        diverged.push("gtfs_stops diverged".to_string());
    }

    assert!(
        diverged.is_empty(),
        "post-drop g-reads detected (Task 3 fixes these): {:?}",
        diverged
    );
}

/// P3f drop gate: `gtfs_agencies_with_routes()` reads only `raptor.*` so it must
/// be g-free. Assert it returns byte-identical output pre/post `drop_full_node_arrays()`.
/// Also asserts the function returns the agency name + its route, so the test is not
/// vacuous: the fixture adds one agency before the drop.
#[test]
fn gtfs_agencies_drop_gate_identical() {
    use maas_rs::ingestion::gtfs::AgencyInfo;
    use maas_rs::structures::contraction::ContractedGraph;

    let (mut g, _origin, _dest) = contraction_t2_graph();
    g.add_transit_agencies(vec![AgencyInfo {
        name: "TestBus".into(),
        url: "https://test.example".into(),
        timezone: "Europe/Brussels".into(),
    }]);
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();

    let before = g.gtfs_agencies_with_routes();
    assert!(
        before.iter().any(|(_, name, _, routes)| name == "TestBus" && !routes.is_empty()),
        "pre-drop agencies must contain TestBus with at least one route"
    );

    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "g arrays dropped");

    let after = g.gtfs_agencies_with_routes();
    assert_eq!(
        format!("{:?}", before),
        format!("{:?}", after),
        "gtfs_agencies_with_routes must be byte-identical pre/post drop"
    );
}

/// Unit test for the flag-less `finalize_contraction` guard.
/// Exercises all three cases: happy-path drop, idempotent re-call, and the
/// no-contracted-graph rebuild signal (Err).
#[test]
fn finalize_contraction_guard() {
    use maas_rs::services::build::finalize_contraction;
    use maas_rs::structures::contraction::ContractedGraph;

    // (a) Happy path: contracted present + nodes present.
    // finalize_contraction must drop interior arrays and return Ok.
    {
        let (mut g, _, _) = contraction_t2_graph();
        let node_count_before = g.node_count();
        assert!(node_count_before > 0, "fixture must have nodes before drop");
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);

        let result = finalize_contraction(&mut g);
        assert!(result.is_ok(), "happy-path finalize must return Ok; got {:?}", result);
        assert_eq!(g.node_count(), 0, "finalize must drop interior arrays");
        assert!(g.contracted.is_some(), "contracted graph must remain after drop");
    }

    // (b) Idempotent: contracted present + already dropped.
    // Calling finalize_contraction again must return Ok and leave node_count at 0.
    {
        let (mut g, _, _) = contraction_t2_graph();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        finalize_contraction(&mut g).expect("first call");
        assert_eq!(g.node_count(), 0, "first call dropped");

        let result = finalize_contraction(&mut g);
        assert!(result.is_ok(), "idempotent second call must return Ok; got {:?}", result);
        assert_eq!(g.node_count(), 0, "node_count must remain 0 after idempotent call");
    }

    // (c) No contracted graph: finalize_contraction must return Err — the rebuild signal
    // (such a graph cannot serve contraction-only routing).
    {
        let (mut g, _, _) = contraction_t2_graph();
        assert!(g.contracted.is_none(), "fixture has no contracted graph");
        let count_before = g.node_count();
        assert!(count_before > 0, "fixture must have nodes");

        let result = finalize_contraction(&mut g);
        assert!(
            result.is_err(),
            "graph with no contracted graph must return Err; got {:?}", result
        );
        assert_eq!(g.node_count(), count_before, "nodes untouched on Err");
    }
}

// ── Station backups (same-station cross-line alternatives) ────────────────────

/// Two routes serving the SAME boarding→alighting pair (SA → SB), plus a
/// same-route sibling trip and a decoy that leaves SA but never reaches SB:
///   Bus  (route 0): T2 dep 08:50 arr 09:05  (sibling, before reference)
///   Bus  (route 0): T0 dep 09:00 arr 09:15  (the reference trip)
///   Tram (route 1): T1 dep 09:10 arr 09:30  (cross-line, after reference)
///   Bus  (route 0): T3 dep 09:05 SA → SX    (decoy, never reaches SB)
fn station_backups_graph() -> Graph {
    let mut g = Graph::new();

    let sa = g.add_node(transit_stop("SA", 50.000, 4.000));
    let sb = g.add_node(transit_stop("SB", 50.000, 4.050));
    let sx = g.add_node(transit_stop("SX", 50.000, 4.090));

    g.add_transit_services(vec![all_days_service()]); // ServiceId(0)

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

    // TripId(0)=T0 bus reference, (1)=T1 tram, (2)=T2 bus sibling, (3)=T3 bus decoy.
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
    ]);
    g.add_transit_trip_ids(vec!["T0".into(), "T1".into(), "T2".into(), "T3".into()]);

    let push_pattern = |g: &mut Graph, route: RouteId, stops: &[NodeID], trips: &[TripId], times: &[(u32, u32)]| {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(stops);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: stops.len() });
        let ts = g.transit_pattern_trips_len();
        for &t in trips {
            g.push_transit_pattern_trip(t);
        }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: trips.len() });
        let sts = g.transit_pattern_stop_times_len();
        for &(arrival, departure) in times {
            g.push_transit_pattern_stop_time(StopTime { arrival, departure, ..Default::default() });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: times.len() });
        g.push_transit_pattern(PatternInfo { route, num_trips: trips.len() as u32 });
    };

    // Pattern 0: Bus SA→SB, trips [T2 (08:50), T0 (09:00)] column-major.
    push_pattern(
        &mut g,
        RouteId(0),
        &[sa, sb],
        &[TripId(2), TripId(0)],
        &[(31800, 31800), (32400, 32400), (32700, 32700), (33300, 33300)],
    );
    // Pattern 1: Tram SA→SB, trip [T1 (09:10)].
    push_pattern(
        &mut g,
        RouteId(1),
        &[sa, sb],
        &[TripId(1)],
        &[(33000, 33000), (34200, 34200)],
    );
    // Pattern 2: Bus SA→SX decoy, trip [T3 (09:05)].
    push_pattern(
        &mut g,
        RouteId(0),
        &[sa, sx],
        &[TripId(3)],
        &[(32700, 32700), (33000, 33000)],
    );

    g.build_raptor_index();
    g
}

#[test]
fn station_backups_returns_cross_line_same_destination() {
    let g = station_backups_graph();
    let board = g.stop_index_of("SA").expect("SA resolves");
    let alight = g.stop_index_of("SB").expect("SB resolves");

    // Reference = T0 (bus, 09:00). Ask for plenty before and after.
    let backups = g.station_backups(TripId(0), board, alight, 5, 5, 1, 0x01);

    // Exactly the sibling (T2) and the cross-line tram (T1); decoy T3 and the
    // reference T0 are both excluded.
    let trips: Vec<_> = backups.iter().map(|b| b.trip).collect();
    assert_eq!(trips, vec![TripId(2), TripId(1)], "chronological by scheduled departure");
    assert!(!trips.contains(&TripId(0)), "reference trip excluded");
    assert!(!trips.contains(&TripId(3)), "decoy not reaching SB excluded");

    // Sibling: before the reference, same route, scheduled 08:50 → 09:05.
    assert_eq!(backups[0].trip, TripId(2));
    assert!(backups[0].same_route);
    assert_eq!(backups[0].scheduled_departure, 31800);
    assert_eq!(backups[0].scheduled_arrival, 32700);
    // Cross-line tram: after the reference, different route, 09:10 → 09:30.
    assert_eq!(backups[1].trip, TripId(1));
    assert!(!backups[1].same_route);
    assert_eq!(backups[1].scheduled_departure, 33000);
    assert_eq!(backups[1].scheduled_arrival, 34200);
}

#[test]
fn station_backups_split_counts_and_unknown_handles() {
    let g = station_backups_graph();
    let board = g.stop_index_of("SA").expect("SA resolves");
    let alight = g.stop_index_of("SB").expect("SB resolves");

    // After-only: just the later tram.
    let after = g.station_backups(TripId(0), board, alight, 0, 5, 1, 0x01);
    assert_eq!(after.iter().map(|b| b.trip).collect::<Vec<_>>(), vec![TripId(1)]);

    // Before-only: just the earlier sibling bus.
    let before = g.station_backups(TripId(0), board, alight, 5, 0, 1, 0x01);
    assert_eq!(before.iter().map(|b| b.trip).collect::<Vec<_>>(), vec![TripId(2)]);

    // A trip that does not serve board→alight (the decoy) yields nothing, no panic.
    assert!(g.station_backups(TripId(3), board, alight, 5, 5, 1, 0x01).is_empty());
    // An out-of-range trip id likewise resolves to empty rather than panicking.
    assert!(g.station_backups(TripId(99), board, alight, 5, 5, 1, 0x01).is_empty());
}

// ── Onboard partial-requery (Phase 2b) ────────────────────────────────────────

/// A bus-mode delay CDF: ~62% on time at margin 0, rising with slack. Lets a
/// downstream transfer reliability land below the top reliability bucket.
fn onboard_bus_cdf() -> DelayCDF {
    DelayCDF {
        bins: vec![
            (-120, 0.10),
            (0, 0.62),
            (120, 0.80),
            (300, 0.95),
            (600, 1.00),
        ],
    }
}

/// A synthetic graph for the onboard partial-requery:
///   Bus trip X (TripId 0, pattern 0): S0 -> S1 -> S2 -> S3
///     S0 09:00, S1 09:05, S2 09:10, S3 09:30
///   Bus trip X' (TripId 2, pattern 0, a later departure of the SAME pattern):
///     S0 09:08:20, S1 09:10, S2 09:15, S3 09:30 — departs S1 after the boarded
///     trip yet still reaches S3 by 09:30, so it is a genuine swap candidate the
///     backward-tighten step would pick for leg[0] were the onboard guard absent.
///   Tram trip Y (TripId 1, pattern 1): S2 -> S4  (shared boarding stop S2)
///     S2 dep 09:11, S4 arr 09:15
/// Destination D snaps to `osm_dest`, a short walk from both S3 (60 s) and S4
/// (75 s). S2 is ~658 s away from `osm_dest`, beyond the egress radius used by the
/// stay-on/transfer tests; the alight-mid-trip test instead targets `osm_s2`,
/// reachable only by alighting at the intermediate stop S2.
fn onboard_graph() -> (Graph, NodeID, LatLng) {
    let mut g = Graph::new();

    let osm_s2 = g.add_node(osm_node("osm_s2", 50.000, 4.020));
    let osm_dest = g.add_node(osm_node("osm_dest", 50.000, 4.030));
    let osm_stub = g.add_node(osm_node("osm_stub", 50.001, 4.030));

    let s0 = g.add_node(transit_stop("S0", 50.000, 4.000));
    let s1 = g.add_node(transit_stop("S1", 50.000, 4.010));
    let s2 = g.add_node(transit_stop("S2", 50.000, 4.020));
    let s3 = g.add_node(transit_stop("S3", 50.000, 4.030));
    let s4 = g.add_node(transit_stop("S4", 50.000, 4.031));

    let street = |g: &mut Graph, a: NodeID, b: NodeID, m: usize| {
        g.add_edge(a, street_edge(a, b, m));
        g.add_edge(b, street_edge(b, a, m));
    };
    street(&mut g, osm_s2, osm_dest, 717);
    street(&mut g, osm_dest, osm_stub, 100);

    let snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (o, d) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                o,
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
                }),
            );
        }
    };
    snap(&mut g, s2, osm_s2, 72);
    snap(&mut g, s3, osm_dest, 72);
    snap(&mut g, s4, osm_dest, 90);

    g.add_transit_services(vec![all_days_service()]); // ServiceId(0)
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "X".into(),
            route_long_name: "Bus X".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "Y".into(),
            route_long_name: "Tram Y".into(),
            route_type: RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(0) = bus X
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(1) = tram Y
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(2) = bus X, later same-pattern departure (swap candidate)
    ]);

    g.add_transit_departures(vec![
        // S0->S1 hop: trip 0 then trip 2 (both bus X, pattern 0).
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 32400,
            arrival: 32700,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 32900,
            arrival: 33000,
            service_id: ServiceId(0),
        },
        // S1->S2 hop.
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: 32700,
            arrival: 33000,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: 33000,
            arrival: 33300,
            service_id: ServiceId(0),
        },
        // S2->S3 hop.
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 2,
            destination_stop_sequence: 3,
            departure: 33000,
            arrival: 34200,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 2,
            destination_stop_sequence: 3,
            departure: 33300,
            arrival: 34200,
            service_id: ServiceId(0),
        },
        // Tram Y, pattern 1.
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 33060,
            arrival: 33300,
            service_id: ServiceId(0),
        },
    ]);
    g.add_edge(
        s0,
        EdgeData::Transit(TransitEdgeData {
            origin: s0,
            destination: s1,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 2 },
            length: 718,
        }),
    );
    g.add_edge(
        s1,
        EdgeData::Transit(TransitEdgeData {
            origin: s1,
            destination: s2,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 2, len: 2 },
            length: 718,
        }),
    );
    g.add_edge(
        s2,
        EdgeData::Transit(TransitEdgeData {
            origin: s2,
            destination: s3,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 4, len: 2 },
            length: 718,
        }),
    );
    g.add_edge(
        s2,
        EdgeData::Transit(TransitEdgeData {
            origin: s2,
            destination: s4,
            route_id: RouteId(1),
            timetable_segment: TimetableSegment { start: 6, len: 1 },
            length: 80,
        }),
    );

    // Pattern 0: Bus X, [S0,S1,S2,S3], 2 trips. Column-major: stop_pos * n_trips + t.
    // Trip 0 (boarded) and trip 2 (a later same-pattern departure that reaches S3
    // no later than trip 0, so the backward-tighten step WOULD swap leg[0] to it if
    // the onboard guard were absent).
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[s0, s1, s2, s3]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 4 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });

        let sts = g.transit_pattern_stop_times_len();
        // (S0, S1, S2, S3) × (trip 0, trip 2), column-major.
        for t in [
            32400u32, 32900, // S0
            32700, 33000, // S1
            33000, 33300, // S2
            34200, 34200, // S3
        ] {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 8 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 2,
        });
    }

    // Pattern 1: Tram Y, [S2,S4], 1 trip.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[s2, s4]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 33060,
            departure: 33060,
        ..Default::default()
        }); // S2, trip 1
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 33300,
            departure: 33300,
        ..Default::default()
        }); // S4, trip 1
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(1),
            num_trips: 1,
        });
    }

    let mut models = HashMap::new();
    models.insert(RouteType::Bus, onboard_bus_cdf());
    g.set_transit_delay_models(models);
    g.set_reliability_bucket_edges(vec![0.50, 0.80, 0.95]);

    g.build_raptor_index();
    enable_contraction(&mut g);

    let dest_ll = LatLng {
        latitude: 50.000,
        longitude: 4.030,
    };
    (g, osm_dest, dest_ll)
}

/// Run the onboard driver from `current_pos` on bus X (pattern 0, within-trip 0)
/// to `osm_dest`, with the given realtime index and egress radius.
fn onboard_plans(
    g: &Graph,
    osm_dest: NodeID,
    dest_ll: LatLng,
    current_pos: u32,
    rt: &RealtimeIndex,
    egress_secs: u32,
) -> Vec<maas_rs::structures::plan::Plan> {
    let ride: OnboardRide = g.build_onboard_ride(0, 0, current_pos, rt);
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = QueryEndpoints {
        origin: dest_ll,
        destination: dest_ll,
        origin_station: None,
        destination_station: None,
    };
    let am = ActiveModes::new(&[Mode::WalkTransit]);
    g.raptor_onboard_tuned_rt_modes_ep(
        &ride,
        osm_dest,
        9663,
        0x01,
        egress_secs,
        &buckets,
        g.raptor.arrival_slack_secs,
        rt,
        &am,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        Some(&ep),
    )
}

/// Test 5: the seed helper enumerates only downstream stops (`pos > current_pos`),
/// with realtime arrivals (scheduled + live delay), in monotonic non-decreasing order.
#[test]
fn onboard_seed_helper_enumerates_downstream_only() {
    let (g, _osm_dest, _dest_ll) = onboard_graph();

    let s2 = g.stop_index_of("S2").unwrap() as u32;
    let s3 = g.stop_index_of("S3").unwrap() as u32;
    let rt = RealtimeIndex::from_delays(0, [((TripId(0), s2), 120), ((TripId(0), s3), 120)]);

    let ride = g.build_onboard_ride(0, 0, 1, &rt);
    assert_eq!(ride.trip_id, TripId(0));
    assert_eq!(ride.current_pos, 1);
    assert_eq!(ride.route_type, Some(RouteType::Bus));

    assert_eq!(ride.seeds.len(), 2);
    assert_eq!(ride.seeds[0].alighted_at, 2);
    assert_eq!(ride.seeds[0].at_stop, s2);
    assert_eq!(ride.seeds[0].arrival, 33000 + 120);
    assert_eq!(ride.seeds[1].alighted_at, 3);
    assert_eq!(ride.seeds[1].at_stop, s3);
    assert_eq!(ride.seeds[1].arrival, 34200 + 120);
    assert!(ride.seeds[0].arrival <= ride.seeds[1].arrival);

    let ride2 = g.build_onboard_ride(0, 0, 2, &RealtimeIndex::new());
    assert_eq!(ride2.seeds.len(), 1);
    assert_eq!(ride2.seeds[0].alighted_at, 3);
}

/// Test 1 (the gate): one onboard query yields, in one shot, the stay-on plan
/// (exactly one transit leg = the boarded trip, no access walk) AND the
/// alight-and-transfer plan (boarded trip first, then a second transit leg).
#[test]
fn onboard_query_yields_stay_on_and_alight_transfer() {
    let (g, osm_dest, dest_ll) = onboard_graph();
    let rt = RealtimeIndex::new();
    let plans = onboard_plans(&g, osm_dest, dest_ll, 1, &rt, 600);

    assert!(!plans.is_empty(), "onboard query returned no plans");

    let transit_legs = |p: &maas_rs::structures::plan::Plan| -> Vec<TripId> {
        p.legs
            .iter()
            .filter_map(|l| match l {
                PlanLeg::Transit(t) => Some(t.trip_id),
                _ => None,
            })
            .collect()
    };
    let first_is_transit =
        |p: &maas_rs::structures::plan::Plan| matches!(p.legs.first(), Some(PlanLeg::Transit(_)));

    // STAY-ON: exactly one transit leg, the boarded trip, first leg is the onboard
    // ride (no access walk), riding to the trip's FINAL stop S3, then an egress walk.
    let stay_on = plans
        .iter()
        .find(|p| transit_legs(p) == vec![TripId(0)])
        .expect("a stay-on plan with exactly the boarded trip as its single transit leg");
    assert!(
        first_is_transit(stay_on),
        "stay-on first leg must be the onboard ride, not an access walk"
    );
    let PlanLeg::Transit(stay_leg) = &stay_on.legs[0] else {
        panic!("stay-on leg[0] must be transit");
    };
    assert_eq!(
        g.stop_id_of_node(stay_leg.to.node_id),
        Some("S3"),
        "stay-on rides to the trip's final stop S3 (distinct from the intermediate-alight case)"
    );
    assert!(
        matches!(stay_on.legs.last(), Some(PlanLeg::Walk(_))),
        "stay-on must carry an egress walk"
    );

    // ALIGHT+TRANSFER: the boarded trip FIRST, then a second transit leg (tram Y).
    let transfer = plans
        .iter()
        .find(|p| transit_legs(p) == vec![TripId(0), TripId(1)])
        .expect("an alight-and-transfer plan: onboard ride then a second transit leg");
    assert!(
        first_is_transit(transfer),
        "transfer plan first leg must be the onboard ride"
    );
}

/// Test 1b: a genuinely distinct ALIGHT-MID-TRIP + WALK outcome. Targeting `osm_s2`
/// (next to the intermediate stop S2) instead of `osm_dest`, the final stop S3 is
/// ~789 s away (beyond the egress radius) so the only egress is to alight at S2 —
/// an intermediate pattern stop (pos 2 < final pos 3) — and walk. The result is a
/// single transit leg of the boarded trip whose alight stop is S2, then a walk leg.
#[test]
fn onboard_alight_midtrip_then_walk() {
    let (g, _osm_dest, _dest_ll) = onboard_graph();
    let osm_s2 = g.nearest_node(50.000, 4.020).expect("osm node near S2");
    let s2_ll = LatLng {
        latitude: 50.000,
        longitude: 4.020,
    };
    let rt = RealtimeIndex::new();
    let plans = onboard_plans(&g, osm_s2, s2_ll, 1, &rt, 600);

    assert!(!plans.is_empty(), "onboard query to osm_s2 returned no plans");

    let alight_walk = plans
        .iter()
        .find(|p| {
            matches!(p.legs.first(), Some(PlanLeg::Transit(t)) if t.trip_id == TripId(0))
                && matches!(p.legs.last(), Some(PlanLeg::Walk(_)))
        })
        .expect("an alight-mid-trip + walk plan: boarded trip then an egress walk");

    let transit_count = alight_walk
        .legs
        .iter()
        .filter(|l| matches!(l, PlanLeg::Transit(_)))
        .count();
    assert!(transit_count > 0, "alight+walk plan must keep its transit leg");
    assert_eq!(transit_count, 1, "alight+walk plan rides exactly the boarded trip");

    let PlanLeg::Transit(leg0) = &alight_walk.legs[0] else {
        panic!("leg[0] must be the onboard transit ride");
    };
    assert_eq!(
        g.stop_id_of_node(leg0.to.node_id),
        Some("S2"),
        "alight must be at the INTERMEDIATE stop S2, not the final stop S3"
    );
    let pat_stops = g.get_pattern_stop_nodes(0);
    let s2_pos = pat_stops.iter().position(|&n| n == leg0.to.node_id).unwrap();
    assert!(
        s2_pos < pat_stops.len() - 1,
        "S2 must be an intermediate stop of pattern 0 (pos {s2_pos} < final {})",
        pat_stops.len() - 1
    );
}

/// Test 2: the backward-tighten guard holds — leg[0] is not swapped to a later
/// same-pattern departure. Its trip is the boarded trip and its start equals the
/// realtime departure at the current-position stop.
///
/// This is load-bearing because pattern 0 now has a genuine swap candidate: trip 2
/// departs S1 at 09:10 (after the boarded trip's 09:05 departure) and still reaches
/// S3 by 09:30, within the backward label at S3. With the guard removed,
/// `tighten_with_backward_labels` swaps leg[0] to trip 2 (start 33000), so this
/// test fails; with the guard present leg[0] keeps trip 0 (realtime start 32880).
#[test]
fn onboard_leg0_not_swapped_keeps_realtime_departure() {
    let (g, osm_dest, dest_ll) = onboard_graph();

    let s1 = g.stop_index_of("S1").unwrap() as u32;
    let s2 = g.stop_index_of("S2").unwrap() as u32;
    let s3 = g.stop_index_of("S3").unwrap() as u32;
    let rt = RealtimeIndex::from_delays(
        0,
        [
            ((TripId(0), s1), 180),
            ((TripId(0), s2), 180),
            ((TripId(0), s3), 180),
        ],
    );
    let plans = onboard_plans(&g, osm_dest, dest_ll, 1, &rt, 600);

    let stay_on = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 1
        })
        .expect("a single-transit stay-on plan");
    let PlanLeg::Transit(leg0) = &stay_on.legs[0] else {
        panic!("leg[0] must be the onboard transit ride");
    };
    assert_eq!(leg0.trip_id, TripId(0), "leg[0] stays the boarded trip");
    assert_eq!(
        leg0.start,
        32700 + 180,
        "leg[0] keeps the realtime departure at S1 (not swapped to a later trip)"
    );
}

/// D1: when the boarded (onboard, first) trip is reported CANCELED, cancellation
/// outranks any stale per-stop delay — the leg keeps its SCHEDULED times and is
/// NOT flagged realtime (mirroring live_refresh). The boarded trip itself is not
/// excluded (it is the user's reality). Injecting a +180s delay AND a
/// cancellation together proves the cancellation wins over the delay.
#[test]
fn onboard_canceled_boarded_trip_keeps_scheduled_times() {
    let (g, osm_dest, dest_ll) = onboard_graph();

    let s1 = g.stop_index_of("S1").unwrap() as u32;
    let s2 = g.stop_index_of("S2").unwrap() as u32;
    let s3 = g.stop_index_of("S3").unwrap() as u32;
    let rt = RealtimeIndex::from_updates(
        0,
        [
            ((TripId(0), s1), 180),
            ((TripId(0), s2), 180),
            ((TripId(0), s3), 180),
        ],
        [TripId(0)],
    );
    let plans = onboard_plans(&g, osm_dest, dest_ll, 1, &rt, 600);

    let stay_on = plans
        .iter()
        .find(|p| {
            p.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                == 1
        })
        .expect("a stay-on plan survives even when the boarded trip is canceled");
    let PlanLeg::Transit(leg0) = &stay_on.legs[0] else {
        panic!("leg[0] must be the onboard transit ride");
    };
    assert_eq!(leg0.trip_id, TripId(0), "leg[0] is still the boarded trip");
    assert_eq!(
        leg0.start, 32700,
        "canceled boarded leg keeps its SCHEDULED departure (delay ignored)"
    );
    assert_eq!(
        leg0.start, leg0.scheduled_start,
        "effective == scheduled for a canceled onboard leg"
    );
    assert!(
        !leg0.realtime,
        "a canceled boarded leg must not be flagged realtime"
    );
}

/// Test 3 (regression): the onboard path does not perturb the normal lat/lng
/// route. `route()` with `onboard_origin = None` over a fixed OD returns plans
/// byte-identical run-to-run (and the existing raptor suite is the wider net).
#[test]
fn lat_lng_route_unchanged_by_onboard_path() {
    let (g, _osm_origin, _osm_dest) = two_route_raptor_graph();
    let mk = || RouteQuery {
        from_lat: 50.000,
        from_lng: 4.000,
        to_lat: 50.000,
        to_lng: 4.041,
        date: chrono::NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
        time: chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
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
    };
    let dbg =
        |ps: &[maas_rs::structures::plan::Plan]| ps.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>();
    let a = route(&g, &mk(), &RealtimeIndex::new()).expect("plans");
    let b = route(&g, &mk(), &RealtimeIndex::new()).expect("plans");
    assert!(!a.is_empty(), "fixed OD must produce at least one plan");
    assert_eq!(
        dbg(&a),
        dbg(&b),
        "onboard_origin=None must leave the lat/lng route byte-identical"
    );

    // Concrete signature for this fixed OD: a single 5-leg plan riding trips 0 then
    // 1, departing 32300 and arriving 35100 (multi-objective street routing is now
    // unconditional, so the egress leg is always rebuilt from its Pareto front). A
    // systematic shift in the normal path (which the run-to-run check alone cannot
    // catch) would break this.
    let sig: Vec<(usize, Vec<u32>, u32, u32)> = a
        .iter()
        .map(|p| {
            let trips: Vec<u32> = p
                .legs
                .iter()
                .filter_map(|l| match l {
                    PlanLeg::Transit(t) => Some(t.trip_id.0),
                    _ => None,
                })
                .collect();
            (p.legs.len(), trips, p.start, p.end)
        })
        .collect();
    assert_eq!(sig, vec![(5, vec![0, 1], 32300, 35100)]);
}

// ── Stage B1: platform connector-coverage measurement ──────────────────────────

use maas_rs::ingestion::osm::{OsmPlatform, PlatformIndex};
use maas_rs::structures::Connector;

fn foot_pair(g: &mut Graph, a: NodeID, b: NodeID, len: usize) {
    g.add_edge(a, street_edge(a, b, len));
    g.add_edge(b, street_edge(b, a, len));
}

/// Platform reachable from a ground street node only by crossing a stairs
/// connector → counted as reachable_via_connector.
#[test]
fn connector_reach_counts_platform_via_stairs() {
    let mut g = Graph::new();
    let gnd = g.add_node(osm_node("g", 50.000, 4.000));
    let s = g.add_node(osm_node("s", 50.0001, 4.0001));
    let p1 = g.add_node(osm_node("p1", 50.0002, 4.0002));
    let p2 = g.add_node(osm_node("p2", 50.0002, 4.0003));
    foot_pair(&mut g, gnd, s, 30);
    foot_pair(&mut g, s, p1, 12);
    foot_pair(&mut g, p1, p2, 40);

    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    levels.insert(p2, 1i16);
    let mut connectors = HashMap::new();
    connectors.insert((s, p1), Connector::Steps);
    connectors.insert((p1, s), Connector::Steps);
    g.set_osm_level_data(levels, connectors);

    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: g.get_node(p1).unwrap().loc(),
        node_ids: vec![p1, p2],
    }]));

    let plat_nodes = g.all_platform_nodes();
    let reach =
        g.platform_connector_reach(&[p1, p2], g.get_node(p1).unwrap().loc(), &plat_nodes, 500);
    assert!(reach.reachable_via_connector, "stairs path should be reachable");
    assert_eq!(reach.path_dist_m, Some(12.0));
}

/// A flat footway directly joining a level-1 platform node to a level-0 concourse
/// (the "teleport") is NOT a connector → must count as no_vertical_path.
#[test]
fn connector_reach_excludes_flat_teleport() {
    let mut g = Graph::new();
    let s = g.add_node(osm_node("s", 50.000, 4.000));
    let gnd = g.add_node(osm_node("g", 50.0001, 4.0));
    let p1 = g.add_node(osm_node("p1", 50.0002, 4.0002));
    let p2 = g.add_node(osm_node("p2", 50.0002, 4.0003));
    foot_pair(&mut g, gnd, s, 30);
    foot_pair(&mut g, s, p1, 12);
    foot_pair(&mut g, p1, p2, 40);

    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    levels.insert(p2, 1i16);
    g.set_osm_level_data(levels, HashMap::new());

    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: g.get_node(p1).unwrap().loc(),
        node_ids: vec![p1, p2],
    }]));

    let plat_nodes = g.all_platform_nodes();
    let reach =
        g.platform_connector_reach(&[p1, p2], g.get_node(p1).unwrap().loc(), &plat_nodes, 500);
    assert!(
        !reach.reachable_via_connector,
        "flat teleport must not count as vertical access"
    );
}

/// An isolated platform (no edge to any ground node) is not reachable.
#[test]
fn connector_reach_excludes_isolated_platform() {
    let mut g = Graph::new();
    let p1 = g.add_node(osm_node("p1", 50.0002, 4.0002));
    let p2 = g.add_node(osm_node("p2", 50.0002, 4.0003));
    foot_pair(&mut g, p1, p2, 40);

    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    levels.insert(p2, 1i16);
    g.set_osm_level_data(levels, HashMap::new());
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: g.get_node(p1).unwrap().loc(),
        node_ids: vec![p1, p2],
    }]));

    let plat_nodes = g.all_platform_nodes();
    let reach =
        g.platform_connector_reach(&[p1, p2], g.get_node(p1).unwrap().loc(), &plat_nodes, 500);
    assert!(!reach.reachable_via_connector);
    assert_eq!(reach.path_dist_m, None);
}

/// Beyond the distance budget the platform is not reached.
#[test]
fn connector_reach_respects_budget() {
    let mut g = Graph::new();
    let s = g.add_node(osm_node("s", 50.000, 4.000));
    let p1 = g.add_node(osm_node("p1", 50.0002, 4.0002));
    foot_pair(&mut g, s, p1, 1000);
    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    let mut connectors = HashMap::new();
    connectors.insert((s, p1), Connector::Steps);
    connectors.insert((p1, s), Connector::Steps);
    g.set_osm_level_data(levels, connectors);
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: g.get_node(p1).unwrap().loc(),
        node_ids: vec![p1],
    }]));
    let plat_nodes = g.all_platform_nodes();
    let reach = g.platform_connector_reach(&[p1], g.get_node(p1).unwrap().loc(), &plat_nodes, 100);
    assert!(!reach.reachable_via_connector);
}

/// Non-regression proxy: a snapped transit stop stays reachable; platform-only
/// nodes (excluded from snapping) don't change the before/after count.
#[test]
fn transit_stop_reachability_additive() {
    let mut g = Graph::new();
    let street = g.add_node(osm_node("st", 50.000, 4.000));
    let stop = g.add_node(transit_stop("Stop", 50.0001, 4.0001));
    g.add_edge(stop, street_edge(stop, street, 15));
    g.add_edge(street, street_edge(street, stop, 15));
    let p1 = g.add_node(osm_node("p1", 50.0005, 4.0005));
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: g.get_node(p1).unwrap().loc(),
        node_ids: vec![p1],
    }]));

    let plat_nodes = g.all_platform_nodes();
    let (total, after, before) = g.transit_stops_reachable(&plat_nodes);
    assert_eq!(total, 1);
    assert_eq!(after, 1);
    assert_eq!(before, 1, "platform additions must not drop reachability");
}

// ── Stage B2a: snap-relocation of platform-matched stops ────────────────────────

use maas_rs::ingestion::gtfs::relocate_matched_stop;

fn foot_neighbors(g: &Graph, a: NodeID) -> Vec<NodeID> {
    g.out_edges(a)
        .iter()
        .filter_map(|e| match e {
            EdgeData::Street(s) if s.foot => Some(s.destination),
            _ => None,
        })
        .collect()
}

fn foot_edge_len(g: &Graph, a: NodeID, b: NodeID) -> Option<usize> {
    g.out_edges(a).iter().find_map(|e| match e {
        EdgeData::Street(s) if s.foot && s.destination == b => Some(s.length),
        _ => None,
    })
}

/// (a) Matched stop whose platform has a real B1 stairs connector reachable from
/// `orig` via ground: stop relocates to the stairs-connected node, NO straight
/// fallback connector is added (the real path already guarantees reachability),
/// and the stop is accessible from orig via the ground→stairs→platform path.
#[test]
fn b2a_relocates_onto_platform_with_stairs() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let gnd = g.add_node(osm_node("gnd", 50.000, 4.0005));
    let p1 = g.add_node(osm_node("p1", 50.001, 4.0005));
    foot_pair(&mut g, orig, gnd, 40);
    foot_pair(&mut g, gnd, p1, 12); // the (only) stairs link to the platform

    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    let mut connectors = HashMap::new();
    connectors.insert((gnd, p1), Connector::Steps);
    connectors.insert((p1, gnd), Connector::Steps);
    g.set_osm_level_data(levels, connectors);

    let plat_loc = g.get_node(p1).unwrap().loc();
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["1".into()],
        level: Some(1.0),
        centroid: plat_loc,
        node_ids: vec![p1],
    }]));

    let stop_loc = LatLng {
        latitude: 50.001,
        longitude: 4.0005,
    };
    let stop = g.add_node(transit_stop("Platform 1", stop_loc.latitude, stop_loc.longitude));

    assert!(relocate_matched_stop(
        &mut g,
        stop,
        stop_loc,
        orig,
        Some("1"),
        None
    ));

    // Boarding happens at the platform: the stop's only foot neighbour is p1.
    assert_eq!(foot_neighbors(&g, stop), vec![p1]);
    // The stop anchor was moved onto the platform node and pinned to its storey.
    assert_eq!(g.get_node(stop).unwrap().loc().latitude, plat_loc.latitude);
    assert_eq!(g.node_level(stop), Some(1));
    // Real path exists (orig→gnd→stairs→p1) so NO straight fallback edge is added.
    assert!(
        foot_edge_len(&g, p1, orig).is_none(),
        "fallback p1→orig must NOT exist when a real path is reachable"
    );

    // The real foot path (ground → stairs → platform → stop) keeps the stop reachable.
    g.build_raptor_index();
    assert!(g.walk_dijkstra(orig, 99_999).contains_key(&stop));
}

/// (a2) Matched stop whose platform is reachable from orig via a FLAT GROUND path
/// (no connector): stop relocates to the ground-connected node, NO fallback connector
/// is added, and the stop is accessible via the flat path.
#[test]
fn b2a_ground_path_no_fallback() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let mid = g.add_node(osm_node("mid", 50.000, 4.0004));
    let p1 = g.add_node(osm_node("p1", 50.000, 4.0008));
    foot_pair(&mut g, orig, mid, 30);
    foot_pair(&mut g, mid, p1, 30); // flat ground path into the platform

    // Surface-level platform (no level tag, no connector).
    g.set_osm_level_data(HashMap::new(), HashMap::new());

    let plat_loc = g.get_node(p1).unwrap().loc();
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["4".into()],
        level: None,
        centroid: plat_loc,
        node_ids: vec![p1],
    }]));

    let stop_loc = plat_loc;
    let stop = g.add_node(transit_stop("Flat Platform", stop_loc.latitude, stop_loc.longitude));

    assert!(relocate_matched_stop(
        &mut g,
        stop,
        stop_loc,
        orig,
        Some("4"),
        None
    ));

    // Relocates to p1 (the reachable ground-connected node).
    assert_eq!(g.get_node(stop).unwrap().loc().latitude, plat_loc.latitude);
    // No straight fallback: the ground path already connects orig to p1.
    assert!(
        foot_edge_len(&g, p1, orig).is_none(),
        "fallback p1→orig must NOT exist when ground path is reachable"
    );
    assert!(
        foot_edge_len(&g, orig, p1).is_none(),
        "fallback orig→p1 must NOT exist when ground path is reachable"
    );

    // Stop is reachable via the flat path.
    g.build_raptor_index();
    assert!(g.walk_dijkstra(orig, 99_999).contains_key(&stop));
}

/// (b) Matched stop whose platform has NO mapped connector: the re-priced fallback
/// connector to the ORIGINAL street node exists with cost > 0, and the stop stays
/// reachable from that original node.
#[test]
fn b2a_fallback_connector_when_no_mapped_stairs() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let other = g.add_node(osm_node("other", 50.000, 4.0010));
    foot_pair(&mut g, orig, other, 60);
    // Platform polyline is isolated (no edge into the street graph).
    let p1 = g.add_node(osm_node("p1", 50.0008, 4.0008));

    let mut levels = HashMap::new();
    levels.insert(p1, 1i16);
    g.set_osm_level_data(levels, HashMap::new());

    let plat_loc = g.get_node(p1).unwrap().loc();
    g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["2".into()],
        level: Some(1.0),
        centroid: plat_loc,
        node_ids: vec![p1],
    }]));

    let stop_loc = plat_loc;
    let stop = g.add_node(transit_stop("Platform 2", stop_loc.latitude, stop_loc.longitude));

    assert!(relocate_matched_stop(
        &mut g,
        stop,
        stop_loc,
        orig,
        Some("2"),
        None
    ));

    let fb_fwd = foot_edge_len(&g, p1, orig).expect("fallback p1->orig must exist");
    let fb_rev = foot_edge_len(&g, orig, p1).expect("fallback orig->p1 must exist");
    assert!(fb_fwd > 0 && fb_rev > 0, "fallback must be re-priced (>0)");

    g.build_raptor_index();
    assert!(
        g.walk_dijkstra(orig, 99_999).contains_key(&stop),
        "stop must remain reachable from its original street node via the fallback"
    );
}

/// (c) Reachability invariant: a stop reachable before relocation (today's free snap)
/// is still reachable from the same street node after relocation.
#[test]
fn b2a_reachability_invariant_preserved() {
    // "Before" world: today's free snap stop <-> orig.
    let mut before = Graph::new();
    let o_b = before.add_node(osm_node("orig", 50.000, 4.0000));
    let stop_b = before.add_node(transit_stop("S", 50.0005, 4.0005));
    before.add_edge(stop_b, street_edge(stop_b, o_b, 20));
    before.add_edge(o_b, street_edge(o_b, stop_b, 20));
    before.build_raptor_index();
    assert!(before.walk_dijkstra(o_b, 99_999).contains_key(&stop_b));

    // "After" world: same stop, but relocated onto its matched platform.
    let mut after = Graph::new();
    let o_a = after.add_node(osm_node("orig", 50.000, 4.0000));
    let p1 = after.add_node(osm_node("p1", 50.0008, 4.0008));
    let plat_loc = after.get_node(p1).unwrap().loc();
    after.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
        refs: vec!["3".into()],
        level: Some(0.0),
        centroid: plat_loc,
        node_ids: vec![p1],
    }]));
    let stop_loc = LatLng {
        latitude: 50.0005,
        longitude: 4.0005,
    };
    let stop_a = after.add_node(transit_stop("S", stop_loc.latitude, stop_loc.longitude));
    assert!(relocate_matched_stop(
        &mut after, stop_a, stop_loc, o_a, Some("3"), None
    ));
    after.build_raptor_index();
    assert!(
        after.walk_dijkstra(o_a, 99_999).contains_key(&stop_a),
        "relocated stop must remain reachable from its original street node"
    );
}

/// (d) An UNMATCHED stop is left untouched: relocate returns false and performs no
/// mutation (anchor, level and edge list all unchanged).
#[test]
fn b2a_unmatched_stop_unchanged() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let stop_loc = LatLng {
        latitude: 50.0005,
        longitude: 4.0005,
    };
    let stop = g.add_node(transit_stop("S", stop_loc.latitude, stop_loc.longitude));
    g.add_edge(stop, street_edge(stop, orig, 20));
    g.add_edge(orig, street_edge(orig, stop, 20));
    // Empty platform index ⇒ no candidate ⇒ PlatformMatch::None.
    g.set_platform_index(PlatformIndex::from_platforms(vec![]));

    let edges_before = g.out_edges(stop).len();

    assert!(!relocate_matched_stop(
        &mut g,
        stop,
        stop_loc,
        orig,
        Some("1"),
        None
    ));

    assert_eq!(g.get_node(stop).unwrap().loc().latitude, stop_loc.latitude);
    assert_eq!(g.get_node(stop).unwrap().loc().longitude, stop_loc.longitude);
    assert_eq!(g.node_level(stop), None);
    assert_eq!(g.out_edges(stop).len(), edges_before);
}

// ── Connector-cost baking (fix for stairs/elevator underpricing) ──────────────
//
// Verifies that `bake_connector_lengths` rewrites edge lengths so that
// `edge_secs(Foot)` — which always computes `length / walking_speed` — yields
// the intended connector time rather than the (too fast) flat walking time.
// Each test reproduces the production path: bake → contract → drop full arrays →
// route via the contracted graph. This ensures the baked lengths land in the
// super-edge segments that survive the interior-node drop.

use maas_rs::ingestion::osm::ConnectorCost;
use maas_rs::structures::contraction::ContractedGraph;

fn build_connector_graph_and_contract(
    connector_kind: Connector,
    run_m: usize,
    cost: ConnectorCost,
) -> (Graph, NodeID, NodeID, ContractedGraph) {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.001, 4.001));

    let edge = |o: NodeID, d: NodeID| {
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: run_m,
            partial: false,
            foot: true,
            bike: false,
            car: false,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        })
    };
    g.add_edge(a, edge(a, b));
    g.add_edge(b, edge(b, a));

    let mut connectors = HashMap::new();
    connectors.insert((a, b), connector_kind);
    connectors.insert((b, a), connector_kind);
    g.set_osm_level_data(HashMap::new(), connectors);

    g.set_connector_cost(cost);
    g.bake_connector_lengths(cost);

    g.build_raptor_index();

    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.drop_full_node_arrays();

    let cg = g.contracted.take().unwrap();
    (g, a, b, cg)
}

#[test]
fn connector_stairs_baked_to_stair_speed() {
    let walk_speed = 1.2_f64;
    let stair_speed = 0.5_f64;
    let run_m = 10usize;

    let cost = ConnectorCost { stairs_speed_mps: stair_speed, ..ConnectorCost::default() };
    let (mut g, a, b, cg) = build_connector_graph_and_contract(Connector::Steps, run_m, cost);
    g.set_walking_speed_mps(walk_speed);

    let dist = g.walk_dijkstra_union(a, u32::MAX, &cg);
    let got = dist[&b];
    let expected = (run_m as f64 / stair_speed).round() as u32;
    let wrong = (run_m as f64 / walk_speed).round() as u32;

    assert_eq!(
        got, expected,
        "stairs: expected {expected}s (10m/{stair_speed}m/s) but got {got}s (flat walk would be {wrong}s)"
    );
    assert!(got > wrong, "stairs must be slower than flat walking: got {got}s, flat={wrong}s");
}

#[test]
fn connector_elevator_baked_to_fixed_secs() {
    let walk_speed = 1.2_f64;
    let elevator_secs = 45.0_f64;
    let run_m = 5usize;

    let cost = ConnectorCost { elevator_secs, ..ConnectorCost::default() };
    let (mut g, a, b, cg) = build_connector_graph_and_contract(Connector::Elevator, run_m, cost);
    g.set_walking_speed_mps(walk_speed);

    let dist = g.walk_dijkstra_union(a, u32::MAX, &cg);
    let got = dist[&b];
    let expected = elevator_secs.round() as u32;
    let wrong = (run_m as f64 / walk_speed).round() as u32;

    assert_eq!(
        got, expected,
        "elevator: expected {expected}s fixed but got {got}s (flat walk would be {wrong}s)"
    );
    assert!(got > wrong, "elevator must cost more than flat walking: got {got}s, flat={wrong}s");
}

#[test]
fn connector_ramp_baked_to_ramp_speed() {
    let walk_speed = 1.2_f64;
    let ramp_speed = 0.9_f64;
    let run_m = 18usize;

    let cost = ConnectorCost { ramp_speed_mps: ramp_speed, ..ConnectorCost::default() };
    let (mut g, a, b, cg) = build_connector_graph_and_contract(Connector::Ramp, run_m, cost);
    g.set_walking_speed_mps(walk_speed);

    let dist = g.walk_dijkstra_union(a, u32::MAX, &cg);
    let got = dist[&b];
    let expected = (run_m as f64 / ramp_speed).round() as u32;
    let wrong = (run_m as f64 / walk_speed).round() as u32;

    assert_eq!(
        got, expected,
        "ramp: expected {expected}s ({run_m}m/{ramp_speed}m/s) but got {got}s"
    );
    assert!(got > wrong, "ramp must be slower than flat walking: got {got}s, flat={wrong}s");
}

#[test]
fn non_connector_foot_edge_length_unchanged() {
    let run_m = 100usize;
    let walk_speed = 1.2_f64;
    let cost = ConnectorCost::default();

    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.001, 4.001));
    let edge = |o: NodeID, d: NodeID| {
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: run_m,
            partial: false,
            foot: true,
            bike: false,
            car: false,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        })
    };
    g.add_edge(a, edge(a, b));
    g.add_edge(b, edge(b, a));

    g.set_osm_level_data(HashMap::new(), HashMap::new());
    g.set_connector_cost(cost);
    g.bake_connector_lengths(cost);
    g.set_walking_speed_mps(walk_speed);

    g.build_raptor_index();

    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.drop_full_node_arrays();
    let cg = g.contracted.take().unwrap();

    let dist = g.walk_dijkstra_union(a, u32::MAX, &cg);
    let got = dist[&b];
    let expected = (run_m as f64 / walk_speed).round() as u32;
    assert_eq!(got, expected, "regular foot edge must be priced at walking speed");
}

// ── pickup_type / drop_off_type gating ───────────────────────────────────────
//
// Flags byte: bit 0 = board_allowed (0x01), bit 1 = alight_allowed (0x02).
// 0x03 = both allowed (normal stop)
// 0x00 = neither (pass-through, pickup_type=1 AND drop_off_type=1)
// 0x01 = board only (drop_off forbidden)
// 0x02 = alight only (pickup forbidden)
//
// Layout: A→B→C, one trip, dep A 09:00, at B 09:10, arr C 09:20.
// osm origins near each stop allow testing which stops are reachable.

fn three_stop_pattern_graph(
    a_flag: u8,
    b_flag: u8,
    c_flag: u8,
) -> (Graph, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();

    // OSM nodes ~5 km apart so no stop is accessible from the wrong origin via
    // the 10-min access walk budget (720 m).  At lat 50° one degree of longitude
    // ≈ 71 600 m, so 0.070° ≈ 5 010 m.
    let osm_a = g.add_node(osm_node("oa", 50.000, 4.000));
    let osm_b = g.add_node(osm_node("ob", 50.000, 4.070));
    let osm_c = g.add_node(osm_node("oc", 50.000, 4.140));

    // Transit stops 111 m north of their paired OSM node (well within 720 m).
    let stop_a = g.add_node(transit_stop("Stop A", 50.001, 4.000));
    let stop_b = g.add_node(transit_stop("Stop B", 50.001, 4.070));
    let stop_c = g.add_node(transit_stop("Stop C", 50.001, 4.140));

    // Street connections (bidirectional)
    let bidi = |g: &mut Graph, u: NodeID, v: NodeID, m: usize| {
        g.add_edge(u, street_edge(u, v, m));
        g.add_edge(v, street_edge(v, u, m));
    };
    bidi(&mut g, osm_a, osm_b, 5010);
    bidi(&mut g, osm_b, osm_c, 5010);
    bidi(&mut g, osm_a, stop_a, 111);
    bidi(&mut g, osm_b, stop_b, 111);
    bidi(&mut g, osm_c, stop_c, 111);

    // Transit edges (for plan reconstruction)
    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 5010,
        }),
    );
    g.add_edge(
        stop_b,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_b,
            destination: stop_c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 5010,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "R".into(),
        route_long_name: "Route R".into(),
        route_type: RouteType::Bus,
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

    // TripSegments for hop A→B and B→C
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 600,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: 9 * 3600 + 600,
            arrival: 9 * 3600 + 1200,
            service_id: ServiceId(0),
        },
    ]);

    // Pattern 0: [stop_a, stop_b, stop_c], 1 trip
    // Column-major: stop_pos * n_trips + trip_idx (n_trips=1)
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b, stop_c]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
            board_allowed: (a_flag & 0x01) != 0,
            alight_allowed: (a_flag & 0x02) != 0,
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 600,
            departure: 9 * 3600 + 600,
            board_allowed: (b_flag & 0x01) != 0,
            alight_allowed: (b_flag & 0x02) != 0,
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1200,
            departure: 9 * 3600 + 1200,
            board_allowed: (c_flag & 0x01) != 0,
            alight_allowed: (c_flag & 0x02) != 0,
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);

    (g, osm_a, osm_b, osm_c)
}

#[test]
fn raptor_pass_through_stop_cannot_board_or_alight() {
    // B is fully pass-through (pickup=1, drop_off=1 → neither allowed).
    // 1. A→C trip must still work (pass-through survived).
    // 2. B→C query must return no plan (cannot board at B).
    let (g, osm_a, osm_b, osm_c) = three_stop_pattern_graph(0x03, 0x00, 0x03);

    // A→C: board at A, pass through B, alight at C — must succeed.
    let plans_ac = g.raptor(osm_a, osm_c, 8 * 3600, 0, 0x7F, 10 * 60);
    assert!(
        !plans_ac.is_empty(),
        "A→C must find a plan even with a pass-through middle stop"
    );
    for p in &plans_ac {
        let transit: Vec<_> = p
            .legs
            .iter()
            .filter(|l| matches!(l, PlanLeg::Transit(_)))
            .collect();
        assert!(
            !transit.is_empty(),
            "A→C plan must use transit (not just walk)"
        );
    }

    // B→C: only reachable stop from origin is B (pass-through) — no boarding allowed.
    let plans_bc = g.raptor(osm_b, osm_c, 8 * 3600, 0, 0x7F, 10 * 60);
    let transit_plans_bc: Vec<_> = plans_bc
        .iter()
        .filter(|p| {
            p.legs
                .iter()
                .any(|l| matches!(l, PlanLeg::Transit(_)))
        })
        .collect();
    assert!(
        transit_plans_bc.is_empty(),
        "B→C must return no transit plan when B is pass-through (got {} transit plans)",
        transit_plans_bc.len()
    );
}

#[test]
fn raptor_pickup_forbidden_alight_allowed() {
    // B has drop_off allowed but pickup forbidden (0x02 = alight_allowed only).
    // A→B: should produce a transit plan (board at A, alight at B).
    // B→C: should return no transit plan (cannot board at B).
    let (g, osm_a, osm_b, osm_c) = three_stop_pattern_graph(0x03, 0x02, 0x03);

    let plans_ab = g.raptor(osm_a, osm_b, 8 * 3600, 0, 0x7F, 10 * 60);
    let has_transit_ab = plans_ab
        .iter()
        .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))));
    assert!(
        has_transit_ab,
        "A→B must find a transit plan when B has alight_allowed (got plans: {})",
        plans_ab.len()
    );

    let plans_bc = g.raptor(osm_b, osm_c, 8 * 3600, 0, 0x7F, 10 * 60);
    let transit_bc: Vec<_> = plans_bc
        .iter()
        .filter(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
        .collect();
    assert!(
        transit_bc.is_empty(),
        "B→C must have no transit plan when B has pickup_forbidden (got {} plans)",
        transit_bc.len()
    );
}

#[test]
fn raptor_alight_forbidden_prevents_stopping_at_stop() {
    // B has board_allowed but alight FORBIDDEN (0x01 = board-only, no alight).
    // A→B: must return no transit plan — cannot alight at B.
    let (g, osm_a, osm_b, _osm_c) = three_stop_pattern_graph(0x03, 0x01, 0x03);

    let plans_ab = g.raptor(osm_a, osm_b, 8 * 3600, 0, 0x7F, 10 * 60);
    let transit_ab: Vec<_> = plans_ab
        .iter()
        .filter(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
        .collect();
    assert!(
        transit_ab.is_empty(),
        "A→B must return no transit plan when B has alight_forbidden (got {} transit plans)",
        transit_ab.len()
    );
}

#[test]
fn raptor_all_allowed_stops_unchanged() {
    // All stops fully open (0x03): existing RAPTOR semantics preserved.
    // A→C should find a transit plan.
    let (g, osm_a, _osm_b, osm_c) = three_stop_pattern_graph(0x03, 0x03, 0x03);

    let plans = g.raptor(osm_a, osm_c, 8 * 3600, 0, 0x7F, 10 * 60);
    assert!(
        !plans.is_empty(),
        "All-allowed stops: must still find a plan"
    );
    let has_transit = plans
        .iter()
        .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))));
    assert!(has_transit, "All-allowed stops: plan must use transit");
}


// ── Stage 1: provably-complete FOOT access/egress (fastest-arrival guarantee) ──
//
// These tests build tiny synthetic graphs whose FASTEST journey requires an
// access or egress foot walk LONGER than the default 600 s discovery radius —
// exactly the case the legacy `with_access_search` (return on first non-empty
// result) silently misses. An independent brute-force oracle computes the true
// fastest arrival; the engine must match it.

/// One boardable transit segment: board `board` at `dep`, alight `alight` at `arr`.
#[derive(Clone, Copy)]
struct Hop {
    board: NodeID,
    alight: NodeID,
    dep: u32,
    arr: u32,
}

/// Street-time model that is the identity on both access and egress seconds
/// (percentile 0.5 ⇒ z = 0, σ = 0), so arrival = label + raw walk seconds and
/// the oracle can predict arrivals exactly.
fn identity_street_time() -> StreetTimeModel {
    StreetTimeModel {
        access_percentile: 0.5,
        sigma_alpha: 0.0,
        sigma_floor: 0.0,
        sigma_cap: 0.5,
    }
}

fn add_street_bidir(g: &mut Graph, a: NodeID, b: NodeID, m: usize) {
    g.add_edge(a, street_edge(a, b, m));
    g.add_edge(b, street_edge(b, a, m));
}

/// Foot-only snap edge (like a GTFS stop→street partial edge), both directions.
fn add_snap_bidir(g: &mut Graph, stop: NodeID, osm: NodeID, m: usize) {
    let mk = |o: NodeID, d: NodeID| {
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
    g.add_edge(stop, mk(stop, osm));
    g.add_edge(osm, mk(osm, stop));
}

/// Push one 2-stop pattern with N trips (column-major stop times) plus its
/// transit edge + trip segments. `deps`/`arrs` are per-trip board/alight times.
#[allow(clippy::too_many_arguments)]
fn add_two_stop_line(
    g: &mut Graph,
    board: NodeID,
    alight: NodeID,
    route: RouteId,
    trips: &[TripId],
    deps: &[u32],
    arrs: &[u32],
    length_m: usize,
) {
    let n = trips.len();
    // Transit edge (reconstruct / timetable lookup), covering all N segments.
    let seg_start = g.get_transit_departures_size();
    let segs: Vec<TripSegment> = (0..n)
        .map(|i| TripSegment {
            trip_id: trips[i],
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: deps[i],
            arrival: arrs[i],
            service_id: ServiceId(0),
        })
        .collect();
    g.add_transit_departures(segs);
    g.add_edge(
        board,
        EdgeData::Transit(TransitEdgeData {
            origin: board,
            destination: alight,
            route_id: route,
            timetable_segment: TimetableSegment { start: seg_start, len: n },
            length: length_m,
        }),
    );

    // Pattern arrays.
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[board, alight]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

    let ts = g.transit_pattern_trips_len();
    for &t in trips {
        g.push_transit_pattern_trip(t);
    }
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: n });

    let sts = g.transit_pattern_stop_times_len();
    // Column-major: all board times, then all alight times.
    for &d in deps {
        g.push_transit_pattern_stop_time(StopTime { arrival: d, departure: d, ..Default::default() });
    }
    for &a in arrs {
        g.push_transit_pattern_stop_time(StopTime { arrival: a, departure: a, ..Default::default() });
    }
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 * n });

    g.push_transit_pattern(PatternInfo { route, num_trips: n as u32 });
}

/// Corridor whose FASTEST journey egresses at a stop ~905 s from the destination
/// (well beyond the 600 s radius). Fast bus A→Z (Z is 905 s from D); slow bus
/// A→Y (Y is 305 s from D) arrives later. Returns (graph, origin, dest, hops).
fn stage1_far_egress_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    // osm corridor (lon = 4.0 + meters/71695).
    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jz = g.add_node(osm_node("jZ", 50.000, 4.0350652));
    let jy = g.add_node(osm_node("jY", 50.000, 4.0451078));
    let d = g.add_node(osm_node("D", 50.000, 4.0501291));

    let stop_a = g.add_node(transit_stop("A", 50.000, 4.0010043));
    let stop_z = g.add_node(transit_stop("Z", 50.000, 4.0350652));
    let stop_y = g.add_node(transit_stop("Y", 50.000, 4.0451078));

    add_street_bidir(&mut g, o, jz, 2514); // 2095 s
    add_street_bidir(&mut g, jz, jy, 720); //  600 s
    add_street_bidir(&mut g, jy, d, 360); //  300 s
    add_snap_bidir(&mut g, stop_a, o, 72); //   60 s
    add_snap_bidir(&mut g, stop_z, jz, 6); //    5 s
    add_snap_bidir(&mut g, stop_y, jy, 6); //    5 s
    // egress D→Z = 300+600+5 = 905 s; D→Y = 300+5 = 305 s.

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "F".into(),
            route_long_name: "Fast".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "S".into(),
            route_long_name: "Slow".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    let mk_trip = |route: RouteId| TripInfo {
        trip_headsign: None,
        route_id: route,
        service_id: ServiceId(0),
        bikes_allowed: None,
    };
    g.add_transit_trips(vec![
        mk_trip(RouteId(0)),
        mk_trip(RouteId(0)),
        mk_trip(RouteId(0)),
        mk_trip(RouteId(1)),
        mk_trip(RouteId(1)),
        mk_trip(RouteId(1)),
    ]);

    // Fast A→Z: 3 trips, 10-min ride.
    let f_dep = [33000u32, 34200, 35400];
    let f_arr = [33600u32, 34800, 36000];
    add_two_stop_line(
        &mut g, stop_a, stop_z, RouteId(0),
        &[TripId(0), TripId(1), TripId(2)], &f_dep, &f_arr, 2438,
    );
    // Slow A→Y: 3 trips, 25-min ride.
    let s_dep = [33000u32, 34200, 35400];
    let s_arr = [34500u32, 35700, 36900];
    add_two_stop_line(
        &mut g, stop_a, stop_y, RouteId(1),
        &[TripId(3), TripId(4), TripId(5)], &s_dep, &s_arr, 3160,
    );

    let mut hops = Vec::new();
    for i in 0..3 {
        hops.push(Hop { board: stop_a, alight: stop_z, dep: f_dep[i], arr: f_arr[i] });
        hops.push(Hop { board: stop_a, alight: stop_y, dep: s_dep[i], arr: s_arr[i] });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, o, d, hops)
}

/// Corridor whose FASTEST journey boards at a stop ~905 s from the origin (well
/// beyond the 600 s radius). Fast bus from the far stop reaches the destination
/// stop earlier than the slow bus from the near stop.
fn stage1_far_access_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jfast = g.add_node(osm_node("jFast", 50.000, 4.0151486));
    let d = g.add_node(osm_node("D", 50.000, 4.0501291));

    let stop_slow = g.add_node(transit_stop("SLOW", 50.000, 4.0010043));
    let stop_fast = g.add_node(transit_stop("FAST", 50.000, 4.0151486));
    let stop_near_d = g.add_node(transit_stop("NEARD", 50.000, 4.0501291));

    add_street_bidir(&mut g, o, jfast, 1086); //  905 s
    add_street_bidir(&mut g, jfast, d, 2508); // 2090 s
    add_snap_bidir(&mut g, stop_slow, o, 72); //   60 s access
    add_snap_bidir(&mut g, stop_fast, jfast, 6); //  5 s (→ 905 s from O)
    add_snap_bidir(&mut g, stop_near_d, d, 6); //    5 s egress
    // access O→FAST = 905 s; O→SLOW = 60 s; walk O→D = 2995 s.

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "F".into(),
            route_long_name: "Fast".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "S".into(),
            route_long_name: "Slow".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
    ]);

    // Fast FAST→NEARD dep 33600 arr 34200; Slow SLOW→NEARD dep 33000 arr 34500.
    add_two_stop_line(
        &mut g, stop_fast, stop_near_d, RouteId(0),
        &[TripId(0)], &[33600], &[34200], 2508,
    );
    add_two_stop_line(
        &mut g, stop_slow, stop_near_d, RouteId(1),
        &[TripId(1)], &[33000], &[34500], 3594,
    );

    let hops = vec![
        Hop { board: stop_fast, alight: stop_near_d, dep: 33600, arr: 34200 },
        Hop { board: stop_slow, alight: stop_near_d, dep: 33000, arr: 34500 },
    ];

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, o, d, hops)
}

/// Independent brute-force fastest arrival for `origin → dest` departing at
/// `dep`: the min over the direct walk and every single-transit-leg journey
/// (walk to a boardable stop, ride, walk to dest), using the engine's own
/// unbounded foot Dijkstra for ground-truth walk seconds. Unrestricted access/
/// egress radius, so it is oblivious to the 600 s heuristic.
fn oracle_fastest_arrival(g: &Graph, origin: NodeID, dest: NodeID, dep: u32, hops: &[Hop]) -> u32 {
    let from_o = g.walk_dijkstra(origin, u32::MAX);
    let to_d = g.walk_dijkstra(dest, u32::MAX);
    let mut best = u32::MAX;
    // Direct walk.
    if let Some(&w) = from_o.get(&dest) {
        best = best.min(dep.saturating_add(w));
    }
    // Single transit leg.
    for h in hops {
        let Some(&wa) = from_o.get(&h.board) else { continue };
        if dep.saturating_add(wa) > h.dep {
            continue; // cannot make this departure
        }
        let Some(&we) = to_d.get(&h.alight) else { continue };
        best = best.min(h.arr.saturating_add(we));
    }
    best
}

fn min_end(plans: &[maas_rs::structures::plan::Plan]) -> u32 {
    plans.iter().map(|p| p.end).min().unwrap_or(u32::MAX)
}

#[test]
fn stage1_egress_beyond_radius_fastest_arrival() {
    // Fastest journey egresses at Z (905 s > 600 s from D). Legacy code returns
    // the slower Y journey (34805); Stage 1 must return the Z journey (34505).
    let (g, o, d, hops) = stage1_far_egress_graph();
    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    // Sanity: the oracle's optimum is the far-egress Z journey (34505), strictly
    // better than the best journey reachable within the 600 s radius (Y = 34805).
    assert_eq!(oracle, 34505, "oracle fastest arrival (via far egress Z)");

    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "must return a plan");
    assert_eq!(
        min_end(&plans),
        oracle,
        "engine must find the far-egress fastest journey (got {}, oracle {})",
        min_end(&plans),
        oracle
    );
}

#[test]
fn stage1_access_beyond_radius_fastest_arrival() {
    // Fastest journey boards FAST (905 s > 600 s from O). Legacy returns the
    // slower SLOW journey (34505); Stage 1 must return the FAST journey (34205).
    let (g, o, d, hops) = stage1_far_access_graph();
    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    assert_eq!(oracle, 34205, "oracle fastest arrival (via far access FAST)");

    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "must return a plan");
    assert_eq!(
        min_end(&plans),
        oracle,
        "engine must find the far-access fastest journey (got {}, oracle {})",
        min_end(&plans),
        oracle
    );
}

#[test]
fn stage1_oracle_equivalence_single_departures() {
    // Across several departures, the single-departure engine must match the
    // brute-force fastest arrival exactly (completeness + soundness).
    // Departures chosen so a transit journey is always the fastest option (the
    // walk-only-dominates regime is a separate walk-vs-transit concern, not an
    // access/egress-completeness one).
    let (g, o, d, hops) = stage1_far_egress_graph();
    for dep in [32400u32, 33000, 33001, 34000] {
        let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
        let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
        assert!(!plans.is_empty(), "dep {dep}: must return a plan");
        assert_eq!(
            min_end(&plans),
            oracle,
            "dep {dep}: engine {} != oracle {}",
            min_end(&plans),
            oracle
        );
    }
}

#[test]
fn stage1_oracle_equivalence_range() {
    // The range driver must also find the window's globally fastest arrival,
    // which is the earliest fast-bus far-egress journey.
    let (g, o, d, hops) = stage1_far_egress_graph();
    let start = 32400;
    let window = 3 * 3600;
    // Global fastest over any boardable departure in the window.
    let mut oracle_global = u32::MAX;
    for dep in start..=(start + window) {
        oracle_global = oracle_global.min(oracle_fastest_arrival(&g, o, d, dep, &hops));
    }
    let plans = g.raptor_range(o, d, start, window, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "range must return plans");
    assert_eq!(
        min_end(&plans),
        oracle_global,
        "range engine fastest {} != oracle {}",
        min_end(&plans),
        oracle_global
    );
}

/// The query-latency profiler is purely additive observability: arming it must
/// not change routing behavior. Runs the same far-egress range query with the
/// profiler off then on and asserts byte-identical plans (via `Debug`), plus
/// sanity-checks that turning it on actually produced a non-trivial
/// decomposition (so this isn't accidentally testing a no-op).
#[test]
fn profile_latency_flag_off_vs_on_yields_identical_plans() {
    use maas_rs::structures::latency_profile;

    let (g, o, d, _hops) = stage1_far_egress_graph();
    let start = 32400;
    let window = 3 * 3600;
    let dbg = |ps: &[maas_rs::structures::plan::Plan]| {
        ps.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>()
    };

    let t0 = latency_profile::begin_query(false);
    let plans_off = g.raptor_range(o, d, start, window, 0, 0x7F, 10 * 60);
    assert!(
        latency_profile::end_query(t0).is_none(),
        "profiler must not report anything when off"
    );

    let t1 = latency_profile::begin_query(true);
    let plans_on = g.raptor_range(o, d, start, window, 0, 0x7F, 10 * 60);
    let profile = latency_profile::end_query(t1).expect("profiling was enabled");

    assert_eq!(
        dbg(&plans_off),
        dbg(&plans_on),
        "profiling must not change routing behavior or results"
    );

    // The profiler actually measured something real: at least one pass ran the
    // per-departure range loop, and backward never exceeds extract.
    let total_departures: u32 = profile.passes.iter().map(|p| p.departures).sum();
    assert!(total_departures > 0, "expected at least one range departure");
    assert!(
        profile.backward <= profile.extract,
        "backward ({:?}) must nest under extract ({:?})",
        profile.backward,
        profile.extract
    );
    let report = profile.report();
    assert!(report.contains("discovery"));
    assert!(report.contains("Pass A"));
}

// ── Near-slow + far-fast retention (accumulate-and-merge soundness) ──
//
// The completeness fix runs two access/egress passes (near-stop radius, then the
// walk-only radius W) and MERGES their plans. These graphs place BOTH a near stop
// (inside the 600 s radius, non-empty Pass A) AND a far stop (beyond it, only
// found by Pass B). They assert two things at once:
//   • soundness — the engine's fastest arrival equals the brute-force oracle, and
//     the oracle's optimum is the FAR journey, so Pass B is doing real work; and
//   • retention — a later-departing NEAR-stop journey, Pareto-non-dominated on the
//     departure axis, must survive the merge (bug #3: the old budget-cap dropped
//     such diverse plans). Both distinct arrivals must appear in the range result.

/// Near+far ACCESS corridor with a later NEAR departure. NEAR boards 60 s from O
/// (inside the radius); FAR boards ~1010 s from O (beyond it). One fast FAR→NEARD
/// trip arrives earliest; the NEAR→NEARD line has an early trip (dominated by FAR)
/// and a late trip no FAR ride can dominate on departure. Fastest = far-access.
fn stage1_near_far_access_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jfar = g.add_node(osm_node("jFar", 50.000, 4.0168283));
    let d = g.add_node(osm_node("D", 50.000, 4.0518168));

    let stop_near = g.add_node(transit_stop("NEAR", 50.000, 4.0010043));
    let stop_far = g.add_node(transit_stop("FAR", 50.000, 4.0168283));
    let stop_neard = g.add_node(transit_stop("NEARD", 50.000, 4.0518168));

    add_street_bidir(&mut g, o, jfar, 1206); // 1005 s
    add_street_bidir(&mut g, jfar, d, 2508); // 2090 s
    add_snap_bidir(&mut g, stop_near, o, 72); //   60 s access
    add_snap_bidir(&mut g, stop_far, jfar, 6); //   5 s (→ 1010 s from O)
    add_snap_bidir(&mut g, stop_neard, d, 6); //    5 s egress
    // access O→FAR = 1010 s (> 600 s); O→NEAR = 60 s; walk O→D = 3095 s.

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "F".into(),
            route_long_name: "Fast".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "S".into(),
            route_long_name: "Slow".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
    ]);

    // Fast FAR→NEARD: single trip dep 33600 arr 34200 (→ dest 34205).
    add_two_stop_line(
        &mut g, stop_far, stop_neard, RouteId(0),
        &[TripId(0)], &[33600], &[34200], 2508,
    );
    // Slow NEAR→NEARD: early trip (dominated) + late trip (retained in a window).
    add_two_stop_line(
        &mut g, stop_near, stop_neard, RouteId(1),
        &[TripId(1), TripId(2)], &[33000, 36000], &[34500, 37500], 3714,
    );

    let hops = vec![
        Hop { board: stop_far, alight: stop_neard, dep: 33600, arr: 34200 },
        Hop { board: stop_near, alight: stop_neard, dep: 33000, arr: 34500 },
        Hop { board: stop_near, alight: stop_neard, dep: 36000, arr: 37500 },
    ];

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, o, d, hops)
}

/// Near+far EGRESS corridor with a later NEAR-egress departure. A fast bus alights
/// at Z (~905 s egress, beyond the radius) yet arrives earliest overall; a slow
/// bus alights at Y (305 s egress, inside the radius) with an early trip (dominated)
/// and a late trip (retained). Fastest = far-egress Z.
fn stage1_near_far_egress_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jz = g.add_node(osm_node("jZ", 50.000, 4.0350652));
    let jy = g.add_node(osm_node("jY", 50.000, 4.0451078));
    let d = g.add_node(osm_node("D", 50.000, 4.0501291));

    let stop_a = g.add_node(transit_stop("A", 50.000, 4.0010043));
    let stop_z = g.add_node(transit_stop("Z", 50.000, 4.0350652));
    let stop_y = g.add_node(transit_stop("Y", 50.000, 4.0451078));

    add_street_bidir(&mut g, o, jz, 2514); // 2095 s
    add_street_bidir(&mut g, jz, jy, 720); //  600 s
    add_street_bidir(&mut g, jy, d, 360); //  300 s
    add_snap_bidir(&mut g, stop_a, o, 72); //   60 s access
    add_snap_bidir(&mut g, stop_z, jz, 6); //    5 s
    add_snap_bidir(&mut g, stop_y, jy, 6); //    5 s
    // egress D→Z = 300+600+5 = 905 s (> 600 s); D→Y = 305 s; walk O→D = 2995 s.

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "F".into(),
            route_long_name: "Fast".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
        RouteInfo {
            route_short_name: "S".into(),
            route_long_name: "Slow".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
    ]);

    // Fast A→Z: single trip dep 33000 arr 33600 (→ dest 34505 via 905 s egress).
    add_two_stop_line(
        &mut g, stop_a, stop_z, RouteId(0),
        &[TripId(0)], &[33000], &[33600], 2438,
    );
    // Slow A→Y: early trip (dominated) + late trip (retained), 305 s egress.
    add_two_stop_line(
        &mut g, stop_a, stop_y, RouteId(1),
        &[TripId(1), TripId(2)], &[33000, 36000], &[34500, 37500], 3160,
    );

    let hops = vec![
        Hop { board: stop_a, alight: stop_z, dep: 33000, arr: 33600 },
        Hop { board: stop_a, alight: stop_y, dep: 33000, arr: 34500 },
        Hop { board: stop_a, alight: stop_y, dep: 36000, arr: 37500 },
    ];

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, o, d, hops)
}

#[test]
fn stage1_near_far_access_finds_far_and_retains_near() {
    let (g, o, d, hops) = stage1_near_far_access_graph();

    // Single departure: fastest is the far-access FAST journey (34205), beyond the
    // 600 s radius, so only the merged Pass B can find it.
    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    assert_eq!(oracle, 34205, "oracle fastest = far-access FAST (via FAR)");
    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "single: must return a plan");
    assert_eq!(min_end(&plans), oracle, "single: engine {} != oracle {}", min_end(&plans), oracle);

    // Range: the globally fastest is still the far-access journey, AND the later
    // NEAR-departure journey (37505) must be retained alongside it, not dropped.
    let start = 32400;
    let window = 4 * 3600;
    let mut oracle_global = u32::MAX;
    for t in start..=(start + window) {
        oracle_global = oracle_global.min(oracle_fastest_arrival(&g, o, d, t, &hops));
    }
    assert_eq!(oracle_global, 34205, "range oracle fastest = far-access FAST");
    let plans = g.raptor_range(o, d, start, window, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "range: must return plans");
    assert_eq!(min_end(&plans), oracle_global, "range: engine {} != oracle {}", min_end(&plans), oracle_global);
    let ends: std::collections::HashSet<u32> = plans.iter().map(|p| p.end).collect();
    assert!(ends.contains(&34205), "range: fast far-access arrival retained; ends={ends:?}");
    assert!(
        ends.contains(&37505),
        "range: later NEAR-departure arrival must be retained (bug #3 regression); ends={ends:?}"
    );
}

#[test]
fn stage1_near_far_egress_finds_far_and_retains_near() {
    let (g, o, d, hops) = stage1_near_far_egress_graph();

    // Single departure: fastest is the far-egress Z journey (34505), whose egress
    // walk (905 s) lies beyond the radius — only merged Pass B reaches it.
    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    assert_eq!(oracle, 34505, "oracle fastest = far-egress (via Z)");
    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "single: must return a plan");
    assert_eq!(min_end(&plans), oracle, "single: engine {} != oracle {}", min_end(&plans), oracle);

    // Range: fastest still far-egress; the later NEAR-egress journey (37805) must
    // be retained.
    let start = 32400;
    let window = 4 * 3600;
    let mut oracle_global = u32::MAX;
    for t in start..=(start + window) {
        oracle_global = oracle_global.min(oracle_fastest_arrival(&g, o, d, t, &hops));
    }
    assert_eq!(oracle_global, 34505, "range oracle fastest = far-egress");
    let plans = g.raptor_range(o, d, start, window, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "range: must return plans");
    assert_eq!(min_end(&plans), oracle_global, "range: engine {} != oracle {}", min_end(&plans), oracle_global);
    let ends: std::collections::HashSet<u32> = plans.iter().map(|p| p.end).collect();
    assert!(ends.contains(&34505), "range: fast far-egress arrival retained; ends={ends:?}");
    assert!(
        ends.contains(&37805),
        "range: later NEAR-egress arrival must be retained (bug #3 regression); ends={ends:?}"
    );
}

/// Item 12: raptorExplain uses the SAME two-pass A/B access/egress search as
/// production `raptor`. On the far-egress graph the fastest journey (34505) needs
/// an egress walk (905 s) beyond the initial 600 s near radius — only merged Pass B
/// reaches it. The OLD growing-radius doubling loop in `with_access_search_debug`
/// returned on the first non-empty radius and MISSED it; the ported two-pass finds
/// it, so the explain plan set now equals prod's.
#[test]
fn explain_two_pass_matches_prod_on_far_egress() {
    let (g, o, d, _hops) = stage1_near_far_egress_graph();
    let dep = 32400;
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let am = ActiveModes::default();
    let rt = RealtimeIndex::new();
    let bike = BikeCost::new(BikeProfile::default());

    let prod = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    let res = g.raptor_explain_tuned_rt_modes(
        o,
        d,
        dep,
        0,
        0x7F,
        10 * 60,
        &buckets,
        900,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &rt,
        &am,
        &bike,
        None,
        maas_rs::structures::cost::FareProfile::default(),
    );

    // The far-egress fastest arrival is discovered by the explain (debug) path.
    assert!(
        res.plans.iter().any(|p| p.end == 34505),
        "explain must reach the far-egress arrival 34505 via Pass B; ends={:?}",
        res.plans.iter().map(|p| p.end).collect::<Vec<_>>()
    );
    // Explain plan SET now equals prod's (access is identical; single departure has
    // no finalization-only divergence here).
    let key = |p: &maas_rs::structures::plan::Plan| (p.mode, p.start, p.end, transit_leg_count(p));
    let mut a: Vec<_> = prod.iter().map(key).collect();
    let mut b: Vec<_> = res.plans.iter().map(key).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "raptorExplain plan set diverged from prod raptor on far egress");
    // Two-pass ran: Pass B beyond Pass A is recorded as one extra pass.
    assert_eq!(res.access.access_attempts, 1, "Pass B must have run for the far egress");
    assert!(!res.access.fell_back_to_walk_only);
}

// ── CCH foot access/egress (chunk 1 smoke test) ─────────────────────────────────

/// A star of foot edges around a central non-stop hub `h`, so the CCH one-to-many
/// traverses real super-edge arcs (not just seed stubs):
///
/// ```text
///   a --60m-- h --120m-- s1(stop)
///             |
///           240m
///             |
///            s2(stop)
/// ```
///
/// Edge LENGTHS are explicit, so at the default 1.2 m/s (speed_mms = 1200) the exact
/// foot seconds are `len * 1000 / 1200`: 60→50, 120→100, 240→200. From `a`, access to
/// `s1` = 50+100 = 150, to `s2` = 50+200 = 250 — a hand-computed anchor independent of
/// any graph routine. Returns `(g, a, h)` with contraction + raptor index built.
fn cch_star_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();
    let h = g.add_node(osm_node("h", 50.000, 4.000));
    let a = g.add_node(osm_node("a", 50.000, 4.001));
    let s1 = g.add_node(transit_stop("S1", 50.001, 4.000));
    let s2 = g.add_node(transit_stop("S2", 50.000, 3.999));
    // Bidirectional foot edges (foot cost is direction-symmetric).
    g.add_edge(h, street_edge(h, a, 60));
    g.add_edge(a, street_edge(a, h, 60));
    g.add_edge(h, street_edge(h, s1, 120));
    g.add_edge(s1, street_edge(s1, h, 120));
    g.add_edge(h, street_edge(h, s2, 240));
    g.add_edge(s2, street_edge(s2, h, 240));
    g.build_raptor_index(); // MUST precede contraction (build_cch_access reads transit_node_to_stop)
    enable_contraction(&mut g);
    (g, a, h)
}

/// `build_cch_access` + `cch_access` reproduce the exact node-level `nearby_stops`
/// Dijkstra and the contracted `nearby_stops_arena`, and match a hand-computed anchor.
#[test]
fn cch_access_matches_exact_walk() {
    let (g, a, _h) = cch_star_graph();
    let cch = g.build_cch_access();
    let a_ll = g.get_node(a).unwrap().loc();
    let radius = g.raptor.edge_snap_radius_m;

    let got = g.cch_access(&cch, a_ll);

    // Independent reference #1: node-level walk Dijkstra (uses g.nodes/edges, no foot_snap,
    // no contracted graph — a genuinely independent oracle).
    let node_ref = g.nearby_stops(a, u32::MAX);
    assert_eq!(got, node_ref, "cch_access must equal node-level nearby_stops");

    // Independent reference #2: contracted coord-based twin, exact (unbounded) set.
    let cg = g.contracted.as_ref().unwrap();
    let arena_ref = cg.nearby_stops_arena(&g, a_ll.latitude, a_ll.longitude, radius, u32::MAX);
    assert_eq!(got, arena_ref, "cch_access must equal nearby_stops_arena");

    // Independent hand-computed anchor: from a, {s1: 150s, s2: 250s} by geometry.
    let mut secs: Vec<u32> = got.iter().map(|&(_, s)| s).collect();
    secs.sort_unstable();
    assert_eq!(secs, vec![150, 250], "hand-computed foot seconds from a");
    assert_eq!(got.len(), 2, "both stops reached");
}

/// `cch_egress` is the symmetric one-to-many: from the hub `h` it must give the exact
/// hub→stop foot seconds ({s1:100, s2:200}) and equal node-level `nearby_stops(h)`.
#[test]
fn cch_egress_matches_exact_walk() {
    let (g, _a, h) = cch_star_graph();
    let cch = g.build_cch_access();
    let h_ll = g.get_node(h).unwrap().loc();

    let got = g.cch_egress(&cch, h_ll);

    let node_ref = g.nearby_stops(h, u32::MAX);
    assert_eq!(got, node_ref, "cch_egress must equal node-level nearby_stops(h)");

    let mut secs: Vec<u32> = got.iter().map(|&(_, s)| s).collect();
    secs.sort_unstable();
    assert_eq!(secs, vec![100, 200], "hand-computed foot seconds from hub h");
}

/// CHUNK 3 split: the cacheable ORDER path (`compute_cch_order` →
/// `build_cch_access_with_order`) must produce a CCH that answers identically to the
/// all-in-one `build_cch_access`. The order is `n` long (a permutation of the junction
/// ranks), independent of the walk-second weights, and is the only thing cached to
/// `cch.bin`.
#[test]
fn cch_split_order_matches_all_in_one() {
    let (g, a, h) = cch_star_graph();

    let order = g.compute_cch_order();
    assert_eq!(order.len(), g.cch_vertex_count(), "order permutes every junction");
    // A valid rank permutation: every rank in 0..n appears exactly once.
    let mut sorted = order.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (0..order.len() as u32).collect::<Vec<_>>());

    let cch_split = g.build_cch_access_with_order(&order);
    let cch_all = g.build_cch_access();
    let a_ll = g.get_node(a).unwrap().loc();
    let h_ll = g.get_node(h).unwrap().loc();

    assert_eq!(
        g.cch_access(&cch_split, a_ll),
        g.cch_access(&cch_all, a_ll),
        "split order reproduces all-in-one access"
    );
    assert_eq!(
        g.cch_egress(&cch_split, h_ll),
        g.cch_egress(&cch_all, h_ll),
        "split order reproduces all-in-one egress"
    );
}

/// CHUNK 3 restore path: `graph.bin` is persisted AFTER `finalize_contraction` drops the
/// interior node/edge arrays, so on `--restore --serve` the CCH is (re)built with empty
/// `g.nodes`/`g.edges`. `extract_foot_graph`/`walk_secs` must read only the contracted
/// arena + `raptor` params (never the dropped arrays), exactly like `nearby_stops_arena`.
/// Assert the CCH answers identically before and after the drop, on the same cached order.
#[test]
fn cch_build_survives_interior_array_drop() {
    let (mut g, a, h) = cch_star_graph();
    let a_ll = g.get_node(a).unwrap().loc();
    let h_ll = g.get_node(h).unwrap().loc();

    let order = g.compute_cch_order();
    let before_access = g.cch_access(&g.build_cch_access_with_order(&order), a_ll);
    let before_egress = g.cch_egress(&g.build_cch_access_with_order(&order), h_ll);

    // Drop the interior arrays exactly as production does before persisting graph.bin.
    maas_rs::services::build::finalize_contraction(&mut g).expect("drop interior arrays");

    // Same order (contracted graph + junction count are untouched by the drop), rebuilt
    // CCH — must answer identically. (`nearby_stops` can't be the oracle here: it needs
    // g.nodes, now empty; pre-drop vs post-drop CCH output is the invariant.)
    let after_access = g.cch_access(&g.build_cch_access_with_order(&order), a_ll);
    let after_egress = g.cch_egress(&g.build_cch_access_with_order(&order), h_ll);
    assert_eq!(before_access, after_access, "access invariant to interior-array drop");
    assert_eq!(before_egress, after_egress, "egress invariant to interior-array drop");
}

// ── CHUNK 2: CCH exactness (superset agreement + end-to-end A/B) ─────────────────

/// (4a) Superset / agreement. On the star graph one stop (S1, 150 s) lies inside a
/// radius `R = 200 s` and one (S2, 250 s) lies beyond it. The exact CCH one-to-many
/// (which never bounds by radius) must, when RESTRICTED to `<= R`, agree stop-for-stop
/// and second-for-second with the radius-bounded two-pass reference
/// (`nearby_stops_union(origin, R, cg)`), AND additionally surface the far S2 that the
/// bounded reference cannot see. `a` sits exactly on a junction node, so the CCH's
/// coord edge-snap and the reference's node seeding collapse to the same seed — the
/// comparison isolates the radius behaviour, not a snap offset.
#[test]
fn cch_access_is_superset_of_bounded_two_pass() {
    let (g, a, _h) = cch_star_graph();
    let cch = g.build_cch_access();
    let a_ll = g.get_node(a).unwrap().loc();
    let cg = g.contracted.as_ref().unwrap();

    // Exact, unbounded CCH access: both stops, {S1:150, S2:250}.
    let exact = g.cch_access(&cch, a_ll);
    assert_eq!(exact.len(), 2, "CCH reaches both stops; got {exact:?}");

    // Bounded two-pass reference at R = 200 s: only S1 (150 s) is within radius.
    let r: u32 = 200;
    let bounded = g.nearby_stops_union(a, r, cg);

    // CCH restricted to <= R must equal the bounded reference stop-for-stop, EQUAL secs.
    let cch_within: Vec<(usize, u32)> =
        exact.iter().copied().filter(|&(_, s)| s <= r).collect();
    assert_eq!(
        cch_within, bounded,
        "CCH restricted to <= {r}s must equal nearby_stops_union(origin, {r}s); \
         cch_within={cch_within:?} bounded={bounded:?}"
    );
    assert_eq!(cch_within.len(), 1, "exactly S1 within {r}s");

    // And the CCH surfaces the far stop the bounded reference misses.
    let far: Vec<(usize, u32)> = exact.iter().copied().filter(|&(_, s)| s > r).collect();
    assert_eq!(far.len(), 1, "exactly S2 beyond {r}s; got {far:?}");
    assert_eq!(far[0].1, 250, "far stop is S2 at 250 s");
    let bounded_stops: std::collections::HashSet<usize> =
        bounded.iter().map(|&(s, _)| s).collect();
    assert!(
        !bounded_stops.contains(&far[0].0),
        "bounded two-pass must NOT contain the far stop"
    );
}

/// (4b) End-to-end A/B through `raptor()`. The OD's fastest journey boards FAR, whose
/// access walk (1010 s) lies beyond the 600 s near radius. With `use_cch_access` OFF
/// the router uses the radius-bounded two-pass foot search; with it ON the CCH exact
/// one-to-many feeds foot access. Toggling the flag on the SAME graph (build + install
/// the CCH once) must leave the fastest arrival no worse and still surface the
/// far-boarding plan.
///
/// NOTE: the two-pass path already widens (Pass B → `min(W, A - start)`) far enough to
/// discover the fastest far-boarding stop, so CCH-off is NOT expected to miss it — the
/// A/B asserts arrival PARITY (`on <= off`) plus far-boarding presence, which is what
/// provably holds; it does not assert CCH-off fails.
#[test]
fn cch_ab_far_boarding_end_to_end() {
    let (mut g, o, d, hops) = stage1_near_far_access_graph();
    let far_board = hops[0].board; // stop FAR: 1010 s access, beyond the 600 s radius.
    let cch = g.build_cch_access();
    g.set_cch(cch);

    let dep = 32400;
    let radius = 10 * 60; // 600 s near radius.

    // CCH OFF — radius-bounded two-pass foot access.
    g.set_use_cch_access(false);
    let off = g.raptor(o, d, dep, 0, 0x7F, radius);
    assert!(!off.is_empty(), "CCH-off must return a plan");
    let off_fastest = min_end(&off);

    // CCH ON — exact one-to-many foot access.
    g.set_use_cch_access(true);
    let on = g.raptor(o, d, dep, 0, 0x7F, radius);
    assert!(!on.is_empty(), "CCH-on must return a plan");
    let on_fastest = min_end(&on);

    assert!(
        on_fastest <= off_fastest,
        "CCH-on fastest {on_fastest} must be <= CCH-off {off_fastest}"
    );
    assert_eq!(
        on_fastest, 34205,
        "CCH-on fastest arrival = far-boarding FAST journey (via FAR)"
    );

    // Specifically finds the far-boarding plan: some plan's first transit leg boards FAR.
    let boards_far = |plans: &[maas_rs::structures::plan::Plan]| {
        plans.iter().any(|p| {
            p.legs.iter().find_map(|l| match l {
                PlanLeg::Transit(t) => Some(t.from.node_id == far_board),
                _ => None,
            }) == Some(true)
        })
    };
    assert!(
        boards_far(&on),
        "CCH-on must surface the far-boarding (FAR) plan"
    );
}

/// (4b-discriminator) Proves `use_cch_access` SPECIFICALLY flips the foot-access seam.
/// `raptor()` output can't discriminate (the two-pass is complete for fastest arrival,
/// so on/off land on the same Pareto set), so a dropped/swapped flag would be invisible
/// there. The pre-Pareto access metadata does discriminate: at the initial 600 s radius
/// the bounded two-pass reports only the NEAR stop (1), while the unbounded CCH
/// one-to-many reports all three (NEAR 60 s, FAR 1010 s, NEARD ~3095 s). A plumbing bug
/// that fed `false` (or the `unrestricted` value) into the `use_cch` slot would leave
/// flag-ON reading the bounded set and this assertion would fail.
#[test]
fn cch_flag_flips_access_set_via_explain() {
    let (mut g, o, d, _hops) = stage1_near_far_access_graph();
    let cch = g.build_cch_access();
    g.set_cch(cch);

    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let am = ActiveModes::default();
    let rt = RealtimeIndex::new();
    let bike = BikeCost::new(BikeProfile::default());
    let run = |g: &Graph| {
        g.raptor_explain_tuned_rt_modes(
            o, d, 32400, 0, 0x7F, 10 * 60, &buckets, 900,
            g.raptor.unrestricted_transfers, g.raptor.use_cch_access,
            &rt, &am, &bike, None,
            maas_rs::structures::cost::FareProfile::default(),
        )
    };

    g.set_use_cch_access(false);
    let off = run(&g);
    g.set_use_cch_access(true);
    let on = run(&g);

    assert_eq!(
        off.access.origin_stops_found, 1,
        "flag OFF: bounded two-pass finds only NEAR within 600 s"
    );
    assert_eq!(
        on.access.origin_stops_found, 3,
        "flag ON: unbounded CCH finds NEAR + FAR + NEARD"
    );
    assert!(
        on.access.origin_stops_found > off.access.origin_stops_found,
        "toggling use_cch_access must widen the discovered access set"
    );
}

// ── Realtime CANCELED-trip correctness ────────────────────────────────────────
//
// These validate the canceled-trips fix (sites A1/A2/A2b/B1/B2/C1/D1). Every
// guard is `&& !rt.is_canceled(trip)` beside the schedule-activity check, so an
// empty index is byte-identical (proven by the rest of the suite); here we assert
// the *with-feed* behaviour: a canceled trip is never boarded, never used as a
// miss-fallback, and never selected by the tightening oracle.

/// Direct single-route graph: origin → [Stop P] → Bus(P,Q) → [Stop Q] → dest.
/// Bus has TWO trips: T0 dep 08:00 arr 08:30, T1 dep 09:00 arr 09:30. Returns the
/// two street endpoints and both stop nodes (needed to call the oracle directly).
fn direct_bus_two_trip_graph() -> (Graph, NodeID, NodeID, NodeID, NodeID) {
    direct_bus_two_trip_graph_perm(true, true)
}

/// Like `direct_bus_two_trip_graph`, but the later bus (T1, 09:00) can have its
/// boarding permission at P (`t1_board`) or alighting permission at Q (`t1_alight`)
/// suppressed — modelling GTFS `pickup_type == 1` / `drop_off_type == 1` at a
/// terminal on a same-pattern later trip (the shape that traps the tightening
/// oracle into re-timing a leg onto an un-boardable/un-alightable trip).
fn direct_bus_two_trip_graph_perm(
    t1_board: bool,
    t1_alight: bool,
) -> (Graph, NodeID, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));
    let stop_p = g.add_node(transit_stop("Stop P", 50.000, 4.001));
    let stop_q = g.add_node(transit_stop("Stop Q", 50.000, 4.040));

    // A long walkable street spine so origin/dest are connected for access/egress.
    g.add_edge(osm_origin, street_edge(osm_origin, osm_dest, 3000));
    g.add_edge(osm_dest, street_edge(osm_dest, osm_origin, 3000));

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (a, b) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                a,
                EdgeData::Street(StreetEdgeData {
                    origin: a,
                    destination: b,
                    length: m,
                    partial: true,
                    foot: true,
                    bike: false,
                    car: false,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_snap(&mut g, stop_p, osm_origin, 72);
    add_snap(&mut g, stop_q, osm_dest, 72);

    g.add_edge(
        stop_p,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_p,
            destination: stop_q,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 2 },
            length: 3000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Bus 1".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(vec![
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(0) = 08:00
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }, // TripId(1) = 09:00
    ]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 8 * 3600,
            arrival: 8 * 3600 + 1800,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 9 * 3600,
            arrival: 9 * 3600 + 1800,
            service_id: ServiceId(0),
        },
    ]);

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_p, stop_q]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });

        let sts = g.transit_pattern_stop_times_len();
        // Stop P column (departures), then Stop Q column (arrivals).
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600,
            departure: 8 * 3600,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600,
            departure: 9 * 3600,
            board_allowed: t1_board,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 8 * 3600 + 1800,
            departure: 8 * 3600 + 1800,
            ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1800,
            departure: 9 * 3600 + 1800,
            alight_allowed: t1_alight,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });

        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 2,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest, stop_p, stop_q)
}

fn first_transit_leg(plan: &maas_rs::structures::plan::Plan) -> &maas_rs::structures::plan::PlanTransitLeg {
    plan.legs
        .iter()
        .find_map(|l| match l {
            PlanLeg::Transit(t) => Some(t),
            _ => None,
        })
        .expect("plan should have a transit leg")
}

/// A1: the forward search must not board a CANCELED trip — it boards the next
/// running trip instead. Cancelling the 08:00 bus (T0) shifts boarding to the
/// 09:00 bus (T1); the arrival moves back by exactly one hour.
#[test]
fn canceled_trip_is_not_boarded_next_runner_used() {
    let (g, o, d, _, _) = direct_bus_two_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    let base_plan = base.iter().min_by_key(|p| p.end).expect("a baseline plan");
    assert_eq!(
        first_transit_leg(base_plan).trip_id,
        TripId(0),
        "baseline should board the earliest bus (T0, 08:00)"
    );
    let base_end = base_plan.end;

    let rt = RealtimeIndex::from_updates(1, [], [TripId(0)]);
    let canc = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    let canc_plan = canc.iter().min_by_key(|p| p.end).expect("a plan on the next bus");
    assert_eq!(
        first_transit_leg(canc_plan).trip_id,
        TripId(1),
        "with T0 canceled the search must board T1 (09:00), never the dead T0"
    );
    assert_eq!(
        canc_plan.end,
        base_end + 3600,
        "boarding the next runner (09:00 vs 08:00) pushes arrival back one hour"
    );
}

/// A2: the tightening oracle (`latest_departure_before_arrival`, shared by
/// chain_bounds and tighten_with_bounds) must never select a CANCELED trip.
/// With both bus trips feasible it returns the latest (T1); cancelling T1 makes
/// it fall back to T0.
#[test]
fn tightening_oracle_skips_canceled_trip() {
    let (g, _, _, p, q) = direct_bus_two_trip_graph();

    let empty = RealtimeIndex::new();
    let (_, dep_latest, _) = g
        .latest_departure_before_arrival(p, q, 0, 10 * 3600, 0, 0x7F, &empty)
        .expect("some feasible bus with no feed");
    assert_eq!(dep_latest, 9 * 3600, "latest feasible bus is T1 (09:00)");

    let rt = RealtimeIndex::from_updates(1, [], [TripId(1)]);
    let (_, dep_after, _) = g
        .latest_departure_before_arrival(p, q, 0, 10 * 3600, 0, 0x7F, &rt)
        .expect("T0 still feasible when T1 is canceled");
    assert_eq!(
        dep_after,
        8 * 3600,
        "with T1 canceled the oracle must fall back to T0 (08:00), never the dead T1"
    );
}

/// Bug #5 (permission-blind tightening): the shared oracle must never select a
/// same-pattern later trip that forbids BOARDING at the boarding stop
/// (GTFS pickup_type == 1). With T1 (09:00) un-boardable at P the oracle must
/// fall back to the boardable T0 (08:00), just as it does for a canceled trip.
#[test]
fn tightening_oracle_skips_unboardable_trip() {
    let (g, _, _, p, q) = direct_bus_two_trip_graph_perm(false, true);
    let empty = RealtimeIndex::new();

    let (_, dep_latest, _) = g
        .latest_departure_before_arrival(p, q, 0, 10 * 3600, 0, 0x7F, &empty)
        .expect("T0 remains feasible when the later T1 forbids boarding");
    assert_eq!(
        dep_latest,
        8 * 3600,
        "oracle must skip the un-boardable T1 (pickup_type=1 at P) and keep T0 (08:00)"
    );
}

/// Bug #5, alight half: the oracle must never select a same-pattern later trip
/// that forbids ALIGHTING at the alighting stop (GTFS drop_off_type == 1). With
/// T1 (09:00) un-alightable at Q the oracle must fall back to T0 (08:00).
#[test]
fn tightening_oracle_skips_unalightable_trip() {
    let (g, _, _, p, q) = direct_bus_two_trip_graph_perm(true, false);
    let empty = RealtimeIndex::new();

    let (_, dep_latest, _) = g
        .latest_departure_before_arrival(p, q, 0, 10 * 3600, 0, 0x7F, &empty)
        .expect("T0 remains feasible when the later T1 forbids alighting");
    assert_eq!(
        dep_latest,
        8 * 3600,
        "oracle must skip the un-alightable T1 (drop_off_type=1 at Q) and keep T0 (08:00)"
    );

    // Both permissions allowed ⇒ the oracle picks the latest (T1), proving the
    // guard is the only thing suppressing it above (not some other exclusion).
    let (g_ok, _, _, p2, q2) = direct_bus_two_trip_graph_perm(true, true);
    let (_, dep_ok, _) = g_ok
        .latest_departure_before_arrival(p2, q2, 0, 10 * 3600, 0, 0x7F, &empty)
        .expect("a feasible bus");
    assert_eq!(
        dep_ok, 9 * 3600,
        "with both permissions the oracle picks the latest feasible trip (T1, 09:00)"
    );
}

/// A2 (end-to-end) + S1: three-pass tightening normally shifts the bus leg to the
/// later 09:00 trip (T1). Cancelling T1 forces the oracle to keep T0, and the
/// re-chained plan must still hold a non-negative transfer margin (S1 invariant;
/// the debug_assert in `tighten_with_bounds` also arms here in test/debug builds).
#[test]
fn tightening_falls_back_when_preferred_trip_canceled() {
    let (g, o, d) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    let base_plan = base
        .iter()
        .find(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2)
        .expect("a bus+tram plan");
    assert_eq!(
        first_transit_leg(base_plan).start,
        9 * 3600,
        "baseline tightening shifts the bus to the 09:00 trip (T1)"
    );

    let rt = RealtimeIndex::from_updates(1, [], [TripId(1)]);
    let canc = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    let canc_plan = canc
        .iter()
        .find(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2)
        .expect("a bus+tram plan with T1 canceled");
    let bus = first_transit_leg(canc_plan);
    assert_eq!(
        bus.trip_id,
        TripId(0),
        "with T1 canceled the tightening oracle must keep T0 (08:00), never the dead T1"
    );
    assert_eq!(bus.start, 8 * 3600, "kept-T0 bus departs at 08:00");

    // S1: every reconstructed transfer margin stays non-negative.
    for leg in &canc_plan.legs {
        if let PlanLeg::Transit(t) = leg {
            if let Some(prev_arr) = t.preceding_arrival {
                assert!(
                    t.start >= prev_arr,
                    "negative transfer margin after canceled-trip fallback: start={} prev_arr={}",
                    t.start,
                    prev_arr
                );
            }
        }
    }
}

/// A1 (egress side): cancelling the ONLY trip of the final leg must remove the
/// journey entirely — the search cannot board the dead vehicle, and there is no
/// alternative, so no through plan survives.
#[test]
fn canceling_sole_final_leg_trip_removes_plan() {
    let (g, o, d) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    assert!(
        base.iter().any(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2),
        "baseline has a bus+tram plan"
    );

    // TripId(2) is the sole tram; cancel it.
    let rt = RealtimeIndex::from_updates(1, [], [TripId(2)]);
    let canc = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    assert!(
        !canc.iter().any(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2),
        "with the only tram canceled, no bus+tram plan may be produced"
    );
}

/// Two-leg graph with a feeder Bus (delay model) and a Tram with THREE trips, so
/// a missed connection has a real next-running fallback. Origin→Bus(A,B)→walk→
/// Tram(C,D)→dest.
fn bus_tram_three_trip_graph() -> (Graph, NodeID, NodeID) {
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
        g.add_edge(a, street_edge(a, b, m));
        g.add_edge(b, street_edge(b, a, m));
    };
    add_street(&mut g, osm_origin, osm_ab, 718);
    add_street(&mut g, osm_ab, osm_b, 645);
    add_street(&mut g, osm_b, osm_cd, 789);
    add_street(&mut g, osm_cd, osm_dest, 789);

    let add_snap = |g: &mut Graph, stop: NodeID, osm: NodeID, m: usize| {
        for (a, b) in [(stop, osm), (osm, stop)] {
            g.add_edge(
                a,
                EdgeData::Street(StreetEdgeData {
                    origin: a,
                    destination: b,
                    length: m,
                    partial: true,
                    foot: true,
                    bike: false,
                    car: false,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_b, 72);
    add_snap(&mut g, stop_c, osm_b, 215);
    add_snap(&mut g, stop_d, osm_dest, 72);

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
            timetable_segment: TimetableSegment { start: 1, len: 3 },
            length: 1290,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
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
    // T0 = bus; T1/T2/T3 = trams (10 min apart).
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(1), service_id: ServiceId(0), bikes_allowed: None },
    ]);
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 28800, arrival: 29100, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 30000, arrival: 30600, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 30600, arrival: 31200, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(3), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 31200, arrival: 31800, service_id: ServiceId(0) },
    ]);

    // Pattern 0: Bus [A,B], 1 trip.
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        g.push_transit_pattern_stop_time(StopTime { arrival: 28800, departure: 28800, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 29100, departure: 29100, ..Default::default() });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    // Pattern 1: Tram [C,D], 3 trips. Column-major: C col (3 deps), D col (3 arrs).
    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_pattern_trip(TripId(3));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 3 });
        let sts = g.transit_pattern_stop_times_len();
        // Stop C departures.
        g.push_transit_pattern_stop_time(StopTime { arrival: 30000, departure: 30000, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 30600, departure: 30600, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 31200, departure: 31200, ..Default::default() });
        // Stop D arrivals.
        g.push_transit_pattern_stop_time(StopTime { arrival: 30600, departure: 30600, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 31200, departure: 31200, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 31800, departure: 31800, ..Default::default() });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 6 });
        g.push_transit_pattern(PatternInfo { route: RouteId(1), num_trips: 3 });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

/// A2b: the lambda backward pass (`raptor_backward` /
/// `latest_trip_arriving_at_stop_before`) must exclude CANCELED trips with the
/// SAME predicate the chain sweep uses, so the two never diverge under the DIFF
/// gate. With a loose target the backward pass would otherwise credit the latest
/// tram (T3); cancelling it must drop the bound to T2's connection, then T1's —
/// and `chain_bounds` must agree at every step.
#[test]
fn lambda_backward_pass_excludes_canceled_and_matches_chain() {
    let (g, origin, dest) = bus_tram_three_trip_graph();
    let date = 0;
    let weekday = 0x7F;
    let plans = g.raptor(origin, dest, 7 * 3600, date, weekday, 10 * 60);
    let plan = plans
        .iter()
        .find(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2)
        .expect("a bus+tram plan");
    let legs = plan.legs.clone();
    let target_stop = plan
        .legs
        .iter()
        .rev()
        .find_map(|l| match l {
            PlanLeg::Transit(t) => Some(g.compact_stop_of_node(t.to.node_id).unwrap()),
            _ => None,
        })
        .unwrap();
    // Loose target at stop D: any of T1/T2/T3 (arr 30600/31200/31800) qualifies,
    // so the backward pass credits the LATEST running tram.
    let target = 31800;

    let lam = |rt: &RealtimeIndex| g.bounds_from_lambda_pub(&legs, target_stop, target, 2, date, weekday, rt);
    let chn = |rt: &RealtimeIndex| g.chain_bounds_pub(&legs, target_stop, target, date, weekday, rt);

    let empty = RealtimeIndex::new();
    let cancel_t3 = RealtimeIndex::from_updates(1, [], [TripId(3)]);
    let cancel_t23 = RealtimeIndex::from_updates(1, [], [TripId(2), TripId(3)]);

    // The feeder-leg bound tightens by exactly one 600s tram interval as each
    // later tram is removed — proving the guard actually excludes them.
    assert_eq!(lam(&empty)[0], 31021, "empty: bound credits T3");
    assert_eq!(lam(&cancel_t3)[0], 30421, "T3 canceled: bound falls back to T2");
    assert_eq!(lam(&cancel_t23)[0], 29821, "T2+T3 canceled: bound falls back to T1");

    // Lambda == chain under every cancellation (the A2b parity the DIFF gate needs).
    for rt in [&empty, &cancel_t3, &cancel_t23] {
        assert_eq!(lam(rt), chn(rt), "lambda backward bounds must equal chain bounds under cancellation");
    }
}

/// B1: the miss-scenario in a plan's arrival distribution must use the next
/// RUNNING trip, not a canceled one. With a flat Bus delay model the connection
/// onto the first tram (T1) is uncertain, so the arrival bag carries a second
/// (miss) scenario at the next tram's arrival. Baseline: miss = T2 (gap 600s).
/// Cancel T2: the miss must skip it and land on T3 (gap 1200s) — never the dead
/// T2 (which would corrupt the reliability bag).
#[test]
fn miss_scenario_uses_next_running_trip_not_canceled() {
    let (mut g, o, d) = bus_tram_three_trip_graph();
    let mut models = HashMap::new();
    // Flat 0.6 on-time probability for any positive margin < 2h, so the tram
    // connection is uncertain (hit_prob 0.6) yet every tram shares one bucket
    // (only T1 is boarded) — isolating the miss-fallback under test.
    models.insert(RouteType::Bus, DelayCDF { bins: vec![(0, 0.6), (7200, 1.0)] });
    g.set_transit_delay_models(models);
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let two_scenario_gap = |plans: &[maas_rs::structures::plan::Plan]| -> u32 {
        let plan = plans
            .iter()
            .filter(|p| p.arrival_distribution.len() >= 2)
            .min_by_key(|p| p.end)
            .expect("a plan with a two-scenario arrival distribution");
        let mut times: Vec<u32> = plan.arrival_distribution.iter().map(|s| s.time).collect();
        times.sort_unstable();
        times[1] - times[0]
    };

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    assert_eq!(
        two_scenario_gap(&base),
        600,
        "baseline miss-scenario is the next tram T2 (arr +600s after T1)"
    );

    let rt = RealtimeIndex::from_updates(1, [], [TripId(2)]);
    let canc = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    assert_eq!(
        two_scenario_gap(&canc),
        1200,
        "with T2 canceled the miss-scenario must skip it and use T3 (arr +1200s)"
    );
}

/// B1 (delay term): the miss arrival returned by `next_trip_arrival` must be
/// shifted by the live delay on that running trip — parity with the hit branch,
/// which already delays. Delaying the next running tram (T2) at its alighting
/// stop by 300s must widen the two-scenario gap by exactly 300s over the
/// no-delay baseline; the empty-delay-map cancellation test leaves this
/// `apply_delay(col[t].arrival, rt.delay(..))` term as a no-op, so this arms it.
#[test]
fn miss_scenario_applies_realtime_delay_to_fallback_arrival() {
    let (mut g, o, d) = bus_tram_three_trip_graph();
    let mut models = HashMap::new();
    models.insert(RouteType::Bus, DelayCDF { bins: vec![(0, 0.6), (7200, 1.0)] });
    g.set_transit_delay_models(models);
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let two_scenario_gap = |plans: &[maas_rs::structures::plan::Plan]| -> u32 {
        let plan = plans
            .iter()
            .filter(|p| p.arrival_distribution.len() >= 2)
            .min_by_key(|p| p.end)
            .expect("a plan with a two-scenario arrival distribution");
        let mut times: Vec<u32> = plan.arrival_distribution.iter().map(|s| s.time).collect();
        times.sort_unstable();
        times[1] - times[0]
    };

    // Baseline (no realtime): miss-scenario is T2's scheduled arrival (gap 600s).
    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    let base_gap = two_scenario_gap(&base);
    assert_eq!(base_gap, 600, "baseline miss = T2 (arr +600s after T1)");

    // Delay the running fallback tram (T2) by 300s at its alighting stop (D). The
    // hit (T1) is untouched, so the miss arrival — and only it — shifts by +300s.
    let stop_d = g.stop_index_of("Stop D").expect("Stop D compact index") as u32;
    let rt = RealtimeIndex::from_delays(1, [((TripId(2), stop_d), 300)]);
    let delayed = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    assert_eq!(
        two_scenario_gap(&delayed),
        base_gap + 300,
        "a +300s live delay on the running fallback tram must push the miss \
         scenario back by exactly 300s (B1 apply_delay term)"
    );
}


// ── date+1 forward midnight-rollover extension ──────────────────────────────────

/// Single bus route whose only departures are the given next-service-day times
/// (each trip is a 10-min A→B hop). A late-night query on the previous day whose
/// window crosses midnight can only reach them via the date+1 forward extension.
///
///   osm_origin (50.000, 4.000) ─72 m─ stop_A (50.000, 4.001)
///                                        │ (bus at `deps`)
///   osm_dest   (50.000, 4.100) ─72 m─ stop_B (50.000, 4.099)
fn next_day_route_graph(deps: &[u32]) -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();
    let n = deps.len() as u32;

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099));

    let add_edge_pair = |g: &mut Graph, a: NodeID, b: NodeID, m: usize, partial: bool| {
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    length: m,
                    partial,
                    foot: true,
                    bike: !partial,
                    car: !partial,
                    attrs: BikeAttrs::road_default(),
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    };
    add_edge_pair(&mut g, osm_origin, osm_dest, 7200, false);
    add_edge_pair(&mut g, stop_a, osm_origin, 72, true);
    add_edge_pair(&mut g, stop_b, osm_dest, 72, true);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: n as usize },
            length: 7000,
        }),
    );

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "N1".into(),
        route_long_name: "Night 1".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(
        (0..n)
            .map(|_| TripInfo {
                trip_headsign: None,
                route_id: RouteId(0),
                service_id: ServiceId(0),
                bikes_allowed: None,
            })
            .collect(),
    );

    g.add_transit_departures(
        (0..n)
            .map(|i| TripSegment {
                trip_id: TripId(i),
                origin_stop_sequence: 0,
                destination_stop_sequence: 1,
                departure: deps[i as usize],
                arrival: deps[i as usize] + 600,
                service_id: ServiceId(0),
            })
            .collect(),
    );

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        for i in 0..n {
            g.push_transit_pattern_trip(TripId(i));
        }
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: n as usize });

        let sts = g.transit_pattern_stop_times_len();
        for &t in deps {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
                ..Default::default()
            });
        }
        for &t in deps {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t + 600,
                departure: t + 600,
                ..Default::default()
            });
        }
        g.push_transit_idx_pattern_stop_times(Lookup {
            start: sts,
            len: 2 * n as usize,
        });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: n,
        });
    }

    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, osm_origin, osm_dest)
}

/// Next-day service at 00:15 and 00:45 — both land inside a 23:30 w120 crossing
/// window's midnight tail, so they are legitimate forward gains.
fn after_midnight_route_graph() -> (Graph, NodeID, NodeID) {
    next_day_route_graph(&[900, 2700])
}

fn has_transit_leg(plans: &[maas_rs::structures::plan::Plan]) -> bool {
    plans
        .iter()
        .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
}

fn origin_dest_ep() -> QueryEndpoints {
    QueryEndpoints {
        origin: LatLng {
            latitude: 50.000,
            longitude: 4.000,
        },
        destination: LatLng {
            latitude: 50.000,
            longitude: 4.100,
        },
        origin_station: None,
        destination_station: None,
    }
}

/// Repro + fix: a late-night window query that crosses midnight must surface the
/// next service day's early-morning trip. The plain (non-overnight) window driver
/// misses it — reproducing the reported "no plan / walk-only" bug — while the
/// overnight driver's forward extension finds it, boarding the 00:15 trip displayed
/// in the query day's frame as 24:15 (87300).
#[test]
fn forward_extension_finds_next_day_early_trip() {
    let (g, origin, dest) = after_midnight_route_graph();
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = origin_dest_ep();
    let am = ActiveModes::default();
    let bike = BikeCost::new(BikeProfile::default());
    let rt = RealtimeIndex::new();

    let start = 84600u32; // 23:30
    let window = 120 * 60u32; // 120 min → [23:30, Sat 01:30] crosses midnight
    let date = 100u32;
    let weekday = 0x10u8; // Friday (all-days service, so any weekday works)
    let min_access = 10 * 60u32;

    // Master behaviour: the plain driver only explores the query day, whose only
    // departures (00:15/00:45) are BEFORE the 23:30 start → no transit plan.
    let base = g.raptor_range_tuned_rt_modes_ep(
        origin,
        dest,
        start,
        window,
        date,
        weekday,
        min_access,
        &buckets,
        g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &rt,
        &am,
        &bike,
        Some(&ep),
        maas_rs::structures::cost::FareProfile::default(),
    );
    assert!(
        !has_transit_leg(&base),
        "plain window driver must miss the next-day trip (bug repro)"
    );

    // The forward extension surfaces the Saturday 00:15 trip.
    let fixed = g.raptor_range_tuned_rt_overnight_modes(
        origin,
        dest,
        start,
        window,
        date,
        weekday,
        min_access,
        &buckets,
        g.raptor.arrival_slack_secs,
        g.raptor.unrestricted_transfers,
        g.raptor.use_cch_access,
        &rt,
        &am,
        &bike,
        Some(&ep),
        maas_rs::structures::cost::FareProfile::default(),
    );
    assert!(
        has_transit_leg(&fixed),
        "forward extension must surface the next-day trip"
    );

    let leg = fixed
        .iter()
        .flat_map(|p| &p.legs)
        .find_map(|l| match l {
            PlanLeg::Transit(t) => Some(t),
            _ => None,
        })
        .expect("a transit leg");
    // Sign check: 00:15 (raw 900) is shifted UP by a day to 24:15 (87300).
    assert_eq!(
        leg.start, 87300,
        "boarding must be shifted +1 day (00:15 → 24:15)"
    );
    // The date+1 leg records a NEGATIVE shift so raw recovery works.
    assert_eq!(leg.time_shift, -86400, "date+1 leg records a negative time_shift");
    // Raw recovery (used by prev/nextDepartures) lands on the Saturday-listed 900.
    assert_eq!(
        leg.start as i64 + leg.time_shift,
        900,
        "raw = displayed + time_shift must recover the next-day-listed departure"
    );
    // Boarding/alighting STOP times must move with the leg (the UI reads
    // from.departure). raw dep 900 → 87300, raw arr 1500 → 87900.
    assert_eq!(
        leg.from.departure,
        Some(87300),
        "boarding-stop departure must be shifted consistently with leg.start"
    );
    assert_eq!(
        leg.to.arrival,
        Some(87900),
        "alighting-stop arrival must be shifted consistently with leg.end"
    );
    // No boarding leaks past the requested window end (Sat 01:30 = 91800).
    for p in &fixed {
        for l in &p.legs {
            if let PlanLeg::Transit(t) = l {
                assert!(
                    t.start <= start + window,
                    "forward plan boards at {} — past the window end {}",
                    t.start,
                    start + window
                );
            }
        }
    }
}

/// Window-leak guard for the date+1 forward (crossing) extension, alongside the
/// degenerate next-day fallback. When the midnight-crossing TAIL of the window
/// contains NO service departure, the range driver's unbounded earliest-arrival
/// probe boards an arbitrarily-late next-day trip; after the +86400 shift its `start`
/// lands past the window end yet a later start is Pareto-favorable, so the CROSSING
/// block must drop any gained plan whose DEPARTURE exceeds the window end — no plan
/// may leak into the in-day gap `(window_end, 86400)`.
///
/// Here the only next-day trip is at 05:30 (raw 19800) — far outside the 23:30 w120
/// tail `[0, 5400]` — so the crossing block contributes nothing. But at these
/// near-midnight starts the same-day walk arrives after midnight, so the DEGENERATE
/// fallback fires and surfaces the 05:30 trip as a properly-shifted next-day plan
/// (`start`/`end >= 86400`). The invariants: (a) no plan departs in the leak gap
/// `(window_end, 86400)`; (b) any transit plan surfaced is a next-day fallback
/// (`>= 86400`), never an in-window crossing-tail gain of the out-of-tail 05:30 trip.
#[test]
fn forward_extension_does_not_leak_past_window_on_empty_tail() {
    // Only next-day service is at 05:30, outside any reasonable crossing-window tail.
    let (g, origin, dest) = next_day_route_graph(&[5 * 3600 + 1800]); // 19800
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = origin_dest_ep();
    let am = ActiveModes::default();
    let bike = BikeCost::new(BikeProfile::default());
    let rt = RealtimeIndex::new();
    let date = 100u32;
    let weekday = 0x10u8;
    let min_access = 10 * 60u32;

    for (start, window) in [(84600u32, 120 * 60u32), (86100u32, 90 * 60u32)] {
        let window_end = start + window;
        let plans = g.raptor_range_tuned_rt_overnight_modes(
            origin,
            dest,
            start,
            window,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        // (a) No plan may leak into the in-day gap (window_end, 86400): the crossing
        // filter still blocks a same-day-clock departure past the window. A next-day
        // fallback plan departs tomorrow (start >= 86400) and is allowed.
        for p in &plans {
            assert!(
                p.start <= window_end || p.start >= 86400,
                "leak: plan departs at {} in the gap ({window_end}, 86400) (start={start}, w={window})",
                p.start
            );
        }
        // (b) The out-of-tail 05:30 trip may ONLY surface as a properly-shifted
        // next-day fallback (start/end >= 86400), never as an in-window crossing gain.
        for p in &plans {
            if p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))) {
                assert!(
                    p.start >= 86400 && p.end >= 86400,
                    "05:30 trip surfaced as an in-window gain (leak): start={} end={} (start={start}, w={window})",
                    p.start,
                    p.end
                );
            }
        }
    }

    // Control: a SERVED tail (00:15/00:45) still yields its legitimate forward gain,
    // and it departs within the window — proving the filter drops only leaks.
    let (gs, o2, d2) = after_midnight_route_graph();
    let served = gs.raptor_range_tuned_rt_overnight_modes(
        o2,
        d2,
        84600,
        120 * 60,
        date,
        weekday,
        min_access,
        &buckets,
        gs.raptor.arrival_slack_secs,
        gs.raptor.unrestricted_transfers,
        gs.raptor.use_cch_access,
        &rt,
        &am,
        &bike,
        Some(&ep),
        maas_rs::structures::cost::FareProfile::default(),
    );
    assert!(
        has_transit_leg(&served),
        "served-tail OD must still gain its in-window next-day trip"
    );
    for p in &served {
        assert!(
            p.start <= 84600 + 120 * 60,
            "served-tail gain departs past the window end: {}",
            p.start
        );
    }
}

/// The single-departure (earliest-arrival) wrapper must never POLLUTE the same-day
/// result and must only APPEND next-day plans under the degenerate gate. Two regimes
/// across the evening:
///   * A same-day plan still reaches the destination before midnight (some
///     `end < 86400`): the wrapper is a strict byte-identical no-op versus the plain
///     driver — no spurious tomorrow-morning departures (the original anti-pollution
///     guarantee, still enforced at 19:00–22:00 here where the ~1 h walk arrives
///     before midnight).
///   * No same-day plan reaches the destination before midnight (every `end >= 86400`
///     — near-midnight starts whose only option, a walk, spills past midnight): the
///     wrapper KEEPS every same-day plan byte-identical and APPENDS the genuine
///     next-day trip. Each appended plan carries next-day-clock times (`start`/`end`
///     `>= 86400`, so the UI marks it "+1 day") and is transit-bearing. This is the
///     intended, gated behaviour of the next-day fallback — a query that truly cannot
///     reach the dest today gains tomorrow's train rather than only a huge walk.
#[test]
fn single_departure_wrapper_pollutes_nothing_appends_next_day_at_evening() {
    let (g, origin, dest) = single_route_many_trips_graph();
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = origin_dest_ep();
    let am = ActiveModes::default();
    let bike = BikeCost::new(BikeProfile::default());
    let rt = RealtimeIndex::new();
    let date = 100u32;
    let weekday = 0x7Fu8;
    let min_access = 10 * 60u32;

    for &start in &[
        19 * 3600u32,
        20 * 3600,
        21 * 3600,
        22 * 3600,
        23 * 3600,
        86340, // 23:59
    ] {
        let base = g.raptor_tuned_rt_modes_ep(
            origin,
            dest,
            start,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        let wrapped = g.raptor_tuned_rt_overnight_modes(
            origin,
            dest,
            start,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );

        // Every same-day plan is preserved byte-identical (no pollution / reorder-drop).
        let wrapped_dbg: Vec<String> = wrapped.iter().map(|p| format!("{p:?}")).collect();
        for b in &base {
            assert!(
                wrapped_dbg.contains(&format!("{b:?}")),
                "same-day plan dropped/mutated at start={start}"
            );
        }
        let extras: Vec<&maas_rs::structures::plan::Plan> = wrapped
            .iter()
            .filter(|p| !base.iter().any(|b| format!("{b:?}") == format!("{p:?}")))
            .collect();

        let same_day_ok = base.iter().any(|p| p.end < 86400);
        if same_day_ok {
            assert!(
                extras.is_empty(),
                "strict no-op expected when a same-day plan reaches before midnight (start={start}); got extras {:?}",
                extras.iter().map(|p| (p.start, p.end)).collect::<Vec<_>>()
            );
        } else {
            assert!(
                !extras.is_empty(),
                "degenerate start={start}: expected an appended next-day plan"
            );
            for e in &extras {
                assert!(
                    e.start >= 86400 && e.end >= 86400,
                    "appended plan must carry next-day-clock times (start/end >= 86400); got start={} end={}",
                    e.start,
                    e.end
                );
                assert!(
                    e.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))),
                    "appended next-day plan must be transit-bearing at start={start}"
                );
            }
        }
    }
}

/// Byte-identity: for daytime queries whose window never crosses midnight (and
/// whose start is past the date-1 threshold), both overnight wrappers are provable
/// no-ops — their output is literally identical to the underlying non-overnight
/// driver. Proven over a spread of daytime start times.
#[test]
fn overnight_wrappers_are_byte_identical_for_daytime_queries() {
    let (g, origin, dest) = single_route_many_trips_graph();
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = origin_dest_ep();
    let am = ActiveModes::default();
    let bike = BikeCost::new(BikeProfile::default());
    let rt = RealtimeIndex::new();
    let date = 100u32;
    let weekday = 0x7Fu8;
    let min_access = 10 * 60u32;

    // Daytime starts (all >= OVERNIGHT_THRESHOLD 18000 so date-1 never fires),
    // with a 60-min window that never reaches 86400 so date+1 never fires either.
    let starts = [
        7 * 3600u32,
        8 * 3600,
        9 * 3600,
        10 * 3600,
        11 * 3600,
        12 * 3600,
        14 * 3600,
        16 * 3600,
        18 * 3600,
        20 * 3600,
    ];
    let window = 60 * 60u32;
    for &start in &starts {
        let base = g.raptor_range_tuned_rt_modes_ep(
            origin,
            dest,
            start,
            window,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        let wrapped = g.raptor_range_tuned_rt_overnight_modes(
            origin,
            dest,
            start,
            window,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        assert_eq!(
            format!("{base:?}"),
            format!("{wrapped:?}"),
            "range overnight wrapper must be byte-identical at start={start}"
        );

        // Single-departure driver: byte-identical at ALL these starts — it has no
        // date+1 forward extension (see single_departure_wrapper_byte_identical_at_evening),
        // and the date-1 backward extension does not fire past the 5 h threshold.
        let sbase = g.raptor_tuned_rt_modes_ep(
            origin,
            dest,
            start,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        let swrapped = g.raptor_tuned_rt_overnight_modes(
            origin,
            dest,
            start,
            date,
            weekday,
            min_access,
            &buckets,
            g.raptor.arrival_slack_secs,
            g.raptor.unrestricted_transfers,
            g.raptor.use_cch_access,
            &rt,
            &am,
            &bike,
            Some(&ep),
            maas_rs::structures::cost::FareProfile::default(),
        );
        assert_eq!(
            format!("{sbase:?}"),
            format!("{swrapped:?}"),
            "single overnight wrapper must be byte-identical at start={start}"
        );
    }
}
