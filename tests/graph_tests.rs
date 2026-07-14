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

fn three_node_street_graph() -> (Graph, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.001));
    let c = g.add_node(osm_node("c", 50.000, 4.002));
    g.add_edge(a, street_edge(a, b, 100));
    g.add_edge(b, street_edge(b, a, 100));
    g.add_edge(b, street_edge(b, c, 100));
    g.add_edge(c, street_edge(c, b, 100));
    (g, a, b, c)
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
    g.add_node(transit_stop("Central", 50.0, 4.0));
    assert!(g.get_id("Central").is_none());
}

#[test]
fn add_edge_increases_edge_count() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.0, 4.0));
    let b = g.add_node(osm_node("b", 50.0, 4.001));
    assert_eq!(g.edge_count(), 2);
    g.add_edge(a, street_edge(a, b, 100));
    assert_eq!(g.edge_count(), 2);
}


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
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.001, 4.000));
    let d = g.nodes_distance(a, b);
    assert!(d > 80 && d < 140, "Expected ~110m, got {d}");
}


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


fn make_transit_graph() -> (Graph, TimetableSegment) {
    let mut g = Graph::new();

    g.add_transit_services(vec![all_days_service()]);

    let segments = vec![
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
        TripSegment {
            trip_id: TripId(2),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 12 * 3600,
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
    assert!(g.next_transit_departure(tt, 8 * 3600, 100, 0x01).is_none());
    assert!(g.next_transit_departure(tt, 8 * 3600, 100, 0x20).is_some());
}


#[test]
fn previous_departures_from_middle_yields_earlier_trips() {
    let (g, tt) = make_transit_graph();
    let prev: Vec<_> = g.previous_departures(tt, 500, 0x7F, 2).collect();
    assert_eq!(prev.len(), 2, "Expected 2 earlier departures");
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
    let next: Vec<_> = g.next_departures(tt, 100, 0x40, 0).collect();
    assert!(next.is_empty(), "Expected no departures on Sunday");
}


#[test]
fn raptor_index_compact_stop_mapping() {
    let mut g = Graph::new();
    let osm = g.add_node(osm_node("osm1", 50.0, 4.0));
    let stop = g.add_node(transit_stop("Stop A", 50.001, 4.001));
    g.build_raptor_index();

    let dist = g.walk_dijkstra(osm, 999999);
    assert!(dist.contains_key(&osm), "Origin should be in dist map");
    assert!(
        !dist.contains_key(&stop),
        "Transit stop should not be walked through"
    );
}


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


const HUB_ORIG: &str = "ORIG";
const HUB_DEST: &str = "DEST";

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

    assert!(
        matches!(transit.legs.last(), Some(PlanLeg::Transit(_))),
        "station destination must alight directly (last leg Transit); got {:?}",
        leg_kinds(transit)
    );
    assert!(
        matches!(transit.legs.first(), Some(PlanLeg::Walk(_))),
        "coordinate origin must keep its access walk leg; got {:?}",
        leg_kinds(transit)
    );
}

#[test]
fn station_to_station_zero_cost_both_ends_keeps_transfer() {
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
    let q = station_query(Some("does-not-exist"), Some("nope"));
    let plans = route(&g, &q, &RealtimeIndex::new())
        .expect("unknown station ids fall back to coordinates and still route");

    let transit = plans
        .iter()
        .find(|p| transit_leg_count(p) >= 1)
        .expect("a transit-bearing plan");

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

    let dist = g.walk_dijkstra(a, 90);
    assert!(dist.contains_key(&b), "b (83s) should be within 90s cutoff");
    assert!(!dist.contains_key(&c), "c (166s) should exceed 90s cutoff");
}

#[test]
fn walk_dijkstra_isolated_node_not_reached() {
    let (mut g, a, _b, _c) = three_node_street_graph();
    let isolated = g.add_node(osm_node("iso", 55.0, 10.0));
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
    assert_eq!(dist[&a], 0);
}


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
    let street = g.add_node(osm_node("s1", 50.000, 4.000));
    let stop = g.add_node(transit_stop("A", 50.000, 4.000));
    g.add_edge(street, street_edge(street, stop, 50));
    g.build_raptor_index();

    let stops = g.nearby_stops(street, 9999);
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0].0, 0);
}


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
    let dist_legacy = g.walk_dijkstra(a, 99999);
    assert!(!dist_legacy.contains_key(&b));
}

#[test]
fn nearby_stops_bike_profile_reaches_farther() {
    let mut g = Graph::new();
    let street = g.add_node(osm_node("s1", 50.000, 4.000));
    let stop = g.add_node(transit_stop("A", 50.000, 4.001));
    g.add_edge(street, street_edge(street, stop, 504));
    g.build_raptor_index();

    let by_foot = g.nearby_stops_profile(street, 200, StreetProfile::Foot);
    let by_bike = g.nearby_stops_profile(street, 200, StreetProfile::Bike);
    assert!(by_foot.is_empty());
    assert_eq!(by_bike.len(), 1);
    assert_eq!(by_bike[0], (0, 120));
}


fn two_route_raptor_graph() -> (Graph, NodeID, NodeID) {
    two_route_raptor_graph_with_bikes(None, None)
}

fn two_route_raptor_graph_with_bikes(
    bus_bikes: Option<bool>,
    tram_bikes: Option<bool>,
) -> (Graph, NodeID, NodeID) {
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
            bikes_allowed: bus_bikes,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: tram_bikes,
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

fn two_operator_brussels_graph(brupass_cents: u32) -> (Graph, NodeID, NodeID) {
    use maas_rs::structures::cost::{
        FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel, TimeWindowOperator,
    };
    let (mut g, origin, dest) = two_route_raptor_graph();
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

fn two_operator_no_zone_graph() -> (Graph, NodeID, NodeID) {
    let (mut g, origin, dest) = two_operator_brussels_graph(260);
    let mut model = g.raptor.fare_model.clone();
    model.agglomerations = Vec::new();
    g.set_fare_model(model);
    (g, origin, dest)
}

#[test]
fn brupass_cap_applies_automatically_for_two_operator_in_zone_journey() {
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
    let (g, origin, dest) = two_operator_brussels_graph(1000);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    assert!(!items.iter().any(|i| i.operator == "Brupass"), "no Brupass item: {items:?}");
    let paid: Vec<_> = items.iter().filter(|i| i.euros > 0.0).collect();
    assert_eq!(paid.len(), 2, "two separate paid tickets: {items:?}");
    let sum: f64 = items.iter().map(|i| i.euros).sum();
    assert!((sum - 5.40).abs() < 1e-9, "breakdown sums to 5.40, got {sum}");
    assert!(items.iter().any(|i| i.operator == "STIB"));
    assert!(items.iter().any(|i| i.operator == "De Lijn"));
}

#[test]
fn breakdown_brupass_one_item_covered_legs_annotated() {
    let (g, origin, dest) = two_operator_brussels_graph(260);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    let brupass_items: Vec<_> = items.iter().filter(|i| i.operator == "Brupass").collect();
    assert_eq!(brupass_items.len(), 1, "exactly one Brupass item: {items:?}");
    assert!((brupass_items[0].euros - 2.60).abs() < 1e-9, "Brupass costs 2.60");
    assert!(brupass_items[0].coverage.is_none(), "the Brupass item itself is paid (coverage None)");
    assert!(
        items.iter().any(|i| i.coverage.as_deref() == Some("Brupass") && i.euros == 0.0),
        "a replaced in-zone leg is annotated: {items:?}"
    );
    let sum: f64 = items.iter().map(|i| i.euros).sum();
    assert!((sum - 2.60).abs() < 1e-9, "breakdown sums to the Brupass price 2.60, got {sum}");
}

#[test]
fn breakdown_one_stib_ticket_across_windowed_transfer() {
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
    let (mut g, origin, dest) = two_operator_brussels_graph(260);
    g.raptor.transit_routes[1].agency_id = AgencyId(0);
    let model = g.raptor.fare_model.clone();
    g.set_fare_model(model);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let min = min_two_transit_price(&g, origin, dest, profile);
    assert!(
        (min - 2.40).abs() < 1e-9 || (min - 4.80).abs() < 1e-9,
        "single-operator in-zone journey stays on STIB tickets, no Brupass, got {min}"
    );
    let items = min_two_transit_breakdown(&g, origin, dest, profile);
    assert!(!items.iter().any(|i| i.operator == "Brupass"), "no Brupass for one operator: {items:?}");
}

#[test]
fn brupass_dearer_not_forced_over_cheaper_tickets() {
    let (g, origin, dest) = two_operator_brussels_graph(1000);
    let profile = maas_rs::structures::cost::FareProfile::default();
    let min = min_two_transit_price(&g, origin, dest, profile);
    assert!(
        (min - 5.40).abs() < 1e-9,
        "a dearer Brupass must not displace the cheaper two-ticket plan, got {min}"
    );
}

#[test]
fn brupass_ignores_subscription_covered_leg() {
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
    assert!(
        items.iter().any(|i| i.operator == "STIB" && i.euros == 0.0
            && i.coverage.as_deref() == Some("STIB subscription")),
        "STIB subscription item preserved (not Brupass-covered): {items:?}"
    );
}


fn stib_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
        enabled: true,
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
    let (with_fares, origin, dest) = two_route_stib_graph();
    let (without_fares, o2, d2) = two_route_raptor_graph();
    let start = 8 * 3600 + 3000;

    let on = with_fares.raptor(origin, dest, start, 0, 0x7F, 10 * 60);
    let off = without_fares.raptor(o2, d2, start, 0, 0x7F, 10 * 60);

    assert!(
        off.iter().all(|p| p.price.is_none()),
        "disabled fares surface no Plan.price"
    );
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


fn sncb_fare_model() -> maas_rs::structures::cost::FareModel {
    use maas_rs::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel};
    FareModel {
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
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

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

fn sncb_three_stop_graph() -> Graph {
    let mut g = Graph::new();
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

    g.set_fare_model(sncb_fare_model());
    g
}

#[test]
fn sncb_railway_km_precompute_is_cumulative_and_monotonic() {
    let g = sncb_three_stop_graph();
    let cum = &g.raptor.sncb_pattern_cum_railway_m[0];
    assert_eq!(cum.len(), 3, "one cumulative entry per pattern stop");
    assert_eq!(cum[0], 0.0, "cumulative distance starts at zero");
    assert!(cum[1] >= cum[0] && cum[2] >= cum[1], "cumulative array is monotonic");

    let d01 = LatLng { latitude: 50.00, longitude: 4.00 }
        .dist(LatLng { latitude: 50.10, longitude: 4.00 });
    let d12 = LatLng { latitude: 50.10, longitude: 4.00 }
        .dist(LatLng { latitude: 50.30, longitude: 4.00 });
    assert!((cum[1] - d01).abs() < 5.0, "cum[1] ≈ rail d(0,1)");
    assert!((cum[2] - (d01 + d12)).abs() < 5.0, "cum[2] ≈ rail d(0,1)+d(1,2)");
}

#[test]
fn sncb_railway_km_falls_back_to_haversine_on_disconnected_rail() {
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
    g.set_fare_model(maas_rs::structures::cost::FareModel::default());
    assert!(
        g.raptor.sncb_pattern_cum_railway_m.is_empty(),
        "disabled fares skip the SNCB railway-km precompute entirely"
    );
}

fn sncb_routable_graph() -> (Graph, NodeID, NodeID, f64) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));

    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Gare B", 50.100, 4.001));
    let stop_c = g.add_node(transit_stop("Gare C", 50.300, 4.001));

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

fn sncb_zoned_routable_graph() -> (Graph, NodeID, NodeID, f64) {
    use maas_rs::structures::LatLng;
    use maas_rs::structures::cost::{
        Agglomeration, AgglomerationZone, FareModel, KnownEurosEpsilon, OperatorFare, OperatorModel,
    };
    let (mut g, origin, dest, _railway_ac) = sncb_routable_graph();
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
    let coords = [(50.000, 4.001), (50.100, 4.001)];
    let d_ab = LatLng { latitude: coords[0].0, longitude: coords[0].1 }
        .dist(LatLng { latitude: coords[1].0, longitude: coords[1].1 });
    (g, origin, dest, d_ab)
}

#[test]
fn sncb_end_to_end_zone_to_station_is_fixed() {
    use maas_rs::structures::cost::Agglomeration;
    let (g, origin, dest, _railway_m_ab) = sncb_zoned_routable_graph();
    assert_eq!(g.raptor.sncb_stop_zone[0], Agglomeration::None, "A outside zone");
    assert_eq!(g.raptor.sncb_stop_zone[1], Agglomeration::Brussels, "B in zone");
    assert_eq!(g.raptor.sncb_stop_zone[2], Agglomeration::Brussels, "C in zone");

    let d_fixed = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
    assert!(d_fixed > 0.0, "A→Brussels has a real fixed distance");

    let plans = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 10 * 60);
    let priced = plans
        .iter()
        .find(|p| transit_leg_count(p) == 1)
        .expect("a single-SNCB-train plan A→C");
    let price = priced.price.as_ref().expect("fares enabled ⇒ Plan.price populated");

    let expected = sncb_test_tariff().fare_cents(d_fixed / 1000.0) as f64 / 100.0;
    assert!(
        (price.known_euros - expected).abs() < 0.02,
        "zoned SNCB price is the bracketed tariff of the fixed zone distance: got {} expected ~{}",
        price.known_euros,
        expected
    );
    assert!(
        price.known_euros > 2.60,
        "zone->station fare must exceed base (ref distance must be non-zero): got {}",
        price.known_euros
    );
}


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
            airport_station_names: vec!["AIRPORT".into()],
        }],
        agglomerations: Vec::new(),
        ..FareModel::default()
    }
}

fn sncb_airport_routable_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();
    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));
    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Gare B", 50.100, 4.001));
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
    assert!(!g.raptor.sncb_airport_stop.is_empty(), "airport tags built when fares on");
    assert!(!g.raptor.sncb_airport_stop[0], "Gare A is not an airport");
    assert!(!g.raptor.sncb_airport_stop[1], "Gare B is not an airport");
    assert!(g.raptor.sncb_airport_stop[2], "Brussels Airport-Zaventem is tagged");
}

#[test]
fn sncb_airport_od_prices_fixed_7_90_end_to_end() {
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

fn shared_hub_two_access_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_hub = g.add_node(osm_node("hub", 50.150, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.300, 4.000));

    let stop_a = g.add_node(transit_stop("Gare A", 50.000, 4.001));
    let stop_h = g.add_node(transit_stop("Gare H", 50.100, 4.001));
    let stop_d = g.add_node(transit_stop("Gare D", 50.300, 4.001));
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
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_p, osm_origin, 72);
    add_snap(&mut g, stop_h, osm_hub, 72);
    add_snap(&mut g, stop_d, osm_dest, 72);

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
    g.add_transit_departures(vec![
        TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 600, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600 + 720, arrival: 9 * 3600 + 1320, service_id: ServiceId(0) },
        TripSegment { trip_id: TripId(2), origin_stop_sequence: 0, destination_stop_sequence: 1,
            departure: 9 * 3600, arrival: 9 * 3600 + 540, service_id: ServiceId(0) },
    ]);

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
    let (mut g, origin, dest) = shared_hub_two_access_graph();
    let with = g.raptor(origin, dest, 8 * 3600 + 3000, 0, 0x7F, 30 * 60);
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
    assert!(without.iter().all(|p| p.price.is_none()), "fares off ⇒ no Plan.price");

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
    add_street(&mut g, osm_b, osm_c, 1434);
    add_street(&mut g, osm_d, osm_dest, 50);

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

#[test]
fn unrestricted_transfers_find_long_inter_stop_walk() {
    let (mut g, origin, dest) = long_walk_transfer_graph();
    let start = 9 * 3600;

    let plans_off = g.raptor(origin, dest, start, 0, 0x7F, 10 * 60);
    assert!(
        plans_off.iter().all(|p| transit_leg_count(p) < 2),
        "with capped transfers no two-transit (bus+tram) plan should exist; got legs {:?}",
        plans_off.iter().map(leg_kinds).collect::<Vec<_>>()
    );

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
    assert!(
        !transfer.geometry.is_empty(),
        "the >1 km transfer leg must carry a reconstructed street geometry"
    );
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
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.008));
    let stop_a = g.add_node(transit_stop("A", 50.000, 4.0011));
    let stop_b = g.add_node(transit_stop("B", 50.000, 4.0071));
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
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_b = g.add_node(osm_node("b", 50.000, 4.010));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.100));
    let stop_board = g.add_node(transit_stop("Board", 50.000, 4.0101));
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
    road(&mut g, osm_o, osm_b, 1100);
    road(&mut g, osm_b, osm_d, 9900);
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
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_p = g.add_node(osm_node("p", 50.000, 4.090));
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
    connector(&mut g, osm_p, stop_p);
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
    let mut g = Graph::new();
    let osm_o = g.add_node(osm_node("o", 50.000, 4.000));
    let osm_near = g.add_node(osm_node("near", 50.000, 4.002));
    let osm_far = g.add_node(osm_node("far", 50.000, 4.030));
    let osm_d = g.add_node(osm_node("d", 50.000, 4.100));
    let stop_near = g.add_node(transit_stop("Near", 50.000, 4.0021));
    let stop_far = g.add_node(transit_stop("Far", 50.000, 4.0301));
    let stop_mid = g.add_node(transit_stop("Mid", 50.000, 4.060));
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
    road(&mut g, osm_o, osm_near, 140);
    road(&mut g, osm_near, osm_far, 2010);
    connector(&mut g, osm_near, stop_near);
    connector(&mut g, osm_far, stop_far);
    connector(&mut g, osm_d, stop_dest);

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
    edge(&mut g, a, b, true, true);
    edge(&mut g, b, c, true, false);
    edge(&mut g, c, d, false, true);
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
    let mut g = Graph::new();
    let o = g.add_node(osm_node("o", 50.000, 4.000));
    let p = g.add_node(osm_node("p", 50.000, 4.010));
    let stop = g.add_node(transit_stop("S", 50.000, 4.0101));
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

            assert!(t.from.arrival.is_some(), "from.arrival must be filled");
            assert!(t.to.departure.is_some(), "to.departure must be filled");

            for step in &t.steps {
                let PlanLegStep::Transit(s) = step else { continue };
                assert!(
                    s.scheduled_arrival.is_some(),
                    "each transit step must carry scheduled_arrival"
                );
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

#[test]
fn raptor_transfer_risk_merges_feeder_and_boarding_delays() {
    let (mut g, origin, dest) = two_route_raptor_graph();

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
        },
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

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_a, stop_b]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });

        let sts = g.transit_pattern_stop_times_len();
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

#[test]
fn raptor_backward_tightening_shifts_first_leg_to_later_trip() {
    let (g, origin, dest) = two_route_multi_trip_graph();

    let plans = g.raptor(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60);

    assert!(!plans.is_empty(), "Expected at least one plan");

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

#[test]
fn raptor_realtime_delay_is_per_trip() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900);
    let base_end = base.iter().map(|p| p.end).min().unwrap();

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

#[test]
fn raptor_skipped_stop_is_not_used_for_alighting() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
    fn has_transit(plans: &[maas_rs::structures::plan::Plan]) -> bool {
        plans
            .iter()
            .any(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
    }

    let empty = RealtimeIndex::new();
    let base = g.raptor_tuned_rt(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &empty);
    assert!(
        has_transit(&base),
        "baseline must reach the destination by transit (the tram)"
    );

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

#[test]
fn raptor_realtime_shows_on_leg_times() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

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

    assert_eq!(g.stib_stop_indices("0470"), vec![3]);
    assert_eq!(g.stib_stop_indices("1234"), vec![2]);
    let mut pref = g.stib_stop_indices("04707");
    pref.sort();
    assert_eq!(pref, vec![0]);
    assert!(g.stib_stop_indices("9999").is_empty());
}

#[test]
fn raptor_realtime_delay_shifts_arrival() {
    let (g, origin, dest) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned(origin, dest, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900);
    assert!(!base.is_empty(), "expected a baseline plan");
    let base_min_end = base.iter().map(|p| p.end).min().unwrap();

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


fn over_tighten_break_graph() -> (Graph, NodeID, NodeID) {
    over_tighten_break_graph_perm(true)
}

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
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
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
        for (idx, dep) in [8 * 3600, 9 * 3600, 9 * 3600 + 1200].into_iter().enumerate() {
            g.push_transit_pattern_stop_time(StopTime {
                arrival: dep,
                departure: dep,
                board_allowed: idx != 1 || t1_board,
                ..Default::default()
            });
        }
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

    assert!(
        min_transfer_margin(&legs) >= 0,
        "baseline plan must be consistent"
    );

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

    let lambda =
        g.bounds_from_lambda_pub(&legs, target_stop, target, 2, date, weekday, &RealtimeIndex::new());
    assert_eq!(
        chain, lambda,
        "chain must reproduce the backward pass on a single-line plan"
    );

    let mut over = chain.clone();
    over[0] = 9 * 3600 + 2100;
    let mut broken = legs.clone();
    g.tighten_with_bounds_pub(&mut broken, &over, date, weekday, &RealtimeIndex::new(), false, false);
    assert!(
        min_transfer_margin(&broken) < 0,
        "an over-credited bound must break the tram connection (got margin {})",
        min_transfer_margin(&broken)
    );

    let mut kept = legs.clone();
    g.tighten_with_bounds_pub(&mut kept, &chain, date, weekday, &RealtimeIndex::new(), false, true);
    assert!(
        min_transfer_margin(&kept) >= 0,
        "chain_bounds must keep the plan consistent (got margin {})",
        min_transfer_margin(&kept)
    );

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

#[test]
fn tightening_never_retimes_onto_unboardable_trip() {
    let (g, origin, dest) = over_tighten_break_graph_perm(false);
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

#[test]
fn tighten_long_transfers_flag_gates_off_table_bound() {
    let (mut g, origin, dest) = long_walk_transfer_graph();
    g.set_unrestricted_transfers(true);
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

    g.set_tighten_long_transfers(true);
    let chain_on =
        g.chain_bounds_pub(&legs, target_stop, target, date, weekday, &RealtimeIndex::new());
    assert!(
        chain_on[0] > 0,
        "flag on must tighten the long-transfer feeder leg (got {})",
        chain_on[0]
    );

    let mut tightened = legs.clone();
    g.tighten_with_bounds_pub(&mut tightened, &chain_on, date, weekday, &RealtimeIndex::new(), false, true);
    assert!(
        min_transfer_margin(&tightened) >= 0,
        "flag-on long-transfer tightening must stay consistent"
    );
}


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


fn single_route_many_trips_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099));

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
    add_street(&mut g, osm_origin, osm_dest, 7200);

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
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_dest, 72);

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

    let base = 9 * 3600u32;
    g.add_transit_departures(
        (0..6u32)
            .map(|i| TripSegment {
                trip_id: TripId(i),
                origin_stop_sequence: 0,
                destination_stop_sequence: 1,
                departure: base + i * 1800,
                arrival: base + i * 1800 + 1800,
                service_id: ServiceId(0),
            })
            .collect(),
    );

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
        for i in 0..6u32 {
            let t = base + i * 1800;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
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

    let plans = g.raptor_range(origin, dest, 9 * 3600, 180 * 60, 0, 0x7F, 10 * 60);

    assert!(
        plans.len() > 1,
        "raptor_range should return multiple Pareto-optimal plans for a 3-hour window \
         with buses every 30 min, but got {} plan(s)",
        plans.len(),
    );

    let mut starts: Vec<u32> = plans.iter().map(|p| p.start).collect();
    starts.sort_unstable();
    starts.dedup();
    assert_eq!(
        starts.len(),
        plans.len(),
        "All plans should have distinct departure times; got starts={:?}",
        starts,
    );

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

fn overtaking_pattern_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099));

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
    add_street(&mut g, osm_origin, osm_dest, 7200, false);
    add_street(&mut g, stop_a, osm_origin, 72, true);
    add_street(&mut g, stop_b, osm_dest, 72, true);

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

#[test]
fn raptor_range_overtaking_no_infeasible_departure_tag() {
    let (g, origin, dest) = overtaking_pattern_graph();

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

fn overtaking_midstop_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.050));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.000));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.051));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 4.099));

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
    add_street(&mut g, osm_origin, osm_dest, 7200, false);
    add_street(&mut g, stop_b, osm_origin, 72, true);
    add_street(&mut g, stop_c, osm_dest, 72, true);

    let a_dep = [32000u32, 32010, 32020, 32030, 32040];
    let b_arr = [32500u32, 32510, 32200, 32530, 32540];
    let c_arr = [32600u32, 32610, 32300, 32900, 32910];

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

#[test]
fn raptor_overtaking_midstop_finds_optimal_trip() {
    let (g, origin, dest) = overtaking_midstop_graph();

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

#[test]
fn build_raptor_index_yields_monotonic_departure_columns() {
    let (g, _origin, _dest) = overtaking_midstop_graph();

    let r = &g.raptor;
    let mut split_into_multiple = false;
    for p in 0..r.transit_patterns.len() {
        let n_stops = r.transit_idx_pattern_stops[p].of(&r.transit_pattern_stops).len();
        let n_trips = r.transit_patterns[p].num_trips as usize;
        let times = r.transit_idx_pattern_stop_times[p].of(&r.transit_pattern_stop_times);
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

#[test]
fn raptor_range_connecting_pattern_not_starved_by_dead_end_pattern() {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.100));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.099));
    let stop_c = g.add_node(transit_stop("Stop C", 50.000, 5.000));

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
    add_street(&mut g, osm_origin, osm_dest, 7200);

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
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_dest, 72);

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

    let base = 9 * 3600u32;
    let mut segs: Vec<TripSegment> = (0..5u32)
        .map(|i| TripSegment {
            trip_id: TripId(i),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: base + 60 + i * 60,
            arrival: base + 60 + i * 60 + 3600,
            service_id: ServiceId(0),
        })
        .collect();
    segs.extend((0..3u32).map(|i| TripSegment {
        trip_id: TripId(5 + i),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: base + 1800 + i * 3600,
        arrival: base + 1800 + i * 3600 + 1800,
        service_id: ServiceId(0),
    }));
    g.add_transit_departures(segs);

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
        for i in 0..5u32 {
            let t = base + 60 + i * 60;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
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
        for i in 0..3u32 {
            let t = base + 1800 + i * 3600;
            g.push_transit_pattern_stop_time(StopTime {
                arrival: t,
                departure: t,
            ..Default::default()
        });
        }
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

    let plans = g.raptor_range(osm_origin, osm_dest, base, 180 * 60, 0, 0x7F, 600);

    assert_eq!(
        plans.len(),
        3,
        "raptor_range should return all 3 connecting trips (09:30, 10:30, 11:30) \
         from a 180-min window, but got {} plan(s). \
         Likely the dead-end pattern starved the interesting-times cap (bug).",
        plans.len(),
    );

    for p in &plans {
        assert!(
            p.end > p.start,
            "plan end <= start: start={} end={}",
            p.start,
            p.end
        );
    }
}

#[test]
fn raptor_range_probe_gate_does_not_drop_windowed_transit_plan() {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.005));
    let stop_a = g.add_node(transit_stop("Stop A", 50.000, 4.001));
    let stop_b = g.add_node(transit_stop("Stop B", 50.000, 4.004));

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
    add_snap(&mut g, stop_a, osm_origin, 72);
    add_snap(&mut g, stop_b, osm_dest, 72);

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

    let d_dep = 33000u32;
    let d_arr = d_dep + 120;
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: d_dep,
        arrival: d_arr,
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

#[test]
fn access_search_doubles_until_walk_plan_returned() {
    let mut g = Graph::new();
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let n1 = g.add_node(osm_node("n1", 50.001, 4.0));
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

    let plans = g.raptor(n0, n1, 0, 0, 0x7F, 1);

    assert_eq!(plans.len(), 1, "expected exactly one walk-only plan");
    assert_eq!(plans[0].legs.len(), 1);
    assert!(
        matches!(plans[0].legs[0], PlanLeg::Walk(_)),
        "single leg should be a walk"
    );
}


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

#[test]
fn raptor_no_backward_walk_same_trip() {
    let (g, origin, dest, stop_a, stop_b) = backward_walk_graph();

    let plans = g.raptor(origin, dest, 9 * 3600 + 600, 0, 0x7F, 30);

    assert!(!plans.is_empty(), "expected at least one plan");

    for plan in &plans {
        let backward_walk = plan
            .legs
            .iter()
            .any(|leg| matches!(leg, PlanLeg::Walk(w) if w.to.node_id == stop_a));
        assert!(!backward_walk, "plan contains a backward walk to stop_A");

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

#[test]
fn raptor_pareto_less_walking_plan_survives() {
    let (g, origin, dest, stop_a, _stop_b) = backward_walk_graph();

    let plans = g.raptor(origin, dest, 9 * 3600 + 600, 0, 0x7F, 30);

    assert!(!plans.is_empty(), "expected at least one plan");

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
    let prev_ok: Vec<_> = g
        .previous_departures(TimetableSegment { start: 0, len: 2 }, 0, 0x7F, 1)
        .collect();
    assert_eq!(prev_ok.len(), 1);
}


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
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
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

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[stop_c, stop_d]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(1));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });
        let sts = g.transit_pattern_stop_times_len();
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

#[test]
fn raptor_returns_fast_risky_and_slow_safe() {
    let (g, origin, dest) = reliability_tradeoff_graph();
    let buckets = ReliabilityBuckets::default();

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
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
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

    let fp = [7u8; 32];
    save_osm_graph(&g, &fp, path_s).unwrap();
    let restored = load_osm_graph(path_s, &fp).unwrap();

    assert_eq!(restored.node_count(), g.node_count());
    assert_eq!(restored.get_id("a"), Some(&a));
    assert_eq!(restored.nearest_node(50.000, 4.000), Some(a));
    assert_eq!(restored.raptor.transit_trips.len(), 0);
}

#[test]
#[ignore]
fn self_pruning_range_real_network_equals_independent() {
    use maas_rs::services::persistence::load_graph_unchecked;
    use std::collections::HashSet;
    use std::time::Instant;

    let g = load_graph_unchecked("graph.bin").expect("load graph.bin");
    let buckets = ReliabilityBuckets::default();
    let date = 9657u32;
    let weekday = 0x7Fu8;
    let start = 9 * 3600u32;

    let battery = [
        ("Schuman->Uccle", 50.843, 4.381, 50.800, 4.338),
        ("Bourse->Midi", 50.848, 4.349, 50.836, 4.336),
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


fn bike_attrs(hw: HighwayClass, isbike: bool, surface: Surface) -> BikeAttrs {
    let mut a = BikeAttrs::road_default();
    a.highway = hw;
    a.isbike = isbike;
    a.surface = surface;
    a
}

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
    edge(&mut g, o, a, 600, cyc);
    edge(&mut g, a, d, 600, cyc);
    edge(&mut g, o, d, 715, prim);
    edge(&mut g, d, stop, 8, snap);

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


#[test]
fn snap_to_edge_projects_onto_long_edge_not_nearest_node() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.004));
    let c = g.add_node(osm_node("c", 50.0002, 4.002));
    g.add_edge(a, street_edge(a, b, 286));
    g.add_edge(b, street_edge(b, a, 286));
    g.add_edge(c, street_edge(c, a, 200));
    g.add_edge(a, street_edge(a, c, 200));
    g.build_edge_index();

    let (plat, plon) = (50.000, 4.002);

    assert_eq!(g.nearest_node(plat, plon), Some(c));

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
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.0085));
    let f = g.add_node(osm_node("f", 50.0001, 4.00425));
    let stub = g.add_node(osm_node("stub", 50.0010, 4.00425));
    g.add_edge(a, street_edge_flags(a, b, 607, false, true));
    g.add_edge(b, street_edge_flags(b, a, 607, false, true));
    g.add_edge(f, street_edge_flags(f, stub, 100, true, false));
    g.add_edge(stub, street_edge_flags(stub, f, 100, true, false));
    g.build_edge_index();

    let (plat, plon) = (50.000, 4.00425);

    assert_eq!(g.nearest_node(plat, plon), Some(f));

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

    let (plat, plon) = (50.0005, 4.003);

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

#[test]
fn t4_drop_g_then_route_identical() {
    use maas_rs::structures::contraction::ContractedGraph;

    let (mut g, origin, dest) = contraction_t2_graph();
    let mut cg = ContractedGraph::from_graph_union(&g);
    cg.build_seg_index();
    g.contracted = Some(cg);

    assert!(
        g.contracted.as_ref().unwrap().junction_of[origin.0] != u32::MAX,
        "origin must be a junction"
    );

    use maas_rs::structures::plan::Plan;
    let dbg = |ps: &[Plan]| ps.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>();

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

    g.drop_full_node_arrays();
    assert_eq!(g.node_count(), 0, "node arrays must be empty after the drop");

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

    let (stop_a, stop_b) = (NodeID(4), NodeID(5));

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
    assert!(
        before.stops_reached.iter().any(|s| s.name == "Stop A"),
        "stops_reached must carry the literal stop name pre-drop"
    );

    let node_before = PlanNode::from_node_id(&g, stop_a).expect("plan node pre-drop");
    assert!(
        format!("{node_before:?}").contains("Stop A"),
        "PlanNode must carry the transit-stop name pre-drop"
    );

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

#[test]
#[ignore]
fn transit_enrich_drop_gate() {
    use chrono::{NaiveDate, NaiveTime};
    use maas_rs::routing::routing_raptor::{route, RouteQuery};
    use maas_rs::services::persistence::load_graph_unchecked;
    use maas_rs::structures::{Config, RealtimeIndex};

    use maas_rs::structures::Mode;

    let mut g = load_graph_unchecked("graph_on.bin").expect("graph_on.bin");
    let config = Config::load("presets/belgium.yaml").expect("presets/belgium.yaml");
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

#[test]
fn finalize_contraction_guard() {
    use maas_rs::services::build::finalize_contraction;
    use maas_rs::structures::contraction::ContractedGraph;

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


fn station_backups_graph() -> Graph {
    let mut g = Graph::new();

    let sa = g.add_node(transit_stop("SA", 50.000, 4.000));
    let sb = g.add_node(transit_stop("SB", 50.000, 4.050));
    let sx = g.add_node(transit_stop("SX", 50.000, 4.090));

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

    push_pattern(
        &mut g,
        RouteId(0),
        &[sa, sb],
        &[TripId(2), TripId(0)],
        &[(31800, 31800), (32400, 32400), (32700, 32700), (33300, 33300)],
    );
    push_pattern(
        &mut g,
        RouteId(1),
        &[sa, sb],
        &[TripId(1)],
        &[(33000, 33000), (34200, 34200)],
    );
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

    let backups = g.station_backups(TripId(0), board, alight, 5, 5, 1, 0x01);

    let trips: Vec<_> = backups.iter().map(|b| b.trip).collect();
    assert_eq!(trips, vec![TripId(2), TripId(1)], "chronological by scheduled departure");
    assert!(!trips.contains(&TripId(0)), "reference trip excluded");
    assert!(!trips.contains(&TripId(3)), "decoy not reaching SB excluded");

    assert_eq!(backups[0].trip, TripId(2));
    assert!(backups[0].same_route);
    assert_eq!(backups[0].scheduled_departure, 31800);
    assert_eq!(backups[0].scheduled_arrival, 32700);
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

    let after = g.station_backups(TripId(0), board, alight, 0, 5, 1, 0x01);
    assert_eq!(after.iter().map(|b| b.trip).collect::<Vec<_>>(), vec![TripId(1)]);

    let before = g.station_backups(TripId(0), board, alight, 5, 0, 1, 0x01);
    assert_eq!(before.iter().map(|b| b.trip).collect::<Vec<_>>(), vec![TripId(2)]);

    assert!(g.station_backups(TripId(3), board, alight, 5, 5, 1, 0x01).is_empty());
    assert!(g.station_backups(TripId(99), board, alight, 5, 5, 1, 0x01).is_empty());
}


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

    g.add_transit_services(vec![all_days_service()]);
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
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
    ]);

    g.add_transit_departures(vec![
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

    {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[s0, s1, s2, s3]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 4 });

        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(TripId(0));
        g.push_transit_pattern_trip(TripId(2));
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });

        let sts = g.transit_pattern_stop_times_len();
        for t in [
            32400u32, 32900,
            32700, 33000,
            33000, 33300,
            34200, 34200,
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
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 33300,
            departure: 33300,
        ..Default::default()
        });
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

    let transfer = plans
        .iter()
        .find(|p| transit_legs(p) == vec![TripId(0), TripId(1)])
        .expect("an alight-and-transfer plan: onboard ride then a second transit leg");
    assert!(
        first_is_transit(transfer),
        "transfer plan first leg must be the onboard ride"
    );
}

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


use maas_rs::ingestion::osm::{OsmPlatform, PlatformIndex};
use maas_rs::structures::Connector;

fn foot_pair(g: &mut Graph, a: NodeID, b: NodeID, len: usize) {
    g.add_edge(a, street_edge(a, b, len));
    g.add_edge(b, street_edge(b, a, len));
}

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

#[test]
fn b2a_relocates_onto_platform_with_stairs() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let gnd = g.add_node(osm_node("gnd", 50.000, 4.0005));
    let p1 = g.add_node(osm_node("p1", 50.001, 4.0005));
    foot_pair(&mut g, orig, gnd, 40);
    foot_pair(&mut g, gnd, p1, 12);

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

    assert_eq!(foot_neighbors(&g, stop), vec![p1]);
    assert_eq!(g.get_node(stop).unwrap().loc().latitude, plat_loc.latitude);
    assert_eq!(g.node_level(stop), Some(1));
    assert!(
        foot_edge_len(&g, p1, orig).is_none(),
        "fallback p1→orig must NOT exist when a real path is reachable"
    );

    g.build_raptor_index();
    assert!(g.walk_dijkstra(orig, 99_999).contains_key(&stop));
}

#[test]
fn b2a_ground_path_no_fallback() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let mid = g.add_node(osm_node("mid", 50.000, 4.0004));
    let p1 = g.add_node(osm_node("p1", 50.000, 4.0008));
    foot_pair(&mut g, orig, mid, 30);
    foot_pair(&mut g, mid, p1, 30);

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

    assert_eq!(g.get_node(stop).unwrap().loc().latitude, plat_loc.latitude);
    assert!(
        foot_edge_len(&g, p1, orig).is_none(),
        "fallback p1→orig must NOT exist when ground path is reachable"
    );
    assert!(
        foot_edge_len(&g, orig, p1).is_none(),
        "fallback orig→p1 must NOT exist when ground path is reachable"
    );

    g.build_raptor_index();
    assert!(g.walk_dijkstra(orig, 99_999).contains_key(&stop));
}

#[test]
fn b2a_fallback_connector_when_no_mapped_stairs() {
    let mut g = Graph::new();
    let orig = g.add_node(osm_node("orig", 50.000, 4.0000));
    let other = g.add_node(osm_node("other", 50.000, 4.0010));
    foot_pair(&mut g, orig, other, 60);
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

#[test]
fn b2a_reachability_invariant_preserved() {
    let mut before = Graph::new();
    let o_b = before.add_node(osm_node("orig", 50.000, 4.0000));
    let stop_b = before.add_node(transit_stop("S", 50.0005, 4.0005));
    before.add_edge(stop_b, street_edge(stop_b, o_b, 20));
    before.add_edge(o_b, street_edge(o_b, stop_b, 20));
    before.build_raptor_index();
    assert!(before.walk_dijkstra(o_b, 99_999).contains_key(&stop_b));

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


fn three_stop_pattern_graph(
    a_flag: u8,
    b_flag: u8,
    c_flag: u8,
) -> (Graph, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_a = g.add_node(osm_node("oa", 50.000, 4.000));
    let osm_b = g.add_node(osm_node("ob", 50.000, 4.070));
    let osm_c = g.add_node(osm_node("oc", 50.000, 4.140));

    let stop_a = g.add_node(transit_stop("Stop A", 50.001, 4.000));
    let stop_b = g.add_node(transit_stop("Stop B", 50.001, 4.070));
    let stop_c = g.add_node(transit_stop("Stop C", 50.001, 4.140));

    let bidi = |g: &mut Graph, u: NodeID, v: NodeID, m: usize| {
        g.add_edge(u, street_edge(u, v, m));
        g.add_edge(v, street_edge(v, u, m));
    };
    bidi(&mut g, osm_a, osm_b, 5010);
    bidi(&mut g, osm_b, osm_c, 5010);
    bidi(&mut g, osm_a, stop_a, 111);
    bidi(&mut g, osm_b, stop_b, 111);
    bidi(&mut g, osm_c, stop_c, 111);

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
    let (g, osm_a, osm_b, osm_c) = three_stop_pattern_graph(0x03, 0x00, 0x03);

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



#[derive(Clone, Copy)]
struct Hop {
    board: NodeID,
    alight: NodeID,
    dep: u32,
    arr: u32,
}

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

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[board, alight]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });

    let ts = g.transit_pattern_trips_len();
    for &t in trips {
        g.push_transit_pattern_trip(t);
    }
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: n });

    let sts = g.transit_pattern_stop_times_len();
    for &d in deps {
        g.push_transit_pattern_stop_time(StopTime { arrival: d, departure: d, ..Default::default() });
    }
    for &a in arrs {
        g.push_transit_pattern_stop_time(StopTime { arrival: a, departure: a, ..Default::default() });
    }
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 * n });

    g.push_transit_pattern(PatternInfo { route, num_trips: n as u32 });
}

fn stage1_far_egress_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jz = g.add_node(osm_node("jZ", 50.000, 4.0350652));
    let jy = g.add_node(osm_node("jY", 50.000, 4.0451078));
    let d = g.add_node(osm_node("D", 50.000, 4.0501291));

    let stop_a = g.add_node(transit_stop("A", 50.000, 4.0010043));
    let stop_z = g.add_node(transit_stop("Z", 50.000, 4.0350652));
    let stop_y = g.add_node(transit_stop("Y", 50.000, 4.0451078));

    add_street_bidir(&mut g, o, jz, 2514);
    add_street_bidir(&mut g, jz, jy, 720);
    add_street_bidir(&mut g, jy, d, 360);
    add_snap_bidir(&mut g, stop_a, o, 72);
    add_snap_bidir(&mut g, stop_z, jz, 6);
    add_snap_bidir(&mut g, stop_y, jy, 6);

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

    let f_dep = [33000u32, 34200, 35400];
    let f_arr = [33600u32, 34800, 36000];
    add_two_stop_line(
        &mut g, stop_a, stop_z, RouteId(0),
        &[TripId(0), TripId(1), TripId(2)], &f_dep, &f_arr, 2438,
    );
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

fn stage1_far_access_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jfast = g.add_node(osm_node("jFast", 50.000, 4.0151486));
    let d = g.add_node(osm_node("D", 50.000, 4.0501291));

    let stop_slow = g.add_node(transit_stop("SLOW", 50.000, 4.0010043));
    let stop_fast = g.add_node(transit_stop("FAST", 50.000, 4.0151486));
    let stop_near_d = g.add_node(transit_stop("NEARD", 50.000, 4.0501291));

    add_street_bidir(&mut g, o, jfast, 1086);
    add_street_bidir(&mut g, jfast, d, 2508);
    add_snap_bidir(&mut g, stop_slow, o, 72);
    add_snap_bidir(&mut g, stop_fast, jfast, 6);
    add_snap_bidir(&mut g, stop_near_d, d, 6);

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

fn oracle_fastest_arrival(g: &Graph, origin: NodeID, dest: NodeID, dep: u32, hops: &[Hop]) -> u32 {
    let from_o = g.walk_dijkstra(origin, u32::MAX);
    let to_d = g.walk_dijkstra(dest, u32::MAX);
    let mut best = u32::MAX;
    if let Some(&w) = from_o.get(&dest) {
        best = best.min(dep.saturating_add(w));
    }
    for h in hops {
        let Some(&wa) = from_o.get(&h.board) else { continue };
        if dep.saturating_add(wa) > h.dep {
            continue;
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
    let (g, o, d, hops) = stage1_far_egress_graph();
    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
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
    let (g, o, d, hops) = stage1_far_egress_graph();
    let start = 32400;
    let window = 3 * 3600;
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


fn stage1_near_far_access_graph() -> (Graph, NodeID, NodeID, Vec<Hop>) {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());

    let o = g.add_node(osm_node("O", 50.000, 4.000000));
    let jfar = g.add_node(osm_node("jFar", 50.000, 4.0168283));
    let d = g.add_node(osm_node("D", 50.000, 4.0518168));

    let stop_near = g.add_node(transit_stop("NEAR", 50.000, 4.0010043));
    let stop_far = g.add_node(transit_stop("FAR", 50.000, 4.0168283));
    let stop_neard = g.add_node(transit_stop("NEARD", 50.000, 4.0518168));

    add_street_bidir(&mut g, o, jfar, 1206);
    add_street_bidir(&mut g, jfar, d, 2508);
    add_snap_bidir(&mut g, stop_near, o, 72);
    add_snap_bidir(&mut g, stop_far, jfar, 6);
    add_snap_bidir(&mut g, stop_neard, d, 6);

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

    add_two_stop_line(
        &mut g, stop_far, stop_neard, RouteId(0),
        &[TripId(0)], &[33600], &[34200], 2508,
    );
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

    add_street_bidir(&mut g, o, jz, 2514);
    add_street_bidir(&mut g, jz, jy, 720);
    add_street_bidir(&mut g, jy, d, 360);
    add_snap_bidir(&mut g, stop_a, o, 72);
    add_snap_bidir(&mut g, stop_z, jz, 6);
    add_snap_bidir(&mut g, stop_y, jy, 6);

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

    add_two_stop_line(
        &mut g, stop_a, stop_z, RouteId(0),
        &[TripId(0)], &[33000], &[33600], 2438,
    );
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

    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    assert_eq!(oracle, 34205, "oracle fastest = far-access FAST (via FAR)");
    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "single: must return a plan");
    assert_eq!(min_end(&plans), oracle, "single: engine {} != oracle {}", min_end(&plans), oracle);

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

    let dep = 32400;
    let oracle = oracle_fastest_arrival(&g, o, d, dep, &hops);
    assert_eq!(oracle, 34505, "oracle fastest = far-egress (via Z)");
    let plans = g.raptor(o, d, dep, 0, 0x7F, 10 * 60);
    assert!(!plans.is_empty(), "single: must return a plan");
    assert_eq!(min_end(&plans), oracle, "single: engine {} != oracle {}", min_end(&plans), oracle);

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

    assert!(
        res.plans.iter().any(|p| p.end == 34505),
        "explain must reach the far-egress arrival 34505 via Pass B; ends={:?}",
        res.plans.iter().map(|p| p.end).collect::<Vec<_>>()
    );
    let key = |p: &maas_rs::structures::plan::Plan| (p.mode, p.start, p.end, transit_leg_count(p));
    let mut a: Vec<_> = prod.iter().map(key).collect();
    let mut b: Vec<_> = res.plans.iter().map(key).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b, "raptorExplain plan set diverged from prod raptor on far egress");
    assert_eq!(res.access.access_attempts, 1, "Pass B must have run for the far egress");
    assert!(!res.access.fell_back_to_walk_only);
}


fn cch_star_graph() -> (Graph, NodeID, NodeID) {
    let mut g = Graph::new();
    let h = g.add_node(osm_node("h", 50.000, 4.000));
    let a = g.add_node(osm_node("a", 50.000, 4.001));
    let s1 = g.add_node(transit_stop("S1", 50.001, 4.000));
    let s2 = g.add_node(transit_stop("S2", 50.000, 3.999));
    g.add_edge(h, street_edge(h, a, 60));
    g.add_edge(a, street_edge(a, h, 60));
    g.add_edge(h, street_edge(h, s1, 120));
    g.add_edge(s1, street_edge(s1, h, 120));
    g.add_edge(h, street_edge(h, s2, 240));
    g.add_edge(s2, street_edge(s2, h, 240));
    g.build_raptor_index();
    enable_contraction(&mut g);
    (g, a, h)
}

#[test]
fn cch_access_matches_exact_walk() {
    let (g, a, _h) = cch_star_graph();
    let cch = g.build_cch_access();
    let a_ll = g.get_node(a).unwrap().loc();
    let radius = g.raptor.edge_snap_radius_m;

    let got = g.cch_access(&cch, a_ll);

    let node_ref = g.nearby_stops(a, u32::MAX);
    assert_eq!(got, node_ref, "cch_access must equal node-level nearby_stops");

    let cg = g.contracted.as_ref().unwrap();
    let arena_ref = cg.nearby_stops_arena(&g, a_ll.latitude, a_ll.longitude, radius, u32::MAX);
    assert_eq!(got, arena_ref, "cch_access must equal nearby_stops_arena");

    let mut secs: Vec<u32> = got.iter().map(|&(_, s)| s).collect();
    secs.sort_unstable();
    assert_eq!(secs, vec![150, 250], "hand-computed foot seconds from a");
    assert_eq!(got.len(), 2, "both stops reached");
}

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

#[test]
fn cch_split_order_matches_all_in_one() {
    let (g, a, h) = cch_star_graph();

    let order = g.compute_cch_order();
    assert_eq!(order.len(), g.cch_vertex_count(), "order permutes every junction");
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

#[test]
fn cch_build_survives_interior_array_drop() {
    let (mut g, a, h) = cch_star_graph();
    let a_ll = g.get_node(a).unwrap().loc();
    let h_ll = g.get_node(h).unwrap().loc();

    let order = g.compute_cch_order();
    let before_access = g.cch_access(&g.build_cch_access_with_order(&order), a_ll);
    let before_egress = g.cch_egress(&g.build_cch_access_with_order(&order), h_ll);

    maas_rs::services::build::finalize_contraction(&mut g).expect("drop interior arrays");

    let after_access = g.cch_access(&g.build_cch_access_with_order(&order), a_ll);
    let after_egress = g.cch_egress(&g.build_cch_access_with_order(&order), h_ll);
    assert_eq!(before_access, after_access, "access invariant to interior-array drop");
    assert_eq!(before_egress, after_egress, "egress invariant to interior-array drop");
}


#[test]
fn cch_access_is_superset_of_bounded_two_pass() {
    let (g, a, _h) = cch_star_graph();
    let cch = g.build_cch_access();
    let a_ll = g.get_node(a).unwrap().loc();
    let cg = g.contracted.as_ref().unwrap();

    let exact = g.cch_access(&cch, a_ll);
    assert_eq!(exact.len(), 2, "CCH reaches both stops; got {exact:?}");

    let r: u32 = 200;
    let bounded = g.nearby_stops_union(a, r, cg);

    let cch_within: Vec<(usize, u32)> =
        exact.iter().copied().filter(|&(_, s)| s <= r).collect();
    assert_eq!(
        cch_within, bounded,
        "CCH restricted to <= {r}s must equal nearby_stops_union(origin, {r}s); \
         cch_within={cch_within:?} bounded={bounded:?}"
    );
    assert_eq!(cch_within.len(), 1, "exactly S1 within {r}s");

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

#[test]
fn cch_ab_far_boarding_end_to_end() {
    let (mut g, o, d, hops) = stage1_near_far_access_graph();
    let far_board = hops[0].board;
    let cch = g.build_cch_access();
    g.set_cch(cch);

    let dep = 32400;
    let radius = 10 * 60;

    g.set_use_cch_access(false);
    let off = g.raptor(o, d, dep, 0, 0x7F, radius);
    assert!(!off.is_empty(), "CCH-off must return a plan");
    let off_fastest = min_end(&off);

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


fn direct_bus_two_trip_graph() -> (Graph, NodeID, NodeID, NodeID, NodeID) {
    direct_bus_two_trip_graph_perm(true, true)
}

fn direct_bus_two_trip_graph_perm(
    t1_board: bool,
    t1_alight: bool,
) -> (Graph, NodeID, NodeID, NodeID, NodeID) {
    let mut g = Graph::new();

    let osm_origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let osm_dest = g.add_node(osm_node("dest", 50.000, 4.041));
    let stop_p = g.add_node(transit_stop("Stop P", 50.000, 4.001));
    let stop_q = g.add_node(transit_stop("Stop Q", 50.000, 4.040));

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
        },
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        },
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

    let (g_ok, _, _, p2, q2) = direct_bus_two_trip_graph_perm(true, true);
    let (_, dep_ok, _) = g_ok
        .latest_departure_before_arrival(p2, q2, 0, 10 * 3600, 0, 0x7F, &empty)
        .expect("a feasible bus");
    assert_eq!(
        dep_ok, 9 * 3600,
        "with both permissions the oracle picks the latest feasible trip (T1, 09:00)"
    );
}

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

#[test]
fn canceling_sole_final_leg_trip_removes_plan() {
    let (g, o, d) = two_route_multi_trip_graph();
    let buckets = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    assert!(
        base.iter().any(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2),
        "baseline has a bus+tram plan"
    );

    let rt = RealtimeIndex::from_updates(1, [], [TripId(2)]);
    let canc = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &rt);
    assert!(
        !canc.iter().any(|p| p.legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count() == 2),
        "with the only tram canceled, no bus+tram plan may be produced"
    );
}

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
        g.push_transit_pattern_stop_time(StopTime { arrival: 30000, departure: 30000, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 30600, departure: 30600, ..Default::default() });
        g.push_transit_pattern_stop_time(StopTime { arrival: 31200, departure: 31200, ..Default::default() });
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
    let target = 31800;

    let lam = |rt: &RealtimeIndex| g.bounds_from_lambda_pub(&legs, target_stop, target, 2, date, weekday, rt);
    let chn = |rt: &RealtimeIndex| g.chain_bounds_pub(&legs, target_stop, target, date, weekday, rt);

    let empty = RealtimeIndex::new();
    let cancel_t3 = RealtimeIndex::from_updates(1, [], [TripId(3)]);
    let cancel_t23 = RealtimeIndex::from_updates(1, [], [TripId(2), TripId(3)]);

    assert_eq!(lam(&empty)[0], 31021, "empty: bound credits T3");
    assert_eq!(lam(&cancel_t3)[0], 30421, "T3 canceled: bound falls back to T2");
    assert_eq!(lam(&cancel_t23)[0], 29821, "T2+T3 canceled: bound falls back to T1");

    for rt in [&empty, &cancel_t3, &cancel_t23] {
        assert_eq!(lam(rt), chn(rt), "lambda backward bounds must equal chain bounds under cancellation");
    }
}

#[test]
fn miss_scenario_uses_next_running_trip_not_canceled() {
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

    let base = g.raptor_tuned_rt(o, d, 7 * 3600, 0, 0x7F, 10 * 60, &buckets, 900, &RealtimeIndex::new());
    let base_gap = two_scenario_gap(&base);
    assert_eq!(base_gap, 600, "baseline miss = T2 (arr +600s after T1)");

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

#[test]
fn forward_extension_finds_next_day_early_trip() {
    let (g, origin, dest) = after_midnight_route_graph();
    let buckets = ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges);
    let ep = origin_dest_ep();
    let am = ActiveModes::default();
    let bike = BikeCost::new(BikeProfile::default());
    let rt = RealtimeIndex::new();

    let start = 84600u32;
    let window = 120 * 60u32;
    let date = 100u32;
    let weekday = 0x10u8;
    let min_access = 10 * 60u32;

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
    assert_eq!(
        leg.start, 87300,
        "boarding must be shifted +1 day (00:15 → 24:15)"
    );
    assert_eq!(leg.time_shift, -86400, "date+1 leg records a negative time_shift");
    assert_eq!(
        leg.start as i64 + leg.time_shift,
        900,
        "raw = displayed + time_shift must recover the next-day-listed departure"
    );
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

#[test]
fn forward_extension_does_not_leak_past_window_on_empty_tail() {
    let (g, origin, dest) = next_day_route_graph(&[5 * 3600 + 1800]);
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
        for p in &plans {
            assert!(
                p.start <= window_end || p.start >= 86400,
                "leak: plan departs at {} in the gap ({window_end}, 86400) (start={start}, w={window})",
                p.start
            );
        }
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
        86340,
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
