/// In-process GraphQL integration tests.
///
/// Tests execute queries directly against the async-graphql Schema without
/// an HTTP server, keeping them fast and hermetic.
use std::sync::Arc;

use async_graphql::{Name, Value};
use gtfs_structures::Availability;
use maas_rs::{
    ingestion::gtfs::{AgencyId, AgencyInfo, RouteId, RouteInfo},
    structures::{
        Graph, LatLng, NodeData, OsmNodeData, TransitStopData,
        raptor::{Lookup, PatternInfo},
    },
    web::app::{QueryRoot, build_schema},
};

type TestSchema = async_graphql::Schema<
    QueryRoot,
    async_graphql::EmptyMutation,
    async_graphql::EmptySubscription,
>;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Wrap a graph in the hot-swappable container the schema now expects.
fn shared(g: Graph) -> maas_rs::services::scheduler::SharedGraph {
    Arc::new(arc_swap::ArcSwap::from_pointee(g))
}

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

fn transit_stop_parent(name: &str, id: &str, lat: f64, lon: f64, parent: Option<&str>) -> NodeData {
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

fn transit_stop_with_platform(name: &str, lat: f64, lon: f64, platform: &str) -> NodeData {
    NodeData::TransitStop(TransitStopData {
        name: name.to_string(),
        lat_lng: LatLng {
            latitude: lat,
            longitude: lon,
        },
        accessibility: Availability::Available,
        id: name.to_string(),
        platform_code: Some(platform.to_string()),
        parent_station: None,
    })
}

fn execute_sync(schema: &TestSchema, query: &str) -> async_graphql::Response {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(schema.execute(query))
}

/// Returns the top-level data as an `async_graphql::Value::Object`.
fn data_obj(resp: async_graphql::Response) -> async_graphql::indexmap::IndexMap<Name, Value> {
    match resp.data {
        Value::Object(m) => m,
        other => panic!("expected Object data, got {other:?}"),
    }
}

fn enable_contraction(g: &mut maas_rs::structures::Graph) {
    use maas_rs::structures::contraction::ContractedGraph;
    let mut cg = ContractedGraph::from_graph_union(g);
    cg.build_seg_index();
    g.contracted = Some(cg);
    g.bake_bike_on_contracted_default();
}

/// Minimal bidirectional foot edge for contraction tests that need the arena-snap R-tree
/// to find a segment near query coordinates.
fn foot_street(
    origin: maas_rs::structures::NodeID,
    destination: maas_rs::structures::NodeID,
    length: usize,
) -> maas_rs::structures::EdgeData {
    maas_rs::structures::EdgeData::Street(maas_rs::structures::StreetEdgeData {
        origin,
        destination,
        length,
        partial: false,
        foot: true,
        bike: false,
        car: false,
        attrs: maas_rs::structures::BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: maas_rs::structures::cost::VarGen::NONE,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn bike_profile_input_is_exposed() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(&schema, r#"{ __type(name: "BikeProfileInput") { name } }"#);
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    match &data["__type"] {
        Value::Object(o) => assert_eq!(o["name"], Value::String("BikeProfileInput".into())),
        other => panic!("BikeProfileInput not in schema: {other:?}"),
    }
}

#[test]
fn raptor_accepts_bike_profile_input() {
    // Empty graph: routing can't snap, but the bikeProfile argument must parse and
    // validate (no "unknown argument/field" schema error) — proving it wires through.
    let schema = build_schema(shared(Graph::new()));
    let q = r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.001,
                 modes: [BIKE],
                 bikeProfile: { avoidUnsafe: false, highway: { primary: 1.0 } }) { mode } }"#;
    let resp = execute_sync(&schema, q);
    for e in &resp.errors {
        let m = e.message.to_lowercase();
        assert!(
            !m.contains("unknown"),
            "schema rejected bikeProfile: {}",
            e.message
        );
        assert!(
            !m.contains("bikeprofile"),
            "bikeProfile not wired: {}",
            e.message
        );
    }
}

#[test]
fn graphql_ping_returns_pong() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(&schema, "{ ping }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    assert_eq!(data["ping"], Value::String("pong".into()));
}

#[test]
fn graphql_raptor_no_nodes_returns_error() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) { start } }"#,
    );
    assert!(!resp.errors.is_empty(), "expected an error for empty graph");
    let msg = resp.errors[0].message.to_lowercase();
    assert!(
        msg.contains("no node"),
        "unexpected error: {}",
        resp.errors[0].message
    );
}

#[test]
fn graphql_raptor_accepts_tuning_overrides() {
    // The reliability/slack override arguments must be part of the schema. With an
    // empty graph the query still fails at routing ("no node"), but it must NOT fail
    // with an unknown-argument schema error.
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01,
                    arrivalSlackSecs: 600, reliabilityBucketEdges: [0.5, 0.9]) { start } }"#,
    );
    assert!(
        resp.errors
            .iter()
            .all(|e| !e.message.to_lowercase().contains("unknown")),
        "tuning override arguments should be recognised by the schema: {:?}",
        resp.errors
    );
}

#[test]
fn graphql_raptor_accepts_modes_argument() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01,
                    modes: [WALK, WALK_TRANSIT, BIKE, BIKE_TRANSIT, BIKE_ON_TRANSIT, BIKE_PICKUP, CAR_PICKUP])
             { start mode accessAlternatives { mode start } } }"#,
    );
    assert!(
        resp.errors.iter().all(|e| {
            let m = e.message.to_lowercase();
            !m.contains("unknown") && !m.contains("invalid value")
        }),
        "modes argument / mode fields should be part of the schema: {:?}",
        resp.errors
    );
}

#[test]
fn graphql_raptor_rejects_empty_modes() {
    let mut g = Graph::new();
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let n1 = g.add_node(osm_node("n1", 50.001, 4.001));
    g.add_edge(n0, foot_street(n0, n1, 150));
    g.add_edge(n1, foot_street(n1, n0, 150));
    g.build_raptor_index();
    enable_contraction(&mut g);
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.001, toLng: 4.001, modes: []) { start } }"#,
    );
    assert!(!resp.errors.is_empty(), "expected an error for empty modes");
    assert!(
        resp.errors[0].message.to_lowercase().contains("modes"),
        "unexpected error: {}",
        resp.errors[0].message
    );
}

#[test]
fn graphql_walk_only_plan_exposes_walk_mode() {
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.0, 4.0));
    let b = g.add_node(osm_node("b", 50.0, 4.001));
    g.add_edge(
        a,
        maas_rs::structures::EdgeData::Street(maas_rs::structures::StreetEdgeData {
            origin: a,
            destination: b,
            length: 80,
            partial: false,
            foot: true,
            bike: false,
            car: false,
            attrs: maas_rs::structures::BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: maas_rs::structures::cost::VarGen::NONE,
        }),
    );
    g.add_edge(b, foot_street(b, a, 80));
    g.build_raptor_index();
    enable_contraction(&mut g);
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.001) { mode } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    match &data["raptor"] {
        Value::List(plans) => {
            assert!(!plans.is_empty());
            match &plans[0] {
                Value::Object(p) => assert_eq!(p["mode"], Value::Enum(Name::new("WALK"))),
                other => panic!("expected plan object, got {other:?}"),
            }
        }
        other => panic!("expected plan list, got {other:?}"),
    }
}

#[test]
fn graphql_raptor_invalid_date_returns_error() {
    let mut g = Graph::new();
    g.add_node(osm_node("n0", 50.0, 4.0));
    g.build_raptor_index();
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01, date: "not-a-date") { start } }"#,
    );
    assert!(!resp.errors.is_empty());
    let msg = &resp.errors[0].message;
    assert!(
        msg.to_lowercase().contains("invalid date"),
        "expected 'invalid date' in error, got: {msg}"
    );
}

#[test]
fn graphql_gtfs_stops_empty_on_no_transit() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(&schema, "{ gtfsStops { id } }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    assert_eq!(data["gtfsStops"], Value::List(vec![]));
}

#[test]
fn graphql_gtfs_agencies_empty_on_no_transit() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(&schema, "{ gtfsAgencies { id } }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    assert_eq!(data["gtfsAgencies"], Value::List(vec![]));
}

#[test]
fn graphql_search_addresses_returns_synthetic_hits() {
    use maas_rs::structures::{AddressIndexBuilder, Named, RealtimeIndex};
    use maas_rs::web::app::{SharedAddressIndex, build_schema_full};

    let mut b = AddressIndexBuilder::new();
    let s = b.intern_street(
        "S1",
        Named {
            display: "Rue de la Loi".into(),
            aliases: vec!["Rue de la Loi".into(), "Wetstraat".into()],
        },
    );
    let m = b.intern_municipality(
        "M1",
        Named {
            display: "Bruxelles".into(),
            aliases: vec!["Bruxelles".into(), "Brussel".into()],
        },
    );
    let liege = b.intern_municipality(
        "M2",
        Named {
            display: "Liège".into(),
            aliases: vec!["Liège".into(), "Luik".into()],
        },
    );
    let p = b.intern_postal("P1", "1000".into());
    let pl = b.intern_postal("P2", "4000".into());
    b.push_record("A1".into(), s, m, p, "16".into(), String::new(), 50.846, 4.367);
    b.push_record("A2".into(), s, liege, pl, "16".into(), String::new(), 50.610, 5.500);
    let index = b.finish();

    let realtime: maas_rs::services::realtime_poller::SharedRealtime =
        Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    let address: SharedAddressIndex = Arc::new(arc_swap::ArcSwap::from_pointee(index));
    let schema = build_schema_full(shared(Graph::new()), realtime, 120, address);

    let resp = execute_sync(
        &schema,
        r#"{ searchAddresses(query: "wetstraat 16", limit: 5) {
              id label lat lon street houseNumber postcode municipality } }"#,
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    let hits = match &data["searchAddresses"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(hits.len(), 2, "same street exists in Brussels and Liège");
    let hit = match &hits[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(
        hit[&Name::new("id")],
        Value::String("A1".into()),
        "no focus → deterministic text ranking, tie-break by record id"
    );
    assert_eq!(
        hit[&Name::new("label")],
        Value::String("Rue de la Loi 16, 1000 Bruxelles".into())
    );

    let focused = execute_sync(
        &schema,
        r#"{ searchAddresses(query: "wetstraat 16", limit: 5, focusLat: 50.61, focusLng: 5.50) {
              id } }"#,
    );
    assert!(focused.errors.is_empty(), "unexpected errors: {:?}", focused.errors);
    let fdata = data_obj(focused);
    let fhits = match &fdata["searchAddresses"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    match &fhits[0] {
        Value::Object(m) => assert_eq!(
            m[&Name::new("id")],
            Value::String("A2".into()),
            "focus near Liège ranks the Liège address first"
        ),
        other => panic!("expected object, got {other:?}"),
    }

    // Attribution is exposed for the required CC-BY credit.
    let attr = execute_sync(&schema, "{ addressAttribution }");
    assert!(attr.errors.is_empty(), "unexpected errors: {:?}", attr.errors);
    let data = data_obj(attr);
    match &data["addressAttribution"] {
        Value::String(s) => assert!(s.contains("BOSA"), "attribution: {s}"),
        other => panic!("expected string, got {other:?}"),
    }
}

#[test]
fn hot_swap_is_visible_to_resolvers() {
    // The scheduler hot-swaps the graph by calling `.store()` on the shared
    // ArcSwap the schema holds. This proves a swap reaches live queries through
    // the SAME schema instance — the core auto-update promise.
    let shared_graph = shared(Graph::new());
    let schema = build_schema(shared_graph.clone());

    // Before: empty graph → no stops.
    let resp = execute_sync(&schema, "{ gtfsStops { id } }");
    let data = data_obj(resp);
    assert_eq!(
        data["gtfsStops"],
        Value::List(vec![]),
        "expected no stops before swap"
    );

    // Build a graph with one known-visible stop and swap it into the same container.
    let mut g = Graph::new();
    g.add_node(transit_stop("Central Station", 50.845, 4.357));
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 0,
    });
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Test Route".into(),
        route_type: gtfs_structures::RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.build_raptor_index();
    shared_graph.store(Arc::new(g));

    // After: the SAME schema must now see the swapped-in stop.
    let resp = execute_sync(&schema, "{ gtfsStops { id name } }");
    let data = data_obj(resp);
    let stops = match &data["gtfsStops"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(stops.len(), 1, "swap must be visible to the live schema");
    let stop_obj = match &stops[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(stop_obj["name"], Value::String("Central Station".into()));
}

#[test]
fn graphql_gtfs_stops_returns_stop_data() {
    let mut g = Graph::new();
    g.add_node(transit_stop("Central Station", 50.845, 4.357));
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 0,
    });
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Test Route".into(),
        route_type: gtfs_structures::RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.build_raptor_index();

    let schema = build_schema(shared(g));
    let resp = execute_sync(&schema, "{ gtfsStops { id name mode } }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );

    let data = data_obj(resp);
    let stops = match &data["gtfsStops"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(stops.len(), 1, "expected 1 stop, got {}", stops.len());
    let stop_obj = match &stops[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(stop_obj["name"], Value::String("Central Station".into()));
    assert_eq!(stop_obj["mode"], Value::String("Bus".into()));
}

#[test]
fn graphql_gtfs_stations_returns_station_data() {
    let mut g = Graph::new();
    // Two platforms sharing a parent_station collapse into ONE station with two
    // members; a standalone stop forms a second single-platform station.
    let p1 = g.add_node(transit_stop_parent("Gent P1", "p1", 51.000, 3.700, Some("Gent")));
    let p2 = g.add_node(transit_stop_parent("Gent P2", "p2", 51.001, 3.701, Some("Gent")));
    g.add_node(transit_stop("Solo Halt", 50.500, 4.200));
    g.add_transit_agencies(vec![AgencyInfo {
        name: "TestRail".into(),
        url: "https://testrail.example".into(),
        timezone: "Europe/Brussels".into(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Test Route".into(),
        route_type: gtfs_structures::RouteType::Rail,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[p1, p2]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });
    g.build_raptor_index();

    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        "{ gtfsStations { id name lat lon operators modes platformCount } }",
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );

    let data = data_obj(resp);
    let stations = match &data["gtfsStations"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(stations.len(), 2, "two physical stations (Gent + Solo)");

    let gent = stations
        .iter()
        .find_map(|s| match s {
            Value::Object(m) if m["id"] == Value::String("Gent".into()) => Some(m),
            _ => None,
        })
        .expect("the deduped Gent station");
    // Shape assertion: id (the StationInfo id, NOT a maas: prefix), name, lat, lon,
    // operators, platformCount all present and well-typed.
    assert_eq!(gent["id"], Value::String("Gent".into()));
    assert!(matches!(gent["name"], Value::String(_)));
    assert!(matches!(gent["lat"], Value::Number(_)));
    assert!(matches!(gent["lon"], Value::Number(_)));
    assert_eq!(
        gent["platformCount"],
        Value::Number(2.into()),
        "Gent collapses two platforms"
    );
    assert!(
        matches!(gent["operators"], Value::List(_)),
        "operators is present and a list; got {:?}",
        gent["operators"]
    );
    assert_eq!(
        gent["modes"],
        Value::List(vec![Value::String("Rail".into())]),
        "modes reports the member route types; got {:?}",
        gent["modes"]
    );
}

#[test]
fn graphql_gtfs_stations_returns_lines_per_mode() {
    let mut g = Graph::new();
    let p1 = g.add_node(transit_stop_parent("Hub P1", "p1", 51.000, 3.700, Some("HUB")));
    let p2 = g.add_node(transit_stop_parent("Hub P2", "p2", 51.001, 3.701, Some("HUB")));
    let dest = g.add_node(transit_stop("Dest", 51.010, 3.710));
    g.add_transit_agencies(vec![AgencyInfo {
        name: "Agency".into(),
        url: String::new(),
        timezone: String::new(),
    }]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "5".into(),
            route_long_name: "Bus 5".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: Some((255, 0, 0)),
            route_text_color: Some((255, 255, 255)),
        },
        RouteInfo {
            route_short_name: "81".into(),
            route_long_name: "Tram 81".into(),
            route_type: gtfs_structures::RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        },
    ]);
    for (route_id, board) in [(0u32, p1), (1u32, p2)] {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(&[board, dest]);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(route_id),
            num_trips: 1,
        });
    }
    g.build_raptor_index();

    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        "{ gtfsStations { id lines { mode shortName color textColor } } }",
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);

    let data = data_obj(resp);
    let stations = match &data["gtfsStations"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    let hub = stations
        .iter()
        .find_map(|s| match s {
            Value::Object(m) if m["id"] == Value::String("HUB".into()) => Some(m),
            _ => None,
        })
        .expect("the deduped HUB station");
    let lines = match &hub["lines"] {
        Value::List(v) => v,
        other => panic!("expected lines list, got {other:?}"),
    };
    assert_eq!(lines.len(), 2, "two distinct lines (Tram 81, Bus 5)");

    let tram = match &lines[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(tram["mode"], Value::String("Tramway".into()));
    assert_eq!(tram["shortName"], Value::String("81".into()));
    assert_eq!(tram["color"], Value::Null, "no-colour route → null");
    assert_eq!(tram["textColor"], Value::Null);

    let bus = match &lines[1] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(bus["mode"], Value::String("Bus".into()));
    assert_eq!(bus["shortName"], Value::String("5".into()));
    assert_eq!(bus["color"], Value::String("FF0000".into()), "bare 6-hex, no '#'");
    assert_eq!(bus["textColor"], Value::String("FFFFFF".into()));
}

#[test]
fn graphql_gtfs_agencies_returns_agency_and_routes() {
    let mut g = Graph::new();
    g.add_transit_agencies(vec![AgencyInfo {
        name: "TestBus".into(),
        url: "https://testbus.example".into(),
        timezone: "Europe/Brussels".into(),
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "42".into(),
        route_long_name: "Universe Express".into(),
        route_type: gtfs_structures::RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.build_raptor_index();

    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        "{ gtfsAgencies { id name url routes { shortName mode } } }",
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );

    let data = data_obj(resp);
    let agencies = match &data["gtfsAgencies"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(agencies.len(), 1, "expected 1 agency");
    let agency = match &agencies[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(agency["name"], Value::String("TestBus".into()));
    assert_eq!(
        agency["url"],
        Value::String("https://testbus.example".into())
    );

    let routes = match &agency["routes"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(routes.len(), 1);
    let route = match &routes[0] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(route["shortName"], Value::String("42".into()));
    assert_eq!(route["mode"], Value::String("Bus".into()));
}

// ── raptorExplain map fields ───────────────────────────────────────────────────

#[test]
fn graphql_raptor_explain_stops_reached_empty_no_transit() {
    let mut g = Graph::new();
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let n1 = g.add_node(osm_node("n1", 50.01, 4.01));
    g.add_edge(n0, foot_street(n0, n1, 1400));
    g.add_edge(n1, foot_street(n1, n0, 1400));
    g.build_raptor_index();
    enable_contraction(&mut g);
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) {
              stopsReached { stopIdx }
           } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let explain = match &data["raptorExplain"] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(
        explain["stopsReached"],
        Value::List(vec![]),
        "expected empty stopsReached for graph with no transit stops"
    );
}

#[test]
fn graphql_raptor_explain_origin_destination_present() {
    let mut g = Graph::new();
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    let n1 = g.add_node(osm_node("n1", 50.01, 4.01));
    g.add_edge(n0, foot_street(n0, n1, 1400));
    g.add_edge(n1, foot_street(n1, n0, 1400));
    g.build_raptor_index();
    enable_contraction(&mut g);
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) {
              origin { lat lon }
              destination { lat lon }
           } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let explain = match &data["raptorExplain"] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    let origin = match &explain["origin"] {
        Value::Object(m) => m,
        other => panic!("expected origin object, got {other:?}"),
    };
    assert!(
        matches!(origin["lat"], Value::Number(_)),
        "origin.lat should be a number"
    );
    assert!(
        matches!(origin["lon"], Value::Number(_)),
        "origin.lon should be a number"
    );
    let destination = match &explain["destination"] {
        Value::Object(m) => m,
        other => panic!("expected destination object, got {other:?}"),
    };
    assert!(matches!(destination["lat"], Value::Number(_)));
    assert!(matches!(destination["lon"], Value::Number(_)));
}

#[test]
fn graphql_raptor_explain_stops_reached_access_stop_round_zero() {
    let mut g = Graph::new();
    // OSM node close to origin
    let n0 = g.add_node(osm_node("n0", 50.0, 4.0));
    // Transit stop ~111m from origin (within MAX_TRANSFER_DISTANCE_M=1000), snapped to n0
    g.add_node(transit_stop("Test Stop", 50.001, 4.0));
    // OSM node near destination (50.02, 4.02); bidirectional edge gives contraction a foot
    // segment the arena-snap can find for both query endpoints.
    let n2 = g.add_node(osm_node("n2", 50.02, 4.02));
    g.add_edge(n0, foot_street(n0, n2, 2800));
    g.add_edge(n2, foot_street(n2, n0, 2800));
    g.build_raptor_index();
    enable_contraction(&mut g);

    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.02, toLng: 4.02) {
              stopsReached { stopIdx round name }
           } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let explain = match &data["raptorExplain"] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    let stops = match &explain["stopsReached"] {
        Value::List(v) => v,
        other => panic!("expected list, got {other:?}"),
    };
    // If a transit stop exists and is reachable within the walk radius, it should appear
    // with round = 0 (access/egress reach)
    if !stops.is_empty() {
        let stop = match &stops[0] {
            Value::Object(m) => m,
            other => panic!("expected object, got {other:?}"),
        };
        assert_eq!(
            stop["round"],
            Value::Number(async_graphql::Number::from(0i32)),
            "access stop should be round 0"
        );
    }
}

#[test]
fn graphql_walk_plan_alternatives_resolve_with_brackets() {
    use maas_rs::structures::cost::VarGen;
    use maas_rs::structures::{BikeAttrs, EdgeData, StreetEdgeData, Surface};
    let mut g = Graph::new();
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.0001));
    let c = g.add_node(osm_node("c", 50.00001, 4.00005));
    g.build_raptor_index();
    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    let mk_edge = |o, d, len, s| {
        let mut at = BikeAttrs::road_default();
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
    g.add_edge(a, mk_edge(a, b, 100, Surface::Unpaved));
    g.add_edge(a, mk_edge(a, c, 90, Surface::Paved));
    g.add_edge(c, mk_edge(c, b, 90, Surface::Paved));
    enable_contraction(&mut g);
    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.0001,
                    modes: [WALK]) {
              legs { ... on PlanWalkLeg {
                alternatives { time p50 p95 length variance }
              } }
           } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let plans = match &data["raptor"] {
        Value::List(v) => v,
        other => panic!("expected plan list, got {other:?}"),
    };
    assert!(!plans.is_empty(), "expected at least one walk plan");
    let plan = match &plans[0] {
        Value::Object(m) => m,
        other => panic!("expected plan object, got {other:?}"),
    };
    let legs = match &plan["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    assert!(!legs.is_empty(), "expected at least one leg");
    let walk_leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected walk leg object, got {other:?}"),
    };
    let alternatives = match &walk_leg["alternatives"] {
        Value::List(v) => v,
        other => panic!("expected alternatives list, got {other:?}"),
    };
    assert!(
        !alternatives.is_empty(),
        "expected at least one alternative"
    );
    for alt in alternatives {
        let alt_obj = match alt {
            Value::Object(m) => m,
            other => panic!("expected alternative object, got {other:?}"),
        };
        let p50 = match &alt_obj["p50"] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            other => panic!("expected p50 number, got {other:?}"),
        };
        let p95 = match &alt_obj["p95"] {
            Value::Number(n) => n.as_u64().unwrap_or(0),
            other => panic!("expected p95 number, got {other:?}"),
        };
        assert!(p95 >= p50, "p95={p95} must be >= p50={p50}");
        assert!(
            matches!(&alt_obj["variance"], Value::Number(_)),
            "the renamed variance axis field resolves"
        );
    }
}

// ── Transit access/egress multiobj fields ─────────────────────────────────────

#[test]
fn graphql_transit_plan_access_leg_has_alternatives_and_leave_by() {
    use gtfs_structures::{Availability, RouteType};
    use maas_rs::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::{
        BikeAttrs, EdgeData, NodeData, NodeID, StreetEdgeData, Surface, TransitEdgeData,
        TransitStopData,
        cost::VarGen,
        raptor::{Lookup, PatternInfo},
    };

    let mut g = Graph::new();

    let origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let via_acc = g.add_node(osm_node("via_acc", 50.001, 4.004));
    let hub_a = g.add_node(osm_node("hub_a", 50.000, 4.008));
    let hub_b = g.add_node(osm_node("hub_b", 50.000, 4.090));
    let via_egr = g.add_node(osm_node("via_egr", 50.001, 4.094));
    let destination = g.add_node(osm_node("dest", 50.000, 4.098));

    let stop_a = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop A".into(),
        id: "SA".into(),
        lat_lng: maas_rs::structures::LatLng {
            latitude: 50.000,
            longitude: 4.0081,
        },
        accessibility: Availability::Available,
        platform_code: None,
        parent_station: None,
    }));
    let stop_b = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop B".into(),
        id: "SB".into(),
        lat_lng: maas_rs::structures::LatLng {
            latitude: 50.000,
            longitude: 4.0901,
        },
        accessibility: Availability::Available,
        platform_code: None,
        parent_station: None,
    }));

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
    let mk_conn = |o: NodeID, d: NodeID| {
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: 8,
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

    g.add_edge(origin, mk_foot(origin, hub_a, 580, Surface::Unpaved));
    g.add_edge(hub_a, mk_foot(hub_a, origin, 580, Surface::Unpaved));
    g.add_edge(origin, mk_foot(origin, via_acc, 420, Surface::Paved));
    g.add_edge(via_acc, mk_foot(via_acc, origin, 420, Surface::Paved));
    g.add_edge(via_acc, mk_foot(via_acc, hub_a, 420, Surface::Paved));
    g.add_edge(hub_a, mk_foot(hub_a, via_acc, 420, Surface::Paved));
    g.add_edge(hub_a, mk_conn(hub_a, stop_a));
    g.add_edge(stop_a, mk_conn(stop_a, hub_a));

    g.add_edge(hub_b, mk_foot(hub_b, destination, 580, Surface::Unpaved));
    g.add_edge(
        destination,
        mk_foot(destination, hub_b, 580, Surface::Unpaved),
    );
    g.add_edge(hub_b, mk_foot(hub_b, via_egr, 420, Surface::Paved));
    g.add_edge(via_egr, mk_foot(via_egr, hub_b, 420, Surface::Paved));
    g.add_edge(via_egr, mk_foot(via_egr, destination, 420, Surface::Paved));
    g.add_edge(
        destination,
        mk_foot(destination, via_egr, 420, Surface::Paved),
    );
    g.add_edge(hub_b, mk_conn(hub_b, stop_b));
    g.add_edge(stop_b, mk_conn(stop_b, hub_b));

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

    let svc = ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    };
    g.add_transit_services(vec![svc]);
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
        arrival: 9 * 3600 + 1080,
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
            arrival: 9 * 3600 + 1080,
            departure: 9 * 3600 + 1080,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    g.build_raptor_index();
    enable_contraction(&mut g);

    let schema = build_schema(shared(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.098,
                    modes: [WALK_TRANSIT],
                    date: "2026-06-23", time: "09:00:00") {
              mode
              legs {
                ... on PlanWalkLeg {
                  alternatives { p50 p95 }
                  leaveBy
                }
              }
           } }"#,
    );
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );

    let data = data_obj(resp);
    let plans = match &data["raptor"] {
        Value::List(v) => v,
        other => panic!("expected plan list, got {other:?}"),
    };

    let transit_plan = plans
        .iter()
        .find(|p| match p {
            Value::Object(m) => m
                .get("mode")
                .map(|v| v == &Value::Enum(async_graphql::Name::new("WALK_TRANSIT")))
                .unwrap_or(false),
            _ => false,
        })
        .expect("expected a WALK_TRANSIT plan");

    let plan_obj = match transit_plan {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    let legs = match &plan_obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };

    let access_leg = legs
        .iter()
        .find_map(|l| match l {
            Value::Object(m) if m.contains_key("leaveBy") && m["leaveBy"] != Value::Null => Some(m),
            _ => None,
        })
        .expect("expected an access walk leg with non-null leaveBy");

    match &access_leg["alternatives"] {
        Value::List(alts) => assert!(
            !alts.is_empty(),
            "access leg must have non-empty alternatives"
        ),
        other => panic!("expected alternatives list, got {other:?}"),
    }

    let lby = &access_leg["leaveBy"];
    assert!(
        matches!(lby, Value::Number(_)),
        "leaveBy must be a number, got {lby:?}"
    );

    let egress_leg = legs.iter().rev().find_map(|l| match l {
        Value::Object(m) if m.contains_key("alternatives") => match &m["alternatives"] {
            Value::List(alts)
                if !alts.is_empty()
                    && m.get("leaveBy").map(|v| v == &Value::Null).unwrap_or(true) =>
            {
                Some(m)
            }
            _ => None,
        },
        _ => None,
    });
    assert!(
        egress_leg.is_some(),
        "expected an egress walk leg with non-empty alternatives"
    );
}

/// Builds a minimal WALK_TRANSIT graph: origin → Stop A —(trip T0)→ Stop B → dest.
/// The single trip carries GTFS `trip_id` "T0"; the stops carry GTFS ids "SA"/"SB".
fn transit_handles_graph() -> Graph {
    use gtfs_structures::{Availability, RouteType};
    use maas_rs::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::{
        BikeAttrs, EdgeData, NodeData, NodeID, StreetEdgeData, TransitEdgeData, TransitStopData,
        cost::VarGen,
        raptor::{Lookup, PatternInfo},
    };

    let mut g = Graph::new();

    let origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let via_acc = g.add_node(osm_node("via_acc", 50.001, 4.004));
    let hub_a = g.add_node(osm_node("hub_a", 50.000, 4.008));
    let hub_b = g.add_node(osm_node("hub_b", 50.000, 4.090));
    let via_egr = g.add_node(osm_node("via_egr", 50.001, 4.094));
    let destination = g.add_node(osm_node("dest", 50.000, 4.098));

    let stop_a = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop A".into(),
        id: "SA".into(),
        lat_lng: LatLng {
            latitude: 50.000,
            longitude: 4.0081,
        },
        accessibility: Availability::Available,
        platform_code: None,
        parent_station: None,
    }));
    let stop_b = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop B".into(),
        id: "SB".into(),
        lat_lng: LatLng {
            latitude: 50.000,
            longitude: 4.0901,
        },
        accessibility: Availability::Available,
        platform_code: None,
        parent_station: None,
    }));

    let mk_foot = |o: NodeID, d: NodeID, len: usize| {
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: len,
            partial: false,
            foot: true,
            bike: true,
            car: false,
            attrs: BikeAttrs::road_default(),
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        })
    };
    let mk_conn = |o: NodeID, d: NodeID| {
        EdgeData::Street(StreetEdgeData {
            origin: o,
            destination: d,
            length: 8,
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
    g.add_edge(origin, mk_foot(origin, via_acc, 200));
    g.add_edge(via_acc, mk_foot(via_acc, origin, 200));
    g.add_edge(via_acc, mk_foot(via_acc, hub_a, 200));
    g.add_edge(hub_a, mk_foot(hub_a, via_acc, 200));
    g.add_edge(hub_a, mk_conn(hub_a, stop_a));
    g.add_edge(stop_a, mk_conn(stop_a, hub_a));

    g.add_edge(hub_b, mk_foot(hub_b, via_egr, 200));
    g.add_edge(via_egr, mk_foot(via_egr, hub_b, 200));
    g.add_edge(via_egr, mk_foot(via_egr, destination, 200));
    g.add_edge(destination, mk_foot(destination, via_egr, 200));
    g.add_edge(hub_b, mk_conn(hub_b, stop_b));
    g.add_edge(stop_b, mk_conn(stop_b, hub_b));

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

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
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
    g.add_transit_trip_ids(vec!["T0".into()]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0,
        destination_stop_sequence: 1,
        departure: 9 * 3600 + 600,
        arrival: 9 * 3600 + 1080,
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
            arrival: 9 * 3600 + 1080,
            departure: 9 * 3600 + 1080,
            ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    }
    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

/// Pull the first PlanTransitLeg object out of a `raptor` response's plan list.
fn first_transit_leg(
    plans: &[Value],
) -> Option<async_graphql::indexmap::IndexMap<Name, Value>> {
    for p in plans {
        let Value::Object(plan) = p else { continue };
        let Some(Value::List(legs)) = plan.get("legs") else {
            continue;
        };
        for l in legs {
            if let Value::Object(m) = l {
                if m.contains_key("tripId") && m["tripId"] != Value::Null {
                    return Some(m.clone());
                }
            }
        }
    }
    None
}

const TRANSIT_HANDLES_QUERY: &str = r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.098,
            modes: [WALK_TRANSIT], date: "2026-06-23", time: "09:00:00") {
      legs {
        ... on PlanTransitLeg {
          tripId
          from { stopId }
          to { stopId }
        }
      }
   } }"#;

#[test]
fn graphql_transit_leg_exposes_trip_id_and_stop_ids() {
    let schema = build_schema(shared(transit_handles_graph()));
    let resp = execute_sync(&schema, TRANSIT_HANDLES_QUERY);
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let plans = match &data["raptor"] {
        Value::List(v) => v,
        other => panic!("expected plan list, got {other:?}"),
    };
    let leg = first_transit_leg(plans).expect("expected a transit leg with a tripId");

    assert_eq!(
        leg["tripId"],
        Value::String("T0".into()),
        "stable GTFS trip_id must be exposed on the transit leg"
    );

    let from = match &leg["from"] {
        Value::Object(m) => m,
        other => panic!("expected from object, got {other:?}"),
    };
    let to = match &leg["to"] {
        Value::Object(m) => m,
        other => panic!("expected to object, got {other:?}"),
    };
    assert_eq!(
        from["stopId"],
        Value::String("SA".into()),
        "board stop id must be the GTFS stop_id"
    );
    assert_eq!(
        to["stopId"],
        Value::String("SB".into()),
        "alight stop id must be the GTFS stop_id"
    );
}

#[test]
fn graphql_transit_leg_trip_id_is_stable_across_queries() {
    let schema = build_schema(shared(transit_handles_graph()));

    let trip_id_of = |schema: &TestSchema| -> Value {
        let resp = execute_sync(schema, TRANSIT_HANDLES_QUERY);
        assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
        let data = data_obj(resp);
        let plans = match &data["raptor"] {
            Value::List(v) => v.clone(),
            other => panic!("expected plan list, got {other:?}"),
        };
        first_transit_leg(&plans)
            .expect("expected a transit leg with a tripId")["tripId"]
            .clone()
    };

    let first = trip_id_of(&schema);
    let second = trip_id_of(&schema);
    assert_eq!(first, Value::String("T0".into()));
    assert_eq!(first, second, "tripId must be stable across repeated queries");
}

#[test]
fn graphql_realtime_generated_at_is_zero_for_empty_index() {
    let schema = build_schema(shared(Graph::new()));
    let resp = execute_sync(&schema, "{ realtimeGeneratedAt }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let generated = &data["realtimeGeneratedAt"];
    assert_ne!(generated, &Value::Null, "realtimeGeneratedAt must be present");
    let n = match generated {
        Value::Number(n) => n.as_i64().expect("generatedAt is an integer"),
        Value::String(s) => s.parse::<i64>().expect("generatedAt parses as i64"),
        other => panic!("expected number generatedAt, got {other:?}"),
    };
    assert_eq!(n, 0, "empty realtime index reports generatedAt = 0");
}

#[test]
fn graphql_realtime_generated_at_tracks_hot_swapped_index() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::services::realtime_poller::SharedRealtime;
    use maas_rs::structures::RealtimeIndex;
    use maas_rs::web::app::build_schema_rt;

    let realtime: SharedRealtime =
        Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    let schema = build_schema_rt(shared(Graph::new()), realtime.clone());

    const GENERATED_AT: i64 = 1_700_000_000;
    let index = RealtimeIndex::from_delays(
        GENERATED_AT,
        std::iter::empty::<((TripId, u32), i32)>(),
    );
    realtime.store(Arc::new(index));

    let resp = execute_sync(&schema, "{ realtimeGeneratedAt }");
    assert!(
        resp.errors.is_empty(),
        "unexpected errors: {:?}",
        resp.errors
    );
    let data = data_obj(resp);
    let generated = &data["realtimeGeneratedAt"];
    let n = match generated {
        Value::Number(n) => n.as_i64().expect("generatedAt is an integer"),
        Value::String(s) => s.parse::<i64>().expect("generatedAt parses as i64"),
        other => panic!("expected number generatedAt, got {other:?}"),
    };
    assert_eq!(
        n, GENERATED_AT,
        "realtimeGeneratedAt must reflect the hot-swapped live index"
    );
}

// ── liveRefresh: stateless realtime overlay ─────────────────────────────────────

/// Builds a transit-only graph with two consecutive legs:
///   Stop A —(trip T0)→ Stop B —(trip T1)→ Stop C.
/// T0: SA dep 33000 → SB arr 33480.  T1: SB dep 34500 → SC arr 35400.
/// Stops carry GTFS ids "SA"/"SB"/"SC"; trips carry ids "T0"/"T1". No street
/// network is needed — `liveRefresh` indexes the schedule by handle, never routes.
fn live_refresh_graph() -> Graph {
    use gtfs_structures::{Availability, RouteType};
    use maas_rs::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::{
        EdgeData, NodeData, NodeID, TransitEdgeData, TransitStopData,
        raptor::{Lookup, PatternInfo},
    };

    let mut g = Graph::new();

    let mk_stop = |g: &mut Graph, name: &str, id: &str, lon: f64| -> NodeID {
        g.add_node(NodeData::TransitStop(TransitStopData {
            name: name.into(),
            id: id.into(),
            lat_lng: LatLng {
                latitude: 50.000,
                longitude: lon,
            },
            accessibility: Availability::Available,
            platform_code: None,
            parent_station: None,
        }))
    };
    let stop_a = mk_stop(&mut g, "Stop A", "SA", 4.000);
    let stop_b = mk_stop(&mut g, "Stop B", "SB", 4.050);
    let stop_c = mk_stop(&mut g, "Stop C", "SC", 4.100);

    g.add_edge(
        stop_a,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_a,
            destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 3500,
        }),
    );
    g.add_edge(
        stop_b,
        EdgeData::Transit(TransitEdgeData {
            origin: stop_b,
            destination: stop_c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 3500,
        }),
    );

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "M".into(),
        route_long_name: "Metro M".into(),
        route_type: RouteType::Subway,
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
    g.add_transit_trip_ids(vec!["T0".into(), "T1".into()]);
    g.add_transit_departures(vec![
        TripSegment {
            trip_id: TripId(0),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 33000,
            arrival: 33480,
            service_id: ServiceId(0),
        },
        TripSegment {
            trip_id: TripId(1),
            origin_stop_sequence: 0,
            destination_stop_sequence: 1,
            departure: 34500,
            arrival: 35400,
            service_id: ServiceId(0),
        },
    ]);

    // Pattern P0: SA → SB carrying trip T0.
    let push_pattern = |g: &mut Graph, stops: &[NodeID], trip: TripId, times: &[(u32, u32)]| {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(stops);
        g.push_transit_idx_pattern_stops(Lookup {
            start: ss,
            len: stops.len(),
        });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(trip);
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        for &(arrival, departure) in times {
            g.push_transit_pattern_stop_time(StopTime { arrival, departure, ..Default::default() });
        }
        g.push_transit_idx_pattern_stop_times(Lookup {
            start: sts,
            len: times.len(),
        });
        g.push_transit_pattern(PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        });
    };
    push_pattern(
        &mut g,
        &[stop_a, stop_b],
        TripId(0),
        &[(33000, 33000), (33480, 33480)],
    );
    push_pattern(
        &mut g,
        &[stop_b, stop_c],
        TripId(1),
        &[(34500, 34500), (35400, 35400)],
    );

    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    g.build_raptor_index();
    g
}

/// Run a `liveRefresh` query against `graph` with the given live index, returning
/// the `liveRefresh` payload object.
fn live_refresh_query(
    graph: Graph,
    rt: maas_rs::structures::RealtimeIndex,
    query: &str,
) -> async_graphql::indexmap::IndexMap<Name, Value> {
    use maas_rs::services::realtime_poller::SharedRealtime;
    use maas_rs::structures::RealtimeIndex;
    use maas_rs::web::app::build_schema_rt;

    let realtime: SharedRealtime =
        Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    let schema = build_schema_rt(shared(graph), realtime.clone());
    realtime.store(Arc::new(rt));

    let resp = execute_sync(&schema, query);
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    match &data["liveRefresh"] {
        Value::Object(m) => m.clone(),
        other => panic!("expected liveRefresh object, got {other:?}"),
    }
}

fn int_field(obj: &async_graphql::indexmap::IndexMap<Name, Value>, key: &str) -> Option<i64> {
    match obj.get(key) {
        Some(Value::Number(n)) => Some(n.as_i64().expect("integer")),
        Some(Value::Null) | None => None,
        other => panic!("expected integer or null for {key}, got {other:?}"),
    }
}

const LIVE_LEG0_QUERY: &str = r#"{ liveRefresh(legs: [
        { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
      legs { tripId found status delaySecs scheduledStart scheduledEnd realtimeStart realtimeEnd }
      transfers { fromLegIndex marginSecs }
      eta scheduledEta generatedAt
   } }"#;

#[test]
fn live_refresh_empty_index_matches_schedule() {
    let obj = live_refresh_query(
        live_refresh_graph(),
        maas_rs::structures::RealtimeIndex::new(),
        LIVE_LEG0_QUERY,
    );

    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    assert_eq!(legs.len(), 1);
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["tripId"], Value::String("T0".into()));
    assert_eq!(leg["found"], Value::Boolean(true));
    assert_eq!(leg["status"], Value::Enum(Name::new("NO_DATA")));
    assert_eq!(int_field(leg, "delaySecs"), Some(0));
    // Realtime equals scheduled with no live data.
    assert_eq!(int_field(leg, "scheduledStart"), Some(33000));
    assert_eq!(int_field(leg, "scheduledEnd"), Some(33480));
    assert_eq!(int_field(leg, "realtimeStart"), Some(33000));
    assert_eq!(int_field(leg, "realtimeEnd"), Some(33480));

    assert_eq!(int_field(&obj, "eta"), Some(33480));
    assert_eq!(int_field(&obj, "scheduledEta"), Some(33480));
    assert_eq!(int_field(&obj, "generatedAt"), Some(0));

    match &obj["transfers"] {
        Value::List(v) => assert!(v.is_empty(), "single leg has no transfers"),
        other => panic!("expected transfers list, got {other:?}"),
    }
}

#[test]
fn live_refresh_delay_shifts_realtime_and_eta() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    let g = live_refresh_graph();
    let board = g.stop_index_of("SA").expect("SA resolves") as u32;
    let alight = g.stop_index_of("SB").expect("SB resolves") as u32;
    let rt = RealtimeIndex::from_delays(
        1_700_000_000,
        [((TripId(0), board), 120), ((TripId(0), alight), 120)],
    );

    let obj = live_refresh_query(g, rt, LIVE_LEG0_QUERY);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["found"], Value::Boolean(true));
    assert_eq!(leg["status"], Value::Enum(Name::new("DELAYED")));
    assert_eq!(int_field(leg, "delaySecs"), Some(120));
    assert_eq!(int_field(leg, "scheduledStart"), Some(33000));
    assert_eq!(int_field(leg, "scheduledEnd"), Some(33480));
    assert_eq!(int_field(leg, "realtimeStart"), Some(33120));
    assert_eq!(int_field(leg, "realtimeEnd"), Some(33600));

    // eta tracks the delayed realtime arrival; scheduled_eta stays on schedule.
    assert_eq!(int_field(&obj, "eta"), Some(33600));
    assert_eq!(int_field(&obj, "scheduledEta"), Some(33480));
    assert_eq!(int_field(&obj, "generatedAt"), Some(1_700_000_000));
}

#[test]
fn live_refresh_canceled_keeps_schedule_and_zero_delay() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    let g = live_refresh_graph();
    let board = g.stop_index_of("SA").expect("SA resolves") as u32;
    // A canceled trip can still carry a stale per-stop delay; cancellation must
    // override it — times stay scheduled and delaySecs is 0.
    let rt = RealtimeIndex::from_updates(
        1_700_000_500,
        [((TripId(0), board), 300)],
        [TripId(0)],
    );

    let obj = live_refresh_query(g, rt, LIVE_LEG0_QUERY);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["found"], Value::Boolean(true));
    assert_eq!(leg["status"], Value::Enum(Name::new("CANCELED")));
    assert_eq!(int_field(leg, "delaySecs"), Some(0));
    assert_eq!(int_field(leg, "scheduledStart"), Some(33000));
    assert_eq!(int_field(leg, "scheduledEnd"), Some(33480));
    assert_eq!(int_field(leg, "realtimeStart"), Some(33000));
    assert_eq!(int_field(leg, "realtimeEnd"), Some(33480));
}

#[test]
fn live_refresh_unknown_trip_is_not_found_without_panic() {
    let q = r#"{ liveRefresh(legs: [
            { tripId: "NOPE", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { tripId found status scheduledStart realtimeEnd }
          eta scheduledEta
       } }"#;
    let obj = live_refresh_query(
        live_refresh_graph(),
        maas_rs::structures::RealtimeIndex::new(),
        q,
    );
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["tripId"], Value::String("NOPE".into()));
    assert_eq!(leg["found"], Value::Boolean(false));
    assert_eq!(leg["status"], Value::Enum(Name::new("NOT_FOUND")));
    assert_eq!(leg["scheduledStart"], Value::Null);
    assert_eq!(leg["realtimeEnd"], Value::Null);
    // Nothing resolved → no eta.
    assert_eq!(obj["eta"], Value::Null);
    assert_eq!(obj["scheduledEta"], Value::Null);
}

#[test]
fn live_refresh_trailing_unresolved_leg_keeps_eta_from_last_resolved() {
    // Leg 0 resolves (T0 SA→SB), leg 1 does not (unknown trip). ETA must come from
    // the last RESOLVED leg, not be nulled by the trailing unresolved one.
    let q = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" },
            { tripId: "NOPE", boardStopId: "SB", alightStopId: "SC" }]) {
          legs { found }
          transfers { fromLegIndex }
          eta scheduledEta
       } }"#;
    let obj = live_refresh_query(
        live_refresh_graph(),
        maas_rs::structures::RealtimeIndex::new(),
        q,
    );
    assert_eq!(int_field(&obj, "eta"), Some(33480));
    assert_eq!(int_field(&obj, "scheduledEta"), Some(33480));
    match &obj["transfers"] {
        Value::List(v) => assert!(v.is_empty(), "no transfer when one side is unresolved"),
        other => panic!("expected transfers list, got {other:?}"),
    }
}

const LIVE_TWO_LEG_QUERY: &str = r#"{ liveRefresh(legs: [
        { tripId: "T0", boardStopId: "SA", alightStopId: "SB" },
        { tripId: "T1", boardStopId: "SB", alightStopId: "SC" }]) {
      legs { tripId found realtimeEnd realtimeStart }
      transfers { fromLegIndex realtimeArrival realtimeDeparture marginSecs reliability }
      eta scheduledEta
   } }"#;

#[test]
fn live_refresh_transfer_margin_shrinks_when_feeder_delayed() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    // Baseline: no realtime → scheduled margin = T1 dep (34500) − T0 arr (33480).
    let obj = live_refresh_query(
        live_refresh_graph(),
        RealtimeIndex::new(),
        LIVE_TWO_LEG_QUERY,
    );
    let transfers = match &obj["transfers"] {
        Value::List(v) => v,
        other => panic!("expected transfers list, got {other:?}"),
    };
    assert_eq!(transfers.len(), 1, "one interior transfer");
    let tr = match &transfers[0] {
        Value::Object(m) => m,
        other => panic!("expected transfer object, got {other:?}"),
    };
    assert_eq!(int_field(tr, "fromLegIndex"), Some(0));
    assert_eq!(int_field(tr, "realtimeArrival"), Some(33480));
    assert_eq!(int_field(tr, "realtimeDeparture"), Some(34500));
    assert_eq!(int_field(tr, "marginSecs"), Some(1020));
    // No delay models in the fixture → reliability is null (margin carries the signal).
    assert_eq!(tr["reliability"], Value::Null);
    // eta follows the last resolved leg (T1 arrival at SC).
    assert_eq!(int_field(&obj, "eta"), Some(35400));

    // Delay the feeder (T0) by 600 s at its alighting stop (SB): realtime arrival
    // climbs, the boarding departure is unchanged, so the margin shrinks.
    let g = live_refresh_graph();
    let t0_alight = g.stop_index_of("SB").expect("SB resolves") as u32;
    let rt = RealtimeIndex::from_delays(1_700_000_000, [((TripId(0), t0_alight), 600)]);
    let obj = live_refresh_query(g, rt, LIVE_TWO_LEG_QUERY);
    let transfers = match &obj["transfers"] {
        Value::List(v) => v,
        other => panic!("expected transfers list, got {other:?}"),
    };
    let tr = match &transfers[0] {
        Value::Object(m) => m,
        other => panic!("expected transfer object, got {other:?}"),
    };
    assert_eq!(int_field(tr, "realtimeArrival"), Some(34080));
    assert_eq!(int_field(tr, "realtimeDeparture"), Some(34500));
    assert_eq!(
        int_field(tr, "marginSecs"),
        Some(420),
        "margin shrinks by the feeder delay"
    );
}

/// `live_refresh_graph` plus a Subway delay model registered for the route type
/// both legs ride. Without a model `transfer_reliability` returns null (every
/// existing fixture test); with one it exercises the real `prob_on_time_vs` path.
fn live_refresh_graph_with_delay_model() -> Graph {
    use gtfs_structures::RouteType;
    use maas_rs::structures::DelayCDF;
    use std::collections::HashMap;

    let mut g = live_refresh_graph();
    // ~62% on time, saturating at +600 s of delay.
    let cdf = DelayCDF {
        bins: vec![
            (-120, 0.10),
            (0, 0.62),
            (120, 0.80),
            (300, 0.95),
            (600, 1.00),
        ],
    };
    let mut models = HashMap::new();
    models.insert(RouteType::Subway, cdf);
    g.set_transit_delay_models(models);
    g
}

#[test]
fn live_refresh_transfer_reliability_high_then_low_as_feeder_delay_eats_margin() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    // Healthy: empty realtime → scheduled margin 1020 s (T1 dep 34500 − T0 arr 33480).
    // With a delay model on the route type both legs ride, `reliability` is Some and,
    // given the comfortable margin, near-certain.
    let obj = live_refresh_query(
        live_refresh_graph_with_delay_model(),
        RealtimeIndex::new(),
        LIVE_TWO_LEG_QUERY,
    );
    let transfers = match &obj["transfers"] {
        Value::List(v) => v,
        other => panic!("expected transfers list, got {other:?}"),
    };
    let tr = match &transfers[0] {
        Value::Object(m) => m,
        other => panic!("expected transfer object, got {other:?}"),
    };
    assert_eq!(int_field(tr, "marginSecs"), Some(1020));
    let healthy = match tr.get("reliability") {
        Some(Value::Number(n)) => n.as_f64().expect("float"),
        other => panic!("delay model registered → reliability must be Some, got {other:?}"),
    };
    assert!(
        healthy > 0.9,
        "healthy positive margin is near-certain, got {healthy}"
    );

    // Delay the feeder (T0) by 1500 s at its alighting stop SB: realtime arrival
    // climbs to 34980, overshooting the unchanged boarding departure (34500), so
    // the margin goes NEGATIVE and reliability collapses. This pins the margin sign
    // end-to-end: flip the subtraction and the healthy case stops being near-certain.
    let g = live_refresh_graph_with_delay_model();
    let t0_alight = g.stop_index_of("SB").expect("SB resolves") as u32;
    let rt = RealtimeIndex::from_delays(1_700_000_000, [((TripId(0), t0_alight), 1500)]);
    let obj = live_refresh_query(g, rt, LIVE_TWO_LEG_QUERY);
    let transfers = match &obj["transfers"] {
        Value::List(v) => v,
        other => panic!("expected transfers list, got {other:?}"),
    };
    let tr = match &transfers[0] {
        Value::Object(m) => m,
        other => panic!("expected transfer object, got {other:?}"),
    };
    assert_eq!(int_field(tr, "realtimeArrival"), Some(34980));
    assert_eq!(int_field(tr, "realtimeDeparture"), Some(34500));
    assert_eq!(
        int_field(tr, "marginSecs"),
        Some(-480),
        "negative margin: feeder arrival overshoots the boarding departure"
    );
    let risky = match tr.get("reliability") {
        Some(Value::Number(n)) => n.as_f64().expect("float"),
        other => panic!("delay model registered → reliability must be Some, got {other:?}"),
    };
    assert!(
        risky < 0.2,
        "a negative margin collapses transfer reliability, got {risky}"
    );
    assert!(
        risky < healthy,
        "delaying the feeder must lower, never raise, reliability ({risky} vs {healthy})"
    );
}

#[test]
fn live_refresh_reversed_or_equal_stops_are_not_found_without_panic() {
    use maas_rs::structures::RealtimeIndex;

    // T0 resolves as a trip, but the board→alight order is wrong (SB→SA, reversed)
    // or degenerate (SA→SA). `scheduled_trip_leg_times` slices `pat_stops[board+1..]`
    // and finds no alighting stop after the boarding one, so the leg is NOT_FOUND —
    // no panic, no eta.
    for (board, alight) in [("SB", "SA"), ("SA", "SA")] {
        let q = format!(
            r#"{{ liveRefresh(legs: [
                {{ tripId: "T0", boardStopId: "{board}", alightStopId: "{alight}" }}]) {{
              legs {{ tripId found status scheduledStart realtimeEnd }}
              eta scheduledEta
           }} }}"#
        );
        let obj = live_refresh_query(live_refresh_graph(), RealtimeIndex::new(), &q);
        let legs = match &obj["legs"] {
            Value::List(v) => v,
            other => panic!("expected legs list, got {other:?}"),
        };
        let leg = match &legs[0] {
            Value::Object(m) => m,
            other => panic!("expected leg object, got {other:?}"),
        };
        assert_eq!(
            leg["found"],
            Value::Boolean(false),
            "board={board} alight={alight} must not resolve"
        );
        assert_eq!(leg["status"], Value::Enum(Name::new("NOT_FOUND")));
        assert_eq!(leg["scheduledStart"], Value::Null);
        assert_eq!(leg["realtimeEnd"], Value::Null);
        assert_eq!(obj["eta"], Value::Null, "nothing resolved → no eta");
        assert_eq!(obj["scheduledEta"], Value::Null);
    }
}

// ── liveRefresh: service alerts ───────────────────────────────────────────────

const LIVE_ALERTS_QUERY: &str = r#"{ liveRefresh(legs: [
        { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
      legs { tripId found alerts { header description cause effect } }
      generatedAt
   } }"#;

/// Build a RealtimeIndex containing a single service alert for the given
/// trip_id / stop_id, with the specified active_period (Unix seconds).
fn rt_with_alert(
    trip_id: &str,
    stop_id: Option<&str>,
    active_start: Option<u64>,
    active_end: Option<u64>,
    header: &str,
    cause: i32,
    effect: i32,
) -> maas_rs::structures::RealtimeIndex {
    use maas_rs::ingestion::realtime::{AlertEntitySelector, ServiceAlert};
    let alert = ServiceAlert {
        header: Some(header.to_string()),
        description: Some("Details here.".to_string()),
        cause: Some(cause),
        effect: Some(effect),
        active_period: vec![(active_start, active_end)],
        informed_entity: vec![AlertEntitySelector {
            trip_id: Some(trip_id.to_string()),
            route_id: None,
            stop_id: stop_id.map(|s| s.to_string()),
        }],
    };
    maas_rs::structures::RealtimeIndex::with_alerts(1_000, [], [], [], [alert])
}

fn extract_leg_alerts(
    obj: &async_graphql::indexmap::IndexMap<Name, Value>,
) -> Vec<async_graphql::indexmap::IndexMap<Name, Value>> {
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    match &leg["alerts"] {
        Value::List(v) => v
            .iter()
            .map(|a| match a {
                Value::Object(m) => m.clone(),
                other => panic!("expected alert object, got {other:?}"),
            })
            .collect(),
        other => panic!("expected alerts list, got {other:?}"),
    }
}

#[test]
fn live_refresh_always_active_alert_appears_on_matching_leg() {
    // Empty active_period → always active, regardless of current wall-clock time.
    use maas_rs::ingestion::realtime::{AlertEntitySelector, ServiceAlert};
    let alert = ServiceAlert {
        header: Some("Disruption on T0".to_string()),
        description: None,
        cause: Some(9),
        effect: Some(1),
        active_period: vec![],
        informed_entity: vec![AlertEntitySelector {
            trip_id: Some("T0".to_string()),
            route_id: None,
            stop_id: None,
        }],
    };
    let rt = maas_rs::structures::RealtimeIndex::with_alerts(1_000, [], [], [], [alert]);
    let obj = live_refresh_query(live_refresh_graph(), rt, LIVE_ALERTS_QUERY);
    let alerts = extract_leg_alerts(&obj);
    assert_eq!(alerts.len(), 1, "one matching alert expected");
    assert_eq!(
        alerts[0]["header"],
        Value::String("Disruption on T0".into())
    );
    assert_eq!(
        alerts[0]["cause"],
        Value::String("MAINTENANCE".into()),
        "cause=9 maps to MAINTENANCE"
    );
    assert_eq!(
        alerts[0]["effect"],
        Value::String("NO_SERVICE".into()),
        "effect=1 maps to NO_SERVICE"
    );
}

#[test]
fn live_refresh_non_matching_alert_not_surfaced() {
    // Trip T99 — will not match a leg for T0.
    let rt = rt_with_alert("T99", None, None, None, "Other line disruption", 1, 3);
    let obj = live_refresh_query(live_refresh_graph(), rt, LIVE_ALERTS_QUERY);
    let alerts = extract_leg_alerts(&obj);
    assert!(alerts.is_empty(), "alert for T99 must not appear on T0 leg");
}

#[test]
fn live_refresh_expired_alert_not_surfaced() {
    // Active period ended at Unix second 1 (always in the past).
    let rt = rt_with_alert("T0", None, Some(0), Some(1), "Ancient disruption", 1, 3);
    let obj = live_refresh_query(live_refresh_graph(), rt, LIVE_ALERTS_QUERY);
    let alerts = extract_leg_alerts(&obj);
    assert!(
        alerts.is_empty(),
        "alert expired (end=1 Unix second) must not appear"
    );
}

#[test]
fn live_refresh_future_alert_not_surfaced() {
    // Active period starts far in the future (year 9999 ≈ 253_402_300_799).
    let far_future: u64 = 253_402_300_799;
    let rt = rt_with_alert(
        "T0",
        None,
        Some(far_future),
        None,
        "Future disruption",
        1,
        3,
    );
    let obj = live_refresh_query(live_refresh_graph(), rt, LIVE_ALERTS_QUERY);
    let alerts = extract_leg_alerts(&obj);
    assert!(
        alerts.is_empty(),
        "alert not yet started (start far future) must not appear"
    );
}

#[test]
fn live_refresh_alert_matching_board_stop_appears() {
    // Alert on "UNRELATED_TRIP" but selector stop_id = "SA" (the boarding stop).
    use maas_rs::ingestion::realtime::{AlertEntitySelector, ServiceAlert};
    let alert = ServiceAlert {
        header: Some("Stop SA closed".to_string()),
        description: None,
        cause: Some(3),
        effect: Some(9),
        active_period: vec![],
        informed_entity: vec![AlertEntitySelector {
            trip_id: Some("UNRELATED_TRIP".to_string()),
            route_id: None,
            stop_id: Some("SA".to_string()),
        }],
    };
    let rt = maas_rs::structures::RealtimeIndex::with_alerts(0, [], [], [], [alert]);
    let obj = live_refresh_query(live_refresh_graph(), rt, LIVE_ALERTS_QUERY);
    let alerts = extract_leg_alerts(&obj);
    assert_eq!(alerts.len(), 1, "alert matching the boarding stop must appear");
}

#[test]
fn live_refresh_no_alerts_returns_empty_list() {
    let obj = live_refresh_query(
        live_refresh_graph(),
        maas_rs::structures::RealtimeIndex::new(),
        LIVE_ALERTS_QUERY,
    );
    let alerts = extract_leg_alerts(&obj);
    assert!(
        alerts.is_empty(),
        "empty realtime index must yield empty alerts list"
    );
}

// ── HTTP routes ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_index_returns_html() {
    use maas_rs::web::app::index_page;
    use poem::{Route, get, test::TestClient};

    let app = Route::new().at("/", get(index_page));
    let client = TestClient::new(app);
    let resp = client.get("/").send().await;
    resp.assert_status_is_ok();
    let ct = resp
        .0
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("text/html"),
        "expected text/html content-type, got: {ct}"
    );
}

#[tokio::test]
async fn get_debug_returns_html() {
    use maas_rs::web::app::debug_page;
    use poem::{Route, get, test::TestClient};

    let app = Route::new().at("/debug", get(debug_page));
    let client = TestClient::new(app);
    let resp = client.get("/debug").send().await;
    resp.assert_status_is_ok();
    let ct = resp
        .0
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("text/html"),
        "expected text/html content-type, got: {ct}"
    );
}

#[tokio::test]
async fn get_maas_js_returns_javascript() {
    use maas_rs::web::app::maas_js_handler;
    use poem::{Route, get, test::TestClient};

    let app = Route::new().at("/maas.js", get(maas_js_handler));
    let client = TestClient::new(app);
    let resp = client.get("/maas.js").send().await;
    resp.assert_status_is_ok();
    let ct = resp
        .0
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("javascript"),
        "expected javascript content-type, got: {ct}"
    );
}

// ── stationBackups (same-station cross-line backups) ──────────────────────────

/// Two routes serving SA → SB (Bus T0 reference + Tram T1 cross-line), a same-
/// route sibling (Bus T2, earlier), and a decoy (Bus T3, SA → SX). The supplied
/// route-type delay models drive catch-reliability through the real
/// `prob_at_least` path; a route type absent from `models` yields `null`.
fn station_backups_graph_with(
    models: std::collections::HashMap<gtfs_structures::RouteType, maas_rs::structures::DelayCDF>,
) -> Graph {
    use gtfs_structures::RouteType;
    use maas_rs::ingestion::gtfs::{
        RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TripId, TripInfo,
    };
    use maas_rs::structures::{NodeID, raptor::Lookup};

    let mut g = Graph::new();

    let sa = g.add_node(transit_stop("SA", 50.000, 4.000));
    let sb = g.add_node(transit_stop("SB", 50.000, 4.050));
    let sx = g.add_node(transit_stop("SX", 50.000, 4.090));

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
    g.add_transit_routes(vec![
        RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus,
            agency_id: AgencyId(0),
            route_color: Some((255, 0, 0)),
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
    // TripId(0)=T0 bus ref, (1)=T1 tram, (2)=T2 bus sibling, (3)=T3 bus decoy.
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

    // Bus SA→SB, trips [T2 (08:50), T0 (09:00)].
    push_pattern(&mut g, RouteId(0), &[sa, sb], &[TripId(2), TripId(0)],
        &[(31800, 31800), (32400, 32400), (32700, 32700), (33300, 33300)]);
    // Tram SA→SB, trip [T1 (09:01, just after the reference so the catch margin is
    // small and below the delay-model's saturation point)].
    push_pattern(&mut g, RouteId(1), &[sa, sb], &[TripId(1)],
        &[(32460, 32460), (33660, 33660)]);
    // Bus SA→SX decoy, trip [T3 (09:05)].
    push_pattern(&mut g, RouteId(0), &[sa, sx], &[TripId(3)],
        &[(32700, 32700), (33000, 33000)]);

    g.set_transit_delay_models(models);

    g.build_raptor_index();
    g
}

/// Bus delay model. pmf: (-120,.10)(0,.52)(120,.18)(300,.15)(600,.05).
fn bus_delay_cdf() -> maas_rs::structures::DelayCDF {
    maas_rs::structures::DelayCDF {
        bins: vec![(-120, 0.10), (0, 0.62), (120, 0.80), (300, 0.95), (600, 1.00)],
    }
}

/// Tramway delay model. pmf: (-120,.10)(-60,.15)(0,.30)(60,.25)(120,.15)(300,.05).
fn tram_delay_cdf() -> maas_rs::structures::DelayCDF {
    maas_rs::structures::DelayCDF {
        bins: vec![(-120, 0.10), (-60, 0.25), (0, 0.55), (60, 0.80), (120, 0.95), (300, 1.00)],
    }
}

/// Fixture with delay models for both backup route types (Bus + Tramway), so
/// every backup scores a `Some` catch-reliability.
fn station_backups_graph() -> Graph {
    use gtfs_structures::RouteType;
    let mut models = std::collections::HashMap::new();
    models.insert(RouteType::Bus, bus_delay_cdf());
    models.insert(RouteType::Tramway, tram_delay_cdf());
    station_backups_graph_with(models)
}

const STATION_BACKUPS_QUERY: &str = r#"{ stationBackups(
        tripId: "T0", boardStopId: "SA", alightStopId: "SB", beforeCount: 5, afterCount: 5) {
      tripId boardStopId alightStopId routeShortName routeLongName mode routeColor sameLine
      scheduledDeparture scheduledArrival realtimeDeparture realtimeArrival reliability
   } }"#;

fn station_backups_list(
    graph: Graph,
    rt: maas_rs::structures::RealtimeIndex,
    query: &str,
) -> Vec<async_graphql::indexmap::IndexMap<Name, Value>> {
    use maas_rs::services::realtime_poller::SharedRealtime;
    use maas_rs::structures::RealtimeIndex;
    use maas_rs::web::app::build_schema_rt;

    let realtime: SharedRealtime = Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    let schema = build_schema_rt(shared(graph), realtime.clone());
    realtime.store(Arc::new(rt));

    let resp = execute_sync(&schema, query);
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    match &data["stationBackups"] {
        Value::List(v) => v
            .iter()
            .map(|o| match o {
                Value::Object(m) => m.clone(),
                other => panic!("expected backup object, got {other:?}"),
            })
            .collect(),
        other => panic!("expected stationBackups list, got {other:?}"),
    }
}

fn reliability_of(obj: &async_graphql::indexmap::IndexMap<Name, Value>) -> f64 {
    match obj.get("reliability") {
        Some(Value::Number(n)) => n.as_f64().expect("float"),
        other => panic!("delay model registered → reliability must be Some, got {other:?}"),
    }
}

#[test]
fn station_backups_returns_cross_line_backups_with_reliabilities() {
    use maas_rs::structures::RealtimeIndex;

    let backups = station_backups_list(station_backups_graph(), RealtimeIndex::new(), STATION_BACKUPS_QUERY);

    // Chronological: sibling bus T2 (08:50) then cross-line tram T1 (09:10). The
    // reference T0 and the decoy T3 (never reaches SB) are excluded.
    let trips: Vec<_> = backups
        .iter()
        .map(|b| match &b["tripId"] {
            Value::String(s) => s.clone(),
            other => panic!("expected tripId string, got {other:?}"),
        })
        .collect();
    assert_eq!(trips, vec!["T2".to_string(), "T1".to_string()]);

    let sibling = &backups[0];
    assert_eq!(sibling["sameLine"], Value::Boolean(true));
    assert_eq!(sibling["routeShortName"], Value::String("1".into()));
    assert_eq!(sibling["routeColor"], Value::String("FF0000".into()));
    assert_eq!(int_field(sibling, "scheduledDeparture"), Some(31800));
    assert_eq!(int_field(sibling, "realtimeDeparture"), Some(31800));
    assert_eq!(int_field(sibling, "scheduledArrival"), Some(32700));

    let tram = &backups[1];
    assert_eq!(tram["sameLine"], Value::Boolean(false));
    assert_eq!(tram["routeShortName"], Value::String("T".into()));
    assert_eq!(tram["mode"], Value::String("Tramway".into()));
    assert_eq!(int_field(tram, "scheduledDeparture"), Some(32460));

    // Concrete catch-probabilities, hand-computed from the registered models.
    // Ready time = T0 scheduled departure at SA = 32400 (no realtime).
    //   Tram T1: slack = 32460 − 32400 = +60  → P(D_tram ≥ −60)
    //            = .15+.30+.25+.15+.05 = 0.90.
    //   Bus  T2: slack = 31800 − 32400 = −600 → P(D_bus ≥ 600) = 0.05.
    assert!((reliability_of(tram) - 0.90).abs() < 1e-6, "tram reliability {}", reliability_of(tram));
    assert!((reliability_of(sibling) - 0.05).abs() < 1e-6, "sibling reliability {}", reliability_of(sibling));

    // The later tram (positive slack) is easier to catch than the earlier sibling
    // (negative slack: it leaves before the one you missed).
    assert!(
        reliability_of(tram) > reliability_of(sibling),
        "later departure must score higher catch-reliability"
    );
}

#[test]
fn station_backups_backup_delay_shifts_realtime_departure_but_not_catch_reliability() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    // Delaying the BACKUP (T1) shifts its displayed realtime departure but leaves
    // catch-reliability untouched: the model scores slack from the *scheduled*
    // departure and folds delay into D_backup, so it must not double-count rt.delay.
    let g = station_backups_graph();
    let board = g.stop_index_of("SA").expect("SA resolves") as u32;
    let rt = RealtimeIndex::from_delays(1_700_000_000, [((TripId(1), board), 300)]);
    let delayed = station_backups_list(g, rt, STATION_BACKUPS_QUERY);
    let tram = delayed.iter().find(|b| b["tripId"] == Value::String("T1".into())).expect("T1 present");

    assert_eq!(int_field(tram, "realtimeDeparture"), Some(32760));
    assert!(
        (reliability_of(tram) - 0.90).abs() < 1e-6,
        "backup rt.delay must not change catch-reliability, got {}",
        reliability_of(tram)
    );
}

#[test]
fn station_backups_original_delay_lowers_catch_reliability() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    // Baseline tram (T1) catch-reliability with no realtime: 0.90 (slack +60).
    let base = station_backups_list(station_backups_graph(), RealtimeIndex::new(), STATION_BACKUPS_QUERY);
    let base_tram = base.iter().find(|b| b["tripId"] == Value::String("T1".into())).expect("T1 present");
    assert!((reliability_of(base_tram) - 0.90).abs() < 1e-6);

    // Delay the ORIGINAL trip T0 by 300 s at SA: the ready time climbs to 32700,
    // slack for T1 falls to 32460 − 32700 = −240, and catch-reliability collapses
    // to P(D_tram ≥ 240) = 0.05. T1's own realtime departure is unchanged.
    let g = station_backups_graph();
    let board = g.stop_index_of("SA").expect("SA resolves") as u32;
    let rt = RealtimeIndex::from_delays(1_700_000_000, [((TripId(0), board), 300)]);
    let delayed = station_backups_list(g, rt, STATION_BACKUPS_QUERY);
    let tram = delayed.iter().find(|b| b["tripId"] == Value::String("T1".into())).expect("T1 present");

    assert_eq!(int_field(tram, "realtimeDeparture"), Some(32460));
    assert!(
        (reliability_of(tram) - 0.05).abs() < 1e-6,
        "delaying the original raises ready time → lower catch-reliability, got {}",
        reliability_of(tram)
    );
}

#[test]
fn station_backups_excludes_canceled_trip() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::RealtimeIndex;

    // Cancel the tram backup T1: it must vanish from the list while the non-canceled
    // sibling bus T2 remains.
    let rt = RealtimeIndex::from_updates(1_700_000_000, [], [TripId(1)]);
    let backups = station_backups_list(station_backups_graph(), rt, STATION_BACKUPS_QUERY);

    let trips: Vec<_> = backups
        .iter()
        .map(|b| match &b["tripId"] {
            Value::String(s) => s.clone(),
            other => panic!("expected tripId string, got {other:?}"),
        })
        .collect();
    assert_eq!(trips, vec!["T2".to_string()], "canceled T1 excluded, T2 kept");
}

#[test]
fn station_backups_null_reliability_without_delay_model() {
    use gtfs_structures::RouteType;
    use maas_rs::structures::RealtimeIndex;

    // Register a model for Bus only: the tram backup T1 has no model for its route
    // type → catch-reliability is null, while the bus sibling T2 still scores.
    let mut models = std::collections::HashMap::new();
    models.insert(RouteType::Bus, bus_delay_cdf());
    let g = station_backups_graph_with(models);

    let backups = station_backups_list(g, RealtimeIndex::new(), STATION_BACKUPS_QUERY);
    let tram = backups.iter().find(|b| b["tripId"] == Value::String("T1".into())).expect("T1 present");
    let sibling = backups.iter().find(|b| b["tripId"] == Value::String("T2".into())).expect("T2 present");

    assert_eq!(tram["reliability"], Value::Null, "no tram model → null reliability");
    assert!((reliability_of(sibling) - 0.05).abs() < 1e-6);
}

#[test]
fn station_backups_unknown_trip_is_empty_without_panic() {
    use maas_rs::structures::RealtimeIndex;

    let q = r#"{ stationBackups(tripId: "NOPE", boardStopId: "SA", alightStopId: "SB") {
        tripId } }"#;
    let backups = station_backups_list(station_backups_graph(), RealtimeIndex::new(), q);
    assert!(backups.is_empty(), "unknown tripId resolves to empty list");
}

// ── Onboard partial-requery (Phase 2b) over GraphQL ───────────────────────────

/// Minimal transit graph for onboard queries: bus trip "T1" over stops A,B,C
/// (ids "A","B","C"), with C a short walk from the destination at (50.000,4.020).
fn onboard_gql_graph() -> Graph {
    use gtfs_structures::RouteType;
    use maas_rs::ingestion::gtfs::{
        ServiceId, ServicePattern, StopTime, TimetableSegment, TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::raptor::Lookup;
    use maas_rs::structures::{
        BikeAttrs, EdgeData, NodeID, StreetEdgeData, TransitEdgeData, cost::VarGen,
    };

    let mut g = Graph::new();
    let osm_dest = g.add_node(osm_node("osm_dest", 50.000, 4.020));
    let osm_stub = g.add_node(osm_node("osm_stub", 50.001, 4.020));
    let a = g.add_node(transit_stop("A", 50.000, 4.000));
    let b = g.add_node(transit_stop("B", 50.000, 4.010));
    let c = g.add_node(transit_stop("C", 50.000, 4.020));

    let street = |g: &mut Graph, o: NodeID, d: NodeID, m: usize, partial: bool| {
        let mk = |o: NodeID, d: NodeID| {
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
            })
        };
        g.add_edge(o, mk(o, d));
        g.add_edge(d, mk(d, o));
    };
    street(&mut g, osm_dest, osm_stub, 100, false);
    street(&mut g, c, osm_dest, 72, true);

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Bus X".into(),
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
    g.add_transit_trip_ids(vec!["T1".to_string()]);

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
            trip_id: TripId(0),
            origin_stop_sequence: 1,
            destination_stop_sequence: 2,
            departure: 32700,
            arrival: 33000,
            service_id: ServiceId(0),
        },
    ]);
    g.add_edge(
        a,
        EdgeData::Transit(TransitEdgeData {
            origin: a,
            destination: b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 1 },
            length: 718,
        }),
    );
    g.add_edge(
        b,
        EdgeData::Transit(TransitEdgeData {
            origin: b,
            destination: c,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 1, len: 1 },
            length: 718,
        }),
    );

    let ss = g.transit_pattern_stops_len();
    g.extend_transit_pattern_stops(&[a, b, c]);
    g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 3 });
    let ts = g.transit_pattern_trips_len();
    g.push_transit_pattern_trip(TripId(0));
    g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
    let sts = g.transit_pattern_stop_times_len();
    for t in [32400u32, 32700, 33000] {
        g.push_transit_pattern_stop_time(StopTime {
            arrival: t,
            departure: t,
            ..Default::default()
        });
    }
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 3 });
    g.push_transit_pattern(PatternInfo {
        route: RouteId(0),
        num_trips: 1,
    });

    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

/// Test 4a: `onboardRaptor(onboardOrigin: {...})` resolves the trip/stop ids and
/// returns onboard-rooted plans whose first leg is the boarded transit ride.
#[test]
fn graphql_onboard_raptor_returns_onboard_rooted_plans() {
    let schema = build_schema(shared(onboard_gql_graph()));
    let resp = execute_sync(
        &schema,
        r#"{ onboardRaptor(toLat: 50.000, toLng: 4.020,
              onboardOrigin: { tripId: "T1", fromStopId: "A" }) {
              mode legs { __typename } } }"#,
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    match &data["onboardRaptor"] {
        Value::List(plans) => {
            assert!(!plans.is_empty(), "expected onboard-rooted plans");
            // At least one plan begins with a transit leg (the onboard ride).
            let has_transit_first = plans.iter().any(|p| match p {
                Value::Object(o) => match &o["legs"] {
                    Value::List(legs) => matches!(
                        legs.first(),
                        Some(Value::Object(l)) if l["__typename"] == Value::String("PlanTransitLeg".into())
                    ),
                    _ => false,
                },
                _ => false,
            });
            assert!(has_transit_first, "onboard plan's first leg must be the transit ride");
        }
        other => panic!("expected plan list, got {other:?}"),
    }
}

/// Test 4b: an unknown onboard `tripId` yields a clean error, not a panic.
#[test]
fn graphql_onboard_raptor_unknown_trip_errors_cleanly() {
    let schema = build_schema(shared(onboard_gql_graph()));
    let resp = execute_sync(
        &schema,
        r#"{ onboardRaptor(toLat: 50.000, toLng: 4.020,
              onboardOrigin: { tripId: "NOPE" }) { start } }"#,
    );
    assert!(!resp.errors.is_empty(), "expected an error for an unknown trip_id");
    let msg = resp.errors[0].message.to_lowercase();
    assert!(
        msg.contains("unknown trip_id") || msg.contains("nope"),
        "expected an unknown-trip error, got: {}",
        resp.errors[0].message
    );
}

// ── Phase 3.5: vehicle position on liveRefresh ─────────────────────────────────

#[test]
fn live_refresh_vehicle_position_present_and_fresh() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::{RealtimeIndex, VehiclePos};

    let g = live_refresh_graph();
    let pos = VehiclePos {
        lat: 50.845_f32,
        lng: 4.352_f32,
        bearing: Some(270.0),
        current_stop_sequence: Some(1),
        timestamp: Some(u64::MAX),
    };
    let rt = RealtimeIndex::with_positions(0, [], [], [(TripId(0), pos)]);

    let q = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { tripId found vehicle { lat lng bearing observedAt stale } }
       } }"#;
    let obj = live_refresh_query(g, rt, q);

    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["tripId"], Value::String("T0".into()));
    assert_eq!(leg["found"], Value::Boolean(true));

    let veh = match &leg["vehicle"] {
        Value::Object(m) => m,
        Value::Null => panic!("expected vehicle object, got null"),
        other => panic!("expected vehicle object, got {other:?}"),
    };
    assert!(matches!(&veh["lat"], Value::Number(_)), "lat must be a number");
    assert!(matches!(&veh["lng"], Value::Number(_)), "lng must be a number");
    assert!(matches!(&veh["bearing"], Value::Number(_)), "bearing must be a number when present");
    assert_eq!(
        veh["stale"],
        Value::Boolean(false),
        "timestamp=u64::MAX is always in the future, must not be stale"
    );
}

#[test]
fn live_refresh_vehicle_position_absent() {
    let q = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { tripId found vehicle { lat lng stale } }
       } }"#;
    let obj = live_refresh_query(
        live_refresh_graph(),
        maas_rs::structures::RealtimeIndex::new(),
        q,
    );
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(
        leg["vehicle"],
        Value::Null,
        "no position in index → vehicle must be null"
    );
}

#[test]
fn live_refresh_vehicle_position_stale() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::{RealtimeIndex, VehiclePos};

    let g = live_refresh_graph();
    let pos = VehiclePos {
        lat: 50.845_f32,
        lng: 4.352_f32,
        bearing: None,
        current_stop_sequence: None,
        timestamp: Some(0),
    };
    let rt = RealtimeIndex::with_positions(0, [], [], [(TripId(0), pos)]);

    let q = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { vehicle { lat lng observedAt stale } }
       } }"#;
    let obj = live_refresh_query(g, rt, q);

    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    let veh = match &leg["vehicle"] {
        Value::Object(m) => m,
        Value::Null => panic!("expected vehicle object, got null — position should be present"),
        other => panic!("expected vehicle object, got {other:?}"),
    };
    assert_eq!(
        veh["stale"],
        Value::Boolean(true),
        "epoch-0 timestamp must always be stale"
    );
    assert_eq!(
        veh["observedAt"],
        Value::Number(async_graphql::Number::from(0i64)),
        "observedAt must be 0 for epoch-0 timestamp"
    );
    assert!(matches!(&veh["lat"], Value::Number(_)), "lat must be a number even when stale");
}

#[test]
fn live_refresh_unresolved_leg_vehicle_is_null() {
    use maas_rs::ingestion::gtfs::TripId;
    use maas_rs::structures::{RealtimeIndex, VehiclePos};

    let pos = VehiclePos {
        lat: 50.845_f32,
        lng: 4.352_f32,
        bearing: None,
        current_stop_sequence: None,
        timestamp: Some(u64::MAX),
    };
    let rt = RealtimeIndex::with_positions(0, [], [], [(TripId(0), pos)]);

    let q = r#"{ liveRefresh(legs: [
            { tripId: "NOPE", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { found vehicle { lat lng stale } }
       } }"#;
    let obj = live_refresh_query(live_refresh_graph(), rt, q);

    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["found"], Value::Boolean(false));
    assert_eq!(
        leg["vehicle"],
        Value::Null,
        "unresolved leg must always have vehicle=null"
    );
}

// ── Platform code on transit leg from/to ──────────────────────────────────────

fn transit_graph_with_platform() -> Graph {
    use gtfs_structures::{Availability, RouteType};
    use maas_rs::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::{
        BikeAttrs, EdgeData, NodeData, NodeID, StreetEdgeData, TransitEdgeData, TransitStopData,
        cost::VarGen,
        raptor::{Lookup, PatternInfo},
    };

    let mut g = Graph::new();

    let origin = g.add_node(osm_node("origin", 50.000, 4.000));
    let hub_a = g.add_node(osm_node("hub_a", 50.000, 4.008));
    let hub_b = g.add_node(osm_node("hub_b", 50.000, 4.090));
    let destination = g.add_node(osm_node("dest", 50.000, 4.098));

    let stop_a = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop A".into(),
        id: "SA".into(),
        lat_lng: LatLng { latitude: 50.000, longitude: 4.0081 },
        accessibility: Availability::Available,
        platform_code: Some("9".into()),
        parent_station: None,
    }));
    let stop_b = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop B".into(),
        id: "SB".into(),
        lat_lng: LatLng { latitude: 50.000, longitude: 4.0901 },
        accessibility: Availability::Available,
        platform_code: None,
        parent_station: None,
    }));

    let mk_foot = |o: NodeID, d: NodeID, len: usize| {
        EdgeData::Street(StreetEdgeData {
            origin: o, destination: d, length: len, partial: false,
            foot: true, bike: true, car: false,
            attrs: BikeAttrs::road_default(), elev_delta: 0,
            surface_speed: 100, var_gen: VarGen::NONE,
        })
    };
    let mk_conn = |o: NodeID, d: NodeID| {
        EdgeData::Street(StreetEdgeData {
            origin: o, destination: d, length: 8, partial: true,
            foot: true, bike: false, car: false,
            attrs: BikeAttrs::road_default(), elev_delta: 0,
            surface_speed: 100, var_gen: VarGen::NONE,
        })
    };

    g.add_edge(origin, mk_foot(origin, hub_a, 200));
    g.add_edge(hub_a, mk_foot(hub_a, origin, 200));
    g.add_edge(hub_a, mk_conn(hub_a, stop_a));
    g.add_edge(stop_a, mk_conn(stop_a, hub_a));
    g.add_edge(hub_b, mk_foot(hub_b, destination, 200));
    g.add_edge(destination, mk_foot(destination, hub_b, 200));
    g.add_edge(hub_b, mk_conn(hub_b, stop_b));
    g.add_edge(stop_b, mk_conn(stop_b, hub_b));

    g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
        origin: stop_a, destination: stop_b, route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 }, length: 5900,
    }));

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F, start_date: 0, end_date: 9999,
        added_dates: vec![], removed_dates: vec![],
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "M".into(), route_long_name: "Metro M".into(),
        route_type: RouteType::Subway, agency_id: AgencyId(0),
        route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0),
        service_id: ServiceId(0), bikes_allowed: None,
    }]);
    g.add_transit_trip_ids(vec!["T0".into()]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: 9 * 3600 + 600, arrival: 9 * 3600 + 1080, service_id: ServiceId(0),
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
            arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600, ..Default::default()
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1080, departure: 9 * 3600 + 1080, ..Default::default()
        });
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    }
    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

#[test]
fn graphql_transit_leg_from_platform_is_exposed() {
    let schema = build_schema(shared(transit_graph_with_platform()));
    let q = r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.0, toLng: 4.098,
                  modes: [WALK_TRANSIT], date: "2026-06-23", time: "09:00:00") {
          legs {
            ... on PlanTransitLeg {
              tripId
              from { stopId platform }
              to { stopId platform }
            }
          }
       } }"#;
    let resp = execute_sync(&schema, q);
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    let plans = match &data["raptor"] {
        Value::List(v) => v,
        other => panic!("expected plan list, got {other:?}"),
    };
    let leg = first_transit_leg(plans).expect("expected a transit leg");

    let from = match &leg["from"] {
        Value::Object(m) => m,
        other => panic!("expected from object, got {other:?}"),
    };
    assert_eq!(
        from["platform"],
        Value::String("9".into()),
        "board stop with platform_code '9' must expose platform: '9'"
    );

    let to = match &leg["to"] {
        Value::Object(m) => m,
        other => panic!("expected to object, got {other:?}"),
    };
    assert_eq!(
        to["platform"],
        Value::Null,
        "alight stop without platform_code must expose platform: null"
    );
}

// ── liveRefresh: platform change ─────────────────────────────────────────────

/// Builds a minimal graph for platform change tests.
/// Trip T0 boards at "station_11" (platform code "11") and alights at "SB".
/// "station_8" (platform code "8") is a sibling platform at the same parent
/// station ("station"). The realtime index says T0 now departs from platform 8.
fn live_refresh_platform_graph() -> Graph {
    use gtfs_structures::{Availability, RouteType};
    use maas_rs::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    };
    use maas_rs::structures::{
        EdgeData, NodeData, NodeID, TransitEdgeData, TransitStopData,
        raptor::{Lookup, PatternInfo},
    };

    let mut g = Graph::new();

    let mk_stop = |g: &mut Graph, name: &str, id: &str, lon: f64, plat: Option<&str>| -> NodeID {
        g.add_node(NodeData::TransitStop(TransitStopData {
            name: name.into(),
            id: id.into(),
            lat_lng: LatLng { latitude: 50.0, longitude: lon },
            accessibility: Availability::Available,
            platform_code: plat.map(|s| s.to_string()),
            parent_station: None,
        }))
    };

    let stop_a11 = mk_stop(&mut g, "Station Pl 11", "station_11", 4.000, Some("11"));
    let _stop_a8 = mk_stop(&mut g, "Station Pl 8",  "station_8",  4.000, Some("8"));
    let stop_b   = mk_stop(&mut g, "Stop B",         "SB",         4.050, None);

    g.add_edge(stop_a11, EdgeData::Transit(TransitEdgeData {
        origin: stop_a11, destination: stop_b,
        route_id: RouteId(0),
        timetable_segment: TimetableSegment { start: 0, len: 1 },
        length: 3500,
    }));

    g.add_transit_services(vec![ServicePattern {
        days_of_week: 0x7F, start_date: 0, end_date: 9999,
        added_dates: vec![], removed_dates: vec![],
    }]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "IC".into(), route_long_name: "Intercity".into(),
        route_type: RouteType::Rail,
        agency_id: AgencyId(0), route_color: None, route_text_color: None,
    }]);
    g.add_transit_trips(vec![TripInfo {
        trip_headsign: None, route_id: RouteId(0),
        service_id: ServiceId(0), bikes_allowed: None,
    }]);
    g.add_transit_trip_ids(vec!["T0".into()]);
    g.add_transit_departures(vec![TripSegment {
        trip_id: TripId(0),
        origin_stop_sequence: 0, destination_stop_sequence: 1,
        departure: 36000, arrival: 36900,
        service_id: ServiceId(0),
    }]);

    let push_pattern = |g: &mut Graph, stops: &[NodeID], trip: TripId, times: &[(u32, u32)]| {
        let ss = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(stops);
        g.push_transit_idx_pattern_stops(Lookup { start: ss, len: stops.len() });
        let ts = g.transit_pattern_trips_len();
        g.push_transit_pattern_trip(trip);
        g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });
        let sts = g.transit_pattern_stop_times_len();
        for &(arr, dep) in times {
            g.push_transit_pattern_stop_time(
                StopTime { arrival: arr, departure: dep, ..Default::default() },
            );
        }
        g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: times.len() });
        g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 1 });
    };
    push_pattern(&mut g, &[stop_a11, stop_b], TripId(0), &[(36000, 36000), (36900, 36900)]);

    g.set_distance_budget(f64::INFINITY);
    g.set_multiobj_street(true);
    g.build_raptor_index();
    g
}

#[test]
fn live_refresh_platform_change_board_stop() {
    use maas_rs::ingestion::gtfs::TripId;
    use std::collections::HashMap;

    let g = live_refresh_platform_graph();

    let trip = TripId(0);
    let actual_compact = g.stop_index_of("station_8").unwrap() as u32;
    let mut swaps: HashMap<(TripId, String), u32> = HashMap::new();
    swaps.insert((trip, "station".to_string()), actual_compact);

    let rt = maas_rs::structures::RealtimeIndex::with_all(0, [], [], [], [], swaps);

    let query = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "station_11", alightStopId: "SB" }]) {
          legs { tripId found platformChangeBoard { from to } platformChangeAlight { from to } }
       } }"#;

    let obj = live_refresh_query(g, rt, query);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    assert_eq!(legs.len(), 1);
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["found"], Value::Boolean(true), "leg must resolve");

    let pc = match &leg["platformChangeBoard"] {
        Value::Object(m) => m,
        other => panic!("expected platformChangeBoard object, got {other:?}"),
    };
    assert_eq!(pc["from"], Value::String("11".into()), "scheduled platform");
    assert_eq!(pc["to"],   Value::String("8".into()),  "actual RT platform");

    assert_eq!(
        leg["platformChangeAlight"],
        Value::Null,
        "alight stop has no platform suffix → no change"
    );
}

#[test]
fn live_refresh_no_platform_change_when_same_platform() {
    use maas_rs::ingestion::gtfs::TripId;
    use std::collections::HashMap;

    let g = live_refresh_platform_graph();

    let trip = TripId(0);
    let same_compact = g.stop_index_of("station_11").unwrap() as u32;
    let mut swaps: HashMap<(TripId, String), u32> = HashMap::new();
    swaps.insert((trip, "station".to_string()), same_compact);

    let rt = maas_rs::structures::RealtimeIndex::with_all(0, [], [], [], [], swaps);

    let query = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "station_11", alightStopId: "SB" }]) {
          legs { tripId found platformChangeBoard { from to } }
       } }"#;

    let obj = live_refresh_query(g, rt, query);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(
        leg["platformChangeBoard"],
        Value::Null,
        "same platform → no change"
    );
}

// ── Route-level alert matching ────────────────────────────────────────────────

/// Build the `live_refresh_graph()` fixture and populate `transit_route_ids`
/// so route-level alert matching works. Both trips (T0, T1) belong to route
/// `RouteId(0)` whose raw GTFS route_id we label "R1".
fn live_refresh_graph_with_route_ids() -> Graph {
    let mut g = live_refresh_graph();
    g.add_transit_route_ids(vec!["R1".into()]);
    g
}

#[test]
fn live_refresh_route_level_alert_surfaces_for_matching_route() {
    use maas_rs::ingestion::realtime::{AlertEntitySelector, ServiceAlert};
    use maas_rs::structures::RealtimeIndex;

    let g = live_refresh_graph_with_route_ids();
    let route_alert = ServiceAlert {
        header: Some("Line disruption".into()),
        description: Some("Service disrupted on R1".into()),
        cause: Some(1),
        effect: Some(2),
        active_period: vec![],
        informed_entity: vec![AlertEntitySelector {
            trip_id: None,
            route_id: Some("R1".into()),
            stop_id: None,
        }],
    };
    let rt = RealtimeIndex::with_alerts(1_700_000_000, [], [], [], [route_alert]);

    let query = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { tripId found alerts { header description effect } }
       } }"#;

    let obj = live_refresh_query(g, rt, query);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    assert_eq!(leg["found"], Value::Boolean(true), "leg must resolve");

    let alerts = match &leg["alerts"] {
        Value::List(v) => v,
        other => panic!("expected alerts list, got {other:?}"),
    };
    assert_eq!(
        alerts.len(),
        1,
        "route-level alert must surface for trip on route R1"
    );
    let alert_obj = match &alerts[0] {
        Value::Object(m) => m,
        other => panic!("expected alert object, got {other:?}"),
    };
    assert_eq!(
        alert_obj["header"],
        Value::String("Line disruption".into()),
        "alert header must match"
    );
    assert_eq!(
        alert_obj["effect"],
        Value::String("REDUCED_SERVICE".into()),
        "effect label must be mapped"
    );
}

#[test]
fn live_refresh_route_level_alert_not_surfaced_for_different_route() {
    use maas_rs::ingestion::realtime::{AlertEntitySelector, ServiceAlert};
    use maas_rs::structures::RealtimeIndex;

    let g = live_refresh_graph_with_route_ids();
    let other_route_alert = ServiceAlert {
        header: Some("Other line alert".into()),
        description: None,
        cause: None,
        effect: None,
        active_period: vec![],
        informed_entity: vec![AlertEntitySelector {
            trip_id: None,
            route_id: Some("R99".into()),
            stop_id: None,
        }],
    };
    let rt = RealtimeIndex::with_alerts(0, [], [], [], [other_route_alert]);

    let query = r#"{ liveRefresh(legs: [
            { tripId: "T0", boardStopId: "SA", alightStopId: "SB" }]) {
          legs { alerts { header } }
       } }"#;

    let obj = live_refresh_query(g, rt, query);
    let legs = match &obj["legs"] {
        Value::List(v) => v,
        other => panic!("expected legs list, got {other:?}"),
    };
    let leg = match &legs[0] {
        Value::Object(m) => m,
        other => panic!("expected leg object, got {other:?}"),
    };
    let alerts = match &leg["alerts"] {
        Value::List(v) => v,
        other => panic!("expected alerts list, got {other:?}"),
    };
    assert!(
        alerts.is_empty(),
        "alert for R99 must not surface for trip on route R1"
    );
}
