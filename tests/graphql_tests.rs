/// In-process GraphQL integration tests.
///
/// Tests execute queries directly against the async-graphql Schema without
/// an HTTP server, keeping them fast and hermetic.
use std::sync::Arc;

use async_graphql::{Name, Value};
use gtfs_structures::Availability;
use maas_rs::{
    ingestion::gtfs::{AgencyId, AgencyInfo, RouteId, RouteInfo},
    structures::{Graph, LatLng, NodeData, OsmNodeData, TransitStopData, raptor::PatternInfo},
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
                    modes: [WALK, WALK_TRANSIT, BIKE, BIKE_TRANSIT, BIKE_ON_TRANSIT])
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
    }));
    let stop_b = g.add_node(NodeData::TransitStop(TransitStopData {
        name: "Stop B".into(),
        id: "SB".into(),
        lat_lng: maas_rs::structures::LatLng {
            latitude: 50.000,
            longitude: 4.0901,
        },
        accessibility: Availability::Available,
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
        });
        g.push_transit_pattern_stop_time(StopTime {
            arrival: 9 * 3600 + 1080,
            departure: 9 * 3600 + 1080,
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
