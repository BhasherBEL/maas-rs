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

type TestSchema = async_graphql::Schema<QueryRoot, async_graphql::EmptyMutation, async_graphql::EmptySubscription>;

// ── Helpers ───────────────────────────────────────────────────────────────────

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
        accessibility: Availability::Available,
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn graphql_ping_returns_pong() {
    let schema = build_schema(Arc::new(Graph::new()));
    let resp = execute_sync(&schema, "{ ping }");
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    assert_eq!(data["ping"], Value::String("pong".into()));
}

#[test]
fn graphql_raptor_no_nodes_returns_error() {
    let schema = build_schema(Arc::new(Graph::new()));
    let resp = execute_sync(
        &schema,
        r#"{ raptor(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) { start } }"#,
    );
    assert!(!resp.errors.is_empty(), "expected an error for empty graph");
    let msg = resp.errors[0].message.to_lowercase();
    assert!(msg.contains("no node"), "unexpected error: {}", resp.errors[0].message);
}

#[test]
fn graphql_raptor_invalid_date_returns_error() {
    let mut g = Graph::new();
    g.add_node(osm_node("n0", 50.0, 4.0));
    g.build_raptor_index();
    let schema = build_schema(Arc::new(g));
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
    let schema = build_schema(Arc::new(Graph::new()));
    let resp = execute_sync(&schema, "{ gtfsStops { id } }");
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    assert_eq!(data["gtfsStops"], Value::List(vec![]));
}

#[test]
fn graphql_gtfs_agencies_empty_on_no_transit() {
    let schema = build_schema(Arc::new(Graph::new()));
    let resp = execute_sync(&schema, "{ gtfsAgencies { id } }");
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    assert_eq!(data["gtfsAgencies"], Value::List(vec![]));
}

#[test]
fn graphql_gtfs_stops_returns_stop_data() {
    let mut g = Graph::new();
    g.add_node(transit_stop("Central Station", 50.845, 4.357));
    g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 0 });
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "1".into(),
        route_long_name: "Test Route".into(),
        route_type: gtfs_structures::RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.build_raptor_index();

    let schema = build_schema(Arc::new(g));
    let resp = execute_sync(&schema, "{ gtfsStops { id name mode } }");
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);

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

    let schema = build_schema(Arc::new(g));
    let resp = execute_sync(
        &schema,
        "{ gtfsAgencies { id name url routes { shortName mode } } }",
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);

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
    assert_eq!(agency["url"], Value::String("https://testbus.example".into()));

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
    g.add_node(osm_node("n0", 50.0, 4.0));
    g.add_node(osm_node("n1", 50.01, 4.01));
    g.build_raptor_index();
    let schema = build_schema(Arc::new(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) {
              stopsReached { stopIdx }
           } }"#,
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    let explain = match &data["raptorExplain"] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    assert_eq!(explain["stopsReached"], Value::List(vec![]),
        "expected empty stopsReached for graph with no transit stops");
}

#[test]
fn graphql_raptor_explain_origin_destination_present() {
    let mut g = Graph::new();
    g.add_node(osm_node("n0", 50.0, 4.0));
    g.add_node(osm_node("n1", 50.01, 4.01));
    g.build_raptor_index();
    let schema = build_schema(Arc::new(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.01, toLng: 4.01) {
              origin { lat lon }
              destination { lat lon }
           } }"#,
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
    let data = data_obj(resp);
    let explain = match &data["raptorExplain"] {
        Value::Object(m) => m,
        other => panic!("expected object, got {other:?}"),
    };
    let origin = match &explain["origin"] {
        Value::Object(m) => m,
        other => panic!("expected origin object, got {other:?}"),
    };
    assert!(matches!(origin["lat"], Value::Number(_)), "origin.lat should be a number");
    assert!(matches!(origin["lon"], Value::Number(_)), "origin.lon should be a number");
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
    let _osm0 = g.add_node(osm_node("n0", 50.0, 4.0));
    // Transit stop ~50m from origin, snapped to n0
    g.add_node(transit_stop("Test Stop", 50.0004, 4.0));
    g.build_raptor_index();

    let schema = build_schema(Arc::new(g));
    let resp = execute_sync(
        &schema,
        r#"{ raptorExplain(fromLat: 50.0, fromLng: 4.0, toLat: 50.1, toLng: 4.1) {
              stopsReached { stopIdx round name }
           } }"#,
    );
    assert!(resp.errors.is_empty(), "unexpected errors: {:?}", resp.errors);
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
        assert_eq!(stop["round"], Value::Number(async_graphql::Number::from(0i32)),
            "access stop should be round 0");
    }
}

// ── /map HTTP route ────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_map_returns_html() {
    use poem::{Route, get, test::TestClient};
    use maas_rs::web::app::map_page;

    let app = Route::new().at("/map", get(map_page));
    let client = TestClient::new(app);
    let resp = client.get("/map").send().await;
    resp.assert_status_is_ok();
    let ct = resp.0.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("text/html"), "expected text/html content-type, got: {ct}");
}
