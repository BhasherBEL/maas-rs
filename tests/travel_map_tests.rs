//! Integration tests for the travel-time map (isochrone / one-to-many reachability)
//! endpoint. Builds small synthetic graphs where reachability + times to a few grid
//! points are known, and asserts:
//!  - cells within `max_secs` are returned, cells beyond are omitted;
//!  - transit lets a far point be reachable when walking there would exceed the budget;
//!  - restricting modes (WALK vs WALK_TRANSIT) restricts the reachable set;
//!  - BEST vs AVERAGE over a 2-departure window differ as expected.

use gtfs_structures::{Availability, RouteType};
use maas_rs::{
    ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    },
    structures::{
        ActiveModes, BikeAttrs, BikeCost, EdgeData, Graph, LatLng, Mode, NodeData, NodeID,
        OsmNodeData, RealtimeIndex, ReliabilityBuckets, StreetEdgeData, StreetTimeModel,
        TransitEdgeData, TransitStopData, TravelAggregation, TravelCell,
        cost::VarGen,
        raptor::{Lookup, PatternInfo},
    },
};

// ── Helpers (mirrors of the graph_tests fixtures) ──────────────────────────────

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

fn all_days_service() -> ServicePattern {
    ServicePattern {
        days_of_week: 0x7F,
        start_date: 0,
        end_date: 9999,
        added_dates: vec![],
        removed_dates: vec![],
    }
}

/// Identity street-time model: routed seconds == median seconds (no stochastic
/// buffering), so access/egress times are predictable in assertions.
fn identity_street_time() -> StreetTimeModel {
    StreetTimeModel {
        access_percentile: 0.5,
        sigma_alpha: 0.0,
        sigma_floor: 0.0,
        sigma_cap: 0.5,
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
        g.push_transit_pattern_stop_time(StopTime {
            arrival: d,
            departure: d,
            ..Default::default()
        });
    }
    for &a in arrs {
        g.push_transit_pattern_stop_time(StopTime {
            arrival: a,
            departure: a,
            ..Default::default()
        });
    }
    g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 2 * n });

    g.push_transit_pattern(PatternInfo { route, num_trips: n as u32 });
}

// Deterministic tuning shared by every case.
const DATE: u32 = 9660; // arbitrary weekday-covered date (service is all-days)
const WEEKDAY: u8 = 0x01; // Monday; service is 0x7F so it is active
const START: u32 = 8 * 3600; // 08:00

fn buckets(g: &Graph) -> ReliabilityBuckets {
    ReliabilityBuckets::new(&g.raptor.reliability_bucket_edges)
}

fn walk_only() -> ActiveModes {
    ActiveModes::new(&[Mode::Walk])
}
fn walk_transit() -> ActiveModes {
    ActiveModes::new(&[Mode::Walk, Mode::WalkTransit])
}

/// Nearest emitted cell to `loc`, if any, with its travel-time seconds.
fn cell_near(cells: &[TravelCell], loc: LatLng, tol_m: f64) -> Option<u32> {
    cells
        .iter()
        .filter(|c| c.loc.dist(loc) <= tol_m)
        .min_by_key(|c| c.loc.dist(loc) as u64)
        .map(|c| c.seconds)
}

/// A long east–west corridor: origin O; a far junction JD ~3.5 km east (well beyond
/// a 20-min walk at 1.2 m/s). Stop A snaps to O; stop B snaps to JD. A fast bus
/// A→B departs at 08:05 and 08:20, riding 5 min. So a point near JD is reachable in
/// ~5–10 min by transit, but ~50 min on foot.
fn corridor_graph() -> Graph {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    // Keep the isochrone grid coarse so the tests run a handful of cells, not thousands.
    g.set_travel_map_grid_step_m(400.0);
    g.set_travel_map_window_sample_secs(600);

    // lon offset per metre at lat 50: ~1/71695 deg.
    let m2lon = |m: f64| 4.0 + m / 71_695.0;
    let o = g.add_node(osm_node("O", 50.000, 4.0));
    let jmid = g.add_node(osm_node("JMID", 50.000, m2lon(1750.0)));
    let jd = g.add_node(osm_node("JD", 50.000, m2lon(3500.0)));

    let stop_a = g.add_node(transit_stop("A", 50.000, m2lon(10.0)));
    let stop_b = g.add_node(transit_stop("B", 50.000, m2lon(3490.0)));

    add_street_bidir(&mut g, o, jmid, 1750);
    add_street_bidir(&mut g, jmid, jd, 1750);
    add_snap_bidir(&mut g, stop_a, o, 12); // ~10 s access
    add_snap_bidir(&mut g, stop_b, jd, 12); // ~10 s egress

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "X".into(),
        route_long_name: "Express".into(),
        route_type: RouteType::Bus,
        agency_id: AgencyId(0),
        route_color: None,
        route_text_color: None,
    }]);
    g.add_transit_trips(vec![
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
    ]);
    // Two departures: 08:05→08:10 and 08:20→08:25 (5-min ride).
    let deps = [START + 300, START + 1200];
    let arrs = [START + 600, START + 1500];
    add_two_stop_line(&mut g, stop_a, stop_b, RouteId(0), &[TripId(0), TripId(1)], &deps, &arrs, 3480);

    g.build_raptor_index();
    enable_contraction(&mut g);
    g
}

fn far_point() -> LatLng {
    // ~200 m west of JD (near stop B), i.e. ~3.3 km east of O.
    LatLng { latitude: 50.000, longitude: 4.0 + 3300.0 / 71_695.0 }
}

fn near_point() -> LatLng {
    // ~300 m east of O — an easy walk.
    LatLng { latitude: 50.000, longitude: 4.0 + 300.0 / 71_695.0 }
}

fn run(g: &Graph, am: &ActiveModes, max_secs: u32) -> Vec<TravelCell> {
    let bike = BikeCost::new(g.raptor.bike_profile);
    g.travel_time_map(
        LatLng { latitude: 50.000, longitude: 4.0 },
        START,
        DATE,
        WEEKDAY,
        max_secs,
        g.raptor.travel_map_grid_step_m,
        am,
        &buckets(g),
        g.raptor.arrival_slack_secs,
        false,
        false,
        &RealtimeIndex::new(),
        &bike,
    )
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[test]
fn walk_only_covers_near_point_and_omits_far_point() {
    let g = corridor_graph();
    // 20-min budget: the near point (~300 m ≈ 250 s walk) is in; the far point
    // (~3.3 km ≈ 2750 s walk) is far beyond 1200 s.
    let cells = run(&g, &walk_only(), 1200);
    assert!(!cells.is_empty(), "walk isochrone must emit some cells");

    let near = cell_near(&cells, near_point(), 250.0).expect("near point must be reachable on foot");
    assert!(near <= 1200, "near point within budget, got {near}s");
    // ~300 m at 1.2 m/s ≈ 250 s (plus grid snap slack).
    assert!(near < 600, "near point should be a short walk, got {near}s");

    assert!(
        cell_near(&cells, far_point(), 250.0).is_none(),
        "far point must be OMITTED for a walk-only 20-min isochrone"
    );
}

#[test]
fn transit_makes_far_point_reachable() {
    let g = corridor_graph();
    // Same 20-min budget, but WALK_TRANSIT: the express bus reaches stop B (near the
    // far point) in ~5–10 min, so the far point becomes reachable.
    let cells = run(&g, &walk_transit(), 1200);
    let far = cell_near(&cells, far_point(), 250.0)
        .expect("far point must be reachable via transit within 20 min");
    // Board ~08:05, ride 5 min, short egress walk: well under the walk-only time.
    assert!(far <= 1200, "far point within transit budget, got {far}s");
    assert!(
        far < 1500,
        "transit far-point time should reflect the ~5-15 min ride+walk, got {far}s"
    );

    // And walk-only at the same budget does NOT reach it — the mode restriction bites.
    let walk_cells = run(&g, &walk_only(), 1200);
    assert!(
        cell_near(&walk_cells, far_point(), 250.0).is_none(),
        "walk-only must not reach the far point at the same budget"
    );
}

#[test]
fn cells_never_exceed_max_secs() {
    let g = corridor_graph();
    let cells = run(&g, &walk_transit(), 900);
    assert!(!cells.is_empty());
    for c in &cells {
        assert!(c.seconds <= 900, "emitted cell exceeds max_secs: {}s", c.seconds);
    }
}

#[test]
fn best_vs_average_differ_over_a_window() {
    let g = corridor_graph();
    let bike = BikeCost::new(g.raptor.bike_profile);
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    // Window [08:00, 08:30] sampled every 10 min → departures 08:00, 08:10, 08:20, 08:30.
    // The far point is reachable via the 08:05 and 08:20 buses; on a departure that
    // just misses a bus, the wait inflates the time. So BEST (best departure) is
    // strictly faster than AVERAGE for the far point.
    let end = START + 1800;
    let common = |agg| {
        g.travel_time_map_window(
            center, START, end, DATE, WEEKDAY, 3000, g.raptor.travel_map_grid_step_m, agg,
            &walk_transit(), &buckets(&g), g.raptor.arrival_slack_secs, false, false,
            &RealtimeIndex::new(), &bike,
        )
    };
    let best = common(TravelAggregation::Best);
    let avg = common(TravelAggregation::Average);

    let best_far = cell_near(&best, far_point(), 300.0).expect("far point best");
    let avg_far = cell_near(&avg, far_point(), 300.0).expect("far point avg");
    assert!(
        avg_far > best_far,
        "AVERAGE ({avg_far}s) must exceed BEST ({best_far}s) at the far point over a window"
    );

    // The near point is a direct walk, departure-independent, so BEST == AVERAGE there.
    let best_near = cell_near(&best, near_point(), 250.0).expect("near best");
    let avg_near = cell_near(&avg, near_point(), 250.0).expect("near avg");
    assert_eq!(
        best_near, avg_near,
        "direct-walk near point is departure-independent: BEST must equal AVERAGE"
    );
}

/// Regression: a boarding stop reachable only by an access walk LONGER than the
/// configured `min_access_secs` (600 s) must still be seeded when the isochrone
/// budget is large. Origin O; stop FAR sits ~900 s walk east of O (well beyond the
/// 600 s access floor); a fast bus FAR→END reaches a distant area quickly. With a
/// 30-min WALK_TRANSIT isochrone the END area must be reachable — proving access is
/// seeded at the budget radius, not the fixed 600 s disc.
#[test]
fn access_radius_widens_to_budget_not_min_access() {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(400.0);
    let m2lon = |m: f64| 4.0 + m / 71_695.0;

    let o = g.add_node(osm_node("O", 50.000, 4.0));
    // ~1080 m east ≈ 900 s walk at 1.2 m/s — beyond the 600 s access floor.
    let jfar = g.add_node(osm_node("JFAR", 50.000, m2lon(1080.0)));
    // END junction ~5 km east, far past any 30-min walk.
    let jend = g.add_node(osm_node("JEND", 50.000, m2lon(5000.0)));

    let stop_far = g.add_node(transit_stop("FAR", 50.000, m2lon(1080.0)));
    let stop_end = g.add_node(transit_stop("END", 50.000, m2lon(4990.0)));

    add_street_bidir(&mut g, o, jfar, 1080); // 900 s walk O->FAR access node
    add_street_bidir(&mut g, jfar, jend, 3920);
    add_snap_bidir(&mut g, stop_far, jfar, 12); // ~10 s snap
    add_snap_bidir(&mut g, stop_end, jend, 12);

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "R".into(),
        route_long_name: "Rocket".into(),
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
    // Bus FAR->END departs 08:16 (after the ~900 s access walk from 08:00), 4-min ride.
    add_two_stop_line(
        &mut g, stop_far, stop_end, RouteId(0), &[TripId(0)],
        &[START + 960], &[START + 1200], 3910,
    );

    g.build_raptor_index();
    enable_contraction(&mut g);

    let bike = BikeCost::new(g.raptor.bike_profile);
    let end_area = LatLng { latitude: 50.000, longitude: m2lon(4900.0) };

    // 30-min budget: reachable (900 s access walk + wait + 4-min ride + short egress).
    let cells = g.travel_time_map(
        LatLng { latitude: 50.000, longitude: 4.0 },
        START,
        DATE,
        WEEKDAY,
        1800,
        g.raptor.travel_map_grid_step_m,
        &walk_transit(),
        &buckets(&g),
        g.raptor.arrival_slack_secs,
        false,
        false,
        &RealtimeIndex::new(),
        &bike,
    );
    assert!(
        cell_near(&cells, end_area, 300.0).is_some(),
        "END area must be reachable: the >600 s access-walk boarding stop must be seeded"
    );
}

// ── OPT-A: inverted fill_area must equal the pre-OPT-A per-cell reference ────────

/// Assert the inverted `travel_time_map` (one multi-source foot field + O(1) per-cell
/// reads) is BIT-IDENTICAL to the pre-OPT-A per-cell reference (two full graph searches
/// per cell) for the given modes/budget: same cell set, same coordinates, same seconds.
fn assert_reference_equiv(
    g: &Graph,
    center: LatLng,
    start: u32,
    max_secs: u32,
    am: &ActiveModes,
) {
    let bike = BikeCost::new(g.raptor.bike_profile);
    let step = g.raptor.travel_map_grid_step_m;
    let new = g.travel_time_map(
        center, start, DATE, WEEKDAY, max_secs, step, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );
    let reference = g.travel_time_map_reference(
        center, start, DATE, WEEKDAY, max_secs, step, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );

    // Cells are generated in the same (i, j) grid order by both paths, so compare directly.
    assert_eq!(
        new.len(),
        reference.len(),
        "cell count differs: inverted={} reference={} (max_secs={max_secs})",
        new.len(),
        reference.len()
    );
    for (a, b) in new.iter().zip(reference.iter()) {
        assert_eq!(
            a.loc.latitude, b.loc.latitude,
            "cell lat differs (max_secs={max_secs})"
        );
        assert_eq!(
            a.loc.longitude, b.loc.longitude,
            "cell lng differs (max_secs={max_secs})"
        );
        assert_eq!(
            a.seconds, b.seconds,
            "cell seconds differ at {:?}: inverted={} reference={} (max_secs={max_secs})",
            a.loc, a.seconds, b.seconds
        );
    }
}

#[test]
fn inverted_fill_equals_reference_corridor() {
    let g = corridor_graph();
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    // Sweep budgets: walk-only (only the centre-direct + same-chain terms fire), and
    // walk+transit (the multi-source stop seeding fires), at several radii so cells land
    // adjacent to stops (subtlety a), on the centre's own chain (b), and reachable by a
    // pure centre walk with no stop (d).
    for &max_secs in &[300u32, 600, 900, 1200, 1800] {
        assert_reference_equiv(&g, center, START, max_secs, &walk_only());
        assert_reference_equiv(&g, center, START, max_secs, &walk_transit());
    }
}

#[test]
fn inverted_fill_equals_reference_walk_only_no_transit() {
    // Pure street graph (no transit): only the centre-direct / same-chain terms exist.
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(150.0);
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.0 + 300.0 / 71_695.0));
    let c = g.add_node(osm_node("c", 50.000, 4.0 + 600.0 / 71_695.0));
    add_street_bidir(&mut g, a, b, 300);
    add_street_bidir(&mut g, b, c, 300);
    g.build_raptor_index();
    enable_contraction(&mut g);
    let center = LatLng { latitude: 50.000, longitude: 4.000 };
    for &max_secs in &[200u32, 400, 600, 900] {
        assert_reference_equiv(&g, center, START, max_secs, &walk_only());
    }
}

/// Subtlety (c): a stop reachable ON FOOT only *behind* another stop. The foot graph makes
/// stop junctions SINKS, so a walk path may never pass THROUGH a stop. This fixture chains
/// O — STOP_MID — STOP_END on one street, so the only foot route from O to STOP_END passes
/// through STOP_MID (a sink) and is therefore blocked for through-walking. Transit reaches
/// both stops. The inverted field must reproduce the reference exactly, proving the manual
/// stop-seed relaxation honours the sink rule (STOP_END's residual walk seeds from STOP_END
/// itself, never leaking a foot path THROUGH STOP_MID).
#[test]
fn inverted_fill_equals_reference_sink_rule() {
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(200.0);
    let m2lon = |m: f64| 4.0 + m / 71_695.0;

    let o = g.add_node(osm_node("O", 50.000, 4.0));
    // Two transit stops directly on the corridor, 200 m apart, each snapping to a nearby
    // OSM node so they become junctions on the chain.
    let jmid = g.add_node(osm_node("JMID", 50.000, m2lon(600.0)));
    let jend = g.add_node(osm_node("JEND", 50.000, m2lon(800.0)));
    let stop_mid = g.add_node(transit_stop("MID", 50.000, m2lon(600.0)));
    let stop_end = g.add_node(transit_stop("END", 50.000, m2lon(800.0)));

    add_street_bidir(&mut g, o, jmid, 600);
    add_street_bidir(&mut g, jmid, jend, 200);
    add_snap_bidir(&mut g, stop_mid, jmid, 6);
    add_snap_bidir(&mut g, stop_end, jend, 6);

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "L".into(),
        route_long_name: "Local".into(),
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
    // Bus MID->END departs 08:05, 2-min ride, so END is reached by transit (its residual
    // walk must NOT route back through MID on foot).
    add_two_stop_line(
        &mut g, stop_mid, stop_end, RouteId(0), &[TripId(0)],
        &[START + 300], &[START + 420], 200,
    );

    g.build_raptor_index();
    enable_contraction(&mut g);
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    for &max_secs in &[300u32, 600, 900, 1200] {
        assert_reference_equiv(&g, center, START, max_secs, &walk_transit());
    }
}

#[test]
fn inverted_fill_equals_reference_access_radius_fixture() {
    // Reuse the >600 s access-walk fixture geometry (a boarding stop far from the centre)
    // and pin equivalence across budgets straddling the access-walk / ride reachability.
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(400.0);
    let m2lon = |m: f64| 4.0 + m / 71_695.0;

    let o = g.add_node(osm_node("O", 50.000, 4.0));
    let jfar = g.add_node(osm_node("JFAR", 50.000, m2lon(1080.0)));
    let jend = g.add_node(osm_node("JEND", 50.000, m2lon(5000.0)));
    let stop_far = g.add_node(transit_stop("FAR", 50.000, m2lon(1080.0)));
    let stop_end = g.add_node(transit_stop("END", 50.000, m2lon(4990.0)));
    add_street_bidir(&mut g, o, jfar, 1080);
    add_street_bidir(&mut g, jfar, jend, 3920);
    add_snap_bidir(&mut g, stop_far, jfar, 12);
    add_snap_bidir(&mut g, stop_end, jend, 12);

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "R".into(),
        route_long_name: "Rocket".into(),
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
    add_two_stop_line(
        &mut g, stop_far, stop_end, RouteId(0), &[TripId(0)],
        &[START + 960], &[START + 1200], 3910,
    );
    g.build_raptor_index();
    enable_contraction(&mut g);
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    for &max_secs in &[600u32, 1200, 1800, 2400] {
        assert_reference_equiv(&g, center, START, max_secs, &walk_transit());
    }
}

// ── OPT-B (horizon) + OPT-C1 (skip-egress): optimized forward pass must equal the
//     unbounded, full-egress reference stop-arrival vector, bit for bit ──────────

/// The production `stop_arrivals` runs the forward pass with the OPT-B horizon and
/// (for non-vehicle modes) OPT-C1 skip-egress; `stop_arrivals_reference` runs the
/// SAME pass with both disabled (unbounded flood, full center egress). They must
/// produce an IDENTICAL per-stop earliest-arrival vector.
fn assert_stop_arrivals_equiv(
    g: &Graph,
    center: LatLng,
    start: u32,
    max_secs: u32,
    am: &ActiveModes,
) {
    let bike = BikeCost::new(g.raptor.bike_profile);
    let opt = g.stop_arrivals(
        center, start, DATE, WEEKDAY, max_secs, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );
    let reference = g.stop_arrivals_reference(
        center, start, DATE, WEEKDAY, max_secs, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );
    assert_eq!(
        opt.len(), reference.len(),
        "stop-arrival vector length differs (max_secs={max_secs})"
    );
    for (s, (o, r)) in opt.iter().zip(reference.iter()).enumerate() {
        // Optimized may leave a stop that arrives strictly AFTER the horizon as
        // u32::MAX (pruned) where the reference records its true (irrelevant) time —
        // that is allowed ONLY for arrivals with offset > max_secs, which fill_area
        // discards anyway. Any stop the reference reaches WITHIN the budget must be
        // identical in the optimized run.
        let ref_offset = r.saturating_sub(start);
        if *r != u32::MAX && ref_offset <= max_secs {
            assert_eq!(
                o, r,
                "stop {s} within-budget arrival differs: opt={o} reference={r} (max_secs={max_secs})"
            );
        }
    }
}

/// End-to-end: the optimized `travel_time_map` (OPT-B + OPT-C1 live) must yield the
/// SAME emitted cells as a fill over the UNBOUNDED, full-egress reference arrivals.
/// This is the strongest guarantee: it pins the user-facing isochrone cells against
/// the pre-optimization forward pass.
fn assert_travel_map_matches_unopt(
    g: &Graph,
    center: LatLng,
    start: u32,
    max_secs: u32,
    am: &ActiveModes,
) {
    let bike = BikeCost::new(g.raptor.bike_profile);
    let step = g.raptor.travel_map_grid_step_m;
    // Production path (optimized forward pass + inverted fill).
    let opt = g.travel_time_map(
        center, start, DATE, WEEKDAY, max_secs, step, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );
    // Reference path: unbounded/full-egress arrivals, then the SAME inverted fill via
    // travel_time_map with the reference arrivals is not directly exposed, so instead
    // rebuild cells from the reference arrivals using the public reference fill.
    let ref_arrivals = g.stop_arrivals_reference(
        center, start, DATE, WEEKDAY, max_secs, am, &buckets(g),
        g.raptor.arrival_slack_secs, false, false, &RealtimeIndex::new(), &bike,
    );
    let reference = g.fill_area_reference_from(center, start, max_secs, step, &ref_arrivals);

    assert_eq!(
        opt.len(), reference.len(),
        "cell count differs vs unopt reference (max_secs={max_secs}): opt={} ref={}",
        opt.len(), reference.len()
    );
    for (a, b) in opt.iter().zip(reference.iter()) {
        assert_eq!(a.loc.latitude, b.loc.latitude, "cell lat differs (max_secs={max_secs})");
        assert_eq!(a.loc.longitude, b.loc.longitude, "cell lng differs (max_secs={max_secs})");
        assert_eq!(
            a.seconds, b.seconds,
            "cell seconds differ at {:?}: opt={} unopt-ref={} (max_secs={max_secs})",
            a.loc, a.seconds, b.seconds
        );
    }
}

#[test]
fn opt_forward_pass_equals_unbounded_reference_corridor() {
    let g = corridor_graph();
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    for &max_secs in &[300u32, 600, 900, 1800] {
        // walk_only: no transit pass runs (both return all-MAX) — trivially equal.
        assert_stop_arrivals_equiv(&g, center, START, max_secs, &walk_only());
        // walk_transit: OPT-B + OPT-C1 both active; arrivals must match the reference.
        assert_stop_arrivals_equiv(&g, center, START, max_secs, &walk_transit());
        assert_travel_map_matches_unopt(&g, center, START, max_secs, &walk_transit());
    }
}

#[test]
fn opt_forward_pass_equals_unbounded_reference_access_radius() {
    // The >600 s access-walk fixture: a boarding stop far from the centre, reachable
    // only on a large budget. Straddle the reachability boundary so the horizon prunes
    // exactly the stops beyond the budget while keeping the in-budget ones identical.
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(400.0);
    let m2lon = |m: f64| 4.0 + m / 71_695.0;

    let o = g.add_node(osm_node("O", 50.000, 4.0));
    let jfar = g.add_node(osm_node("JFAR", 50.000, m2lon(1080.0)));
    let jend = g.add_node(osm_node("JEND", 50.000, m2lon(5000.0)));
    let stop_far = g.add_node(transit_stop("FAR", 50.000, m2lon(1080.0)));
    let stop_end = g.add_node(transit_stop("END", 50.000, m2lon(4990.0)));
    add_street_bidir(&mut g, o, jfar, 1080);
    add_street_bidir(&mut g, jfar, jend, 3920);
    add_snap_bidir(&mut g, stop_far, jfar, 12);
    add_snap_bidir(&mut g, stop_end, jend, 12);

    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "R".into(),
        route_long_name: "Rocket".into(),
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
    add_two_stop_line(
        &mut g, stop_far, stop_end, RouteId(0), &[TripId(0)],
        &[START + 960], &[START + 1200], 3910,
    );
    g.build_raptor_index();
    enable_contraction(&mut g);
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    for &max_secs in &[600u32, 900, 1200, 1500, 1800, 2400] {
        assert_stop_arrivals_equiv(&g, center, START, max_secs, &walk_transit());
        assert_travel_map_matches_unopt(&g, center, START, max_secs, &walk_transit());
    }
}

#[test]
fn opt_forward_pass_equals_unbounded_reference_sink_rule() {
    // Multi-stop-on-corridor (sink-rule) fixture, mirrored from the OPT-A test.
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(200.0);
    let m2lon = |m: f64| 4.0 + m / 71_695.0;

    let o = g.add_node(osm_node("O", 50.000, 4.0));
    let jmid = g.add_node(osm_node("JMID", 50.000, m2lon(600.0)));
    let jend = g.add_node(osm_node("JEND", 50.000, m2lon(800.0)));
    let stop_mid = g.add_node(transit_stop("MID", 50.000, m2lon(600.0)));
    let stop_end = g.add_node(transit_stop("END", 50.000, m2lon(800.0)));
    add_street_bidir(&mut g, o, jmid, 600);
    add_street_bidir(&mut g, jmid, jend, 200);
    add_snap_bidir(&mut g, stop_mid, jmid, 6);
    add_snap_bidir(&mut g, stop_end, jend, 6);
    g.add_transit_services(vec![all_days_service()]);
    g.add_transit_routes(vec![RouteInfo {
        route_short_name: "L".into(),
        route_long_name: "Local".into(),
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
    add_two_stop_line(
        &mut g, stop_mid, stop_end, RouteId(0), &[TripId(0)],
        &[START + 300], &[START + 420], 200,
    );
    g.build_raptor_index();
    enable_contraction(&mut g);
    let center = LatLng { latitude: 50.000, longitude: 4.0 };
    for &max_secs in &[300u32, 600, 900, 1200] {
        assert_stop_arrivals_equiv(&g, center, START, max_secs, &walk_transit());
        assert_travel_map_matches_unopt(&g, center, START, max_secs, &walk_transit());
    }
}

#[test]
fn walk_only_isochrone_needs_no_transit() {
    // A pure street graph (no transit at all): the walk isochrone still fills.
    let mut g = Graph::new();
    g.set_street_time(identity_street_time());
    g.set_walking_speed_mps(1.2);
    g.set_travel_map_grid_step_m(200.0);
    let a = g.add_node(osm_node("a", 50.000, 4.000));
    let b = g.add_node(osm_node("b", 50.000, 4.0 + 300.0 / 71_695.0));
    add_street_bidir(&mut g, a, b, 300);
    g.build_raptor_index();
    enable_contraction(&mut g);

    let bike = BikeCost::new(g.raptor.bike_profile);
    let cells = g.travel_time_map(
        LatLng { latitude: 50.000, longitude: 4.000 },
        START,
        DATE,
        WEEKDAY,
        600,
        g.raptor.travel_map_grid_step_m,
        &walk_only(),
        &buckets(&g),
        g.raptor.arrival_slack_secs,
        false,
        false,
        &RealtimeIndex::new(),
        &bike,
    );
    assert!(!cells.is_empty(), "walk-only isochrone must emit cells with no transit");
    // The origin itself is time 0.
    let origin = cell_near(&cells, LatLng { latitude: 50.0, longitude: 4.0 }, 150.0)
        .expect("a cell at the origin");
    assert!(origin < 200, "origin cell should be ~0 s, got {origin}s");
}

// ── Per-query grid step + safety cap ─────────────────────────────────────────────

/// Single-departure travel-time map with an EXPLICIT per-query grid step (metres).
fn run_step(g: &Graph, am: &ActiveModes, max_secs: u32, step_m: f64) -> Vec<TravelCell> {
    let bike = BikeCost::new(g.raptor.bike_profile);
    g.travel_time_map(
        LatLng { latitude: 50.000, longitude: 4.0 },
        START,
        DATE,
        WEEKDAY,
        max_secs,
        step_m,
        am,
        &buckets(g),
        g.raptor.arrival_slack_secs,
        false,
        false,
        &RealtimeIndex::new(),
        &bike,
    )
}

/// A finer grid step must yield MORE cells than a coarser one on the same query
/// (density scales ~1/step²), when neither hits the safety cap.
#[test]
fn finer_step_yields_more_cells() {
    let g = corridor_graph();
    let fine = run_step(&g, &walk_transit(), 1200, 100.0);
    let coarse = run_step(&g, &walk_transit(), 1200, 400.0);
    assert!(!fine.is_empty() && !coarse.is_empty());
    assert!(
        fine.len() > coarse.len(),
        "finer step must yield more cells: fine={} coarse={}",
        fine.len(),
        coarse.len()
    );
}

/// SAFETY CAP: a very small grid step over a LARGE reachable area must be coarsened
/// so the emitted cell count stays bounded by `travel_map_max_cells`, never blowing
/// up to millions of cells.
#[test]
fn tiny_step_on_large_area_is_bounded_by_cap() {
    let mut g = corridor_graph();
    // A small cap so the coarsening fires with a modest fixture; a 10 m step over the
    // corridor's multi-km reachable box would otherwise be tens of thousands of cells.
    let cap: u64 = 500;
    g.set_travel_map_max_cells(cap);

    let cells = run_step(&g, &walk_transit(), 1800, 10.0);
    assert!(!cells.is_empty(), "capped fill must still emit cells");
    assert!(
        (cells.len() as u64) <= cap,
        "cell count {} must be bounded by the cap {cap}",
        cells.len()
    );

    // Sanity: without the tiny step (a coarse step the cap does not touch) the same
    // query is well under the cap too, and the tiny-step result did not silently
    // collapse to a single cell.
    assert!(cells.len() > 1, "coarsened fill should still be a real grid");
}
