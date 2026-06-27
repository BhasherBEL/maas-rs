use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};

use crate::ingestion::gtfs::date_to_days;
use crate::structures::plan::{ExplainResult, Plan};
use crate::structures::{
    ActiveModes, Endpoint, Graph, Mode, NodeID, RealtimeIndex, ReliabilityBuckets,
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
}

/// Effective bike cost profile for a query: the per-request override if present,
/// else the graph's configured default.
fn resolve_bike_profile(graph: &Graph, query: &RouteQuery) -> crate::structures::BikeProfile {
    query.bike_profile.unwrap_or(graph.raptor.bike_profile)
}

/// Edge-snap a coordinate for a bike: prefer the nearest *rideable* edge so a bike
/// isn't dropped onto a footway and forced to push from the first metre; only fall
/// back to a foot-only edge when no bike edge is in range. `None` when edge snapping
/// is off or no usable edge is within range/guard.
fn bike_edge_endpoint(graph: &Graph, lat: f64, lng: f64) -> Option<Endpoint> {
    if !graph.raptor.edge_snap {
        return None;
    }
    let radius = graph.raptor.edge_snap_radius_m;
    let guard = graph.raptor.max_snap_distance_m as f64;
    let snap = graph
        .snap_to_edge(lat, lng, radius, |s| s.bike)
        .or_else(|| graph.snap_to_edge(lat, lng, radius, |s| s.foot));
    let (ep, perp) = snap?;
    (perp <= guard).then_some(ep)
}

/// The bike-snapped origin/destination [`Endpoint`]s for a query: the nearest
/// rideable edge projection, falling back to the snapped node when no edge is in
/// range. Fed to `enrich_street_legs`, which routes a direct bike leg between these
/// (stitching the on-edge stubs) so the leg starts/ends at the exact clicked points.
fn bike_endpoints(
    graph: &Graph,
    query: &RouteQuery,
    origin: NodeID,
    destination: NodeID,
) -> (Endpoint, Endpoint) {
    // Contracted routing uses the junction-to-junction bike scaffold (no on-edge
    // projection stubs), and `snap_to_edge` reads the dropped g kdtree — so bridge to
    // the bounding-junction NodeIDs directly. Enrich routes these junction-to-junction.
    if graph.use_contracted() {
        return (Endpoint::Node(origin), Endpoint::Node(destination));
    }
    let o = bike_edge_endpoint(graph, query.from_lat, query.from_lng)
        .unwrap_or(Endpoint::Node(origin));
    let d = bike_edge_endpoint(graph, query.to_lat, query.to_lng)
        .unwrap_or(Endpoint::Node(destination));
    (o, d)
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
    let cg = graph.contracted.as_ref().unwrap();
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

/// Snap a query coordinate to the street network, rejecting coordinates that
/// land farther than the configured snap-distance guard.
fn snap_node(
    graph: &Graph,
    lat: f64,
    lng: f64,
    endpoint: &str,
) -> Result<crate::structures::NodeID, async_graphql::Error> {
    let (dist_m, node) = graph
        .nearest_node_dist(lat, lng)
        .ok_or_else(|| async_graphql::Error::new(format!("No node near {endpoint}")))?;
    let max = graph.raptor.max_snap_distance_m;
    if dist_m > max as f64 {
        return Err(async_graphql::Error::new(format!(
            "{endpoint} is too far from the network (nearest node {:.0} m away, max {} m)",
            dist_m, max
        )));
    }
    Ok(*node)
}

use crate::structures::QueryEndpoints;

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

    let (origin, destination, endpoints) = if graph.use_contracted() {
        let (o, o_coord) = arena_snap_node(graph, query.from_lat, query.from_lng, "departure")?;
        let (d, d_coord) = arena_snap_node(graph, query.to_lat, query.to_lng, "arrival")?;
        (
            o,
            d,
            Some(QueryEndpoints {
                origin: o_coord,
                destination: d_coord,
            }),
        )
    } else {
        let o = snap_node(graph, query.from_lat, query.from_lng, "departure")?;
        let d = snap_node(graph, query.to_lat, query.to_lng, "arrival")?;
        (o, d, None)
    };

    let min_access = query
        .min_access_secs
        .unwrap_or(graph.raptor.min_access_secs);

    Ok((origin, destination, time, date, weekday, min_access, endpoints))
}

pub fn route(
    graph: &Graph,
    query: &RouteQuery,
    rt: &RealtimeIndex,
) -> Result<Vec<Plan>, async_graphql::Error> {
    let (origin, destination, time, date, weekday, min_access, endpoints) =
        resolve_query_params(graph, query)?;
    let ep = endpoints.as_ref();
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let am = resolve_modes(query)?;

    let bike = crate::structures::BikeCost::new(
        resolve_bike_profile(graph, query),
        graph.raptor.walking_speed_mps,
    );
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
                rt,
                &am,
                &bike,
                query.terminal_deadline,
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
            rt,
            &am,
            &bike,
            query.terminal_deadline,
            ep,
        ),
    };

    let (bike_origin, bike_dest) = bike_endpoints(graph, query, origin, destination);
    graph.enrich_street_legs(
        &mut plans,
        origin,
        destination,
        bike_origin,
        bike_dest,
        &bike,
        query.terminal_deadline,
    );

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
    let (origin, destination, time, date, weekday, min_access, endpoints) =
        resolve_query_params(graph, query)?;
    let ep = endpoints.as_ref();
    let (buckets, slack) = resolve_tuning(graph, query)?;
    let am = resolve_modes(query)?;

    // Note: the explain path does not apply the overnight pass — it's a debug view
    // of a single RAPTOR run and overnight merging would complicate candidate provenance.
    let bike = crate::structures::BikeCost::new(
        resolve_bike_profile(graph, query),
        graph.raptor.walking_speed_mps,
    );
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
            rt,
            &am,
            &bike,
            ep,
        ),
    };

    let (bike_origin, bike_dest) = bike_endpoints(graph, query, origin, destination);
    graph.enrich_street_legs(
        &mut result.plans,
        origin,
        destination,
        bike_origin,
        bike_dest,
        &bike,
        query.terminal_deadline,
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{LatLng, NodeData, OsmNodeData};

    fn graph_with_node_at(lat: f64, lon: f64) -> Graph {
        let mut g = Graph::new();
        g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "n1".to_string(),
            lat_lng: LatLng {
                latitude: lat,
                longitude: lon,
            },
        }));
        g.build_raptor_index();
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
            reliability_bucket_edges: None,
            modes: None,
            bike_profile: None,
            terminal_deadline: false,
        }
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
        g.set_multiobj_street(true);
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
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Walk]),
            bike_profile: None,
            terminal_deadline: false,
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
        g.set_multiobj_street(true);
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
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Bike]),
            bike_profile: None,
            terminal_deadline: false,
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
    /// kdtree. It passes once snapping is arena-based and gated on `use_contracted()` so
    /// flag-on snaps via the segment R-tree whether or not g is present — making the drop
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

    /// CORRECTNESS companion to the drop oracle (the enrich-surface t2 analog): with
    /// `g` PRESENT in both runs and JUNCTION endpoints (no snapping confound), the
    /// contracted reconstruction (`leg_option`/`street_steps` off carried arena edges)
    /// must be byte-identical to the full-graph reconstruction (off `path_edges`). The
    /// drop oracle is g-freeness only (before/after are both flag-on, so a consistently-
    /// wrong carry-through passes it); this gate catches a wrong contracted
    /// reconstruction. Calls the leg-option/step builders directly so it depends only on
    /// reconstruction, not the flag-off/flag-on snapping policy difference.
    #[test]
    fn contracted_leg_options_match_full_graph() {
        use crate::structures::cost::{LegRole, RoutingMode};
        use crate::structures::BikeCost;
        use crate::structures::NodeID;
        use crate::structures::plan::PlanPlace;

        let g_off = coord_drop_gate_graph(false);
        let g_on = coord_drop_gate_graph(true);
        let bike = BikeCost::new(g_off.raptor.bike_profile, g_off.raptor.walking_speed_mps);
        // a → b: the whole [a,i1,i2,i3,b] chain. Both endpoints are junctions, so there
        // is no snapping; only the per-edge reconstruction differs across the flag.
        let (a, b) = (NodeID(0), NodeID(4));
        for mode in [RoutingMode::Bike, RoutingMode::Walk, RoutingMode::Drive] {
            let off = g_off.multiobj_leg_options(a, b, mode, LegRole::Neutral, &bike);
            let on = g_on.multiobj_leg_options(a, b, mode, LegRole::Neutral, &bike);
            let to = PlanPlace {
                node_id: b,
                stop_position: None,
                arrival: None,
                departure: None,
            };
            let steps_off =
                g_off.street_steps(&off[0].nodes, &off[0].edges, mode, &bike, 0, to.clone());
            let steps_on = g_on.street_steps(&on[0].nodes, &on[0].edges, mode, &bike, 0, to);
            assert_eq!(
                format!("{steps_off:?}"),
                format!("{steps_on:?}"),
                "{mode:?}: contracted street steps must match full-graph street steps"
            );
            // `edges` is the internal arena carry (graphql-skipped); flag-off never
            // populates it, so clear it before comparing the user-facing reconstruction.
            // Its correctness is exercised by the street_steps equality above + the drop
            // oracle's geometry/step equality.
            let strip = |mut v: Vec<crate::structures::plan::LegOption>| {
                for o in &mut v {
                    o.edges.clear();
                }
                v
            };
            assert_eq!(
                format!("{:?}", strip(off)),
                format!("{:?}", strip(on)),
                "{mode:?}: contracted leg options must match full-graph leg options"
            );
        }
    }

    /// Chain a — i1 — i2 — i3 — b: i1..i3 are degree-2 interior (contracted away),
    /// a and b are degree-1 junctions. A coordinate near i2 snaps mid-super-edge.
    /// `contract` ⇒ build + bake the union contracted graph and flip `node_contraction`.
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
        g.set_multiobj_street(true);
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
            g.set_node_contraction(true);
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

    #[test]
    fn bike_arrival_reaches_projection_on_oneway_dest_edge() {
        // Destination sits mid-way along a ONEWAY rideable edge a→b, nearer to b.
        // `Endpoint::node()` would pick b — but b has no edge toward the projection
        // (the oneway only allows a→proj), so the leg must route to a and ride the
        // a→proj stub. Regression: enrich re-routed node-to-node to b and ended the
        // leg ~one stub short of the clicked point. The served leg's final geometry
        // point must equal the projection, not node b.
        use crate::structures::plan::PlanLeg;
        use crate::structures::Mode;
        let mut g = Graph::new();
        let o = osm(&mut g, "o", 50.000, 4.000);
        let a = osm(&mut g, "a", 50.000, 4.001);
        let b = osm(&mut g, "b", 50.000, 4.003);
        street(&mut g, o, a, 100, true, true); // access O→A
        street(&mut g, a, b, 200, true, true); // ONEWAY A→B (no B→A)
        g.build_raptor_index();
        g.set_distance_budget(f64::INFINITY);
        g.set_multiobj_street(true);
        g.build_edge_index();

        // Click at lon 4.0025 (t=0.75 along A→B): proj nearer to B, so node()=B.
        let q = RouteQuery {
            from_lat: 50.000,
            from_lng: 4.000,
            to_lat: 50.000,
            to_lng: 4.0025,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Bike]),
            bike_profile: None,
            terminal_deadline: false,
        };
        let plans = route(&g, &q, &RealtimeIndex::new()).unwrap();
        let bike = plans
            .iter()
            .find(|p| p.mode == Mode::Bike)
            .expect("a bike plan");
        let PlanLeg::Walk(leg) = bike.legs.last().expect("a leg") else {
            panic!("expected a walk leg in a bike plan")
        };
        let last = leg.geometry.last().expect("geometry");
        assert!(
            (last.lat - 50.000).abs() < 1e-6 && (last.lon - 4.0025).abs() < 1e-6,
            "leg must end at the on-edge projection (50.000, 4.0025), got ({}, {})",
            last.lat,
            last.lon
        );
    }

    #[test]
    fn bike_arrival_does_not_overshoot_to_farther_natural_end() {
        // Destination on a TWO-WAY edge a(north)–b(south), projected nearer to a (so
        // the nearer-by-stub heuristic would pick a). But the origin is south, so the
        // natural route reaches b first; ending at a forces a doubling-back overshoot
        // north past the clicked point. The cost-optimal end is b — the leg must never
        // run north of the projection.
        use crate::structures::plan::PlanLeg;
        use crate::structures::Mode;
        let mut g = Graph::new();
        let o = osm(&mut g, "o", 50.000, 4.000);
        let b = osm(&mut g, "b", 50.001, 4.000); // south end of the cycleway
        let a = osm(&mut g, "a", 50.003, 4.000); // north end
        street(&mut g, o, b, 111, true, true); // O↔b road
        street(&mut g, b, o, 111, true, true);
        street(&mut g, a, b, 222, true, true); // a↔b cycleway, two-way
        street(&mut g, b, a, 222, true, true);
        g.build_raptor_index();
        g.set_distance_budget(f64::INFINITY);
        g.set_multiobj_street(true);
        g.build_edge_index();

        // Click nearer to a (lat 50.0022 ⇒ ~89 m from a, ~133 m from b).
        let q = RouteQuery {
            from_lat: 50.000,
            from_lng: 4.000,
            to_lat: 50.0022,
            to_lng: 4.000,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Bike]),
            bike_profile: None,
            terminal_deadline: false,
        };
        let plans = route(&g, &q, &RealtimeIndex::new()).unwrap();
        let bike = plans
            .iter()
            .find(|p| p.mode == Mode::Bike)
            .expect("a bike plan");
        let PlanLeg::Walk(leg) = bike.legs.last().expect("a leg") else {
            panic!("expected a walk leg")
        };
        let max_lat = leg
            .geometry
            .iter()
            .map(|c| c.lat)
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            max_lat <= 50.0022 + 1e-4,
            "route overshot north of the projection to lat {max_lat} (expected ≤ ~50.0022)"
        );
        let last = leg.geometry.last().expect("geometry");
        assert!(
            (last.lat - 50.0022).abs() < 1e-3,
            "leg should end at the projection (~50.0022), got {}",
            last.lat
        );
    }

    #[test]
    fn gated_long_bike_leg_still_snaps_to_projection() {
        // A bike leg longer than the enrich gate gets no alternatives, but must still
        // be edge-snapped (single route reaching the exact clicked point) — the old
        // ungated snapper guaranteed this; the enrich-path rewrite must not regress it.
        use crate::structures::plan::PlanLeg;
        use crate::structures::Mode;
        let mut g = Graph::new();
        let o = osm(&mut g, "o", 50.000, 4.000);
        let a = osm(&mut g, "a", 50.000, 4.001);
        let b = osm(&mut g, "b", 50.000, 4.003);
        street(&mut g, o, a, 100, true, true);
        street(&mut g, a, b, 200, true, true); // ONEWAY A→B
        g.build_raptor_index();
        g.set_distance_budget(f64::INFINITY);
        g.set_multiobj_street(true);
        g.set_multiobj_street_max_len_m(1); // gate every bike leg
        g.build_edge_index();

        let q = RouteQuery {
            from_lat: 50.000,
            from_lng: 4.000,
            to_lat: 50.000,
            to_lng: 4.0025,
            date: NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(),
            time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: None,
            arrival_slack_secs: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Bike]),
            bike_profile: None,
            terminal_deadline: false,
        };
        let plans = route(&g, &q, &RealtimeIndex::new()).unwrap();
        let bike = plans
            .iter()
            .find(|p| p.mode == Mode::Bike)
            .expect("a bike plan");
        let PlanLeg::Walk(leg) = bike.legs.last().expect("a leg") else {
            panic!("expected a walk leg")
        };
        assert!(
            leg.alternatives.is_empty(),
            "gated leg carries no picker alternatives"
        );
        let last = leg.geometry.last().expect("geometry");
        assert!(
            (last.lat - 50.000).abs() < 1e-6 && (last.lon - 4.0025).abs() < 1e-6,
            "gated leg must still snap to projection (50.000, 4.0025), got ({}, {})",
            last.lat,
            last.lon
        );
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

    fn osm(g: &mut Graph, id: &str, lat: f64, lon: f64) -> NodeID {
        g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: id.into(),
            lat_lng: LatLng {
                latitude: lat,
                longitude: lon,
            },
        }))
    }

    #[test]
    fn bike_edge_endpoint_prefers_bike_edge_over_nearer_footway() {
        // A foot-only edge sits closest to the query; a bike edge is a touch
        // farther. Bike snapping must land on the bike edge, never the footway.
        let mut g = Graph::new();
        let fa = osm(&mut g, "fa", 50.0001, 4.000);
        let fb = osm(&mut g, "fb", 50.0001, 4.006);
        let ba = osm(&mut g, "ba", 49.9997, 4.000);
        let bb = osm(&mut g, "bb", 49.9997, 4.006);
        street(&mut g, fa, fb, 430, true, false);
        street(&mut g, fb, fa, 430, true, false);
        street(&mut g, ba, bb, 430, false, true);
        street(&mut g, bb, ba, 430, false, true);
        g.build_edge_index();

        let ep = bike_edge_endpoint(&g, 50.0000, 4.003).expect("snaps to an edge");
        let Endpoint::OnEdge { a, b, .. } = ep else {
            panic!("expected OnEdge");
        };
        assert!(
            (a == ba && b == bb) || (a == bb && b == ba),
            "bike snap landed on the footway instead of the bike edge: {a:?},{b:?}"
        );
    }

    #[test]
    fn bike_edge_endpoint_falls_back_to_foot_when_no_bike_edge() {
        // Only a foot-only edge is in range: a bike with no rideable edge nearby
        // still snaps onto the footway (it will push from there).
        let mut g = Graph::new();
        let fa = osm(&mut g, "fa", 50.0001, 4.000);
        let fb = osm(&mut g, "fb", 50.0001, 4.006);
        street(&mut g, fa, fb, 430, true, false);
        street(&mut g, fb, fa, 430, true, false);
        g.build_edge_index();

        let ep = bike_edge_endpoint(&g, 50.0000, 4.003).expect("falls back to footway");
        let Endpoint::OnEdge { a, b, .. } = ep else {
            panic!("expected OnEdge");
        };
        assert!(
            (a == fa && b == fb) || (a == fb && b == fa),
            "expected the footway as fallback: {a:?},{b:?}"
        );
    }
}
