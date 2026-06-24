//! Post-hoc path-time distribution (Phase 3). Sums per-edge `(mean, variance)`
//! moments along an ALREADY-CHOSEN path and inflates the total variance by a
//! systematic coefficient-of-variation term so long legs widen honestly. This is
//! the generalization of the access/egress `StreetTimeModel` from the two special
//! cases to an arbitrary path. Probability NEVER enters the search — this runs
//! only here, on a fixed node list. `TimeMoments::bracket()` yields `[p50, p95]`.
//!
//! SOFT SPOT: walk/drive edge means use `length / mode_speed` (via `edge_moments`)
//! while bike uses `BikeCost::edge_time`; both are the same primitives the search
//! uses for the Time axis, modulo sub-second rounding. Parallel edges between the
//! same node pair are disambiguated by "first matching street edge" — street
//! graphs rarely carry parallel edges and their moments differ negligibly.

use super::{Graph, PrevCtx};
use crate::structures::cost::{
    RoutingMode, TimeMoments, edge_moments, edge_time_penalty, edge_variance,
};
use crate::structures::{BikeCost, EdgeData, NodeID};

impl Graph {
    /// Sum per-edge time moments along `nodes` (a fixed path) for `mode`, then add
    /// the systematic variance term `(systematic_cv · mean)²`. Returns `ZERO` for a
    /// path with fewer than two nodes. Use `.bracket()` for `[p50, p95]`.
    pub fn annotate_path(&self, nodes: &[NodeID], mode: RoutingMode) -> TimeMoments {
        if nodes.len() < 2 {
            return TimeMoments::ZERO;
        }
        let speed = match mode {
            RoutingMode::Walk => self.raptor.walking_speed_mps,
            RoutingMode::Bike => self.raptor.cycling_speed_mps,
            RoutingMode::Drive => self.raptor.driving_speed_mps,
        };
        let bike = self.default_bike_cost();
        let model = self.raptor.variance_model;

        let mut total = TimeMoments::ZERO;
        let mut incoming: Option<(f64, f64)> = None;
        // Previous-edge context (bike): mirrors the search fold so the displayed
        // [p50,p95] reflects the SAME corner slow-down, dismount stop, and dismount
        // uncertainty the route was chosen on.
        let mut prev: Option<PrevCtx> = None;
        for w in nodes.windows(2) {
            let Some(street) = self.edges[w[0].0].iter().find_map(|e| match e {
                EdgeData::Street(s) if s.destination == w[1] => Some(s),
                _ => None,
            }) else {
                incoming = None;
                prev = None;
                continue;
            };
            let this_dir = self.dir_between(w[0], w[1]);
            let mut mean = if mode == RoutingMode::Bike {
                bike.edge_time(street) as f64 + edge_time_penalty(street, &model)
            } else {
                edge_moments(street, speed, &model).mean
            };
            let mut var = edge_variance(mode, street, &model, incoming, this_dir);
            if mode == RoutingMode::Bike {
                mean += bike.speed_change_secs(prev, street, this_dir);
                let this_push = BikeCost::is_push(&street.attrs);
                if let Some(p) = prev {
                    if !p.push && this_push {
                        var += model.push_sigma * model.push_sigma;
                    }
                }
                let exit_speed = bike.required_speed(prev, street, this_dir);
                prev = Some(PrevCtx {
                    dir: this_dir,
                    len: street.length as f64,
                    cruise: if this_push { 0.0 } else { bike.cruise_speed(street) },
                    push: this_push,
                    speed: exit_speed,
                });
            }
            total = total.added(&TimeMoments { mean, var });
            incoming = Some(this_dir);
        }
        total.var += (self.raptor.systematic_cv * total.mean).powi(2);
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::VarGen;
    use crate::structures::{
        BikeAttrs, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData, Surface,
    };

    fn straight_path_graph() -> (Graph, Vec<NodeID>) {
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
        let b = g.add_node(mk("b", 50.000, 4.001));
        let c = g.add_node(mk("c", 50.000, 4.002));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, vg: VarGen| {
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
                car: true,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: vg,
            })
        };
        g.add_edge(a, edge(a, b, 120, VarGen::SIGNALIZED));
        g.add_edge(b, edge(b, c, 240, VarGen::NONE));
        (g, vec![a, b, c])
    }

    #[test]
    fn empty_or_single_node_path_is_zero() {
        let (g, path) = straight_path_graph();
        assert_eq!(g.annotate_path(&[], RoutingMode::Walk), TimeMoments::ZERO);
        assert_eq!(
            g.annotate_path(&path[..1], RoutingMode::Walk),
            TimeMoments::ZERO
        );
    }

    #[test]
    fn mean_is_sum_of_edge_means_walk() {
        let (g, path) = straight_path_graph();
        let speed = g.raptor.walking_speed_mps;
        let signal_delay = g.raptor.variance_model.signal_delay_minor;
        let expected_mean = (120.0 / speed).round() + signal_delay + (240.0 / speed).round();
        let m = g.annotate_path(&path, RoutingMode::Walk);
        assert_eq!(
            m.mean, expected_mean,
            "p50 is the sum of per-edge kinematic means plus the signalized edge's delay"
        );
    }

    #[test]
    fn variance_includes_generators_and_systematic_term() {
        let (mut g, path) = straight_path_graph();
        g.set_systematic_cv(0.0);
        let indep = g.annotate_path(&path, RoutingMode::Walk);
        assert!(
            indep.var > 0.0,
            "the signalized edge contributes generator variance"
        );
        g.set_systematic_cv(0.1);
        let widened = g.annotate_path(&path, RoutingMode::Walk);
        assert!(
            widened.var > indep.var,
            "systematic_cv widens total variance"
        );
        assert!(
            widened.bracket().1 > indep.bracket().1,
            "p95 widens with systematic_cv"
        );
        let expected = indep.var + (0.1 * indep.mean).powi(2);
        assert!(
            (widened.var - expected).abs() < 1e-6,
            "systematic term is (cv*mean)^2"
        );
    }

    #[test]
    fn front_variance_axis_equals_bracket_independent_var() {
        let (mut g, path) = straight_path_graph();
        g.set_systematic_cv(0.0);
        let mode = RoutingMode::Walk;
        let m = g.annotate_path(&path, mode);
        let model = g.raptor.variance_model;
        let mut axis_sum = 0.0;
        let mut incoming: Option<(f64, f64)> = None;
        for w in path.windows(2) {
            let s = g.edges[w[0].0]
                .iter()
                .find_map(|e| match e {
                    EdgeData::Street(s) if s.destination == w[1] => Some(s),
                    _ => None,
                })
                .unwrap();
            let this_dir = g.dir_between(w[0], w[1]);
            axis_sum += edge_variance(mode, s, &model, incoming, this_dir);
            incoming = Some(this_dir);
        }
        assert!(
            (axis_sum - m.var).abs() < 1e-6,
            "front Variance axis must equal the bracket's independent-variance term"
        );
    }

    #[test]
    fn systematic_cv_zero_is_pure_independent_sum() {
        let (mut g, path) = straight_path_graph();
        g.set_systematic_cv(0.0);
        let m = g.annotate_path(&path, RoutingMode::Walk);
        let model = g.raptor.variance_model;
        let e_ab = model.variance(VarGen::SIGNALIZED, HighwayClass::Residential);
        let e_bc = model.variance(VarGen::NONE, HighwayClass::Residential);
        assert!((m.var - (e_ab + e_bc)).abs() < 1e-6);
    }

    #[test]
    fn bike_mean_is_kinematic_edge_time_plus_penalty() {
        let (g, path) = straight_path_graph();
        let bike = g.default_bike_cost();
        let model = g.raptor.variance_model;
        let kinematic: u32 = path
            .windows(2)
            .map(|w| {
                let s = g.edges[w[0].0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == w[1] => Some(s),
                        _ => None,
                    })
                    .unwrap();
                bike.edge_time(s)
            })
            .sum();
        let m = g.annotate_path(&path, RoutingMode::Bike);
        assert_eq!(
            m.mean,
            kinematic as f64 + model.signal_delay_minor,
            "bike p50 sums BikeCost::edge_time plus the signalized edge's expected delay (path is straight, no corner cost)"
        );
    }

    /// Two short edges meeting at a sharp corner, so the displayed bike p50 includes
    /// the physical corner slow-down — matching the Time axis the route is chosen on.
    fn corner_path_graph() -> (Graph, Vec<NodeID>) {
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
        // a→b heads east, b→c heads north: a ~90° corner at b over short (~8 m) edges
        // — tight enough to force a slow-down at the commuter cruise speed.
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0003));
        let c = g.add_node(mk("c", 50.0002, 4.0003));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Tertiary;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: len,
                foot: true,
                bike: true,
                car: true,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 8));
        g.add_edge(b, edge(b, c, 8));
        (g, vec![a, b, c])
    }

    #[test]
    fn bike_p50_includes_corner_slowdown() {
        let (g, path) = corner_path_graph();
        let bike = g.default_bike_cost();
        // Raw kinematic sum (no speed-change) — what p50 would be without corners.
        let kinematic: u32 = path
            .windows(2)
            .map(|w| {
                let s = g.edges[w[0].0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == w[1] => Some(s),
                        _ => None,
                    })
                    .unwrap();
                bike.edge_time(s)
            })
            .sum();
        let m = g.annotate_path(&path, RoutingMode::Bike);
        assert!(
            m.mean > kinematic as f64,
            "a tight corner adds slow-down time to the displayed p50: {} > {}",
            m.mean,
            kinematic
        );
        // The first edge alone (no preceding edge ⇒ no corner) costs only its kinematic
        // time, confirming the extra in the full path is the per-vertex corner term.
        let first_only = g
            .annotate_path(&[path[0], path[1]], RoutingMode::Bike)
            .mean;
        assert_eq!(
            first_only,
            bike.edge_time(
                g.edges[path[0].0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == path[1] => Some(s),
                        _ => None,
                    })
                    .unwrap()
            ) as f64,
            "a single leading edge carries no corner cost"
        );
    }

    /// A ride → push → ride path: the displayed p50 must include the dismount stop and
    /// the remount restart, and the variance must carry the once-per-run push σ², so
    /// the shown bracket matches the axis the route was chosen on.
    fn dismount_path_graph() -> (Graph, Vec<NodeID>) {
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
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0010));
        let c = g.add_node(mk("c", 50.0000, 4.0020));
        let d = g.add_node(mk("d", 50.0000, 4.0030));
        g.build_raptor_index();
        let edge = |o: NodeID, dn: NodeID, len: usize, push: bool| {
            let mut at = BikeAttrs::road_default();
            at.highway = if push {
                HighwayClass::Footway
            } else {
                HighwayClass::Tertiary
            };
            at.surface = Surface::Paved;
            if push {
                at.bikeaccess = false;
                at.footaccess = true;
            }
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: dn,
                partial: false,
                length: len,
                foot: true,
                bike: !push,
                car: false,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 70, false));
        g.add_edge(b, edge(b, c, 70, true));
        g.add_edge(c, edge(c, d, 70, false));
        (g, vec![a, b, c, d])
    }

    #[test]
    fn bike_p50_and_variance_include_dismount_stop_and_push_sigma() {
        let (g, path) = dismount_path_graph();
        let bike = g.default_bike_cost();
        let model = g.raptor.variance_model;
        let kinematic: u32 = path
            .windows(2)
            .map(|w| {
                let s = g.edges[w[0].0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == w[1] => Some(s),
                        _ => None,
                    })
                    .unwrap();
                bike.edge_time(s)
            })
            .sum();
        let m = g.annotate_path(&path, RoutingMode::Bike);
        assert!(
            m.mean > kinematic as f64,
            "a dismount adds a stop+restart to the displayed p50: {} > {}",
            m.mean,
            kinematic
        );
        // The dismount run contributes its once-per-run uncertainty σ² to the bracket.
        let mut g0 = g;
        g0.set_systematic_cv(0.0);
        let m0 = g0.annotate_path(&path, RoutingMode::Bike);
        assert!(
            m0.var >= model.push_sigma * model.push_sigma - 1e-6,
            "dismount uncertainty (push_sigma²) is present in the bracket variance: {}",
            m0.var
        );
    }

    #[test]
    #[ignore]
    fn walk_front_fastest_diag_real_brussels() {
        use crate::structures::cost::{Axis, LegRole};
        let path = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let mut g = Graph::new();
        crate::ingestion::osm::load_pbf_file(path, None, 4.0, &Default::default(), &mut g).unwrap();
        g.build_raptor_index();
        let (_, &o) = g.nearest_node_dist(50.847, 4.423).expect("origin snaps");
        let (_, &d) = g.nearest_node_dist(50.835, 4.410).expect("dest snaps");
        let bike = g.default_bike_cost();

        let res = g.multiobj_search(
            o,
            d,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &g.raptor.cost_weights,
            &g.raptor.epsilon,
            g.raptor.distance_budget,
            true,
        );
        let front_min_time = res
            .front
            .iter()
            .map(|p| p.cost.get(Axis::Time))
            .fold(f64::INFINITY, f64::min);
        let fastest_front = res
            .front
            .iter()
            .min_by(|a, b| {
                a.cost
                    .get(Axis::Time)
                    .partial_cmp(&b.cost.get(Axis::Time))
                    .unwrap()
            })
            .unwrap();
        let fastest_p50 = g
            .annotate_path(&fastest_front.nodes, RoutingMode::Walk)
            .mean;

        // Selection must preserve the fastest path: trimming the SAME front to k
        // representatives must keep its Time-minimum (the user's default fast route).
        let idx = crate::structures::graph::representatives::select_representatives(
            &res.front,
            g.raptor.representatives_k,
            RoutingMode::Walk.axes(),
        );
        let reps_min_time = idx
            .iter()
            .map(|&i| res.front[i].cost.get(Axis::Time))
            .fold(f64::INFINITY, f64::min);

        eprintln!(
            "WALKDIAG front_len={} front_min_time={front_min_time:.0} fastest_p50={fastest_p50:.0} reps_k={} reps_min_time={reps_min_time:.0}",
            res.front.len(),
            idx.len(),
        );
        assert_eq!(
            reps_min_time, front_min_time,
            "representative selection must keep the front's Time-minimum (fastest route)"
        );
    }

    #[test]
    #[ignore]
    fn annotate_path_smoke_real_brussels() {
        use crate::structures::cost::LegRole;
        let path = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let mut g = Graph::new();
        crate::ingestion::osm::load_pbf_file(path, None, 4.0, &Default::default(), &mut g).unwrap();
        g.build_raptor_index();
        let (_, &o) = g.nearest_node_dist(50.846, 4.352).expect("origin snaps");
        let (_, &d) = g.nearest_node_dist(50.851, 4.358).expect("dest snaps");
        let bike = g.default_bike_cost();

        let reps = g.multiobj_representatives(o, d, RoutingMode::Walk, LegRole::Neutral, &bike);
        assert!(!reps.is_empty(), "a walk route exists");
        let fastest = reps
            .iter()
            .min_by(|a, b| {
                a.cost
                    .get(crate::structures::cost::Axis::Time)
                    .partial_cmp(&b.cost.get(crate::structures::cost::Axis::Time))
                    .unwrap()
            })
            .unwrap();

        let moments = g.annotate_path(&fastest.nodes, RoutingMode::Walk);
        let (p50, p95) = moments.bracket();
        eprintln!(
            "ANNOTATE walk p50={p50:.0}s p95={p95:.0}s var={:.0}",
            moments.var
        );
        assert!(p50 > 0.0, "non-trivial median time");
        assert!(p95 >= p50, "p95 is at least p50");
        assert!(p95 > p50, "a real multi-edge leg carries non-zero spread");
    }
}
