//! Post-pass replacing a plan's access/egress/direct walk legs with multi-objective
//! versions. Runs once over the FINAL plans, deduped per (from,to,mode,role).

use std::collections::HashMap;

use super::Graph;
use crate::structures::cost::{BalanceWeights, LegRole, RoutingMode};
use crate::structures::plan::{
    LegOption, Plan, PlanLeg, PlanPlace, PlanWalkLeg, highlight_index,
    initial_cursor,
};
use crate::structures::{BikeCost, Mode, NodeID};

impl Graph {
    pub fn enrich_street_legs(
        &self,
        plans: &mut [Plan],
        origin: NodeID,
        destination: NodeID,
        bike: &BikeCost,
        terminal_deadline: bool,
    ) {
        let mut memo: HashMap<(NodeID, NodeID, RoutingMode, LegRole), Vec<LegOption>> =
            HashMap::new();
        for plan in plans.iter_mut() {
            self.enrich_one(
                plan,
                origin,
                destination,
                bike,
                terminal_deadline,
                &mut memo,
            );
        }
    }

    fn enrich_one(
        &self,
        plan: &mut Plan,
        origin: NodeID,
        destination: NodeID,
        bike: &BikeCost,
        terminal_deadline: bool,
        memo: &mut HashMap<(NodeID, NodeID, RoutingMode, LegRole), Vec<LegOption>>,
    ) {
        let n = plan.legs.len();
        let has_transit = plan.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_)));
        if !has_transit {
            for i in 0..n {
                if let PlanLeg::Walk(w) = &plan.legs[i] {
                    let mode = mode_of(w.street_mode);
                    let role = if terminal_deadline {
                        LegRole::Deadline
                    } else {
                        LegRole::Neutral
                    };
                    let opts =
                        options(self, w.from.node_id, w.to.node_id, mode, role, bike, memo);
                    if let Some(new) = self.rebuild_leg(w, &opts, mode, bike, None) {
                        plan.legs[i] = PlanLeg::Walk(new);
                    }
                }
            }
            return;
        }
        if let Some(PlanLeg::Walk(w)) = plan.legs.first() {
            if w.from.node_id == origin {
                let board = leg_start(&plan.legs[1]);
                let mode = mode_of(w.street_mode);
                let opts = options(
                    self,
                    origin,
                    w.to.node_id,
                    mode,
                    LegRole::Deadline,
                    bike,
                    memo,
                );
                if let Some(new) =
                    self.rebuild_leg(w, &opts, mode, bike, Some((board, plan.start)))
                {
                    plan.legs[0] = PlanLeg::Walk(new);
                }
            }
        }
        if n >= 2 {
            if let Some(PlanLeg::Walk(w)) = plan.legs.last() {
                if w.to.node_id == destination {
                    let alight = w.start;
                    let old_end = w.end;
                    let mode = mode_of(w.street_mode);
                    let opts = options(
                        self,
                        w.from.node_id,
                        destination,
                        mode,
                        LegRole::Neutral,
                        bike,
                        memo,
                    );
                    if !opts.is_empty() {
                        let cur = highlight_index(&opts, None, &self.raptor.balance);
                        let chosen = &opts[cur];
                        let end = alight + chosen.p50;
                        let to = PlanPlace {
                            node_id: destination,
                            stop_position: None,
                            arrival: Some(end),
                            departure: None,
                        };
                        let steps =
                            self.street_steps(&chosen.nodes, &chosen.edges, mode, bike, alight, to);
                        let mut new = w.clone();
                        new.to = to;
                        new.end = end;
                        new.duration = chosen.p50;
                        new.length = chosen.length;
                        new.cycleroute_length = chosen.cycleroute_length;
                        new.elevation_gain = chosen.elevation_gain;
                        new.geometry = chosen.geometry.clone();
                        new.steps = steps;
                        new.alternatives = opts;
                        new.leave_by = None;
                        *plan.legs.last_mut().unwrap() = PlanLeg::Walk(new);
                        // Shift the arrival timeline by the egress delta so the transit
                        // delay-CDF spread in `arrival_distribution` is preserved.
                        let delta = end as i64 - old_end as i64;
                        plan.end = end;
                        plan.expected_end =
                            (plan.expected_end as i64 + delta).max(end as i64) as u32;
                        for sc in &mut plan.arrival_distribution {
                            sc.time = (sc.time as i64 + delta).max(0) as u32;
                        }
                    }
                }
            }
        }
    }

    /// `deadline = Some((board, earliest))` anchors the leg's END to `board` and
    /// sets leave_by (access); `None` anchors the START (direct/egress).
    fn rebuild_leg(
        &self,
        old: &PlanWalkLeg,
        opts: &[LegOption],
        mode: RoutingMode,
        bike: &BikeCost,
        deadline: Option<(u32, u32)>,
    ) -> Option<PlanWalkLeg> {
        if opts.is_empty() {
            return None;
        }
        let mut leg = old.clone();
        let (start, end, leave_by, cur) = match deadline {
            Some((board, earliest)) => {
                let (s, lb, c) = access_timing(opts, board, earliest, &self.raptor.balance);
                (s, board, Some(lb), c)
            }
            None => {
                let c = initial_cursor(opts, &self.raptor.balance);
                (old.start, old.start + opts[c].p50, None, c)
            }
        };
        let chosen = &opts[cur];
        let to = PlanPlace {
            node_id: leg.to.node_id,
            stop_position: None,
            arrival: Some(end),
            departure: None,
        };
        leg.steps = self.street_steps(&chosen.nodes, &chosen.edges, mode, bike, start, to);
        leg.from = PlanPlace {
            node_id: leg.from.node_id,
            stop_position: None,
            arrival: None,
            departure: Some(start),
        };
        leg.to = to;
        leg.start = start;
        leg.end = end;
        leg.duration = chosen.p50;
        leg.length = chosen.length;
        leg.cycleroute_length = chosen.cycleroute_length;
        leg.elevation_gain = chosen.elevation_gain;
        leg.geometry = chosen.geometry.clone();
        leg.alternatives = opts.to_vec();
        leg.leave_by = leave_by;
        Some(leg)
    }

}

fn mode_of(m: Mode) -> RoutingMode {
    match m {
        Mode::Bike => RoutingMode::Bike,
        Mode::Car => RoutingMode::Drive,
        _ => RoutingMode::Walk,
    }
}

fn leg_start(l: &PlanLeg) -> u32 {
    match l {
        PlanLeg::Walk(w) => w.start,
        PlanLeg::Transit(t) => t.start,
    }
}

pub(super) fn access_timing(
    options: &[LegOption],
    board: u32,
    earliest: u32,
    balance: &BalanceWeights,
) -> (u32, u32, usize) {
    let window = board.saturating_sub(earliest);
    let cur = highlight_index(options, Some(window), balance);
    // `.min(board)`: an access leg can never start after it ends (boarding time).
    let leg_start = board.saturating_sub(options[cur].p50).max(earliest).min(board);
    let leave_by = board.saturating_sub(options[cur].p95);
    (leg_start, leave_by, cur)
}

fn options(
    g: &Graph,
    from: NodeID,
    to: NodeID,
    mode: RoutingMode,
    role: LegRole,
    bike: &BikeCost,
    memo: &mut HashMap<(NodeID, NodeID, RoutingMode, LegRole), Vec<LegOption>>,
) -> Vec<LegOption> {
    memo.entry((from, to, mode, role))
        .or_insert_with(|| g.multiobj_leg_options(from, to, mode, role, bike))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::VarGen;
    use crate::structures::plan::{
        ArrivalScenario, Plan, PlanLeg, PlanPlace, PlanTransitLeg, PlanWalkLeg,
    };
    use crate::structures::{
        BikeAttrs, EdgeData, HighwayClass, LatLng, Mode, NodeData, NodeID, OsmNodeData,
        StreetEdgeData, Surface,
    };

    fn enable_contraction(g: &mut Graph) {
        use crate::structures::contraction::ContractedGraph;
        let mut cg = ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted_default();
    }

    fn enrich_graph() -> (Graph, NodeID, NodeID) {
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
        let o = g.add_node(mk("o", 50.0, 4.00000));
        let oc = g.add_node(mk("oc", 50.00003, 4.00003));
        let s = g.add_node(mk("s", 50.0, 4.00010));
        g.build_raptor_index();
        g.set_distance_budget(f64::INFINITY);
        let e = |a, b, len, su| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = su;
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
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
        g.add_edge(o, e(o, s, 100, Surface::Unpaved));
        g.add_edge(o, e(o, oc, 60, Surface::Paved));
        g.add_edge(oc, e(oc, s, 60, Surface::Paved));
        // Back-edge makes s a proper junction endpoint so contraction reaches it.
        g.add_edge(s, e(s, o, 100, Surface::Paved));
        (g, o, s)
    }

    fn walk_leg(from: NodeID, to: NodeID, start: u32, end: u32) -> PlanWalkLeg {
        let place = |n, dep, arr| PlanPlace {
            node_id: n,
            stop_position: None,
            arrival: arr,
            departure: dep,
        };
        PlanWalkLeg {
            from: place(from, Some(start), None),
            to: place(to, None, Some(end)),
            start,
            end,
            duration: end - start,
            length: 0,
            cycleroute_length: None,
            elevation_gain: None,
            street_mode: Mode::Walk,
            steps: vec![],
            geometry: vec![],
            alternatives: vec![],
            leave_by: None,
        }
    }

    fn bike_enrich_graph() -> (Graph, NodeID, NodeID) {
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
        let o = g.add_node(mk("o", 50.0, 4.00000));
        let oc = g.add_node(mk("oc", 50.00010, 4.00010));
        let s = g.add_node(mk("s", 50.0, 4.00020));
        g.build_raptor_index();
        g.set_distance_budget(f64::INFINITY);
        let e = |a, b, len, elev: i16| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
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
        g.add_edge(o, e(o, s, 100, 8));
        g.add_edge(o, e(o, oc, 400, 0));
        g.add_edge(oc, e(oc, s, 400, 0));
        g.raptor.set_bike_select_dplus(true);
        (g, o, s)
    }

    fn bike_leg(from: NodeID, to: NodeID, start: u32, end: u32) -> PlanWalkLeg {
        let mut leg = walk_leg(from, to, start, end);
        leg.street_mode = Mode::Bike;
        leg
    }

    #[test]
    fn enrich_sets_access_alternatives_and_leave_by() {
        let (mut g, o, s) = enrich_graph();
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let access = walk_leg(o, s, 500, 600);
        let transit = PlanTransitLeg {
            length: 0,
            start: 600,
            end: 900,
            duration: 300,
            scheduled_start: 600,
            scheduled_end: 900,
            realtime: false,
            from: PlanPlace {
                node_id: s,
                stop_position: None,
                arrival: None,
                departure: Some(600),
            },
            to: PlanPlace {
                node_id: s,
                stop_position: None,
                arrival: Some(900),
                departure: None,
            },
            steps: vec![],
            geometry: vec![],
            transfer_risk: None,
            trip_id: crate::ingestion::gtfs::TripId(0),
            preceding_arrival: None,
            preceding_route_type: None,
            route_type: None,
            following_route_type: None,
            following_margin_secs: None,
            bikes_allowed: None,
            time_shift: 0,
        };
        let plan = Plan {
            legs: vec![PlanLeg::Walk(access), PlanLeg::Transit(transit)],
            start: 500,
            end: 900,
            mode: Mode::WalkTransit,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: 900,
                probability: 1.0,
            }],
            expected_end: 900,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            o,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(acc) = &plans[0].legs[0] else {
            panic!()
        };
        assert!(
            acc.alternatives.len() >= 2,
            "access leg gets multiobj alternatives"
        );
        assert!(acc.leave_by.is_some(), "ingress carries leave_by");
        assert_eq!(
            acc.end, 600,
            "access leg still ends at the fixed boarding time"
        );
    }

    fn transit_leg(from: NodeID, to: NodeID, start: u32, end: u32) -> PlanTransitLeg {
        PlanTransitLeg {
            length: 0,
            start,
            end,
            duration: end - start,
            scheduled_start: start,
            scheduled_end: end,
            realtime: false,
            from: PlanPlace {
                node_id: from,
                stop_position: None,
                arrival: None,
                departure: Some(start),
            },
            to: PlanPlace {
                node_id: to,
                stop_position: None,
                arrival: Some(end),
                departure: None,
            },
            steps: vec![],
            geometry: vec![],
            transfer_risk: None,
            trip_id: crate::ingestion::gtfs::TripId(0),
            preceding_arrival: None,
            preceding_route_type: None,
            route_type: None,
            following_route_type: None,
            following_margin_secs: None,
            bikes_allowed: None,
            time_shift: 0,
        }
    }

    #[test]
    fn enrich_egress_recomputes_arrival_from_highlighted_p50() {
        let (mut g, o, s) = enrich_graph();
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let alight = 2000u32;
        let transit = transit_leg(s, o, 1000, alight);
        let egress = walk_leg(o, s, alight, alight + 90);
        let plan = Plan {
            legs: vec![PlanLeg::Transit(transit), PlanLeg::Walk(egress)],
            start: 1000,
            end: alight + 90,
            mode: Mode::WalkTransit,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: alight + 90,
                probability: 1.0,
            }],
            expected_end: alight + 90,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            s,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(eg) = plans[0].legs.last().unwrap() else {
            panic!()
        };
        assert!(
            eg.alternatives.len() >= 2,
            "egress leg gets multiobj alternatives"
        );
        assert!(eg.leave_by.is_none(), "egress carries no leave_by");
        assert_eq!(eg.start, alight, "egress starts at the fixed alight time");
        let end = alight + eg.duration;
        assert_eq!(eg.end, end, "egress end = alight + p50");
        assert_eq!(
            plans[0].end, end,
            "plan arrival recomputed from the highlighted egress"
        );
        assert!(
            plans[0].expected_end >= end,
            "expected_end never precedes the arrival"
        );
        assert_eq!(
            plans[0].arrival_distribution.len(),
            1,
            "single deterministic arrival"
        );
        assert_eq!(
            plans[0].arrival_distribution[0].time, end,
            "arrival distribution at the recomputed end"
        );
        assert_eq!(plans[0].arrival_distribution[0].probability, 1.0);
    }

    #[test]
    fn enrich_direct_plan_gets_alternatives_anchored_at_start() {
        let (mut g, o, s) = enrich_graph();
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let leg = walk_leg(o, s, 300, 400);
        let plan = Plan {
            legs: vec![PlanLeg::Walk(leg)],
            start: 300,
            end: 400,
            mode: Mode::Walk,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: 400,
                probability: 1.0,
            }],
            expected_end: 400,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            o,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(w) = &plans[0].legs[0] else {
            panic!()
        };
        assert!(
            w.alternatives.len() >= 2,
            "direct walk leg gets alternatives"
        );
        assert!(w.leave_by.is_none(), "direct leg carries no leave_by");
        assert_eq!(w.start, 300, "direct leg keeps its start anchor");
        assert_eq!(w.end, 300 + w.duration, "direct leg end = start + p50");
    }

    #[test]
    fn enrich_direct_bike_plan_gets_alternatives_anchored_at_start() {
        let (g, o, s) = bike_enrich_graph();
        let bike = g.default_bike_cost();
        let leg = bike_leg(o, s, 300, 400);
        let plan = Plan {
            legs: vec![PlanLeg::Walk(leg)],
            start: 300,
            end: 400,
            mode: Mode::Bike,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: 400,
                probability: 1.0,
            }],
            expected_end: 400,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            o,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(w) = &plans[0].legs[0] else {
            panic!()
        };
        assert!(
            w.alternatives.len() >= 2,
            "direct bike leg gets alternatives"
        );
        assert_eq!(w.street_mode, Mode::Bike, "stays a bike leg");
        assert!(w.leave_by.is_none(), "direct leg carries no leave_by");
        assert_eq!(w.start, 300, "direct leg keeps its start anchor");
        assert_eq!(w.end, 300 + w.duration, "direct leg end = start + p50");
    }

    #[test]
    fn enrich_sets_bike_access_alternatives_and_leave_by() {
        let (g, o, s) = bike_enrich_graph();
        let bike = g.default_bike_cost();
        let access = bike_leg(o, s, 500, 600);
        let transit = transit_leg(s, s, 600, 900);
        let plan = Plan {
            legs: vec![PlanLeg::Walk(access), PlanLeg::Transit(transit)],
            start: 500,
            end: 900,
            mode: Mode::BikeToTransit,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: 900,
                probability: 1.0,
            }],
            expected_end: 900,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            o,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(acc) = &plans[0].legs[0] else {
            panic!()
        };
        assert!(
            acc.alternatives.len() >= 2,
            "bike access leg gets multiobj alternatives"
        );
        assert_eq!(acc.street_mode, Mode::Bike, "stays a bike leg");
        assert!(acc.leave_by.is_some(), "bike ingress carries leave_by");
        assert_eq!(
            acc.end, 600,
            "bike access leg still ends at the fixed boarding time"
        );
    }

    #[test]
    fn enrich_bike_egress_recomputes_arrival_from_highlighted_p50() {
        let (g, o, s) = bike_enrich_graph();
        let bike = g.default_bike_cost();
        let alight = 2000u32;
        let transit = transit_leg(s, o, 1000, alight);
        let egress = bike_leg(o, s, alight, alight + 90);
        let plan = Plan {
            legs: vec![PlanLeg::Transit(transit), PlanLeg::Walk(egress)],
            start: 1000,
            end: alight + 90,
            mode: Mode::BikeToTransit,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: alight + 90,
                probability: 1.0,
            }],
            expected_end: alight + 90,
            price: None,
        };
        let mut plans = vec![plan];
        g.enrich_street_legs(
            &mut plans,
            s,
            s,
            &bike,
            false,
        );
        let PlanLeg::Walk(eg) = plans[0].legs.last().unwrap() else {
            panic!()
        };
        assert!(
            eg.alternatives.len() >= 2,
            "bike egress leg gets multiobj alternatives"
        );
        assert_eq!(eg.street_mode, Mode::Bike, "stays a bike leg");
        assert!(eg.leave_by.is_none(), "egress carries no leave_by");
        assert_eq!(eg.start, alight, "egress starts at the fixed alight time");
        let end = alight + eg.duration;
        assert_eq!(eg.end, end, "egress end = alight + p50");
        assert_eq!(
            plans[0].end, end,
            "plan arrival recomputed from the highlighted egress"
        );
    }
}
