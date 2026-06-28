//! Build production direct-street `Plan`s from the multi-objective engine: each
//! representative becomes a `LegOption` (geometry from its node path, `[p50,p95]`
//! from `annotate_path`, axes from its `CostVector`, bike ride/push steps rebuilt
//! from the per-edge `is_push` attribute), and the config-`balance` cursor picks the
//! highlighted leg. Weights touch only the cursor; the option set is untouched.

use super::Graph;
use crate::structures::cost::{Axis, LegRole, RoutingMode};
use crate::structures::graph::bike_cost::BikeCost;
use crate::structures::plan::{
    ArrivalScenario, LegOption, Plan, PlanLeg, PlanLegStep, PlanPlace, PlanWalkLeg,
    PlanWalkLegStep, initial_cursor,
};
use crate::structures::{Mode, NodeID, StreetEdgeData};

impl Graph {
    /// Time-agnostic representative options for a street leg `from → to` under
    /// `mode`/`role`. Empty when no route exists. Shared by direct plans and
    /// transit access/egress; callers attach timing/placement.
    pub(crate) fn multiobj_leg_options(
        &self,
        from: NodeID,
        to: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
    ) -> Vec<LegOption> {
        let opts = self
            .multiobj_representatives_budgeted(
                from,
                to,
                mode,
                role,
                bike,
                self.raptor.distance_budget,
                true,
            )
            .iter()
            .map(|p| self.leg_option(&p.nodes, &p.edges, p.cost, mode, bike, 0))
            .collect::<Vec<_>>();
        let opts = Graph::dedup_leg_options(opts);
        self.select_diverse(opts, self.raptor.alt_max_share_factor)
    }

    /// Drop options weakly dominated on the DISPLAYED objectives, each bucketed to
    /// the granularity the user actually sees: time → whole minutes, surface →
    /// 0.01× roughness, climb → whole metres, reliability → whole minutes. Keeps an
    /// earlier index on exact read-alikes. An option that only "wins" below display
    /// granularity (e.g. 20 s faster, both "10 min") is pruned; genuine visible
    /// Time/Surface/Climb/Reliability trade-offs survive.
    pub(crate) fn dedup_leg_options(opts: Vec<LegOption>) -> Vec<LegOption> {
        // Displayed differentiators, each bucketed to the granularity the user sees:
        // time → minutes, cycleway exposure → 100 m, surface → 0.01× roughness,
        // climb → metres, reliability → minutes. CyclewayDeficit is a *core* bike axis
        // (Time › Cycleway › D+): without it the most-cycleway route — slower, but on
        // bike infra so equal/better on the other displayed axes — is wrongly dropped
        // as "dominated", collapsing the very trade-off the user routes on.
        let key = |o: &LegOption| {
            (
                (o.p50 as f64 / 60.0).round(),
                (o.cycleway_deficit / 100.0).round(),
                (o.surface / (o.length.max(1) as f64) * 100.0).round(),
                o.dplus.round(),
                (o.p95.saturating_sub(o.p50) as f64 / 60.0).round(),
            )
        };
        let keys: Vec<(f64, f64, f64, f64, f64)> = opts.iter().map(key).collect();
        opts.iter()
            .enumerate()
            .filter(|(i, _)| {
                !keys.iter().enumerate().any(|(j, kj)| {
                    if j == *i {
                        return false;
                    }
                    let ki = keys[*i];
                    let le = kj.0 <= ki.0
                        && kj.1 <= ki.1
                        && kj.2 <= ki.2
                        && kj.3 <= ki.3
                        && kj.4 <= ki.4;
                    if !le {
                        return false;
                    }
                    let lt = kj.0 < ki.0
                        || kj.1 < ki.1
                        || kj.2 < ki.2
                        || kj.3 < ki.3
                        || kj.4 < ki.4;
                    lt || j < *i
                })
            })
            .map(|(_, o)| o.clone())
            .collect()
    }

    pub(crate) fn shared_fraction_edges(
        &self,
        p: &[StreetEdgeData],
        q: &[StreetEdgeData],
    ) -> f64 {
        use std::collections::HashSet;
        let qset: HashSet<(usize, usize)> =
            q.iter().map(|e| (e.origin.0, e.destination.0)).collect();
        let mut total = 0usize;
        let mut shared = 0usize;
        for s in p {
            total += s.length;
            if qset.contains(&(s.origin.0, s.destination.0)) {
                shared += s.length;
            }
        }
        if total == 0 { 0.0 } else { shared as f64 / total as f64 }
    }

    /// Greedily keep options (in their incoming rank order) that share at most
    /// `max_share` of their length with every already-kept, higher-ranked option —
    /// ADGW limited-sharing applied pairwise, replacing display-granularity dedup so
    /// two geometrically near-identical routes are never both surfaced.
    pub(crate) fn select_diverse(&self, opts: Vec<LegOption>, max_share: f64) -> Vec<LegOption> {
        let mut kept: Vec<LegOption> = Vec::new();
        for o in opts {
            let diverse = kept.iter().all(|k| {
                let share = if !o.edges.is_empty() && !k.edges.is_empty() {
                    self.shared_fraction_edges(&o.edges, &k.edges)
                } else {
                    0.0
                };
                share <= max_share
            });
            if diverse {
                kept.push(o);
            }
        }
        kept
    }

    /// Direct multi-objective street plan: representatives → `LegOption`s → a single
    /// `Walk` leg highlighting the `balance` cursor, with the others as alternatives.
    /// `None` when no route exists. `role == Deadline` runs the search with the
    /// variance-proxy axis active (calmer routes).
    pub fn multiobj_direct_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
        start_time: u32,
    ) -> Option<Plan> {
        let options = self.multiobj_leg_options(origin, destination, mode, role, bike);
        if options.is_empty() {
            return None;
        }
        let cur = initial_cursor(&options, &self.raptor.balance);

        let chosen = &options[cur];
        let secs = chosen.p50;
        let end = start_time + secs;
        let smode = match mode {
            RoutingMode::Walk => Mode::Walk,
            RoutingMode::Bike => Mode::Bike,
            RoutingMode::Drive => Mode::Car,
        };
        let from = PlanPlace {
            node_id: origin,
            stop_position: None,
            arrival: None,
            departure: Some(start_time),
        };
        let to = PlanPlace {
            node_id: destination,
            stop_position: None,
            arrival: Some(end),
            departure: None,
        };
        let steps = self.street_steps(&chosen.nodes, &chosen.edges, mode, bike, start_time, to);

        Some(Plan {
            legs: vec![PlanLeg::Walk(PlanWalkLeg {
                from,
                to,
                start: start_time,
                end,
                duration: secs,
                length: chosen.length,
                cycleroute_length: chosen.cycleroute_length,
                elevation_gain: chosen.elevation_gain,
                street_mode: smode,
                steps,
                geometry: chosen.geometry.clone(),
                alternatives: vec![],
                leave_by: None,
            })],
            start: start_time,
            end,
            mode: smode,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
        })
        .map(|mut plan| {
            if let PlanLeg::Walk(leg) = &mut plan.legs[0] {
                leg.alternatives = options;
            }
            plan
        })
    }

    /// Per-step street edges for a reconstructed path, from the carried arena edges
    /// (the g-free authority). Empty carried ⇒ empty. Returns owned edges so the
    /// carried far-coords can be dropped (geometry is built separately from them).
    pub(super) fn recon_edges(
        &self,
        _nodes: &[NodeID],
        carried: &[(StreetEdgeData, (f64, f64), crate::structures::LatLng)],
    ) -> Vec<StreetEdgeData> {
        carried.iter().map(|(e, _, _)| *e).collect()
    }

    /// Geometry of a reconstructed path from the carried far-coords (interior nodes
    /// have no `junction_coord`, so `node_coord` would panic post-drop). Empty
    /// carried ⇒ empty geometry.
    fn recon_geometry(
        &self,
        nodes: &[NodeID],
        carried: &[(StreetEdgeData, (f64, f64), crate::structures::LatLng)],
    ) -> Vec<crate::structures::plan::PlanCoordinate> {
        if carried.is_empty() {
            return Vec::new();
        }
        let mut geom = Vec::with_capacity(carried.len() + 1);
        if let Some(&first) = nodes.first() {
            let o = self.node_loc(first);
            geom.push(crate::structures::plan::PlanCoordinate {
                lat: o.latitude,
                lon: o.longitude,
            });
        }
        for (_, _, far) in carried {
            geom.push(crate::structures::plan::PlanCoordinate {
                lat: far.latitude,
                lon: far.longitude,
            });
        }
        geom
    }

    fn leg_option(
        &self,
        nodes: &[NodeID],
        carried: &[(StreetEdgeData, (f64, f64), crate::structures::LatLng)],
        cost: crate::structures::cost::CostVector,
        mode: RoutingMode,
        bike: &BikeCost,
        _start_time: u32,
    ) -> LegOption {
        let recon = self.recon_edges(nodes, carried);
        let (p50f, p95f) = self.annotate_path_edges(nodes, carried, mode).bracket();
        let length: usize = recon.iter().map(|s| s.length).sum();
        let unpaved_length: usize = recon
            .iter()
            .filter(|s| s.attrs.surface == crate::structures::Surface::Unpaved)
            .map(|s| s.length)
            .sum();
        let dplus = cost.get(Axis::Dplus);
        let cyc_deficit = cost.get(Axis::CyclewayDeficit);
        // Displayed ascent (metres). For walk the Dplus axis already IS the denoised
        // ascent. For bike the Dplus axis is a routing cost that blends raw ascent with
        // the downhill SAFETY penalty (`downhillcost`), so it is NOT metres of climb —
        // displaying it shows absurd figures (thousands of "metres" on a flat city
        // ride). Recompute the true ascent as denoised metres over the path, with the
        // same elevation-buffer hysteresis used for walk D+.
        let elevation_gain = if mode == RoutingMode::Bike {
            let mut ehbu = 0.0;
            let mut asc = 0.0;
            for s in &recon {
                let (charged, new_ehbu) =
                    bike.walk_ascent_step(ehbu, s.elev_delta as f64, s.length as f64);
                asc += charged;
                ehbu = new_ehbu;
            }
            asc += ehbu; // flush residual sustained climb (matches the walk D+ flush)
            Some(asc.round() as usize)
        } else {
            Some(dplus.round() as usize)
        };
        let cycleroute_length = if mode == RoutingMode::Bike {
            Some(length.saturating_sub(cyc_deficit.round() as usize))
        } else {
            None
        };
        let dismount_length: usize = if mode == RoutingMode::Bike {
            recon
                .iter()
                .filter(|s| BikeCost::is_push(&s.attrs))
                .map(|s| s.length)
                .sum()
        } else {
            0
        };
        let mut dismount_runs: Vec<crate::structures::plan::DismountRun> = Vec::new();
        if mode == RoutingMode::Bike {
            let mut run_start: Option<usize> = None;
            for (i, s) in recon.iter().enumerate() {
                if BikeCost::is_push(&s.attrs) {
                    run_start.get_or_insert(i);
                } else if let Some(st) = run_start.take() {
                    dismount_runs.push(crate::structures::plan::DismountRun { start: st, end: i });
                }
            }
            if let Some(st) = run_start.take() {
                dismount_runs.push(crate::structures::plan::DismountRun {
                    start: st,
                    end: nodes.len().saturating_sub(1),
                });
            }
        }
        let geometry = self.recon_geometry(nodes, carried);
        LegOption {
            time: cost.get(Axis::Time),
            dplus,
            surface: cost.get(Axis::Surface),
            variance: cost.get(Axis::Variance),
            cycleway_deficit: cyc_deficit,
            p50: p50f.round() as u32,
            p95: p95f.round() as u32,
            length,
            unpaved_length,
            dismount_length,
            dismount_runs,
            elevation_gain,
            cycleroute_length,
            geometry,
            nodes: nodes.to_vec(),
            edges: recon,
        }
    }

    /// Build leg steps for a chosen path. Walk/Drive ⇒ one plain step. Bike ⇒ group
    /// consecutive ride/push runs (push = `BikeCost::is_push`) into dismount-aware
    /// steps. `recon` carries the per-step arena edges (from `LegOption::edges`) and
    /// is the g-free authority; empty ⇒ a degenerate zero-length/zero-time step.
    pub(crate) fn street_steps(
        &self,
        nodes: &[NodeID],
        recon: &[StreetEdgeData],
        mode: RoutingMode,
        bike: &BikeCost,
        start_time: u32,
        to: PlanPlace,
    ) -> Vec<PlanLegStep> {
        let edges: &[StreetEdgeData] = recon;
        if mode != RoutingMode::Bike {
            let length: usize = edges.iter().map(|s| s.length).sum();
            let secs = self.annotate_steps_secs(recon, mode);
            return vec![PlanLegStep::Walk(PlanWalkLegStep::plain(length, secs, to))];
        }
        let mut steps: Vec<PlanLegStep> = Vec::new();
        let mut i = 0;
        let mut cum_time = 0u32;
        // Group consecutive ride/push runs. `geometry` is one coord per node, so
        // edge k connects geometry[k]→geometry[k+1]; a run over edges [start_idx, i)
        // spans geometry indices [start_idx, i] and ends at nodes[i].
        while i < edges.len() {
            let push = BikeCost::is_push(&edges[i].attrs);
            let start_idx = i;
            let (mut run_len, mut run_time) = (0usize, 0u32);
            while i < edges.len() && BikeCost::is_push(&edges[i].attrs) == push {
                run_len += edges[i].length;
                run_time += bike.edge_time(&edges[i]);
                i += 1;
            }
            cum_time += run_time;
            let node_id = nodes[i]; // edges[start_idx..i] end at nodes[i]
            steps.push(PlanLegStep::Walk(PlanWalkLegStep {
                length: run_len,
                time: run_time,
                place: PlanPlace {
                    node_id,
                    stop_position: None,
                    arrival: Some(start_time + cum_time),
                    departure: None,
                },
                dismount: push,
                geom_start: start_idx,
                geom_end: i,
            }));
        }
        if steps.is_empty() {
            let length: usize = edges.iter().map(|s| s.length).sum();
            steps.push(PlanLegStep::Walk(PlanWalkLegStep::plain(length, 0, to)));
        }
        steps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::VarGen;
    use crate::structures::cost::{LegRole, RoutingMode};
    use crate::structures::plan::PlanLeg;
    use crate::structures::{
        BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, NodeID, OsmNodeData, StreetEdgeData,
        Surface,
    };

    /// Build + bake the contracted graph so reconstruction carries the arena edges —
    /// production is contraction-only, so the leg/step builders read carried `.edges`.
    fn enable_contraction(g: &mut Graph) {
        use crate::structures::contraction::ContractedGraph;
        let mut cg = ContractedGraph::from_graph_union(g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted_default();
    }

    fn detour_graph() -> (Graph, NodeID, NodeID) {
        let mut g = Graph::new();
        g.set_distance_budget(f64::INFINITY);
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
        let edge = |o: NodeID, d: NodeID, len: usize, s: Surface| {
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
        g.add_edge(a, edge(a, b, 100, Surface::Unpaved));
        g.add_edge(a, edge(a, c, 90, Surface::Paved));
        g.add_edge(c, edge(c, b, 90, Surface::Paved));
        // b has no outgoing edges, so the contraction builder would skip it (k=0) and
        // no super-edge would reach b. A back-edge makes b a proper junction endpoint.
        g.add_edge(b, edge(b, a, 100, Surface::Paved));
        (g, a, b)
    }

    #[test]
    fn direct_plan_has_alternatives_with_brackets_and_balanced_leg() {
        let (mut g, a, b) = detour_graph();
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let plan = g
            .multiobj_direct_plan(a, b, RoutingMode::Walk, LegRole::Neutral, &bike, 30_000)
            .expect("a plan");
        let PlanLeg::Walk(leg) = &plan.legs[0] else {
            panic!("walk leg")
        };
        assert!(
            leg.alternatives.len() >= 2,
            "front surfaced as alternatives"
        );
        for o in &leg.alternatives {
            assert!(o.p95 >= o.p50, "bracket ordered");
            assert_eq!(*o.nodes.first().unwrap(), a);
            assert_eq!(*o.nodes.last().unwrap(), b);
        }
        // Leg fields mirror the highlighted (balanced) option.
        let cur = crate::structures::plan::initial_cursor(&leg.alternatives, &g.raptor.balance);
        assert_eq!(
            leg.length, leg.alternatives[cur].length,
            "leg mirrors highlighted option"
        );
        assert_eq!(plan.start, 30_000);
    }

    #[test]
    #[ignore]
    fn direct_plan_smoke_real_brussels() {
        use crate::structures::cost::{LegRole, RoutingMode};
        use std::time::Instant;
        let path = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let mut g = Graph::new();
        let t0 = Instant::now();
        crate::ingestion::osm::load_pbf_file(path, None, 4.0, &Default::default(), &mut g).unwrap();
        eprintln!(
            "SMOKE pbf_load={:.1?} nodes={}",
            t0.elapsed(),
            g.nodes.len()
        );
        g.build_raptor_index();
        eprintln!("SMOKE build_raptor={:.1?}", t0.elapsed());
        let (_, &o) = g.nearest_node_dist(50.846, 4.352).expect("origin snaps");
        let (_, &d) = g.nearest_node_dist(50.851, 4.358).expect("dest snaps");
        eprintln!("SMOKE origin={:?} dest={:?}", o, d);
        let bike = g.default_bike_cost();
        for mode in [RoutingMode::Walk, RoutingMode::Bike] {
            let ts = Instant::now();
            let plan = g
                .multiobj_direct_plan(o, d, mode, LegRole::Neutral, &bike, 28_800)
                .unwrap_or_else(|| panic!("direct plan must succeed for {mode:?}"));
            let PlanLeg::Walk(leg) = &plan.legs[0] else {
                panic!("expected walk leg")
            };
            assert!(
                !leg.alternatives.is_empty(),
                "{mode:?}: expected non-empty alternatives"
            );
            for alt in &leg.alternatives {
                assert!(
                    alt.p95 >= alt.p50,
                    "{mode:?}: p95={} must be >= p50={}",
                    alt.p95,
                    alt.p50
                );
                eprintln!(
                    "DIRECT mode={mode:?} options={} p50={} p95={} length={}",
                    leg.alternatives.len(),
                    alt.p50,
                    alt.p95,
                    alt.length
                );
            }
            if mode == RoutingMode::Bike {
                assert!(!leg.steps.is_empty(), "bike plan must have steps");
            }
            eprintln!("SMOKE mode={mode:?} elapsed={:.1?}", ts.elapsed());
        }
    }

    /// End-to-end feature validation on the real Belgium graph: run the ACTUAL
    /// leg-options pipeline (multiobj_representatives_budgeted → leg_option → dedup →
    /// select_diverse) the GraphQL `raptor` query feeds the UI from, with the shipped
    /// E1 config (bucketing + corridor + budget 0.15). Asserts the user sees several
    /// geographically distinct bike alternatives that span the cycleway axis, each
    /// with full polyline geometry — and that budget 0.15 does not collapse a short
    /// walk leg to a single option.
    #[test]
    #[ignore]
    fn leg_options_e2e_real_belgium() {
        use std::time::Instant;
        let dem = crate::ingestion::osm::Dem::load("data/belgium-DTM-20m.tif").ok();
        let mut g = Graph::new();
        let t0 = Instant::now();
        crate::ingestion::osm::load_pbf_file("data/belgium-latest.osm.pbf", dem.as_ref(), 4.0, &Default::default(), &mut g)
            .unwrap();
        g.build_raptor_index();
        // Shipped E1 production config.
        g.set_bike_bucket_cyc_k(0.11);
        g.set_bike_bucket_dpl_k(0.013);
        g.set_distance_budget(0.15);
        eprintln!("E2E load+index={:.1?}", t0.elapsed());
        let bike = g.default_bike_cost();

        // --- Bike: ~10 km across Brussels ---
        let (_, &o) = g.nearest_node_dist(50.796, 4.298).expect("o");
        let (_, &d) = g.nearest_node_dist(50.878, 4.402).expect("d");
        // Stage diagnostics: representatives (post select_representatives) → final.
        let reps = g.multiobj_representatives_budgeted(
            o, d, RoutingMode::Bike, LegRole::Neutral, &bike, g.raptor.distance_budget, true,
        );
        let mut rep_cyc: Vec<i64> = reps
            .iter()
            .map(|p| p.cost.get(crate::structures::cost::Axis::CyclewayDeficit).round() as i64)
            .collect();
        rep_cyc.sort();
        eprintln!("E2E representatives={} cyc={:?}", reps.len(), rep_cyc);
        let tb = Instant::now();
        let opts = g.multiobj_leg_options(o, d, RoutingMode::Bike, LegRole::Neutral, &bike);
        eprintln!("E2E bike leg_options={} elapsed={:.2?}", opts.len(), tb.elapsed());
        for (i, op) in opts.iter().enumerate() {
            let elev = op.elevation_gain.unwrap_or(0);
            eprintln!(
                "E2E   bike#{i} p50={} cyc_def={:.0} dplus={:.0} elev_gain={} ({:.1} m/km) len={} nodes={}",
                op.p50, op.cycleway_deficit, op.dplus, elev,
                elev as f64 / (op.length.max(1) as f64) * 1000.0, op.length, op.nodes.len()
            );
        }
        assert!(opts.len() >= 3, "bike leg must surface ≥3 alternatives, got {}", opts.len());
        // Displayed elevation gain must be real metres of ascent — never the Dplus
        // routing-cost blend (which folds in the ×100 downhill penalty). Brussels is
        // flat; bound D+ per km well under any plausible cost-blend figure.
        for op in &opts {
            let per_km = op.elevation_gain.unwrap_or(0) as f64 / (op.length.max(1) as f64) * 1000.0;
            assert!(
                per_km < 30.0,
                "displayed D+ must be ascent metres, not a cost blend: {:.0} m/km",
                per_km
            );
        }
        // Geometry survives (not a straight junction-to-junction jump).
        assert!(
            opts.iter().all(|o| o.geometry.len() >= 10 && o.nodes.len() >= 10),
            "each alternative must carry full polyline geometry"
        );
        // The cycleway span the user routes on is preserved through the pipeline.
        let mut cyc: Vec<i64> = opts.iter().map(|o| o.cycleway_deficit.round() as i64).collect();
        cyc.sort();
        cyc.dedup();
        assert!(cyc.len() >= 3, "alternatives must span the cycleway axis, got {cyc:?}");
        assert!(
            *cyc.first().unwrap() < 3000,
            "the most-cycleway alternative must survive (min cyc_def={})",
            cyc.first().unwrap()
        );

        // --- Walk: ~1.5 km (budget-0.15 degradation check) ---
        let (_, &wo) = g.nearest_node_dist(50.846, 4.352).expect("wo");
        let (_, &wd) = g.nearest_node_dist(50.852, 4.368).expect("wd");
        let tw = Instant::now();
        let wopts = g.multiobj_leg_options(wo, wd, RoutingMode::Walk, LegRole::Neutral, &bike);
        eprintln!("E2E walk leg_options={} elapsed={:.2?}", wopts.len(), tw.elapsed());
        assert!(
            !wopts.is_empty(),
            "budget 0.15 must still return a walk route (degradation check)"
        );
    }

    #[test]
    fn bike_elevation_gain_is_ascent_not_descent_penalty() {
        // A pure descent: the bike Dplus axis is now the denoised per-edge ascent
        // only (max(0, Δ) = 0 here) — the old in-search descent-SAFETY penalty was
        // dropped as label-setting-unsound. The DISPLAYED elevation gain must be ~0:
        // you do not climb while going downhill. Regression for the UI showing
        // thousands of "metres" of D+ on a flat/descending city ride.
        //
        // (Was: the bike Dplus *cost* axis blew up because it folded in the downhill
        // safety penalty, downhillcost=100. That term is gone now.)
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0100));
        g.build_raptor_index();
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Residential;
        at.surface = Surface::Paved;
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a, destination: b, partial: false, length: 1000,
                foot: true, bike: true, car: false, attrs: at, elev_delta: -100,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Bike, LegRole::Neutral, &bike);
        assert_eq!(opts.len(), 1);
        let o = &opts[0];
        assert_eq!(
            o.dplus, 0.0,
            "the D+ axis is denoised ascent only; a pure descent contributes 0 (no descent penalty)"
        );
        assert!(
            o.elevation_gain.unwrap() < 5,
            "displayed ascent must be ~0 for a pure descent, got {:?} m",
            o.elevation_gain
        );
    }

    #[test]
    fn bike_leg_option_reports_dismount_length() {
        // A path with one push segment (bikeaccess=false, footaccess=true) and one
        // rideable segment: the LegOption must report the push metres as dismount_length
        // so the UI can show how much of an alternative is walked, not ridden.
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, LatLng, NodeData, OsmNodeData, StreetEdgeData,
        };
        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0100));
        let c = g.add_node(mk("c", 50.0000, 4.0200));
        g.build_raptor_index();
        let mut push = BikeAttrs::road_default();
        push.bikeaccess = false; // not rideable -> must dismount
        push.footaccess = true;
        let ride = BikeAttrs::road_default();
        let edge = |o: NodeID, d: NodeID, len: usize, at: BikeAttrs| {
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, partial: false, length: len,
                foot: true, bike: true, car: false, attrs: at, elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 500, push));
        g.add_edge(b, edge(b, c, 700, ride));
        let bike = g.default_bike_cost();
        let o = g.leg_option_for_nodes_test(&[a, b, c], RoutingMode::Bike, &bike);
        assert_eq!(o.dismount_length, 500, "only the push segment counts as dismount");
        assert_eq!(o.length, 1200);
        // The push run spans geometry indices [0,1] (edge a->b); ride edge b->c is not
        // included. Lets the UI paint the dismount stretch of a selected alternative.
        assert_eq!(o.dismount_runs.len(), 1, "one contiguous push run");
        assert_eq!(o.dismount_runs[0].start, 0);
        assert_eq!(o.dismount_runs[0].end, 1);
    }

    #[test]
    fn bike_search_times_push_at_push_speed_not_cycling() {
        // A push (dismount) edge must be costed in the search at the (slow) PUSH speed,
        // like its displayed time — not at cycling speed, which would make footway
        // shortcuts look as fast as riding and flood routes with dismounts.
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, LatLng, NodeData, OsmNodeData, StreetEdgeData,
        };
        let mut g = Graph::new();
        g.set_walking_speed_mps(1.0);
        g.set_cycling_speed_mps(5.0);
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0040));
        g.build_raptor_index();
        let mut push = BikeAttrs::road_default();
        push.bikeaccess = false;
        push.footaccess = true;
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a, destination: b, partial: false, length: 300,
                foot: true, bike: true, car: false, attrs: push, elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Bike, LegRole::Neutral, &bike);
        assert!(!opts.is_empty(), "push route must exist");
        // 300 m at the default push speed (0.9 m/s) ⇒ ~333 s, far above the 60 s a
        // cycling-speed (5.0 m/s) costing would give. (No ride→push boundary here: the
        // single edge from the origin has no previous edge, so no dismount stop.)
        let expect = (300.0 / g.raptor.bike_profile.push_speed_mps).round() as u32;
        assert_eq!(
            opts[0].time.round() as u32, expect,
            "300 m push must cost ~{expect} s at push speed, not 60 s at cycling speed; got {}",
            opts[0].time
        );
        assert!(opts[0].time > 300.0, "push is slower than cycling");
    }

    #[test]
    fn bike_search_avoids_wrong_way_oneway() {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        g.set_distance_budget(0.5);
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0100));
        let c = g.add_node(mk("c", 50.0030, 4.0050));
        g.build_raptor_index();
        let mk_attr = |ww: bool| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Tertiary;
            at.surface = Surface::Paved;
            at.wrong_way = ww;
            at
        };
        let edge = |o: NodeID, d: NodeID, len: usize, ww: bool| {
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, partial: false, length: len,
                foot: true, bike: true, car: false, attrs: mk_attr(ww), elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        // Direct a->b is a contraflow (wrong_way); the legal detour a->c->b is longer.
        g.add_edge(a, edge(a, b, 700, true));
        g.add_edge(a, edge(a, c, 450, false));
        g.add_edge(c, edge(c, b, 450, false));
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Bike, LegRole::Neutral, &bike);
        assert!(!opts.is_empty(), "a legal route must exist");
        for o in &opts {
            // Re-derive the per-step edges from the node path (the former
            // `path_edges` derivation); this synthetic graph is not contracted so
            // the carried `.edges` are empty here.
            let edges: Vec<&StreetEdgeData> = o
                .nodes
                .windows(2)
                .filter_map(|w| {
                    g.edges[w[0].0].iter().find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == w[1] => Some(s),
                        _ => None,
                    })
                })
                .collect();
            let ww: usize = edges
                .iter()
                .filter(|e| e.attrs.wrong_way)
                .map(|e| e.length)
                .sum();
            assert_eq!(ww, 0, "no alternative may ride against a one-way");
            let length: usize = edges.iter().map(|e| e.length).sum();
            assert!(length >= 900, "must take the legal detour, got {length}");
        }
    }

    #[test]
    fn bike_leg_options_surface_most_cycleway_detour() {
        // The direct edge is shorter but off bike infrastructure (high cyc deficit);
        // the detour is slightly longer but fully on cycleways (deficit 0) — a genuine
        // Time↔Cycleway trade-off. With bucketing + budget + dedup all active, the
        // most-cycleway route must still surface (regression for the whole pipeline:
        // engine front → representatives → dedup → select_diverse).
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        g.set_bike_bucket_cyc_k(0.11);
        g.set_bike_bucket_dpl_k(0.013);
        g.set_distance_budget(0.5);
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        // Scaled so the detour is several minutes slower — a *visible* time difference,
        // so display-granularity dedup keeps both (a sub-minute gap would correctly
        // collapse to one). Coords keep haversine ≤ edge length (corridor stays sound).
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let c = g.add_node(mk("c", 50.0040, 4.0150));
        let b = g.add_node(mk("b", 50.0000, 4.0300));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, cycle: bool| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = Surface::Paved;
            at.isbike = cycle;
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: d, partial: false, length: len,
                foot: true, bike: true, car: false, attrs: at, elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 2130, false)); // direct, off-infra (high cyc deficit)
        g.add_edge(a, edge(a, c, 1500, true)); // longer detour on cycleway (deficit 0)
        g.add_edge(c, edge(c, b, 1500, true));
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Bike, LegRole::Neutral, &bike);
        assert!(opts.len() >= 2, "expected a Time↔Cycleway trade-off pair, got {}", opts.len());
        assert!(
            opts.iter().any(|o| o.cycleway_deficit < 1.0),
            "the most-cycleway (deficit≈0) detour must survive the pipeline; cyc_defs={:?}",
            opts.iter().map(|o| o.cycleway_deficit).collect::<Vec<_>>()
        );
        assert!(
            opts.iter().any(|o| o.cycleway_deficit > 100.0),
            "the off-infrastructure direct route must also be offered"
        );
    }

    #[test]
    fn multiobj_leg_options_returns_front_for_a_connected_leg() {
        let (mut g, a, b) = detour_graph();
        g.set_distance_budget(f64::INFINITY);
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Walk, LegRole::Neutral, &bike);
        assert!(opts.len() >= 2, "leg options surface the front");
        for o in &opts {
            assert!(o.p95 >= o.p50);
            assert_eq!(*o.nodes.first().unwrap(), a);
            assert_eq!(*o.nodes.last().unwrap(), b);
        }
    }

    fn climb_detour_graph() -> (Graph, NodeID, NodeID) {
        let mut g = Graph::new();
        g.set_distance_budget(f64::INFINITY);
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let a = g.add_node(mk("a", 50.000, 4.0000));
        let c = g.add_node(mk("c", 50.0010, 4.0010));
        let b = g.add_node(mk("b", 50.000, 4.0020));
        g.build_raptor_index();
        let e = |o: NodeID, d: NodeID, len: usize, elev: i16| {
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
        g.add_edge(a, e(a, b, 100, 8)); // short, climbs 8 m
        g.add_edge(a, e(a, c, 400, 0)); // long, flat
        g.add_edge(c, e(c, b, 400, 0));
        // b has no outgoing edges; a back-edge makes it a proper junction endpoint
        // so contraction reaches it (otherwise the builder skips it, k=0).
        g.add_edge(b, e(b, a, 100, -8));
        // The only trade-off here is climb-vs-flat, so exercise the D+-as-selection
        // path (off by default; on real graphs Time prices climbing instead).
        g.raptor.set_bike_select_dplus(true);
        (g, a, b)
    }

    impl Graph {
        fn leg_option_for_nodes_test(
            &self,
            nodes: &[NodeID],
            mode: RoutingMode,
            bike: &BikeCost,
        ) -> LegOption {
            // Derive the carried arena steps from the node path (the former
            // `path_edges` derivation) so `.edges` is the g-free authority the
            // production reconstruction now reads.
            let carried: Vec<(StreetEdgeData, (f64, f64), LatLng)> = nodes
                .windows(2)
                .filter_map(|w| {
                    self.edges[w[0].0].iter().find_map(|e| match e {
                        EdgeData::Street(s) if s.destination == w[1] => {
                            Some((*s, self.dir_between(w[0], w[1]), self.node_loc(w[1])))
                        }
                        _ => None,
                    })
                })
                .collect();
            // cost vector irrelevant to shared_fraction/select_diverse; ZERO is fine here.
            self.leg_option(
                nodes,
                &carried,
                crate::structures::cost::CostVector::ZERO,
                mode,
                bike,
                0,
            )
        }
    }

    #[test]
    fn select_diverse_drops_near_identical_routes() {
        let (g, a, b) = climb_detour_graph();
        let bike = g.default_bike_cost();
        // Two genuinely distinct routes share 0% -> both kept.
        let direct = g.leg_option_for_nodes_test(&[a, b], RoutingMode::Bike, &bike);
        let detour = g.leg_option_for_nodes_test(&[a, /*c*/ NodeID(1), b], RoutingMode::Bike, &bike);
        assert!(
            g.shared_fraction_edges(&direct.edges, &detour.edges) < 0.01,
            "disjoint routes"
        );
        let kept = g.select_diverse(vec![direct.clone(), detour.clone()], 0.6);
        assert_eq!(kept.len(), 2, "distinct routes both kept");
        // A duplicate of the first shares 100% -> dropped.
        let kept2 = g.select_diverse(vec![direct.clone(), direct.clone()], 0.6);
        assert_eq!(kept2.len(), 1, "near-identical route dropped");
    }

    #[test]
    fn bike_leg_options_span_time_and_dplus_from_front() {
        use crate::structures::Surface;
        // Reuse detour_graph shape but add an elevation trade-off: direct short+climby,
        // detour long+flat. Both are on the (Time, D+) front.
        let (mut g, a, b) = climb_detour_graph();
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let opts = g.multiobj_leg_options(a, b, RoutingMode::Bike, LegRole::Neutral, &bike);
        for o in &opts {
            eprintln!(
                "OPT p50={} elevation_gain={:?} cycleroute_length={:?} length={}",
                o.p50, o.elevation_gain, o.cycleroute_length, o.length
            );
        }
        assert!(opts.len() >= 2, "front yields a time/climb trade-off pair");
        let max_dplus = opts.iter().map(|o| o.elevation_gain.unwrap_or(0)).max().unwrap();
        let min_dplus = opts.iter().map(|o| o.elevation_gain.unwrap_or(0)).min().unwrap();
        assert!(max_dplus > min_dplus, "options differ in climb (D+ is a core axis)");
        let _ = Surface::Paved;
    }

    #[test]
    fn leg_options_drop_minute_identical_reliability_dupes() {
        fn opt(p50: u32, p95: u32, surface: f64, dplus: f64) -> crate::structures::plan::LegOption {
            crate::structures::plan::LegOption {
                time: p50 as f64,
                dplus,
                surface,
                variance: 0.0,
                cycleway_deficit: 0.0,
                p50,
                p95,
                length: 1000,
                unpaved_length: 0,
                dismount_length: 0,
                dismount_runs: vec![],
                elevation_gain: Some(dplus as usize),
                cycleroute_length: None,
                geometry: vec![],
                nodes: vec![],
                edges: vec![],
            }
        }
        let opts = vec![
            opt(600, 645, 900.0, 4.0),
            opt(600, 630, 900.0, 4.0),
            opt(640, 700, 900.0, 1.0),
        ];
        let kept = Graph::dedup_leg_options(opts);
        assert_eq!(
            kept.len(),
            2,
            "minute-identical dup dropped, climb trade-off kept"
        );
        assert!(
            kept.iter().any(|o| o.dplus == 1.0),
            "the flatter option survives"
        );
    }

    #[test]
    fn leg_options_prune_subdisplay_only_wins() {
        // Two routes both read "10 min", same surface/climb/reliability; one is 20 s
        // faster in raw seconds — a win the user can't see. It must be pruned, not
        // offered as a separate route.
        fn opt(p50: u32, surface: f64) -> crate::structures::plan::LegOption {
            crate::structures::plan::LegOption {
                time: p50 as f64,
                dplus: 4.0,
                surface,
                variance: 0.0,
                cycleway_deficit: 0.0,
                p50,
                p95: p50 + 60,
                length: 1000,
                unpaved_length: 0,
                dismount_length: 0,
                dismount_runs: vec![],
                elevation_gain: Some(4),
                cycleroute_length: None,
                geometry: vec![],
                nodes: vec![],
                edges: vec![],
            }
        }
        // A is 20 s faster (raw) but rougher; B is smoother (raw) but slower. They
        // CROSS on raw axes (neither raw-dominates), so without bucketing both
        // survive. Yet both read "10 min" and "1.02×" (102) — identical on every
        // displayed bucket — so display-granularity dedup must collapse them to one.
        let kept = Graph::dedup_leg_options(vec![opt(600, 1021.0), opt(620, 1019.0)]);
        assert_eq!(kept.len(), 1, "a sub-display-only win is pruned");
    }

    #[test]
    fn leg_options_keep_most_cycleway_route() {
        // The fast route is off bike infrastructure (high cycleway deficit); the
        // slower route is on cycleways (low deficit), equal on the other displayed
        // axes. CyclewayDeficit is a core bike axis (Time › Cycleway › D+), so the
        // most-cycleway route is a real trade-off and must survive dedup — without it
        // in the key, the slower route is wrongly dropped as "dominated", which
        // collapsed the alternatives the UI showed.
        fn opt(p50: u32, cyc: f64) -> crate::structures::plan::LegOption {
            crate::structures::plan::LegOption {
                time: p50 as f64,
                dplus: 5.0,
                surface: 1000.0,
                variance: 0.0,
                cycleway_deficit: cyc,
                p50,
                p95: p50 + 60,
                length: 1000,
                unpaved_length: 0,
                dismount_length: 0,
                dismount_runs: vec![],
                elevation_gain: Some(5),
                cycleroute_length: None,
                geometry: vec![],
                nodes: vec![],
                edges: vec![],
            }
        }
        let kept = Graph::dedup_leg_options(vec![opt(600, 9000.0), opt(660, 200.0)]);
        assert_eq!(kept.len(), 2, "the most-cycleway route survives despite being slower");
        assert!(
            kept.iter().any(|o| o.cycleway_deficit < 1000.0),
            "the low-deficit (most-cycleway) option is kept"
        );
    }

    #[test]
    fn bike_plan_rebuilds_ride_push_steps() {
        // A path with a foot-only (push) middle edge yields a distinct push step.
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
        let a = g.add_node(mk("a", 50.0, 4.0000));
        let m = g.add_node(mk("m", 50.0, 4.0010));
        let b = g.add_node(mk("b", 50.0, 4.0020));
        g.build_raptor_index();
        let mut ride = BikeAttrs::road_default();
        ride.highway = HighwayClass::Residential;
        ride.surface = Surface::Paved;
        let mut push = BikeAttrs::road_default();
        push.highway = HighwayClass::Footway;
        push.surface = Surface::Paved;
        push.bikeaccess = false;
        push.footaccess = true;
        let mk_e = |o, d, len, at| {
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
        g.add_edge(a, mk_e(a, m, 100, ride));
        g.add_edge(m, mk_e(m, b, 100, push));
        // Reverse edges make `m` a degree-2 interior node (contracted away) and a/b
        // proper junctions, so the a→b super-edge carries both the ride and push steps.
        g.add_edge(m, mk_e(m, a, 100, ride));
        g.add_edge(b, mk_e(b, m, 100, push));
        enable_contraction(&mut g);
        let bike = g.default_bike_cost();
        let plan = g
            .multiobj_direct_plan(a, b, RoutingMode::Bike, LegRole::Neutral, &bike, 0)
            .expect("bike plan");
        let PlanLeg::Walk(leg) = &plan.legs[0] else {
            panic!()
        };
        let dismount: Vec<bool> = leg
            .steps
            .iter()
            .map(|s| match s {
                PlanLegStep::Walk(w) => w.dismount,
                _ => false,
            })
            .collect();
        assert!(
            dismount.contains(&true) && dismount.contains(&false),
            "ride+push segmented, got {:?}",
            dismount
        );
    }
}
