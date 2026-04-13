use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap, HashMap},
};

use gtfs_structures::RouteType;
use kdtree::distance::squared_euclidean;

use crate::{
    ingestion::gtfs::{StopTime, TimetableSegment, TripId},
    structures::{
        EdgeData, NodeID, ScenarioBag, degrees_to_meters,
        plan::{
            ArrivalScenario, Plan, PlanCoordinate, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg,
            PlanTransitLegStep, PlanWalkLeg, PlanWalkLegStep, TransferRisk,
        },
        raptor::Trace,
    },
};

use super::{Graph, MAX_ROUNDS, WALKING_SPEED_MS};

impl Graph {
    pub fn raptor(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> Vec<Plan> {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / WALKING_SPEED_MS) as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self
            .nearest_stop_secs(origin, straight_line_secs)
            .max(min_access_secs);

        loop {
            let sources: Vec<(usize, u32)> = self
                .nearby_stops(origin, access_secs)
                .into_iter()
                .map(|(s, w)| (s, start_time + w))
                .collect();

            let targets = self.nearby_stops(destination, access_secs);

            if !sources.is_empty() && !targets.is_empty() {
                let results =
                    self.raptor_inner(&sources, &targets, start_time, access_secs, date, weekday, origin, destination);
                if !results.is_empty() {
                    return results;
                }
            }

            access_secs = access_secs.saturating_mul(2);

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(
                    self.walk_dijkstra(origin, u32::MAX)
                        .get(&destination)
                        .copied()
                        .unwrap_or(u32::MAX),
                );
            }

            if let Some(actual) = walk_only_secs {
                if access_secs >= actual {
                    return if actual < u32::MAX {
                        vec![self.build_walk_plan(origin, destination, start_time, actual)]
                    } else {
                        vec![]
                    };
                }
            }
        }
    }

    pub fn raptor_inner(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        access_secs: u32,
        date: u32,
        weekday: u8,
        origin: NodeID,
        destination: NodeID,
    ) -> Vec<Plan> {
        let n_stops = self.transit_stop_to_node.len();
        let n_patterns = self.transit_patterns.len();

        let mut best = vec![ScenarioBag::EMPTY; n_stops];
        let mut labels = vec![vec![ScenarioBag::EMPTY; n_stops]; MAX_ROUNDS + 1];
        let mut labels_rt: Vec<Vec<Option<RouteType>>> =
            vec![vec![None; n_stops]; MAX_ROUNDS + 1];
        let mut traces = vec![vec![Trace::NONE; n_stops]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_stops];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];

        for &(stop, time) in sources {
            let bag = ScenarioBag::single(time);
            labels[0][stop] = bag;
            best[stop] = bag;
            // labels_rt[0][stop] remains None = walking access
            Self::mark(stop, &mut marked, &mut is_marked);
        }

        self.apply_transfers(
            &mut labels[0],
            &mut labels_rt[0],
            &mut best,
            &mut traces[0],
            &mut marked,
            &mut is_marked,
            start_time + access_secs,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                rest[0].copy_from_slice(&prev[k - 1]);
            }
            {
                let (prev_rt, rest_rt) = labels_rt.split_at_mut(k);
                rest_rt[0].copy_from_slice(&prev_rt[k - 1]);
            }

            self.collect_routes(&marked, &mut queue, &mut queue_pos);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(&best, targets);

            {
                let (prev, rest) = labels.split_at_mut(k);
                let (prev_rt, rest_rt) = labels_rt.split_at_mut(k);
                let prev_slice = &prev[k - 1];
                let curr_slice = &mut rest[0];
                let prev_rt_slice = &prev_rt[k - 1];
                let curr_rt_slice = &mut rest_rt[0];

                for &pat in &queue {
                    self.scan_route(
                        pat,
                        queue_pos[pat],
                        date,
                        weekday,
                        cutoff,
                        prev_slice,
                        prev_rt_slice,
                        curr_slice,
                        curr_rt_slice,
                        &mut best,
                        &mut traces[k],
                        &mut marked,
                        &mut is_marked,
                    );
                }
            }

            for &p in &queue {
                queue_pos[p] = u32::MAX;
            }
            queue.clear();

            self.apply_transfers(
                &mut labels[k],
                &mut labels_rt[k],
                &mut best,
                &mut traces[k],
                &mut marked,
                &mut is_marked,
                cutoff,
            );

            if marked.is_empty() {
                break;
            }
        }

        self.extract(
            sources, targets, start_time, date, weekday, &labels, &labels_rt, &traces, origin,
            destination,
        )
    }

    fn build_walk_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
    ) -> Plan {
        let end = start_time + walk_secs;
        let length = (walk_secs as f64 * WALKING_SPEED_MS) as usize;

        let to_place = PlanPlace {
            node_id: destination,
            stop_position: None,
            arrival: Some(end),
            departure: None,
        };

        Plan {
            legs: vec![PlanLeg::Walk(PlanWalkLeg {
                from: PlanPlace {
                    node_id: origin,
                    stop_position: None,
                    arrival: None,
                    departure: Some(start_time),
                },
                to: to_place,
                start: start_time,
                end,
                duration: walk_secs,
                length,
                steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                    length,
                    time: walk_secs,
                    place: to_place,
                })],
                geometry: self.walk_path(origin, destination),
            })],
            start: start_time,
            end,
            arrival_distribution: vec![ArrivalScenario { time: end, probability: 1.0 }],
        }
    }

    fn collect_routes(&self, marked: &[usize], queue: &mut Vec<usize>, queue_pos: &mut [u32]) {
        for &stop in marked {
            let pats = self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
            for &(pat_id, pos) in pats {
                let p = pat_id.0 as usize;
                if queue_pos[p] == u32::MAX {
                    queue.push(p);
                }
                queue_pos[p] = queue_pos[p].min(pos);
            }
        }
    }

    fn scan_route(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: u32,
        prev: &[ScenarioBag],
        prev_rt: &[Option<RouteType>],
        curr: &mut [ScenarioBag],
        curr_rt: &mut [Option<RouteType>],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
    ) {
        let pat_stops = self.transit_idx_pattern_stops[pattern].of(&self.transit_pattern_stops);
        let n_trips = self.transit_patterns[pattern].num_trips as usize;
        if n_trips == 0 {
            return;
        }

        let route_id = self.transit_patterns[pattern].route;
        let pat_rt = self.transit_routes[route_id.0 as usize].route_type;

        let all_times =
            self.transit_idx_pattern_stop_times[pattern].of(&self.transit_pattern_stop_times);
        let trip_ids = self.transit_idx_pattern_trips[pattern].of(&self.transit_pattern_trips);

        // (trip_index, boarding_pos, hit_prob)
        let mut boarded: Option<(usize, u32, f32)> = None;

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            if let Some((t, bp, hit_prob)) = boarded {
                let arr = col[t].arrival;
                if arr < cutoff {
                    let bag = if hit_prob < 1.0 {
                        let miss_arr =
                            self.next_trip_arrival(trip_ids, t + 1, col, date, weekday);
                        match miss_arr {
                            Some(ma) => {
                                ScenarioBag::with_scenarios(arr, hit_prob, ma, 1.0 - hit_prob)
                            }
                            None => ScenarioBag::with_scenarios(
                                arr,
                                hit_prob,
                                u32::MAX,
                                1.0 - hit_prob,
                            ),
                        }
                    } else {
                        ScenarioBag::single(arr)
                    };

                    if bag.improves_on(&best[stop]) {
                        curr[stop].try_improve(&bag);
                        best[stop].try_improve(&bag);
                        curr_rt[stop] = Some(pat_rt);
                        traces[stop] = Trace {
                            pattern: pattern as u32,
                            trip: t as u32,
                            boarded_at: bp,
                            alighted_at: pos as u32,
                            from_stop: u32::MAX,
                        };
                        Self::mark(stop, marked, is_marked);
                    }
                }
            }

            if prev[stop].is_reached() {
                let min_dep = prev[stop].expected() as u32;
                let t_start = col.partition_point(|st| st.departure < min_dep);
                for t in t_start..n_trips {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        if boarded.map_or(true, |(ct, _, _)| t < ct) {
                            let trip_dep = col[t].departure;

                            let total_hit_prob: f32 = prev[stop]
                                .scenarios()
                                .iter()
                                .map(|s| {
                                    let buf = trip_dep as i32 - s.time as i32;
                                    let p_make = match prev_rt[stop] {
                                        Some(rt) => self
                                            .transit_delay_models
                                            .get(&rt)
                                            .map(|cdf| cdf.prob_on_time(buf))
                                            .unwrap_or(1.0),
                                        None => 1.0, // walking access = deterministic
                                    };
                                    s.prob * p_make
                                })
                                .sum();

                            boarded = Some((t, pos as u32, total_hit_prob));
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Find the arrival time of the next active trip after `start` at `col` (alighting column).
    fn next_trip_arrival(
        &self,
        trip_ids: &[TripId],
        start: usize,
        col: &[StopTime],
        date: u32,
        weekday: u8,
    ) -> Option<u32> {
        (start..trip_ids.len())
            .find(|&t| self.is_trip_active(trip_ids[t], date, weekday))
            .map(|t| col[t].arrival)
    }

    /// Find the departure time of the next active trip after `after_trip` at `boarding_col`
    /// (the stop-times column for the boarding stop position).
    fn next_active_trip_departure(
        &self,
        trip_ids: &[TripId],
        after_trip: usize,
        boarding_col: &[StopTime],
        date: u32,
        weekday: u8,
    ) -> Option<u32> {
        (after_trip..trip_ids.len())
            .find(|&t| self.is_trip_active(trip_ids[t], date, weekday))
            .map(|t| boarding_col[t].departure)
    }

    fn apply_transfers(
        &self,
        labels: &mut [ScenarioBag],
        labels_rt: &mut [Option<RouteType>],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        cutoff: u32,
    ) {
        let n = marked.len();
        for i in 0..n {
            let stop = marked[i];
            let time = labels[stop].earliest();
            if time >= cutoff {
                continue;
            }

            let transfers = self.transit_idx_stop_transfers[stop].of(&self.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.transit_node_to_stop[target_node.0] as usize;

                let bag = labels[stop].shifted_by(walk);
                if bag.earliest() >= cutoff {
                    continue;
                }
                if bag.improves_on(&best[target]) {
                    labels[target].try_improve(&bag);
                    best[target].try_improve(&bag);
                    labels_rt[target] = labels_rt[stop]; // preserve incoming route type through walk
                    traces[target] = Trace {
                        pattern: u32::MAX,
                        trip: u32::MAX,
                        boarded_at: u32::MAX,
                        alighted_at: u32::MAX,
                        from_stop: stop as u32,
                    };
                    Self::mark(target, marked, is_marked);
                }
            }
        }
    }

    /// Returns `true` if any transit leg in the plan moves the user farther from the
    /// destination than where that leg started (backward detour).
    /// A 150 m slack is allowed to tolerate minor alignment bends.
    fn has_backward_transit_leg(&self, plan: &Plan, destination: NodeID) -> bool {
        const BACKWARD_SLACK_M: usize = 150;
        plan.legs.iter().any(|leg| {
            if let PlanLeg::Transit(t) = leg {
                let from_dist = self.nodes_distance(t.from.node_id, destination);
                let to_dist = self.nodes_distance(t.to.node_id, destination);
                to_dist > from_dist.saturating_add(BACKWARD_SLACK_M)
            } else {
                false
            }
        })
    }

    const EXTREME_RISK_RELIABILITY: f32 = 0.10;
    const EXTREME_RISK_WAIT_SECS: u32 = 7200;

    fn is_extreme_risk(plan: &Plan) -> bool {
        plan.legs.iter().any(|leg| {
            if let PlanLeg::Transit(t) = leg {
                if let Some(ref risk) = t.transfer_risk {
                    if risk.reliability < Self::EXTREME_RISK_RELIABILITY {
                        let wait = risk
                            .next_departure
                            .map(|nd| nd.saturating_sub(risk.scheduled_departure))
                            .unwrap_or(u32::MAX);
                        return wait > Self::EXTREME_RISK_WAIT_SECS;
                    }
                }
            }
            false
        })
    }

    fn extract(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        labels_rt: &[Vec<Option<RouteType>>],
        traces: &[Vec<Trace>],
        origin: NodeID,
        destination: NodeID,
    ) -> Vec<Plan> {
        let mut candidates = Vec::new();
        let mut pareto_best = u32::MAX;

        for k in 0..=MAX_ROUNDS {
            let mut best_arr = u32::MAX;
            let mut best_stop = 0usize;
            let mut best_walk = 0u32;

            for &(s, w) in targets {
                let total = labels[k][s].earliest().saturating_add(w);
                if total < best_arr {
                    best_arr = total;
                    best_stop = s;
                    best_walk = w;
                }
            }

            if best_arr >= pareto_best {
                continue;
            }

            pareto_best = best_arr;

            let (mut legs, origin_stop) =
                self.reconstruct(k, best_stop, date, weekday, labels, labels_rt, traces);

            if legs.is_empty() {
                continue;
            }

            // Three-pass tightening: compute backward labels then slide every
            // transit leg (including the last) to the latest feasible departure.
            // This maximises waiting at the origin instead of at intermediate stops.
            let transit_count =
                legs.iter().filter(|l| matches!(l, PlanLeg::Transit(_))).count();
            if transit_count > 0 {
                let lambda = self.raptor_backward(
                    best_stop,
                    best_arr.saturating_sub(best_walk),
                    transit_count,
                    date,
                    weekday,
                );
                self.tighten_with_backward_labels(&mut legs, &lambda, date, weekday);
            }

            if let Some(&(_, stop_arrival)) = sources.iter().find(|&&(s, _)| s == origin_stop) {
                let first_walk = stop_arrival.saturating_sub(start_time);
                if first_walk > 0 {
                    let stop_node = self.transit_stop_to_node[origin_stop];
                    let length = (first_walk as f64 * WALKING_SPEED_MS) as usize;
                    // Depart at the latest possible moment so the user arrives at the
                    // stop exactly when the first transit vehicle boards, not at query
                    // time (which would leave them waiting invisibly on the platform).
                    let walk_start = legs
                        .first()
                        .map(|l| match l {
                            PlanLeg::Transit(t) => t.start.saturating_sub(first_walk),
                            PlanLeg::Walk(w) => w.start.saturating_sub(first_walk),
                        })
                        .unwrap_or(start_time)
                        .max(start_time); // can never depart before query time
                    let walk_end = walk_start + first_walk;
                    let to_place = PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: Some(walk_end),
                        departure: None,
                    };
                    legs.insert(
                        0,
                        PlanLeg::Walk(PlanWalkLeg {
                            from: PlanPlace {
                                node_id: origin,
                                stop_position: None,
                                arrival: None,
                                departure: Some(walk_start),
                            },
                            to: to_place,
                            start: walk_start,
                            end: walk_end,
                            duration: first_walk,
                            length,
                            steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                                length,
                                time: first_walk,
                                place: to_place,
                            })],
                            geometry: self.walk_path(origin, stop_node),
                        }),
                    );
                }
            }

            if best_walk > 0 {
                let walk_start = labels[k][best_stop].earliest();
                let stop_node = self.transit_stop_to_node[best_stop];
                let length = (best_walk as f64 * WALKING_SPEED_MS) as usize;
                let to_place = PlanPlace {
                    node_id: destination,
                    stop_position: None,
                    arrival: Some(best_arr),
                    departure: None,
                };
                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: None,
                        departure: Some(walk_start),
                    },
                    to: to_place,
                    start: walk_start,
                    end: best_arr,
                    duration: best_walk,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: best_walk,
                        place: to_place,
                    })],
                    geometry: self.walk_path(stop_node, destination),
                }));
            }

            let departure = legs
                .first()
                .map(|l| match l {
                    PlanLeg::Walk(w) => w.start,
                    PlanLeg::Transit(t) => t.start,
                })
                .unwrap_or(start_time);

            let arrival_bag = labels[k][best_stop].shifted_by(best_walk);
            // Expand each scenario with the last transit vehicle's delay CDF.
            // Each CDF bin becomes a separate output scenario: one entry per
            // (internal scenario × CDF bin), with probability = s.prob × bin_mass.
            let arrival_distribution: Vec<ArrivalScenario> =
                match labels_rt[k][best_stop].and_then(|rt| self.transit_delay_models.get(&rt)) {
                    Some(cdf) if !cdf.bins.is_empty() => {
                        let mut dist =
                            Vec::with_capacity(arrival_bag.scenarios().len() * cdf.bins.len());
                        let mut prev_cum = 0.0f32;
                        for &(delay, cum_prob) in &cdf.bins {
                            let bin_mass = cum_prob - prev_cum;
                            if bin_mass > 0.0 {
                                for s in arrival_bag.scenarios() {
                                    dist.push(ArrivalScenario {
                                        time: s.time.saturating_add_signed(delay),
                                        probability: s.prob * bin_mass,
                                    });
                                }
                            }
                            prev_cum = cum_prob;
                        }
                        dist.sort_by_key(|s| s.time);
                        dist
                    }
                    _ => arrival_bag
                        .scenarios()
                        .iter()
                        .map(|s| ArrivalScenario { time: s.time, probability: s.prob })
                        .collect(),
                };

            candidates.push(Plan {
                legs: Self::merge_consecutive_walks(legs),
                start: departure,
                end: best_arr,
                arrival_distribution,
            });
        }

        // Safety net: only apply filter if at least one candidate passes.
        if candidates.iter().any(|p| !Self::is_extreme_risk(p)) {
            candidates.retain(|p| !Self::is_extreme_risk(p));
        }
        // Filter plans where a transit leg moves the user away from the destination
        // (i.e. a "backward" leg). Allow up to 150 m of regression to tolerate
        // slightly curved alignments; anything more is a wrong-direction detour.
        if candidates.iter().any(|p| !self.has_backward_transit_leg(p, destination)) {
            candidates.retain(|p| !self.has_backward_transit_leg(p, destination));
        }
        candidates
    }

    /// Merge any two consecutive `PlanLeg::Walk` segments into one.
    /// This collapses a transfer-walk + last-mile-walk that share no boarding in between.
    fn merge_consecutive_walks(legs: Vec<PlanLeg>) -> Vec<PlanLeg> {
        let mut out: Vec<PlanLeg> = Vec::with_capacity(legs.len());
        for leg in legs {
            match (out.last_mut(), &leg) {
                (Some(PlanLeg::Walk(prev)), PlanLeg::Walk(next)) => {
                    let mut merged_geo = prev.geometry.clone();
                    // Avoid duplicating the shared waypoint coordinate.
                    if merged_geo.last().map(|c| (c.lat, c.lon))
                        == next.geometry.first().map(|c| (c.lat, c.lon))
                    {
                        merged_geo.extend_from_slice(&next.geometry[1..]);
                    } else {
                        merged_geo.extend_from_slice(&next.geometry);
                    }
                    let new_duration = prev.duration + next.duration;
                    let new_length = prev.length + next.length;
                    let new_end = next.end;
                    let to = next.to;
                    let step = PlanLegStep::Walk(PlanWalkLegStep {
                        length: new_length,
                        time: new_duration,
                        place: to,
                    });
                    *prev = PlanWalkLeg {
                        from: prev.from,
                        to,
                        start: prev.start,
                        end: new_end,
                        duration: new_duration,
                        length: new_length,
                        steps: vec![step],
                        geometry: merged_geo,
                    };
                }
                _ => out.push(leg),
            }
        }
        out
    }

    fn reconstruct(
        &self,
        round: usize,
        target_stop: usize,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        labels_rt: &[Vec<Option<RouteType>>],
        traces: &[Vec<Trace>],
    ) -> (Vec<PlanLeg>, usize) {
        let mut legs = Vec::new();
        let mut stop = target_stop;
        let mut k = round;

        loop {
            let trace = traces[k][stop];
            if !trace.is_transit() && !trace.is_transfer() {
                break;
            }

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let start = labels[k][from].earliest();
                let end = labels[k][stop].earliest();
                let duration = end - start;
                let from_node = self.transit_stop_to_node[from];
                let to_node = self.transit_stop_to_node[stop];
                let length = (duration as f64 * WALKING_SPEED_MS) as usize;

                let to_place = PlanPlace {
                    stop_position: None,
                    arrival: Some(end),
                    departure: None,
                    node_id: to_node,
                };

                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        stop_position: None,
                        arrival: None,
                        departure: Some(start),
                        node_id: from_node,
                    },
                    to: to_place,
                    start,
                    end,
                    duration,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: duration,
                        place: to_place,
                    })],
                    geometry: self.walk_path(from_node, to_node),
                }));
                stop = from;
                continue;
            }

            let p = trace.pattern as usize;
            let t = trace.trip as usize;
            let bp = trace.boarded_at as usize;
            let ap = trace.alighted_at as usize;

            let pat_stops = self.transit_idx_pattern_stops[p].of(&self.transit_pattern_stops);
            let n_trips = self.transit_patterns[p].num_trips as usize;
            let times = self.transit_idx_pattern_stop_times[p].of(&self.transit_pattern_stop_times);
            let trip_ids = self.transit_idx_pattern_trips[p].of(&self.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let bs = self.transit_node_to_stop[pat_stops[bp].0] as usize;
            let boarding_col = &times[bp * n_trips..(bp + 1) * n_trips];

            let transfer_risk = if k == 0 || labels_rt[k - 1][bs].is_none() {
                None // first transit leg — walked directly from origin, no transfer uncertainty
            } else {
                let rt = labels_rt[k - 1][bs].unwrap();
                let arrival_at_bs = labels[k - 1][bs].earliest();
                let margin = board_dep as i32 - arrival_at_bs as i32;
                let next_departure =
                    self.next_active_trip_departure(trip_ids, t + 1, boarding_col, date, weekday);
                let (reliability, next_reliability) =
                    match self.transit_delay_models.get(&rt) {
                        Some(cdf) => (
                            cdf.prob_on_time(margin),
                            next_departure.map(|nd| {
                                cdf.prob_on_time(nd as i32 - arrival_at_bs as i32)
                            }),
                        ),
                        None => (1.0, None),
                    };
                Some(TransferRisk {
                    reliability,
                    scheduled_departure: board_dep,
                    next_departure,
                    next_reliability,
                })
            };

            let route_id = self.transit_patterns[p].route;

            let mut steps = Vec::with_capacity(ap - bp);
            let mut total_length = 0usize;
            for s in (bp + 1)..=ap {
                let seg_len = self.nodes_distance(pat_stops[s - 1], pat_stops[s]);
                total_length += seg_len;

                let arr = times[s * n_trips + t].arrival;
                let prev_dep = times[(s - 1) * n_trips + t].departure;

                // Look up the TimetableSegment for this hop from the transit edge.
                let timetable_segment = self.edges[pat_stops[s - 1].0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Transit(te)
                            if te.destination == pat_stops[s] && te.route_id == route_id =>
                        {
                            Some(te.timetable_segment)
                        }
                        _ => None,
                    })
                    .unwrap_or(TimetableSegment { start: 0, len: 0 });

                // For the boarding step, find the absolute departure_index
                // so previousDepartures / nextDepartures can locate alternatives.
                // For subsequent steps, find_alternatives recomputes it by trip_id.
                let departure_index = if s == bp + 1 {
                    self.transit_departures
                        [timetable_segment.start..timetable_segment.start + timetable_segment.len]
                        .iter()
                        .position(|ts| ts.trip_id == trip_ids[t])
                        .map(|i| timetable_segment.start + i)
                        .unwrap_or(timetable_segment.start)
                } else {
                    0
                };

                steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    length: seg_len,
                    time: arr - prev_dep,
                    place: PlanPlace {
                        node_id: pat_stops[s],
                        stop_position: Some(s as u32),
                        arrival: Some(arr),
                        departure: if s < ap {
                            Some(times[s * n_trips + t].departure)
                        } else {
                            None
                        },
                    },
                    date,
                    weekday,
                    timetable_segment,
                    departure_index,
                }));
            }

            let transit_geometry: Vec<PlanCoordinate> = match self.get_pattern_shape(p) {
                Some((shape_pts, stop_idx)) => {
                    let from = stop_idx[bp] as usize;
                    let to = stop_idx[ap] as usize;
                    shape_pts[from..=to]
                        .iter()
                        .map(|coord| PlanCoordinate { lat: coord.latitude, lon: coord.longitude })
                        .collect()
                }
                None => (bp..=ap).map(|s| self.node_coord(pat_stops[s])).collect(),
            };

            legs.push(PlanLeg::Transit(PlanTransitLeg {
                from: PlanPlace {
                    stop_position: Some(bp as u32),
                    arrival: None,
                    departure: Some(board_dep),
                    node_id: pat_stops[bp],
                },
                to: PlanPlace {
                    stop_position: Some(ap as u32),
                    arrival: Some(alight_arr),
                    departure: None,
                    node_id: pat_stops[ap],
                },
                start: board_dep,
                end: alight_arr,
                trip_id: trip_ids[t],
                length: total_length,
                duration: alight_arr - board_dep,
                steps,
                geometry: transit_geometry,
                transfer_risk,
                preceding_arrival: if k == 0 || labels_rt[k - 1][bs].is_none() {
                    None
                } else {
                    Some(labels[k - 1][bs].earliest())
                },
                preceding_route_type: if k == 0 { None } else { labels_rt[k - 1][bs] },
                bikes_allowed: self.get_trip(trip_ids[t]).and_then(|t| t.bikes_allowed),
            }));

            stop = self.transit_node_to_stop[pat_stops[bp].0] as usize;
            k -= 1;
        }

        legs.reverse();

        (legs, stop)
    }

    /// Find the latest trip (by index) in `col` whose arrival is ≤ `max_arrival`
    /// and that is active on the given date/weekday.
    /// Returns the trip index within the pattern (not an absolute index).
    fn latest_trip_arriving_at_stop_before(
        &self,
        col: &[StopTime],
        trip_ids: &[TripId],
        max_arrival: u32,
        date: u32,
        weekday: u8,
    ) -> Option<usize> {
        // Scan backward: trips are sorted by departure but arrival ordering is
        // not strictly guaranteed across all GTFS feeds, so avoid partition_point.
        // We want the latest-indexed active trip whose arrival ≤ max_arrival.
        for t in (0..col.len()).rev() {
            if col[t].arrival <= max_arrival {
                let svc = self.transit_trips[trip_ids[t].0 as usize].service_id;
                if self.transit_services[svc.0 as usize].is_active(date, weekday) {
                    return Some(t);
                }
            }
        }
        None
    }

    /// Pass 2 of three-pass RAPTOR: backward label computation.
    ///
    /// Computes `lambda[remaining][stop]` = latest time you can be ready at
    /// `stop` with `remaining` transit legs still available and still reach
    /// the destination by `target_latest_arr`.  0 = unreachable (sentinel).
    ///
    /// `num_transit_legs` is the number of transit legs K reconstructed from
    /// the forward pass.  `lambda` has dimensions `[0..=K][0..n_stops]`.
    fn raptor_backward(
        &self,
        target_compact_stop: usize,
        target_latest_arr: u32,
        num_transit_legs: usize,
        date: u32,
        weekday: u8,
    ) -> Vec<Vec<u32>> {
        let n_stops = self.transit_stop_to_node.len();
        let n_patterns = self.transit_patterns.len();

        // lambda[remaining][stop]: 0 = unreachable (sentinel value)
        let mut lambda: Vec<Vec<u32>> = vec![vec![0u32; n_stops]; num_transit_legs + 1];

        let mut marked: Vec<usize> = Vec::new();
        let mut is_marked: Vec<bool> = vec![false; n_stops];

        // ── Seed: remaining = 0 ──────────────────────────────────────────────
        if target_latest_arr > 0 {
            lambda[0][target_compact_stop] = target_latest_arr;
            Self::mark(target_compact_stop, &mut marked, &mut is_marked);
        }

        // Reverse footpath relaxation from the seeded stop (one hop)
        self.apply_reverse_footpaths(&mut lambda[0], &mut marked, &mut is_marked);

        // ── Rounds 1..=K ─────────────────────────────────────────────────────
        for round in 1..=num_transit_legs {
            if marked.is_empty() {
                break;
            }

            // Collect patterns that serve any marked stop (no position restriction:
            // backward scan always goes from last stop to first).
            let mut queue: Vec<usize> = Vec::new();
            let mut in_queue: Vec<bool> = vec![false; n_patterns];

            for &stop in &marked {
                let pats =
                    self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
                for &(pat_id, _pos) in pats {
                    let p = pat_id.0 as usize;
                    if !in_queue[p] {
                        in_queue[p] = true;
                        queue.push(p);
                    }
                }
            }

            marked.clear();
            is_marked.fill(false);

            // Backward scan for each queued pattern
            for &pat in &queue {
                let pat_stops =
                    self.transit_idx_pattern_stops[pat].of(&self.transit_pattern_stops);
                let n_trips = self.transit_patterns[pat].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }
                let all_times = self.transit_idx_pattern_stop_times[pat]
                    .of(&self.transit_pattern_stop_times);
                let trip_ids =
                    self.transit_idx_pattern_trips[pat].of(&self.transit_pattern_trips);

                // t_star: trip index (within pattern) that can alight at a stop
                // where lambda[round-1] is set, and gives the latest departure
                // from stops earlier in the pattern.
                let mut t_star: Option<usize> = None;

                for pos in (0..pat_stops.len()).rev() {
                    let compact = self.transit_node_to_stop[pat_stops[pos].0];
                    if compact == u32::MAX {
                        continue;
                    }
                    let stop = compact as usize;
                    let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

                    // Step A: propagate t_star — label this (earlier) stop with
                    // t_star's departure time from this position.
                    if let Some(t) = t_star {
                        let dep = col[t].departure;
                        if dep > 0 && dep > lambda[round][stop] {
                            lambda[round][stop] = dep;
                            Self::mark(stop, &mut marked, &mut is_marked);
                        }
                    }

                    // Step B: update t_star — if this stop has a backward label
                    // (lambda[round-1][stop] > 0), find the latest trip whose
                    // arrival here is ≤ that label.  If it gives a later departure
                    // at this position than the current t_star, adopt it.
                    if lambda[round - 1][stop] > 0 {
                        if let Some(t) = self.latest_trip_arriving_at_stop_before(
                            col,
                            trip_ids,
                            lambda[round - 1][stop],
                            date,
                            weekday,
                        ) {
                            let update = match t_star {
                                None => true,
                                Some(ct) => col[t].departure > col[ct].departure,
                            };
                            if update {
                                t_star = Some(t);
                            }
                        }
                    }
                }
            }

            // Reverse footpath relaxation from newly-marked stops (one hop)
            self.apply_reverse_footpaths(&mut lambda[round], &mut marked, &mut is_marked);
        }

        lambda
    }

    /// One-hop reverse footpath relaxation for backward RAPTOR.
    ///
    /// For each currently-marked stop `s`, propagates its backward label to
    /// every stop `src` that can walk TO `s`: `lambda[src] = lambda[s] - walk`.
    /// Only iterates the stops present in `marked` at call time (one hop).
    fn apply_reverse_footpaths(
        &self,
        lambda_k: &mut Vec<u32>,
        marked: &mut Vec<usize>,
        is_marked: &mut Vec<bool>,
    ) {
        let n = marked.len(); // snapshot: only process original entries
        for i in 0..n {
            let stop = marked[i];
            if stop >= self.transit_idx_stop_reverse_transfers.len() {
                continue;
            }
            let rev = self.transit_idx_stop_reverse_transfers[stop]
                .of(&self.transit_stop_reverse_transfers);
            for &(source, walk_time) in rev {
                let t = lambda_k[stop].saturating_sub(walk_time);
                if t > 0 && t > lambda_k[source] {
                    lambda_k[source] = t;
                    Self::mark(source, marked, is_marked);
                }
            }
        }
    }

    /// Pass 3 of three-pass RAPTOR: tighten transit legs using backward labels.
    ///
    /// Replaces `tighten_transit_legs`.  Unlike the old function:
    /// - Uses `lambda[remaining][alighting_compact]` as the deadline for each leg
    ///   instead of the next leg's original departure.  This makes left-to-right
    ///   processing correct because the labels already encode all future constraints.
    /// - Also tightens the **last** transit leg (not just pairs).
    fn tighten_with_backward_labels(
        &self,
        legs: &mut Vec<PlanLeg>,
        lambda: &[Vec<u32>],
        date: u32,
        weekday: u8,
    ) {
        let transit_indices: Vec<usize> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| {
                if matches!(l, PlanLeg::Transit(_)) { Some(i) } else { None }
            })
            .collect();

        let k = transit_indices.len();
        if k == 0 {
            return;
        }

        // Tracks earliest valid boarding time for the next leg (arrival of this
        // leg + intermediate walk).
        let mut current_time: u32 = 0;

        for i in 0..k {
            let ti = transit_indices[i];
            // `remaining` = number of transit legs after this one.
            let remaining = k - i - 1;

            let (boarding_node, alighting_node, leg_start) = match &legs[ti] {
                PlanLeg::Transit(t) => (t.from.node_id, t.to.node_id, t.start),
                _ => unreachable!(),
            };

            let alighting_compact = self.transit_node_to_stop[alighting_node.0];

            // Use backward label as the alighting deadline.
            let max_alighting = if alighting_compact != u32::MAX && remaining < lambda.len() {
                lambda[remaining][alighting_compact as usize]
            } else {
                0
            };

            // Compute walk time between this leg and the next transit leg (for
            // advancing current_time after this leg, regardless of tightening).
            let walk_to_next: u32 = if i < k - 1 {
                let next_ti = transit_indices[i + 1];
                legs[ti + 1..next_ti]
                    .iter()
                    .map(|l| match l { PlanLeg::Walk(w) => w.duration, _ => 0 })
                    .sum()
            } else {
                0
            };

            if max_alighting > 0 {
                let min_dep = if i == 0 { leg_start } else { current_time };

                if let Some((dep_idx, new_dep, _)) = self.latest_departure_before_arrival(
                    boarding_node,
                    alighting_node,
                    min_dep,
                    max_alighting,
                    date,
                    weekday,
                ) {
                    if new_dep > leg_start {
                        let cloned = match &legs[ti] {
                            PlanLeg::Transit(t) => t.clone(),
                            _ => unreachable!(),
                        };
                        if let Ok(mut alts) = cloned.find_alternatives(
                            self,
                            std::iter::once((dep_idx, &self.transit_departures[dep_idx])),
                            1,
                        ) {
                            if let Some(new_leg) = alts.pop() {
                                legs[ti] = PlanLeg::Transit(new_leg);
                            }
                        }
                    }
                }
            }

            // Advance current_time and, if there is a next transit leg, shift
            // intermediate walk legs forward and recompute transfer risk.
            let new_leg_end =
                match &legs[ti] { PlanLeg::Transit(t) => t.end, _ => unreachable!() };

            if i < k - 1 {
                let next_ti = transit_indices[i + 1];

                // Shift every walk leg between ti and next_ti.
                let mut cursor = new_leg_end;
                for l in legs[ti + 1..next_ti].iter_mut() {
                    if let PlanLeg::Walk(w) = l {
                        let new_start = cursor;
                        let new_end = new_start + w.duration;
                        w.start = new_start;
                        w.end = new_end;
                        w.from.departure = Some(new_start);
                        w.to.arrival = Some(new_end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(new_end);
                            }
                        }
                        cursor = new_end;
                    }
                }
                current_time = cursor; // = new_leg_end + walk_to_next

                // Update the next transit leg's preceding-arrival context and
                // recompute transfer_risk with the updated margin.
                if let PlanLeg::Transit(next_t) = &mut legs[next_ti] {
                    next_t.preceding_arrival = Some(cursor);
                    if let Some(prt) = next_t.preceding_route_type {
                        let margin = next_t.start as i32 - cursor as i32;
                        let next_dep =
                            next_t.transfer_risk.as_ref().and_then(|r| r.next_departure);
                        let (rel, next_rel) = match self.transit_delay_models.get(&prt) {
                            Some(cdf) => (
                                cdf.prob_on_time(margin),
                                next_dep.map(|nd| {
                                    cdf.prob_on_time(nd as i32 - cursor as i32)
                                }),
                            ),
                            None => (1.0, None),
                        };
                        next_t.transfer_risk = Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: next_t.start,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                        });
                    } else {
                        next_t.transfer_risk = None;
                    }
                }
            } else {
                // Last leg: no following leg to update; current_time not needed.
                let _ = walk_to_next; // suppress unused-variable warning
            }
        }
    }

    #[inline]
    fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.transit_trips[trip_id.0 as usize].service_id;
        self.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    #[inline]
    fn target_cutoff(best: &[ScenarioBag], targets: &[(usize, u32)]) -> u32 {
        targets
            .iter()
            .map(|&(s, w)| {
                if best[s].is_reached() {
                    (best[s].expected() + w as f32) as u32
                } else {
                    u32::MAX
                }
            })
            .min()
            .unwrap_or(u32::MAX)
    }

    #[inline]
    fn mark(stop: usize, marked: &mut Vec<usize>, is_marked: &mut [bool]) {
        if !is_marked[stop] {
            is_marked[stop] = true;
            marked.push(stop);
        }
    }

    fn node_coord(&self, id: NodeID) -> PlanCoordinate {
        let loc = self.nodes[id.0].loc();
        PlanCoordinate { lat: loc.latitude, lon: loc.longitude }
    }

    /// Returns the sequence of OSM nodes forming the shortest walking path
    /// from `origin` to `destination`, converted to lat/lon coordinates.
    ///
    /// Falls back to a two-point straight line if no path is found.
    fn walk_path(&self, origin: NodeID, destination: NodeID) -> Vec<PlanCoordinate> {
        if origin == destination {
            let c = self.node_coord(origin);
            return vec![c];
        }

        const WALK_MMS: u32 = (WALKING_SPEED_MS * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut parent: HashMap<NodeID, NodeID> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        'outer: while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }
            if node == destination {
                break 'outer;
            }
            // Do not expand through transit stop nodes (except the origin which
            // may itself be a transit-stop-snapped OSM node).
            if node != origin && self.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / WALK_MMS as u64) as u32;
                            let nd = d.saturating_add(t);
                            let entry = dist.entry(street.destination).or_insert(u32::MAX);
                            if nd < *entry {
                                *entry = nd;
                                parent.insert(street.destination, node);
                                pq.push(Reverse((nd, street.destination)));
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                                parent.entry(transit.destination).or_insert(node);
                            }
                        }
                    }
                }
            }
        }

        if !dist.contains_key(&destination) {
            return vec![self.node_coord(origin), self.node_coord(destination)];
        }

        // Backtrack from destination to origin.
        let mut path_nodes = vec![destination];
        let mut current = destination;
        while current != origin {
            match parent.get(&current) {
                Some(&p) => {
                    path_nodes.push(p);
                    current = p;
                }
                None => break,
            }
        }
        path_nodes.reverse();
        path_nodes.iter().map(|&n| self.node_coord(n)).collect()
    }

    pub fn walk_dijkstra(&self, origin: NodeID, max_seconds: u32) -> HashMap<NodeID, u32> {
        const WALK_MMS: u32 = (WALKING_SPEED_MS * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }

            if self.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }

            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / WALK_MMS as u64) as u32;
                            let nd = d.saturating_add(t);
                            if nd <= max_seconds {
                                let entry = dist.entry(street.destination).or_insert(u32::MAX);
                                if nd < *entry {
                                    *entry = nd;
                                    pq.push(Reverse((nd, street.destination)));
                                }
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                            }
                        }
                    }
                }
            }
        }

        dist
    }

    fn nearest_stop_secs(&self, node: NodeID, straight_line_secs: u32) -> u32 {
        let loc = self.nodes[node.0].loc();
        self.transit_stops_tree
            .nearest(&[loc.latitude, loc.longitude], 1, &squared_euclidean)
            .ok()
            .and_then(|v| v.into_iter().next())
            .map(|(dist_sq, _)| {
                let dist_m = degrees_to_meters(dist_sq, loc.latitude);
                (dist_m / WALKING_SPEED_MS) as u32
            })
            .unwrap_or(straight_line_secs)
    }

    pub fn nearby_stops(&self, origin: NodeID, max_walk_secs: u32) -> Vec<(usize, u32)> {
        let walk_times = self.walk_dijkstra(origin, max_walk_secs);

        let mut stops = Vec::new();
        for (&node, &walk_secs) in &walk_times {
            let compact = self.transit_node_to_stop[node.0];
            if compact != u32::MAX {
                stops.push((compact as usize, walk_secs));
            }
        }
        stops
    }

    /// Collect every origin-departure time `T` in `[earliest, latest]` such that
    /// a transit vehicle departs from some access stop at `T + walk_secs`.
    ///
    /// Returns times sorted **descending** (latest first) so a Range-RAPTOR loop
    /// can progressively tighten the `target_cutoff` on each iteration.
    fn collect_interesting_times(
        &self,
        raw_stops: &[(usize, u32)], // (compact_stop_idx, walk_secs_from_origin)
        earliest_origin_departure: u32,
        latest_origin_departure: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<u32> {
        // Collect up to MAX_PER_PATTERN departure times per (stop, pattern) pair so
        // that high-frequency dead-end patterns cannot starve lower-frequency patterns
        // that actually connect origin to destination.  A global cap of MAX_TOTAL
        // bounds the number of RAPTOR iterations to keep query latency predictable.
        const MAX_PER_PATTERN: usize = 5;
        const MAX_TOTAL: usize = 20;

        let mut departure_times: BTreeSet<u32> = BTreeSet::new();

        'outer: for &(stop, walk_secs) in raw_stops {
            let earliest_at_stop = earliest_origin_departure.saturating_add(walk_secs);
            let latest_at_stop = latest_origin_departure.saturating_add(walk_secs);

            let pats = self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
            for &(pat_id, stop_pos) in pats {
                let p = pat_id.0 as usize;
                let n_trips = self.transit_patterns[p].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }

                let stop_times =
                    self.transit_idx_pattern_stop_times[p].of(&self.transit_pattern_stop_times);
                let trip_ids =
                    self.transit_idx_pattern_trips[p].of(&self.transit_pattern_trips);

                // Column-major layout: times for stop_pos are at [stop_pos*n_trips .. (stop_pos+1)*n_trips]
                let col = &stop_times[stop_pos as usize * n_trips..(stop_pos as usize + 1) * n_trips];

                // Binary search for the window [earliest_at_stop, latest_at_stop]
                let lo = col.partition_point(|st| st.departure < earliest_at_stop);
                let hi = col.partition_point(|st| st.departure <= latest_at_stop);

                let mut per_pattern_count = 0usize;
                for t in lo..hi {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        departure_times.insert(col[t].departure - walk_secs);
                        per_pattern_count += 1;
                        if per_pattern_count >= MAX_PER_PATTERN {
                            break; // enough trips sampled from this pattern
                        }
                        if departure_times.len() >= MAX_TOTAL {
                            break 'outer; // global cap reached
                        }
                    }
                }

                if departure_times.len() >= MAX_TOTAL {
                    break 'outer;
                }
            }
        }

        departure_times.into_iter().collect()
    }

    /// Range-RAPTOR: run RAPTOR for every interesting departure within
    /// `[start_time, start_time + window_secs]` and return the Pareto-optimal set
    /// of plans (transfer count × arrival time × departure time).
    pub fn raptor_range(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> Vec<Plan> {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / WALKING_SPEED_MS) as u32;

        let mut walk_only_secs: Option<u32> = None;
        // Always search within at least min_access_secs walk so that stops in
        // the forward direction are included alongside the geometrically-nearest
        // stop (which may be behind the origin relative to the destination).
        // Without this minimum, RAPTOR can receive only a single "backward" stop
        // as a source, produce a detour plan, and the has_backward_transit_leg
        // safety net refuses to filter it because no non-backward candidate
        // exists.  Configurable via config.yaml or the walk_radius_secs query
        // parameter; defaults to 10 min (600 s).
        let mut access_secs = self
            .nearest_stop_secs(origin, straight_line_secs)
            .max(min_access_secs);

        loop {
            let raw_stops = self.nearby_stops(origin, access_secs);
            let targets = self.nearby_stops(destination, access_secs);

            if !raw_stops.is_empty() && !targets.is_empty() {
                let sources: Vec<(usize, u32)> = raw_stops
                    .iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                // Probe at start_time to confirm this access_secs is valid.
                let probe = self.raptor_inner(
                    &sources,
                    &targets,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                );
                if !probe.is_empty() {
                    let departure_times = self.collect_interesting_times(
                        &raw_stops,
                        start_time,
                        start_time.saturating_add(window_secs),
                        date,
                        weekday,
                    );

                    if departure_times.is_empty() {
                        return probe;
                    }

                    let mut all_plans = Vec::new();
                    for t in departure_times {
                        let sources_t: Vec<(usize, u32)> = raw_stops
                            .iter()
                            .map(|&(s, w)| (s, t + w))
                            .collect();
                        let plans = self.raptor_inner(
                            &sources_t,
                            &targets,
                            t,
                            access_secs,
                            date,
                            weekday,
                            origin,
                            destination,
                        );
                        all_plans.extend(plans);
                    }
                    return Self::pareto_filter(all_plans);
                }
            }

            access_secs = access_secs.saturating_mul(2);

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(
                    self.walk_dijkstra(origin, u32::MAX)
                        .get(&destination)
                        .copied()
                        .unwrap_or(u32::MAX),
                );
            }

            if let Some(actual) = walk_only_secs {
                if access_secs >= actual {
                    return if actual < u32::MAX {
                        vec![self.build_walk_plan(origin, destination, start_time, actual)]
                    } else {
                        vec![]
                    };
                }
            }
        }
    }

    /// Remove dominated plans from `plans`.
    ///
    /// Plan A dominates plan B when A is at least as good in all three dimensions
    /// (departure time, arrival time, transfer count) and strictly better in at
    /// least one.
    fn pareto_filter(plans: Vec<Plan>) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        let mut result: Vec<Plan> = Vec::new();

        'outer: for plan in plans {
            let tc_p = transfer_count(&plan);
            // Skip plan if any existing plan is at least as good in every dimension
            // (handles both dominated plans and exact duplicates).
            for existing in &result {
                let tc_e = transfer_count(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start {
                    continue 'outer;
                }
            }
            // Remove any plans in `result` that `plan` dominates.
            result.retain(|existing| {
                let tc_e = transfer_count(existing);
                !(tc_p <= tc_e
                    && plan.end <= existing.end
                    && plan.start >= existing.start
                    && (tc_p < tc_e || plan.end < existing.end || plan.start > existing.start))
            });
            result.push(plan);
        }

        result
    }
}
