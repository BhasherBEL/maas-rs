use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::TimetableSegment,
    structures::{
        EdgeData, NodeID, ScenarioBag,
        plan::{
            ArrivalScenario, CandidateStatus, Plan, PlanCandidate, PlanLeg, PlanLegStep, PlanPlace,
            PlanTransitLeg, PlanTransitLegStep, PlanWalkLeg, PlanWalkLegStep, TransferRisk,
        },
    },
};

use super::Graph;

impl Graph {
    pub(super) fn build_walk_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
    ) -> Plan {
        let end = start_time + walk_secs;
        let length = (walk_secs as f64 * self.raptor.walking_speed_mps) as usize;

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

    /// Returns `true` if any transit leg in the plan moves the user farther from the
    /// destination than where that leg started (backward detour).
    /// A 150 m slack is allowed to tolerate minor alignment bends.
    pub(super) fn has_backward_transit_leg(&self, plan: &Plan, destination: NodeID) -> bool {
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

    pub(super) fn is_extreme_risk(plan: &Plan) -> bool {
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

    pub(super) fn extract_with_debug(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        labels_rt: &[Vec<Option<RouteType>>],
        traces: &[Vec<crate::structures::raptor::Trace>],
        origin: NodeID,
        destination: NodeID,
        mut debug_sink: Option<&mut Vec<PlanCandidate>>,
    ) -> Vec<Plan> {
        use super::MAX_ROUNDS;

        let mut candidates: Vec<Plan> = Vec::new();
        // Parallel to `candidates`: index of each candidate in `debug_sink`.
        // Populated even when debug_sink is None (dummy values) so the zip works.
        let mut sink_indices: Vec<usize> = Vec::new();
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
                if let Some(ref mut sink) = debug_sink {
                    sink.push(PlanCandidate {
                        round: k,
                        origin_departure: start_time,
                        plan: None,
                        status: CandidateStatus::NotImproving,
                    });
                }
                continue;
            }

            pareto_best = best_arr;

            let (mut legs, origin_stop) =
                self.reconstruct(k, best_stop, date, weekday, labels, labels_rt, traces);

            if legs.is_empty() {
                if let Some(ref mut sink) = debug_sink {
                    sink.push(PlanCandidate {
                        round: k,
                        origin_departure: start_time,
                        plan: None,
                        status: CandidateStatus::ReconstructionEmpty,
                    });
                }
                continue;
            }

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
                    let stop_node = self.raptor.transit_stop_to_node[origin_stop];
                    let length = (first_walk as f64 * self.raptor.walking_speed_mps) as usize;
                    let walk_start = legs
                        .first()
                        .map(|l| match l {
                            PlanLeg::Transit(t) => t.start.saturating_sub(first_walk),
                            PlanLeg::Walk(w) => w.start.saturating_sub(first_walk),
                        })
                        .unwrap_or(start_time)
                        .max(start_time);
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
                let stop_node = self.raptor.transit_stop_to_node[best_stop];
                let length = (best_walk as f64 * self.raptor.walking_speed_mps) as usize;
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
            let arrival_distribution: Vec<ArrivalScenario> =
                match labels_rt[k][best_stop].and_then(|rt| self.raptor.transit_delay_models.get(&rt)) {
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

            let plan = Plan {
                legs: Self::merge_consecutive_walks(legs),
                start: departure,
                end: best_arr,
                arrival_distribution,
            };

            if let Some(ref mut sink) = debug_sink {
                sink_indices.push(sink.len());
                sink.push(PlanCandidate {
                    round: k,
                    origin_departure: start_time,
                    plan: Some(plan.clone()),
                    status: CandidateStatus::Kept,
                });
            } else {
                sink_indices.push(candidates.len()); // dummy — never used to index sink
            }
            candidates.push(plan);
        }

        if candidates.iter().any(|p| !Self::is_extreme_risk(p)) {
            let mut new_candidates = Vec::new();
            let mut new_sink_indices = Vec::new();
            for (plan, si) in candidates.into_iter().zip(sink_indices.into_iter()) {
                if Self::is_extreme_risk(&plan) {
                    if let Some(ref mut sink) = debug_sink {
                        sink[si].status = CandidateStatus::ExtremeRisk;
                    }
                } else {
                    new_candidates.push(plan);
                    new_sink_indices.push(si);
                }
            }
            candidates = new_candidates;
            sink_indices = new_sink_indices;
        }

        if candidates.iter().any(|p| !self.has_backward_transit_leg(p, destination)) {
            let mut new_candidates = Vec::new();
            let mut new_sink_indices = Vec::new();
            for (plan, si) in candidates.into_iter().zip(sink_indices.into_iter()) {
                if self.has_backward_transit_leg(&plan, destination) {
                    if let Some(ref mut sink) = debug_sink {
                        sink[si].status = CandidateStatus::BackwardDetour;
                    }
                } else {
                    new_candidates.push(plan);
                    new_sink_indices.push(si);
                }
            }
            candidates = new_candidates;
        }

        candidates
    }

    /// Merge any two consecutive `PlanLeg::Walk` segments into one.
    pub(super) fn merge_consecutive_walks(legs: Vec<PlanLeg>) -> Vec<PlanLeg> {
        let mut out: Vec<PlanLeg> = Vec::with_capacity(legs.len());
        for leg in legs {
            match (out.last_mut(), &leg) {
                (Some(PlanLeg::Walk(prev)), PlanLeg::Walk(next)) => {
                    let mut merged_geo = prev.geometry.clone();
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

    pub(super) fn reconstruct(
        &self,
        round: usize,
        target_stop: usize,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        labels_rt: &[Vec<Option<RouteType>>],
        traces: &[Vec<crate::structures::raptor::Trace>],
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
                let from_node = self.raptor.transit_stop_to_node[from];
                let to_node = self.raptor.transit_stop_to_node[stop];
                let length = (duration as f64 * self.raptor.walking_speed_mps) as usize;

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

            let pat_stops = self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);
            let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
            let times = self.raptor.transit_idx_pattern_stop_times[p].of(&self.raptor.transit_pattern_stop_times);
            let trip_ids = self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let bs = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
            let boarding_col = &times[bp * n_trips..(bp + 1) * n_trips];

            let transfer_risk = if k == 0 || labels_rt[k - 1][bs].is_none() {
                None
            } else {
                let rt = labels_rt[k - 1][bs].unwrap();
                let arrival_at_bs = labels[k - 1][bs].earliest();
                let margin = board_dep as i32 - arrival_at_bs as i32;
                let next_departure =
                    self.next_active_trip_departure(trip_ids, t + 1, boarding_col, date, weekday);
                let (reliability, next_reliability) =
                    match self.raptor.transit_delay_models.get(&rt) {
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

            let route_id = self.raptor.transit_patterns[p].route;

            let mut steps = Vec::with_capacity(ap - bp);
            let mut total_length = 0usize;
            for s in (bp + 1)..=ap {
                let seg_len = self.nodes_distance(pat_stops[s - 1], pat_stops[s]);
                total_length += seg_len;

                let arr = times[s * n_trips + t].arrival;
                let prev_dep = times[(s - 1) * n_trips + t].departure;

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

                let departure_index = if s == bp + 1 {
                    self.raptor.transit_departures
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
                    place: crate::structures::plan::PlanPlace {
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

            let transit_geometry: Vec<crate::structures::plan::PlanCoordinate> =
                match self.get_pattern_shape(p) {
                    Some((shape_pts, stop_idx)) => {
                        let from = stop_idx[bp] as usize;
                        let to = stop_idx[ap] as usize;
                        shape_pts[from..=to]
                            .iter()
                            .map(|coord| crate::structures::plan::PlanCoordinate {
                                lat: coord.latitude,
                                lon: coord.longitude,
                            })
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

            stop = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
            k -= 1;
        }

        legs.reverse();

        (legs, stop)
    }

    /// Pass 3 of three-pass RAPTOR: tighten transit legs using backward labels.
    pub(super) fn tighten_with_backward_labels(
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

        let mut current_time: u32 = 0;

        for i in 0..k {
            let ti = transit_indices[i];
            let remaining = k - i - 1;

            let (boarding_node, alighting_node, leg_start) = match &legs[ti] {
                PlanLeg::Transit(t) => (t.from.node_id, t.to.node_id, t.start),
                _ => unreachable!(),
            };

            let alighting_compact = self.raptor.transit_node_to_stop[alighting_node.0];

            let max_alighting = if alighting_compact != u32::MAX && remaining < lambda.len() {
                lambda[remaining][alighting_compact as usize]
            } else {
                0
            };

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
                            std::iter::once((dep_idx, &self.raptor.transit_departures[dep_idx])),
                            1,
                        ) {
                            if let Some(new_leg) = alts.pop() {
                                legs[ti] = PlanLeg::Transit(new_leg);
                            }
                        }
                    }
                }
            }

            let new_leg_end =
                match &legs[ti] { PlanLeg::Transit(t) => t.end, _ => unreachable!() };

            if i < k - 1 {
                let next_ti = transit_indices[i + 1];

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
                current_time = cursor;

                if let PlanLeg::Transit(next_t) = &mut legs[next_ti] {
                    next_t.preceding_arrival = Some(cursor);
                    if let Some(prt) = next_t.preceding_route_type {
                        let margin = next_t.start as i32 - cursor as i32;
                        let next_dep =
                            next_t.transfer_risk.as_ref().and_then(|r| r.next_departure);
                        let (rel, next_rel) = match self.raptor.transit_delay_models.get(&prt) {
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
                let _ = walk_to_next;
            }
        }
    }

    /// Remove dominated plans from `plans`.
    ///
    /// Plan A dominates plan B when A is at least as good in all three dimensions
    /// (departure time, arrival time, transfer count) and strictly better in one.
    pub(super) fn pareto_filter(plans: Vec<Plan>) -> Vec<Plan> {
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
            for existing in &result {
                let tc_e = transfer_count(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start {
                    continue 'outer;
                }
            }
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

    /// Debug-aware pareto filter.
    ///
    /// `plan_to_sink_idx[i]` is the index of `plans[i]` in `sink`.
    /// Dominated plans have their `sink` entry updated with the dominator's index.
    pub(super) fn pareto_filter_with_debug(
        plans: Vec<Plan>,
        plan_to_sink_idx: &[usize],
        sink: &mut Vec<PlanCandidate>,
    ) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        let mut result: Vec<Plan> = Vec::new();
        let mut result_sink_idx: Vec<usize> = Vec::new();

        'outer: for (plan, &sink_idx) in plans.into_iter().zip(plan_to_sink_idx.iter()) {
            let tc_p = transfer_count(&plan);

            // Check if `plan` is dominated by any existing result.
            for (i, existing) in result.iter().enumerate() {
                let tc_e = transfer_count(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start {
                    sink[sink_idx].status = CandidateStatus::ParetoDominated {
                        dominator_index: result_sink_idx[i],
                    };
                    continue 'outer;
                }
            }

            // Mark result members dominated by `plan`.
            let mut dominated = vec![false; result.len()];
            for (i, existing) in result.iter().enumerate() {
                let tc_e = transfer_count(existing);
                if tc_p <= tc_e
                    && plan.end <= existing.end
                    && plan.start >= existing.start
                    && (tc_p < tc_e || plan.end < existing.end || plan.start > existing.start)
                {
                    dominated[i] = true;
                    sink[result_sink_idx[i]].status = CandidateStatus::ParetoDominated {
                        dominator_index: sink_idx,
                    };
                }
            }

            let (new_result, new_result_sink_idx): (Vec<Plan>, Vec<usize>) = result
                .into_iter()
                .zip(result_sink_idx.into_iter())
                .zip(dominated.iter())
                .filter_map(|((p, si), &dom)| if dom { None } else { Some((p, si)) })
                .unzip();
            result = new_result;
            result_sink_idx = new_result_sink_idx;

            result.push(plan);
            result_sink_idx.push(sink_idx);
        }

        result
    }
}
