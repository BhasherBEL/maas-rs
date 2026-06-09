use super::raptor_route::{Label, LabelSet};

use crate::{
    ingestion::gtfs::TimetableSegment,
    structures::{
        EdgeData, NodeID, RealtimeIndex, ReliabilityBuckets, ScenarioBag,
        plan::{
            ArrivalScenario, CandidateStatus, Plan, PlanCandidate, PlanLeg, PlanLegStep, PlanPlace,
            PlanTransitLeg, PlanTransitLegStep, PlanWalkLeg, PlanWalkLegStep, TransferRisk,
        },
    },
};

use super::Graph;

/// Apply a signed realtime delay (seconds) to a time, clamped at 0.
#[inline]
fn apply_signed_delay(t: u32, delay: i32) -> u32 {
    (t as i64 + delay as i64).max(0) as u32
}

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
            expected_end: end,
        }
    }

    const EXTREME_RISK_RELIABILITY: f32 = 0.10;
    const EXTREME_RISK_WAIT_SECS: u32 = 7200;
    const TIGHTEN_MIN_RELIABILITY: f32 = 0.80;

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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn extract_with_debug(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<LabelSet>],
        buckets: &ReliabilityBuckets,
        origin: NodeID,
        destination: NodeID,
        rt: &RealtimeIndex,
        mut debug_sink: Option<&mut Vec<PlanCandidate>>,
    ) -> Vec<Plan> {
        use super::MAX_ROUNDS;

        let mut candidates: Vec<Plan> = Vec::new();
        // Parallel to `candidates`: index of each candidate in `debug_sink`.
        // Populated even when debug_sink is None (dummy values) so the zip works.
        let mut sink_indices: Vec<usize> = Vec::new();
        // Best arrival seen so far per reliability bucket (cross-round pruning, the
        // multi-criteria analogue of the old single `pareto_best`).
        let n_buckets = buckets.bucket(1.0) as usize + 1;
        let mut bucket_best = vec![u32::MAX; n_buckets];

        for k in 0..=MAX_ROUNDS {
            // For this round, the earliest arrival (incl. egress walk) per bucket,
            // and which (stop, walk) achieves it.
            let mut per_bucket: Vec<Option<(u32, usize, u32)>> = vec![None; n_buckets];
            for &(s, w) in targets {
                for l in labels[k][s].iter() {
                    let b = buckets.bucket(l.reliability) as usize;
                    let arr = l.bag.earliest().saturating_add(w);
                    match per_bucket[b] {
                        Some((cur, ..)) if cur <= arr => {}
                        _ => per_bucket[b] = Some((arr, s, w)),
                    }
                }
            }

            for b in 0..n_buckets {
                let (best_arr, best_stop, best_walk) = match per_bucket[b] {
                    Some(t) => t,
                    None => continue,
                };

                if best_arr >= bucket_best[b] {
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
                bucket_best[b] = best_arr;

                let (mut legs, origin_stop) =
                    self.reconstruct(k, best_stop, b as u8, date, weekday, labels, buckets);

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

            // The destination-stop label this candidate was built from.
            let chosen = Self::pick_label(labels, buckets, k, best_stop, b as u8);
            let chosen_bag = chosen.map(|l| l.bag).unwrap_or(ScenarioBag::EMPTY);
            let chosen_rt = chosen.and_then(|l| l.route_type);

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

            // Realtime post-pass: shift leg times by live delays, re-chain the
            // timeline, and recompute transfer reliability on the new margins.
            self.apply_realtime(&mut legs, rt);

            // Record each transit leg's downstream connection *after* tighten and
            // realtime have settled the final scheduled times, so the outbound
            // margin used to score alternatives matches the leg's actual arrival.
            Self::link_following_connections(&mut legs);

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
                let walk_start = chosen_bag.earliest();
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

            let arrival_bag = chosen_bag.shifted_by(best_walk);
            let arrival_distribution: Vec<ArrivalScenario> =
                match chosen_rt.and_then(|rt| self.raptor.transit_delay_models.get(&rt)) {
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

            let expected_end = arrival_distribution
                .iter()
                .map(|s| s.time as f64 * s.probability as f64)
                .sum::<f64>() as u32;
            let plan = Plan {
                legs: Self::merge_consecutive_walks(legs),
                start: departure,
                end: best_arr,
                arrival_distribution,
                expected_end,
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
        }

        if candidates.iter().any(|p| !Self::is_extreme_risk(p)) {
            let mut new_candidates = Vec::new();
            for (plan, si) in candidates.into_iter().zip(sink_indices.into_iter()) {
                if Self::is_extreme_risk(&plan) {
                    if let Some(ref mut sink) = debug_sink {
                        sink[si].status = CandidateStatus::ExtremeRisk;
                    }
                } else {
                    new_candidates.push(plan);
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

    /// Picks the label in bucket `b` at `(k, stop)`, falling back to the fastest label.
    fn pick_label<'a>(
        labels: &'a [Vec<LabelSet>],
        buckets: &ReliabilityBuckets,
        k: usize,
        stop: usize,
        b: u8,
    ) -> Option<&'a Label> {
        labels[k][stop]
            .get_by_bucket(buckets, b)
            .or_else(|| labels[k][stop].min_arrival_label())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reconstruct(
        &self,
        round: usize,
        target_stop: usize,
        start_bucket: u8,
        date: u32,
        weekday: u8,
        labels: &[Vec<LabelSet>],
        buckets: &ReliabilityBuckets,
    ) -> (Vec<PlanLeg>, usize) {
        let mut legs = Vec::new();
        let mut stop = target_stop;
        let mut k = round;
        let mut cur_bucket = start_bucket;

        loop {
            let trace = match Self::pick_label(labels, buckets, k, stop, cur_bucket) {
                Some(l) => l.trace,
                None => break,
            };
            if !trace.is_transit() && !trace.is_transfer() {
                break;
            }

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let start = Self::pick_label(labels, buckets, k, from, trace.from_bucket)
                    .map(|l| l.bag.earliest())
                    .unwrap_or(0);
                let end = Self::pick_label(labels, buckets, k, stop, cur_bucket)
                    .map(|l| l.bag.earliest())
                    .unwrap_or(start);
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
                cur_bucket = trace.from_bucket;
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

            // Predecessor label this leg boarded from (its bucket recorded in the trace).
            let preceding = if k == 0 {
                None
            } else {
                Self::pick_label(labels, buckets, k - 1, bs, trace.from_bucket)
            };
            let preceding_rt = preceding.and_then(|l| l.route_type);
            let preceding_arr = preceding.map(|l| l.bag.earliest());

            let transfer_risk = if preceding_rt.is_none() {
                None
            } else {
                let rt = preceding_rt.unwrap();
                let arrival_at_bs = preceding_arr.unwrap();
                let margin = board_dep as i32 - arrival_at_bs as i32;
                let next_departure =
                    self.next_active_trip_departure(trip_ids, t + 1, boarding_col, date, weekday);
                let board = self
                    .route_type_of_trip(trip_ids[t])
                    .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                let (reliability, next_reliability) =
                    match self.raptor.transit_delay_models.get(&rt) {
                        Some(cdf) => (
                            cdf.prob_on_time_vs(board, margin),
                            next_departure.map(|nd| {
                                cdf.prob_on_time_vs(board, nd as i32 - arrival_at_bs as i32)
                            }),
                        ),
                        None => (1.0, None),
                    };
                Some(TransferRisk {
                    reliability,
                    scheduled_departure: board_dep,
                    next_departure,
                    next_reliability,
                    margin_secs: Some(margin),
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
                scheduled_start: board_dep,
                scheduled_end: alight_arr,
                realtime: false,
                trip_id: trip_ids[t],
                length: total_length,
                duration: alight_arr - board_dep,
                steps,
                geometry: transit_geometry,
                transfer_risk,
                preceding_arrival: if preceding_rt.is_none() { None } else { preceding_arr },
                preceding_route_type: preceding_rt,
                route_type: self.route_type_of_trip(trip_ids[t]),
                // Populated by `link_following_connections` once the legs are in
                // forward order (the next transit leg isn't known yet here).
                following_route_type: None,
                following_margin_secs: None,
                bikes_allowed: self.get_trip(trip_ids[t]).and_then(|t| t.bikes_allowed),
            }));

            stop = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
            cur_bucket = trace.from_bucket;
            k -= 1;
        }

        legs.reverse();

        (legs, stop)
    }

    /// Fills `following_route_type` / `following_margin_secs` on each transit leg
    /// from the next transit leg in the (forward-ordered) chain. The margin is the
    /// scheduled outbound slack: next boarding − this leg's scheduled arrival −
    /// intervening transfer walk. Last transit leg keeps `None` (no connection to
    /// make). Operates on the transit/transfer chain only — access/egress walks are
    /// attached later and never follow a transit leg here.
    fn link_following_connections(legs: &mut [PlanLeg]) {
        // (index, scheduled_start, route_type) of each transit leg, in order.
        let transit: Vec<(usize, u32, Option<gtfs_structures::RouteType>)> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| match l {
                PlanLeg::Transit(t) => Some((i, t.scheduled_start, t.route_type)),
                _ => None,
            })
            .collect();

        for w in transit.windows(2) {
            let (i, _, _) = w[0];
            let (j, next_start, next_rt) = w[1];
            // Sum any transfer-walk durations sitting between the two transit legs.
            let walk: u32 = legs[i + 1..j]
                .iter()
                .map(|l| match l {
                    PlanLeg::Walk(wk) => wk.duration,
                    _ => 0,
                })
                .sum();
            if let PlanLeg::Transit(t) = &mut legs[i] {
                t.following_route_type = next_rt;
                t.following_margin_secs =
                    Some(next_start as i32 - t.scheduled_end as i32 - walk as i32);
            }
        }
    }

    /// Realtime post-pass: rewrite each transit leg's times from scheduled to
    /// effective (scheduled + live delay), re-chain the whole timeline, and
    /// recompute transfer reliability on the new margins. Walks between legs
    /// follow the (possibly delayed) preceding arrival. With an empty index this
    /// is a no-op, so schedule-only behaviour is preserved exactly.
    ///
    /// Runs *before* the access/egress walks are attached, so `legs` here is the
    /// transit/transfer chain only; `cursor` is the running effective arrival.
    pub(super) fn apply_realtime(&self, legs: &mut [PlanLeg], rt: &RealtimeIndex) {
        if rt.is_empty() {
            return;
        }
        let compact = |node: NodeID| -> Option<u32> {
            let c = self.raptor.transit_node_to_stop[node.0];
            if c == u32::MAX { None } else { Some(c) }
        };

        let mut cursor: Option<u32> = None;
        for leg in legs.iter_mut() {
            match leg {
                PlanLeg::Transit(t) => {
                    let board = compact(t.from.node_id);
                    let alight = compact(t.to.node_id);
                    let d_board = board.map_or(0, |s| rt.delay(t.trip_id, s));
                    let d_alight = alight.map_or(0, |s| rt.delay(t.trip_id, s));
                    let has_rt = board.is_some_and(|s| rt.delay_opt(t.trip_id, s).is_some())
                        || alight.is_some_and(|s| rt.delay_opt(t.trip_id, s).is_some());

                    t.scheduled_start = t.start;
                    t.scheduled_end = t.end;
                    t.start = apply_signed_delay(t.start, d_board);
                    t.end = apply_signed_delay(t.end, d_alight);
                    t.realtime = has_rt;
                    t.duration = t.end.saturating_sub(t.start);
                    t.from.departure = Some(t.start);
                    t.to.arrival = Some(t.end);

                    for step in t.steps.iter_mut() {
                        if let PlanLegStep::Transit(s) = step {
                            if let Some(sc) = compact(s.place.node_id) {
                                let d = rt.delay(t.trip_id, sc);
                                s.place.arrival = s.place.arrival.map(|a| apply_signed_delay(a, d));
                                s.place.departure = s.place.departure.map(|x| apply_signed_delay(x, d));
                            }
                        }
                    }

                    // Recompute the transfer onto this leg from the realtime arrival.
                    if let (Some(prev_arr), Some(prt)) = (cursor, t.preceding_route_type) {
                        let margin = t.start as i32 - prev_arr as i32;
                        let next_dep = t.transfer_risk.as_ref().and_then(|r| r.next_departure);
                        let board = t
                            .route_type
                            .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                        let (rel, next_rel) = match self.raptor.transit_delay_models.get(&prt) {
                            Some(cdf) => (
                                cdf.prob_on_time_vs(board, margin),
                                next_dep
                                    .map(|nd| cdf.prob_on_time_vs(board, nd as i32 - prev_arr as i32)),
                            ),
                            None => (1.0, None),
                        };
                        t.preceding_arrival = Some(prev_arr);
                        t.transfer_risk = Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: t.scheduled_start,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                            margin_secs: Some(margin),
                        });
                    }
                    cursor = Some(t.end);
                }
                PlanLeg::Walk(w) => {
                    if let Some(prev) = cursor {
                        let dur = w.duration;
                        w.start = prev;
                        w.end = prev + dur;
                        w.from.departure = Some(w.start);
                        w.to.arrival = Some(w.end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(w.end);
                            }
                        }
                        cursor = Some(w.end);
                    }
                }
            }
        }
    }

    /// Pass 3 of three-pass RAPTOR: tighten transit legs using backward labels.
    fn reliability_capped_alighting(
        &self,
        feeder_rt: Option<gtfs_structures::RouteType>,
        board_rt: Option<gtfs_structures::RouteType>,
        walk_to_next: u32,
        next_start: u32,
        max_alighting: u32,
    ) -> u32 {
        if feeder_rt
            .and_then(|rt| self.raptor.transit_delay_models.get(&rt))
            .is_none()
        {
            return max_alighting;
        }
        let reliable = |alight: u32| {
            self.transfer_on_time_prob(
                feeder_rt,
                board_rt,
                alight.saturating_add(walk_to_next),
                next_start,
            ) >= Self::TIGHTEN_MIN_RELIABILITY
        };
        if reliable(max_alighting) {
            return max_alighting;
        }
        if !reliable(0) {
            return 0;
        }
        let mut lo = 0u32;
        let mut hi = max_alighting;
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if reliable(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }

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

            let max_alighting = if i < k - 1 && max_alighting > 0 {
                let next_ti = transit_indices[i + 1];
                let (next_start, next_rt) = match &legs[next_ti] {
                    PlanLeg::Transit(t) => (t.start, t.route_type),
                    _ => unreachable!(),
                };
                let feeder_rt = match &legs[ti] {
                    PlanLeg::Transit(t) => t.route_type,
                    _ => unreachable!(),
                };
                self.reliability_capped_alighting(
                    feeder_rt, next_rt, walk_to_next, next_start, max_alighting,
                )
            } else {
                max_alighting
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
                        let board = next_t
                            .route_type
                            .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                        let (rel, next_rel) = match self.raptor.transit_delay_models.get(&prt) {
                            Some(cdf) => (
                                cdf.prob_on_time_vs(board, margin),
                                next_dep.map(|nd| {
                                    cdf.prob_on_time_vs(board, nd as i32 - cursor as i32)
                                }),
                            ),
                            None => (1.0, None),
                        };
                        next_t.transfer_risk = Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: next_t.start,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                            margin_secs: Some(margin),
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
    /// Plan A dominates plan B when A is at least as good in all four dimensions
    /// (departure time, arrival time, transfer count, walking duration) and strictly
    /// better in at least one.
    /// Plan reliability = product of each transit leg's `transfer_risk.reliability`
    /// (legs without a risk count as 1.0). Walk-only plans = 1.0.
    pub(super) fn plan_reliability(plan: &Plan) -> f32 {
        plan.legs
            .iter()
            .filter_map(|l| {
                if let PlanLeg::Transit(t) = l {
                    t.transfer_risk.as_ref().map(|r| r.reliability)
                } else {
                    None
                }
            })
            .product::<f32>()
    }

    pub(super) fn pareto_filter(plans: Vec<Plan>, buckets: &ReliabilityBuckets) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        fn walk_secs(plan: &Plan) -> u32 {
            plan.legs.iter().filter_map(|l| {
                if let PlanLeg::Walk(w) = l { Some(w.duration) } else { None }
            }).sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        let mut result: Vec<Plan> = Vec::new();

        'outer: for plan in plans {
            let tc_p = transfer_count(&plan);
            let ws_p = walk_secs(&plan);
            let rb_p = rel_bucket(&plan);
            for existing in &result {
                let tc_e = transfer_count(existing);
                let ws_e = walk_secs(existing);
                let rb_e = rel_bucket(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start && ws_e <= ws_p && rb_e >= rb_p {
                    continue 'outer;
                }
            }
            result.retain(|existing| {
                let tc_e = transfer_count(existing);
                let ws_e = walk_secs(existing);
                let rb_e = rel_bucket(existing);
                !(tc_p <= tc_e
                    && plan.end <= existing.end
                    && plan.start >= existing.start
                    && ws_p <= ws_e
                    && rb_p >= rb_e
                    && (tc_p < tc_e || plan.end < existing.end || plan.start > existing.start || ws_p < ws_e || rb_p > rb_e))
            });
            result.push(plan);
        }

        result.sort_by(|a, b| {
            a.end
                .cmp(&b.end)
                .then(b.start.cmp(&a.start))
                .then(rel_bucket(b).cmp(&rel_bucket(a)))
        });
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
        buckets: &ReliabilityBuckets,
    ) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        fn walk_secs(plan: &Plan) -> u32 {
            plan.legs.iter().filter_map(|l| {
                if let PlanLeg::Walk(w) = l { Some(w.duration) } else { None }
            }).sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        let mut result: Vec<Plan> = Vec::new();
        let mut result_sink_idx: Vec<usize> = Vec::new();

        'outer: for (plan, &sink_idx) in plans.into_iter().zip(plan_to_sink_idx.iter()) {
            let tc_p = transfer_count(&plan);
            let ws_p = walk_secs(&plan);
            let rb_p = rel_bucket(&plan);

            // Check if `plan` is dominated by any existing result.
            for (i, existing) in result.iter().enumerate() {
                let tc_e = transfer_count(existing);
                let ws_e = walk_secs(existing);
                let rb_e = rel_bucket(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start && ws_e <= ws_p && rb_e >= rb_p {
                    sink[sink_idx].status = CandidateStatus::ParetoDominated {
                        dominator_index: result_sink_idx[i],
                        departure_worse: existing.start > plan.start,
                        arrival_worse: existing.end < plan.end,
                        transfers_worse: tc_e < tc_p,
                        reliability_worse: rb_e > rb_p,
                    };
                    continue 'outer;
                }
            }

            // Mark result members dominated by `plan`.
            let mut dominated = vec![false; result.len()];
            for (i, existing) in result.iter().enumerate() {
                let tc_e = transfer_count(existing);
                let ws_e = walk_secs(existing);
                let rb_e = rel_bucket(existing);
                if tc_p <= tc_e
                    && plan.end <= existing.end
                    && plan.start >= existing.start
                    && ws_p <= ws_e
                    && rb_p >= rb_e
                    && (tc_p < tc_e || plan.end < existing.end || plan.start > existing.start || ws_p < ws_e || rb_p > rb_e)
                {
                    dominated[i] = true;
                    sink[result_sink_idx[i]].status = CandidateStatus::ParetoDominated {
                        dominator_index: sink_idx,
                        departure_worse: plan.start > existing.start,
                        arrival_worse: plan.end < existing.end,
                        transfers_worse: tc_p < tc_e,
                        reliability_worse: rb_p > rb_e,
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

        result.sort_by(|a, b| {
            a.end
                .cmp(&b.end)
                .then(b.start.cmp(&a.start))
                .then(rel_bucket(b).cmp(&rel_bucket(a)))
        });
        result
    }
}
