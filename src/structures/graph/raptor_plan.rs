use super::raptor_route::{Label, LabelSet, ModeContext};

use crate::{
    ingestion::gtfs::TimetableSegment,
    structures::{
        EdgeData, Mode, NodeID, RealtimeIndex, ReliabilityBuckets, Scenario, ScenarioBag,
        VehicleState,
        delay::DelayCDF,
        plan::{
            AccessAlternative, ArrivalScenario, CandidateStatus, Plan, PlanCandidate, PlanLeg,
            PlanLegStep, PlanPlace, PlanTransitLeg, PlanTransitLegStep, PlanWalkLeg,
            PlanWalkLegStep, TransferRisk,
        },
    },
};

use super::{Graph, raptor_access::StreetProfile};

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
        self.build_street_plan(
            origin,
            destination,
            start_time,
            walk_secs,
            StreetProfile::Foot,
        )
    }

    /// Direct street-only plan (the whole journey walked or ridden).
    pub(super) fn build_street_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        secs: u32,
        profile: StreetProfile,
    ) -> Plan {
        let end = start_time + secs;
        let (speed, mode) = match profile {
            StreetProfile::Foot => (self.raptor.walking_speed_mps, Mode::Walk),
            StreetProfile::Bike => (self.raptor.cycling_speed_mps, Mode::Bike),
            StreetProfile::Car => (self.raptor.driving_speed_mps, Mode::Car),
        };
        let length = (secs as f64 * speed) as usize;

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
                duration: secs,
                length,
                cycleroute_length: None,
                elevation_gain: None,
                street_mode: mode,
                steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                    length, secs, to_place,
                ))],
                geometry: self.street_path(origin, destination, profile),
            })],
            start: start_time,
            end,
            mode,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
        }
    }

    /// Cost-routed direct bike plan: geometry follows the minimum-cost route and
    /// the duration is its accumulated kinematic time. Returns `None` if the
    /// destination is unreachable within `max_secs`.
    pub(super) fn build_bike_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        max_secs: u32,
        bike: &crate::structures::BikeCost,
    ) -> Option<Plan> {
        let p = self.bike_cost_path(origin, destination, max_secs, bike)?;
        let end = start_time + p.secs;
        let geometry: Vec<_> = p.nodes.iter().map(|&n| self.node_coord(n)).collect();
        let to_place = PlanPlace {
            node_id: destination,
            stop_position: None,
            arrival: Some(end),
            departure: None,
        };

        // Group consecutive edges by ride/push into steps so the client can show
        // (and time) dismount stretches distinctly. Each step covers the inclusive
        // geometry range [start_idx, i].
        let mut steps: Vec<PlanLegStep> = Vec::new();
        let mut i = 0;
        let mut cum_time = 0u32;
        while i < p.edges.len() {
            let push = p.edges[i].push;
            let start_idx = i;
            let (mut run_len, mut run_time) = (0usize, 0u32);
            while i < p.edges.len() && p.edges[i].push == push {
                run_len += p.edges[i].length;
                run_time += p.edges[i].time;
                i += 1;
            }
            cum_time += run_time;
            steps.push(PlanLegStep::Walk(PlanWalkLegStep {
                length: run_len,
                time: run_time,
                place: PlanPlace {
                    node_id: p.nodes[i],
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
            steps.push(PlanLegStep::Walk(PlanWalkLegStep::plain(
                p.length, p.secs, to_place,
            )));
        }

        Some(Plan {
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
                duration: p.secs,
                length: p.length,
                cycleroute_length: Some(p.cycleroute_length),
                elevation_gain: Some(p.ascent),
                street_mode: Mode::Bike,
                steps,
                geometry,
            })],
            start: start_time,
            end,
            mode: Mode::Bike,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
        })
    }

    const EXTREME_RISK_RELIABILITY: f32 = 0.10;
    const EXTREME_RISK_WAIT_SECS: u32 = 7200;
    const TIGHTEN_MIN_RELIABILITY: f32 = 0.80;

    pub(super) fn is_extreme_risk(plan: &Plan) -> bool {
        plan.legs.iter().any(|leg| {
            if let PlanLeg::Transit(t) = leg
                && let Some(ref risk) = t.transfer_risk
                && risk.reliability < Self::EXTREME_RISK_RELIABILITY
            {
                let wait = risk
                    .next_departure
                    .map(|nd| nd.saturating_sub(risk.scheduled_departure))
                    .unwrap_or(u32::MAX);
                return wait > Self::EXTREME_RISK_WAIT_SECS;
            }
            false
        })
    }

    /// Arrival distribution and expected arrival from a scenario bag.
    ///
    /// Scenarios with `time == u32::MAX` mean "connection missed, no later trip
    /// today": they carry probability but no finite arrival, so they are excluded
    /// from the published distribution *before* the delay-CDF convolution (which
    /// would otherwise shift the sentinel to a bogus finite time). `expected_end`
    /// is the expectation conditioned on actually arriving; when no scenario is
    /// reachable it falls back to `fallback_end`.
    pub(crate) fn arrival_stats(
        bag: &ScenarioBag,
        cdf: Option<&DelayCDF>,
        fallback_end: u32,
    ) -> (Vec<ArrivalScenario>, u32) {
        let reachable: Vec<Scenario> = bag
            .scenarios()
            .iter()
            .copied()
            .filter(|s| s.time != u32::MAX)
            .collect();

        let dist: Vec<ArrivalScenario> = match cdf {
            Some(cdf) if !cdf.bins.is_empty() => {
                let mut dist = Vec::with_capacity(reachable.len() * cdf.bins.len());
                let mut prev_cum = 0.0f32;
                for &(delay, cum_prob) in &cdf.bins {
                    let bin_mass = cum_prob - prev_cum;
                    if bin_mass > 0.0 {
                        for s in &reachable {
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
            _ => reachable
                .iter()
                .map(|s| ArrivalScenario {
                    time: s.time,
                    probability: s.prob,
                })
                .collect(),
        };

        let mass: f64 = dist.iter().map(|s| s.probability as f64).sum();
        let expected_end = if mass > 0.0 {
            (dist
                .iter()
                .map(|s| s.time as f64 * s.probability as f64)
                .sum::<f64>()
                / mass) as u32
        } else {
            fallback_end
        };
        (dist, expected_end)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn extract_with_debug(
        &self,
        mc: &ModeContext,
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<LabelSet>],
        buckets: &ReliabilityBuckets,
        origin: NodeID,
        destination: NodeID,
        rt: &RealtimeIndex,
        mut debug_sink: Option<&mut Vec<PlanCandidate>>,
        departure_stamp: u32,
        arena: &[Label],
    ) -> Vec<Plan> {
        use super::MAX_ROUNDS;

        let n_states = mc.n_states();
        // Mode class: walk-rooted plans must not be suppressed by faster
        // bike-state arrivals — the burden comparison happens at plan level.
        let class_of = |vs: VehicleState| -> usize {
            match vs {
                VehicleState::Walked => 0,
                VehicleState::BikeInHand | VehicleState::BikeDropped => 1,
                VehicleState::CarParked | VehicleState::CarEgress => 2,
            }
        };
        let n_classes = 3;

        let mut candidates: Vec<Plan> = Vec::new();
        // Parallel to `candidates`: index of each candidate in `debug_sink`.
        // Populated even when debug_sink is None (dummy values) so the zip works.
        let mut sink_indices: Vec<usize> = Vec::new();
        // Best arrival seen so far per (mode class, reliability bucket) — the
        // cross-round pruning, the multi-criteria analogue of the old single
        // `pareto_best`.
        let n_buckets = buckets.bucket(1.0) as usize + 1;
        let n_keys = n_classes * n_buckets;
        let mut bucket_best = vec![u32::MAX; n_keys];

        for k in 0..=MAX_ROUNDS {
            // For this round, the earliest arrival (incl. egress walk/ride) per
            // (class, bucket), and which (stop, walk, state) achieves it.
            let mut per_key: Vec<Option<(u32, usize, u32, usize)>> = vec![None; n_keys];
            for (sidx, vs) in mc.am.states() {
                let class = class_of(vs);
                for &(s, w) in &mc.egress[sidx] {
                    for l in labels[k][s * n_states + sidx].iter() {
                        if l.created_by != departure_stamp {
                            continue;
                        }
                        let b = buckets.bucket(l.reliability) as usize;
                        let key = class * n_buckets + b;
                        let arr = l.bag.earliest().saturating_add(w);
                        match per_key[key] {
                            Some((cur, ..)) if cur <= arr => {}
                            _ => per_key[key] = Some((arr, s, w, sidx)),
                        }
                    }
                }
            }

            for key in 0..n_keys {
                let b = key % n_buckets;
                let (best_arr, best_stop, best_walk, dest_sidx) = match per_key[key] {
                    Some(t) => t,
                    None => continue,
                };

                if best_arr >= bucket_best[key] {
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
                bucket_best[key] = best_arr;

                // The destination-stop label this candidate was built from; its arena
                // chain is the EXACT journey (no grid re-lookup → no bucket drift).
                let cell = best_stop * n_states + dest_sidx;
                let chosen = Self::pick_label(&labels[k][cell], buckets, b as u8, departure_stamp);
                let chosen_bag = chosen.map(|l| l.bag).unwrap_or(ScenarioBag::EMPTY);
                let chosen_rt = chosen.and_then(|l| l.route_type);

                let (mut legs, origin_stop, root_state) = match chosen {
                    Some(l) => self.reconstruct(arena, l.arena_id, date, weekday),
                    None => (Vec::new(), best_stop, 0),
                };

                let root_vs = mc.am.state_at(root_state as usize);
                let dest_vs = mc.am.state_at(dest_sidx);
                let mode = match root_vs {
                    VehicleState::Walked => Mode::WalkTransit,
                    // Car states never transition, so the root state names the mode:
                    // CarParked = drove & parked (park & ride); CarEgress = picked
                    // up by car at the destination station (kiss & ride).
                    VehicleState::CarParked => Mode::CarDropOff,
                    VehicleState::CarEgress => Mode::CarPickup,
                    // Bike-rooted: the egress state tells park-and-ride apart from
                    // carry-on-board. BikeInHand egress = bike ridden to the
                    // destination; BikeDropped egress = parked at the station and
                    // walked. The explicit BikeToTransit label takes precedence
                    // over the BikeTransit union when selected.
                    VehicleState::BikeInHand | VehicleState::BikeDropped => match dest_vs {
                        VehicleState::BikeInHand if mc.am.selected(Mode::BikeOnTransit) => {
                            Mode::BikeOnTransit
                        }
                        VehicleState::BikeInHand => Mode::BikeTransit,
                        _ if mc.am.selected(Mode::BikeToTransit) => Mode::BikeToTransit,
                        _ => Mode::BikeTransit,
                    },
                };

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

                let transit_count = legs
                    .iter()
                    .filter(|l| matches!(l, PlanLeg::Transit(_)))
                    .count();
                // A transit-mode candidate must actually use transit. With a wide
                // vehicle access radius the search can reach the destination via
                // access + transfer + egress with zero transit legs — a degenerate
                // direct ride that also dodges the direct-duration filter (it looks
                // "direct"). Direct rides are emitted by the direct-plan machinery, so
                // drop any zero-transit candidate here.
                if transit_count == 0 {
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
                // The backward pass is bike-unaware (it would re-board trips a carried
                // bike is not allowed on), so only walk-rooted plans are tightened.
                if transit_count > 0 && mode == Mode::WalkTransit {
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

                let (access_profile, access_mode) = match root_vs {
                    VehicleState::Walked | VehicleState::CarEgress => {
                        (StreetProfile::Foot, Mode::Walk)
                    }
                    VehicleState::BikeInHand | VehicleState::BikeDropped => {
                        (StreetProfile::Bike, Mode::Bike)
                    }
                    VehicleState::CarParked => (StreetProfile::Car, Mode::Car),
                };
                if let Some(&(_, first_walk)) = mc.access[root_state as usize]
                    .iter()
                    .find(|&&(s, _)| s == origin_stop)
                {
                    if first_walk > 0 {
                        let stop_node = self.raptor.transit_stop_to_node[origin_stop];
                        let speed = match access_profile {
                            StreetProfile::Foot => self.raptor.walking_speed_mps,
                            StreetProfile::Bike => self.raptor.cycling_speed_mps,
                            StreetProfile::Car => self.raptor.driving_speed_mps,
                        };
                        let length = (first_walk as f64 * speed) as usize;
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
                                cycleroute_length: None,
                elevation_gain: None,
                                street_mode: access_mode,
                                steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                                    length, first_walk, to_place,
                                ))],
                                geometry: self.street_path(origin, stop_node, access_profile),
                            }),
                        );
                    }
                }

                if best_walk > 0 {
                    let (egress_profile, egress_mode) = match dest_vs {
                        VehicleState::BikeInHand => (StreetProfile::Bike, Mode::Bike),
                        VehicleState::CarEgress => (StreetProfile::Car, Mode::Car),
                        _ => (StreetProfile::Foot, Mode::Walk),
                    };
                    let walk_start = chosen_bag.earliest();
                    let stop_node = self.raptor.transit_stop_to_node[best_stop];
                    let speed = match egress_profile {
                        StreetProfile::Foot => self.raptor.walking_speed_mps,
                        StreetProfile::Bike => self.raptor.cycling_speed_mps,
                        StreetProfile::Car => self.raptor.driving_speed_mps,
                    };
                    let length = (best_walk as f64 * speed) as usize;
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
                        street_mode: egress_mode,
                        cycleroute_length: None,
                elevation_gain: None,
                        steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                            length, best_walk, to_place,
                        ))],
                        geometry: self.street_path(stop_node, destination, egress_profile),
                    }));
                }

                // Re-chain trailing walks onto the realtime-settled legs and read the
                // plan bounds off the final timeline, so a live delay can never leave
                // `end` lagging behind `start` (the historical negative-duration bug).
                let (departure, arrival) = Self::plan_timeline(&mut legs);

                let arrival_bag = chosen_bag.shifted_by(best_walk);
                let (arrival_distribution, expected_end) = Self::arrival_stats(
                    &arrival_bag,
                    chosen_rt.and_then(|rt| self.raptor.transit_delay_models.get(&rt)),
                    arrival,
                );
                // The expected (mean) arrival must never precede the deterministic
                // realtime arrival the legs actually show.
                let expected_end = expected_end.max(arrival);
                let plan = Plan {
                    legs: Self::merge_consecutive_walks(legs),
                    start: departure,
                    end: arrival,
                    mode,
                    access_alternatives: vec![],
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
            for (plan, si) in candidates.into_iter().zip(sink_indices) {
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

    /// Re-chains any walk leg that *follows* another leg onto that leg's end
    /// (preserving its duration), then returns `(plan.start, plan.end)` read off
    /// the final legs. The first leg is the anchor and is never shifted.
    ///
    /// The realtime post-pass (`apply_realtime`) settles the transit/transfer
    /// chain, but the egress walk is attached afterwards from the *scheduled*
    /// alight time. Under a live delay this leaves the egress (and the plan's
    /// `end`) lagging the realtime arrival, which can make the displayed
    /// duration negative. Re-chaining trailing walks here keeps the timeline
    /// monotonic; for schedule-only plans every leg already chains, so it is a
    /// no-op.
    pub(super) fn plan_timeline(legs: &mut [PlanLeg]) -> (u32, u32) {
        let mut cursor: Option<u32> = None;
        for leg in legs.iter_mut() {
            match leg {
                PlanLeg::Walk(w) => {
                    if let Some(prev_end) = cursor {
                        let dur = w.duration;
                        w.start = prev_end;
                        w.end = prev_end + dur;
                        w.from.departure = Some(w.start);
                        w.to.arrival = Some(w.end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(w.end);
                            }
                        }
                    }
                    cursor = Some(w.end);
                }
                PlanLeg::Transit(t) => cursor = Some(t.end),
            }
        }
        let leg_start = |l: &PlanLeg| match l {
            PlanLeg::Walk(w) => w.start,
            PlanLeg::Transit(t) => t.start,
        };
        let leg_end = |l: &PlanLeg| match l {
            PlanLeg::Walk(w) => w.end,
            PlanLeg::Transit(t) => t.end,
        };
        let start = legs.first().map(leg_start).unwrap_or(0);
        let end = legs.last().map(leg_end).unwrap_or(start);
        (start, end)
    }

    /// Merge any two consecutive `PlanLeg::Walk` segments into one.
    pub(super) fn merge_consecutive_walks(legs: Vec<PlanLeg>) -> Vec<PlanLeg> {
        let mut out: Vec<PlanLeg> = Vec::with_capacity(legs.len());
        for leg in legs {
            match (out.last_mut(), &leg) {
                (Some(PlanLeg::Walk(prev)), PlanLeg::Walk(next))
                    if prev.street_mode == next.street_mode =>
                {
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
                    let step = PlanLegStep::Walk(PlanWalkLegStep::plain(
                        new_length,
                        new_duration,
                        to,
                    ));
                    *prev = PlanWalkLeg {
                        from: prev.from,
                        to,
                        start: prev.start,
                        end: new_end,
                        duration: new_duration,
                        length: new_length,
                        cycleroute_length: None,
                elevation_gain: None,
                        street_mode: prev.street_mode,
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
    /// Selects the reconstruction label at `(k, stop)` for bucket `b`, considering
    /// only labels created by departure `stamp`. Single-pass queries stamp every
    /// label `0` and pass `stamp = 0`, so the filter is a no-op there; the range
    /// driver passes the current departure index so reconstruction follows only
    /// that departure's traces through the grid shared across departures.
    fn pick_label<'a>(
        set: &'a LabelSet,
        buckets: &ReliabilityBuckets,
        b: u8,
        stamp: u32,
    ) -> Option<&'a Label> {
        set.iter()
            .find(|l| l.created_by == stamp && buckets.bucket(l.reliability) == b)
            .or_else(|| {
                set.iter()
                    .filter(|l| l.created_by == stamp)
                    .min_by_key(|l| l.bag.earliest())
            })
    }

    /// Rebuilds the ordered legs of a journey by following EXACT parent pointers
    /// through the per-pass `arena` from the destination label `start_id`. Unlike the
    /// old grid re-lookup (which re-found predecessors by `(round, stop, bucket)` and
    /// could drift to an overwritten label), this reproduces the precise trips the
    /// search used, so the reconstructed reliability matches the search reliability.
    pub(super) fn reconstruct(
        &self,
        arena: &[Label],
        start_id: u32,
        date: u32,
        weekday: u8,
    ) -> (Vec<PlanLeg>, usize, u8) {
        let mut legs = Vec::new();
        let mut origin_stop = 0usize;
        let mut cur = start_id;

        while cur != u32::MAX {
            let node = &arena[cur as usize];
            let trace = node.trace;
            if !trace.is_transit() && !trace.is_transfer() {
                break; // reached the source / root
            }
            let parent = node.parent;
            let parent_node = if parent != u32::MAX {
                Some(&arena[parent as usize])
            } else {
                None
            };

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let to = node.at_stop as usize;
                let start = parent_node.map(|l| l.bag.earliest()).unwrap_or(0);
                let end = node.bag.earliest();
                let duration = end.saturating_sub(start);
                let from_node = self.raptor.transit_stop_to_node[from];
                let to_node = self.raptor.transit_stop_to_node[to];
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
                    cycleroute_length: None,
                elevation_gain: None,
                    street_mode: Mode::Walk,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                        length, duration, to_place,
                    ))],
                    geometry: self.walk_path(from_node, to_node),
                }));
                origin_stop = from;
                cur = parent;
                continue;
            }

            let p = trace.pattern as usize;
            let t = trace.trip as usize;
            let bp = trace.boarded_at as usize;
            let ap = trace.alighted_at as usize;

            let pat_stops =
                self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);
            let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
            let times = self.raptor.transit_idx_pattern_stop_times[p]
                .of(&self.raptor.transit_pattern_stop_times);
            let trip_ids =
                self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let bs = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
            let boarding_col = &times[bp * n_trips..(bp + 1) * n_trips];

            // EXACT predecessor label this leg boarded from (the arena parent).
            let preceding_rt = parent_node.and_then(|l| l.route_type);
            let preceding_arr = parent_node.map(|l| l.bag.earliest());

            let transfer_risk = if let (Some(rt), Some(arrival_at_bs)) =
                (preceding_rt, preceding_arr)
            {
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
            } else {
                None
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
                preceding_arrival: if preceding_rt.is_none() {
                    None
                } else {
                    preceding_arr
                },
                preceding_route_type: preceding_rt,
                route_type: self.route_type_of_trip(trip_ids[t]),
                // Populated by `link_following_connections` once the legs are in
                // forward order (the next transit leg isn't known yet here).
                following_route_type: None,
                following_margin_secs: None,
                bikes_allowed: self.get_trip(trip_ids[t]).and_then(|t| t.bikes_allowed),
                time_shift: 0,
            }));

            origin_stop = bs;
            cur = parent;
        }

        legs.reverse();

        // `cur` is now the source/root label this journey was seeded from; its
        // state identifies the access profile (walk vs bike).
        let root_state = arena.get(cur as usize).map(|l| l.state).unwrap_or(0);

        (legs, origin_stop, root_state)
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
                        if let PlanLegStep::Transit(s) = step
                            && let Some(sc) = compact(s.place.node_id)
                        {
                            let d = rt.delay(t.trip_id, sc);
                            s.place.arrival = s.place.arrival.map(|a| apply_signed_delay(a, d));
                            s.place.departure = s.place.departure.map(|x| apply_signed_delay(x, d));
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
                                next_dep.map(|nd| {
                                    cdf.prob_on_time_vs(board, nd as i32 - prev_arr as i32)
                                }),
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
        legs: &mut [PlanLeg],
        lambda: &[Vec<u32>],
        date: u32,
        weekday: u8,
    ) {
        let transit_indices: Vec<usize> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| {
                if matches!(l, PlanLeg::Transit(_)) {
                    Some(i)
                } else {
                    None
                }
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
                    .map(|l| match l {
                        PlanLeg::Walk(w) => w.duration,
                        _ => 0,
                    })
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
                    feeder_rt,
                    next_rt,
                    walk_to_next,
                    next_start,
                    max_alighting,
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
                ) && new_dep > leg_start
                {
                    let cloned = match &legs[ti] {
                        PlanLeg::Transit(t) => t.clone(),
                        _ => unreachable!(),
                    };
                    if let Ok(mut alts) = cloned.find_alternatives(
                        self,
                        std::iter::once((dep_idx, &self.raptor.transit_departures[dep_idx])),
                        1,
                    ) && let Some(new_leg) = alts.pop()
                    {
                        legs[ti] = PlanLeg::Transit(new_leg);
                    }
                }
            }

            let new_leg_end = match &legs[ti] {
                PlanLeg::Transit(t) => t.end,
                _ => unreachable!(),
            };

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
                        let next_dep = next_t.transfer_risk.as_ref().and_then(|r| r.next_departure);
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
    /// Plan A dominates plan B when A is at least as good on all four Pareto axes
    /// (departure ↑, arrival ↓, transfer count ↓, reliability bucket ↑) and strictly
    /// better in at least one; walking duration is a tie-break attribute, not an axis.
    /// Plan reliability = product of each transit leg's `transfer_risk.reliability`
    /// (legs without a risk count as 1.0). Walk-only plans = 1.0.
    pub fn plan_reliability(plan: &Plan) -> f32 {
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

    /// Total street (non-transit) seconds of a plan.
    fn plan_street_secs(plan: &Plan) -> u32 {
        plan.legs
            .iter()
            .filter_map(|l| {
                if let PlanLeg::Walk(w) = l {
                    Some(w.duration)
                } else {
                    None
                }
            })
            .sum()
    }

    /// The plan's transit core: the exact sequence of boarded trip segments.
    /// Plans sharing a core are the same journey with different street legs.
    fn transit_core(plan: &Plan) -> Vec<(u32, usize, usize)> {
        plan.legs
            .iter()
            .filter_map(|l| match l {
                PlanLeg::Transit(t) => Some((t.trip_id.0, t.from.node_id.0, t.to.node_id.0)),
                _ => None,
            })
            .collect()
    }

    /// Collapses same-transit-core plans that differ only in street access/egress
    /// into one plan with `access_alternatives`. The primary is the
    /// lightest-burden member; a member stays a standalone plan when it arrives
    /// strictly earlier than the primary (a genuine Pareto endpoint, e.g. a
    /// ridden egress) — otherwise its only possible advantage is departure time
    /// or street comfort, which is exactly what the alternatives convey.
    /// Same-mode non-primary members are exact time-duplicates and are dropped.
    /// Direct plans (empty core) pass through untouched.
    pub(super) fn group_access_alternatives(plans: Vec<Plan>) -> Vec<Plan> {
        use std::collections::HashMap;

        let keys: Vec<Vec<(u32, usize, usize)>> = plans.iter().map(Self::transit_core).collect();
        let mut groups: HashMap<&[(u32, usize, usize)], Vec<usize>> = HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            if !key.is_empty() {
                groups.entry(key.as_slice()).or_default().push(i);
            }
        }

        let mut slots: Vec<Option<Plan>> = plans.into_iter().map(Some).collect();

        for members in groups.values() {
            if members.len() < 2 {
                continue;
            }
            let &primary_idx = members
                .iter()
                .min_by_key(|&&i| {
                    let p = slots[i].as_ref().unwrap();
                    (
                        p.mode.burden(),
                        p.end,
                        std::cmp::Reverse(p.start),
                        Self::plan_street_secs(p),
                    )
                })
                .unwrap();
            let primary_end = slots[primary_idx].as_ref().unwrap().end;

            let mut alternatives: Vec<AccessAlternative> = Vec::new();
            for &i in members {
                if i == primary_idx {
                    continue;
                }
                if slots[i].as_ref().unwrap().end < primary_end {
                    continue; // genuinely earlier arrival: stays a standalone plan
                }
                let member = slots[i].take().unwrap();
                let alt = AccessAlternative {
                    mode: member.mode,
                    start: member.start,
                    end: member.end,
                    expected_end: member.expected_end,
                    street_secs: Self::plan_street_secs(&member),
                };
                alternatives.extend(member.access_alternatives);
                if member.mode != slots[primary_idx].as_ref().unwrap().mode {
                    alternatives.push(alt);
                }
            }

            let primary = slots[primary_idx].as_mut().unwrap();
            for alt in alternatives {
                let dup = primary
                    .access_alternatives
                    .iter_mut()
                    .find(|a| a.mode == alt.mode);
                match dup {
                    // Keep the latest-departing variant per mode.
                    Some(existing) if existing.start >= alt.start => {}
                    Some(existing) => *existing = alt,
                    None => primary.access_alternatives.push(alt),
                }
            }
            primary
                .access_alternatives
                .sort_by_key(|a| (a.mode.burden(), a.start));
        }

        slots.into_iter().flatten().collect()
    }

    /// Final plan pipeline: collapse access twins, drop transit plans no faster
    /// than an equal-or-lighter-burden direct ride, then burden-aware Pareto.
    pub(super) fn finalize_plans(plans: Vec<Plan>, buckets: &ReliabilityBuckets) -> Vec<Plan> {
        let grouped = Self::group_access_alternatives(plans);
        Self::pareto_filter(Self::prune_slower_than_direct(grouped), buckets)
    }

    /// Drops any transit plan whose total duration is *strictly longer* than a
    /// direct street plan of equal-or-lighter burden. In a windowed search a
    /// later-departing transit plan can survive Pareto on the departure axis
    /// even though just walking/cycling/driving straight there is quicker; such
    /// a plan is never worth showing. A lighter-burden direct ride suppresses a
    /// heavier transit plan, but never the reverse (cycling-direct cannot bump a
    /// walk+transit option).
    pub(super) fn prune_slower_than_direct(plans: Vec<Plan>) -> Vec<Plan> {
        let is_direct = |p: &Plan| !p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_)));
        let dur = |p: &Plan| p.end.saturating_sub(p.start);

        // best_direct[b] = shortest direct duration available at burden <= b.
        let mut best_direct = [u32::MAX; 3];
        for p in &plans {
            if is_direct(p) {
                let d = dur(p);
                for slot in best_direct.iter_mut().skip(p.mode.burden() as usize) {
                    *slot = (*slot).min(d);
                }
            }
        }

        plans
            .into_iter()
            .filter(|p| is_direct(p) || dur(p) <= best_direct[p.mode.burden() as usize])
            .collect()
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
            plan.legs
                .iter()
                .filter_map(|l| {
                    if let PlanLeg::Walk(w) = l {
                        Some(w.duration)
                    } else {
                        None
                    }
                })
                .sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        // 4-D Pareto: (transfers ↓, end ↓, start ↑, reliability_bucket ↑), guarded
        // by mode burden: a plan may only dominate plans of equal-or-heavier
        // burden, so a bike/car plan must strictly beat every lighter-mode plan
        // on some axis to survive, while a walk plan can never be deleted by a
        // heavier one. Walk seconds and burden are NOT axes — they only break
        // exact 4-axis ties: lower burden first, then lower walk.
        let dominates = |a: &Plan, b: &Plan| {
            let (tc_a, tc_b) = (transfer_count(a), transfer_count(b));
            let (rb_a, rb_b) = (rel_bucket(a), rel_bucket(b));
            a.mode.burden() <= b.mode.burden()
                && tc_a <= tc_b
                && a.end <= b.end
                && a.start >= b.start
                && rb_a >= rb_b
                && (tc_a < tc_b
                    || a.end < b.end
                    || a.start > b.start
                    || rb_a > rb_b
                    || a.mode.burden() < b.mode.burden()
                    || walk_secs(a) < walk_secs(b))
        };
        let equal_4 = |a: &Plan, b: &Plan| {
            transfer_count(a) == transfer_count(b)
                && a.end == b.end
                && a.start == b.start
                && rel_bucket(a) == rel_bucket(b)
        };
        let tie_break_wins = |a: &Plan, b: &Plan| {
            a.mode.burden() < b.mode.burden()
                || (a.mode.burden() == b.mode.burden() && walk_secs(a) <= walk_secs(b))
        };

        let mut result: Vec<Plan> = Vec::new();

        'outer: for plan in plans {
            for existing in &result {
                if dominates(existing, &plan)
                    || (equal_4(existing, &plan) && tie_break_wins(existing, &plan))
                {
                    continue 'outer;
                }
            }
            result.retain(|existing| !dominates(&plan, existing));
            result.push(plan);
        }

        result.sort_by(|a, b| {
            a.end
                .cmp(&b.end)
                .then(b.start.cmp(&a.start))
                .then(rel_bucket(b).cmp(&rel_bucket(a)))
                .then(a.mode.burden().cmp(&b.mode.burden()))
                .then(walk_secs(a).cmp(&walk_secs(b)))
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
        sink: &mut [PlanCandidate],
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
            plan.legs
                .iter()
                .filter_map(|l| {
                    if let PlanLeg::Walk(w) = l {
                        Some(w.duration)
                    } else {
                        None
                    }
                })
                .sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        // Burden-guarded 4-D Pareto with burden/walk tie-breaks (see `pareto_filter`).
        let dominates = |a: &Plan, b: &Plan| {
            let (tc_a, tc_b) = (transfer_count(a), transfer_count(b));
            let (rb_a, rb_b) = (rel_bucket(a), rel_bucket(b));
            a.mode.burden() <= b.mode.burden()
                && tc_a <= tc_b
                && a.end <= b.end
                && a.start >= b.start
                && rb_a >= rb_b
                && (tc_a < tc_b
                    || a.end < b.end
                    || a.start > b.start
                    || rb_a > rb_b
                    || a.mode.burden() < b.mode.burden()
                    || walk_secs(a) < walk_secs(b))
        };
        let equal_4 = |a: &Plan, b: &Plan| {
            transfer_count(a) == transfer_count(b)
                && a.end == b.end
                && a.start == b.start
                && rel_bucket(a) == rel_bucket(b)
        };
        let tie_break_wins = |a: &Plan, b: &Plan| {
            a.mode.burden() < b.mode.burden()
                || (a.mode.burden() == b.mode.burden() && walk_secs(a) <= walk_secs(b))
        };

        let mut result: Vec<Plan> = Vec::new();
        let mut result_sink_idx: Vec<usize> = Vec::new();

        'outer: for (plan, &sink_idx) in plans.into_iter().zip(plan_to_sink_idx.iter()) {
            let tc_p = transfer_count(&plan);
            let rb_p = rel_bucket(&plan);

            // Check if `plan` is dominated by (or a higher-walk twin of) any result.
            for (i, existing) in result.iter().enumerate() {
                if dominates(existing, &plan)
                    || (equal_4(existing, &plan) && tie_break_wins(existing, &plan))
                {
                    let tc_e = transfer_count(existing);
                    let rb_e = rel_bucket(existing);
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
                if dominates(&plan, existing) {
                    let tc_e = transfer_count(existing);
                    let rb_e = rel_bucket(existing);
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
                .zip(result_sink_idx)
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
                .then(a.mode.burden().cmp(&b.mode.burden()))
                .then(walk_secs(a).cmp(&walk_secs(b)))
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::delay::{DelayCDF, ScenarioBag};

    #[test]
    fn arrival_stats_excludes_unreachable_miss_scenarios() {
        let bag = ScenarioBag::with_scenarios(36000, 0.6, u32::MAX, 0.4);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 37000);
        assert_eq!(dist.len(), 1);
        assert_eq!(dist[0].time, 36000);
        assert!((dist[0].probability - 0.6).abs() < 1e-6);
        assert_eq!(expected_end, 36000);
    }

    #[test]
    fn arrival_stats_unreachable_survives_negative_delay_convolution() {
        let bag = ScenarioBag::with_scenarios(36000, 0.5, u32::MAX, 0.5);
        let cdf = DelayCDF {
            bins: vec![(-180, 0.2), (0, 0.8), (120, 1.0)],
        };
        let (dist, expected_end) = Graph::arrival_stats(&bag, Some(&cdf), 37000);
        assert!(
            dist.iter().all(|s| s.time < 200_000),
            "sentinel leaked: {dist:?}"
        );
        assert!(
            (35820..=36120).contains(&expected_end),
            "got {expected_end}"
        );
    }

    #[test]
    fn arrival_stats_all_unreachable_falls_back_to_best_arrival() {
        let bag = ScenarioBag::with_scenarios(u32::MAX, 0.6, u32::MAX, 0.4);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 37000);
        assert!(dist.is_empty());
        assert_eq!(expected_end, 37000);
    }

    #[test]
    fn arrival_stats_pure_bag_keeps_full_expectation() {
        let bag = ScenarioBag::with_scenarios(36000, 0.75, 36600, 0.25);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 36000);
        assert_eq!(dist.len(), 2);
        assert_eq!(expected_end, 36150);
    }

    // ── grouping + burden-aware pareto ───────────────────────────────────────

    use crate::ingestion::gtfs::TripId;
    use crate::structures::Mode;

    fn place(node: usize) -> PlanPlace {
        PlanPlace {
            node_id: NodeID(node),
            stop_position: None,
            arrival: None,
            departure: None,
        }
    }

    fn walk_leg(street_mode: Mode, start: u32, end: u32) -> PlanLeg {
        PlanLeg::Walk(PlanWalkLeg {
            length: 0,
            cycleroute_length: None,
                elevation_gain: None,
            start,
            end,
            duration: end - start,
            street_mode,
            from: place(0),
            to: place(1),
            steps: vec![],
            geometry: vec![],
        })
    }

    fn transit_leg(trip: u32, from: usize, to: usize, start: u32, end: u32) -> PlanLeg {
        PlanLeg::Transit(PlanTransitLeg {
            length: 0,
            start,
            end,
            duration: end - start,
            scheduled_start: start,
            scheduled_end: end,
            realtime: false,
            from: place(from),
            to: place(to),
            steps: vec![],
            geometry: vec![],
            transfer_risk: None,
            trip_id: TripId(trip),
            preceding_arrival: None,
            preceding_route_type: None,
            route_type: None,
            following_route_type: None,
            following_margin_secs: None,
            bikes_allowed: None,
            time_shift: 0,
        })
    }

    fn plan(mode: Mode, start: u32, end: u32, legs: Vec<PlanLeg>) -> Plan {
        Plan {
            legs,
            start,
            end,
            mode,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
        }
    }

    fn buckets() -> ReliabilityBuckets {
        ReliabilityBuckets::new(&[0.50, 0.80, 0.95])
    }

    #[test]
    fn plan_timeline_rechains_trailing_walk_after_realtime_shift() {
        // Transit leg delayed to 720..800 by realtime; the egress walk still
        // carries its stale scheduled times (200..260). The plan summary must
        // follow the realtime legs, not the stale ones.
        let mut legs = vec![
            walk_leg(Mode::Walk, 100, 120),   // access (anchor — not re-chained)
            transit_leg(7, 10, 11, 720, 800), // realtime-delayed boarding
            walk_leg(Mode::Walk, 200, 260),   // egress, stale 60s walk
        ];
        let (start, end) = Graph::plan_timeline(&mut legs);
        assert_eq!(start, 100);
        assert_eq!(
            end, 860,
            "egress must chain off the realtime arrival (800 + 60)"
        );
        assert!(end >= start);
        match &legs[2] {
            PlanLeg::Walk(w) => {
                assert_eq!(w.start, 800);
                assert_eq!(w.end, 860);
            }
            _ => panic!("expected egress walk"),
        }
    }

    #[test]
    fn plan_timeline_is_noop_when_already_chained() {
        let mut legs = vec![
            walk_leg(Mode::Walk, 100, 120),
            transit_leg(7, 10, 11, 120, 200),
            walk_leg(Mode::Walk, 200, 260),
        ];
        let (start, end) = Graph::plan_timeline(&mut legs);
        assert_eq!((start, end), (100, 260));
    }

    #[test]
    fn burden_tie_goes_to_lighter_mode() {
        let core = || vec![transit_leg(7, 10, 11, 100, 200)];
        // Different cores would be needed to dodge grouping; use pareto directly.
        let walk = plan(Mode::WalkTransit, 90, 210, core());
        let bike = plan(Mode::BikeTransit, 90, 210, core());
        let out = Graph::pareto_filter(vec![bike, walk], &buckets());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mode, Mode::WalkTransit);
    }

    #[test]
    fn heavier_mode_survives_on_strict_improvement() {
        let walk = plan(
            Mode::WalkTransit,
            90,
            250,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeTransit,
            90,
            210,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::pareto_filter(vec![walk, bike], &buckets());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn lighter_mode_never_dominated_by_heavier() {
        // Bike strictly better on every axis — the walk plan must still survive.
        let walk = plan(
            Mode::WalkTransit,
            80,
            300,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeTransit,
            90,
            210,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::pareto_filter(vec![walk, bike], &buckets());
        assert_eq!(
            out.len(),
            2,
            "{:?}",
            out.iter().map(|p| p.mode).collect::<Vec<_>>()
        );
    }

    #[test]
    fn same_core_groups_into_alternatives() {
        let walk = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![
                walk_leg(Mode::Walk, 80, 100),
                transit_leg(7, 10, 11, 100, 200),
                walk_leg(Mode::Walk, 200, 260),
            ],
        );
        let bike = plan(
            Mode::BikeTransit,
            94,
            260,
            vec![
                walk_leg(Mode::Bike, 94, 100),
                transit_leg(7, 10, 11, 100, 200),
                walk_leg(Mode::Walk, 200, 260),
            ],
        );
        let out = Graph::group_access_alternatives(vec![walk, bike]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mode, Mode::WalkTransit);
        assert_eq!(out[0].access_alternatives.len(), 1);
        let alt = &out[0].access_alternatives[0];
        assert_eq!(alt.mode, Mode::BikeTransit);
        assert_eq!(alt.start, 94);
    }

    #[test]
    fn same_core_earlier_arrival_stays_standalone() {
        // Ridden egress arrives earlier: a genuine Pareto endpoint, not a twin.
        let walk = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeOnTransit,
            94,
            220,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let out = Graph::group_access_alternatives(vec![walk, bike]);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn different_core_not_grouped() {
        let a = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let b = plan(
            Mode::BikeTransit,
            94,
            260,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::group_access_alternatives(vec![a, b]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|p| p.access_alternatives.is_empty()));
    }

    #[test]
    fn direct_shorter_duration_suppresses_longer_same_burden_transit() {
        // Bike-direct: 21 min, leave now (burden 1). Bike+transit: 24 min but
        // departs later (burden 1) — it only survived Pareto on the later
        // departure. Since cycling straight there is shorter, drop it.
        let bike_direct = plan(Mode::Bike, 0, 1260, vec![walk_leg(Mode::Bike, 0, 1260)]);
        let bike_transit = plan(
            Mode::BikeOnTransit,
            300,
            1740,
            vec![transit_leg(7, 10, 11, 400, 1700)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, bike_transit], &buckets());
        assert!(
            out.iter().all(|p| p.mode != Mode::BikeOnTransit),
            "a bike+transit slower than cycling direct must be dropped: {:?}",
            out.iter()
                .map(|p| (p.mode, p.end - p.start))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn direct_does_not_suppress_lighter_burden_transit() {
        // Bike-direct (burden 1) must NOT suppress a longer WALK+transit (burden
        // 0) — a lighter mode is always worth offering.
        let bike_direct = plan(Mode::Bike, 0, 1260, vec![walk_leg(Mode::Bike, 0, 1260)]);
        let walk_transit = plan(
            Mode::WalkTransit,
            300,
            1740,
            vec![transit_leg(7, 10, 11, 400, 1700)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, walk_transit], &buckets());
        assert!(
            out.iter().any(|p| p.mode == Mode::WalkTransit),
            "lighter-burden walk+transit must survive a heavier bike-direct"
        );
    }

    #[test]
    fn direct_does_not_suppress_faster_transit() {
        // Transit beats cycling direct on duration here — it must survive.
        let bike_direct = plan(Mode::Bike, 0, 1500, vec![walk_leg(Mode::Bike, 0, 1500)]);
        let bike_transit = plan(
            Mode::BikeOnTransit,
            0,
            1320,
            vec![transit_leg(7, 10, 11, 100, 1300)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, bike_transit], &buckets());
        assert!(
            out.iter().any(|p| p.mode == Mode::BikeOnTransit),
            "a bike+transit faster than cycling direct must survive"
        );
    }

    #[test]
    fn direct_plans_never_grouped() {
        let a = plan(Mode::Walk, 80, 260, vec![walk_leg(Mode::Walk, 80, 260)]);
        let b = plan(Mode::Bike, 80, 140, vec![walk_leg(Mode::Bike, 80, 140)]);
        let out = Graph::group_access_alternatives(vec![a, b]);
        assert_eq!(out.len(), 2);
    }
}
