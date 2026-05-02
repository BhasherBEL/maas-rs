use std::collections::BTreeSet;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{StopTime, TripId},
    structures::{
        NodeID, ScenarioBag,
        plan::{AccessInfo, CandidateStatus, ExplainResult, Plan, PlanCandidate},
        raptor::Trace,
    },
};

use super::{Graph, MAX_ROUNDS};

impl Graph {
    /// Retry loop shared by `raptor` and `raptor_range`.
    ///
    /// Doubles `access_secs` until `try_routing` returns a non-empty result or
    /// the walk-only time is reached (at which point a walk-only plan is returned).
    /// `try_routing(raw_stops, targets, access_secs)` receives the current stop
    /// lists and walk radius and returns candidate plans (empty = no result yet).
    fn with_access_search<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        mut try_routing: F,
    ) -> Vec<Plan>
    where
        F: FnMut(&[(usize, u32)], &[(usize, u32)], u32) -> Vec<Plan>,
    {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / self.raptor.walking_speed_mps) as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self
            .nearest_stop_secs(origin, straight_line_secs)
            .max(min_access_secs);

        loop {
            let raw_stops = self.nearby_stops(origin, access_secs);
            let targets = self.nearby_stops(destination, access_secs);

            if !raw_stops.is_empty() && !targets.is_empty() {
                let results = try_routing(&raw_stops, &targets, access_secs);
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

    pub fn raptor(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> Vec<Plan> {
        self.with_access_search(origin, destination, start_time, min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> = raw_stops.iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                self.raptor_inner(&sources, targets, start_time, access_secs, date, weekday, origin, destination)
            })
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
        self.raptor_inner_with_debug(
            sources, targets, start_time, access_secs, date, weekday, origin, destination,
        )
        .0
    }

    pub(super) fn raptor_inner_with_debug(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        access_secs: u32,
        date: u32,
        weekday: u8,
        origin: NodeID,
        destination: NodeID,
    ) -> (Vec<Plan>, Vec<PlanCandidate>) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let n_patterns = self.raptor.transit_patterns.len();

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

        let mut candidates: Vec<PlanCandidate> = Vec::new();
        let plans = self.extract_with_debug(
            sources,
            targets,
            start_time,
            date,
            weekday,
            &labels,
            &labels_rt,
            &traces,
            origin,
            destination,
            Some(&mut candidates),
        );
        (plans, candidates)
    }

    /// Debug variant of `with_access_search`.
    ///
    /// The closure returns `Option<(Vec<Plan>, Vec<PlanCandidate>)>`:
    /// `None` = no result yet, widen radius; `Some(...)` = routing succeeded.
    fn with_access_search_debug<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        mut try_routing: F,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, AccessInfo)
    where
        F: FnMut(&[(usize, u32)], &[(usize, u32)], u32) -> Option<(Vec<Plan>, Vec<PlanCandidate>)>,
    {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / self.raptor.walking_speed_mps) as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self
            .nearest_stop_secs(origin, straight_line_secs)
            .max(min_access_secs);
        let mut attempts: u32 = 0;

        loop {
            let raw_stops = self.nearby_stops(origin, access_secs);
            let targets = self.nearby_stops(destination, access_secs);

            if !raw_stops.is_empty() && !targets.is_empty() {
                if let Some((plans, candidates)) = try_routing(&raw_stops, &targets, access_secs) {
                    let access = AccessInfo {
                        walk_radius_secs: access_secs,
                        walk_radius_meters: (access_secs as f64 * self.raptor.walking_speed_mps) as u32,
                        origin_stops_found: raw_stops.len() as u32,
                        destination_stops_found: targets.len() as u32,
                        access_attempts: attempts,
                        fell_back_to_walk_only: false,
                    };
                    return (plans, candidates, access);
                }
            }

            access_secs = access_secs.saturating_mul(2);
            attempts += 1;

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
                    let (plans, candidates) = if actual < u32::MAX {
                        let plan = self.build_walk_plan(origin, destination, start_time, actual);
                        let candidate = PlanCandidate {
                            round: 0,
                            origin_departure: start_time,
                            plan: Some(plan.clone()),
                            status: CandidateStatus::Kept,
                        };
                        (vec![plan], vec![candidate])
                    } else {
                        (vec![], vec![])
                    };
                    let access = AccessInfo {
                        walk_radius_secs: access_secs,
                        walk_radius_meters: (access_secs as f64 * self.raptor.walking_speed_mps) as u32,
                        origin_stops_found: 0,
                        destination_stops_found: 0,
                        access_attempts: attempts,
                        fell_back_to_walk_only: true,
                    };
                    return (plans, candidates, access);
                }
            }
        }
    }

    pub fn raptor_explain(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> ExplainResult {
        let (plans, candidates, access) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> =
                    raw_stops.iter().map(|&(s, w)| (s, start_time + w)).collect();
                let (plans, cands) = self.raptor_inner_with_debug(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                );
                if plans.is_empty() { None } else { Some((plans, cands)) }
            },
        );
        ExplainResult { plans, candidates, access }
    }

    pub fn raptor_range_explain(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> ExplainResult {
        let (plans, candidates, access) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> =
                    raw_stops.iter().map(|&(s, w)| (s, start_time + w)).collect();
                let (probe, probe_cands) = self.raptor_inner_with_debug(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                );
                if probe.is_empty() {
                    return None;
                }

                let departure_times = self.collect_interesting_times(
                    raw_stops,
                    start_time,
                    start_time.saturating_add(window_secs),
                    date,
                    weekday,
                );
                if departure_times.is_empty() {
                    return Some((probe, probe_cands));
                }

                let mut all_plans = probe;
                let mut all_candidates = probe_cands;

                for t in departure_times {
                    let sources_t: Vec<(usize, u32)> =
                        raw_stops.iter().map(|&(s, w)| (s, t + w)).collect();
                    let (plans_t, mut cands_t) = self.raptor_inner_with_debug(
                        &sources_t, targets, t, access_secs, date, weekday, origin, destination,
                    );
                    all_plans.extend(plans_t);
                    all_candidates.append(&mut cands_t);
                }

                // Build the plan→sink mapping: each Kept candidate corresponds to
                // one plan in all_plans (in order).
                let plan_to_sink_idx: Vec<usize> = all_candidates
                    .iter()
                    .enumerate()
                    .filter_map(|(ci, c)| {
                        if matches!(c.status, CandidateStatus::Kept) { Some(ci) } else { None }
                    })
                    .collect();

                let final_plans =
                    Self::pareto_filter_with_debug(all_plans, &plan_to_sink_idx, &mut all_candidates);

                Some((final_plans, all_candidates))
            },
        );
        ExplainResult { plans, candidates, access }
    }

    fn collect_routes(&self, marked: &[usize], queue: &mut Vec<usize>, queue_pos: &mut [u32]) {
        for &stop in marked {
            let pats = self.raptor.transit_idx_stop_patterns[stop].of(&self.raptor.transit_stop_patterns);
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
        let pat_stops = self.raptor.transit_idx_pattern_stops[pattern].of(&self.raptor.transit_pattern_stops);
        let n_trips = self.raptor.transit_patterns[pattern].num_trips as usize;
        if n_trips == 0 {
            return;
        }

        let route_id = self.raptor.transit_patterns[pattern].route;
        let pat_rt = self.raptor.transit_routes[route_id.0 as usize].route_type;

        let all_times =
            self.raptor.transit_idx_pattern_stop_times[pattern].of(&self.raptor.transit_pattern_stop_times);
        let trip_ids = self.raptor.transit_idx_pattern_trips[pattern].of(&self.raptor.transit_pattern_trips);

        let mut boarded: Option<(usize, u32, f32)> = None;

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.raptor.transit_node_to_stop[pat_stops[pos].0] as usize;
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
                                            .raptor.transit_delay_models
                                            .get(&rt)
                                            .map(|cdf| cdf.prob_on_time(buf))
                                            .unwrap_or(1.0),
                                        None => 1.0,
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

    pub(super) fn next_active_trip_departure(
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

            let transfers = self.raptor.transit_idx_stop_transfers[stop].of(&self.raptor.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.raptor.transit_node_to_stop[target_node.0] as usize;

                let bag = labels[stop].shifted_by(walk);
                if bag.earliest() >= cutoff {
                    continue;
                }
                if bag.improves_on(&best[target]) {
                    labels[target].try_improve(&bag);
                    best[target].try_improve(&bag);
                    labels_rt[target] = labels_rt[stop];
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

    #[inline]
    pub(super) fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.raptor.transit_trips[trip_id.0 as usize].service_id;
        self.raptor.transit_services[svc.0 as usize].is_active(date, weekday)
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
    pub(super) fn mark(stop: usize, marked: &mut Vec<usize>, is_marked: &mut [bool]) {
        if !is_marked[stop] {
            is_marked[stop] = true;
            marked.push(stop);
        }
    }

    /// Collect every origin-departure time in `[earliest, latest]` such that
    /// a transit vehicle departs from some access stop at `T + walk_secs`.
    ///
    /// Returns times sorted **descending** (latest first).
    fn collect_interesting_times(
        &self,
        raw_stops: &[(usize, u32)],
        earliest_origin_departure: u32,
        latest_origin_departure: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<u32> {
        const MAX_PER_PATTERN: usize = 5;
        const MAX_TOTAL: usize = 20;

        let mut departure_times: BTreeSet<u32> = BTreeSet::new();

        'outer: for &(stop, walk_secs) in raw_stops {
            let earliest_at_stop = earliest_origin_departure.saturating_add(walk_secs);
            let latest_at_stop = latest_origin_departure.saturating_add(walk_secs);

            let pats = self.raptor.transit_idx_stop_patterns[stop].of(&self.raptor.transit_stop_patterns);
            for &(pat_id, stop_pos) in pats {
                let p = pat_id.0 as usize;
                let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }

                let stop_times =
                    self.raptor.transit_idx_pattern_stop_times[p].of(&self.raptor.transit_pattern_stop_times);
                let trip_ids =
                    self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);

                let col = &stop_times[stop_pos as usize * n_trips..(stop_pos as usize + 1) * n_trips];

                let lo = col.partition_point(|st| st.departure < earliest_at_stop);
                let hi = col.partition_point(|st| st.departure <= latest_at_stop);

                let mut per_pattern_count = 0usize;
                for t in lo..hi {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        departure_times.insert(col[t].departure - walk_secs);
                        per_pattern_count += 1;
                        if per_pattern_count >= MAX_PER_PATTERN {
                            break;
                        }
                        if departure_times.len() >= MAX_TOTAL {
                            break 'outer;
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
    /// `[start_time, start_time + window_secs]` and return the Pareto-optimal set.
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
        self.with_access_search(origin, destination, start_time, min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> = raw_stops.iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                let probe = self.raptor_inner(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                );
                if probe.is_empty() {
                    return vec![];
                }

                let departure_times = self.collect_interesting_times(
                    raw_stops,
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
                    let sources_t: Vec<(usize, u32)> = raw_stops.iter()
                        .map(|&(s, w)| (s, t + w))
                        .collect();
                    let plans = self.raptor_inner(
                        &sources_t, targets, t, access_secs, date, weekday, origin, destination,
                    );
                    all_plans.extend(plans);
                }
                Self::pareto_filter(all_plans)
            })
    }
}
