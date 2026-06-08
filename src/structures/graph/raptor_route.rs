use std::collections::BTreeSet;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{StopTime, TripId},
    structures::{
        NodeData, NodeID, RealtimeIndex, ReliabilityBuckets, ScenarioBag,
        plan::{AccessInfo, CandidateStatus, ExplainResult, Plan, PlanCandidate, PlanCoordinate, StopPathLeg, StopReach},
        raptor::Trace,
    },
};

use super::{Graph, MAX_ROUNDS};

/// Maximum labels kept per `(round, stop)` cell. One per reliability bucket, so this
/// bounds the supported bucket count (edges.len()+2). Default config uses 5.
pub(super) const MAX_LABELS: usize = 16;

/// Apply a signed realtime delay (seconds) to a scheduled time, clamped at 0.
#[inline]
fn apply_delay(scheduled: u32, delay: i32) -> u32 {
    (scheduled as i64 + delay as i64).max(0) as u32
}

/// A label currently riding a route during a single `scan_route` pass.
#[derive(Clone, Copy)]
pub(super) struct Riding {
    /// Trip index within the pattern (smaller = earlier = arrives earlier downstream).
    t: usize,
    boarded_at: u32,
    /// Marginal on-time probability for arrival-bag construction.
    hit_prob: f32,
    /// Cumulative path reliability (for bucketing).
    reliability: f32,
    /// Reliability bucket of the prev label this was boarded from (for reconstruction).
    from_bucket: u8,
}

/// One RAPTOR label: an arrival-time distribution plus the cumulative reliability
/// and the trace needed to reconstruct how the stop was reached.
#[derive(Clone, Copy)]
pub(super) struct Label {
    pub bag: ScenarioBag,
    pub route_type: Option<RouteType>,
    pub reliability: f32,
    pub trace: Trace,
}

impl Label {
    pub const NONE: Self = Self {
        bag: ScenarioBag::EMPTY,
        route_type: None,
        reliability: 0.0,
        trace: Trace::NONE,
    };
}

/// Bounded Pareto set of labels per `(round, stop)`, traded off on
/// `(scheduled arrival ↓, reliability bucket ↑)`. At most one label per bucket
/// (the earliest-arriving one). Fixed-capacity & `Copy` — no heap per cell.
#[derive(Clone, Copy)]
pub(super) struct LabelSet {
    labels: [Label; MAX_LABELS],
    len: u8,
}

impl LabelSet {
    pub const EMPTY: Self = Self {
        labels: [Label::NONE; MAX_LABELS],
        len: 0,
    };

    #[inline]
    pub fn is_reached(&self) -> bool {
        self.len > 0
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.labels[..self.len as usize].iter()
    }

    /// Earliest (scheduled, best-case) arrival across all labels (`u32::MAX` if empty).
    /// This is the time axis of the Pareto front — risk lives in the reliability axis,
    /// so we compare on scheduled arrival, not probability-weighted expected arrival.
    pub fn earliest(&self) -> u32 {
        self.iter().map(|l| l.bag.earliest()).min().unwrap_or(u32::MAX)
    }

    /// The label whose reliability falls in bucket `b`, if any.
    pub fn get_by_bucket(&self, buckets: &ReliabilityBuckets, b: u8) -> Option<&Label> {
        self.iter().find(|l| buckets.bucket(l.reliability) == b)
    }

    /// The label with the earliest scheduled arrival (fastest), if any.
    pub fn min_arrival_label(&self) -> Option<&Label> {
        self.iter().fold(None, |acc, l| match acc {
            None => Some(l),
            Some(b) if l.bag.earliest() < b.bag.earliest() => Some(l),
            Some(b) => Some(b),
        })
    }

    /// Pareto-inserts `cand`. Returns true if the set changed (i.e. `cand` was kept).
    /// Dominance over (scheduled arrival ↓, reliability bucket ↑): an existing label
    /// dominates `cand` if it has a `>=` bucket AND a `<=` earliest arrival. At most
    /// one label survives per bucket (the earliest-arriving one). A label is never
    /// overridden by a later-arriving one in the same bucket.
    pub fn insert(&mut self, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.bag.earliest();

        for l in self.iter() {
            if buckets.bucket(l.reliability) >= cb && l.bag.earliest() <= ce {
                return false;
            }
        }

        // Drop existing labels dominated by `cand`.
        let mut w = 0usize;
        for i in 0..self.len as usize {
            let e = self.labels[i];
            let eb = buckets.bucket(e.reliability);
            let ee = e.bag.earliest();
            let dominated = eb <= cb && ee >= ce && (eb < cb || ee > ce);
            if !dominated {
                self.labels[w] = e;
                w += 1;
            }
        }
        self.len = w as u8;

        if (self.len as usize) < MAX_LABELS {
            self.labels[self.len as usize] = cand;
            self.len += 1;
            return true;
        }

        // Full (only possible with very fine custom bucket edges): replace the worst
        // label (lowest bucket, then latest arrival) if `cand` beats it.
        let mut worst = 0usize;
        for i in 1..self.len as usize {
            let wb = buckets.bucket(self.labels[worst].reliability);
            let ib = buckets.bucket(self.labels[i].reliability);
            if ib < wb
                || (ib == wb && self.labels[i].bag.earliest() > self.labels[worst].bag.earliest())
            {
                worst = i;
            }
        }
        let wb = buckets.bucket(self.labels[worst].reliability);
        if cb > wb || (cb == wb && ce < self.labels[worst].bag.earliest()) {
            self.labels[worst] = cand;
            return true;
        }
        false
    }
}

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

    /// Convenience wrapper using the graph's configured buckets and slack.
    pub fn raptor(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
    ) -> Vec<Plan> {
        let buckets = ReliabilityBuckets::new(&self.raptor.reliability_bucket_edges);
        self.raptor_tuned(origin, destination, start_time, date, weekday, min_access_secs,
            &buckets, self.raptor.arrival_slack_secs)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
    ) -> Vec<Plan> {
        self.raptor_tuned_rt(origin, destination, start_time, date, weekday, min_access_secs,
            buckets, slack, &RealtimeIndex::new())
    }

    /// `raptor_tuned` with a realtime delay index applied to trip times.
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned_rt(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> Vec<Plan> {
        self.with_access_search(origin, destination, start_time, min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> = raw_stops.iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                self.raptor_inner(&sources, targets, start_time, access_secs, date, weekday, origin, destination, buckets, slack, rt)
            })
    }

    #[allow(clippy::too_many_arguments)]
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
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> Vec<Plan> {
        self.raptor_inner_with_debug(
            sources, targets, start_time, access_secs, date, weekday, origin, destination,
            buckets, slack, rt,
        )
        .0
    }

    #[allow(clippy::too_many_arguments)]
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
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let n_patterns = self.raptor.transit_patterns.len();

        let mut best = vec![LabelSet::EMPTY; n_stops];
        let mut labels = vec![vec![LabelSet::EMPTY; n_stops]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_stops];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];

        for &(stop, time) in sources {
            let lab = Label {
                bag: ScenarioBag::single(time),
                route_type: None,
                reliability: 1.0,
                trace: Trace::NONE,
            };
            labels[0][stop].insert(lab, buckets);
            best[stop].insert(lab, buckets);
            Self::mark(stop, &mut marked, &mut is_marked);
        }

        self.apply_transfers(
            &mut labels[0],
            &mut best,
            buckets,
            &mut marked,
            &mut is_marked,
            start_time + access_secs,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                rest[0].copy_from_slice(&prev[k - 1]);
            }

            self.collect_routes(&marked, &mut queue, &mut queue_pos);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(&best, targets, slack);

            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev_slice = &prev[k - 1];
                let curr_slice = &mut rest[0];

                for &pat in &queue {
                    self.scan_route(
                        pat,
                        queue_pos[pat],
                        date,
                        weekday,
                        cutoff,
                        prev_slice,
                        curr_slice,
                        &mut best,
                        buckets,
                        rt,
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
                &mut best,
                buckets,
                &mut marked,
                &mut is_marked,
                cutoff,
            );

            if marked.is_empty() {
                break;
            }
        }

        let stops_reached: Vec<StopReach> = (0..n_stops)
            .filter_map(|stop_idx| {
                for k in 0..=MAX_ROUNDS {
                    if labels[k][stop_idx].is_reached() {
                        let node_id = self.raptor.transit_stop_to_node[stop_idx];
                        let loc = self.nodes[node_id.0].loc();
                        let name = match &self.nodes[node_id.0] {
                            NodeData::TransitStop(s) => s.name.clone(),
                            _ => String::new(),
                        };
                        let path = self.path_to_stop(stop_idx, k, origin, &labels);
                        return Some(StopReach {
                            stop_idx: stop_idx as u32,
                            round: k as u8,
                            arrival_secs: labels[k][stop_idx].earliest(),
                            lat: loc.latitude,
                            lon: loc.longitude,
                            name,
                            path,
                        });
                    }
                }
                None
            })
            .collect();

        let mut candidates: Vec<PlanCandidate> = Vec::new();
        let plans = self.extract_with_debug(
            sources,
            targets,
            start_time,
            date,
            weekday,
            &labels,
            buckets,
            origin,
            destination,
            rt,
            Some(&mut candidates),
        );
        (plans, candidates, stops_reached)
    }

    /// Follows RAPTOR traces backward from `stop_idx` at `round` and builds the
    /// ordered sequence of legs (walk / transit) that the algorithm used to reach it.
    /// Transit legs include all intermediate pattern stops as geometry waypoints.
    fn path_to_stop(
        &self,
        stop_idx: usize,
        round: usize,
        origin: NodeID,
        labels: &[Vec<LabelSet>],
    ) -> Vec<StopPathLeg> {
        let mut legs: Vec<StopPathLeg> = Vec::new();
        let mut stop = stop_idx;
        let mut k = round;

        loop {
            let trace = match labels[k][stop].min_arrival_label() {
                Some(l) => l.trace,
                None => break,
            };
            let to_node = self.raptor.transit_stop_to_node[stop];
            let to_loc = self.nodes[to_node.0].loc();

            if trace.is_transit() {
                let p = trace.pattern as usize;
                let bp = trace.boarded_at as usize;
                let ap = trace.alighted_at as usize;
                let pat_stops = self.raptor.transit_idx_pattern_stops[p]
                    .of(&self.raptor.transit_pattern_stops);

                let geometry: Vec<PlanCoordinate> = (bp..=ap)
                    .map(|i| {
                        let loc = self.nodes[pat_stops[i].0].loc();
                        PlanCoordinate { lat: loc.latitude, lon: loc.longitude }
                    })
                    .collect();

                let route_id = self.raptor.transit_patterns[p].route;
                let route_label = self.raptor.transit_routes[route_id.0 as usize].route_short_name.clone();

                legs.push(StopPathLeg { is_transit: true, route_label, geometry });

                let boarding_stop = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
                stop = boarding_stop;
                k = k.saturating_sub(1);
            } else if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let from_node = self.raptor.transit_stop_to_node[from];
                let from_loc = self.nodes[from_node.0].loc();
                legs.push(StopPathLeg {
                    is_transit: false,
                    route_label: String::new(),
                    geometry: vec![
                        PlanCoordinate { lat: from_loc.latitude, lon: from_loc.longitude },
                        PlanCoordinate { lat: to_loc.latitude, lon: to_loc.longitude },
                    ],
                });
                stop = from;
                // k stays the same for transfers
            } else {
                // Access walk: origin → this stop
                let from_loc = self.nodes[origin.0].loc();
                legs.push(StopPathLeg {
                    is_transit: false,
                    route_label: String::new(),
                    geometry: vec![
                        PlanCoordinate { lat: from_loc.latitude, lon: from_loc.longitude },
                        PlanCoordinate { lat: to_loc.latitude, lon: to_loc.longitude },
                    ],
                });
                break;
            }
        }

        legs.reverse();
        legs
    }

    /// Debug variant of `with_access_search`.
    ///
    /// The closure returns `Option<(Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>)>`:
    /// `None` = no result yet, widen radius; `Some(...)` = routing succeeded.
    fn with_access_search_debug<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        mut try_routing: F,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, AccessInfo, Vec<StopReach>)
    where
        F: FnMut(&[(usize, u32)], &[(usize, u32)], u32) -> Option<(Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>)>,
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
                if let Some((plans, candidates, stops)) = try_routing(&raw_stops, &targets, access_secs) {
                    let access = AccessInfo {
                        walk_radius_secs: access_secs,
                        walk_radius_meters: (access_secs as f64 * self.raptor.walking_speed_mps) as u32,
                        origin_stops_found: raw_stops.len() as u32,
                        destination_stops_found: targets.len() as u32,
                        access_attempts: attempts,
                        fell_back_to_walk_only: false,
                    };
                    return (plans, candidates, access, stops);
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
                    return (plans, candidates, access, vec![]);
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
        let buckets = ReliabilityBuckets::new(&self.raptor.reliability_bucket_edges);
        self.raptor_explain_tuned(origin, destination, start_time, date, weekday, min_access_secs,
            &buckets, self.raptor.arrival_slack_secs)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_explain_tuned(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
    ) -> ExplainResult {
        self.raptor_explain_tuned_rt(origin, destination, start_time, date, weekday,
            min_access_secs, buckets, slack, &RealtimeIndex::new())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_explain_tuned_rt(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> =
                    raw_stops.iter().map(|&(s, w)| (s, start_time + w)).collect();
                let (plans, cands, stops) = self.raptor_inner_with_debug(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                    buckets, slack, rt,
                );
                if plans.is_empty() { None } else { Some((plans, cands, stops)) }
            },
        );
        ExplainResult {
            plans,
            candidates,
            access,
            stops_reached,
            origin: self.node_coord(origin),
            destination: self.node_coord(destination),
        }
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
        let buckets = ReliabilityBuckets::new(&self.raptor.reliability_bucket_edges);
        self.raptor_range_explain_tuned(origin, destination, start_time, window_secs, date, weekday,
            min_access_secs, &buckets, self.raptor.arrival_slack_secs)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_explain_tuned(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
    ) -> ExplainResult {
        self.raptor_range_explain_tuned_rt(origin, destination, start_time, window_secs, date,
            weekday, min_access_secs, buckets, slack, &RealtimeIndex::new())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_explain_tuned_rt(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> =
                    raw_stops.iter().map(|&(s, w)| (s, start_time + w)).collect();
                let (probe, probe_cands, probe_stops) = self.raptor_inner_with_debug(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                    buckets, slack, rt,
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
                    return Some((probe, probe_cands, probe_stops));
                }

                let mut all_plans = probe;
                let mut all_candidates = probe_cands;

                for t in departure_times {
                    let sources_t: Vec<(usize, u32)> =
                        raw_stops.iter().map(|&(s, w)| (s, t + w)).collect();
                    let (plans_t, mut cands_t, _stops_t) = self.raptor_inner_with_debug(
                        &sources_t, targets, t, access_secs, date, weekday, origin, destination,
                        buckets, slack, rt,
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
                    Self::pareto_filter_with_debug(all_plans, &plan_to_sink_idx, &mut all_candidates, buckets);

                Some((final_plans, all_candidates, probe_stops))
            },
        );
        ExplainResult {
            plans,
            candidates,
            access,
            stops_reached,
            origin: self.node_coord(origin),
            destination: self.node_coord(destination),
        }
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

    #[allow(clippy::too_many_arguments)]
    fn scan_route(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: u32,
        prev: &[LabelSet],
        curr: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        rt: &RealtimeIndex,
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

        // Labels currently riding this route. Pareto set over (trip index ↓, bucket ↑):
        // a smaller trip index arrives earlier at every downstream stop.
        let mut riding: Vec<Riding> = Vec::new();

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.raptor.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            // 1. Settle arrivals at this stop for every riding label.
            for r in &riding {
                // Realtime: shift the scheduled arrival by the live delay for this
                // trip at this stop (0 when no realtime info — inert default).
                let arr = apply_delay(col[r.t].arrival, rt.delay(trip_ids[r.t], stop as u32));
                if arr >= cutoff {
                    continue;
                }
                let bag = if r.hit_prob < 1.0 {
                    let miss_arr = self.next_trip_arrival(trip_ids, r.t + 1, col, date, weekday);
                    match miss_arr {
                        Some(ma) => ScenarioBag::with_scenarios(arr, r.hit_prob, ma, 1.0 - r.hit_prob),
                        None => ScenarioBag::with_scenarios(arr, r.hit_prob, u32::MAX, 1.0 - r.hit_prob),
                    }
                } else {
                    ScenarioBag::single(arr)
                };
                let cand = Label {
                    bag,
                    route_type: Some(pat_rt),
                    reliability: r.reliability,
                    trace: Trace {
                        pattern: pattern as u32,
                        trip: r.t as u32,
                        boarded_at: r.boarded_at,
                        alighted_at: pos as u32,
                        from_stop: u32::MAX,
                        from_bucket: r.from_bucket,
                    },
                };
                if best[stop].insert(cand, buckets) {
                    curr[stop].insert(cand, buckets);
                    Self::mark(stop, marked, is_marked);
                }
            }

            // 2. Board from each prev label at this stop. We board the earliest
            //    catchable trip, then successively-later trips that reach a *higher*
            //    reliability bucket (waiting longer = safer connection), up to CERTAIN.
            //    Reliability is monotonic in departure margin, so buckets only rise.
            let max_bucket = buckets.bucket(1.0);
            if prev[stop].is_reached() {
                for pl in prev[stop].iter() {
                    let from_bucket = buckets.bucket(pl.reliability);
                    let min_dep = pl.bag.earliest() as u32;
                    let t_start = col.partition_point(|st| st.departure < min_dep);
                    let mut best_bucket_seen: Option<u8> = None;
                    for t in t_start..n_trips {
                        if !self.is_trip_active(trip_ids[t], date, weekday) {
                            continue;
                        }
                        // Realtime: effective departure = scheduled + live delay.
                        let trip_dep = apply_delay(col[t].departure, rt.delay(trip_ids[t], stop as u32));

                        // Cumulative reliability — same per-transfer formula as
                        // reconstruction (earliest-based), so buckets agree.
                        let factor = self.transfer_on_time_prob(
                            pl.route_type,
                            Some(pat_rt),
                            pl.bag.earliest(),
                            trip_dep,
                        );
                        let rel = pl.reliability * factor;
                        let cb = buckets.bucket(rel);

                        // Only board this trip if it reaches a bucket we haven't covered
                        // yet for this prev label (the earliest trip per bucket level).
                        if best_bucket_seen.map_or(false, |bs| cb <= bs) {
                            continue;
                        }
                        best_bucket_seen = Some(cb);

                        // Marginal on-time probability over the prev arrival
                        // distribution — used to build the arrival-time bag.
                        let hit_prob: f32 = pl
                            .bag
                            .scenarios()
                            .iter()
                            .map(|s| {
                                let buf = trip_dep as i32 - s.time as i32;
                                let p_make = match pl.route_type {
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

                        Self::push_riding(
                            &mut riding,
                            Riding {
                                t,
                                boarded_at: pos as u32,
                                hit_prob,
                                reliability: rel,
                                from_bucket,
                            },
                            buckets,
                        );
                        if cb >= max_bucket {
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Pareto-inserts a riding label into the route bag over (trip index ↓, bucket ↑).
    fn push_riding(riding: &mut Vec<Riding>, cand: Riding, buckets: &ReliabilityBuckets) {
        let cb = buckets.bucket(cand.reliability);
        for r in riding.iter() {
            if r.t <= cand.t && buckets.bucket(r.reliability) >= cb {
                return; // dominated
            }
        }
        riding.retain(|r| {
            let rb = buckets.bucket(r.reliability);
            !(cand.t <= r.t && cb >= rb && (cand.t < r.t || cb > rb))
        });
        if riding.len() < MAX_LABELS {
            riding.push(cand);
        }
    }

    /// Probability of boarding a vehicle departing at `board_dep` given arrival at
    /// the boarding stop at `arr_at_stop` on a preceding leg of type `prev_rt`.
    /// `1.0` when there is no preceding transit leg or no delay model. Shared by the
    /// RAPTOR core (label reliability) and reconstruction (`TransferRisk.reliability`).
    pub(super) fn transfer_on_time_prob(
        &self,
        prev_rt: Option<RouteType>,
        board_rt: Option<RouteType>,
        arr_at_stop: u32,
        board_dep: u32,
    ) -> f32 {
        match prev_rt.and_then(|rt| self.raptor.transit_delay_models.get(&rt)) {
            Some(cdf) => {
                let board = board_rt.and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                cdf.prob_on_time_vs(board, board_dep as i32 - arr_at_stop as i32)
            }
            None => 1.0,
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
        labels: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        cutoff: u32,
    ) {
        let n = marked.len();
        for i in 0..n {
            let stop = marked[i];
            let src = labels[stop]; // Copy; releases the borrow on `labels`.
            if !src.is_reached() || src.earliest() >= cutoff {
                continue;
            }

            let transfers = self.raptor.transit_idx_stop_transfers[stop].of(&self.raptor.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.raptor.transit_node_to_stop[target_node.0] as usize;

                for l in src.iter() {
                    let bag = l.bag.shifted_by(walk);
                    if bag.earliest() >= cutoff {
                        continue;
                    }
                    let cand = Label {
                        bag,
                        route_type: l.route_type,
                        reliability: l.reliability,
                        trace: Trace {
                            pattern: u32::MAX,
                            trip: u32::MAX,
                            boarded_at: u32::MAX,
                            alighted_at: u32::MAX,
                            from_stop: stop as u32,
                            from_bucket: buckets.bucket(l.reliability),
                        },
                    };
                    if best[target].insert(cand, buckets) {
                        labels[target].insert(cand, buckets);
                        Self::mark(target, marked, is_marked);
                    }
                }
            }
        }
    }

    #[inline]
    pub(super) fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.raptor.transit_trips[trip_id.0 as usize].service_id;
        self.raptor.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    /// Cutoff = (minimum expected arrival at any target + its egress walk) + `slack`.
    /// `slack` widens the explored arrival band so safer-but-slower plans survive.
    #[inline]
    fn target_cutoff(best: &[LabelSet], targets: &[(usize, u32)], slack: u32) -> u32 {
        targets
            .iter()
            .map(|&(s, w)| {
                if best[s].is_reached() {
                    best[s].earliest().saturating_add(w)
                } else {
                    u32::MAX
                }
            })
            .min()
            .unwrap_or(u32::MAX)
            .saturating_add(slack)
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
        let buckets = ReliabilityBuckets::new(&self.raptor.reliability_bucket_edges);
        self.raptor_range_tuned(origin, destination, start_time, window_secs, date, weekday,
            min_access_secs, &buckets, self.raptor.arrival_slack_secs)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
    ) -> Vec<Plan> {
        self.raptor_range_tuned_rt(origin, destination, start_time, window_secs, date, weekday,
            min_access_secs, buckets, slack, &RealtimeIndex::new())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned_rt(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> Vec<Plan> {
        self.with_access_search(origin, destination, start_time, min_access_secs,
            |raw_stops, targets, access_secs| {
                let sources: Vec<(usize, u32)> = raw_stops.iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                let probe = self.raptor_inner(
                    &sources, targets, start_time, access_secs, date, weekday, origin, destination,
                    buckets, slack, rt,
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
                        buckets, slack, rt,
                    );
                    all_plans.extend(plans);
                }
                Self::pareto_filter(all_plans, buckets)
            })
    }
}

#[cfg(test)]
mod label_tests {
    use super::*;

    fn lbl(time: u32, rel: f32) -> Label {
        Label {
            bag: ScenarioBag::single(time),
            route_type: None,
            reliability: rel,
            trace: Trace::NONE,
        }
    }

    #[test]
    fn labelset_keeps_one_per_bucket_min_expected() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl(100, 1.0), &b));
        assert!(!s.insert(lbl(200, 1.0), &b)); // same bucket, later -> rejected
        assert_eq!(s.iter().count(), 1);
        assert_eq!(s.earliest(), 100);
        assert!(s.insert(lbl(50, 1.0), &b)); // same bucket, earlier -> replaces
        assert_eq!(s.iter().count(), 1);
        assert_eq!(s.earliest(), 50);
    }

    #[test]
    fn labelset_keeps_pareto_tradeoff() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl(100, 0.10), &b)); // fast, risky
        assert!(s.insert(lbl(200, 1.00), &b)); // slow, safe
        assert_eq!(s.iter().count(), 2);
    }

    #[test]
    fn labelset_drops_dominated() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl(100, 1.00), &b)); // fast AND safe
        assert!(!s.insert(lbl(200, 0.10), &b)); // slow AND risky -> dominated
        assert_eq!(s.iter().count(), 1);
    }

    /// Within a bucket, dominance is by SCHEDULED arrival, not expected. A label with
    /// an earlier scheduled arrival but worse expected arrival must NOT be overridden
    /// by one with a later scheduled arrival but better expected. (Regression: the
    /// expected-based version made the front flip with query start time.)
    #[test]
    fn labelset_earlier_scheduled_wins_over_better_expected() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        // Same reliability bucket (both CERTAIN). First: scheduled 100 but expected 910
        // (90% chance of a late miss). Second: scheduled & expected 200.
        let risky_early = Label {
            bag: ScenarioBag::with_scenarios(100, 0.1, 1000, 0.9),
            route_type: None,
            reliability: 1.0,
            trace: Trace::NONE,
        };
        assert_eq!(risky_early.bag.earliest(), 100);
        assert!(s.insert(risky_early, &b));
        // Later scheduled arrival, same bucket -> must be rejected (no override).
        assert!(!s.insert(lbl(200, 1.0), &b));
        assert_eq!(s.iter().count(), 1);
        assert_eq!(s.earliest(), 100, "earliest-scheduled label must survive");
    }
}
