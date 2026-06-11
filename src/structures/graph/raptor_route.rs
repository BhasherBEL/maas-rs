use std::collections::BTreeSet;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{StopTime, TripId},
    structures::{
        NodeData, NodeID, RealtimeIndex, ReliabilityBuckets, ScenarioBag,
        plan::{AccessInfo, CandidateStatus, ExplainResult, Plan, PlanCandidate, PlanCoordinate, PlanLeg, PlanLegStep, StopPathLeg, StopReach},
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
    /// Transit-leg count of the prev label (alight labels get `from_round + 1`).
    from_round: u8,
    /// Arena index of the prev label this trip was boarded from (the alight label's parent).
    from_arena: u32,
}

/// One RAPTOR label: an arrival-time distribution plus the cumulative reliability
/// and the trace needed to reconstruct how the stop was reached.
#[derive(Clone, Copy)]
pub(super) struct Label {
    pub bag: ScenarioBag,
    pub route_type: Option<RouteType>,
    pub reliability: f32,
    pub trace: Trace,
    /// Index of the departure (in the range driver's latest-first ordering) whose
    /// pass created this label. 0 for single-pass queries. NOT a Pareto axis — it
    /// lets per-departure extraction select this-departure target labels.
    pub created_by: u32,
    /// Compact stop id this label resides at (needed to recover a footpath leg's
    /// destination during reconstruction, which the trace alone does not record).
    pub at_stop: u32,
    /// Number of transit legs used to create this label (footpaths don't count).
    /// More precise than the grid round index under carry-forward; cross-stamp
    /// pruning compares it so a many-leg ghost can't prune a fewer-leg label.
    pub round: u8,
    /// Arena index of the predecessor label (`u32::MAX` = source/root). Reconstruction
    /// follows these exact pointers instead of re-looking-up grid cells by bucket,
    /// which would drift to a different (overwritten) label and mis-score the plan.
    pub parent: u32,
    /// Own index in the per-pass label arena (`u32::MAX` until pushed).
    pub arena_id: u32,
}

impl Label {
    pub const NONE: Self = Self {
        bag: ScenarioBag::EMPTY,
        route_type: None,
        reliability: 0.0,
        trace: Trace::NONE,
        created_by: 0,
        at_stop: u32::MAX,
        round: 0,
        parent: u32::MAX,
        arena_id: u32::MAX,
    };

    /// Pushes `lab` into the per-pass arena, stamping its `arena_id`, and returns the
    /// updated copy to store in the grid so children can reference it as a parent.
    #[inline]
    pub fn arena_push(arena: &mut Vec<Label>, mut lab: Label) -> Label {
        lab.arena_id = arena.len() as u32;
        arena.push(lab);
        lab
    }
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

    /// The label with the earliest scheduled arrival (fastest), if any.
    pub fn min_arrival_label(&self) -> Option<&Label> {
        self.iter().fold(None, |acc, l| match acc {
            None => Some(l),
            Some(b) if l.bag.earliest() < b.bag.earliest() => Some(l),
            Some(b) => Some(b),
        })
    }

    /// True if some member dominates `cand` over (scheduled arrival ↓, bucket ↑):
    /// `>=` bucket AND `<=` earliest arrival. Used as a per-pass cross-round local
    /// prune that does NOT mutate the set (unlike `insert`).
    pub fn dominates(&self, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.bag.earliest();
        self.iter()
            .any(|l| buckets.bucket(l.reliability) >= cb && l.bag.earliest() <= ce)
    }

    /// Pareto-inserts `cand`. Returns true if the set changed (i.e. `cand` was kept).
    ///
    /// Same-stamp dominance is bucket-level (scheduled arrival ↓, reliability
    /// bucket ↑) — it matches the bucketed per-pass output, so at most one label
    /// per bucket survives within a departure.
    ///
    /// Cross-stamp (carried labels from later departures, the self-pruning range
    /// driver) dominance requires `>=` PRECISE reliability: two prefixes in the
    /// same bucket carry different precise reliabilities that can quantize to
    /// different final buckets downstream, so a bucket-level cross-stamp prune
    /// drops genuinely Pareto-optimal plans (the historical ~4% range misses).
    /// Precise-rel + arrival domination IS sound: an extension of the dominator
    /// has `<=` arrival (same trips catchable, `>=` transfer margins) and `>=`
    /// precise reliability at every downstream step.
    pub fn insert(&mut self, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.bag.earliest();

        for l in self.iter() {
            let dominates = if l.created_by == cand.created_by {
                buckets.bucket(l.reliability) >= cb
            } else {
                // Cross-stamp: precise reliability AND no extra transit legs —
                // a many-leg ghost pruning a fewer-leg label would lose plans
                // that win the output filter on the transfers axis.
                l.reliability >= cand.reliability && l.round <= cand.round
            };
            if dominates && l.bag.earliest() <= ce {
                return false;
            }
        }

        // Drop existing labels dominated by `cand`. Same-stamp: bucket-level (one
        // label per bucket per departure). Cross-stamp: ghosts are pruning-only
        // (their departure is already extracted), so evicting on bucket level only
        // weakens future pruning — never output correctness.
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

        // Full cell. Prefer evicting an old-stamp ghost: ghosts can never reach the
        // output again, while `cand` (the newest stamp) may be needed for it. Among
        // ghosts evict the weakest pruner (lowest precise reliability, then latest
        // arrival).
        let mut ghost: Option<usize> = None;
        for i in 0..self.len as usize {
            if self.labels[i].created_by == cand.created_by {
                continue;
            }
            ghost = Some(match ghost {
                None => i,
                Some(g) => {
                    let (gr, ge) = (self.labels[g].reliability, self.labels[g].bag.earliest());
                    let (ir, ie) = (self.labels[i].reliability, self.labels[i].bag.earliest());
                    if ir < gr || (ir == gr && ie > ge) { i } else { g }
                }
            });
        }
        if let Some(g) = ghost {
            self.labels[g] = cand;
            return true;
        }

        // All same-stamp (only possible with very fine custom bucket edges): replace
        // the worst label (lowest bucket, then latest arrival) if `cand` beats it.
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

            if let Some(actual) = walk_only_secs
                && access_secs >= actual {
                    return if actual < u32::MAX {
                        vec![self.build_walk_plan(origin, destination, start_time, actual)]
                    } else {
                        vec![]
                    };
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
            buckets, slack, rt, false,
        )
        .0
    }

    /// Core RAPTOR over one departure. `want_debug` gates the *discardable* debug
    /// instrumentation — the `stops_reached` survey and the per-candidate `PlanCandidate`
    /// sink (which clones every plan). The production path (`raptor_inner`) passes `false`
    /// so neither is computed; only `raptor_explain*` pays for them.
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
        want_debug: bool,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let n_patterns = self.raptor.transit_patterns.len();

        let mut best = vec![LabelSet::EMPTY; n_stops];
        let mut labels = vec![vec![LabelSet::EMPTY; n_stops]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_stops];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];
        let mut arena: Vec<Label> = Vec::new();

        self.run_departure_into(
            sources, targets, start_time, access_secs, date, weekday, buckets, slack, rt,
            0, false,
            &mut best, &mut labels, &mut marked, &mut is_marked, &mut queue, &mut queue_pos,
            &mut arena,
        );

        // Discardable debug survey: only built for `raptor_explain*`.
        let stops_reached: Vec<StopReach> = if want_debug {
            (0..n_stops)
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
                .collect()
        } else {
            Vec::new()
        };

        // The candidate sink clones every kept plan; only `raptor_explain*` needs it.
        let mut candidates: Vec<PlanCandidate> = Vec::new();
        let debug_sink = if want_debug { Some(&mut candidates) } else { None };
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
            debug_sink,
            0,
            &arena,
        );
        (plans, candidates, stops_reached)
    }

    /// Runs one RAPTOR departure (seed → rounds) into caller-owned grids.
    ///
    /// `best` is per-pass: it is **reset** here and gates only the per-pass
    /// cross-round local prune + `target_cutoff`. `labels` is **carried** across
    /// departures by the self-pruning range driver and is NOT reset — marking is
    /// gated on per-round `labels[k]` improvement (the round-stratified, transfers-
    /// preserving, cross-departure bound). `stamp` brands every label this pass
    /// creates so reconstruction can follow this-departure traces. When `carried`,
    /// the round-start carry-forward Pareto-**merges** `labels[k-1]` into the
    /// already-populated `labels[k]`; single-pass uses the faster `copy_from_slice`.
    #[allow(clippy::too_many_arguments)]
    fn run_departure_into(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        access_secs: u32,
        date: u32,
        weekday: u8,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
        stamp: u32,
        carried: bool,
        best: &mut [LabelSet],
        labels: &mut [Vec<LabelSet>],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        queue: &mut Vec<usize>,
        queue_pos: &mut [u32],
        arena: &mut Vec<Label>,
    ) {
        let n_stops = best.len();

        // `best` is the per-pass exploration bound — reset it; `labels` is carried.
        for b in best.iter_mut() {
            *b = LabelSet::EMPTY;
        }
        marked.clear();
        is_marked.fill(false);
        // The arena holds this pass's labels for exact parent-pointer reconstruction.
        // Same-stamp build keeps a pass's chains within its own arena, so reset it per
        // pass; carried foreign grid labels keep stale `arena_id`s that are never followed
        // (extraction and boarding both filter to the current pass's stamp).
        arena.clear();

        for &(stop, time) in sources {
            let lab = Label::arena_push(arena, Label {
                bag: ScenarioBag::single(time),
                route_type: None,
                reliability: 1.0,
                trace: Trace::NONE,
                created_by: stamp,
                at_stop: stop as u32,
                round: 0,
                parent: u32::MAX,
                arena_id: u32::MAX,
            });
            labels[0][stop].insert(lab, buckets);
            best[stop].insert(lab, buckets);
            Self::mark(stop, marked, is_marked);
        }

        self.apply_transfers(
            &mut labels[0],
            best,
            buckets,
            marked,
            is_marked,
            start_time + access_secs,
            stamp,
            arena,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev_k = &prev[k - 1];
                let curr_k = &mut rest[0];
                if carried {
                    // Carried-aware carry-forward: keep this round's already-carried
                    // labels and fold in the ≤(k-1)-trip frontier.
                    for stop in 0..n_stops {
                        if prev_k[stop].is_reached() {
                            for lab in prev_k[stop].iter() {
                                curr_k[stop].insert(*lab, buckets);
                            }
                        }
                    }
                } else {
                    curr_k.copy_from_slice(prev_k);
                }
            }

            self.collect_routes(marked, queue, queue_pos);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(best, targets, slack);

            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev_slice = &prev[k - 1];
                let curr_slice = &mut rest[0];

                let qp: &[u32] = queue_pos;
                let chunks = Self::scan_chunks(queue.len());
                if chunks <= 1 {
                    let mut cands: Vec<Label> = Vec::new();
                    for &pat in queue.iter() {
                        self.scan_route_collect(
                            pat, qp[pat], date, weekday, cutoff,
                            prev_slice, best, buckets, rt, stamp, &mut cands,
                        );
                    }
                    self.apply_scan_candidates(
                        &cands, curr_slice, best, buckets, marked, is_marked, arena,
                    );
                } else {
                    // Phase A: read-only scans in parallel over contiguous queue
                    // chunks (each thread emits candidates in scan order). Phase B
                    // applies them in queue order against the live `best`/grid —
                    // the exact consideration stream the sequential loop produces,
                    // so output (and arena ids) are identical regardless of
                    // thread scheduling.
                    let chunk_size = queue.len().div_ceil(chunks);
                    let best_ro: &[LabelSet] = best;
                    let collected: Vec<Vec<Label>> = std::thread::scope(|s| {
                        let handles: Vec<_> = queue
                            .chunks(chunk_size)
                            .map(|chunk| {
                                s.spawn(move || {
                                    let mut cands: Vec<Label> = Vec::new();
                                    for &pat in chunk {
                                        self.scan_route_collect(
                                            pat, qp[pat], date, weekday, cutoff,
                                            prev_slice, best_ro, buckets, rt, stamp, &mut cands,
                                        );
                                    }
                                    cands
                                })
                            })
                            .collect();
                        handles.into_iter().map(|h| h.join().unwrap()).collect()
                    });
                    for cands in &collected {
                        self.apply_scan_candidates(
                            cands, curr_slice, best, buckets, marked, is_marked, arena,
                        );
                    }
                }
            }

            for &p in queue.iter() {
                queue_pos[p] = u32::MAX;
            }
            queue.clear();

            self.apply_transfers(
                &mut labels[k],
                best,
                buckets,
                marked,
                is_marked,
                cutoff,
                stamp,
                arena,
            );

            if marked.is_empty() {
                break;
            }
        }
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

        while let Some(l) = labels[k][stop].min_arrival_label() {
            let trace = l.trace;
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

            if !raw_stops.is_empty() && !targets.is_empty()
                && let Some((plans, candidates, stops)) = try_routing(&raw_stops, &targets, access_secs) {
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

            if let Some(actual) = walk_only_secs
                && access_secs >= actual {
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
                    buckets, slack, rt, true,
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
                    buckets, slack, rt, true,
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
                        buckets, slack, rt, true,
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

    /// Read-only route scan: walks `pattern` building the riding set from `prev`
    /// and pushes every surviving candidate label into `out` in scan order.
    /// `best` is the round-start snapshot used only as a domination *prefilter*
    /// (it can lag the live set — `apply_scan_candidates` re-checks against the
    /// live one, so stale pruning here is sound and merely less aggressive).
    /// Being free of writes to the shared grids, route scans can run in parallel.
    #[allow(clippy::too_many_arguments)]
    fn scan_route_collect(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: u32,
        prev: &[LabelSet],
        best: &[LabelSet],
        buckets: &ReliabilityBuckets,
        rt: &RealtimeIndex,
        stamp: u32,
        out: &mut Vec<Label>,
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
                    created_by: stamp,
                    at_stop: stop as u32,
                    round: r.from_round.saturating_add(1),
                    parent: r.from_arena,
                    arena_id: u32::MAX,
                };
                if best[stop].dominates(cand, buckets) {
                    continue;
                }
                out.push(cand);
            }

            // 2. Board from each prev label at this stop. We board the earliest
            //    catchable trip, then successively-later trips that reach a *higher*
            //    reliability bucket (waiting longer = safer connection), up to CERTAIN.
            //    Reliability is monotonic in departure margin, so buckets only rise.
            let max_bucket = buckets.bucket(1.0);
            if prev[stop].is_reached() {
                for pl in prev[stop].iter() {
                    // Build only from THIS pass's labels: an `i`-journey must descend
                    // from the `i`-source (departure d_i). Boarding from a later
                    // departure's label would fabricate `i`'s journey out of `j`'s.
                    // The carried grid still PRUNES across departures (curr.insert),
                    // it just isn't a build source. Single-pass stamps all 0 → no-op.
                    if pl.created_by != stamp {
                        continue;
                    }
                    let from_bucket = buckets.bucket(pl.reliability);
                    let min_dep = pl.bag.earliest();
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
                        if best_bucket_seen.is_some_and(|bs| cb <= bs) {
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
                                from_round: pl.round,
                                from_arena: pl.arena_id,
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

    /// Applies collected scan candidates in order against the live grids: re-checks
    /// domination on the up-to-date `best` (the collect-time snapshot may lag), then
    /// arena-pushes and inserts. Replaying candidates in queue order makes the
    /// result — including arena ids — identical to a fully sequential scan.
    fn apply_scan_candidates(
        &self,
        cands: &[Label],
        curr: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        arena: &mut Vec<Label>,
    ) {
        for &cand in cands {
            let stop = cand.at_stop as usize;
            // `best` is THIS pass's cross-round bound: it drives `target_cutoff`
            // and the local prune, so it must always capture what this departure
            // can reach (independent of the carried grid) — otherwise the cutoff
            // degrades and the pass explores the whole network. Only *marking* is
            // gated on the carried per-round set, which is what self-prunes earlier
            // departures against later ones.
            if best[stop].dominates(cand, buckets) {
                continue;
            }
            let cand = Label::arena_push(arena, cand);
            best[stop].insert(cand, buckets);
            if curr[stop].insert(cand, buckets) {
                Self::mark(stop, marked, is_marked);
            }
        }
    }

    /// Number of parallel chunks for a round's route-scan queue. 1 = stay
    /// sequential (small queues are not worth the spawn cost).
    /// `MAAS_SCAN_THREADS` overrides the thread budget (1 = force sequential).
    fn scan_chunks(queue_len: usize) -> usize {
        static THREADS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let threads = *THREADS.get_or_init(|| {
            std::env::var("MAAS_SCAN_THREADS")
                .ok()
                .and_then(|s| s.parse().ok())
                .filter(|&n| n >= 1)
                .unwrap_or_else(|| {
                    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
                })
        });
        if queue_len < 32 || threads == 1 {
            return 1;
        }
        threads.min(queue_len / 8).max(1)
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
        stamp: u32,
        arena: &mut Vec<Label>,
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
                    // Transfer only this pass's labels (see scan_route): an `i`-journey
                    // descends from the `i`-source. Single-pass stamps all 0 → no-op.
                    if l.created_by != stamp {
                        continue;
                    }
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
                        created_by: stamp,
                        at_stop: target as u32,
                        round: l.round,
                        parent: l.arena_id,
                        arena_id: u32::MAX,
                    };
                    if best[target].dominates(cand, buckets) {
                        continue;
                    }
                    let cand = Label::arena_push(arena, cand);
                    best[target].insert(cand, buckets);
                    if labels[target].insert(cand, buckets) {
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
    /// Returns times sorted **ascending** (earliest first).
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
        // Self-pruning range RAPTOR (rRAPTOR). One label grid is carried across all
        // interesting departures, which are processed **latest → earliest** so a
        // later-departing journey prunes earlier ones. `best` is reset per pass (it
        // gives the per-pass cross-round local prune + `target_cutoff`); `labels` is
        // carried, and marking is gated on per-round `labels[k]` improvement — the
        // round-stratified bound that preserves the transfers axis across departures.
        // Each pass reconstructs its own plans (filtered by `created_by`) before the
        // next pass mutates the grid. Output is the 4-D Pareto set
        // (departure ↑, arrival ↓, transfers ↓, reliability ↑); walk is an attribute.
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

                let n_stops = self.raptor.transit_stop_to_node.len();
                let n_patterns = self.raptor.transit_patterns.len();
                let mut best = vec![LabelSet::EMPTY; n_stops];
                let mut labels = vec![vec![LabelSet::EMPTY; n_stops]; MAX_ROUNDS + 1];
                let mut marked = Vec::with_capacity(2048);
                let mut is_marked = vec![false; n_stops];
                let mut queue = Vec::with_capacity(512);
                let mut queue_pos = vec![u32::MAX; n_patterns];
                let mut arena: Vec<Label> = Vec::new();

                let mut times = departure_times;
                times.sort_unstable_by(|a, b| b.cmp(a)); // latest first

                let mut all_plans = Vec::new();
                for (i, t) in times.into_iter().enumerate() {
                    let stamp = i as u32;
                    let sources_t: Vec<(usize, u32)> = raw_stops.iter()
                        .map(|&(s, w)| (s, t + w))
                        .collect();
                    self.run_departure_into(
                        &sources_t, targets, t, access_secs, date, weekday, buckets, slack, rt,
                        stamp, true,
                        &mut best, &mut labels, &mut marked, &mut is_marked, &mut queue, &mut queue_pos,
                        &mut arena,
                    );
                    let plans = self.extract_with_debug(
                        &sources_t, targets, t, date, weekday, &labels, buckets, origin, destination, rt,
                        None, stamp, &arena,
                    );
                    all_plans.extend(plans);
                }
                Self::pareto_filter(all_plans, buckets)
            })
    }

    /// Reference range driver: each interesting departure runs as an independent
    /// from-scratch RAPTOR pass. Kept as the correctness oracle for the self-pruning
    /// `raptor_range_tuned_rt` (their 4-D Pareto outputs must be set-equal).
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_independent_rt(
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

    /// Public reference range query (default buckets / no realtime), mirroring
    /// `raptor_range` but using the independent-passes oracle. For tests.
    pub fn raptor_range_independent(
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
        self.raptor_range_independent_rt(origin, destination, start_time, window_secs, date, weekday,
            min_access_secs, &buckets, self.raptor.arrival_slack_secs, &RealtimeIndex::new())
    }
    /// Trips departing between midnight and this threshold may actually be
    /// overnight extensions of the previous service day (GTFS times > 86400).
    const OVERNIGHT_THRESHOLD_SECS: u32 = 5 * 3600;

    /// Rotate a 7-bit weekday bitmask one day backward (Mon=0x01 → Sun=0x40).
    fn prev_weekday(wd: u8) -> u8 {
        ((wd >> 1) | ((wd & 1) << 6)) & 0x7F
    }

    /// Subtract `shift` from every time field in a Plan (departure/arrival/step times).
    /// `date`/`weekday` in leg steps are left untouched — they remain the previous
    /// service day's values, which is correct for previous/next departure lookups.
    fn shift_plan(mut plan: Plan, shift: u32) -> Plan {
        plan.start = plan.start.saturating_sub(shift);
        plan.end = plan.end.saturating_sub(shift);
        plan.expected_end = plan.expected_end.saturating_sub(shift);
        for s in &mut plan.arrival_distribution {
            s.time = s.time.saturating_sub(shift);
        }
        for leg in &mut plan.legs {
            match leg {
                PlanLeg::Walk(w) => {
                    w.start = w.start.saturating_sub(shift);
                    w.end = w.end.saturating_sub(shift);
                    for step in &mut w.steps {
                        *step = match *step {
                            PlanLegStep::Walk(mut ws) => { ws.time = ws.time.saturating_sub(shift); PlanLegStep::Walk(ws) }
                            PlanLegStep::Transit(mut ts) => { ts.time = ts.time.saturating_sub(shift); PlanLegStep::Transit(ts) }
                        };
                    }
                }
                PlanLeg::Transit(t) => {
                    t.start = t.start.saturating_sub(shift);
                    t.end = t.end.saturating_sub(shift);
                    t.scheduled_start = t.scheduled_start.saturating_sub(shift);
                    t.scheduled_end = t.scheduled_end.saturating_sub(shift);
                    if let Some(tr) = &mut t.transfer_risk {
                        tr.scheduled_departure = tr.scheduled_departure.saturating_sub(shift);
                        tr.next_departure = tr.next_departure.map(|d| d.saturating_sub(shift));
                    }
                    t.preceding_arrival = t.preceding_arrival.map(|a| a.saturating_sub(shift));
                    for step in &mut t.steps {
                        *step = match *step {
                            PlanLegStep::Walk(mut ws) => { ws.time = ws.time.saturating_sub(shift); PlanLegStep::Walk(ws) }
                            PlanLegStep::Transit(mut ts) => { ts.time = ts.time.saturating_sub(shift); PlanLegStep::Transit(ts) }
                        };
                    }
                    t.time_shift = shift;
                }
            }
        }
        plan
    }

    /// Like `raptor_tuned_rt` but also finds overnight trips from the previous
    /// service day when querying in the early-morning window (before 05:00).
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned_rt_overnight(
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
        let mut plans = self.raptor_tuned_rt(origin, destination, start_time, date, weekday, min_access_secs, buckets, slack, rt);

        if start_time < Self::OVERNIGHT_THRESHOLD_SECS && date > 0 {
            let overnight = self.raptor_tuned_rt(
                origin, destination,
                start_time + 86400,
                date - 1, Self::prev_weekday(weekday),
                min_access_secs, buckets, slack, rt,
            );
            let normalized: Vec<Plan> = overnight.into_iter().map(|p| Self::shift_plan(p, 86400)).collect();
            if !normalized.is_empty() {
                plans.extend(normalized);
                plans = Self::pareto_filter(plans, buckets);
            }
        }

        plans
    }

    /// Like `raptor_range_tuned_rt` but also finds overnight trips from the
    /// previous service day when querying in the early-morning window.
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned_rt_overnight(
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
        let mut plans = self.raptor_range_tuned_rt(origin, destination, start_time, window_secs, date, weekday, min_access_secs, buckets, slack, rt);

        if start_time < Self::OVERNIGHT_THRESHOLD_SECS && date > 0 {
            let overnight = self.raptor_range_tuned_rt(
                origin, destination,
                start_time + 86400,
                window_secs,
                date - 1, Self::prev_weekday(weekday),
                min_access_secs, buckets, slack, rt,
            );
            let normalized: Vec<Plan> = overnight.into_iter().map(|p| Self::shift_plan(p, 86400)).collect();
            if !normalized.is_empty() {
                plans.extend(normalized);
                plans = Self::pareto_filter(plans, buckets);
            }
        }

        plans
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
            created_by: 0,
            at_stop: u32::MAX,
            round: 0,
            parent: u32::MAX,
            arena_id: u32::MAX,
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
            created_by: 0,
            at_stop: u32::MAX,
            round: 0,
            parent: u32::MAX,
            arena_id: u32::MAX,
        };
        assert_eq!(risky_early.bag.earliest(), 100);
        assert!(s.insert(risky_early, &b));
        // Later scheduled arrival, same bucket -> must be rejected (no override).
        assert!(!s.insert(lbl(200, 1.0), &b));
        assert_eq!(s.iter().count(), 1);
        assert_eq!(s.earliest(), 100, "earliest-scheduled label must survive");
    }

    fn lbl_stamped(time: u32, rel: f32, stamp: u32) -> Label {
        let mut l = lbl(time, rel);
        l.created_by = stamp;
        l
    }

    /// Cross-departure pruning must compare PRECISE reliability, not buckets.
    /// A carried label from a later departure (older stamp) with the same bucket
    /// but lower precise reliability must NOT suppress the new label — their
    /// extensions can quantize to different final buckets (the ~4% range misses).
    #[test]
    fn labelset_cross_stamp_prune_requires_precise_reliability() {
        let b = ReliabilityBuckets::default(); // edges [0.5, 0.8, 0.95]
        let mut s = LabelSet::EMPTY;
        // Carried ghost from stamp 0: bucket 2 (0.80..0.95), arrival 100.
        assert!(s.insert(lbl_stamped(100, 0.81, 0), &b));
        // New pass (stamp 1), same bucket & arrival but HIGHER precise reliability:
        // must be inserted, not bucket-pruned.
        assert!(
            s.insert(lbl_stamped(100, 0.94, 1), &b),
            "cross-stamp bucket prune dropped a higher-precise-reliability label"
        );
        // Same situation but the ghost's precise reliability is >= the candidate's:
        // the prune IS sound and must still fire.
        let mut s2 = LabelSet::EMPTY;
        assert!(s2.insert(lbl_stamped(100, 0.94, 0), &b));
        assert!(!s2.insert(lbl_stamped(100, 0.81, 1), &b));
    }

    /// Cross-stamp pruning must respect the transfers axis: a ghost that used MORE
    /// transit legs (higher creation round) must not prune a label with fewer legs,
    /// even at better arrival/reliability — the pruned label's extensions can win
    /// the output Pareto filter on the transfers axis.
    #[test]
    fn labelset_cross_stamp_prune_respects_transfer_count() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        // Ghost from stamp 0: 3 transit legs, arrival 100, reliability 0.99.
        let mut ghost = lbl_stamped(100, 0.99, 0);
        ghost.round = 3;
        assert!(s.insert(ghost, &b));
        // Carried label from stamp 1 with only 1 leg, worse arrival/rel: must coexist.
        let mut cand = lbl_stamped(120, 0.85, 1);
        cand.round = 1;
        assert!(
            s.insert(cand, &b),
            "ghost with more transit legs pruned a fewer-leg label (transfers axis)"
        );
        // Same ghost but with <= legs soundly dominates.
        let mut s2 = LabelSet::EMPTY;
        let mut g2 = lbl_stamped(100, 0.99, 0);
        g2.round = 1;
        assert!(s2.insert(g2, &b));
        let mut c2 = lbl_stamped(120, 0.85, 1);
        c2.round = 1;
        assert!(!s2.insert(c2, &b));
    }

    /// Within one departure (same stamp) bucket-level domination is the contract
    /// (it matches the bucketed output) and must keep working unchanged.
    #[test]
    fn labelset_same_stamp_prune_stays_bucketed() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl_stamped(100, 0.81, 1), &b));
        assert!(!s.insert(lbl_stamped(100, 0.94, 1), &b), "same-stamp same-bucket same-arrival must stay pruned");
    }

    /// When a cell is full, old-stamp ghosts (pruning-only, already extracted)
    /// must be evicted before a non-dominated current-stamp label is sacrificed.
    #[test]
    fn labelset_full_cell_evicts_ghost_before_current_stamp() {
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        // Fill the cell with ghosts from MAX_LABELS earlier departures, all in the
        // top bucket, arrival and precise reliability both increasing — pairwise
        // non-dominated cross-stamp, so they all coexist.
        for i in 0..MAX_LABELS {
            let g = lbl_stamped(100 + i as u32, 0.951 + 0.003 * i as f32, i as u32);
            assert!(s.insert(g, &b), "ghost {i} should coexist");
        }
        assert_eq!(s.iter().count(), MAX_LABELS);
        // Current-stamp candidate: latest arrival but the highest precise reliability
        // in the cell — no ghost soundly dominates it. The bucket-based worst-
        // replacement would reject it (same bucket, latest arrival); it must instead
        // displace a ghost, because ghosts can never appear in output again.
        let stamp = MAX_LABELS as u32;
        let cand = lbl_stamped(100 + MAX_LABELS as u32 + 5, 0.9999, stamp);
        assert!(s.insert(cand, &b), "current-stamp label lost to ghosts in a full cell");
        assert!(s.iter().any(|l| l.created_by == stamp), "current-stamp label must be present");
    }
}
