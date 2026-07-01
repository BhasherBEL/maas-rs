use std::collections::BTreeSet;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{StopTime, TripId},
    structures::{
        ALL_STATES, ActiveModes, NodeID, RealtimeIndex, ReliabilityBuckets, ScenarioBag,
        VehicleState,
        plan::{
            AccessInfo, CandidateStatus, ExplainResult, Plan, PlanCandidate, PlanCoordinate,
            PlanLeg, PlanLegStep, StopPathLeg, StopReach,
        },
        raptor::Trace,
    },
};

use super::{BikeCost, Graph, MAX_ROUNDS, raptor_access::StreetProfile};

/// Projected snap coordinates for a contracted (flag-on) query. The origin/destination
/// `NodeID`s threaded alongside are bounding junctions (stable identity, survive the
/// interior-node drop); these coordinates are the PROJECTED foot-snap points, used for
/// plan endpoint geometry, the straight-line heuristic, and coord-based access/egress —
/// never a junction shortcut. `None` ⇒ flag-off, the NodeID path stays byte-identical.
pub struct QueryEndpoints {
    pub origin: crate::structures::LatLng,
    pub destination: crate::structures::LatLng,
    pub origin_station: Option<Vec<usize>>,
    pub destination_station: Option<Vec<usize>>,
}

/// Per-query mode resolution: the active vehicle states plus per-state
/// access/egress stop lists (walk/ride seconds, relative to the endpoint).
/// Grid cells are addressed as `stop * n_states + state`.
pub(super) struct ModeContext<'a> {
    pub am: &'a ActiveModes,
    pub access: Vec<Vec<(usize, u32)>>,
    pub egress: Vec<Vec<(usize, u32)>>,
    pub dest_station: Option<Vec<usize>>,
}

impl<'a> ModeContext<'a> {
    /// Builds the per-state lists from per-profile stop lists.
    /// Access: `Walked`/`CarEgress` walk, bike states ride, `CarParked` drives.
    /// Egress: `BikeInHand` rides, `CarEgress` is driven, everything else walks.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        am: &'a ActiveModes,
        foot_access: &[(usize, u32)],
        bike_access: &[(usize, u32)],
        car_access: &[(usize, u32)],
        foot_egress: &[(usize, u32)],
        bike_egress: &[(usize, u32)],
        car_egress: &[(usize, u32)],
        dest_station: Option<&[usize]>,
    ) -> Self {
        let mut access = vec![Vec::new(); am.n_states()];
        let mut egress = vec![Vec::new(); am.n_states()];
        for (sidx, vs) in am.states() {
            access[sidx] = match vs {
                VehicleState::Walked | VehicleState::CarEgress | VehicleState::BikeEgress => {
                    foot_access.to_vec()
                }
                VehicleState::BikeInHand | VehicleState::BikeDropped => bike_access.to_vec(),
                VehicleState::CarParked => car_access.to_vec(),
            };
            egress[sidx] = match vs {
                VehicleState::Walked | VehicleState::BikeDropped | VehicleState::CarParked => {
                    foot_egress.to_vec()
                }
                VehicleState::BikeInHand | VehicleState::BikeEgress => bike_egress.to_vec(),
                VehicleState::CarEgress => car_egress.to_vec(),
            };
        }
        ModeContext {
            am,
            access,
            egress,
            dest_station: dest_station.map(|p| p.to_vec()),
        }
    }

    pub fn n_states(&self) -> usize {
        self.am.n_states()
    }

    pub fn any_access(&self) -> bool {
        self.access.iter().any(|a| !a.is_empty())
    }

    pub fn any_egress(&self) -> bool {
        self.egress.iter().any(|e| !e.is_empty())
    }

    /// Union of all states' access stops (min seconds per stop, sorted by stop).
    /// Used by the range driver's interesting-departure collection.
    pub fn merged_access(&self) -> Vec<(usize, u32)> {
        let mut best: std::collections::HashMap<usize, u32> = std::collections::HashMap::new();
        for list in &self.access {
            for &(s, w) in list {
                let e = best.entry(s).or_insert(u32::MAX);
                *e = (*e).min(w);
            }
        }
        let mut v: Vec<(usize, u32)> = best.into_iter().collect();
        v.sort_unstable_by_key(|&(s, _)| s);
        v
    }

    /// `(in_hand_idx, dropped_idx)` when the free drop transition is active.
    pub fn drop_transition(&self) -> Option<(u8, u8)> {
        match (
            self.am.state_of(VehicleState::BikeInHand),
            self.am.state_of(VehicleState::BikeDropped),
        ) {
            (Some(i), Some(d)) => Some((i as u8, d as u8)),
            _ => None,
        }
    }
}

/// Maximum labels kept per `(round, stop)` cell. One per reliability bucket, so this
/// bounds the supported bucket count (edges.len()+2). Default config uses 5.
pub(super) const MAX_LABELS: usize = 16;

/// Apply a signed realtime delay (seconds) to a scheduled time, clamped at 0.
#[inline]
fn apply_delay(scheduled: u32, delay: i32) -> u32 {
    (scheduled as i64 + delay as i64).max(0) as u32
}

/// One remaining downstream stop of an onboard ride: where the boarded vehicle
/// will next stop, and when (realtime arrival = scheduled + live delay).
#[derive(Clone, Copy, Debug)]
pub struct OnboardSeed {
    /// Pattern position of this downstream stop (`> current_pos`).
    pub alighted_at: u32,
    /// Compact stop index reached.
    pub at_stop: u32,
    /// Realtime arrival time at the stop, seconds since midnight.
    pub arrival: u32,
}

/// A resolved onboard origin: the boarded trip (identified WITHIN its pattern,
/// not as a global `TripId`), the current pattern position, and every remaining
/// downstream stop with its realtime arrival. Seeds the onboard partial-requery.
#[derive(Clone, Debug)]
pub struct OnboardRide {
    /// Pattern carrying the boarded trip.
    pub pattern: u32,
    /// Within-pattern index of the boarded trip (position in `transit_pattern_trips`).
    pub trip_within: u32,
    /// Global GTFS-internal trip id of the boarded trip (for realtime lookups).
    pub trip_id: TripId,
    /// Pattern position the user has just passed / is currently at.
    pub current_pos: u32,
    /// Route type of the boarded trip (feeder route type for downstream transfers).
    pub route_type: Option<RouteType>,
    /// Remaining downstream stops, in pattern order.
    pub seeds: Vec<OnboardSeed>,
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
    /// Compact vehicle-state index of the boarding label (alight into the same state).
    state: u8,
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
    /// Compact vehicle-state index (always 0 in single-state walk routing).
    pub state: u8,
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
        state: 0,
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
        self.iter()
            .map(|l| l.bag.earliest())
            .min()
            .unwrap_or(u32::MAX)
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
                    if ir < gr || (ir == gr && ie > ge) {
                        i
                    } else {
                        g
                    }
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
    /// Foot access/egress stops within `max_secs` of `origin`. Routes over the
    /// contracted graph when enabled (`nearby_stops_union`), else the full graph.
    fn foot_nearby_stops(&self, origin: NodeID, max_secs: u32) -> Vec<(usize, u32)> {
        let cg = self.contracted.as_ref().unwrap();
        self.nearby_stops_union(origin, max_secs, cg)
    }

    /// Foot access/egress stops within `max_secs` of a coord-snapped endpoint when a
    /// contracted query supplies the projected coordinate (`coord`) — g-free, surviving the
    /// interior-node drop. Without it, falls back to the NodeID `foot_nearby_stops`.
    fn foot_nearby_stops_ep(
        &self,
        origin: NodeID,
        max_secs: u32,
        coord: Option<crate::structures::LatLng>,
    ) -> Vec<(usize, u32)> {
        if let Some(c) = coord {
            let cg = self.contracted.as_ref().unwrap();
            let radius = self.raptor.edge_snap_radius_m;
            return cg.nearby_stops_arena(self, c.latitude, c.longitude, radius, max_secs);
        }
        self.foot_nearby_stops(origin, max_secs)
    }

    /// Car access/egress stops within `max_secs` of `origin`. Mirrors
    /// `nearby_stops_union` but over the phased car search; identical shape and
    /// (sorted) order to `nearby_stops_profile(Car)`.
    fn car_nearby_stops(&self, origin: NodeID, max_secs: u32) -> Vec<(usize, u32)> {
        let cg = self.contracted.as_ref().unwrap();
        let dist = self.car_dijkstra_union(origin, max_secs, cg);
        let mut stops: Vec<(usize, u32)> = dist
            .iter()
            .filter_map(|(&jn, &secs)| {
                let compact = self.raptor.transit_node_to_stop[jn.0];
                (compact != u32::MAX).then_some((compact as usize, secs))
            })
            .collect();
        stops.sort_unstable_by_key(|&(stop, _)| stop);
        stops
    }

    /// Foot seconds `origin`→`destination` (`u32::MAX` = unreachable). Routes over
    /// the contracted graph when enabled, else a full-graph walk Dijkstra.
    fn walk_secs_to(&self, origin: NodeID, destination: NodeID, bound: u32) -> u32 {
        let cg = self.contracted.as_ref().unwrap();
        cg.walk_secs_point_to_point(self, origin, destination, bound)
            .unwrap_or(u32::MAX)
    }

    /// Foot seconds `origin`→`destination` keyed on the PROJECTED snap coordinates when a
    /// contracted query supplies them (`ep`) — g-free, so it survives the interior-node
    /// drop. Without `ep` (flag-off, or junction-keyed callers) falls back to the NodeID
    /// `walk_secs_to`, byte-identical.
    fn walk_secs_to_ep(
        &self,
        origin: NodeID,
        destination: NodeID,
        bound: u32,
        ep: Option<&QueryEndpoints>,
    ) -> u32 {
        if let Some(ep) = ep {
            let cg = self.contracted.as_ref().unwrap();
            let radius = self.raptor.edge_snap_radius_m;
            return cg
                .walk_secs_coord_to_coord(self, ep.origin, ep.destination, radius, bound)
                .unwrap_or(u32::MAX);
        }
        self.walk_secs_to(origin, destination, bound)
    }

    /// Straight-line meters between endpoints, using the PROJECTED snap coordinates when a
    /// contracted query supplies them (`ep`), else `nodes_distance`. The 0.99 haversine
    /// discount matches `nodes_distance` so the heuristic is identical for junction inputs.
    fn endpoint_distance(
        &self,
        origin: NodeID,
        destination: NodeID,
        ep: Option<&QueryEndpoints>,
    ) -> usize {
        match ep {
            Some(ep) => (ep.origin.dist(ep.destination) * 0.99) as usize,
            None => self.nodes_distance(origin, destination),
        }
    }

    /// Seconds to the nearest transit stop from a query origin, keyed on the projected
    /// snap coordinate when available (g-free), else `nearest_stop_secs(node)`.
    fn nearest_stop_secs_ep(
        &self,
        origin: NodeID,
        straight_line_secs: u32,
        coord: Option<crate::structures::LatLng>,
    ) -> u32 {
        match coord {
            Some(c) => self.nearest_stop_secs_coord(c, straight_line_secs),
            None => self.nearest_stop_secs(origin, straight_line_secs),
        }
    }

    /// Car seconds `origin`→`destination` (`None` = unreachable). Routes over the
    /// contracted graph when enabled, else a full-graph car Dijkstra.
    fn car_secs_to(&self, origin: NodeID, destination: NodeID, bound: u32) -> Option<u32> {
        let cg = self.contracted.as_ref().unwrap();
        self.car_dijkstra_union(origin, bound, cg)
            .get(&destination)
            .copied()
    }

    /// Enumerates the onboard ride seeds for a user currently aboard trip
    /// `trip_within` (within-pattern index) of `pattern`, having passed pattern
    /// position `current_pos`. Every downstream stop (`pos > current_pos`) is a
    /// reached label at its realtime arrival (scheduled + `rt.delay`). Stops at or
    /// before `current_pos` are excluded; arrivals are monotonic non-decreasing.
    pub fn build_onboard_ride(
        &self,
        pattern: u32,
        trip_within: u32,
        current_pos: u32,
        rt: &RealtimeIndex,
    ) -> OnboardRide {
        let p = pattern as usize;
        let t = trip_within as usize;
        let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
        let pat_stops =
            self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);
        let times = self.raptor.transit_idx_pattern_stop_times[p]
            .of(&self.raptor.transit_pattern_stop_times);
        let trip_ids =
            self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);
        let trip_id = trip_ids[t];
        let route_type = self.route_type_of_trip(trip_id);

        let mut seeds = Vec::new();
        for pos in (current_pos as usize + 1)..pat_stops.len() {
            let compact = self.raptor.transit_node_to_stop[pat_stops[pos].0];
            if compact == u32::MAX {
                continue;
            }
            let sched_arr = times[pos * n_trips + t].arrival;
            let arrival = apply_delay(sched_arr, rt.delay(trip_id, compact));
            seeds.push(OnboardSeed {
                alighted_at: pos as u32,
                at_stop: compact,
                arrival,
            });
        }

        OnboardRide {
            pattern,
            trip_within,
            trip_id,
            current_pos,
            route_type,
            seeds,
        }
    }

    /// Locates a boarded `trip` within its pattern and resolves the current
    /// pattern position. `from_stop` (compact index) or `from_seq` (pattern
    /// position) are advisory overrides; without either, the current position is
    /// the last pattern stop whose realtime departure is `<= now`. Returns
    /// `(pattern, within-pattern trip index, current_pos)`, or `None` when the
    /// trip is unknown or the user is already at the trip's final stop (nothing
    /// downstream to route).
    pub fn locate_onboard_trip(
        &self,
        trip: TripId,
        from_stop: Option<usize>,
        from_seq: Option<u32>,
        now: u32,
        rt: &RealtimeIndex,
    ) -> Option<(u32, u32, u32)> {
        let (p, t) = self
            .raptor
            .transit_idx_pattern_trips
            .iter()
            .enumerate()
            .find_map(|(p, lk)| {
                lk.of(&self.raptor.transit_pattern_trips)
                    .iter()
                    .position(|&x| x == trip)
                    .map(|t| (p, t))
            })?;
        let pat_stops =
            self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);
        let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
        let times = self.raptor.transit_idx_pattern_stop_times[p]
            .of(&self.raptor.transit_pattern_stop_times);

        let current_pos = if let Some(stop) = from_stop {
            pat_stops
                .iter()
                .position(|&n| self.raptor.transit_node_to_stop[n.0] as usize == stop)?
                as u32
        } else if let Some(seq) = from_seq {
            seq.min(pat_stops.len().saturating_sub(1) as u32)
        } else {
            let mut pos = 0u32;
            for (i, &node) in pat_stops.iter().enumerate() {
                let compact = self.raptor.transit_node_to_stop[node.0];
                let dep = apply_delay(times[i * n_trips + t].departure, rt.delay(trip, compact));
                if dep <= now {
                    pos = i as u32;
                } else {
                    break;
                }
            }
            pos
        };

        if current_pos as usize + 1 >= pat_stops.len() {
            return None;
        }
        Some((p as u32, t as u32, current_pos))
    }

    /// Egress-only onboard partial-requery: seed the boarded trip's remaining
    /// downstream stops (round 0, one transit leg each) and run the normal rounds
    /// + transfers onward to a lat/lng `destination`. No access side, no radius
    /// widening. Phase 1 routes WalkTransit egress (state 0 = `Walked`).
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_onboard_tuned_rt_modes_ep(
        &self,
        ride: &OnboardRide,
        destination: NodeID,
        date: u32,
        weekday: u8,
        egress_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        if ride.seeds.is_empty() {
            return Vec::new();
        }
        let foot_egress = self.egress_times(self.foot_nearby_stops_ep(
            destination,
            egress_secs,
            ep.map(|e| e.destination),
        ));
        let mc = ModeContext::build(am, &[], &[], &[], &foot_egress, &[], &[], None);
        if !mc.any_egress() {
            return Vec::new();
        }
        let plans = self.raptor_onboard_inner(&mc, ride, date, weekday, destination, buckets, slack, rt);
        Self::finalize_plans(plans, buckets)
    }

    /// Production onboard RAPTOR core: allocates the per-pass grids, seeds the
    /// onboard ride via `run_departure_into`, and extracts. Mirrors the non-debug
    /// `raptor_inner` setup but injects onboard seeds in place of foot access.
    #[allow(clippy::too_many_arguments)]
    fn raptor_onboard_inner(
        &self,
        mc: &ModeContext,
        ride: &OnboardRide,
        date: u32,
        weekday: u8,
        destination: NodeID,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> Vec<Plan> {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let n_states = mc.n_states();
        let n_cells = n_stops * n_states;
        let n_patterns = self.raptor.transit_patterns.len();

        let mut best = vec![LabelSet::EMPTY; n_cells];
        let mut labels = vec![vec![LabelSet::EMPTY; n_cells]; MAX_ROUNDS + 1];
        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_cells];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];
        let mut arena: Vec<Label> = Vec::new();

        self.run_departure_into(
            mc,
            0,
            0,
            date,
            weekday,
            buckets,
            slack,
            rt,
            0,
            false,
            &mut best,
            &mut labels,
            &mut marked,
            &mut is_marked,
            &mut queue,
            &mut queue_pos,
            &mut arena,
            Some(ride),
        );

        self.extract_with_debug(
            mc,
            0,
            date,
            weekday,
            &labels,
            buckets,
            destination,
            destination,
            rt,
            None,
            0,
            &arena,
            true,
        )
    }

    /// Retry loop shared by `raptor` and `raptor_range`.
    ///
    /// Doubles `access_secs` until `try_routing` returns a non-empty result or
    /// the walk-only time is reached (at which point a walk-only plan is returned).
    /// `try_routing(raw_stops, targets, access_secs)` receives the current stop
    /// lists and walk radius and returns candidate plans (empty = no result yet).
    #[allow(clippy::too_many_arguments)]
    fn with_access_search<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        slack: u32,
        buckets: &ReliabilityBuckets,
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
        mut try_routing: F,
    ) -> Vec<Plan>
    where
        F: FnMut(&ModeContext, u32) -> Vec<Plan>,
    {
        // No transit mode selected: the direct street plans are the whole answer.
        if !am.wants_transit() {
            let walk_secs = if am.wants_direct_walk() {
                self.walk_secs_to_ep(origin, destination, u32::MAX, ep)
            } else {
                u32::MAX
            };
            let plans = self.direct_fallback_plans(
                am,
                origin,
                destination,
                start_time,
                walk_secs,
                bike,
                terminal_deadline,
                ep,
            );
            return Self::finalize_plans(plans, buckets);
        }

        let straight_line_secs =
            (self.endpoint_distance(origin, destination, ep) as f64 / self.raptor.walking_speed_mps)
                as u32;

        let both_stations = ep.is_some_and(|e| {
            e.origin_station.is_some() && e.destination_station.is_some()
        });

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self
            .nearest_stop_secs_ep(origin, straight_line_secs, ep.map(|e| e.origin))
            .max(min_access_secs);

        loop {
            let mc = self.build_mode_context(am, origin, destination, access_secs, bike, ep);

            if mc.any_access() && mc.any_egress() {
                let mut results = try_routing(&mc, access_secs);
                if !results.is_empty() {
                    self.append_bounded_direct_plans(
                        am,
                        origin,
                        destination,
                        start_time,
                        slack,
                        bike,
                        terminal_deadline,
                        &mut results,
                        ep,
                    );
                    return Self::finalize_plans(results, buckets);
                }
            }

            if both_stations {
                let actual = self.walk_secs_to_ep(origin, destination, u32::MAX, ep);
                let plans = self.direct_fallback_plans(
                    am,
                    origin,
                    destination,
                    start_time,
                    actual,
                    bike,
                    terminal_deadline,
                    ep,
                );
                return Self::finalize_plans(plans, buckets);
            }

            access_secs = access_secs.saturating_mul(2);

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(self.walk_secs_to_ep(origin, destination, u32::MAX, ep));
            }

            if let Some(actual) = walk_only_secs
                && access_secs >= actual
            {
                let plans = self.direct_fallback_plans(
                    am,
                    origin,
                    destination,
                    start_time,
                    actual,
                    bike,
                    terminal_deadline,
                    ep,
                );
                return Self::finalize_plans(plans, buckets);
            }
        }
    }

    /// Inflate access-leg seconds to the conservative percentile (buffer the
    /// connection). Stop ids are unchanged; only the seconds are transformed.
    pub(crate) fn access_times(&self, stops: Vec<(usize, u32)>) -> Vec<(usize, u32)> {
        let m = &self.raptor.street_time;
        stops
            .into_iter()
            .map(|(s, t)| (s, m.access_secs(t)))
            .collect()
    }

    /// Adjust egress-leg seconds to the distribution mean (honest arrival).
    pub(crate) fn egress_times(&self, stops: Vec<(usize, u32)>) -> Vec<(usize, u32)> {
        let m = &self.raptor.street_time;
        stops
            .into_iter()
            .map(|(s, t)| (s, m.egress_secs(t)))
            .collect()
    }

    /// The bike/car access budget (seconds) for a trip whose crow-flies walk-time is
    /// `crow_secs`: a fraction of the trip, clamped to the floor (short trips keep the
    /// local radius) and the ceiling (keep the access Dijkstra bounded).
    pub(crate) fn vehicle_access_budget(&self, crow_secs: u32) -> u32 {
        ((self.raptor.vehicle_access_fraction * crow_secs as f64) as u32)
            .clamp(self.raptor.vehicle_access_secs, self.raptor.vehicle_access_max_secs)
    }

    /// Per-profile access/egress stop discovery for the active states. Each list
    /// is computed only when some active state needs it: foot access for
    /// `Walked`/`CarEgress`, bike access for the bike states, car access for
    /// `CarParked`; egress mirror-imaged (foot for parked/dropped/walked, bike
    /// for `BikeInHand`, car for `CarEgress`).
    fn build_mode_context<'a>(
        &self,
        am: &'a ActiveModes,
        origin: NodeID,
        destination: NodeID,
        access_secs: u32,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
    ) -> ModeContext<'a> {
        use VehicleState::*;
        let has = |s| am.state_of(s).is_some();
        // Foot access stays local (the nearest-stop radius). Bike/car reach a
        // better hub farther out, so their discovery uses a wider budget that scales
        // with trip length — on a long journey you would ride farther to reach a
        // well-connected hub. Clamped to a floor (short trips keep the local radius)
        // and a ceiling (keep the access Dijkstra bounded).
        let crow_secs = (self.endpoint_distance(origin, destination, ep) as f64
            / self.raptor.walking_speed_mps) as u32;
        let vehicle_secs = access_secs.max(self.vehicle_access_budget(crow_secs));

        let station_zero = |platforms: &[usize]| -> Vec<(usize, u32)> {
            platforms.iter().map(|&s| (s, 0)).collect()
        };
        let origin_station = ep.and_then(|e| e.origin_station.as_deref());
        let dest_station = ep.and_then(|e| e.destination_station.as_deref());

        let foot_access = if let Some(p) = origin_station {
            station_zero(p)
        } else if has(Walked) || has(CarEgress) || has(BikeEgress) {
            self.access_times(self.foot_nearby_stops_ep(origin, access_secs, ep.map(|e| e.origin)))
        } else {
            vec![]
        };
        let bike_access = if let Some(p) = origin_station {
            station_zero(p)
        } else if has(BikeInHand) || has(BikeDropped) {
            self.access_times(self.bike_nearby_stops(origin, vehicle_secs, bike))
        } else {
            vec![]
        };
        let car_access = if let Some(p) = origin_station {
            station_zero(p)
        } else if has(CarParked) {
            self.access_times(self.car_nearby_stops(origin, vehicle_secs))
        } else {
            vec![]
        };
        let foot_egress = if let Some(p) = dest_station {
            station_zero(p)
        } else if has(Walked) || has(BikeDropped) || has(CarParked) {
            self.egress_times(self.foot_nearby_stops_ep(
                destination,
                access_secs,
                ep.map(|e| e.destination),
            ))
        } else {
            vec![]
        };
        let bike_egress = if let Some(p) = dest_station {
            station_zero(p)
        } else if has(BikeInHand) || has(BikeEgress) {
            self.egress_times(self.bike_nearby_stops(destination, vehicle_secs, bike))
        } else {
            vec![]
        };
        let car_egress = if let Some(p) = dest_station {
            station_zero(p)
        } else if has(CarEgress) {
            self.egress_times(self.car_nearby_stops(destination, vehicle_secs))
        } else {
            vec![]
        };

        // A bike/car can drive far enough to reach a stop that is also within
        // egress range of the destination. Such a stop is a place you'd simply
        // *arrive* at — boarding transit there to reach that same destination is
        // pointless, and seeding it would let a round-0 "drove there" label
        // poison `target_cutoff` and Pareto-dominate the real transit arrivals at
        // that stop, collapsing park&ride to a walk fallback. Drop those stops
        // from the vehicle access lists (foot access is left alone: its overlap on
        // very short trips is the desirable "don't ride slower than walking").
        let egress_stops: std::collections::HashSet<usize> = foot_egress
            .iter()
            .chain(bike_egress.iter())
            .chain(car_egress.iter())
            .map(|&(s, _)| s)
            .collect();
        let mut bike_access = bike_access;
        let mut car_access = car_access;
        if !egress_stops.is_empty() {
            bike_access.retain(|&(s, _)| !egress_stops.contains(&s));
            car_access.retain(|&(s, _)| !egress_stops.contains(&s));
        }

        ModeContext::build(
            am,
            &foot_access,
            &bike_access,
            &car_access,
            &foot_egress,
            &bike_egress,
            &car_egress,
            dest_station,
        )
    }

    /// Appends direct street plans that arrive within `best transit arrival +
    /// slack` — the only window in which they can survive the final Pareto.
    /// Bike-direct is a candidate whenever a bike mode is in play (a
    /// bike+transit plan must also beat plain cycling); walk-direct only when
    /// `WALK` is selected without `WALK_TRANSIT` (with it, the legacy
    /// walk-fallback semantics apply unchanged).
    #[allow(clippy::too_many_arguments)]
    fn append_bounded_direct_plans(
        &self,
        am: &ActiveModes,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        slack: u32,
        bike: &BikeCost,
        _terminal_deadline: bool,
        results: &mut Vec<Plan>,
        ep: Option<&QueryEndpoints>,
    ) {
        let best_end = match results.iter().map(|p| p.end).min() {
            Some(e) => e,
            None => return,
        };
        let bound = best_end.saturating_sub(start_time).saturating_add(slack);

        if (am.wants_direct_bike() || am.state_of(VehicleState::BikeInHand).is_some())
            && let Some(scalar) =
                self.build_bike_plan_ep(origin, destination, start_time, bound, bike, ep)
        {
            results.push(scalar);
        }
        if am.wants_direct_car() {
            if let Some(secs) = self.car_secs_to(origin, destination, bound) {
                results.push(self.build_street_plan_ep(
                    origin,
                    destination,
                    start_time,
                    secs,
                    StreetProfile::Car,
                    ep,
                ));
            }
        }
        if am.wants_direct_walk() && !am.selected(crate::structures::Mode::WalkTransit) {
            let secs = self.walk_secs_to_ep(origin, destination, bound, ep);
            if secs < u32::MAX {
                results.push(self.build_walk_plan_ep(origin, destination, start_time, secs, ep));
            }
        }
    }

    /// Direct (no-transit) plans returned when transit routing finds nothing.
    /// `walk_secs` is the full walk time to the destination (`u32::MAX` = unreachable).
    fn direct_fallback_plans(
        &self,
        am: &ActiveModes,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
        bike: &BikeCost,
        _terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        let mut plans = Vec::new();
        if walk_secs < u32::MAX {
            plans.push(self.build_walk_plan_ep(origin, destination, start_time, walk_secs, ep));
        }
        if am.wants_direct_bike() || am.state_of(VehicleState::BikeInHand).is_some() {
            if let Some(scalar) =
                self.build_bike_plan_ep(origin, destination, start_time, u32::MAX, bike, ep)
            {
                plans.push(scalar);
            }
        }
        if am.wants_direct_car() {
            if let Some(car_secs) = self.car_secs_to(origin, destination, u32::MAX) {
                plans.push(self.build_street_plan_ep(
                    origin,
                    destination,
                    start_time,
                    car_secs,
                    StreetProfile::Car,
                    ep,
                ));
            }
        }
        plans
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
        self.raptor_modes(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            &ActiveModes::default(),
        )
    }

    /// `raptor` over an explicit mode selection.
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_modes(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        am: &ActiveModes,
    ) -> Vec<Plan> {
        let buckets = ReliabilityBuckets::new(&self.raptor.reliability_bucket_edges);
        self.raptor_tuned_rt_modes(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            &buckets,
            self.raptor.arrival_slack_secs,
            &RealtimeIndex::new(),
            am,
            &self.default_bike_cost(),
            false,
        )
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
        self.raptor_tuned_rt(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            &RealtimeIndex::new(),
        )
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
        self.raptor_tuned_rt_modes(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned_rt_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
    ) -> Vec<Plan> {
        self.raptor_tuned_rt_modes_ep(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            am,
            bike,
            terminal_deadline,
            None,
        )
    }

    /// `raptor_tuned_rt_modes` carrying the projected snap coordinates (`ep`) for g-free
    /// contracted access/geometry. `None` ⇒ NodeID path, byte-identical.
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned_rt_modes_ep(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            buckets,
            am,
            bike,
            terminal_deadline,
            ep,
            |mc, access_secs| {
                self.raptor_inner(
                    mc,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                    buckets,
                    slack,
                    rt,
                )
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn raptor_inner(
        &self,
        mc: &ModeContext,
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
            mc,
            start_time,
            access_secs,
            date,
            weekday,
            origin,
            destination,
            buckets,
            slack,
            rt,
            false,
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
        mc: &ModeContext,
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
        let n_states = mc.n_states();
        let n_cells = n_stops * n_states;
        let n_patterns = self.raptor.transit_patterns.len();

        let mut best = vec![LabelSet::EMPTY; n_cells];
        let mut labels = vec![vec![LabelSet::EMPTY; n_cells]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_cells];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];
        let mut arena: Vec<Label> = Vec::new();

        self.run_departure_into(
            mc,
            start_time,
            access_secs,
            date,
            weekday,
            buckets,
            slack,
            rt,
            0,
            false,
            &mut best,
            &mut labels,
            &mut marked,
            &mut is_marked,
            &mut queue,
            &mut queue_pos,
            &mut arena,
            None,
        );

        // Discardable debug survey: only built for `raptor_explain*`.
        let stops_reached: Vec<StopReach> = if want_debug {
            (0..n_stops)
                .filter_map(|stop_idx| {
                    for k in 0..=MAX_ROUNDS {
                        let reached =
                            (0..n_states).any(|s| labels[k][stop_idx * n_states + s].is_reached());
                        if reached {
                            let node_id = self.raptor.transit_stop_to_node[stop_idx];
                            let loc = self.node_loc(node_id);
                            let name = self.raptor.transit_stop_names[stop_idx].clone();
                            let path = self.path_to_stop(stop_idx, k, origin, &labels, n_states);
                            let arrival_secs = (0..n_states)
                                .map(|s| labels[k][stop_idx * n_states + s].earliest())
                                .min()
                                .unwrap_or(u32::MAX);
                            return Some(StopReach {
                                stop_idx: stop_idx as u32,
                                round: k as u8,
                                arrival_secs,
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
        let debug_sink = if want_debug {
            Some(&mut candidates)
        } else {
            None
        };
        let plans = self.extract_with_debug(
            mc,
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
            false,
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
        mc: &ModeContext,
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
        onboard: Option<&OnboardRide>,
    ) {
        let n_states = mc.n_states();
        let n_cells = best.len();
        let drop_to = mc.drop_transition();

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

        // Seed round 0. Onboard partial-requery seeds the boarded trip's remaining
        // downstream stops as already-reached transit labels (one transit leg) in
        // place of foot access; the normal access loop is otherwise untouched.
        if let Some(ride) = onboard {
            for seed in &ride.seeds {
                let lab = Label::arena_push(
                    arena,
                    Label {
                        bag: ScenarioBag::single(seed.arrival),
                        route_type: ride.route_type,
                        reliability: 1.0,
                        trace: Trace {
                            pattern: ride.pattern,
                            trip: ride.trip_within,
                            boarded_at: ride.current_pos,
                            alighted_at: seed.alighted_at,
                            from_stop: u32::MAX,
                            from_bucket: 0,
                        },
                        created_by: stamp,
                        at_stop: seed.at_stop,
                        round: 1,
                        parent: u32::MAX,
                        arena_id: u32::MAX,
                        state: 0,
                    },
                );
                let cell = seed.at_stop as usize * n_states;
                labels[0][cell].insert(lab, buckets);
                best[cell].insert(lab, buckets);
                Self::mark(cell, marked, is_marked);
            }
        } else {
            for (sidx, _vs) in mc.am.states() {
                for &(stop, walk) in &mc.access[sidx] {
                    let lab = Label::arena_push(
                        arena,
                        Label {
                            bag: ScenarioBag::single(start_time + walk),
                            route_type: None,
                            reliability: 1.0,
                            trace: Trace::NONE,
                            created_by: stamp,
                            at_stop: stop as u32,
                            round: 0,
                            parent: u32::MAX,
                            arena_id: u32::MAX,
                            state: sidx as u8,
                        },
                    );
                    let cell = stop * n_states + sidx;
                    labels[0][cell].insert(lab, buckets);
                    best[cell].insert(lab, buckets);
                    Self::mark(cell, marked, is_marked);
                    // Free drop at the access stop (park & ride).
                    if let Some((in_hand, dropped)) = drop_to
                        && sidx as u8 == in_hand
                    {
                        let mut d = lab;
                        d.state = dropped;
                        let d = Label::arena_push(arena, d);
                        let dcell = stop * n_states + dropped as usize;
                        labels[0][dcell].insert(d, buckets);
                        best[dcell].insert(d, buckets);
                        Self::mark(dcell, marked, is_marked);
                    }
                }
            }
        }

        // Round-0 transfer bound: foot access uses the uniform access radius; the
        // onboard seeds have no access radius, so they extend up to the egress-based
        // target cutoff (the only bound that keeps onboard transfers finite).
        let seed_bound = if onboard.is_some() {
            Self::target_cutoff(best, mc, slack)
        } else {
            [start_time + access_secs; ALL_STATES.len()]
        };
        self.apply_transfers(
            &mut labels[0],
            best,
            buckets,
            marked,
            is_marked,
            seed_bound,
            stamp,
            arena,
            n_states,
            drop_to,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev_k = &prev[k - 1];
                let curr_k = &mut rest[0];
                if carried {
                    // Carried-aware carry-forward: keep this round's already-carried
                    // labels and fold in the ≤(k-1)-trip frontier.
                    for cell in 0..n_cells {
                        if prev_k[cell].is_reached() {
                            for lab in prev_k[cell].iter() {
                                curr_k[cell].insert(*lab, buckets);
                            }
                        }
                    }
                } else {
                    curr_k.copy_from_slice(prev_k);
                }
            }

            self.collect_routes(marked, queue, queue_pos, n_states);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(best, mc, slack);

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
                            pat, qp[pat], date, weekday, cutoff, prev_slice, best, buckets, rt,
                            stamp, &mut cands, mc,
                        );
                    }
                    self.apply_scan_candidates(
                        &cands, curr_slice, best, buckets, marked, is_marked, arena, n_states,
                        drop_to,
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
                                            pat, qp[pat], date, weekday, cutoff, prev_slice,
                                            best_ro, buckets, rt, stamp, &mut cands, mc,
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
                            cands, curr_slice, best, buckets, marked, is_marked, arena, n_states,
                            drop_to,
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
                n_states,
                drop_to,
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
        n_states: usize,
    ) -> Vec<StopPathLeg> {
        // Earliest-arriving label at (round, stop) across all vehicle states.
        let min_label_at = |k: usize, stop: usize| -> Option<Label> {
            (0..n_states)
                .filter_map(|s| labels[k][stop * n_states + s].min_arrival_label().copied())
                .min_by_key(|l| l.bag.earliest())
        };

        let mut legs: Vec<StopPathLeg> = Vec::new();
        let mut stop = stop_idx;
        let mut k = round;

        while let Some(l) = min_label_at(k, stop) {
            let trace = l.trace;
            let to_node = self.raptor.transit_stop_to_node[stop];
            let to_loc = self.node_loc(to_node);

            if trace.is_transit() {
                let p = trace.pattern as usize;
                let bp = trace.boarded_at as usize;
                let ap = trace.alighted_at as usize;
                let pat_stops =
                    self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);

                let geometry: Vec<PlanCoordinate> = (bp..=ap)
                    .map(|i| {
                        let loc = self.node_loc(pat_stops[i]);
                        PlanCoordinate {
                            lat: loc.latitude,
                            lon: loc.longitude,
                        }
                    })
                    .collect();

                let route_id = self.raptor.transit_patterns[p].route;
                let route_label = self.raptor.transit_routes[route_id.0 as usize]
                    .route_short_name
                    .clone();

                legs.push(StopPathLeg {
                    is_transit: true,
                    route_label,
                    geometry,
                });

                let boarding_stop = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
                stop = boarding_stop;
                k = k.saturating_sub(1);
            } else if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let from_node = self.raptor.transit_stop_to_node[from];
                let from_loc = self.node_loc(from_node);
                legs.push(StopPathLeg {
                    is_transit: false,
                    route_label: String::new(),
                    geometry: vec![
                        PlanCoordinate {
                            lat: from_loc.latitude,
                            lon: from_loc.longitude,
                        },
                        PlanCoordinate {
                            lat: to_loc.latitude,
                            lon: to_loc.longitude,
                        },
                    ],
                });
                stop = from;
                // k stays the same for transfers
            } else {
                // Access walk: origin → this stop
                let from_loc = self.node_loc(origin);
                legs.push(StopPathLeg {
                    is_transit: false,
                    route_label: String::new(),
                    geometry: vec![
                        PlanCoordinate {
                            lat: from_loc.latitude,
                            lon: from_loc.longitude,
                        },
                        PlanCoordinate {
                            lat: to_loc.latitude,
                            lon: to_loc.longitude,
                        },
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
    #[allow(clippy::too_many_arguments)]
    fn with_access_search_debug<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        mut try_routing: F,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, AccessInfo, Vec<StopReach>)
    where
        F: FnMut(&ModeContext, u32) -> Option<(Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>)>,
    {
        let straight_line_secs =
            (self.endpoint_distance(origin, destination, ep) as f64 / self.raptor.walking_speed_mps)
                as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self
            .nearest_stop_secs_ep(origin, straight_line_secs, ep.map(|e| e.origin))
            .max(min_access_secs);
        let mut attempts: u32 = 0;

        loop {
            let mc = self.build_mode_context(am, origin, destination, access_secs, bike, ep);

            if mc.any_access()
                && mc.any_egress()
                && let Some((plans, candidates, stops)) = try_routing(&mc, access_secs)
            {
                let access = AccessInfo {
                    walk_radius_secs: access_secs,
                    walk_radius_meters: (access_secs as f64 * self.raptor.walking_speed_mps) as u32,
                    origin_stops_found: mc.merged_access().len() as u32,
                    destination_stops_found: mc.egress.iter().map(|e| e.len()).max().unwrap_or(0)
                        as u32,
                    access_attempts: attempts,
                    fell_back_to_walk_only: false,
                };
                return (plans, candidates, access, stops);
            }

            access_secs = access_secs.saturating_mul(2);
            attempts += 1;

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(self.walk_secs_to_ep(origin, destination, u32::MAX, ep));
            }

            if let Some(actual) = walk_only_secs
                && access_secs >= actual
            {
                let plans = self.direct_fallback_plans(
                    am,
                    origin,
                    destination,
                    start_time,
                    actual,
                    bike,
                    false,
                    ep,
                );
                let candidates = plans
                    .iter()
                    .map(|plan| PlanCandidate {
                        round: 0,
                        origin_departure: start_time,
                        plan: Some(plan.clone()),
                        status: CandidateStatus::Kept,
                    })
                    .collect();
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
        self.raptor_explain_tuned(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            &buckets,
            self.raptor.arrival_slack_secs,
        )
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
        self.raptor_explain_tuned_rt(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            &RealtimeIndex::new(),
        )
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
        self.raptor_explain_tuned_rt_modes(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_explain_tuned_rt_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            am,
            bike,
            ep,
            |mc, access_secs| {
                let (plans, cands, stops) = self.raptor_inner_with_debug(
                    mc,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                    buckets,
                    slack,
                    rt,
                    true,
                );
                if plans.is_empty() {
                    None
                } else {
                    Some((plans, cands, stops))
                }
            },
        );
        let (oc, dc) = self.explain_endpoint_coords(origin, destination, ep);
        ExplainResult {
            plans,
            candidates,
            access,
            stops_reached,
            origin: oc,
            destination: dc,
        }
    }

    /// Endpoint coordinates for an `ExplainResult`: the projected snap coords when a
    /// contracted query supplies them (g-free, survive the drop), else `node_coord`.
    fn explain_endpoint_coords(
        &self,
        origin: NodeID,
        destination: NodeID,
        ep: Option<&QueryEndpoints>,
    ) -> (PlanCoordinate, PlanCoordinate) {
        match ep {
            Some(ep) => (
                PlanCoordinate { lat: ep.origin.latitude, lon: ep.origin.longitude },
                PlanCoordinate { lat: ep.destination.latitude, lon: ep.destination.longitude },
            ),
            None => (self.node_coord(origin), self.node_coord(destination)),
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
        self.raptor_range_explain_tuned_rt(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            &RealtimeIndex::new(),
        )
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
        self.raptor_range_explain_tuned_rt_modes(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_explain_tuned_rt_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            am,
            bike,
            ep,
            |mc, access_secs| {
                let (probe, probe_cands, probe_stops) = self.raptor_inner_with_debug(
                    mc,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                    buckets,
                    slack,
                    rt,
                    true,
                );
                if probe.is_empty() {
                    return None;
                }

                let departure_times = self.collect_interesting_times(
                    &mc.merged_access(),
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
                    let (plans_t, mut cands_t, _stops_t) = self.raptor_inner_with_debug(
                        mc,
                        t,
                        access_secs,
                        date,
                        weekday,
                        origin,
                        destination,
                        buckets,
                        slack,
                        rt,
                        true,
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
                        if matches!(c.status, CandidateStatus::Kept) {
                            Some(ci)
                        } else {
                            None
                        }
                    })
                    .collect();

                let final_plans = Self::pareto_filter_with_debug(
                    all_plans,
                    &plan_to_sink_idx,
                    &mut all_candidates,
                    buckets,
                );

                Some((final_plans, all_candidates, probe_stops))
            },
        );
        let (oc, dc) = self.explain_endpoint_coords(origin, destination, ep);
        ExplainResult {
            plans,
            candidates,
            access,
            stops_reached,
            origin: oc,
            destination: dc,
        }
    }

    fn collect_routes(
        &self,
        marked: &[usize],
        queue: &mut Vec<usize>,
        queue_pos: &mut [u32],
        n_states: usize,
    ) {
        for &cell in marked {
            let stop = cell / n_states;
            let pats =
                self.raptor.transit_idx_stop_patterns[stop].of(&self.raptor.transit_stop_patterns);
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
        cutoff: [u32; ALL_STATES.len()],
        prev: &[LabelSet],
        best: &[LabelSet],
        buckets: &ReliabilityBuckets,
        rt: &RealtimeIndex,
        stamp: u32,
        out: &mut Vec<Label>,
        mc: &ModeContext,
    ) {
        let pat_stops =
            self.raptor.transit_idx_pattern_stops[pattern].of(&self.raptor.transit_pattern_stops);
        let n_trips = self.raptor.transit_patterns[pattern].num_trips as usize;
        if n_trips == 0 {
            return;
        }

        let n_states = mc.n_states();
        let in_hand_idx = mc.am.state_of(VehicleState::BikeInHand).map(|i| i as u8);

        let route_id = self.raptor.transit_patterns[pattern].route;
        let pat_rt = self.raptor.transit_routes[route_id.0 as usize].route_type;

        let all_times = self.raptor.transit_idx_pattern_stop_times[pattern]
            .of(&self.raptor.transit_pattern_stop_times);
        let trip_ids =
            self.raptor.transit_idx_pattern_trips[pattern].of(&self.raptor.transit_pattern_trips);

        // Labels currently riding this route. Pareto set over (trip index ↓, bucket ↑):
        // a smaller trip index arrives earlier at every downstream stop.
        let mut riding: Vec<Riding> = Vec::new();

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.raptor.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            // 1. Settle arrivals at this stop for every riding label.
            for r in &riding {
                // GTFS drop_off_type == 1: passengers may not alight here — skip
                // the label write and keep riding, but do not break the loop.
                if !col[r.t].alight_allowed {
                    continue;
                }
                // Realtime: shift the scheduled arrival by the live delay for this
                // trip at this stop (0 when no realtime info — inert default).
                let arr = apply_delay(col[r.t].arrival, rt.delay(trip_ids[r.t], stop as u32));
                if arr >= cutoff[r.state as usize] {
                    continue;
                }
                let bag = if r.hit_prob < 1.0 {
                    let miss_arr = self.next_trip_arrival(trip_ids, r.t + 1, col, date, weekday);
                    match miss_arr {
                        Some(ma) => {
                            ScenarioBag::with_scenarios(arr, r.hit_prob, ma, 1.0 - r.hit_prob)
                        }
                        None => {
                            ScenarioBag::with_scenarios(arr, r.hit_prob, u32::MAX, 1.0 - r.hit_prob)
                        }
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
                    state: r.state,
                };
                if best[stop * n_states + r.state as usize].dominates(cand, buckets) {
                    continue;
                }
                out.push(cand);
            }

            // 2. Board from each prev label at this stop. We board the earliest
            //    catchable trip, then successively-later trips that reach a *higher*
            //    reliability bucket (waiting longer = safer connection), up to CERTAIN.
            //    Reliability is monotonic in departure margin, so buckets only rise.
            let max_bucket = buckets.bucket(1.0);
            for sidx in 0..n_states {
                let prev_set = &prev[stop * n_states + sidx];
                if !prev_set.is_reached() {
                    continue;
                }
                let needs_bikes = in_hand_idx == Some(sidx as u8);
                for pl in prev_set.iter() {
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
                        // GTFS pickup_type == 1: passengers may not board here.
                        if !col[t].board_allowed {
                            continue;
                        }
                        // Carrying a bike: only trips that explicitly allow it.
                        if needs_bikes
                            && self.raptor.transit_trips[trip_ids[t].0 as usize].bikes_allowed
                                != Some(true)
                        {
                            continue;
                        }
                        // Realtime: effective departure = scheduled + live delay.
                        let trip_dep =
                            apply_delay(col[t].departure, rt.delay(trip_ids[t], stop as u32));
                        // `t_start` (a `partition_point`) assumes the column is sorted by
                        // scheduled departure; overtaking trips leave it non-monotonic, so
                        // guard against boarding a trip that departs before the passenger
                        // can reach this stop (`min_dep`) — otherwise a label arrives
                        // before its parent and surfaces as a negative access-walk.
                        if trip_dep < min_dep {
                            continue;
                        }

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
                                        .raptor
                                        .transit_delay_models
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
                                state: sidx as u8,
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
    #[allow(clippy::too_many_arguments)]
    fn apply_scan_candidates(
        &self,
        cands: &[Label],
        curr: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        for &cand in cands {
            // `best` is THIS pass's cross-round bound: it drives `target_cutoff`
            // and the local prune, so it must always capture what this departure
            // can reach (independent of the carried grid) — otherwise the cutoff
            // degrades and the pass explores the whole network. Only *marking* is
            // gated on the carried per-round set, which is what self-prunes earlier
            // departures against later ones.
            Self::insert_candidate(
                self, cand, curr, best, buckets, marked, is_marked, arena, n_states, drop_to,
            );
        }
    }

    /// Inserts `cand` into its `(stop, state)` cell (best-prune → arena → grids →
    /// mark), then — when the candidate is in `BikeInHand` and `BikeDropped` is
    /// active — inserts a state-rewritten copy: the free, irreversible drop.
    #[allow(clippy::too_many_arguments)]
    fn insert_candidate(
        &self,
        cand: Label,
        curr: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        let cell = cand.at_stop as usize * n_states + cand.state as usize;
        if !best[cell].dominates(cand, buckets) {
            let pushed = Label::arena_push(arena, cand);
            best[cell].insert(pushed, buckets);
            if curr[cell].insert(pushed, buckets) {
                Self::mark(cell, marked, is_marked);
            }
        }
        if let Some((in_hand, dropped)) = drop_to
            && cand.state == in_hand
        {
            let mut d = cand;
            d.state = dropped;
            let dcell = d.at_stop as usize * n_states + dropped as usize;
            if !best[dcell].dominates(d, buckets) {
                let pushed = Label::arena_push(arena, d);
                best[dcell].insert(pushed, buckets);
                if curr[dcell].insert(pushed, buckets) {
                    Self::mark(dcell, marked, is_marked);
                }
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
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1)
                })
        });
        if queue_len < 32 || threads == 1 {
            return 1;
        }
        threads.min(queue_len / 8).max(1)
    }

    /// Pareto-inserts a riding label into the route bag over (trip index ↓, bucket ↑).
    /// Domination only applies within the same vehicle state: riders in different
    /// states alight into different cells (and a `Walked` rider must never be
    /// pruned by a bike-state one, or the walk plan disappears before the
    /// plan-level burden comparison).
    fn push_riding(riding: &mut Vec<Riding>, cand: Riding, buckets: &ReliabilityBuckets) {
        let cb = buckets.bucket(cand.reliability);
        for r in riding.iter() {
            if r.state == cand.state && r.t <= cand.t && buckets.bucket(r.reliability) >= cb {
                return; // dominated
            }
        }
        riding.retain(|r| {
            let rb = buckets.bucket(r.reliability);
            !(r.state == cand.state && cand.t <= r.t && cb >= rb && (cand.t < r.t || cb > rb))
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

    #[allow(clippy::too_many_arguments)]
    fn apply_transfers(
        &self,
        labels: &mut [LabelSet],
        best: &mut [LabelSet],
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        cutoff: [u32; ALL_STATES.len()],
        stamp: u32,
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        let n = marked.len();
        for i in 0..n {
            let cell = marked[i];
            let stop = cell / n_states;
            let sidx = (cell % n_states) as u8;
            let state_cutoff = cutoff[sidx as usize];
            let src = labels[cell]; // Copy; releases the borrow on `labels`.
            if !src.is_reached() || src.earliest() >= state_cutoff {
                continue;
            }

            let transfers = self.raptor.transit_idx_stop_transfers[stop]
                .of(&self.raptor.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.raptor.transit_node_to_stop[target_node.0] as usize;

                for l in src.iter() {
                    // Transfer only this pass's labels (see scan_route): an `i`-journey
                    // descends from the `i`-source. Single-pass stamps all 0 → no-op.
                    if l.created_by != stamp {
                        continue;
                    }
                    let bag = l.bag.shifted_by(walk);
                    if bag.earliest() >= state_cutoff {
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
                        state: sidx,
                    };
                    self.insert_candidate(
                        cand, labels, best, buckets, marked, is_marked, arena, n_states, drop_to,
                    );
                }
            }
        }
    }

    #[inline]
    pub(super) fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.raptor.transit_trips[trip_id.0 as usize].service_id;
        self.raptor.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    /// Cutoff = (minimum expected arrival at any target + its egress walk) + `slack`,
    /// over every active state's egress list. `slack` widens the explored arrival
    /// band so safer-but-slower plans survive.
    #[inline]
    /// Per-compact-state arrival cutoff, indexed by `state` (compact idx). A
    /// label of burden `b` is bounded only by the best egress arrival across
    /// states of burden `≤ b` (+ `slack`): a heavier state (e.g. a fast park&ride
    /// drive) must never tighten the cutoff that prunes a lighter state's
    /// exploration, or the lighter plan is starved before the plan-level burden
    /// Pareto can protect it. Unreached burdens leave `u32::MAX` (no pruning).
    fn target_cutoff(best: &[LabelSet], mc: &ModeContext, slack: u32) -> [u32; ALL_STATES.len()] {
        let n_states = mc.n_states();
        let mut per_burden = [u32::MAX; 3];
        for (sidx, vs) in mc.am.states() {
            let b = vs.burden() as usize;
            for &(s, w) in &mc.egress[sidx] {
                let cell = s * n_states + sidx;
                if best[cell].is_reached() {
                    per_burden[b] = per_burden[b].min(best[cell].earliest().saturating_add(w));
                }
            }
        }
        // Prefix-min over burden: cutoff for burden b sees burdens 0..=b only.
        let mut prefix = [u32::MAX; 3];
        let mut acc = u32::MAX;
        for b in 0..3 {
            acc = acc.min(per_burden[b]);
            prefix[b] = acc.saturating_add(slack);
        }
        let mut out = [u32::MAX; ALL_STATES.len()];
        for (sidx, vs) in mc.am.states() {
            out[sidx] = prefix[vs.burden() as usize];
        }
        out
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

            let pats =
                self.raptor.transit_idx_stop_patterns[stop].of(&self.raptor.transit_stop_patterns);
            for &(pat_id, stop_pos) in pats {
                let p = pat_id.0 as usize;
                let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }

                let stop_times = self.raptor.transit_idx_pattern_stop_times[p]
                    .of(&self.raptor.transit_pattern_stop_times);
                let trip_ids =
                    self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);

                let col =
                    &stop_times[stop_pos as usize * n_trips..(stop_pos as usize + 1) * n_trips];

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
        self.raptor_range_tuned(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            &buckets,
            self.raptor.arrival_slack_secs,
        )
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
        self.raptor_range_tuned_rt(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            &RealtimeIndex::new(),
        )
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
        self.raptor_range_tuned_rt_modes(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned_rt_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
    ) -> Vec<Plan> {
        self.raptor_range_tuned_rt_modes_ep(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            am,
            bike,
            terminal_deadline,
            None,
        )
    }

    /// `raptor_range_tuned_rt_modes` carrying the projected snap coordinates (`ep`).
    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned_rt_modes_ep(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
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
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            buckets,
            am,
            bike,
            terminal_deadline,
            ep,
            |mc, access_secs| {
                let probe = self.raptor_inner(
                    mc,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                    buckets,
                    slack,
                    rt,
                );
                if probe.is_empty() {
                    return vec![];
                }

                let departure_times = self.collect_interesting_times(
                    &mc.merged_access(),
                    start_time,
                    start_time.saturating_add(window_secs),
                    date,
                    weekday,
                );
                if departure_times.is_empty() {
                    return probe;
                }

                let n_cells = self.raptor.transit_stop_to_node.len() * mc.n_states();
                let n_patterns = self.raptor.transit_patterns.len();
                let mut best = vec![LabelSet::EMPTY; n_cells];
                let mut labels = vec![vec![LabelSet::EMPTY; n_cells]; MAX_ROUNDS + 1];
                let mut marked = Vec::with_capacity(2048);
                let mut is_marked = vec![false; n_cells];
                let mut queue = Vec::with_capacity(512);
                let mut queue_pos = vec![u32::MAX; n_patterns];
                let mut arena: Vec<Label> = Vec::new();

                let mut times = departure_times;
                times.sort_unstable_by(|a, b| b.cmp(a)); // latest first

                let mut all_plans = Vec::new();
                for (i, t) in times.into_iter().enumerate() {
                    let stamp = i as u32;
                    self.run_departure_into(
                        mc,
                        t,
                        access_secs,
                        date,
                        weekday,
                        buckets,
                        slack,
                        rt,
                        stamp,
                        true,
                        &mut best,
                        &mut labels,
                        &mut marked,
                        &mut is_marked,
                        &mut queue,
                        &mut queue_pos,
                        &mut arena,
                        None,
                    );
                    let plans = self.extract_with_debug(
                        mc,
                        t,
                        date,
                        weekday,
                        &labels,
                        buckets,
                        origin,
                        destination,
                        rt,
                        None,
                        stamp,
                        &arena,
                        false,
                    );
                    all_plans.extend(plans);
                }
                Self::finalize_plans(all_plans, buckets)
            },
        )
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
        self.raptor_range_independent_rt_modes(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_independent_rt_modes(
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
        am: &ActiveModes,
    ) -> Vec<Plan> {
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            buckets,
            am,
            &self.default_bike_cost(),
            false,
            None,
            |mc, access_secs| {
                let probe = self.raptor_inner(
                    mc,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                    buckets,
                    slack,
                    rt,
                );
                if probe.is_empty() {
                    return vec![];
                }

                let departure_times = self.collect_interesting_times(
                    &mc.merged_access(),
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
                    let plans = self.raptor_inner(
                        mc,
                        t,
                        access_secs,
                        date,
                        weekday,
                        origin,
                        destination,
                        buckets,
                        slack,
                        rt,
                    );
                    all_plans.extend(plans);
                }
                Self::finalize_plans(all_plans, buckets)
            },
        )
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
        self.raptor_range_independent_rt(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            &buckets,
            self.raptor.arrival_slack_secs,
            &RealtimeIndex::new(),
        )
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
                            PlanLegStep::Walk(mut ws) => {
                                ws.time = ws.time.saturating_sub(shift);
                                PlanLegStep::Walk(ws)
                            }
                            PlanLegStep::Transit(mut ts) => {
                                ts.time = ts.time.saturating_sub(shift);
                                PlanLegStep::Transit(ts)
                            }
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
                            PlanLegStep::Walk(mut ws) => {
                                ws.time = ws.time.saturating_sub(shift);
                                PlanLegStep::Walk(ws)
                            }
                            PlanLegStep::Transit(mut ts) => {
                                ts.time = ts.time.saturating_sub(shift);
                                PlanLegStep::Transit(ts)
                            }
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
        self.raptor_tuned_rt_overnight_modes(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_tuned_rt_overnight_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        let mut plans = self.raptor_tuned_rt_modes_ep(
            origin,
            destination,
            start_time,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            am,
            bike,
            terminal_deadline,
            ep,
        );

        if start_time < Self::OVERNIGHT_THRESHOLD_SECS && date > 0 {
            let overnight = self.raptor_tuned_rt_modes_ep(
                origin,
                destination,
                start_time + 86400,
                date - 1,
                Self::prev_weekday(weekday),
                min_access_secs,
                buckets,
                slack,
                rt,
                am,
                bike,
                terminal_deadline,
                ep,
            );
            let normalized: Vec<Plan> = overnight
                .into_iter()
                .map(|p| Self::shift_plan(p, 86400))
                .collect();
            if !normalized.is_empty() {
                plans.extend(normalized);
                plans = Self::finalize_plans(plans, buckets);
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
        self.raptor_range_tuned_rt_overnight_modes(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            &ActiveModes::default(),
            &self.default_bike_cost(),
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn raptor_range_tuned_rt_overnight_modes(
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
        am: &ActiveModes,
        bike: &BikeCost,
        terminal_deadline: bool,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        let mut plans = self.raptor_range_tuned_rt_modes_ep(
            origin,
            destination,
            start_time,
            window_secs,
            date,
            weekday,
            min_access_secs,
            buckets,
            slack,
            rt,
            am,
            bike,
            terminal_deadline,
            ep,
        );

        if start_time < Self::OVERNIGHT_THRESHOLD_SECS && date > 0 {
            let overnight = self.raptor_range_tuned_rt_modes_ep(
                origin,
                destination,
                start_time + 86400,
                window_secs,
                date - 1,
                Self::prev_weekday(weekday),
                min_access_secs,
                buckets,
                slack,
                rt,
                am,
                bike,
                terminal_deadline,
                ep,
            );
            let normalized: Vec<Plan> = overnight
                .into_iter()
                .map(|p| Self::shift_plan(p, 86400))
                .collect();
            if !normalized.is_empty() {
                plans.extend(normalized);
                plans = Self::finalize_plans(plans, buckets);
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
            state: 0,
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
            state: 0,
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
        assert!(
            !s.insert(lbl_stamped(100, 0.94, 1), &b),
            "same-stamp same-bucket same-arrival must stay pruned"
        );
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
        assert!(
            s.insert(cand, &b),
            "current-stamp label lost to ghosts in a full cell"
        );
        assert!(
            s.iter().any(|l| l.created_by == stamp),
            "current-stamp label must be present"
        );
    }
}

#[cfg(test)]
mod street_time_tests {
    use crate::structures::{Graph, StreetTimeModel};

    #[test]
    fn vehicle_access_budget_scales_then_clamps() {
        // Defaults: floor 1200 s, fraction 0.06, ceiling 2700 s.
        let g = Graph::new();
        // Short trip ⇒ floored to the local radius.
        assert_eq!(g.vehicle_access_budget(10_000), 1200, "short trip keeps the floor");
        // Mid/long trip ⇒ scales with the crow-flies time (0.06 × 30000 = 1800).
        assert_eq!(g.vehicle_access_budget(30_000), 1800, "long trip rides farther");
        // Very long trip ⇒ clamped to the ceiling.
        assert_eq!(g.vehicle_access_budget(100_000), 2700, "ceiling bounds the search");
    }

    #[test]
    fn access_buffers_egress_is_mean() {
        let mut g = Graph::new();
        g.set_street_time(StreetTimeModel::default());
        let acc = g.access_times(vec![(0, 120), (1, 600)]);
        let egr = g.egress_times(vec![(0, 120), (1, 600)]);
        assert!(
            acc[0].1 > 120 && acc[1].1 > 600,
            "access is buffered above median"
        );
        assert!(
            egr[0].1 > 120 && egr[1].1 > 600,
            "egress mean is strictly above the median"
        );
        assert!(
            egr[0].1 < acc[0].1,
            "egress mean is below the p85 access buffer"
        );
        assert_eq!(acc[0].0, 0);
        assert_eq!(acc[1].0, 1);
    }

    #[test]
    fn null_model_is_identity_for_both_legs() {
        let mut g = Graph::new();
        g.set_street_time(StreetTimeModel {
            access_percentile: 0.5,
            sigma_alpha: 0.0,
            sigma_floor: 0.0,
            sigma_cap: 0.0,
        });
        assert_eq!(g.access_times(vec![(0, 300)]), vec![(0, 300)]);
        assert_eq!(g.egress_times(vec![(0, 300)]), vec![(0, 300)]);
    }
}
