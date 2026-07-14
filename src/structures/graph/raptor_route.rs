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

use super::{BikeCost, Graph, MAX_ROUNDS, latency_profile, raptor_access::StreetProfile};

pub struct QueryEndpoints {
    pub origin: crate::structures::LatLng,
    pub destination: crate::structures::LatLng,
    pub origin_station: Option<Vec<usize>>,
    pub destination_station: Option<Vec<usize>>,
}

/// A cell is valid this generation iff `vgen[j] == cur_gen`; else its distance reads `u32::MAX`.
#[derive(Default)]
struct TransferScratch {
    vgen: Vec<u32>,
    dist: Vec<u32>,
    src: Vec<u32>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(u32, u32)>>,
    cur_gen: u32,
}

impl TransferScratch {
    fn ensure(&mut self, n: usize) {
        if self.vgen.len() < n {
            self.vgen.resize(n, 0);
            self.dist.resize(n, u32::MAX);
            self.src.resize(n, u32::MAX);
        }
    }

    fn new_generation(&mut self) {
        self.heap.clear();
        self.cur_gen = self.cur_gen.wrapping_add(1);
        if self.cur_gen == 0 {
            self.vgen.iter_mut().for_each(|g| *g = 0);
            self.cur_gen = 1;
        }
    }

    #[inline]
    fn get(&self, j: usize) -> u32 {
        if self.vgen[j] == self.cur_gen {
            self.dist[j]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set(&mut self, j: usize, d: u32, s: u32) {
        self.vgen[j] = self.cur_gen;
        self.dist[j] = d;
        self.src[j] = s;
    }
}

thread_local! {
    static TRANSFER_SCRATCH: std::cell::RefCell<TransferScratch> =
        std::cell::RefCell::new(TransferScratch::default());
}

/// Grid cells are addressed as `stop * n_states + state`.
pub(super) struct ModeContext<'a> {
    pub am: &'a ActiveModes,
    pub access: Vec<Vec<(usize, u32)>>,
    pub egress: Vec<Vec<(usize, u32)>>,
    pub dest_station: Option<Vec<usize>>,
    pub unrestricted_transfers: bool,
    pub trip_active_memo: Option<TripActiveMemo>,
    pub fare_profile: crate::structures::cost::FareProfile,
    /// Opt-in absolute-time arrival horizon (travel-map only); `None` leaves the pass unbounded.
    pub horizon: Option<u32>,
}

impl<'a> ModeContext<'a> {
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
        unrestricted_transfers: bool,
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
            unrestricted_transfers,
            trip_active_memo: None,
            fare_profile: crate::structures::cost::FareProfile::default(),
            horizon: None,
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

/// One label per reliability bucket: bounds the supported bucket count (`edges.len()+2`).
pub(super) const MAX_LABELS: usize = 16;

#[inline]
pub(super) fn apply_delay(scheduled: u32, delay: i32) -> u32 {
    (scheduled as i64 + delay as i64).max(0) as u32
}

#[derive(Clone, Copy, Debug)]
pub struct OnboardSeed {
    /// Pattern position, invariant `> current_pos`.
    pub alighted_at: u32,
    pub at_stop: u32,
    pub arrival: u32,
}

#[derive(Clone, Debug)]
pub struct OnboardRide {
    pub pattern: u32,
    /// Within-pattern index of the boarded trip.
    pub trip_within: u32,
    pub trip_id: TripId,
    pub current_pos: u32,
    pub route_type: Option<RouteType>,
    pub seeds: Vec<OnboardSeed>,
}

#[derive(Clone, Copy)]
pub(super) struct Riding {
    /// Trip index within pattern; smaller = arrives earlier at every downstream stop.
    t: usize,
    boarded_at: u32,
    hit_prob: f32,
    reliability: f32,
    from_bucket: u8,
    from_round: u8,
    from_arena: u32,
    state: u8,
}

#[derive(Clone, Copy)]
pub(super) struct Label {
    pub bag: ScenarioBag,
    pub route_type: Option<RouteType>,
    pub reliability: f32,
    pub trace: Trace,
    /// Departure stamp; 0 for single-pass. NOT a Pareto axis; filters this-departure
    /// labels during extraction/build.
    pub created_by: u32,
    pub at_stop: u32,
    /// Transit-leg count (footpaths excluded). Cross-stamp pruning compares it so a
    /// many-leg ghost can't prune a fewer-leg label.
    pub round: u8,
    /// Arena index of the predecessor (`u32::MAX` = root). Reconstruction MUST follow
    /// these exact pointers; a bucket re-lookup drifts to an overwritten label.
    pub parent: u32,
    pub arena_id: u32,
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

    #[inline]
    pub fn arena_push(arena: &mut Vec<Label>, mut lab: Label) -> Label {
        lab.arena_id = arena.len() as u32;
        arena.push(lab);
        lab
    }
}

/// Bounded Pareto set per `(round, stop)` on `(scheduled arrival ↓, reliability bucket ↑)`,
/// at most one label per bucket.
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

    /// Earliest SCHEDULED arrival (not expected), `u32::MAX` if empty.
    pub fn earliest(&self) -> u32 {
        self.iter()
            .map(|l| l.bag.earliest())
            .min()
            .unwrap_or(u32::MAX)
    }

    pub fn min_arrival_label(&self) -> Option<&Label> {
        self.iter().fold(None, |acc, l| match acc {
            None => Some(l),
            Some(b) if l.bag.earliest() < b.bag.earliest() => Some(l),
            Some(b) => Some(b),
        })
    }

    pub fn dominates(&self, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.bag.earliest();
        self.iter()
            .any(|l| buckets.bucket(l.reliability) >= cb && l.bag.earliest() <= ce)
    }

    /// Pareto-inserts `cand`. Same-stamp dominance is bucket-level; cross-stamp dominance
    /// MUST require `>=` PRECISE reliability (not bucket), or Pareto-optimal plans are
    /// dropped (same-bucket prefixes can quantize to different final buckets downstream).
    pub fn insert(&mut self, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.bag.earliest();

        for l in self.iter() {
            let dominates = if l.created_by == cand.created_by {
                buckets.bucket(l.reliability) >= cb
            } else {
                l.reliability >= cand.reliability && l.round <= cand.round
            };
            if dominates && l.bag.earliest() <= ce {
                return false;
            }
        }

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

        // Full cell: evict an old-stamp ghost (can never reach output again) before `cand`.
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

pub(super) fn slim_grid_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("MAAS_SLIM_GRID")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true)
    })
}

pub(super) fn trip_memo_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("MAAS_TRIP_MEMO")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true)
    })
}

/// Tri-state per trip: `0` = unknown, `1` = inactive, `2` = active. Lazy fill is a benign
/// race: every writer stores the SAME pure value, so `Relaxed` suffices.
pub(super) struct TripActiveMemo {
    states: Vec<std::sync::atomic::AtomicU8>,
    /// Debug guard: the `(date, weekday)` first queried, packed `(date << 8) | weekday`
    /// (`u64::MAX` = unset). Every later lookup MUST use the same key (memo is per-date).
    #[cfg(debug_assertions)]
    dw: std::sync::atomic::AtomicU64,
}

impl TripActiveMemo {
    pub(super) fn new(n_trips: usize) -> Self {
        let mut states = Vec::with_capacity(n_trips);
        states.resize_with(n_trips, || std::sync::atomic::AtomicU8::new(0));
        TripActiveMemo {
            states,
            #[cfg(debug_assertions)]
            dw: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Summary {
    pub arena_id: u32,
    pub earliest: u32,
    pub reliability: f32,
    pub created_by: u32,
    pub round: u8,
}

impl Summary {
    #[inline]
    fn of(l: &Label) -> Self {
        Summary {
            arena_id: l.arena_id,
            earliest: l.bag.earliest(),
            reliability: l.reliability,
            created_by: l.created_by,
            round: l.round,
        }
    }
}

/// Asserts the R6 invariant `arena[l.arena_id] == l`.
#[allow(dead_code)]
fn label_matches_arena(stored: &Label, arena: &[Label]) -> bool {
    let id = stored.arena_id as usize;
    if id >= arena.len() {
        return false;
    }
    let a = &arena[id];
    a.arena_id == stored.arena_id
        && a.created_by == stored.created_by
        && a.at_stop == stored.at_stop
        && a.round == stored.round
        && a.parent == stored.parent
        && a.state == stored.state
        && a.reliability == stored.reliability
        && a.route_type == stored.route_type
        && a.bag.earliest() == stored.bag.earliest()
        && a.trace.pattern == stored.trace.pattern
        && a.trace.trip == stored.trace.trip
        && a.trace.boarded_at == stored.trace.boarded_at
        && a.trace.alighted_at == stored.trace.alighted_at
        && a.trace.from_stop == stored.trace.from_stop
        && a.trace.from_bucket == stored.trace.from_bucket
}

/// A grid cell in either representation: full `LabelSet` or 20-byte-summary `SlimSet`.
pub(super) trait LabelCell: Copy {
    fn is_reached(&self) -> bool;
    fn earliest_arrival(&self) -> u32;
    fn count(&self) -> usize;
    fn summary_at(&self, i: usize) -> Summary;
    fn full_at(&self, i: usize, arena: &[Label]) -> Label;
    fn min_arrival_full(&self, arena: &[Label]) -> Option<Label>;
}

impl LabelCell for LabelSet {
    #[inline]
    fn is_reached(&self) -> bool {
        self.len > 0
    }
    #[inline]
    fn earliest_arrival(&self) -> u32 {
        LabelSet::earliest(self)
    }
    #[inline]
    fn count(&self) -> usize {
        self.len as usize
    }
    #[inline]
    fn summary_at(&self, i: usize) -> Summary {
        Summary::of(&self.labels[i])
    }
    #[inline]
    fn full_at(&self, i: usize, arena: &[Label]) -> Label {
        let l = self.labels[i];
        debug_assert!(
            label_matches_arena(&l, arena),
            "R6 baseline invariant violated: arena[{}] != stored grid label",
            l.arena_id
        );
        l
    }
    #[inline]
    fn min_arrival_full(&self, arena: &[Label]) -> Option<Label> {
        self.min_arrival_label().map(|l| {
            debug_assert!(label_matches_arena(l, arena));
            *l
        })
    }
}

/// Per-bucket Pareto front as 20-byte summaries (R6). `insert_summary` MUST stay a
/// field-for-field port of `LabelSet::insert` so the two produce byte-identical streams.
#[derive(Clone, Copy)]
pub(super) struct SlimSet {
    summaries: [Summary; MAX_LABELS],
    len: u8,
}

impl SlimSet {
    pub const EMPTY: Self = SlimSet {
        summaries: [Summary {
            arena_id: u32::MAX,
            earliest: u32::MAX,
            reliability: 0.0,
            created_by: 0,
            round: 0,
        }; MAX_LABELS],
        len: 0,
    };

    #[inline]
    fn iter(&self) -> impl Iterator<Item = &Summary> {
        self.summaries[..self.len as usize].iter()
    }

    #[inline]
    fn earliest_val(&self) -> u32 {
        self.iter().map(|s| s.earliest).min().unwrap_or(u32::MAX)
    }

    fn insert_summary(&mut self, cand: Summary, buckets: &ReliabilityBuckets) -> bool {
        let cb = buckets.bucket(cand.reliability);
        let ce = cand.earliest;

        for s in self.iter() {
            let dominates = if s.created_by == cand.created_by {
                buckets.bucket(s.reliability) >= cb
            } else {
                s.reliability >= cand.reliability && s.round <= cand.round
            };
            if dominates && s.earliest <= ce {
                return false;
            }
        }

        let mut w = 0usize;
        for i in 0..self.len as usize {
            let e = self.summaries[i];
            let eb = buckets.bucket(e.reliability);
            let ee = e.earliest;
            let dominated = eb <= cb && ee >= ce && (eb < cb || ee > ce);
            if !dominated {
                self.summaries[w] = e;
                w += 1;
            }
        }
        self.len = w as u8;

        if (self.len as usize) < MAX_LABELS {
            self.summaries[self.len as usize] = cand;
            self.len += 1;
            return true;
        }

        let mut ghost: Option<usize> = None;
        for i in 0..self.len as usize {
            if self.summaries[i].created_by == cand.created_by {
                continue;
            }
            ghost = Some(match ghost {
                None => i,
                Some(g) => {
                    let (gr, ge) = (self.summaries[g].reliability, self.summaries[g].earliest);
                    let (ir, ie) = (self.summaries[i].reliability, self.summaries[i].earliest);
                    if ir < gr || (ir == gr && ie > ge) {
                        i
                    } else {
                        g
                    }
                }
            });
        }
        if let Some(g) = ghost {
            self.summaries[g] = cand;
            return true;
        }

        let mut worst = 0usize;
        for i in 1..self.len as usize {
            let wb = buckets.bucket(self.summaries[worst].reliability);
            let ib = buckets.bucket(self.summaries[i].reliability);
            if ib < wb
                || (ib == wb && self.summaries[i].earliest > self.summaries[worst].earliest)
            {
                worst = i;
            }
        }
        let wb = buckets.bucket(self.summaries[worst].reliability);
        if cb > wb || (cb == wb && ce < self.summaries[worst].earliest) {
            self.summaries[worst] = cand;
            return true;
        }
        false
    }
}

impl LabelCell for SlimSet {
    #[inline]
    fn is_reached(&self) -> bool {
        self.len > 0
    }
    #[inline]
    fn earliest_arrival(&self) -> u32 {
        self.earliest_val()
    }
    #[inline]
    fn count(&self) -> usize {
        self.len as usize
    }
    #[inline]
    fn summary_at(&self, i: usize) -> Summary {
        self.summaries[i]
    }
    #[inline]
    fn full_at(&self, i: usize, arena: &[Label]) -> Label {
        arena[self.summaries[i].arena_id as usize]
    }
    #[inline]
    fn min_arrival_full(&self, arena: &[Label]) -> Option<Label> {
        // First-wins on ties, matching `LabelSet::min_arrival_label`'s fold.
        let mut best: Option<&Summary> = None;
        for s in self.iter() {
            best = match best {
                None => Some(s),
                Some(b) if s.earliest < b.earliest => Some(s),
                Some(b) => Some(b),
            };
        }
        best.map(|s| arena[s.arena_id as usize])
    }
}

/// One round-row of the carried RAPTOR label grid, generic over `FullRow`/`SlimRow`.
pub(super) trait LabelRow: Sync + Sized {
    type Cell: LabelCell;
    fn empty(n_cells: usize) -> Self;
    fn cell(&self, cell: usize) -> Self::Cell;
    fn is_reached(&self, cell: usize) -> bool;
    fn earliest(&self, cell: usize) -> u32;
    fn insert(&mut self, cell: usize, label: Label, buckets: &ReliabilityBuckets) -> bool;
    /// Carry `src` (round k-1) into `self` (round k). `carried == false` is the
    /// single-pass whole-row copy; `carried == true` Pareto-merges `src` into `self`.
    fn carry_from(
        &mut self,
        src: &Self,
        n_cells: usize,
        carried: bool,
        buckets: &ReliabilityBuckets,
    );
}

pub(super) struct FullRow(Vec<LabelSet>);

impl LabelRow for FullRow {
    type Cell = LabelSet;
    #[inline]
    fn empty(n_cells: usize) -> Self {
        FullRow(vec![LabelSet::EMPTY; n_cells])
    }
    #[inline]
    fn cell(&self, cell: usize) -> LabelSet {
        self.0[cell]
    }
    #[inline]
    fn is_reached(&self, cell: usize) -> bool {
        self.0[cell].len > 0
    }
    #[inline]
    fn earliest(&self, cell: usize) -> u32 {
        self.0[cell].earliest()
    }
    #[inline]
    fn insert(&mut self, cell: usize, label: Label, buckets: &ReliabilityBuckets) -> bool {
        self.0[cell].insert(label, buckets)
    }
    fn carry_from(
        &mut self,
        src: &Self,
        n_cells: usize,
        carried: bool,
        buckets: &ReliabilityBuckets,
    ) {
        if carried {
            for cell in 0..n_cells {
                if src.0[cell].len > 0 {
                    for lab in src.0[cell].iter() {
                        self.0[cell].insert(*lab, buckets);
                    }
                }
            }
        } else {
            self.0.copy_from_slice(&src.0);
        }
    }
}

/// Slim round-row (R6): summary cells with lazy allocation and a reached-cell index. The
/// carried-merge is per-cell independent (cell `c` of `dst` depends only on `src[c]` and
/// `dst[c]`), so it is order-invariant.
pub(super) struct SlimRow {
    n_cells: usize,
    /// Empty ⇒ row untouched (all cells `EMPTY`); else length `n_cells`.
    cells: Vec<SlimSet>,
    /// Indices of cells with ≥1 label (deduplicated).
    reached: Vec<u32>,
}

impl SlimRow {
    #[inline]
    fn materialize(&mut self) {
        if self.cells.is_empty() {
            self.cells = vec![SlimSet::EMPTY; self.n_cells];
        }
    }
}

impl LabelRow for SlimRow {
    type Cell = SlimSet;
    #[inline]
    fn empty(n_cells: usize) -> Self {
        SlimRow {
            n_cells,
            cells: Vec::new(),
            reached: Vec::new(),
        }
    }
    #[inline]
    fn cell(&self, cell: usize) -> SlimSet {
        if self.cells.is_empty() {
            SlimSet::EMPTY
        } else {
            self.cells[cell]
        }
    }
    #[inline]
    fn is_reached(&self, cell: usize) -> bool {
        !self.cells.is_empty() && self.cells[cell].len > 0
    }
    #[inline]
    fn earliest(&self, cell: usize) -> u32 {
        if self.cells.is_empty() {
            u32::MAX
        } else {
            self.cells[cell].earliest_val()
        }
    }
    #[inline]
    fn insert(&mut self, cell: usize, label: Label, buckets: &ReliabilityBuckets) -> bool {
        self.materialize();
        let was_reached = self.cells[cell].len > 0;
        let changed = self.cells[cell].insert_summary(Summary::of(&label), buckets);
        if !was_reached && self.cells[cell].len > 0 {
            self.reached.push(cell as u32);
        }
        changed
    }
    fn carry_from(
        &mut self,
        src: &Self,
        _n_cells: usize,
        carried: bool,
        buckets: &ReliabilityBuckets,
    ) {
        if src.cells.is_empty() {
            // Prev row untouched: nothing to carry.
            if !carried {
                self.cells.clear();
                self.reached.clear();
            }
            return;
        }
        if carried {
            // Fold prev's ≤(k-1)-trip frontier into this round over prev's reached cells.
            self.materialize();
            let mut order: Vec<u32> = src.reached.clone();
            order.sort_unstable();
            for &c in &order {
                let c = c as usize;
                let was_reached = self.cells[c].len > 0;
                for s in src.cells[c].iter() {
                    self.cells[c].insert_summary(*s, buckets);
                }
                if !was_reached && self.cells[c].len > 0 {
                    self.reached.push(c as u32);
                }
            }
        } else {
            self.cells.clear();
            self.cells.extend_from_slice(&src.cells);
            self.reached.clear();
            self.reached.extend_from_slice(&src.reached);
        }
    }
}

/// Toggle (env `MAAS_COMPACT_BEST`, default ON) for the compact `best` backend (R3).
pub(super) fn compact_best_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("MAAS_COMPACT_BEST")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true)
    })
}

/// Per-pass forward-exploration bound (`best`), reset every pass so all its labels share
/// one stamp. Same-stamp LabelSet reads (`is_reached`/`earliest`/`dominates`/`insert`)
/// reduce EXACTLY to per-reliability-bucket minimum scheduled arrival, so the `Compact`
/// per-bucket-`min` backend reproduces them bit-for-bit while `n_buckets <= MAX_LABELS`.
pub(super) enum BestGrid {
    Labels(Vec<LabelSet>),
    Compact {
        /// Flat `n_cells * n_buckets` min scheduled arrivals (`u32::MAX` = unreached).
        /// Cell `c`, bucket `b` → `arr[c*nb + b]`.
        arr: Vec<u32>,
        n_buckets: usize,
        n_cells: usize,
    },
}

impl BestGrid {
    /// Compact backend iff the toggle is on AND `n_buckets <= MAX_LABELS` (above that the
    /// same-stamp LabelSet can overflow and diverge from the compact per-bucket min).
    pub(super) fn new(n_cells: usize, buckets: &ReliabilityBuckets) -> Self {
        let n_buckets = buckets.bucket(1.0) as usize + 1;
        if compact_best_enabled() && n_buckets <= MAX_LABELS {
            BestGrid::Compact {
                arr: vec![u32::MAX; n_cells * n_buckets],
                n_buckets,
                n_cells,
            }
        } else {
            BestGrid::Labels(vec![LabelSet::EMPTY; n_cells])
        }
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        match self {
            BestGrid::Labels(v) => v.len(),
            BestGrid::Compact { n_cells, .. } => *n_cells,
        }
    }

    #[inline]
    pub(super) fn reset(&mut self) {
        match self {
            BestGrid::Labels(v) => {
                for b in v.iter_mut() {
                    *b = LabelSet::EMPTY;
                }
            }
            BestGrid::Compact { arr, .. } => arr.fill(u32::MAX),
        }
    }

    #[inline]
    pub(super) fn is_reached(&self, cell: usize) -> bool {
        match self {
            BestGrid::Labels(v) => v[cell].is_reached(),
            BestGrid::Compact { arr, n_buckets, .. } => arr
                [cell * n_buckets..(cell + 1) * n_buckets]
                .iter()
                .any(|&a| a != u32::MAX),
        }
    }

    #[inline]
    pub(super) fn earliest(&self, cell: usize) -> u32 {
        match self {
            BestGrid::Labels(v) => v[cell].earliest(),
            BestGrid::Compact { arr, n_buckets, .. } => arr
                [cell * n_buckets..(cell + 1) * n_buckets]
                .iter()
                .copied()
                .min()
                .unwrap_or(u32::MAX),
        }
    }

    /// Same predicate as `LabelSet::dominates`: some member with bucket `>=` and arrival `<=`.
    #[inline]
    pub(super) fn dominates(&self, cell: usize, cand: Label, buckets: &ReliabilityBuckets) -> bool {
        match self {
            BestGrid::Labels(v) => v[cell].dominates(cand, buckets),
            BestGrid::Compact { arr, n_buckets, .. } => {
                let cb = buckets.bucket(cand.reliability) as usize;
                let ce = cand.bag.earliest();
                arr[cell * n_buckets + cb..(cell + 1) * n_buckets]
                    .iter()
                    .any(|&a| a <= ce)
            }
        }
    }

    /// Same-stamp equivalent of `LabelSet::insert`: keep the per-bucket min scheduled arrival.
    #[inline]
    pub(super) fn insert(&mut self, cell: usize, cand: Label, buckets: &ReliabilityBuckets) {
        match self {
            BestGrid::Labels(v) => {
                v[cell].insert(cand, buckets);
            }
            BestGrid::Compact { arr, n_buckets, .. } => {
                let cb = buckets.bucket(cand.reliability) as usize;
                let ce = cand.bag.earliest();
                let slot = &mut arr[cell * *n_buckets + cb];
                if ce < *slot {
                    *slot = ce;
                }
            }
        }
    }
}

impl Graph {
    /// Foot access/egress stops within `max_secs` of `origin`.
    fn foot_nearby_stops(&self, origin: NodeID, max_secs: u32) -> Vec<(usize, u32)> {
        let cg = self.contracted.as_ref().unwrap();
        self.nearby_stops_union(origin, max_secs, cg)
    }

    /// Foot access/egress stops, keyed on the projected snap `coord` when supplied (g-free,
    /// survives the interior-node drop), else falls back to `foot_nearby_stops`.
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

    /// Car access/egress stops within `max_secs` of `origin` (sorted by stop).
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

    /// Foot seconds `origin`→`destination` (`u32::MAX` = unreachable).
    fn walk_secs_to(&self, origin: NodeID, destination: NodeID, bound: u32) -> u32 {
        let cg = self.contracted.as_ref().unwrap();
        cg.walk_secs_point_to_point(self, origin, destination, bound)
            .unwrap_or(u32::MAX)
    }

    /// Foot seconds keyed on projected snap coords (`ep`, g-free), else `walk_secs_to`.
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

    /// Straight-line meters between endpoints (projected snap coords via `ep`, else
    /// `nodes_distance`). The 0.99 haversine discount MUST match `nodes_distance`.
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

    /// Seconds to the nearest transit stop, keyed on projected snap coord when available.
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

    /// Car seconds `origin`→`destination` (`None` = unreachable).
    fn car_secs_to(&self, origin: NodeID, destination: NodeID, bound: u32) -> Option<u32> {
        let cg = self.contracted.as_ref().unwrap();
        self.car_dijkstra_union(origin, bound, cg)
            .get(&destination)
            .copied()
    }

    /// Onboard ride seeds for a user aboard trip `trip_within` of `pattern` past
    /// `current_pos`: one reached label per downstream stop (`pos > current_pos`) at its
    /// realtime arrival.
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

    /// Locates a boarded `trip` and its current pattern position. `from_stop`/`from_seq`
    /// override; else current position is the last pattern stop with realtime departure
    /// `<= now`. Returns `(pattern, trip index, current_pos)`, or `None` if unknown or at
    /// the final stop.
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

    /// Egress-only onboard partial-requery: seed the boarded trip's downstream stops and
    /// run rounds + transfers onward to `destination`. No access side. State 0 = `Walked`.
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
        unrestricted: bool,
        use_cch: bool,
        ep: Option<&QueryEndpoints>,
    ) -> Vec<Plan> {
        if ride.seeds.is_empty() {
            return Vec::new();
        }
        let raw_egress = if use_cch && self.cch.is_some() {
            let coord = ep.map(|e| e.destination).unwrap_or_else(|| self.node_loc(destination));
            self.cch_egress(self.cch.as_ref().unwrap(), coord)
        } else {
            self.foot_nearby_stops_ep(destination, egress_secs, ep.map(|e| e.destination))
        };
        let foot_egress = self.egress_times(raw_egress);
        let mc = ModeContext::build(am, &[], &[], &[], &foot_egress, &[], &[], None, unrestricted);
        if !mc.any_egress() {
            return Vec::new();
        }
        let plans = self.raptor_onboard_inner(&mc, ride, date, weekday, destination, buckets, slack, rt);
        Self::finalize_plans(plans, buckets)
    }

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
        if slim_grid_enabled() {
            self.raptor_onboard_grid::<SlimRow>(mc, ride, date, weekday, destination, buckets, slack, rt)
        } else {
            self.raptor_onboard_grid::<FullRow>(mc, ride, date, weekday, destination, buckets, slack, rt)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn raptor_onboard_grid<R: LabelRow>(
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

        let mut best = BestGrid::new(n_cells, buckets);
        let mut labels: Vec<R> = (0..=MAX_ROUNDS).map(|_| R::empty(n_cells)).collect();
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
            &mut std::collections::HashMap::new(),
        )
    }

    /// Pass A near-stop access radius: nearest-stop reach clamped to the `min_access_secs`
    /// floor. Shared by prod and debug so their access geometry can't drift.
    #[allow(clippy::too_many_arguments)]
    fn near_access_radius(
        &self,
        origin: NodeID,
        destination: NodeID,
        min_access_secs: u32,
        ep: Option<&QueryEndpoints>,
    ) -> u32 {
        let straight_line_secs =
            (self.endpoint_distance(origin, destination, ep) as f64 / self.raptor.walking_speed_mps)
                as u32;
        self.nearest_stop_secs_ep(origin, straight_line_secs, ep.map(|e| e.origin))
            .max(min_access_secs)
    }

    /// Pass B admissible radius `min(W, best_arrival - start)`, computing the unbounded
    /// walk-only `W` only when the arrival-derived radius could exceed it (lazy-W).
    /// Returns `(bound, w_opt)`; `best_arrival = None` ⇒ Pass A found no plan. See the
    /// completeness argument in `with_access_search`.
    fn admissible_access_bound(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        best_arrival: Option<u32>,
        ep: Option<&QueryEndpoints>,
    ) -> (u32, Option<u32>) {
        let w_lower_secs = ((self.endpoint_distance(origin, destination, ep) as f64
            - 2.0 * self.raptor.edge_snap_radius_m)
            .max(0.0)
            / self.raptor.walking_speed_mps) as u32;
        let mut w_opt: Option<u32> = None;
        let bound = match best_arrival {
            Some(a) => {
                let b = a.saturating_sub(start_time);
                if b > w_lower_secs {
                    let w = self.walk_secs_to_ep(origin, destination, u32::MAX, ep);
                    w_opt = Some(w);
                    b.min(w)
                } else {
                    b
                }
            }
            None => {
                let w = self.walk_secs_to_ep(origin, destination, u32::MAX, ep);
                w_opt = Some(w);
                w
            }
        };
        (bound, w_opt)
    }

    #[allow(clippy::too_many_arguments)]
    fn with_access_search<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        buckets: &ReliabilityBuckets,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
        mut try_routing: F,
    ) -> Vec<Plan>
    where
        F: FnMut(&ModeContext, u32) -> Vec<Plan>,
    {
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
                ep,
            );
            return Self::finalize_plans(plans, buckets);
        }

        let both_stations = ep.is_some_and(|e| {
            e.origin_station.is_some() && e.destination_station.is_some()
        });

        // Fastest-arrival guarantee: any journey arriving before the best arrival `A`
        // must have BOTH foot access and egress shorter than `A - start_time`
        // (arrival >= start_time + access + egress), so searching both radii out to
        // `A - start_time` finds every journey faster than `A`; two passes suffice, capped
        // by the walk-only time `W`. Pass A (near-stop radius) yields `A` cheaply and, with
        // its tighter cutoff, protects near-boarding Pareto variants. Pass B (admissible
        // `min(W, A - start_time)`) catches faster far-access journeys. `start_time` (never
        // a tightened per-plan departure) is the only baseline. `build_mode_context` uses
        // `access_secs` for BOTH foot access and egress radius.
        let mut all: Vec<Plan> = Vec::new();
        let mut access_secs = self.near_access_radius(origin, destination, min_access_secs, ep);

        let mc = latency_profile::time_discovery(|| {
            self.build_mode_context(am, origin, destination, access_secs, bike, unrestricted, use_cch, ep, fare_profile)
        });
        if mc.any_access() && mc.any_egress() {
            latency_profile::begin_pass();
            all.extend(try_routing(&mc, access_secs));
        }

        // Station endpoints supply their complete platform set at zero access cost, so
        // Pass A is already complete.
        if both_stations {
            if all.is_empty() {
                let actual = self.walk_secs_to_ep(origin, destination, u32::MAX, ep);
                all = self.direct_fallback_plans(
                    am, origin, destination, start_time, actual, bike, ep,
                );
            } else {
                self.append_bounded_direct_plans(
                    am, origin, destination, start_time, slack, bike,
                    &mut all, ep,
                );
            }
            return Self::finalize_plans(all, buckets);
        }

        // Pass B — admissible radius `min(W, A_est - start)`. `admissible_access_bound`
        // skips computing the unbounded `W` via the provable lower bound
        // `(endpoint_distance - 2*snap_radius) / speed <= W`. (straight_line_secs alone is
        // NOT sound: `W` excludes the perpendicular coord->projection snap stubs.)
        let (bound, w_opt) = self.admissible_access_bound(
            origin,
            destination,
            start_time,
            all.iter().map(|p| p.end).min(),
            ep,
        );
        if access_secs < bound {
            access_secs = bound;
            let mc = latency_profile::time_discovery(|| {
                self.build_mode_context(am, origin, destination, access_secs, bike, unrestricted, use_cch, ep, fare_profile)
            });
            if mc.any_access() && mc.any_egress() {
                latency_profile::begin_pass();
                all.extend(try_routing(&mc, access_secs));
            }
        }

        // No transit plan at any radius up to W: fall back to direct / walk-only plans.
        if all.is_empty() {
            let w = w_opt.unwrap_or_else(|| self.walk_secs_to_ep(origin, destination, u32::MAX, ep));
            let plans = self.direct_fallback_plans(
                am, origin, destination, start_time, w, bike, ep,
            );
            return Self::finalize_plans(plans, buckets);
        }

        self.append_bounded_direct_plans(
            am, origin, destination, start_time, slack, bike,
            &mut all, ep,
        );
        Self::finalize_plans(all, buckets)
    }

    /// Inflate access-leg seconds to the conservative percentile (buffer the connection).
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

    /// Bike/car access budget: a fraction of `crow_secs`, clamped to floor and ceiling.
    pub(crate) fn vehicle_access_budget(&self, crow_secs: u32) -> u32 {
        ((self.raptor.vehicle_access_fraction * crow_secs as f64) as u32)
            .clamp(self.raptor.vehicle_access_secs, self.raptor.vehicle_access_max_secs)
    }

    /// Per-profile access/egress stop discovery for the active states.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_mode_context<'a>(
        &self,
        am: &'a ActiveModes,
        origin: NodeID,
        destination: NodeID,
        access_secs: u32,
        bike: &BikeCost,
        unrestricted: bool,
        use_cch: bool,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> ModeContext<'a> {
        self.build_mode_context_opts(
            am, origin, destination, access_secs, bike, unrestricted, use_cch, ep, fare_profile,
            false,
        )
    }

    /// `build_mode_context` with `skip_egress` (OPT-C1, travel-map only): skips the egress
    /// sweep entirely. Bit-identical ONLY iff `!am.uses_vehicle()` (the egress set feeds the
    /// isochrone-cleared grid and the vehicle-access retain-filter); the caller MUST gate on
    /// exactly that.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_mode_context_opts<'a>(
        &self,
        am: &'a ActiveModes,
        origin: NodeID,
        destination: NodeID,
        access_secs: u32,
        bike: &BikeCost,
        unrestricted: bool,
        use_cch: bool,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
        skip_egress: bool,
    ) -> ModeContext<'a> {
        use VehicleState::*;
        let has = |s| am.state_of(s).is_some();
        // Bike/car use a wider, trip-length-scaled budget than the local foot radius.
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
            let raw = if use_cch && self.cch.is_some() {
                let coord = ep.map(|e| e.origin).unwrap_or_else(|| self.node_loc(origin));
                self.cch_access(self.cch.as_ref().unwrap(), coord)
            } else {
                self.foot_nearby_stops_ep(origin, access_secs, ep.map(|e| e.origin))
            };
            self.access_times(raw)
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
        let foot_egress = if skip_egress {
            vec![]
        } else if let Some(p) = dest_station {
            station_zero(p)
        } else if has(Walked) || has(BikeDropped) || has(CarParked) {
            let raw = if use_cch && self.cch.is_some() {
                let coord = ep.map(|e| e.destination).unwrap_or_else(|| self.node_loc(destination));
                self.cch_egress(self.cch.as_ref().unwrap(), coord)
            } else {
                self.foot_nearby_stops_ep(destination, access_secs, ep.map(|e| e.destination))
            };
            self.egress_times(raw)
        } else {
            vec![]
        };
        let bike_egress = if skip_egress {
            vec![]
        } else if let Some(p) = dest_station {
            station_zero(p)
        } else if has(BikeInHand) || has(BikeEgress) {
            self.egress_times(self.bike_nearby_stops(destination, vehicle_secs, bike))
        } else {
            vec![]
        };
        let car_egress = if skip_egress {
            vec![]
        } else if let Some(p) = dest_station {
            station_zero(p)
        } else if has(CarEgress) {
            self.egress_times(self.car_nearby_stops(destination, vehicle_secs))
        } else {
            vec![]
        };

        // Drop vehicle-access stops that are also egress stops: a round-0 "drove there"
        // label would poison `target_cutoff` and dominate real transit arrivals there,
        // collapsing park&ride to a walk fallback. Foot access is left alone.
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

        let mut mc = ModeContext::build(
            am,
            &foot_access,
            &bike_access,
            &car_access,
            &foot_egress,
            &bike_egress,
            &car_egress,
            dest_station,
            unrestricted,
        );
        if trip_memo_enabled() {
            mc.trip_active_memo = Some(TripActiveMemo::new(self.raptor.transit_trips.len()));
        }
        mc.fare_profile = fare_profile;
        mc
    }

    /// Appends direct street plans arriving within `best transit arrival + slack` (the
    /// only window in which they can survive the final Pareto).
    #[allow(clippy::too_many_arguments)]
    fn append_bounded_direct_plans(
        &self,
        am: &ActiveModes,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        slack: u32,
        bike: &BikeCost,
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
    fn direct_fallback_plans(
        &self,
        am: &ActiveModes,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
        bike: &BikeCost,
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
            self.raptor.unrestricted_transfers,
            self.raptor.use_cch_access,
            rt,
            am,
            bike,
            None,
            crate::structures::cost::FareProfile::default(),
        )
    }

    /// `raptor_tuned_rt_modes` carrying projected snap coordinates (`ep`, g-free access).
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> Vec<Plan> {
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            unrestricted,
            use_cch,
            buckets,
            am,
            bike,
            ep,
            fare_profile,
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

    /// Core RAPTOR over one departure. `want_debug` gates the discardable `stops_reached`
    /// survey and per-candidate sink; production passes `false`.
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
        if slim_grid_enabled() {
            self.raptor_inner_grid::<SlimRow>(
                mc, start_time, access_secs, date, weekday, origin, destination, buckets, slack,
                rt, want_debug,
            )
        } else {
            self.raptor_inner_grid::<FullRow>(
                mc, start_time, access_secs, date, weekday, origin, destination, buckets, slack,
                rt, want_debug,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn raptor_inner_grid<R: LabelRow>(
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

        let (mut best, mut labels) = latency_profile::time_grid_alloc(|| {
            (
                BestGrid::new(n_cells, buckets),
                (0..=MAX_ROUNDS).map(|_| R::empty(n_cells)).collect::<Vec<R>>(),
            )
        });

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_cells];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];
        let mut arena: Vec<Label> = Vec::new();

        latency_profile::time_forward(|| {
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
            )
        });

        let stops_reached: Vec<StopReach> = if want_debug {
            (0..n_stops)
                .filter_map(|stop_idx| {
                    for k in 0..=MAX_ROUNDS {
                        let reached = (0..n_states)
                            .any(|s| labels[k].is_reached(stop_idx * n_states + s));
                        if reached {
                            let node_id = self.raptor.transit_stop_to_node[stop_idx];
                            let loc = self.node_loc(node_id);
                            let name = self.raptor.transit_stop_names[stop_idx].clone();
                            let path =
                                self.path_to_stop(stop_idx, k, origin, &labels, &arena, n_states);
                            let arrival_secs = (0..n_states)
                                .map(|s| labels[k].earliest(stop_idx * n_states + s))
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

        let mut candidates: Vec<PlanCandidate> = Vec::new();
        let debug_sink = if want_debug {
            Some(&mut candidates)
        } else {
            None
        };
        let plans = latency_profile::time_extract(|| {
            self.extract_with_debug(
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
                &mut std::collections::HashMap::new(),
            )
        });
        (plans, candidates, stops_reached)
    }

    /// Runs one RAPTOR departure (seed → rounds) into caller-owned grids. `best` is
    /// per-pass (RESET here, gates the cross-round prune + `target_cutoff`); `labels` is
    /// CARRIED across departures and NOT reset, marking gated on per-round `labels[k]`
    /// improvement. `stamp` brands every label so reconstruction follows this-departure
    /// traces. `carried` Pareto-merges `labels[k-1]` into `labels[k]`; else whole-row copy.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn run_departure_into<R: LabelRow>(
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
        best: &mut BestGrid,
        labels: &mut [R],
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

        best.reset();
        marked.clear();
        is_marked.fill(false);
        // Arena is reset per pass: carried foreign grid labels keep stale `arena_id`s that
        // are never followed (extraction and boarding both filter to the current stamp).
        arena.clear();

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
                labels[0].insert(cell, lab, buckets);
                best.insert(cell, lab, buckets);
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
                    labels[0].insert(cell, lab, buckets);
                    best.insert(cell, lab, buckets);
                    Self::mark(cell, marked, is_marked);
                    // Free drop at the access stop (park & ride).
                    if let Some((in_hand, dropped)) = drop_to
                        && sidx as u8 == in_hand
                    {
                        let mut d = lab;
                        d.state = dropped;
                        let d = Label::arena_push(arena, d);
                        let dcell = stop * n_states + dropped as usize;
                        labels[0].insert(dcell, d, buckets);
                        best.insert(dcell, d, buckets);
                        Self::mark(dcell, marked, is_marked);
                    }
                }
            }
        }

        // Round-0 transfer bound: onboard seeds (no access radius) use the egress-based
        // cutoff; foot access uses the uniform radius, saturating so a near-unbounded
        // `access_secs` (MCR/flood path) reads as u32::MAX instead of wrapping.
        let seed_bound = if onboard.is_some() {
            Self::target_cutoff(best, mc, slack)
        } else {
            [start_time.saturating_add(access_secs); ALL_STATES.len()]
        };
        if mc.unrestricted_transfers {
            self.apply_transfers_mcr(
                mc,
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
        } else {
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
        }

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev_k = &prev[k - 1];
                let curr_k = &mut rest[0];
                curr_k.carry_from(prev_k, n_cells, carried, buckets);
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
                let arena_ro: &[Label] = arena;
                let chunks = Self::scan_chunks(queue.len());
                if chunks <= 1 {
                    let mut cands: Vec<Label> = Vec::new();
                    for &pat in queue.iter() {
                        self.scan_route_collect(
                            pat, qp[pat], date, weekday, cutoff, prev_slice, best, buckets, rt,
                            stamp, arena_ro, &mut cands, mc,
                        );
                    }
                    self.apply_scan_candidates(
                        &cands, curr_slice, best, buckets, marked, is_marked, arena, n_states,
                        drop_to,
                    );
                } else {
                    // Parallel read-only scans, then apply in queue order against the live
                    // grid: the exact sequential consideration stream, so output and arena
                    // ids are identical regardless of thread scheduling.
                    let chunk_size = queue.len().div_ceil(chunks);
                    let best_ro: &BestGrid = best;
                    let collected: Vec<Vec<Label>> = std::thread::scope(|s| {
                        let handles: Vec<_> = queue
                            .chunks(chunk_size)
                            .map(|chunk| {
                                s.spawn(move || {
                                    let mut cands: Vec<Label> = Vec::new();
                                    for &pat in chunk {
                                        self.scan_route_collect(
                                            pat, qp[pat], date, weekday, cutoff, prev_slice,
                                            best_ro, buckets, rt, stamp, arena_ro, &mut cands, mc,
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

            if mc.unrestricted_transfers {
                self.apply_transfers_mcr(
                    mc,
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
            } else {
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
            }

            if marked.is_empty() {
                break;
            }
        }
    }

    /// Follows RAPTOR traces backward from `(round, stop_idx)` into an ordered leg list
    /// (walk/transit), transit legs carrying intermediate pattern stops as geometry.
    fn path_to_stop<R: LabelRow>(
        &self,
        stop_idx: usize,
        round: usize,
        origin: NodeID,
        labels: &[R],
        arena: &[Label],
        n_states: usize,
    ) -> Vec<StopPathLeg> {
        let min_label_at = |k: usize, stop: usize| -> Option<Label> {
            (0..n_states)
                .filter_map(|s| labels[k].cell(stop * n_states + s).min_arrival_full(arena))
                .min_by_key(|l| l.bag.earliest())
        };

        let mut legs: Vec<StopPathLeg> = Vec::new();
        let mut stop = stop_idx;
        let mut k = round;

        while let Some(l) = min_label_at(k, stop) {
            debug_assert!(
                l.created_by == 0,
                "path_to_stop must be single-pass (stamp 0)"
            );
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
            } else {
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

    /// Debug variant of `with_access_search`, mirroring its two-pass A/B access search
    /// (shared `near_access_radius` / `admissible_access_bound` helpers) so the debug and
    /// prod access geometry can't drift. The closure returns RAW un-Pareto'd plans per
    /// pass; both passes are union-Pareto'd once (debug analog of `finalize_plans`). NOTE:
    /// this path skips prod's `group_access_alternatives`/`prune_slower_than_direct`/
    /// `append_bounded_direct_plans`, so its plan SET can differ from prod on finalization
    /// grounds (never on access grounds).
    #[allow(clippy::too_many_arguments)]
    fn with_access_search_debug<F>(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        min_access_secs: u32,
        unrestricted: bool,
        use_cch: bool,
        buckets: &ReliabilityBuckets,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
        mut try_routing: F,
    ) -> (Vec<Plan>, Vec<PlanCandidate>, AccessInfo, Vec<StopReach>)
    where
        F: FnMut(&ModeContext, u32) -> Option<(Vec<Plan>, Vec<PlanCandidate>, Vec<StopReach>)>,
    {
        let walk_fallback = |walk_secs: u32, radius: u32, extra_passes: u32| {
            let plans =
                self.direct_fallback_plans(am, origin, destination, start_time, walk_secs, bike, ep);
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
                walk_radius_secs: radius,
                walk_radius_meters: (radius as f64 * self.raptor.walking_speed_mps) as u32,
                origin_stops_found: 0,
                destination_stops_found: 0,
                access_attempts: extra_passes,
                fell_back_to_walk_only: true,
            };
            (plans, candidates, access, Vec::new())
        };

        let both_stations =
            ep.is_some_and(|e| e.origin_station.is_some() && e.destination_station.is_some());

        let mut access_secs = self.near_access_radius(origin, destination, min_access_secs, ep);
        let pass_a_radius = access_secs;

        let mut all_plans: Vec<Plan> = Vec::new();
        let mut all_cands: Vec<PlanCandidate> = Vec::new();
        let mut stops_reached: Vec<StopReach> = Vec::new();
        let mut passes_run: u32 = 0;
        let mut origin_stops = 0u32;
        let mut dest_stops = 0u32;
        let mut recorded = false;

        let mc =
            self.build_mode_context(am, origin, destination, access_secs, bike, unrestricted, use_cch, ep, fare_profile);
        if mc.any_access() && mc.any_egress() {
            origin_stops = mc.merged_access().len() as u32;
            dest_stops = mc.egress.iter().map(|e| e.len()).max().unwrap_or(0) as u32;
            recorded = true;
            passes_run += 1;
            if let Some((p, c, s)) = try_routing(&mc, access_secs) {
                all_plans.extend(p);
                all_cands.extend(c);
                stops_reached = s;
            }
        }

        if both_stations {
            if all_plans.is_empty() {
                let actual = self.walk_secs_to_ep(origin, destination, u32::MAX, ep);
                return walk_fallback(actual, pass_a_radius, passes_run.saturating_sub(1));
            }
        } else {
            let (bound, w_opt) = self.admissible_access_bound(
                origin,
                destination,
                start_time,
                all_plans.iter().map(|p| p.end).min(),
                ep,
            );
            if access_secs < bound {
                access_secs = bound;
                let mc = self.build_mode_context(
                    am, origin, destination, access_secs, bike, unrestricted, use_cch, ep, fare_profile,
                );
                if mc.any_access() && mc.any_egress() {
                    if !recorded {
                        origin_stops = mc.merged_access().len() as u32;
                        dest_stops = mc.egress.iter().map(|e| e.len()).max().unwrap_or(0) as u32;
                    }
                    passes_run += 1;
                    if let Some((p, c, s)) = try_routing(&mc, access_secs) {
                        all_plans.extend(p);
                        all_cands.extend(c);
                        if s.len() > stops_reached.len() {
                            stops_reached = s;
                        }
                    }
                }
            }

            if all_plans.is_empty() {
                let w = w_opt
                    .unwrap_or_else(|| self.walk_secs_to_ep(origin, destination, u32::MAX, ep));
                return walk_fallback(w, access_secs, passes_run.saturating_sub(1));
            }
        }

        // Each Kept candidate maps, in order, to one plan in `all_plans`.
        let plan_to_sink_idx: Vec<usize> = all_cands
            .iter()
            .enumerate()
            .filter_map(|(ci, c)| matches!(c.status, CandidateStatus::Kept).then_some(ci))
            .collect();
        let final_plans =
            Self::pareto_filter_with_debug(all_plans, &plan_to_sink_idx, &mut all_cands, buckets);

        let access = AccessInfo {
            walk_radius_secs: pass_a_radius,
            walk_radius_meters: (pass_a_radius as f64 * self.raptor.walking_speed_mps) as u32,
            origin_stops_found: origin_stops,
            destination_stops_found: dest_stops,
            access_attempts: passes_run.saturating_sub(1),
            fell_back_to_walk_only: false,
        };
        (final_plans, all_cands, access, stops_reached)
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            unrestricted,
            use_cch,
            buckets,
            am,
            bike,
            ep,
            fare_profile,
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

    /// Endpoint coordinates for an `ExplainResult`: projected snap coords via `ep`, else
    /// `node_coord`.
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> ExplainResult {
        let (plans, candidates, access, stops_reached) = self.with_access_search_debug(
            origin,
            destination,
            start_time,
            min_access_secs,
            unrestricted,
            use_cch,
            buckets,
            am,
            bike,
            ep,
            fare_profile,
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
                    rt,
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

                // RAW union; Pass A/B Pareto-filtered once in `with_access_search_debug`.
                Some((all_plans, all_candidates, probe_stops))
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

    /// Read-only route scan over `pattern`, pushing surviving candidates into `out` in
    /// scan order. `best` is a lagging domination PREFILTER only (`apply_scan_candidates`
    /// re-checks the live set, so stale pruning here is sound). Write-free ⇒ parallelizable.
    #[allow(clippy::too_many_arguments)]
    fn scan_route_collect<R: LabelRow>(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: [u32; ALL_STATES.len()],
        prev: &R,
        best: &BestGrid,
        buckets: &ReliabilityBuckets,
        rt: &RealtimeIndex,
        stamp: u32,
        arena: &[Label],
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

        // Price is annotated post-hoc (`plan_price_posthoc`); the scan is price-blind.

        let all_times = self.raptor.transit_idx_pattern_stop_times[pattern]
            .of(&self.raptor.transit_pattern_stop_times);
        let trip_ids =
            self.raptor.transit_idx_pattern_trips[pattern].of(&self.raptor.transit_pattern_trips);

        // Riding Pareto set over (trip index ↓, bucket ↑): a smaller trip index arrives
        // earlier at every downstream stop.
        let mut riding: Vec<Riding> = Vec::new();

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.raptor.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            // 1. Settle arrivals at this stop for every riding label.
            for r in &riding {
                // No alighting on drop_off_type==1 or a realtime SKIPPED stop; keep riding.
                if !col[r.t].alight_allowed || rt.is_skipped(trip_ids[r.t], stop as u32) {
                    continue;
                }
                let arr = apply_delay(col[r.t].arrival, rt.delay(trip_ids[r.t], stop as u32));
                if arr >= cutoff[r.state as usize] {
                    continue;
                }
                let bag = if r.hit_prob < 1.0 {
                    let miss_arr = self.next_trip_arrival(
                        mc,
                        trip_ids,
                        r.t + 1,
                        col,
                        date,
                        weekday,
                        rt,
                        stop as u32,
                    );
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
                if best.dominates(stop * n_states + r.state as usize, cand, buckets) {
                    continue;
                }
                out.push(cand);
            }

            // 2. Board the earliest catchable trip, then later trips reaching a HIGHER
            //    reliability bucket (buckets rise monotonically with departure margin).
            let max_bucket = buckets.bucket(1.0);
            for sidx in 0..n_states {
                let prev_cell = stop * n_states + sidx;
                if !prev.is_reached(prev_cell) {
                    continue;
                }
                let prev_set = prev.cell(prev_cell);
                let needs_bikes = in_hand_idx == Some(sidx as u8);
                for pi in 0..prev_set.count() {
                    // Build ONLY from this pass's labels: an `i`-journey must descend from
                    // the `i`-source, else it fabricates `i`'s journey out of `j`'s. The
                    // carried grid still prunes across departures, just isn't a build source.
                    if prev_set.summary_at(pi).created_by != stamp {
                        continue;
                    }
                    let pl = prev_set.full_at(pi, arena);
                    let from_bucket = buckets.bucket(pl.reliability);
                    let min_dep = pl.bag.earliest();
                    let t_start = col.partition_point(|st| st.departure < min_dep);
                    let mut best_bucket_seen: Option<u8> = None;
                    for t in t_start..n_trips {
                        // Skip inactive or CANCELED trips (board the next running one).
                        if !self.is_trip_active_memo(mc, trip_ids[t], date, weekday)
                            || rt.is_canceled(trip_ids[t])
                        {
                            continue;
                        }
                        // No boarding on pickup_type==1 or a realtime SKIPPED stop.
                        if !col[t].board_allowed || rt.is_skipped(trip_ids[t], stop as u32) {
                            continue;
                        }
                        // Carrying a bike: only trips that explicitly allow it.
                        if needs_bikes
                            && self.raptor.transit_trips[trip_ids[t].0 as usize].bikes_allowed
                                != Some(true)
                        {
                            continue;
                        }
                        let trip_dep =
                            apply_delay(col[t].departure, rt.delay(trip_ids[t], stop as u32));
                        // Overtaking trips make the delayed column non-monotonic; guard
                        // against boarding before `min_dep`, else a label arrives before its
                        // parent (surfaces as a negative access-walk).
                        if trip_dep < min_dep {
                            continue;
                        }

                        // Cumulative reliability: same earliest-based per-transfer formula
                        // as reconstruction, so buckets agree.
                        let factor = self.transfer_on_time_prob(
                            pl.route_type,
                            Some(pat_rt),
                            pl.bag.earliest(),
                            trip_dep,
                        );
                        let rel = pl.reliability * factor;
                        let cb = buckets.bucket(rel);

                        // Board only if it reaches an as-yet-uncovered bucket (earliest
                        // trip per bucket level).
                        if best_bucket_seen.is_some_and(|bs| cb <= bs) {
                            continue;
                        }
                        best_bucket_seen = Some(cb);

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

    /// Applies scan candidates in queue order against the live grids (re-checks domination
    /// on the up-to-date `best`), making the result — arena ids included — identical to a
    /// sequential scan.
    #[allow(clippy::too_many_arguments)]
    fn apply_scan_candidates<R: LabelRow>(
        &self,
        cands: &[Label],
        curr: &mut R,
        best: &mut BestGrid,
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        for &cand in cands {
            // `best` must capture what THIS departure reaches (independent of the carried
            // grid), or `target_cutoff` degrades and the pass floods the whole network.
            // Only marking is gated on the carried per-round set.
            Self::insert_candidate(
                self, cand, curr, best, buckets, marked, is_marked, arena, n_states, drop_to,
            );
        }
    }

    /// Inserts `cand` into its `(stop, state)` cell (best-prune → arena → grids → mark),
    /// then inserts the free irreversible `BikeInHand`→`BikeDropped` drop when active.
    #[allow(clippy::too_many_arguments)]
    fn insert_candidate<R: LabelRow>(
        &self,
        cand: Label,
        curr: &mut R,
        best: &mut BestGrid,
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        let cell = cand.at_stop as usize * n_states + cand.state as usize;
        if !best.dominates(cell, cand, buckets) {
            let pushed = Label::arena_push(arena, cand);
            best.insert(cell, pushed, buckets);
            if curr.insert(cell, pushed, buckets) {
                Self::mark(cell, marked, is_marked);
            }
        }
        if let Some((in_hand, dropped)) = drop_to
            && cand.state == in_hand
        {
            let mut d = cand;
            d.state = dropped;
            let dcell = d.at_stop as usize * n_states + dropped as usize;
            if !best.dominates(dcell, d, buckets) {
                let pushed = Label::arena_push(arena, d);
                best.insert(dcell, pushed, buckets);
                if curr.insert(dcell, pushed, buckets) {
                    Self::mark(dcell, marked, is_marked);
                }
            }
        }
    }

    /// Parallel chunk count for a route-scan queue (1 = sequential). `MAAS_SCAN_THREADS`
    /// overrides the thread budget.
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

    /// Pareto-inserts a riding label over (trip index ↓, bucket ↑). Domination applies
    /// ONLY within the same vehicle state (a `Walked` rider must never be pruned by a
    /// bike-state one, or the walk plan vanishes before the plan-level burden comparison).
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

    /// Probability of catching a vehicle departing at `board_dep` given arrival at
    /// `arr_at_stop` on a preceding `prev_rt` leg (`1.0` if none / no delay model).
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

    #[allow(clippy::too_many_arguments)]
    fn next_trip_arrival(
        &self,
        mc: &ModeContext,
        trip_ids: &[TripId],
        start: usize,
        col: &[StopTime],
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
        stop: u32,
    ) -> Option<u32> {
        (start..trip_ids.len())
            .find(|&t| {
                // The miss-fallback trip must itself be running: skip CANCELED trips.
                self.is_trip_active_memo(mc, trip_ids[t], date, weekday)
                    && !rt.is_canceled(trip_ids[t])
            })
            .map(|t| apply_delay(col[t].arrival, rt.delay(trip_ids[t], stop)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn next_active_trip_departure(
        &self,
        trip_ids: &[TripId],
        after_trip: usize,
        boarding_col: &[StopTime],
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Option<u32> {
        (after_trip..trip_ids.len())
            .find(|&t| {
                // The displayed "next departure" must be a RUNNING trip: skip CANCELED.
                self.is_trip_active(trip_ids[t], date, weekday) && !rt.is_canceled(trip_ids[t])
            })
            .map(|t| boarding_col[t].departure)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_transfers<R: LabelRow>(
        &self,
        labels: &mut R,
        best: &mut BestGrid,
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
            let src = labels.cell(cell);
            if !src.is_reached() || src.earliest_arrival() >= state_cutoff {
                continue;
            }

            let transfers = self.raptor.transit_idx_stop_transfers[stop]
                .of(&self.raptor.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.raptor.transit_node_to_stop[target_node.0] as usize;

                for li in 0..src.count() {
                    // Transfer ONLY this pass's labels (an `i`-journey descends from the
                    // `i`-source); see `scan_route_collect`.
                    if src.summary_at(li).created_by != stamp {
                        continue;
                    }
                    let l = src.full_at(li, arena);
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

    /// MCR replacement for `apply_transfers`: resolve inter-stop transfers with a live,
    /// per-active-state, multi-source, cutoff-bounded foot-Dijkstra over the contracted
    /// junction graph (finds >1 km inter-stop walks the capped table drops). One Dijkstra
    /// per active state. Single-hop semantics are exact: a source stop is seeded OUT of
    /// (its junction never pushed), a stop-junction reached as target EMITS a candidate
    /// then dead-ends (never through-routed); each target settles at
    /// `min_s(arr(s) + dist_nonstop(s, t))`. The winning source's reliability bag is
    /// carried (shifted by the walk) — an approximation that strictly dominates the table.
    #[allow(clippy::too_many_arguments)]
    fn apply_transfers_mcr<R: LabelRow>(
        &self,
        mc: &ModeContext,
        labels: &mut R,
        best: &mut BestGrid,
        buckets: &ReliabilityBuckets,
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        cutoff: [u32; ALL_STATES.len()],
        stamp: u32,
        arena: &mut Vec<Label>,
        n_states: usize,
        drop_to: Option<(u8, u8)>,
    ) {
        use std::cmp::Reverse;

        let cg = self.contracted.as_ref().unwrap();
        let n_junctions = cg.junction_count();

        // Snapshot marked cells BEFORE any insert: inserts append marks that must not be
        // re-seeded (a transfer is a single hop, never chained).
        let n_marked = marked.len();

        // Collect all candidates, insert after the Dijkstras finish (the scratch borrow
        // must not overlap `insert_candidate`). Equivalent to the inline one-hop path.
        let mut cands: Vec<Label> = Vec::new();

        let mut settles: u64 = 0;

        TRANSFER_SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            scratch.ensure(n_junctions);

            for (sidx, _vs) in mc.am.states() {
                // NOTE: `target_cutoff` is `u32::MAX` for a burden with no egress reached
                // yet, so this Dijkstra is then UNBOUNDED and floods the whole foot
                // component. Sound and terminating, but a perf risk at country scale.
                let state_cutoff = cutoff[sidx];
                let sidx8 = sidx as u8;

                // Seeds: the min-earliest current-stamp label per marked cell of this state.
                struct Seed {
                    stop: usize,
                    ji: usize,
                    arr: u32,
                    label: Label,
                }
                let mut seeds: Vec<Seed> = Vec::new();
                for i in 0..n_marked {
                    let cell = marked[i];
                    if (cell % n_states) as u8 != sidx8 {
                        continue;
                    }
                    let stop = cell / n_states;
                    let set = labels.cell(cell);
                    if !set.is_reached() {
                        continue;
                    }
                    let mut chosen: Option<Label> = None;
                    for li in 0..set.count() {
                        let sm = set.summary_at(li);
                        if sm.created_by != stamp {
                            continue;
                        }
                        if sm.earliest >= state_cutoff {
                            continue;
                        }
                        match chosen {
                            Some(c) if c.bag.earliest() <= sm.earliest => {}
                            _ => chosen = Some(set.full_at(li, arena)),
                        }
                    }
                    let Some(label) = chosen else { continue };
                    let node = self.raptor.transit_stop_to_node[stop];
                    let Some(&ji) = cg.junction_of.get(node.0).filter(|&&j| j != u32::MAX) else {
                        continue;
                    };
                    seeds.push(Seed {
                        stop,
                        ji: ji as usize,
                        arr: label.bag.earliest(),
                        label,
                    });
                }
                if seeds.is_empty() {
                    continue;
                }

                scratch.new_generation();
                // Seed OUT of each source stop-junction (never push it — dead-end for
                // through-walking).
                for (slot, seed) in seeds.iter().enumerate() {
                    for se in &cg.adjacency[seed.ji] {
                        let Some(t) = cg.walk_secs(self, se) else {
                            continue;
                        };
                        let nd = seed.arr.saturating_add(t);
                        if nd > state_cutoff {
                            continue;
                        }
                        let to = se.to as usize;
                        if nd < scratch.get(to) {
                            scratch.set(to, nd, slot as u32);
                            scratch.heap.push(Reverse((nd, to as u32)));
                        }
                    }
                }

                while let Some(Reverse((d, ji_u))) = scratch.heap.pop() {
                    let ji = ji_u as usize;
                    // Generation-aware stale check: `vgen` mismatch = unvisited this gen.
                    if scratch.vgen[ji] != scratch.cur_gen || d > scratch.dist[ji] {
                        continue;
                    }
                    settles += 1;
                    let jn = cg.junctions[ji];
                    let target_compact = self
                        .raptor
                        .transit_node_to_stop
                        .get(jn.0)
                        .copied()
                        .unwrap_or(u32::MAX);
                    if target_compact != u32::MAX {
                        // Reached a stop as a transfer target: emit from its winning source,
                        // then dead-end (never through-route at a stop).
                        let slot = scratch.src[ji] as usize;
                        let seed = &seeds[slot];
                        let target = target_compact as usize;
                        // Self-transfer is always dominated by the source's own label; skip.
                        if target != seed.stop {
                            let walk = d.saturating_sub(seed.arr);
                            let bag = seed.label.bag.shifted_by(walk);
                            if bag.earliest() < state_cutoff {
                            let l = &seed.label;
                            cands.push(Label {
                                bag,
                                route_type: l.route_type,
                                reliability: l.reliability,
                                trace: Trace {
                                    pattern: u32::MAX,
                                    trip: u32::MAX,
                                    boarded_at: u32::MAX,
                                    alighted_at: u32::MAX,
                                    from_stop: seed.stop as u32,
                                    from_bucket: buckets.bucket(l.reliability),
                                },
                                created_by: stamp,
                                at_stop: target as u32,
                                round: l.round,
                                parent: l.arena_id,
                                arena_id: u32::MAX,
                                state: sidx8,
                                });
                            }
                        }
                        continue;
                    }
                    // Non-stop junction: expand, carrying the winning source slot.
                    let slot = scratch.src[ji];
                    for se in &cg.adjacency[ji] {
                        let Some(t) = cg.walk_secs(self, se) else {
                            continue;
                        };
                        let nd = d.saturating_add(t);
                        if nd > state_cutoff {
                            continue;
                        }
                        let to = se.to as usize;
                        if nd < scratch.get(to) {
                            scratch.set(to, nd, slot);
                            scratch.heap.push(Reverse((nd, to as u32)));
                        }
                    }
                }
            }
        });

        if std::env::var_os("MAAS_MCR_DEBUG").is_some() {
            eprintln!(
                "[mcr] stamp={stamp} settles={settles} emitted={}",
                cands.len()
            );
        }

        for cand in cands {
            self.insert_candidate(
                cand, labels, best, buckets, marked, is_marked, arena, n_states, drop_to,
            );
        }
    }

    #[inline]
    pub(super) fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.raptor.transit_trips[trip_id.0 as usize].service_id;
        self.raptor.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    /// Memoized `is_trip_active`. With a `TripActiveMemo` the tri-state cache is
    /// consulted/filled; else an exact passthrough. Byte-identical (memo stores the pure
    /// result for the query's fixed `(date, weekday)`).
    #[inline]
    pub(super) fn is_trip_active_memo(
        &self,
        mc: &ModeContext,
        trip_id: TripId,
        date: u32,
        weekday: u8,
    ) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        match &mc.trip_active_memo {
            Some(m) => {
                #[cfg(debug_assertions)]
                {
                    let key = ((date as u64) << 8) | weekday as u64;
                    let prev = m.dw.swap(key, Relaxed);
                    debug_assert!(
                        prev == u64::MAX || prev == key,
                        "TripActiveMemo reused across differing (date, weekday)"
                    );
                }
                let slot = &m.states[trip_id.0 as usize];
                match slot.load(Relaxed) {
                    1 => false,
                    2 => true,
                    _ => {
                        let active = self.is_trip_active(trip_id, date, weekday);
                        slot.store(if active { 2 } else { 1 }, Relaxed);
                        active
                    }
                }
            }
            None => self.is_trip_active(trip_id, date, weekday),
        }
    }

    #[inline]
    /// Per-compact-state arrival cutoff. A label of burden `b` is bounded only by the best
    /// egress arrival across burdens `≤ b` (+ `slack`): a heavier state must never tighten
    /// a lighter state's cutoff, or the lighter plan is starved before the plan-level burden
    /// Pareto can protect it. Unreached burdens leave `u32::MAX` (no pruning).
    fn target_cutoff(best: &BestGrid, mc: &ModeContext, slack: u32) -> [u32; ALL_STATES.len()] {
        let n_states = mc.n_states();
        let mut per_burden = [u32::MAX; 3];
        for (sidx, vs) in mc.am.states() {
            let b = vs.burden() as usize;
            for &(s, w) in &mc.egress[sidx] {
                let cell = s * n_states + sidx;
                if best.is_reached(cell) {
                    per_burden[b] = per_burden[b].min(best.earliest(cell).saturating_add(w));
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
        // OPT-B opt-in absolute-time horizon (travel-map only, `None` in production): cap
        // each cutoff at `h`. RAPTOR arrivals are monotone non-decreasing, so pruning any
        // arrival `> h` cannot change an arrival `<= h`; bit-identical for a `<= h` isochrone.
        if let Some(h) = mc.horizon {
            for c in out.iter_mut() {
                *c = (*c).min(h);
            }
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

    /// Origin-departure times in `[earliest, latest]` where a vehicle departs some access
    /// stop at `T + walk_secs`. Returned ascending.
    #[allow(clippy::too_many_arguments)]
    fn collect_interesting_times(
        &self,
        raw_stops: &[(usize, u32)],
        earliest_origin_departure: u32,
        latest_origin_departure: u32,
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
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
                    // Skip CANCELED trips so a dead departure doesn't consume the
                    // MAX_TOTAL budget.
                    if self.is_trip_active(trip_ids[t], date, weekday)
                        && !rt.is_canceled(trip_ids[t])
                    {
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

    /// Range-RAPTOR over every interesting departure in `[start_time, +window_secs]`.
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
            self.raptor.unrestricted_transfers,
            self.raptor.use_cch_access,
            rt,
            am,
            bike,
            None,
            crate::structures::cost::FareProfile::default(),
        )
    }

    /// `raptor_range_tuned_rt_modes` carrying projected snap coordinates (`ep`).
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> Vec<Plan> {
        // Self-pruning rRAPTOR: one carried grid, departures processed latest → earliest so
        // a later-departing journey prunes earlier ones. Each pass reconstructs its own
        // plans (filtered by `created_by`). Output is the 4-D Pareto set (departure ↑,
        // arrival ↓, transfers ↓, reliability ↑). The backward memo is shared across all
        // passes/departures: `raptor_backward` is a pure function of its key, so exact.
        let mut bw_cache: std::collections::HashMap<(usize, u32, usize, u32, u8), Vec<Vec<u32>>> =
            std::collections::HashMap::new();
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            unrestricted,
            use_cch,
            buckets,
            am,
            bike,
            ep,
            fare_profile,
            |mc, access_secs| {
                // Empty window ⇒ run the probe (the only source of "next service is after
                // the window", since the range loop is window-bounded) and return it raw.
                // Else skip the probe; `range_departures` decides feasibility via an exact
                // monotone forward-reachability guard.
                let departure_times = self.collect_interesting_times(
                    &mc.merged_access(),
                    start_time,
                    start_time.saturating_add(window_secs),
                    date,
                    weekday,
                    rt,
                );
                if departure_times.is_empty() {
                    return latency_profile::time_probe(|| {
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
                    });
                }

                let all_plans = if slim_grid_enabled() {
                    self.range_departures::<SlimRow>(
                        mc,
                        start_time,
                        departure_times,
                        access_secs,
                        date,
                        weekday,
                        origin,
                        destination,
                        buckets,
                        slack,
                        rt,
                        &mut bw_cache,
                    )
                } else {
                    self.range_departures::<FullRow>(
                        mc,
                        start_time,
                        departure_times,
                        access_secs,
                        date,
                        weekday,
                        origin,
                        destination,
                        buckets,
                        slack,
                        rt,
                        &mut bw_cache,
                    )
                };
                Self::finalize_plans(all_plans, buckets)
            },
        )
    }

    /// Self-pruning range departure loop: one carried grid, departures latest → earliest.
    #[allow(clippy::too_many_arguments)]
    fn range_departures<R: LabelRow>(
        &self,
        mc: &ModeContext,
        start_time: u32,
        departure_times: Vec<u32>,
        access_secs: u32,
        date: u32,
        weekday: u8,
        origin: NodeID,
        destination: NodeID,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
        bw_cache: &mut std::collections::HashMap<(usize, u32, usize, u32, u8), Vec<Vec<u32>>>,
    ) -> Vec<Plan> {
        let n_cells = self.raptor.transit_stop_to_node.len() * mc.n_states();
        let n_patterns = self.raptor.transit_patterns.len();
        let (mut best, mut labels) = latency_profile::time_grid_alloc(|| {
            (
                BestGrid::new(n_cells, buckets),
                (0..=MAX_ROUNDS).map(|_| R::empty(n_cells)).collect::<Vec<R>>(),
            )
        });
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
            // Feasibility guard (perf only). The first pass (latest departure) has minimal
            // reachability, so its empty egress proves nothing. Instead probe `start_time`,
            // the MAXIMAL-reachability seed (monotone: seeding earlier boards a superset of
            // trips); it floods with no cutoff pruning so its reached set is EXACT. If even
            // it reaches no egress, the whole range is provably empty.
            let mut infeasible = false;
            let plans = latency_profile::time_range_departure(|| {
                latency_profile::time_forward(|| {
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
                    )
                });
                if i == 0
                    && !Self::egress_reached(&best, mc)
                    && !self.forward_reaches_egress::<R>(
                        mc, start_time, access_secs, date, weekday, buckets, slack, rt,
                    )
                {
                    infeasible = true;
                    return Vec::new();
                }
                latency_profile::time_extract(|| {
                    self.extract_with_debug(
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
                        bw_cache,
                    )
                })
            });
            if infeasible {
                return Vec::new();
            }
            all_plans.extend(plans);
        }
        all_plans
    }

    /// True iff any active-state egress cell is reached in `best` (feasibility guard).
    fn egress_reached(best: &BestGrid, mc: &ModeContext) -> bool {
        let n_states = mc.n_states();
        for (sidx, _vs) in mc.am.states() {
            for &(s, _w) in &mc.egress[sidx] {
                if best.is_reached(s * n_states + sidx) {
                    return true;
                }
            }
        }
        false
    }

    /// Forward-only RAPTOR pass at `start_time` into FRESH throwaway grids (the range
    /// loop's carried `labels`/`arena` must not be touched), returning egress reachability.
    #[allow(clippy::too_many_arguments)]
    fn forward_reaches_egress<R: LabelRow>(
        &self,
        mc: &ModeContext,
        start_time: u32,
        access_secs: u32,
        date: u32,
        weekday: u8,
        buckets: &ReliabilityBuckets,
        slack: u32,
        rt: &RealtimeIndex,
    ) -> bool {
        let n_cells = self.raptor.transit_stop_to_node.len() * mc.n_states();
        let n_patterns = self.raptor.transit_patterns.len();
        let mut best = BestGrid::new(n_cells, buckets);
        let mut labels: Vec<R> = (0..=MAX_ROUNDS).map(|_| R::empty(n_cells)).collect();
        let mut marked = Vec::new();
        let mut is_marked = vec![false; n_cells];
        let mut queue = Vec::new();
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
        Self::egress_reached(&best, mc)
    }

    /// Reference range driver: each departure runs as an independent from-scratch pass.
    /// Correctness oracle for `raptor_range_tuned_rt` (their 4-D outputs must be set-equal).
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
            self.raptor.unrestricted_transfers,
            self.raptor.use_cch_access,
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
    ) -> Vec<Plan> {
        self.with_access_search(
            origin,
            destination,
            start_time,
            min_access_secs,
            slack,
            unrestricted,
            use_cch,
            buckets,
            am,
            &self.default_bike_cost(),
            None,
            crate::structures::cost::FareProfile::default(),
            |mc, access_secs| {
                // Empty window ⇒ run the probe and return it raw; else run every departure
                // from scratch. Set-equal to the tuned driver (its reachability short-circuit
                // returns `vec![]` exactly when this loop would be empty).
                let departure_times = self.collect_interesting_times(
                    &mc.merged_access(),
                    start_time,
                    start_time.saturating_add(window_secs),
                    date,
                    weekday,
                    rt,
                );
                if departure_times.is_empty() {
                    return self.raptor_inner(
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

    /// Public reference range query (default buckets / no realtime), the oracle. For tests.
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
    /// Trips departing before this threshold may be overnight extensions of the previous
    /// service day (GTFS times > 86400).
    const OVERNIGHT_THRESHOLD_SECS: u32 = 5 * 3600;

    /// Rotate a 7-bit weekday bitmask one day backward (Mon=0x01 → Sun=0x40).
    fn prev_weekday(wd: u8) -> u8 {
        ((wd >> 1) | ((wd & 1) << 6)) & 0x7F
    }

    /// Rotate a 7-bit weekday bitmask one day forward (Sun=0x40 → Mon=0x01).
    fn next_weekday(wd: u8) -> u8 {
        ((wd << 1) | ((wd & 0x40) >> 6)) & 0x7F
    }

    /// Subtract signed `shift` from every time field. `shift > 0` normalizes a date-1
    /// overnight pass down into the query day; `shift < 0` normalizes a date+1 pass up.
    /// Leg `date`/`weekday` are left UNTOUCHED (the trip's listed service day; recovered
    /// via `raw = displayed + time_shift`). Times clamp at 0.
    fn shift_plan(mut plan: Plan, shift: i64) -> Plan {
        let sub = |x: u32| (x as i64 - shift).max(0) as u32;
        plan.start = sub(plan.start);
        plan.end = sub(plan.end);
        plan.expected_end = sub(plan.expected_end);
        for s in &mut plan.arrival_distribution {
            s.time = sub(s.time);
        }
        for leg in &mut plan.legs {
            match leg {
                PlanLeg::Walk(w) => {
                    w.start = sub(w.start);
                    w.end = sub(w.end);
                    for step in &mut w.steps {
                        *step = match *step {
                            PlanLegStep::Walk(mut ws) => {
                                ws.time = sub(ws.time);
                                PlanLegStep::Walk(ws)
                            }
                            PlanLegStep::Transit(mut ts) => {
                                ts.time = sub(ts.time);
                                PlanLegStep::Transit(ts)
                            }
                        };
                    }
                }
                PlanLeg::Transit(t) => {
                    t.start = sub(t.start);
                    t.end = sub(t.end);
                    t.scheduled_start = sub(t.scheduled_start);
                    t.scheduled_end = sub(t.scheduled_end);
                    // Boarding/alighting stop times must move with the leg.
                    t.from.departure = t.from.departure.map(sub);
                    t.to.arrival = t.to.arrival.map(sub);
                    if let Some(tr) = &mut t.transfer_risk {
                        tr.scheduled_departure = sub(tr.scheduled_departure);
                        tr.next_departure = tr.next_departure.map(sub);
                    }
                    t.preceding_arrival = t.preceding_arrival.map(sub);
                    for step in &mut t.steps {
                        *step = match *step {
                            PlanLegStep::Walk(mut ws) => {
                                ws.time = sub(ws.time);
                                PlanLegStep::Walk(ws)
                            }
                            PlanLegStep::Transit(mut ts) => {
                                ts.time = sub(ts.time);
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

    /// Earliest-arrival probe on the NEXT service day (searched `date+1` from midnight, then
    /// shifted `+86400` so plans carry next-day-clock times and Pareto-dominate the "ride
    /// partway + huge egress walk" fabrication). Filtered to TRANSIT-bearing plans: a next-day
    /// street-only copy would survive Pareto as a spurious incomparable duplicate.
    #[allow(clippy::too_many_arguments)]
    fn next_day_transit_fallback(
        &self,
        origin: NodeID,
        destination: NodeID,
        date: u32,
        weekday: u8,
        min_access_secs: u32,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> Vec<Plan> {
        let forward = self.raptor_tuned_rt_modes_ep(
            origin,
            destination,
            0,
            date + 1,
            Self::next_weekday(weekday),
            min_access_secs,
            buckets,
            slack,
            unrestricted,
            use_cch,
            rt,
            am,
            bike,
            ep,
            fare_profile,
        );
        forward
            .into_iter()
            .filter(|p| p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_))))
            .map(|p| Self::shift_plan(p, -86400))
            .collect()
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
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
            unrestricted,
            use_cch,
            rt,
            am,
            bike,
            ep,
            fare_profile,
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
                unrestricted,
                use_cch,
                rt,
                am,
                bike,
                ep,
                fare_profile,
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

        // The date+1 fallback is GATED on `!same_day_ok` (no plan reaches the dest before
        // midnight): a blind forward extension departs tomorrow and, once shifted, is
        // Pareto-INCOMPARABLE to a same-day plan, so it would survive `finalize_plans` with
        // spurious departures. A normal same-day query keeps `same_day_ok` ⇒ no-op.
        let same_day_ok = plans.iter().any(|p| p.end < 86400);
        if !same_day_ok {
            let forward = self.next_day_transit_fallback(
                origin,
                destination,
                date,
                weekday,
                min_access_secs,
                buckets,
                slack,
                unrestricted,
                use_cch,
                rt,
                am,
                bike,
                ep,
                fare_profile,
            );
            if !forward.is_empty() {
                plans.extend(forward);
                plans = Self::finalize_plans(plans, buckets);
            }
        }

        plans
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
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        am: &ActiveModes,
        bike: &BikeCost,
        ep: Option<&QueryEndpoints>,
        fare_profile: crate::structures::cost::FareProfile,
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
            unrestricted,
            use_cch,
            rt,
            am,
            bike,
            ep,
            fare_profile,
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
                unrestricted,
                use_cch,
                rt,
                am,
                bike,
                ep,
                fare_profile,
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

        // Forward (date+1) extension when the window crosses midnight: next-day trips at
        // early times fall in the window but are invisible to the date-day pass. The forward
        // window is TRIMMED to the crossing tail so that after the +86400 shift the covered
        // departures land precisely on `[86400, start_time+window_secs]` and never past it.
        // Gate `start+window > 86400` makes it a no-op for a daytime window.
        if start_time.saturating_add(window_secs) > 86400 {
            let eff_start = start_time.saturating_sub(86400);
            let fwd_window = start_time
                .saturating_add(window_secs)
                .saturating_sub(86400)
                .saturating_sub(eff_start);
            let forward = self.raptor_range_tuned_rt_modes_ep(
                origin,
                destination,
                eff_start,
                fwd_window,
                date + 1,
                Self::next_weekday(weekday),
                min_access_secs,
                buckets,
                slack,
                unrestricted,
                use_cch,
                rt,
                am,
                bike,
                ep,
                fare_profile,
            );
            // Enforce the window bound on DEPARTURE, not boarding: the range driver's
            // empty-window probe can board an arbitrarily-late date+1 trip that survives
            // `finalize_plans` after the shift. A legitimate forward gain always has its
            // `start` (origin-leave time) <= the window end.
            let window_end = start_time.saturating_add(window_secs);
            let normalized: Vec<Plan> = forward
                .into_iter()
                .map(|p| Self::shift_plan(p, -86400))
                .filter(|p| p.start <= window_end)
                .collect();
            if !normalized.is_empty() {
                plans.extend(normalized);
                plans = Self::finalize_plans(plans, buckets);
            }
        }

        // Degenerate fallback when the query day can't reach the dest before midnight (the
        // crossing block above only helps when the WINDOW itself spilled past midnight).
        // Gated on `!same_day_ok`, so a normal same-day-reaching window is a no-op. Crossing-
        // block plans depart tomorrow (`end >= 86400`) so never flip `same_day_ok`.
        let same_day_ok = plans.iter().any(|p| p.end < 86400);
        if !same_day_ok {
            let forward = self.next_day_transit_fallback(
                origin,
                destination,
                date,
                weekday,
                min_access_secs,
                buckets,
                slack,
                unrestricted,
                use_cch,
                rt,
                am,
                bike,
                ep,
                fare_profile,
            );
            if !forward.is_empty() {
                plans.extend(forward);
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
    fn labelset_dominance_is_price_blind() {
        // Search is price-blind: a strictly-later same-bucket label is dominated.
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl(100, 1.0), &b));
        assert!(
            !s.insert(lbl(200, 1.0), &b),
            "later same-bucket label is dominated (search is price-blind)"
        );
        assert_eq!(s.iter().count(), 1);
    }

    /// `next_weekday` is the exact inverse of `prev_weekday` and wraps Sun→Mon (a bijection
    /// on the 7-bit ring, so the date±1 overnight passes land on the right service day).
    #[test]
    fn next_weekday_rotates_and_wraps() {
        assert_eq!(Graph::next_weekday(0x01), 0x02);
        assert_eq!(Graph::next_weekday(0x10), 0x20);
        assert_eq!(Graph::next_weekday(0x20), 0x40);
        assert_eq!(Graph::next_weekday(0x40), 0x01); // Sun → Mon (wrap)
        for i in 0..7u8 {
            let wd = 1u8 << i;
            assert_eq!(Graph::prev_weekday(Graph::next_weekday(wd)), wd);
            assert_eq!(Graph::next_weekday(Graph::prev_weekday(wd)), wd);
        }
    }

    #[test]
    fn trip_active_memo_tristate_roundtrip() {
        // Memo contract: fresh reads `unknown` (0); a filled slot reads back its bool; fill
        // is idempotent. This is the invariant `is_trip_active_memo` relies on.
        use std::sync::atomic::Ordering::Relaxed;
        let m = TripActiveMemo::new(4);
        for slot in &m.states {
            assert_eq!(slot.load(Relaxed), 0, "fresh slot must be unknown");
        }
        let fill = |i: usize, active: bool| {
            let slot = &m.states[i];
            if slot.load(Relaxed) == 0 {
                slot.store(if active { 2 } else { 1 }, Relaxed);
            }
            match slot.load(Relaxed) {
                1 => false,
                2 => true,
                _ => unreachable!(),
            }
        };
        assert!(fill(0, true));
        assert!(!fill(1, false));
        // Cache hit: value is stable, independent of the re-passed truth.
        assert!(fill(0, false));
        assert!(!fill(1, true));
        assert_eq!(m.states[2].load(Relaxed), 0, "untouched slot stays unknown");
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

    /// R3 exactness: the compact `best` backend must return BIT-IDENTICAL
    /// `is_reached`/`earliest`/`dominates` to the legacy backend after any same-stamp
    /// insert stream. Driven with a deterministic pseudo-random sequence.
    #[test]
    fn bestgrid_compact_matches_labels_exactly() {
        let b = ReliabilityBuckets::default();
        let n_buckets = b.bucket(1.0) as usize + 1;
        let n_cells = 8usize;
        let mut compact = BestGrid::Compact {
            arr: vec![u32::MAX; n_cells * n_buckets],
            n_buckets,
            n_cells,
        };
        let mut legacy = BestGrid::Labels(vec![LabelSet::EMPTY; n_cells]);

        // Reliabilities chosen to land in each bucket, incl. the CERTAIN band.
        let rels = [0.05f32, 0.40, 0.55, 0.70, 0.85, 0.92, 0.97, 0.999, 1.0];
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as u32
        };

        for _ in 0..4000 {
            let cell = (next() as usize) % n_cells;
            let time = 30_000 + (next() % 12_000);
            let rel = rels[(next() as usize) % rels.len()];
            let mut cand = lbl(time, rel);
            cand.at_stop = cell as u32;
            compact.insert(cell, cand, &b);
            legacy.insert(cell, cand, &b);

            assert_eq!(compact.is_reached(cell), legacy.is_reached(cell), "is_reached cell {cell}");
            assert_eq!(compact.earliest(cell), legacy.earliest(cell), "earliest cell {cell}");
            for &pr in &rels {
                for pt in [20_000u32, 35_000, 36_000, 42_000, 50_000] {
                    let probe = lbl(pt, pr);
                    assert_eq!(
                        compact.dominates(cell, probe, &b),
                        legacy.dominates(cell, probe, &b),
                        "dominates cell {cell} probe (t={pt}, rel={pr})"
                    );
                }
            }
        }
        for cell in 0..n_cells {
            assert_eq!(compact.is_reached(cell), legacy.is_reached(cell));
            assert_eq!(compact.earliest(cell), legacy.earliest(cell));
            for &pr in &rels {
                for pt in [0u32, 30_000, 40_000, 45_000, u32::MAX] {
                    let probe = lbl(pt, pr);
                    assert_eq!(
                        compact.dominates(cell, probe, &b),
                        legacy.dominates(cell, probe, &b),
                    );
                }
            }
        }

        compact.reset();
        legacy.reset();
        for cell in 0..n_cells {
            assert!(!compact.is_reached(cell));
            assert!(!legacy.is_reached(cell));
            assert_eq!(compact.earliest(cell), u32::MAX);
            assert_eq!(legacy.earliest(cell), u32::MAX);
        }
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
        let b = ReliabilityBuckets::default();
        let mut s = LabelSet::EMPTY;
        assert!(s.insert(lbl_stamped(100, 0.81, 0), &b));
        // Same bucket & arrival, HIGHER precise reliability: must NOT be bucket-pruned.
        assert!(
            s.insert(lbl_stamped(100, 0.94, 1), &b),
            "cross-stamp bucket prune dropped a higher-precise-reliability label"
        );
        // Ghost precise reliability >= candidate's: the prune IS sound and must fire.
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
        let mut ghost = lbl_stamped(100, 0.99, 0);
        ghost.round = 3;
        assert!(s.insert(ghost, &b));
        // Fewer-leg label with worse arrival/rel must coexist (not pruned by more legs).
        let mut cand = lbl_stamped(120, 0.85, 1);
        cand.round = 1;
        assert!(
            s.insert(cand, &b),
            "ghost with more transit legs pruned a fewer-leg label (transfers axis)"
        );
        // Ghost with <= legs soundly dominates.
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
        // MAX_LABELS ghosts, pairwise non-dominated cross-stamp, so all coexist.
        for i in 0..MAX_LABELS {
            let g = lbl_stamped(100 + i as u32, 0.951 + 0.003 * i as f32, i as u32);
            assert!(s.insert(g, &b), "ghost {i} should coexist");
        }
        assert_eq!(s.iter().count(), MAX_LABELS);
        // Current-stamp candidate no ghost dominates: must displace a ghost (ghosts can
        // never appear in output again), not lose the bucket-based worst-replacement.
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
        assert_eq!(g.vehicle_access_budget(10_000), 1200, "short trip keeps the floor");
        assert_eq!(g.vehicle_access_budget(30_000), 1800, "long trip rides farther");
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
