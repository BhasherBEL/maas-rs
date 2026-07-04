//! Mode-parametrized multi-objective (Pareto) street search over the Phase-0
//! `CostVector`. Martins-style label-setting: a lexicographically-ordered queue
//! makes each popped label permanent; per-node `LabelSet`s hold the non-dominated
//! frontier, ε-pruned to stay sparse. Probability never enters here — the cost
//! vector is fully deterministic.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::structures::cost::{
    Axis, CostVector, CostWeights, Epsilon, LegRole, RoutingMode, edge_cost_vector,
};
use crate::structures::{BikeCost, BikeProfile, EdgeData, LatLng, NodeID, StreetEdgeData};

use super::contraction::SuperEdge;
use super::{Graph, PrevCtx};

/// Objective-space grid for bucket pruning. Cell size `sizes[i] > 0.0` snaps axis
/// `i` to fixed cells; `0.0` leaves it un-bucketed. Unlike ε-dominance (relative,
/// non-transitive — a chain of near-neighbours each survive, and a faster
/// neighbour can *absorb* an axis extreme), a fixed grid keeps one representative
/// per cell, so the extreme cell keeps its own point. Used to bound the per-node
/// frontier on the diversity axes while preserving the span the user routes on.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Buckets {
    sizes: [f64; crate::structures::cost::AXIS_COUNT],
}

impl Buckets {
    pub(super) const NONE: Buckets = Buckets {
        sizes: [0.0; crate::structures::cost::AXIS_COUNT],
    };

    #[inline]
    fn active(&self) -> bool {
        self.sizes.iter().any(|&s| s > 0.0)
    }

    /// Exact integer cell key over the bucketed axes (20 bits each, ≤3 axes ⇒
    /// fits u64 with no collision). Costs are non-negative, so the floored index
    /// is non-negative. Inactive axes contribute nothing. The index is naturally
    /// ~1/k (cell size and axis value both scale with route distance), far below
    /// the 20-bit field; the clamp is defensive against a pathological tiny cell.
    #[inline]
    fn cell(&self, c: &CostVector) -> u64 {
        let mut key = 0u64;
        for (i, &ax) in Axis::ALL.iter().enumerate() {
            if self.sizes[i] > 0.0 {
                let idx = ((c.get(ax) / self.sizes[i]).floor().max(0.0) as u64).min(0xF_FFFF);
                key = (key << 20) | idx;
            }
        }
        key
    }
}

/// The non-dominated label frontier at a single node. Small (ε-pruned, optionally
/// bucket-capped), so a linear scan on insert is the right data structure.
#[derive(Debug, Default)]
pub(super) struct LabelSet {
    costs: Vec<CostVector>,
}

impl LabelSet {
    #[cfg(test)]
    pub(super) fn new() -> Self {
        LabelSet { costs: Vec::new() }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.costs.len()
    }

    /// Try to admit `c`. Returns false (rejecting `c`) when an existing label
    /// *weakly* dominates it (≤ on every axis, equality included) or ε-dominates
    /// it. The weak test is essential: `dominates` requires a strict improvement,
    /// so two equal cost vectors neither dominate nor ε-dominate each other — and
    /// without weak rejection a hot node accumulates tens of thousands of byte-
    /// identical labels, turning every scan over it quadratic. Otherwise inserts
    /// `c`, evicting every existing label that `c` strictly or ε-dominates.
    pub(super) fn try_add(&mut self, c: CostVector, eps: &Epsilon, buckets: &Buckets) -> bool {
        if self
            .costs
            .iter()
            .any(|e| e.weakly_dominates(&c) || e.eps_dominates(&c, eps))
        {
            return false;
        }
        self.costs
            .retain(|e| !c.dominates(e) && !c.eps_dominates(e, eps));
        // Bucket cap: at most one label per objective-space cell. `c` is already
        // non-dominated here; if it shares a cell with a survivor, keep the
        // lexicographically smaller one (Time-first ⇒ "fastest for this trade-off"),
        // so the per-node frontier is bounded by the cell count, not the ε grid.
        if buckets.active() {
            let ck = buckets.cell(&c);
            if let Some(pos) = self.costs.iter().position(|e| buckets.cell(e) == ck) {
                if lex_cmp(&c, &self.costs[pos]) == Ordering::Less {
                    self.costs[pos] = c;
                    return true;
                }
                return false;
            }
        }
        self.costs.push(c);
        true
    }

    /// Exact membership (used by the search's stale-label check).
    pub(super) fn contains(&self, c: &CostVector) -> bool {
        self.costs.iter().any(|e| e == c)
    }
}

/// Per-node admissible lower bounds on each active axis' remaining cost to the
/// destination. Each entry is a `CostVector` whose active-axis components never
/// exceed the true minimum remaining cost on that axis, so adding it to a label's
/// `g` yields an admissible `f` key. Inactive/unreachable axes stay at 0.0 (always
/// a valid underestimate).
#[derive(Debug, Default)]
pub(super) struct Heuristics {
    per_node: Vec<CostVector>,
}

impl Heuristics {
    #[inline]
    fn h(&self, node: NodeID) -> CostVector {
        self.per_node[node.0]
    }
}

/// One path in the destination Pareto front.
#[derive(Debug, Clone)]
pub struct ParetoPath {
    pub nodes: Vec<NodeID>,
    /// Per-step `(arena edge, dir, far coord)` aligned to `nodes.windows(2)`, carried
    /// from the arena so reconstruction (geometry/length/surface/dismount) is g-free.
    /// Empty when the search ran off-contract (reconstruction falls back to `path_edges`).
    pub edges: Vec<(StreetEdgeData, (f64, f64), LatLng)>,
    pub cost: CostVector,
    /// Bike elevation hysteresis buffer state at the destination `(ehbd, ehbu)`.
    pub elev_buffer: (f64, f64),
}

#[derive(Debug, Clone, Default)]
pub struct MultiObjResult {
    pub front: Vec<ParetoPath>,
    /// Number of labels popped from the queue (test-only diagnostic; proves that
    /// target pruning bounds the search instead of exploring the whole envelope).
    #[cfg(test)]
    pub expansions: u64,
    /// Total labels created (test-only diagnostic for the perf bottleneck study).
    #[cfg(test)]
    pub total_labels: usize,
    /// Largest per-node Pareto frontier surviving at search end (test-only).
    #[cfg(test)]
    pub max_labels_per_node: usize,
    /// Number of distinct nodes that received at least one label (test-only).
    #[cfg(test)]
    pub nodes_explored: usize,
}

// Test-only profiler: total time + calls spent in `street_edge_transition`, to see
// whether the per-segment cost (not label/Pareto-set ops) is the multiobj bottleneck.
#[cfg(test)]
thread_local! {
    pub(super) static TRANS_N: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

struct Label {
    node: NodeID,
    cost: CostVector,
    elev: (f64, f64),
    parent: Option<usize>,
    len: u32,
    var_accum: f64,
    /// First node entered from the parent junction (the start of the degree-2 chain
    /// this label's edge contracts). Equals `node` for an un-contracted single hop.
    /// Lets path reconstruction re-walk the chain to recover the shape geometry.
    first_step: NodeID,
    /// Direction of the actual last edge arriving at `node` — the incoming vector for
    /// the turn-variance term. Under contraction the parent is several nodes back, so
    /// this cannot be derived from the parent and must be carried explicitly.
    arrive_dir: (f64, f64),
    /// Length (m), cruise speed (m/s) and push-state of that last arriving edge, so the
    /// per-vertex speed-change cost (corner radius needs `min(L_prev, L_this)`; the
    /// dismount stop needs the prev ride's cruise) can be charged at the next edge.
    arrive_len: f64,
    arrive_cruise: f64,
    arrive_push: bool,
    /// Carried (exit) speed of that last arriving edge — the speed the bike actually
    /// leaves it at (bend's safe speed / 0 mid-push / cruise on a straight). Lets the
    /// next vertex charge only the change to its required speed, so a sustained curve
    /// is one decel-in + one accel-out rather than a slow-down per segment.
    arrive_speed: f64,
}

/// Heap entry. The `BinaryHeap` is a max-heap, so `Ord` is reversed to pop the
/// lexicographically smallest cost vector first — the ordering that makes
/// label-setting sound for multi-objective search.
struct QLabel {
    key: CostVector,
    idx: usize,
}
impl PartialEq for QLabel {
    fn eq(&self, o: &Self) -> bool {
        lex_cmp(&self.key, &o.key) == Ordering::Equal
    }
}
impl Eq for QLabel {}
impl PartialOrd for QLabel {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for QLabel {
    fn cmp(&self, o: &Self) -> Ordering {
        lex_cmp(&o.key, &self.key)
    }
}

fn lex_cmp(a: &CostVector, b: &CostVector) -> Ordering {
    for &ax in &Axis::ALL {
        match a.get(ax).partial_cmp(&b.get(ax)).unwrap_or(Ordering::Equal) {
            Ordering::Equal => continue,
            ord => return ord,
        }
    }
    Ordering::Equal
}

/// Retain only paths whose cost is not strictly dominated by another path's cost.
/// Record-on-pop can leave a transiently-recorded path in the front if its
/// dominator arrived later, so this final pass makes the front non-dominated by
/// construction — independent of pop order or heuristic admissibility.
fn pareto_filter(front: Vec<ParetoPath>) -> Vec<ParetoPath> {
    let costs: Vec<CostVector> = front.iter().map(|p| p.cost).collect();
    front
        .into_iter()
        .enumerate()
        .filter(|(i, p)| {
            !costs
                .iter()
                .enumerate()
                .any(|(j, c)| j != *i && c.dominates(&p.cost))
        })
        .map(|(_, p)| p)
        .collect()
}

impl Graph {
    #[cfg(test)]
    pub(super) fn multiobj_search_uniform(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
    ) -> MultiObjResult {
        let weights = self.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let bike = BikeCost::new(self.raptor.bike_profile);
        self.multiobj_search(
            origin,
            destination,
            mode,
            LegRole::Neutral,
            &bike,
            &weights,
            &eps,
            f64::INFINITY,
            false,
        )
    }

    /// Build per-axis admissible heuristics toward `destination`. For each axis in
    /// `mode.axes()`, run a backward Dijkstra (over reversed mode-usable edges) that
    /// minimizes the SUM of that axis' per-edge contribution, with the turn penalty
    /// disabled (`incoming = None`) so the Variance axis is underestimated. Edge weights are
    /// scaled to integer bits by truncation (`(value*1000.0) as u64`), so descaling
    /// floors the bound ⇒ admissible. Unreachable nodes keep a 0.0 bound (still a
    /// valid lower bound; never INFINITY, which would corrupt `added`).
    #[cfg(test)]
    fn build_heuristics(
        &self,
        destination: NodeID,
        mode: RoutingMode,
        profile: &crate::structures::BikeProfile,
        weights: &CostWeights,
        speed: f64,
    ) -> Heuristics {
        let n = self.nodes.len();
        let mut per_node = vec![CostVector::ZERO; n];

        let mut rev: Vec<Vec<(NodeID, &crate::structures::StreetEdgeData)>> = vec![Vec::new(); n];
        for u in 0..n {
            let uid = NodeID(u);
            let Some(neighbors) = self.edges.get(u) else {
                continue;
            };
            for edge in neighbors {
                let EdgeData::Street(street) = edge else {
                    continue;
                };
                let this_dir = self.dir_between(uid, street.destination);
                if edge_cost_vector(
                    mode,
                    street,
                    profile,
                    weights,
                    &self.raptor.variance_model,
                    speed,
                    None,
                    this_dir,
                )
                .is_some()
                {
                    rev[street.destination.0].push((uid, street));
                }
            }
        }

        for &axis in mode.axes() {
            let mut dist = vec![u64::MAX; n];
            dist[destination.0] = 0;
            let mut heap: BinaryHeap<std::cmp::Reverse<(u64, NodeID)>> = BinaryHeap::new();
            heap.push(std::cmp::Reverse((0, destination)));
            while let Some(std::cmp::Reverse((d, node))) = heap.pop() {
                if d > dist[node.0] {
                    continue;
                }
                if node != destination && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                    continue;
                }
                for &(pred, street) in &rev[node.0] {
                    let this_dir = self.dir_between(pred, street.destination);
                    let Some(cv) = edge_cost_vector(
                        mode,
                        street,
                        profile,
                        weights,
                        &self.raptor.variance_model,
                        speed,
                        None,
                        this_dir,
                    ) else {
                        continue;
                    };
                    let w = (cv.get(axis) * 1000.0) as u64;
                    let nd = d.saturating_add(w);
                    if nd < dist[pred.0] {
                        dist[pred.0] = nd;
                        heap.push(std::cmp::Reverse((nd, pred)));
                    }
                }
            }
            for u in 0..n {
                if dist[u] != u64::MAX {
                    per_node[u].set(axis, dist[u] as f64 / 1000.0);
                }
            }
        }

        Heuristics { per_node }
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn multiobj_search_informed(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
        weights: &CostWeights,
        eps: &Epsilon,
        distance_budget: f64,
    ) -> MultiObjResult {
        let speed = self.mode_speed(mode);
        let profile = bike.profile();
        let h = self.build_heuristics(destination, mode, &profile, weights, speed);
        self.multiobj_search_core(
            origin,
            destination,
            mode,
            role,
            bike,
            weights,
            eps,
            distance_budget,
            Some(&h),
            false,
        )
    }

    /// Mode-parametrized multi-objective search. Returns the ε-Pareto front of
    /// paths from `origin` to `destination`. `distance_budget` δ is the RCSP
    /// detour factor: only paths whose length ≤ (1+δ)·shortest are explored.
    /// Pass `f64::INFINITY` to disable the budget (identical to prior behavior).
    #[allow(clippy::too_many_arguments)]
    pub fn multiobj_search(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
        weights: &CostWeights,
        eps: &Epsilon,
        distance_budget: f64,
        astar: bool,
    ) -> MultiObjResult {
        self.multiobj_search_core(
            origin,
            destination,
            mode,
            role,
            bike,
            weights,
            eps,
            distance_budget,
            None,
            astar,
        )
    }

    /// Core label-setting loop. `heuristic = None` is the uninformed search (the
    /// public default, bit-identical to prior behavior). `Some(h)` keys the heap by
    /// `f = g.added(&h(node))`; dominance, `try_add`, and the stale-check stay on `g`,
    /// so the Pareto front is invariant to the heuristic (it only reorders pops).
    #[allow(clippy::too_many_arguments)]
    fn multiobj_search_core(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
        weights: &CostWeights,
        eps: &Epsilon,
        distance_budget: f64,
        heuristic: Option<&Heuristics>,
        astar: bool,
    ) -> MultiObjResult {
        let _ = role;
        // A* Time heuristic helps Walk/Bike but is pathological for Drive (label churn on
        // the un-prunable Variance axis; slower and collapses the front), so force it off.
        let astar = astar && mode != RoutingMode::Drive;
        let front_axes = mode.effective_front_axes(self.raptor.bike_select_dplus);
        let speed = self.mode_speed(mode);
        let profile = bike.profile();
        let cv = self.raptor.systematic_cv;

        // RCSP distance budget without an O(graph) precompute. `cap` is the maximum
        // admissible accumulated length, set lazily from the first (lexicographically-
        // smallest, ≈ shortest) destination arrival; a label whose own length exceeds
        // it is pruned. No reverse-adjacency build, no per-node lower-bound table —
        // the prior O(nodes+edges) precompute made every call scale with the whole
        // graph, which is fatal when a search runs per access/egress or direct leg
        // over a country-sized network. `INFINITY` budget disables the cap entirely.
        let budget_active = distance_budget.is_finite();
        let mut cap: Option<u64> = None;

        let mut labels: Vec<Label> = Vec::new();
        // Sparse per-node label frontier: only nodes actually reached get an entry,
        // so the search costs O(explored), not O(graph). A dense Vec sized to every
        // node makes each call O(nodes) just to allocate — fine on a small graph,
        // catastrophic when the search runs repeatedly over a country-sized graph
        // (e.g. per access/egress leg during plan reconstruction).
        let mut sets: HashMap<usize, LabelSet> = HashMap::new();
        let mut heap: BinaryHeap<QLabel> = BinaryHeap::new();

        labels.push(Label {
            node: origin,
            cost: CostVector::ZERO,
            elev: (0.0, 0.0),
            parent: None,
            len: 0,
            var_accum: 0.0,
            first_step: origin,
            arrive_dir: (0.0, 0.0),
            arrive_len: 0.0,
            arrive_cruise: 0.0,
            arrive_push: false,
            arrive_speed: 0.0,
        });
        sets.entry(origin.0)
            .or_default()
            .try_add(CostVector::ZERO, eps, &Buckets::NONE);
        // On-the-fly degree-2 contraction: skip creating labels at forced single-successor
        // shape vertices, following the chain to the next junction (replayed from the arena).
        // All modes contract on the union cg (`g.contracted`). A direct/access street leg is
        // single-phase, so drive replays the same per-segment cost as walk. `bike_cg()` returns
        // the cg the search reads.
        let contract = self.contracted.is_some();
        // Cost-baked super-edges available ⇒ the front paths' demoted axes (D+/Surface/
        // Variance) are canonical during search and must be recomputed exactly at the end.
        // Baking is bike-only; walk/drive replay the per-segment cost exactly in-search.
        let baked_mode = contract && mode == RoutingMode::Bike && self.bike_cg().is_some();
        // Junctions bounding an interior destination's chain — re-walk (not bake) there.
        let dest_guard = if baked_mode {
            self.dest_guard_junctions(destination)
        } else {
            Vec::new()
        };
        let dest_loc = self.node_loc(destination);
        // Distance-adaptive grid bucketing on each mode's diversity axes. Cell size ∝
        // origin→dest straight-line distance, so the per-node frontier stays bounded
        // regardless of route length while the relevant trade-off span (which the
        // user routes on) is preserved cell-by-cell. Bike buckets CyclewayDeficit/
        // Dplus; Drive buckets Variance; Walk buckets Surface — the axes measured to
        // grow combinatorially with distance on each mode's un-bucketed front. A
        // coefficient of 0 disables bucketing on that axis (strict no-op ⇒ unchanged
        // behavior).
        let buckets = {
            let mut sizes = [0.0; crate::structures::cost::AXIS_COUNT];
            let mut active = false;
            match mode {
                RoutingMode::Bike => {
                    let kc = self.raptor.bike_bucket_cyc_k;
                    // Drop the Dplus bucket dimension when D+ is demoted from the
                    // front axes (it is no longer a selection/diversity axis).
                    let kd = if front_axes.contains(&Axis::Dplus) {
                        self.raptor.bike_bucket_dpl_k
                    } else {
                        0.0
                    };
                    if kc > 0.0 || kd > 0.0 {
                        let d = self.node_loc(origin).dist(dest_loc);
                        if kc > 0.0 {
                            sizes[Axis::CyclewayDeficit.index()] = kc * d;
                            active = true;
                        }
                        if kd > 0.0 {
                            sizes[Axis::Dplus.index()] = kd * d;
                            active = true;
                        }
                    }
                }
                RoutingMode::Drive => {
                    let kv = self.raptor.drive_bucket_var_k;
                    if kv > 0.0 {
                        let d = self.node_loc(origin).dist(dest_loc);
                        sizes[Axis::Variance.index()] = kv * d;
                        active = true;
                    }
                }
                RoutingMode::Walk => {
                    let ks = self.raptor.walk_bucket_surf_k;
                    if ks > 0.0 {
                        let d = self.node_loc(origin).dist(dest_loc);
                        sizes[Axis::Surface.index()] = ks * d;
                        active = true;
                    }
                }
            }
            if active { Buckets { sizes } } else { Buckets::NONE }
        };
        // A* lower bound on remaining Time = straight-line distance / the FASTEST
        // speed the cost model can produce, so it can never exceed the true remaining
        // time (admissible ⇒ the Pareto front is unchanged). Bike time is kinematic
        // and capped at `profile.max_speed`, so that cap — not the configured cruising
        // speed — is the ceiling. (Per-edge times are integer-rounded by ≤1 s; that
        // slack sits below the ε Time floor, so it never moves the ε-front.)
        let max_speed = match mode {
            RoutingMode::Walk => self.raptor.walking_speed_mps,
            RoutingMode::Bike => bike.profile().max_speed / 3.6,
            RoutingMode::Drive => self.raptor.driving_speed_mps,
        };
        let inv_max_speed = 1.0 / max_speed.max(0.1);
        let f_key = |g: &CostVector, node: NodeID| {
            if astar {
                let mut h = CostVector::ZERO;
                h.set(
                    Axis::Time,
                    self.node_loc(node).dist(dest_loc) * inv_max_speed,
                );
                g.added(&h)
            } else if let Some(h) = heuristic {
                g.added(&h.h(node))
            } else {
                *g
            }
        };
        heap.push(QLabel {
            key: f_key(&CostVector::ZERO, origin),
            idx: 0,
        });

        let mut front: Vec<ParetoPath> = Vec::new();
        // Costs of completed destination paths. A label whose admissible lower
        // bound on its own destination cost is weakly dominated by one of these
        // can never contribute a new non-dominated path, so it is pruned. Edge
        // costs are non-negative, so a label's `f` key (`g`, or `g + h` when
        // informed) is a valid lower bound — making this pruning exact: the front
        // is unchanged, only the explored region shrinks.
        let mut dest_front: Vec<CostVector> = Vec::new();
        #[cfg(test)]
        let mut expand_count: u64 = 0;
        while let Some(QLabel { key, idx }) = heap.pop() {
            #[cfg(test)]
            {
                expand_count += 1;
            }
            let node = labels[idx].node;
            let g_cost = labels[idx].cost;
            let elev = labels[idx].elev;
            let cur_len = labels[idx].len;

            if !sets
                .get(&node.0)
                .is_some_and(|s| s.contains(&g_cost.project(front_axes)))
            {
                continue;
            }
            // Distance-budget cap (lazy): a label may have been enqueued before the
            // cap was set on the first destination arrival, so re-check at pop. The
            // corridor form (len so far + straight-line remainder to dest) catches
            // labels that can no longer complete within budget — see the enqueue site.
            if let Some(cap_val) = cap {
                let d_remain = self.node_loc(node).dist(dest_loc);
                if cur_len as f64 + d_remain > cap_val as f64 {
                    continue;
                }
            }
            // Target pruning: drop labels already covered by the destination front.
            if dest_front.iter().any(|d| d.weakly_dominates(&key)) {
                continue;
            }
            if node == destination {
                if budget_active && cap.is_none() {
                    cap = Some(((1.0 + distance_budget) * cur_len as f64) as u64);
                }
                dest_front.retain(|d| !g_cost.project(front_axes).weakly_dominates(d));
                dest_front.push(g_cost.project(front_axes));
                // Flush the walk ascent buffer at the destination: the residual ehbu
                // (sustained climb not yet charged because it never exceeded the
                // buffer) is real ascent, so add it — D+ then reflects the true net
                // gain instead of undercounting small climbs by up to the buffer.
                let (nodes, edges) = self.expand_path(&labels, idx, contract);
                // Under cost-baking the label's demoted axes (D+/Surface/Variance) are
                // canonical; recompute the full exact cost over the reconstructed path so
                // the reported front matches the un-contracted search bit-for-bit. Replays
                // the arena edges `expand_path` carried — g-free.
                let mut rec_cost = if baked_mode {
                    self.replay_path_exact(&edges, mode, bike, weights, &profile, speed, cv)
                } else {
                    g_cost
                };
                if mode == RoutingMode::Walk {
                    rec_cost.set(Axis::Dplus, rec_cost.get(Axis::Dplus) + elev.1);
                }
                front.push(ParetoPath {
                    nodes,
                    edges,
                    cost: rec_cost,
                    elev_buffer: elev,
                });
                continue;
            }
            if node != origin && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            // Previous-edge context = the actual last edge arriving at `node` (carried on
            // the label, since under contraction the parent is several nodes back). Feeds
            // the turn-variance term AND the per-vertex speed-change (corner/stop) cost.
            let prev_ctx = labels[idx].parent.map(|_| PrevCtx {
                dir: labels[idx].arrive_dir,
                len: labels[idx].arrive_len,
                cruise: labels[idx].arrive_cruise,
                push: labels[idx].arrive_push,
                speed: labels[idx].arrive_speed,
            });
            let guard_junction = !dest_guard.is_empty() && dest_guard.contains(&node);
            // Neighbour list as `(first edge, first-step coord, Option<super-edge>)`. Under
            // contraction a **junction** expands its arena super-edges directly (g-free —
            // edge + coord from `segs`); an interior node (e.g. a snapped origin) still
            // falls back to `g.edges`. Off contraction this is exactly the old g.edges scan.
            let cgref = self.bike_cg();
            let arena_ses = if contract {
                cgref.and_then(|cg| {
                    let ji = cg.junction_of[node.0];
                    (ji != u32::MAX).then(|| cg.adjacency[ji as usize].as_slice())
                })
            } else {
                None
            };
            let neigh: Vec<(&StreetEdgeData, LatLng, Option<&SuperEdge>)> = match (arena_ses, cgref) {
                (Some(ses), Some(cg)) => ses
                    .iter()
                    .map(|se| {
                        let s0 = &cg.segs[se.seg_start as usize];
                        (&s0.edge, s0.far, Some(se))
                    })
                    .collect(),
                _ => {
                    let Some(neighbors) = self.edges.get(node.0) else {
                        continue;
                    };
                    neighbors
                        .iter()
                        .filter_map(|e| match e {
                            EdgeData::Street(s) => {
                                Some((s, self.nodes[s.destination.0].loc(), None::<&SuperEdge>))
                            }
                            _ => None,
                        })
                        .collect()
                }
            };
            // `node`'s own coordinate, arena-sourced when it's a contracted junction.
            let node_loc = match cgref {
                Some(cg) if contract && cg.junction_of[node.0] != u32::MAX => {
                    cg.junction_coord[cg.junction_of[node.0] as usize]
                }
                _ => self.nodes[node.0].loc(),
            };
            for (street, first_step_loc, se_direct) in neigh {
                let first_step = street.destination;
                // First edge of the (possibly contracted) super-edge.
                let new_len0 = cur_len.saturating_add(street.length as u32);
                // Geometric length corridor (ellipse, foci origin/dest, major axis =
                // cap): a path through a node needs at least the straight-line remainder
                // to reach dest, so `len + d_remain > cap` can never complete within
                // budget. This cuts the lateral fan-out that multi-objective target
                // pruning structurally cannot (an off-axis label is behind on Time but
                // ahead on the accumulation axes Cyc/Dplus, so no dest path dominates
                // it). Reuses the haversine the A* Time heuristic needs. Admissibility
                // note: edge lengths are *floored* haversines (pbf.rs), so the stored
                // remaining length can sit up to ~1 m/edge below the exact straight
                // line — the corridor is thus exact only to that sub-metre-per-edge
                // truncation, far below the ε floor and the 15% budget slack, so in
                // practice it does not move the ε-approximate front. Checked at the
                // first step (here, cheap early-skip) and again at the terminal node.
                if let Some(cap_val) = cap {
                    let dr0 = first_step_loc.dist(dest_loc);
                    if new_len0 as f64 + dr0 > cap_val as f64 {
                        continue;
                    }
                }
                // A cost-baked super-edge (≥2 segments) adds its precomputed cost in O(1)
                // and jumps to the far junction — UNLESS this junction bounds the chain
                // containing the destination (then we must re-walk to stop at the interior
                // destination). Front axes (Time, Cyc) are exact; demoted axes are
                // canonical and recomputed for the final paths.
                let se_opt = if se_direct.is_some() {
                    se_direct
                } else if contract {
                    cgref.and_then(|cg| cg.super_edge(node, first_step))
                } else {
                    None
                };
                // In baked-mode (bike) an un-bakeable ≥2-segment super-edge contains an
                // impassable segment, so the chain is a dead-end (its interior nodes have no
                // other exit) — skip it. (At a guard junction we still re-walk, to reach an
                // interior destination that may lie before the block.) Walk/Drive never bake,
                // so this skip does not apply — they always seg-replay through the chain.
                if baked_mode
                    && !guard_junction
                    && se_opt.is_some_and(|se| se.baked.is_none() && se.nodes.len() >= 2)
                {
                    continue;
                }
                let baked = if baked_mode && !guard_junction {
                    se_opt.and_then(|se| se.baked.as_ref().map(|bk| (se.to, bk)))
                } else {
                    None
                };
                let (t_cost, t_elev, t_var, t_len, t_node, t_arrive, t_ctx, t_node_loc) =
                    if let Some((to, bk)) = baked {
                        let (delta, exit) = bk.traverse(prev_ctx, bike);
                        let cg = self.bike_cg().unwrap();
                        let tn = cg.junctions[to as usize];
                        let tn_loc = cg.junction_coord[to as usize];
                        // elev / var carried unchanged: they feed only the demoted
                        // D+/Variance axes (recomputed exactly for the final paths).
                        (
                            g_cost.added(&delta),
                            elev,
                            labels[idx].var_accum,
                            cur_len.saturating_add(bk.length as u32),
                            tn,
                            exit.dir,
                            exit,
                            tn_loc,
                        )
                    } else {
                        // `dir_coords(node_loc, first_step_loc)` == `dir_between(node,
                        // first_step)` (same formula, same coords) but arena-sourced — pass it
                        // in so the transition is g-free (no internal `dir_between`).
                        let first_dir =
                            super::contraction::dir_coords(node_loc, first_step_loc);
                        let Some((mut t_cost, mut t_elev, mut t_var)) = self
                            .street_edge_transition_dir(
                                mode, street, Some(first_dir), &profile, weights, speed, cv, bike,
                                prev_ctx, &g_cost, elev, labels[idx].var_accum,
                            )
                        else {
                            continue;
                        };
                        let mut t_len = new_len0;
                        let mut t_node = first_step;
                        let mut t_arrive = first_dir;
                        let mut t_ctx = self.arrival_ctx(bike, prev_ctx, street, t_arrive);
                        let mut t_node_loc = first_step_loc;
                        // Re-walk the degree-2 chain (un-baked single-edge, or a guard
                        // junction): same per-segment cost, stopping at an interior dest.
                        if let (true, Some(se), Some(cg)) = (contract, se_opt, cgref) {
                            // Arena seg-replay (g-free): the super-edge's segments after the
                            // first, threading direction from consecutive far-coords.
                            let mut prev_far = first_step_loc;
                            let lo = se.seg_start as usize + 1;
                            let hi = (se.seg_start + se.seg_len) as usize;
                            for seg in &cg.segs[lo..hi] {
                                if t_node == destination
                                    || self.raptor.transit_node_to_stop[t_node.0] != u32::MAX
                                    || cg.junction_of[t_node.0] != u32::MAX
                                {
                                    break;
                                }
                                let dir = super::contraction::dir_coords(prev_far, seg.far);
                                let Some((c2, e2, v2)) = self.street_edge_transition_dir(
                                    mode, &seg.edge, Some(dir), &profile, weights, speed, cv, bike,
                                    Some(t_ctx), &t_cost, t_elev, t_var,
                                ) else {
                                    break;
                                };
                                t_cost = c2;
                                t_elev = e2;
                                t_var = v2;
                                t_len = t_len.saturating_add(seg.edge.length as u32);
                                t_arrive = dir;
                                t_ctx = self.arrival_ctx(bike, Some(t_ctx), &seg.edge, dir);
                                t_node = seg.edge.destination;
                                t_node_loc = seg.far;
                                prev_far = seg.far;
                            }
                        } else if contract {
                            // g-fallback re-walk (no known super-edge, e.g. an interior
                            // snapped origin). Anchors on `junction_of` like the arena path.
                            let junc_of = self.bike_cg().map(|cg| &cg.junction_of);
                            let mut prev = node;
                            let mut cur = first_step;
                            let mut guard = 0u32;
                            loop {
                                if cur == destination
                                    || self.raptor.transit_node_to_stop[cur.0] != u32::MAX
                                    || junc_of.is_some_and(|j| j[cur.0] != u32::MAX)
                                {
                                    break;
                                }
                                let Some((next, nstreet)) = self.bike_chain_next(prev, cur) else {
                                    break;
                                };
                                let Some((c2, e2, v2)) = self.street_edge_transition(
                                    mode, nstreet, &profile, weights, speed, cv, bike, Some(t_ctx),
                                    &t_cost, t_elev, t_var,
                                ) else {
                                    break;
                                };
                                t_cost = c2;
                                t_elev = e2;
                                t_var = v2;
                                t_len = t_len.saturating_add(nstreet.length as u32);
                                t_arrive = self.dir_between(cur, next);
                                t_ctx = self.arrival_ctx(bike, Some(t_ctx), nstreet, t_arrive);
                                prev = cur;
                                cur = next;
                                t_node = next;
                                t_node_loc = self.nodes[next.0].loc();
                                guard += 1;
                                if guard > 1_000_000 {
                                    break;
                                }
                            }
                        }
                        (t_cost, t_elev, t_var, t_len, t_node, t_arrive, t_ctx, t_node_loc)
                    };
                // A replay that stopped at an interior node (not a junction, the
                // destination, or a stop) hit a segment impassable for this mode mid-chain
                // — a dead-end (its interior nodes have no other exit). Don't seed a label
                // there: it can never reach the dest, would re-walk the whole chain again
                // (O(chain²) blow-up), and — post node-array drop — its interior id has no
                // `junction_coord`, so `node_loc` would panic. Applies to every contracted
                // mode (walk/drive seg-replay can dead-end on a foot/car-impassable segment
                // just as a baked bike chain can).
                if contract
                    && t_node != destination
                    && self.raptor.transit_node_to_stop[t_node.0] == u32::MAX
                    && self
                        .bike_cg()
                        .is_some_and(|cg| cg.junction_of[t_node.0] == u32::MAX)
                {
                    continue;
                }
                // Corridor + target pruning + admission at the terminal node.
                let d_remain = t_node_loc.dist(dest_loc);
                if let Some(cap_val) = cap {
                    if t_len as f64 + d_remain > cap_val as f64 {
                        continue;
                    }
                }
                let new_key = if astar {
                    let mut h = CostVector::ZERO;
                    h.set(Axis::Time, d_remain * inv_max_speed);
                    t_cost.added(&h)
                } else if let Some(hh) = heuristic {
                    t_cost.added(&hh.h(t_node))
                } else {
                    t_cost
                };
                if dest_front.iter().any(|d| d.weakly_dominates(&new_key)) {
                    continue;
                }
                if !sets
                    .entry(t_node.0)
                    .or_default()
                    .try_add(t_cost.project(front_axes), eps, &buckets)
                {
                    continue;
                }
                let nidx = labels.len();
                labels.push(Label {
                    node: t_node,
                    cost: t_cost,
                    elev: t_elev,
                    parent: Some(idx),
                    len: t_len,
                    var_accum: t_var,
                    first_step,
                    arrive_dir: t_arrive,
                    arrive_len: t_ctx.len,
                    arrive_cruise: t_ctx.cruise,
                    arrive_push: t_ctx.push,
                    arrive_speed: t_ctx.speed,
                });
                heap.push(QLabel {
                    key: new_key,
                    idx: nidx,
                });
            }
        }

        MultiObjResult {
            front: pareto_filter(front),
            #[cfg(test)]
            expansions: expand_count,
            #[cfg(test)]
            total_labels: labels.len(),
            #[cfg(test)]
            max_labels_per_node: sets.values().map(|s| s.len()).max().unwrap_or(0),
            #[cfg(test)]
            nodes_explored: sets.len(),
        }
    }

    /// If `cur` is a degree-2 bike pass-through — exactly two *distinct* bikeable
    /// street neighbours — return the one that continues the chain away from `prev`,
    /// with a bikeable edge to it. Returns `None` at junctions (≥3 neighbours),
    /// dead-ends (<2), or if `prev` isn't a neighbour. Used to contract forced
    /// single-successor chains so the search creates a label only at the next
    /// junction, not at every shape vertex. No allocation (two fixed slots).
    fn bike_chain_next(&self, prev: NodeID, cur: NodeID) -> Option<(NodeID, &StreetEdgeData)> {
        let neigh = self.edges.get(cur.0)?;
        let mut n1: Option<NodeID> = None;
        let mut n2: Option<NodeID> = None;
        for e in neigh {
            let EdgeData::Street(s) = e else { continue };
            if !s.bike {
                continue;
            }
            let d = s.destination;
            if n1 == Some(d) || n2 == Some(d) {
                continue;
            }
            if n1.is_none() {
                n1 = Some(d);
            } else if n2.is_none() {
                n2 = Some(d);
            } else {
                return None; // a third distinct neighbour ⇒ junction
            }
        }
        let (n1, n2) = (n1?, n2?);
        let next = if n1 == prev {
            n2
        } else if n2 == prev {
            n1
        } else {
            return None;
        };
        let edge = neigh.iter().find_map(|e| match e {
            EdgeData::Street(s) if s.bike && s.destination == next => Some(s),
            _ => None,
        })?;
        Some((next, edge))
    }

    /// Reconstruct the full node path (including the shape vertices skipped by
    /// degree-2 contraction) for the label at `idx`. Walks the parent chain of
    /// junction labels, and for each hop re-walks the unique degree-2 chain from the
    /// parent junction via the stored `first_step` to the label's node. With
    /// `contract == false` every hop is a single edge (`first_step == node`), so this
    /// degenerates to the plain junction-to-junction backtrack.
    fn expand_path(
        &self,
        labels: &[Label],
        idx: usize,
        contract: bool,
    ) -> (Vec<NodeID>, Vec<(StreetEdgeData, (f64, f64), LatLng)>) {
        let mut chain = vec![idx];
        let mut i = idx;
        while let Some(p) = labels[i].parent {
            chain.push(p);
            i = p;
        }
        chain.reverse(); // origin … destination
        let cg = self.bike_cg();
        // `out` is the node path; `incoming[n]` is the (edge, dir) reaching `n` on the
        // walk — recorded on first visit so it survives `strip_cycles` (which keeps the
        // first occurrence). Lets the exact replay run from arena edges, g-free.
        let mut out = vec![labels[chain[0]].node];
        let mut incoming: HashMap<NodeID, (StreetEdgeData, (f64, f64), LatLng)> = HashMap::new();
        // `pj`'s coordinate, arena-sourced when it is a junction.
        let loc_of = |n: NodeID| match cg {
            Some(c) if c.junction_of[n.0] != u32::MAX => {
                c.junction_coord[c.junction_of[n.0] as usize]
            }
            _ => self.nodes[n.0].loc(),
        };
        for w in chain.windows(2) {
            let pj = labels[w[0]].node; // parent junction
            let nj = labels[w[1]].node; // this label's node
            let fs = labels[w[1]].first_step;
            if !contract {
                out.push(nj); // off-contract: replay not called ⇒ edges unused
                continue;
            }
            if fs == nj {
                // Single-edge super-edge pj→nj; edge + dir from the arena (g fallback).
                let e = match cg.and_then(|c| c.super_edge(pj, fs)) {
                    Some(se) => {
                        let s0 = cg.unwrap().segs[se.seg_start as usize];
                        Some((s0.edge, super::contraction::dir_coords(loc_of(pj), s0.far), s0.far))
                    }
                    None => super::contraction::ContractedGraph::bike_edge(self, pj, nj)
                        .map(|edge| (*edge, self.dir_between(pj, nj), self.nodes[nj.0].loc())),
                };
                if let Some(e) = e {
                    incoming.entry(nj).or_insert(e);
                }
                out.push(nj);
                continue;
            }
            // Expand the super-edge pj → fs → … → nj from the arena (its node chain is
            // exactly what the search followed), stopping at nj (the far junction or an
            // interior dest). g-free when pj is a junction.
            if let (Some(c), Some(se)) = (cg, cg.and_then(|c| c.super_edge(pj, fs))) {
                let mut prev_far = loc_of(pj);
                for k in 0..se.nodes.len() {
                    let n = se.nodes[k];
                    let seg = c.segs[se.seg_start as usize + k];
                    let dir = super::contraction::dir_coords(prev_far, seg.far);
                    incoming.entry(n).or_insert((seg.edge, dir, seg.far));
                    out.push(n);
                    prev_far = seg.far;
                    if n == nj {
                        break;
                    }
                }
                continue;
            }
            // Fallback (pj not a junction, e.g. a snapped interior origin): re-walk the
            // degree-2 chain on the full graph.
            if let Some(edge) = super::contraction::ContractedGraph::bike_edge(self, pj, fs) {
                incoming
                    .entry(fs)
                    .or_insert((*edge, self.dir_between(pj, fs), self.nodes[fs.0].loc()));
            }
            out.push(fs);
            let mut prev = pj;
            let mut cur = fs;
            let mut guard = 0u32;
            while cur != nj {
                let Some((next, street)) = self.bike_chain_next(prev, cur) else {
                    break;
                };
                incoming
                    .entry(next)
                    .or_insert((*street, self.dir_between(cur, next), self.nodes[next.0].loc()));
                out.push(next);
                prev = cur;
                cur = next;
                guard += 1;
                if guard > 100_000 {
                    break;
                }
            }
        }
        let nodes = Self::strip_cycles(out);
        // Edges aligned to the (cycle-stripped) node path: each kept node's first-visit
        // incoming edge, which — because `strip_cycles` keeps the first occurrence —
        // correctly connects it to its predecessor in the simplified path.
        let edges: Vec<(StreetEdgeData, (f64, f64), LatLng)> = nodes
            .windows(2)
            .filter_map(|w| incoming.get(&w[1]).copied())
            .collect();
        (nodes, edges)
    }

    /// Remove node revisits from a reconstructed walk, leaving a simple path. A label
    /// whose parent chain re-enters a node survives ε-dominance/bucketing without being
    /// pruned; splicing the loop is always cost-non-increasing (additive non-negative
    /// edge costs) and keeps the path connected (the kept occurrence is adjacent to
    /// whatever followed the later one).
    fn strip_cycles(walk: Vec<NodeID>) -> Vec<NodeID> {
        let mut out: Vec<NodeID> = Vec::with_capacity(walk.len());
        let mut pos: std::collections::HashMap<NodeID, usize> = std::collections::HashMap::new();
        for n in walk {
            if let Some(&i) = pos.get(&n) {
                for k in (i + 1)..out.len() {
                    pos.remove(&out[k]);
                }
                out.truncate(i + 1);
            } else {
                pos.insert(n, out.len());
                out.push(n);
            }
        }
        out
    }

    /// Exact full cost of a reconstructed bike node path, replayed from a null entry via
    /// the same `street_edge_transition` the search uses. Used to recompute the demoted
    /// axes for the few final front paths under cost-baking (where the search carried
    /// them only canonically).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn replay_path_exact(
        &self,
        edges: &[(StreetEdgeData, (f64, f64), LatLng)],
        mode: RoutingMode,
        bike: &BikeCost,
        weights: &CostWeights,
        profile: &crate::structures::BikeProfile,
        speed: f64,
        cv: f64,
    ) -> CostVector {
        let (mut cost, mut elev, mut var) = (CostVector::ZERO, (0.0, 0.0), 0.0);
        let mut prev: Option<PrevCtx> = None;
        for (edge, dir, _far) in edges {
            if let Some((c, e, v)) = self.street_edge_transition_dir(
                mode, edge, Some(*dir), profile, weights, speed, cv, bike, prev, &cost, elev, var,
            ) {
                cost = c;
                elev = e;
                var = v;
                prev = Some(self.arrival_ctx(bike, prev, edge, *dir));
            }
        }
        cost
    }

    /// Per-edge cost + carried-state transition, shared by the Pareto search and the
    /// scalar representative search so the ride/push + elevation-hysteresis logic
    /// lives in exactly one place. Given the accumulated `g_cost`/`elev`/`var_accum`
    /// at `street.origin`, returns the extended `(cost, elev_buffer, var_accum)` at
    /// `street.destination`, or `None` if the edge is impassable in `mode`.
    /// Build the `PrevCtx` describing an edge just traversed in direction `dir`: its
    /// length, push-state, cruise speed, and the carried (exit) speed the bike leaves it
    /// at. The exit speed is the edge's `required_speed` given the context it was entered
    /// from (`prev`) — the bend's safe speed on a curve, `0` on a push, cruise on a
    /// straight — so the next vertex charges only the change, not a full slow-down.
    fn arrival_ctx(
        &self,
        bike: &BikeCost,
        prev: Option<PrevCtx>,
        street: &StreetEdgeData,
        dir: (f64, f64),
    ) -> PrevCtx {
        let push = BikeCost::is_push(&street.attrs);
        PrevCtx {
            dir,
            len: street.length as f64,
            cruise: if push { 0.0 } else { bike.cruise_speed(street) },
            push,
            speed: bike.required_speed(prev, street, dir),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn street_edge_transition(
        &self,
        mode: RoutingMode,
        street: &StreetEdgeData,
        profile: &BikeProfile,
        weights: &CostWeights,
        speed: f64,
        cv: f64,
        bike: &BikeCost,
        prev: Option<PrevCtx>,
        g_cost: &CostVector,
        elev: (f64, f64),
        var_accum: f64,
    ) -> Option<(CostVector, (f64, f64), f64)> {
        self.street_edge_transition_dir(
            mode, street, None, profile, weights, speed, cv, bike, prev, g_cost, elev, var_accum,
        )
    }

    /// As [`street_edge_transition`], with the edge direction supplied (g-free). `dir
    /// = None` recomputes it from the edge endpoints via `dir_between` (reads `g`,
    /// byte-identical to before); `Some(d)` uses the carried arena direction.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn street_edge_transition_dir(
        &self,
        mode: RoutingMode,
        street: &StreetEdgeData,
        dir: Option<(f64, f64)>,
        profile: &BikeProfile,
        weights: &CostWeights,
        speed: f64,
        cv: f64,
        bike: &BikeCost,
        prev: Option<PrevCtx>,
        g_cost: &CostVector,
        elev: (f64, f64),
        var_accum: f64,
    ) -> Option<(CostVector, (f64, f64), f64)> {
        #[cfg(test)]
        TRANS_N.with(|c| c.set(c.get() + 1));
        let this_dir = dir.unwrap_or_else(|| self.dir_between(street.origin, street.destination));
        let incoming = prev.map(|p| p.dir);
        let mut edge_cv = edge_cost_vector(
            mode,
            street,
            profile,
            weights,
            &self.raptor.variance_model,
            speed,
            incoming,
            this_dir,
        )?;
        if mode == RoutingMode::Bike {
            // Physically-grounded speed-change time: the corner slow-down (decel to the
            // bend's safe speed + re-accel) and the dismount stop / remount restart, all
            // charged at the boundary into this edge. Needs the previous edge's length
            // and cruise speed, which is why it lives here (the fold) and not per-edge.
            let extra = bike.speed_change_secs(prev, street, this_dir);
            edge_cv.set(Axis::Time, edge_cv.get(Axis::Time) + extra);
            // Dismount uncertainty: a once-per-run variance bump at the ride→push
            // boundary (the same boundary the stop is charged on), so a route that
            // dismounts is honestly shown as less predictable.
            if let Some(p) = prev {
                if !p.push && BikeCost::is_push(&street.attrs) {
                    let ps = self.raptor.variance_model.push_sigma;
                    edge_cv.set(Axis::Variance, edge_cv.get(Axis::Variance) + ps * ps);
                }
            }
        }
        let new_elev = if mode == RoutingMode::Bike {
            // D+ is the denoised per-edge ascent baked at ingestion (smoothed
            // `elev_delta` → `dplus(e)`); no in-search hysteresis is added. The old
            // path-coupled `elevation_step` buffer was unsound for label-setting and
            // is dropped. NOTE: it also carried BRouter's descent-SAFETY penalty
            // (`downhillcost`); removing it drops descent-avoidance from route
            // selection — a candidate follow-up (per-edge additive descent penalty).
            elev
        } else if mode == RoutingMode::Walk {
            // Denoise walk D+ with the bike's elevation hysteresis so a noisy
            // DEM can't inflate ascent on the direct path (and make a detour
            // look "flatter"). Replaces the raw per-edge max(0, Δ).
            let (asc, ehbu) =
                bike.walk_ascent_step(elev.1, street.elev_delta as f64, street.length as f64);
            edge_cv.set(Axis::Dplus, asc);
            (elev.0, ehbu)
        } else {
            elev
        };
        let edge_var = edge_cv.get(Axis::Variance);
        let new_var_accum = var_accum + edge_var;
        let mut new_cost = g_cost.added(&edge_cv);
        // The Variance slot carries the full reliability variance — additive crossing
        // variance PLUS the systematic (cv·time)² term, matching the displayed
        // [p50,p95] bracket. It is a non-decreasing function of the additive
        // (Σvar, time) pair, and Time is also a dominance axis, so dominance is
        // preserved under extension and the front stays sound.
        new_cost.set(
            Axis::Variance,
            new_var_accum + cv * cv * new_cost.get(Axis::Time) * new_cost.get(Axis::Time),
        );
        Some((new_cost, new_elev, new_var_accum))
    }

    pub(super) fn mode_speed(&self, mode: RoutingMode) -> f64 {
        match mode {
            RoutingMode::Walk => self.raptor.walking_speed_mps,
            RoutingMode::Bike => self.raptor.walking_speed_mps,
            RoutingMode::Drive => self.raptor.driving_speed_mps,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::{Axis, CostVector, Epsilon};
    use crate::structures::{Graph, NodeID, RaptorIndex};

    fn cv(time: f64, variance: f64) -> CostVector {
        CostVector::from_active(&[Axis::Time, Axis::Variance], &[time, variance])
    }

    #[test]
    fn strip_cycles_splices_revisited_nodes() {
        // A reconstructed walk that revisits a node (ε-dominance can keep a
        // node-cyclic label) must be spliced to a simple path — removing the loop
        // is always cost-non-increasing on additive non-negative costs.
        let n = |i: u32| NodeID(i as usize);
        // 1→2→3→2→4 : the 2→3→2 loop is spliced out.
        assert_eq!(
            Graph::strip_cycles(vec![n(1), n(2), n(3), n(2), n(4)]),
            vec![n(1), n(2), n(4)]
        );
        // Already simple: unchanged.
        assert_eq!(
            Graph::strip_cycles(vec![n(1), n(2), n(3)]),
            vec![n(1), n(2), n(3)]
        );
        // Nested/again-revisited origin: 1→2→3→1→2→4 collapses correctly.
        assert_eq!(
            Graph::strip_cycles(vec![n(1), n(2), n(3), n(1), n(2), n(4)]),
            vec![n(1), n(2), n(4)]
        );
    }

    #[test]
    fn pareto_filter_drops_dominated_keeps_tradeoffs_and_equals() {
        let path = |time: f64, comfort: f64| ParetoPath {
            nodes: vec![NodeID(0)],
            edges: Vec::new(),
            cost: cv(time, comfort),
            elev_buffer: (0.0, 0.0),
        };
        let filtered = pareto_filter(vec![
            path(10.0, 5.0),
            path(12.0, 7.0),
            path(8.0, 9.0),
            path(10.0, 5.0),
        ]);
        let mut costs: Vec<(u64, u64)> = filtered
            .iter()
            .map(|p| {
                (
                    p.cost.get(Axis::Time) as u64,
                    p.cost.get(Axis::Variance) as u64,
                )
            })
            .collect();
        costs.sort();
        assert_eq!(
            costs,
            vec![(8, 9), (10, 5), (10, 5)],
            "drops (12,7); keeps both trade-offs and the equal duplicate"
        );
    }

    // Documents a known lossy property of the (CyclewayDeficit, Dplus) grid bucket:
    // it can evict a label that *dominates* a worse label sitting in a different cell,
    // so the dominated label can survive in the final frontier. This bounds the
    // per-node frontier for performance at the cost of exactness; the test pins the
    // behaviour so any change to the bucket rule is a deliberate, visible decision.
    #[test]
    fn bucket_eviction_can_keep_a_dominated_label() {
        let axes = [
            Axis::Time,
            Axis::CyclewayDeficit,
            Axis::Dplus,
            Axis::Surface,
        ];
        let mk = |t: f64, cyc: f64, dpl: f64, surf: f64| {
            CostVector::from_active(&axes, &[t, cyc, dpl, surf])
        };
        // Both diversity axes (Cyc, Dplus) are bucketed, as in the live bike search.
        // z & ride share BOTH bucket cells (same Cyc and Dplus cell) and differ only on
        // the un-bucketed Surface axis: z is faster but rougher, ride slower but smooth —
        // a genuine trade-off the bucket nonetheless collapses (keeps lex-min Time = z).
        // push has a higher deficit ⇒ a different Cyc cell, smooth like ride, and is
        // tied with ride on Dplus, so ride strictly dominates push.
        let z = mk(10.0, 50.0, 50.0, 99.0);
        let ride = mk(20.0, 60.0, 55.0, 5.0);
        let push = mk(100.0, 150.0, 55.0, 5.0);
        assert!(ride.dominates(&push), "ride must dominate push by construction");
        assert!(!z.dominates(&ride), "z must not dominate ride (Surface trade-off)");
        assert!(!z.dominates(&push), "z must not dominate push (Surface trade-off)");
        let eps = Epsilon::uniform(0.0, 0.0);

        // Cells size 100 on both diversity axes ⇒ z & ride share cell (0,0); push is in
        // a different Cyc cell (deficit 150 ⇒ cell 1).
        let mut sizes = [0.0; crate::structures::cost::AXIS_COUNT];
        sizes[Axis::CyclewayDeficit.index()] = 100.0;
        sizes[Axis::Dplus.index()] = 100.0;
        let buckets = Buckets { sizes };

        let mut bucketed = LabelSet::new();
        bucketed.try_add(z, &eps, &buckets);
        bucketed.try_add(ride, &eps, &buckets); // bucket-evicted by z (same cell, z lex-smaller)
        bucketed.try_add(push, &eps, &buckets); // survives alone in cell 1
        let has_push = bucketed.contains(&push);
        let has_ride = bucketed.contains(&ride);
        assert!(
            has_push && !has_ride,
            "bucketing drops the dominating ride label and keeps the dominated push label"
        );

        // Without buckets, ride survives and weakly-dominates push ⇒ push never enters.
        let mut exact = LabelSet::new();
        exact.try_add(z, &eps, &Buckets::NONE);
        exact.try_add(ride, &eps, &Buckets::NONE);
        exact.try_add(push, &eps, &Buckets::NONE);
        assert!(
            exact.contains(&ride) && !exact.contains(&push),
            "without bucketing the dominated push label is correctly rejected"
        );
    }

    #[cfg(test)]
    fn tiny_detour_graph() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.010));
        let c = g.add_node(mk("c", 50.001, 4.005));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, surface: Surface| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = surface;
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
        (g, a, b)
    }

    #[cfg(test)]
    fn tiny_triple_graph() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.010));
        let c = g.add_node(mk("c", 50.001, 4.005));
        let d = g.add_node(mk("d", 49.999, 4.005));
        g.build_raptor_index();
        let edge = |o: NodeID, dn: NodeID, len: usize, surface: Surface| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = surface;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: dn,
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
        g.add_edge(a, edge(a, d, 70, Surface::Unpaved));
        g.add_edge(d, edge(d, b, 80, Surface::Paved));
        (g, a, b)
    }

    #[cfg(test)]
    fn astar_fixture() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        // Compact coords: a→b ~7 m straight-line, edges 100/90/90 m ⇒ haversine ≤ length.
        let a = g.add_node(mk("a", 50.00000, 4.00000));
        let b = g.add_node(mk("b", 50.00000, 4.00010));
        let c = g.add_node(mk("c", 50.00005, 4.00005));
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
        (g, a, b)
    }

    fn front_costs(r: &MultiObjResult) -> Vec<(u64, u64)> {
        let mut v: Vec<(u64, u64)> = r
            .front
            .iter()
            .map(|p| {
                (
                    (p.cost.get(Axis::Time) * 1000.0) as u64,
                    (p.cost.get(Axis::Surface) * 1000.0) as u64,
                )
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn astar_front_equals_uninformed_and_expands_no_more() {
        let (g, a, b) = astar_fixture();
        let bike = g.default_bike_cost();
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let plain = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let astar = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            true,
        );
        assert_eq!(
            front_costs(&plain),
            front_costs(&astar),
            "A* must not change the Pareto front"
        );
        assert!(
            astar.expansions <= plain.expansions,
            "A* expands no more labels ({} vs {})",
            astar.expansions,
            plain.expansions
        );
    }

    // Bike rides a steep descent at the profile's `max_speed` cap (12.5 m/s by
    // default), which is FASTER than the configured cruising speed — so the A* bound
    // must come from that cap or it over-estimates remaining time and breaks the
    // front. Realistic coords (haversine ≈ edge length) keep the bound tight.
    #[cfg(test)]
    fn bike_descent_fixture() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        // ~2 km straight east; detour bows north. Edge lengths ≈ haversine.
        let a = g.add_node(mk("a", 50.0000, 4.0000));
        let b = g.add_node(mk("b", 50.0000, 4.0280));
        let c = g.add_node(mk("c", 50.0010, 4.0140));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, s: Surface, elev: i16| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Tertiary;
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
                elev_delta: elev,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 2008, Surface::Unpaved, -150)); // fast-but-rough descent
        g.add_edge(a, edge(a, c, 1100, Surface::Paved, -75));
        g.add_edge(c, edge(c, b, 1100, Surface::Paved, -75)); // smoother detour
        (g, a, b)
    }

    #[test]
    fn astar_bike_descent_front_invariant() {
        let (g, a, b) = bike_descent_fixture();
        let bike = g.default_bike_cost();
        let w = g.raptor.cost_weights;
        // Production-like ε absorbs the ≤1 s/edge time rounding; the descent-speed
        // bound bug (if reintroduced) is multi-second and would still show.
        let eps = Epsilon::uniform(2.0, 0.0);
        let plain = g.multiobj_search(
            a,
            b,
            RoutingMode::Bike,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let astar = g.multiobj_search(
            a,
            b,
            RoutingMode::Bike,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            true,
        );
        assert_eq!(
            front_costs(&plain),
            front_costs(&astar),
            "A* (bike descent) must not change the Pareto front"
        );
    }

    fn front_key(r: &MultiObjResult) -> Vec<(u64, u64)> {
        let mut v: Vec<(u64, u64)> = r
            .front
            .iter()
            .map(|p| {
                (
                    (p.cost.get(Axis::Time) * 1000.0) as u64,
                    (p.cost.get(Axis::Surface) * 1000.0) as u64,
                )
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn heuristic_front_equals_uninformed_front() {
        let (g, a, b) = tiny_detour_graph();
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let plain = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let informed = g.multiobj_search_informed(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
        );
        assert_eq!(
            front_key(&plain),
            front_key(&informed),
            "heuristics must not change the Pareto front"
        );
    }

    #[test]
    fn heuristic_front_equals_uninformed_front_triple() {
        let (g, a, b) = tiny_triple_graph();
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let plain = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let informed = g.multiobj_search_informed(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
        );
        assert!(
            plain.front.len() >= 2,
            "expected a non-trivial front, got {}",
            plain.front.len()
        );
        assert_eq!(
            front_key(&plain),
            front_key(&informed),
            "heuristics must not change the Pareto front"
        );
    }

    /// `a` reaches the destination `b` by one short paved edge. From `a` a long
    /// chain of `field_len` paved nodes also fans out; every chain label has
    /// strictly higher Time AND Surface than `b`, so `b` weakly dominates them
    /// all. Target pruning must settle `b` first and then refuse to expand the
    /// chain. Without it, the search walks the entire chain.
    #[cfg(test)]
    fn tiny_target_prune_graph(field_len: usize) -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.001));
        let field: Vec<NodeID> = (0..field_len)
            .map(|i| g.add_node(mk(&format!("f{i}"), 50.001 + i as f64 * 1e-4, 4.000)))
            .collect();
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize| {
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
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        // Cheap direct route a→b (10 m).
        g.add_edge(a, edge(a, b, 10));
        // Long chain a→f0→f1→… (50 m hops) — every node strictly worse than b.
        let mut prev = a;
        for &f in &field {
            g.add_edge(prev, edge(prev, f, 50));
            prev = f;
        }
        (g, a, b)
    }

    #[test]
    fn target_pruning_bounds_search_to_destination_front() {
        let (g, a, b) = tiny_target_prune_graph(2000);
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        // No distance budget, so only target pruning can stop the 2000-node chain.
        let res = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        assert_eq!(
            res.front.len(),
            1,
            "only the direct a→b path reaches the destination"
        );
        assert!(
            res.expansions < 50,
            "target pruning must cut the dominated chain; expanded {} labels",
            res.expansions
        );
    }

    #[test]
    fn search_finds_pareto_tradeoff_walk() {
        let (g, a, b) = tiny_detour_graph();
        let res = g.multiobj_search_uniform(a, b, crate::structures::cost::RoutingMode::Walk);
        assert!(
            res.front.len() >= 2,
            "expected a time/surface trade-off, got {}",
            res.front.len()
        );
        for p in &res.front {
            assert_eq!(*p.nodes.first().unwrap(), a);
            assert_eq!(*p.nodes.last().unwrap(), b);
        }
    }

    #[test]
    fn labelset_rejects_dominated_keeps_tradeoffs() {
        let eps = Epsilon::uniform(0.0, 0.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cv(10.0, 5.0), &eps, &Buckets::NONE));
        assert!(!set.try_add(cv(10.0, 6.0), &eps, &Buckets::NONE));
        assert!(set.try_add(cv(8.0, 9.0), &eps, &Buckets::NONE));
        assert!(set.try_add(cv(7.0, 4.0), &eps, &Buckets::NONE));
        assert_eq!(set.len(), 1, "dominating label evicts the others");
    }

    #[test]
    fn labelset_rejects_exact_duplicates() {
        // Distinct paths routinely reach a node with byte-identical cost vectors
        // (same integer Time, same Surface). A duplicate must NOT be re-admitted —
        // otherwise a hot node accumulates tens of thousands of identical labels
        // and every scan over it turns the search quadratic.
        let eps = Epsilon::uniform(0.0, 0.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cv(298.0, 398.0), &eps, &Buckets::NONE));
        for _ in 0..1000 {
            assert!(
                !set.try_add(cv(298.0, 398.0), &eps, &Buckets::NONE),
                "identical cost must be rejected"
            );
        }
        assert_eq!(
            set.len(),
            1,
            "a node keeps one label per distinct cost, not duplicates"
        );
    }

    #[test]
    fn labelset_eps_prunes_near_neighbours() {
        let eps = Epsilon::uniform(1.0, 0.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cv(10.0, 5.0), &eps, &Buckets::NONE));
        assert!(!set.try_add(cv(10.5, 5.0), &eps, &Buckets::NONE));
    }

    #[test]
    fn labelset_contains_exact() {
        let eps = Epsilon::uniform(0.0, 0.0);
        let mut set = LabelSet::new();
        set.try_add(cv(10.0, 5.0), &eps, &Buckets::NONE);
        assert!(set.contains(&cv(10.0, 5.0)));
        assert!(!set.contains(&cv(10.0, 6.0)));
    }

    // ---- bucket pruning (grid-cap on diversity axes) ----

    fn cvc(time: f64, cyc: f64) -> CostVector {
        CostVector::from_active(&[Axis::Time, Axis::CyclewayDeficit], &[time, cyc])
    }
    fn buckets_cyc(size: f64) -> Buckets {
        let mut sizes = [0.0; crate::structures::cost::AXIS_COUNT];
        sizes[Axis::CyclewayDeficit.index()] = size;
        Buckets { sizes }
    }

    #[test]
    fn bucket_collapses_same_cell_tradeoff_keeps_min_time() {
        // (300,150) and (250,180) are a genuine trade-off (neither dominates), so
        // ε-Pareto alone keeps both. They share cyc cell 1 (150,180 → ⌊x/100⌋=1),
        // so bucketing collapses them to one, keeping the lower-Time representative.
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_cyc(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvc(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvc(250.0, 180.0), &eps, &bk));
        assert_eq!(set.len(), 1, "same-cell trade-off collapses to one label");
        assert_eq!(
            set.costs[0].get(Axis::Time),
            250.0,
            "the cell keeps the lower-Time (fastest-for-this-trade-off) representative"
        );
        // A higher-Time newcomer in the same cell is rejected.
        assert!(!set.try_add(cvc(400.0, 110.0), &eps, &bk));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn bucket_keeps_distinct_cells_preserves_extreme() {
        // Three trade-off labels (cyc up as time down) in DIFFERENT cyc cells
        // (50→0, 150→1, 250→2). All survive, so the cycleway extreme (cyc=50) is
        // preserved — the failure mode coarse ε-dominance exhibits (absorbing the
        // min-cyc length-detour into a faster, higher-cyc neighbour).
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_cyc(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvc(400.0, 50.0), &eps, &bk));
        assert!(set.try_add(cvc(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvc(250.0, 250.0), &eps, &bk));
        assert_eq!(set.len(), 3, "labels in distinct cells all survive");
        let min_cyc = set
            .costs
            .iter()
            .map(|c| c.get(Axis::CyclewayDeficit))
            .fold(f64::INFINITY, f64::min);
        assert_eq!(min_cyc, 50.0, "the cycleway-extreme cell keeps its representative");
    }

    #[test]
    fn bucket_none_is_strict_noop() {
        // With NONE, the same-cell trade-off that bucketing would collapse must
        // both survive — bucketing is opt-in and never changes ε-Pareto behavior.
        let eps = Epsilon::uniform(0.0, 0.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvc(300.0, 150.0), &eps, &Buckets::NONE));
        assert!(set.try_add(cvc(250.0, 180.0), &eps, &Buckets::NONE));
        assert_eq!(set.len(), 2, "no-bucket keeps both trade-offs");
    }

    #[test]
    fn bucket_still_dominance_prunes() {
        // A strictly-dominated label is rejected regardless of which cell it lands
        // in — bucketing is layered on top of (not instead of) Pareto dominance.
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_cyc(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvc(250.0, 150.0), &eps, &bk));
        assert!(!set.try_add(cvc(300.0, 250.0), &eps, &bk));
        assert_eq!(set.len(), 1);
    }

    // ---- bucket pruning on Drive's Variance axis / Walk's Surface axis ----
    // Mirrors the CyclewayDeficit bucket tests above: same generic mechanism
    // (`Buckets`/`LabelSet`), applied to the axes each mode buckets.

    fn cvv(time: f64, variance: f64) -> CostVector {
        CostVector::from_active(&[Axis::Time, Axis::Variance], &[time, variance])
    }
    fn buckets_var(size: f64) -> Buckets {
        let mut sizes = [0.0; crate::structures::cost::AXIS_COUNT];
        sizes[Axis::Variance.index()] = size;
        Buckets { sizes }
    }

    #[test]
    fn bucket_collapses_variance_tradeoff_on_drive_axis() {
        // (300,150) and (250,180) trade Time against Variance (neither dominates),
        // so ε-Pareto alone keeps both. They share variance cell 1 (150,180 →
        // ⌊x/100⌋=1), so bucketing collapses them to the lower-Time representative —
        // exactly the CyclewayDeficit bucket behavior, on Drive's axis instead.
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_var(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvv(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvv(250.0, 180.0), &eps, &bk));
        assert_eq!(set.len(), 1, "same-cell trade-off collapses to one label");
        assert_eq!(
            set.costs[0].get(Axis::Time),
            250.0,
            "the cell keeps the lower-Time (fastest-for-this-trade-off) representative"
        );
    }

    #[test]
    fn bucket_keeps_distinct_variance_cells_preserves_extreme() {
        // Three trade-off labels (variance up as time down) in DIFFERENT variance
        // cells (50→0, 150→1, 250→2) all survive, preserving the low-variance
        // extreme — the reliability-conscious route stays on the frontier instead
        // of being absorbed by a faster, higher-variance neighbour.
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_var(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvv(400.0, 50.0), &eps, &bk));
        assert!(set.try_add(cvv(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvv(250.0, 250.0), &eps, &bk));
        assert_eq!(set.len(), 3, "labels in distinct cells all survive");
        let min_var = set
            .costs
            .iter()
            .map(|c| c.get(Axis::Variance))
            .fold(f64::INFINITY, f64::min);
        assert_eq!(min_var, 50.0, "the low-variance extreme cell keeps its representative");
    }

    fn cvs(time: f64, surface: f64) -> CostVector {
        CostVector::from_active(&[Axis::Time, Axis::Surface], &[time, surface])
    }
    fn buckets_surf(size: f64) -> Buckets {
        let mut sizes = [0.0; crate::structures::cost::AXIS_COUNT];
        sizes[Axis::Surface.index()] = size;
        Buckets { sizes }
    }

    #[test]
    fn bucket_collapses_surface_tradeoff_on_walk_axis() {
        // Same mechanism, on Walk's Surface axis: a paved-but-slower vs. faster-but-
        // rougher trade-off sharing a surface cell collapses to the faster one.
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_surf(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvs(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvs(250.0, 180.0), &eps, &bk));
        assert_eq!(set.len(), 1, "same-cell trade-off collapses to one label");
        assert_eq!(set.costs[0].get(Axis::Time), 250.0);
    }

    #[test]
    fn bucket_keeps_distinct_surface_cells_preserves_extreme() {
        let eps = Epsilon::uniform(0.0, 0.0);
        let bk = buckets_surf(100.0);
        let mut set = LabelSet::new();
        assert!(set.try_add(cvs(400.0, 50.0), &eps, &bk));
        assert!(set.try_add(cvs(300.0, 150.0), &eps, &bk));
        assert!(set.try_add(cvs(250.0, 250.0), &eps, &bk));
        assert_eq!(set.len(), 3, "labels in distinct cells all survive");
        let min_surf = set
            .costs
            .iter()
            .map(|c| c.get(Axis::Surface))
            .fold(f64::INFINITY, f64::min);
        assert_eq!(min_surf, 50.0, "the smoothest-surface extreme cell keeps its representative");
    }

    #[test]
    fn distance_budget_prunes_long_detours() {
        let (g, a, b) = tiny_detour_graph();
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let tight = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            0.0,
            false,
        );
        let loose = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            1.0,
            false,
        );
        assert!(
            tight.front.iter().all(|p| p.nodes.len() == 2),
            "δ=0 forbids the 180m detour, got paths {:?}",
            tight
                .front
                .iter()
                .map(|p| p.nodes.len())
                .collect::<Vec<_>>()
        );
        assert!(
            loose.front.iter().any(|p| p.nodes.len() == 3),
            "δ=1 admits the detour"
        );
    }

    #[test]
    fn walk_front_min_time_matches_scalar_dijkstra() {
        let (g, a, b) = tiny_detour_graph();
        let scalar = g.walk_dijkstra(a, u32::MAX);
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let min_time = res
            .front
            .iter()
            .map(|p| p.cost.get(crate::structures::cost::Axis::Time))
            .fold(f64::INFINITY, f64::min);
        assert_eq!(
            min_time as u32, scalar[&b],
            "engine's fastest walk must equal scalar dijkstra time to b"
        );
    }

    #[cfg(test)]
    fn tiny_signal_choice_graph() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.010));
        let c = g.add_node(mk("c", 50.001, 4.005));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, vg: VarGen| {
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
                elev_delta: 0,
                surface_speed: 100,
                var_gen: vg,
            })
        };
        g.add_edge(a, edge(a, b, 100, VarGen::SIGNALIZED));
        g.add_edge(a, edge(a, c, 90, VarGen::NONE));
        g.add_edge(c, edge(c, b, 90, VarGen::NONE));
        (g, a, b)
    }

    #[test]
    fn variance_axis_is_always_on_signal_tradeoff() {
        use crate::structures::cost::Axis;
        let (g, a, b) = tiny_signal_choice_graph();
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        assert!(
            res.front.iter().all(|p| p.cost.get(Axis::Variance) > 0.0),
            "reliability variance is always positive (systematic term on any non-zero time)"
        );
        let by_time = res.front.iter().min_by(|x, y| {
            x.cost
                .get(Axis::Time)
                .partial_cmp(&y.cost.get(Axis::Time))
                .unwrap()
        });
        let by_rel = res.front.iter().min_by(|x, y| {
            x.cost
                .get(Axis::Variance)
                .partial_cmp(&y.cost.get(Axis::Variance))
                .unwrap()
        });
        assert!(
            res.front.len() >= 2
                && by_time.map(|p| p.nodes.clone()) != by_rel.map(|p| p.nodes.clone()),
            "the fastest path and the most-reliable path differ — reliability is a live front axis"
        );
    }

    #[test]
    fn variance_slot_holds_reliability_var_not_raw() {
        use crate::structures::cost::{Axis, VarGen, VarianceModel};
        let (mut g, a, b) = tiny_signal_choice_graph();
        g.set_representatives_k(6);
        let cv = g.raptor.systematic_cv;
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let model = VarianceModel::default();
        let raw_signal =
            model.variance(VarGen::SIGNALIZED, crate::structures::HighwayClass::Residential);
        let direct = res
            .front
            .iter()
            .find(|p| p.nodes.len() == 2)
            .expect("direct signalized path is on the front");
        let t = direct.cost.get(Axis::Time);
        let expected = raw_signal + cv * cv * t * t;
        assert!(
            (direct.cost.get(Axis::Variance) - expected).abs() < 1e-6,
            "Variance slot = raw_var + (cv*time)^2; got {} expected {}",
            direct.cost.get(Axis::Variance),
            expected
        );
        assert!(cv * cv * t * t > 0.0, "systematic term must be present");
    }

    #[test]
    #[ignore]
    fn astar_perf_diag_real_brussels() {
        use crate::structures::cost::RoutingMode;
        use std::time::Instant;
        let path = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let mut g = Graph::new();
        crate::ingestion::osm::load_pbf_file(path, None, 4.0, &Default::default(), &mut g).unwrap();
        g.build_raptor_index();
        // ~2.4 km apart in central Brussels.
        let (_, &o) = g.nearest_node_dist(50.841, 4.415).expect("o");
        let (_, &d) = g.nearest_node_dist(50.845, 4.381).expect("d");
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = g.raptor.epsilon;
        let budget = g.raptor.distance_budget;
        for mode in [RoutingMode::Walk, RoutingMode::Bike] {
            for astar in [false, true] {
                let t = Instant::now();
                let r =
                    g.multiobj_search(o, d, mode, LegRole::Neutral, &bike, &w, &eps, budget, astar);
                eprintln!(
                    "DIAG mode={:?} astar={} elapsed={:.2?} expansions={} front={}",
                    mode,
                    astar,
                    t.elapsed(),
                    r.expansions,
                    r.front.len()
                );
            }
        }
    }

    #[test]
    #[ignore]
    fn multiobj_smoke_real_brussels() {
        use crate::structures::cost::RoutingMode;
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

        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = g.raptor.epsilon;
        let budget = g.raptor.distance_budget;
        eprintln!("SMOKE budget={} eps={:?}", budget, eps);

        for mode in [RoutingMode::Walk, RoutingMode::Bike] {
            let ts = Instant::now();
            let res =
                g.multiobj_search(o, d, mode, LegRole::Neutral, &bike, &w, &eps, budget, false);
            eprintln!(
                "SMOKE mode={:?} search={:.1?} front_size={} expansions={}",
                mode,
                ts.elapsed(),
                res.front.len(),
                res.expansions
            );
            assert!(
                !res.front.is_empty(),
                "{:?}: expected a non-empty front between nearby Brussels points",
                mode
            );
            let fastest = res
                .front
                .iter()
                .min_by(|a, b| {
                    a.cost
                        .get(Axis::Time)
                        .partial_cmp(&b.cost.get(Axis::Time))
                        .unwrap()
                })
                .unwrap();
            assert!(
                fastest.nodes.len() >= 2,
                "fastest path must traverse at least one edge"
            );
            assert_eq!(*fastest.nodes.first().unwrap(), o, "path starts at origin");
            assert_eq!(
                *fastest.nodes.last().unwrap(),
                d,
                "path ends at destination"
            );
            for w2 in fastest.nodes.windows(2) {
                let connected = g.edges[w2[0].0]
                    .iter()
                    .any(|e| matches!(e, EdgeData::Street(s) if s.destination == w2[1]));
                assert!(connected, "consecutive path nodes must share a street edge");
            }
        }
    }

    #[test]
    fn walk_dplus_denoised_over_noise_bumps() {
        use crate::structures::cost::{Axis, VarGen};
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let b = g.add_node(mk("b", 50.0, 4.0010));
        let c = g.add_node(mk("c", 50.0, 4.0020));
        g.build_raptor_index();
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Residential;
        at.surface = Surface::Paved;
        let mk_edge = |o, d, len, elev: i16| {
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
        // +2 m then −2 m over 80 m edges: raw max(0,Δ) sum = 2 m of phantom ascent;
        // the hysteresis (5 m buffer) must absorb it → ~0 m on the Dplus axis.
        g.add_edge(a, mk_edge(a, b, 80, 2));
        g.add_edge(b, mk_edge(b, c, 80, -2));
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            c,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        let dplus = res
            .front
            .iter()
            .map(|p| p.cost.get(Axis::Dplus))
            .fold(f64::INFINITY, f64::min);
        assert_eq!(
            dplus, 0.0,
            "noise bumps must not accumulate as walk ascent (raw would be 2)"
        );
    }

    #[test]
    fn walk_dplus_flushes_small_net_climb() {
        use crate::structures::cost::{Axis, VarGen};
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let b = g.add_node(mk("b", 50.0, 4.0010));
        g.build_raptor_index();
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Residential;
        at.surface = Surface::Paved;
        // A real +3 m net climb (below the 5 m buffer): the residual must be flushed
        // at the destination so D+ = 3, not 0.
        g.add_edge(
            a,
            EdgeData::Street(StreetEdgeData {
                origin: a,
                destination: b,
                partial: false,
                length: 100,
                foot: true,
                bike: true,
                car: false,
                attrs: at,
                elev_delta: 3,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            }),
        );
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            b,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        assert_eq!(res.front.len(), 1);
        assert_eq!(
            res.front[0].cost.get(Axis::Dplus),
            3.0,
            "small net climb flushed to its true value"
        );
    }

    #[test]
    fn walk_single_path_time_equals_scalar() {
        use crate::structures::cost::{Axis, VarGen};
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
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
        let b = g.add_node(mk("b", 50.0, 4.0010));
        let c = g.add_node(mk("c", 50.0, 4.0020));
        g.build_raptor_index();
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Residential;
        at.surface = Surface::Paved;
        let mk_edge = |o, d, len| {
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
        g.add_edge(a, mk_edge(a, b, 137));
        g.add_edge(b, mk_edge(b, c, 211));
        let scalar = g.walk_dijkstra(a, u32::MAX);
        let bike = BikeCost::new(g.raptor.bike_profile);
        let w = g.raptor.cost_weights;
        let eps = Epsilon::uniform(0.0, 0.0);
        let res = g.multiobj_search(
            a,
            c,
            RoutingMode::Walk,
            LegRole::Neutral,
            &bike,
            &w,
            &eps,
            f64::INFINITY,
            false,
        );
        assert_eq!(
            res.front.len(),
            1,
            "single straight path → one front member"
        );
        assert_eq!(
            res.front[0].cost.get(Axis::Time) as u32,
            scalar[&c],
            "engine time must equal scalar dijkstra exactly"
        );
    }

    #[test]
    fn bike_front_collapses_demoted_axes_to_core() {
        use crate::structures::cost::{Epsilon, LegRole, RoutingMode, VarGen};
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData,
            StreetEdgeData, Surface,
        };
        let mut g = Graph::new();
        g.set_distance_budget(f64::INFINITY);
        // Zero the search epsilon so the only collapse mechanism under test is the
        // 3-axis projection, not ε-domination: the default Variance ε floor (150) is
        // far larger than a single residential edge's variance (~0.6), which would
        // otherwise ε-collapse the genuine Surface↔Variance trade-off on its own.
        g.raptor.epsilon = Epsilon::uniform(0.0, 0.0);
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let a = g.add_node(mk("a", 50.000, 4.0000));
        let b = g.add_node(mk("b", 50.000, 4.0010));
        g.build_raptor_index();
        let edge = |surface: Surface, vg: VarGen| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = surface;
            EdgeData::Street(StreetEdgeData {
                origin: a, destination: b, partial: false, length: 100,
                surface_speed: 100,
                foot: true, bike: true, car: false, attrs: at, elev_delta: 0, var_gen: vg,
            })
        };
        // Two parallel a->b edges: equal on the 3 core axes (Time, CyclewayDeficit, D+),
        // trading off on the demoted axes — A smoother but noisier, B rougher but calmer.
        g.add_edge(a, edge(Surface::Paved, VarGen::SIGNALIZED));
        g.add_edge(a, edge(Surface::Unpaved, VarGen::NONE));
        let bike = g.default_bike_cost();
        let front = g.multiobj_representatives_budgeted(
            a, b, RoutingMode::Bike, LegRole::Neutral, &bike, f64::INFINITY, true,
        );
        assert_eq!(front.len(), 1, "3-axis front collapses Surface/Variance-only trade-offs");
    }

    // ---- synthetic diagnostic: distance-adaptive bucketing bounds the frontier
    // regardless of route length (Drive/Variance, Walk/Surface) ----
    //
    // K disjoint origin→hub_i→destination branches (private hub/sub-chain per
    // branch, sharing no intermediate node), each a genuine Time↔diversity-axis
    // trade-off: as i increases, Time strictly decreases and the diversity axis
    // (Variance or Surface) strictly increases, so all K branches are mutually
    // non-dominated. This is the combinatorial-with-distance blowup the design
    // notes describe: every branch is a candidate route (e.g. "take i more
    // signalized junctions to save a little time"), so the un-bucketed frontier
    // grows with the number of branches (i.e. with route length/complexity), while
    // distance-adaptive bucketing (cell ∝ origin→dest straight-line distance)
    // should keep the frontier roughly constant as that grows, instead of scaling
    // with it. Branches are disjoint (only O and D are shared) so bucket eviction
    // is exercised exactly once, at the destination — matching how the bucket cap
    // is meant to pick representatives from otherwise-independent alternatives,
    // not how it behaves when forced to arbitrate at every hop of a shared path
    // (see `bucket_eviction_can_keep_a_dominated_label` for that lossy property).

    #[cfg(test)]
    fn drive_fanout_graph(n: usize, hop_m: f64) -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        g.set_distance_budget(f64::INFINITY);
        g.raptor.epsilon = Epsilon::uniform(0.0, 0.0);
        let lat: f64 = 50.850;
        let m_per_deg = 111_320.0 * lat.to_radians().cos();
        let mk = |eid: String| NodeData::OsmNode(OsmNodeData { eid, lat_lng: LatLng { latitude: lat, longitude: 4.300 } });
        let o = g.add_node(mk("o".into()));
        let d = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "d".into(),
            lat_lng: LatLng { latitude: lat, longitude: 4.300 + (n as f64 * hop_m) / m_per_deg },
        }));
        // Branch i: i private zero-length signalized (SIGNALIZED, Secondary) hops
        // — each a fixed, realistic +324 s² (secondary-road signal σ=18) — followed
        // by one non-signalized "safe distance" edge whose length (and therefore
        // Time) strictly decreases with i. Variance and Time are controlled by
        // fully independent knobs (hop count vs. final edge length), so every
        // branch i=0..n is a genuine, mutually non-dominated trade-off.
        let mut hub_nodes = Vec::new();
        for i in 0..=n {
            let mut cur = o;
            for j in 0..i {
                let nxt = g.add_node(mk(format!("sig{i}_{j}")));
                hub_nodes.push((cur, nxt));
                cur = nxt;
            }
            hub_nodes.push((cur, d)); // marker: final safe edge endpoint pair, length filled below
        }
        g.build_raptor_index();
        let signal_edge = |o: NodeID, dn: NodeID| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Secondary;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: dn, partial: false, length: 0,
                foot: false, bike: false, car: true, attrs: at, elev_delta: 0,
                surface_speed: 100, var_gen: VarGen::SIGNALIZED,
            })
        };
        let safe_edge = |o: NodeID, dn: NodeID, len: usize| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = Surface::Paved;
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: dn, partial: false, length: len,
                foot: false, bike: false, car: true, attrs: at, elev_delta: 0,
                surface_speed: 100, var_gen: VarGen::NONE,
            })
        };
        const L0: usize = 20_000;
        // Speed=11 m/s ⇒ street_secs truncates to whole seconds (speed_mms/1000 = 11 m
        // per second); DL must exceed that so every branch gets a distinct integer
        // Time, or truncation ties would let a same-Time, higher-variance branch get
        // weakly-dominated away — an artifact of the test fixture, not of bucketing.
        const DL: usize = 20;
        let mut idx = 0;
        for i in 0..=n {
            for _ in 0..i {
                let (a, b) = hub_nodes[idx];
                g.add_edge(a, signal_edge(a, b));
                idx += 1;
            }
            let (a, b) = hub_nodes[idx];
            idx += 1;
            g.add_edge(a, safe_edge(a, b, L0 - i * DL));
        }
        (g, o, d)
    }

    #[test]
    fn drive_variance_bucket_bounds_frontier_growth_with_distance() {
        let bike = BikeCost::new(crate::structures::BikeProfile::default());
        let w = CostWeights::default();
        let eps = Epsilon::uniform(0.0, 0.0);
        // ~700 m/branch-equivalent: N=15 ⇒ ~10.5 km, N=60 ⇒ ~42 km O-D distance
        // (realistic direct-drive scale); both the Variance range (324·N) and the
        // O-D distance (700·N) scale linearly with N in lockstep, so the
        // range/(k·distance) bucket-count ratio is invariant to N by construction.
        let hop_m = 702.82;

        let search = |n: usize, kv: f64| {
            let (mut g, a, b) = drive_fanout_graph(n, hop_m);
            g.set_drive_bucket_var_k(kv);
            g.multiobj_search(
                a, b, RoutingMode::Drive, LegRole::Neutral, &bike, &w, &eps, f64::INFINITY, false,
            )
            .front
            .len()
        };

        let plain_15 = search(15, 0.0);
        let plain_60 = search(60, 0.0);
        // Not exactly N+1: the Variance axis also carries a systematic (cv·Time)²
        // term (see `variance_slot_holds_reliability_var_not_raw`) that shrinks
        // slightly as Time drops, occasionally dominating a close-by branch. The
        // qualitative property under test is growth with route length/complexity,
        // not the exact combinatorial count.
        assert!(
            plain_15 >= 8,
            "at least half the 16 branches must be genuine (non-dominated) trade-offs, got {plain_15}"
        );
        assert!(
            plain_60 >= plain_15 * 2,
            "unbucketed frontier must keep growing as route length/complexity grows: {plain_15} -> {plain_60}"
        );

        let kv = RaptorIndex::default_drive_bucket_var_k();
        let bucketed_15 = search(15, kv);
        let bucketed_60 = search(60, kv);
        assert!(
            bucketed_15 < plain_15 && bucketed_60 < plain_60,
            "bucketing must shrink the frontier at both scales: {bucketed_15} vs {plain_15}, {bucketed_60} vs {plain_60}"
        );
        assert!(
            bucketed_60 <= bucketed_15 + 3,
            "bucketed frontier must stay roughly flat as distance quadruples (15={bucketed_15}, 60={bucketed_60}), not scale with route length like the unbucketed case ({plain_15} -> {plain_60})"
        );
        assert!(
            bucketed_60 >= 2,
            "bucketing must not collapse all diversity — at least a low-variance/fast trade-off must survive, got {bucketed_60}"
        );
    }

    #[cfg(test)]
    fn walk_fanout_graph(n: usize, hop_m: f64) -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        g.set_distance_budget(f64::INFINITY);
        g.raptor.epsilon = Epsilon::uniform(0.0, 0.0);
        let lat: f64 = 50.850;
        let m_per_deg = 111_320.0 * lat.to_radians().cos();
        let mk = |eid: String| NodeData::OsmNode(OsmNodeData { eid, lat_lng: LatLng { latitude: lat, longitude: 4.300 } });
        let o = g.add_node(mk("o".into()));
        let d = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "d".into(),
            lat_lng: LatLng { latitude: lat, longitude: 4.300 + (n as f64 * hop_m) / m_per_deg },
        }));
        let mut hubs = Vec::with_capacity(n + 1);
        for i in 0..=n {
            hubs.push(g.add_node(mk(format!("h{i}"))));
        }
        g.build_raptor_index();
        let edge = |o: NodeID, dn: NodeID, len: usize, surface: Surface| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = surface;
            EdgeData::Street(StreetEdgeData {
                origin: o, destination: dn, partial: false, length: len,
                foot: true, bike: false, car: false, attrs: at, elev_delta: 0,
                surface_speed: 100, var_gen: VarGen::NONE,
            })
        };
        // Branch i: an Unpaved leg of length x_i (roughness) followed by a Paved
        // leg of length y_i, solved so Time_i = (x_i+y_i)/speed strictly decreases
        // and Surface_i = 2.5·x_i + y_i strictly increases with i (independent
        // linear staircases: L_i = L0 - i·DL for total length, S_i = L0 + i·DS for
        // target Surface, both anchored at branch 0 = all-paved, x_0=0).
        const L0: f64 = 20_000.0;
        // Speed=1.2 m/s ⇒ street_secs truncates to whole seconds (speed_mms/1000 =
        // 1.2 m per second); DL must exceed that so every branch gets a distinct
        // integer Time (see the analogous comment in `drive_fanout_graph`).
        const DL: f64 = 5.0;
        const DS: f64 = 88.0;
        for i in 0..=n {
            let l_i = L0 - i as f64 * DL;
            let s_i = L0 + i as f64 * DS;
            let x_i = ((s_i - l_i) / 1.5).round().max(0.0);
            let y_i = (l_i - x_i).max(0.0);
            g.add_edge(o, edge(o, hubs[i], x_i as usize, Surface::Unpaved));
            g.add_edge(hubs[i], edge(hubs[i], d, y_i as usize, Surface::Paved));
        }
        (g, o, d)
    }

    #[test]
    fn walk_surface_bucket_bounds_frontier_growth_with_distance() {
        let bike = BikeCost::new(crate::structures::BikeProfile::default());
        let w = CostWeights::default();
        let eps = Epsilon::uniform(0.0, 0.0);
        let hop_m = 702.82;

        let search = |n: usize, ks: f64| {
            let (mut g, a, b) = walk_fanout_graph(n, hop_m);
            g.set_walk_bucket_surf_k(ks);
            g.multiobj_search(
                a, b, RoutingMode::Walk, LegRole::Neutral, &bike, &w, &eps, f64::INFINITY, false,
            )
            .front
            .len()
        };

        let plain_15 = search(15, 0.0);
        let plain_60 = search(60, 0.0);
        // Not exactly N+1: Walk's Variance axis also carries the (cv·Time)²
        // systematic term (constant here, since these edges never set var_gen), so
        // this is looser than the drive test's caveat, but kept the same shape for
        // consistency and robustness to future cost-model changes.
        assert!(
            plain_15 >= 8,
            "at least half the 16 branches must be genuine (non-dominated) trade-offs, got {plain_15}"
        );
        assert!(
            plain_60 >= plain_15 * 2,
            "unbucketed frontier must keep growing as route length/complexity grows: {plain_15} -> {plain_60}"
        );

        let ks = RaptorIndex::default_walk_bucket_surf_k();
        let bucketed_15 = search(15, ks);
        let bucketed_60 = search(60, ks);
        assert!(
            bucketed_15 < plain_15 && bucketed_60 < plain_60,
            "bucketing must shrink the frontier at both scales: {bucketed_15} vs {plain_15}, {bucketed_60} vs {plain_60}"
        );
        assert!(
            bucketed_60 <= bucketed_15 + 3,
            "bucketed frontier must stay roughly flat as distance quadruples (15={bucketed_15}, 60={bucketed_60}), not scale with route length like the unbucketed case ({plain_15} -> {plain_60})"
        );
        assert!(
            bucketed_60 >= 2,
            "bucketing must not collapse all diversity — at least a smooth/rough trade-off must survive, got {bucketed_60}"
        );
    }
}
