//! Exact foot access/egress via a Customizable Contraction Hierarchy (CCH).
//!
//! The contracted foot graph (junctions + super-edges) is extracted once, an inertial
//! nested-dissection order is computed, the CCH structure is built, and the walk-second
//! weights are customized into a metric. All transit-stop junctions are the fixed
//! one-to-many target set. A query snaps an origin/destination coordinate to its ≤2
//! bounding junctions (exactly as [`ContractedGraph::nearby_stops_arena`] /
//! [`ContractedGraph::foot_snap_entries`] do) and runs a single multi-source
//! one-to-many sweep to every stop — the CCH twin of `nearby_stops_arena`.
//!
//! Foot cost is direction-symmetric, so the same index answers both **access**
//! (origin → stops) and **egress** (dest → stops = stops → dest): [`Graph::cch_access`]
//! and [`Graph::cch_egress`] both dispatch to the same one-to-many sweep.

use routingkit_cch::{CCH, CCHMetric, CCHOneToMany, compute_order_inertial};

use super::Graph;
use crate::structures::LatLng;

/// A built + customized CCH for exact foot access/egress to all transit stops.
///
/// Self-referential: `metric` borrows the heap-stable `*cch`. `metric` is declared
/// before `cch` so it drops first (Rust drops fields in declaration order), releasing
/// the borrow before the box is freed. `CchAccess` must never be `Clone`/`Copy` — a
/// clone's metric would dangle to the original box.
pub struct CchAccess {
    /// Customized walk-second metric bound to `*cch`. Lifetime laundered to `'static`;
    /// the borrow is kept valid by the box below and the field drop order.
    metric: CCHMetric<'static>,
    /// Boxed so the CCH has a stable address across moves of `CchAccess` (the FFI query
    /// reads the Rust `&CCH` for bounds checks, so that reference must stay valid).
    _cch: Box<CCH>,
    /// Junction index of each pinned target, aligned 1:1 with `stop_compact`.
    targets: Vec<u32>,
    /// Compact stop id of each pinned target (same index as `targets`).
    stop_compact: Vec<u32>,
}

impl std::fmt::Debug for CchAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CchAccess")
            .field("targets", &self.targets.len())
            .finish_non_exhaustive()
    }
}

/// The extracted foot super-edge graph plus its pinned stop-target set — everything the
/// CCH needs that is a pure function of the contracted graph. Produced once by
/// [`Graph::extract_foot_graph`] and consumed by either the order computation (the ~56 s
/// [`compute_order_inertial`]) or the fast CCH assembly, so neither path re-walks the
/// adjacency lists more than once.
struct FootGraph {
    /// Vertex count (`cg.junctions.len()`); also the length of any valid order.
    n: usize,
    tail: Vec<u32>,
    head: Vec<u32>,
    weight: Vec<u32>,
    lat: Vec<f32>,
    lng: Vec<f32>,
    /// Junction index of each pinned target.
    targets: Vec<u32>,
    /// Compact stop id of each pinned target (aligned with `targets`).
    stop_compact: Vec<u32>,
}

impl Graph {
    /// Extract the foot super-edge graph + stop-target set from the contracted graph: stop
    /// junctions are sinks (their outgoing arcs are dropped, mirroring
    /// `export`/`walk_dijkstra_union_seeded`'s dead-end rule) so a shortest path never walks
    /// *through* a stop. Shared by [`Graph::compute_cch_order`] and
    /// [`Graph::build_cch_access_with_order`].
    ///
    /// Panics if no contracted graph is present (`build_raptor_index()` +
    /// `enable_contraction`/`from_graph_union` must have run first).
    fn extract_foot_graph(&self) -> FootGraph {
        let cg = self
            .contracted
            .as_ref()
            .expect("extract_foot_graph: contracted graph must be present");
        let n = cg.junctions.len();
        let lat: Vec<f32> = cg.junction_coord.iter().map(|c| c.latitude as f32).collect();
        let lng: Vec<f32> = cg.junction_coord.iter().map(|c| c.longitude as f32).collect();

        // Foot arcs: super-edge `ji -> se.to`, weight = `walk_secs`. Stop junctions are
        // sinks (skip their outgoing arcs).
        let (mut tail, mut head, mut weight) = (Vec::new(), Vec::new(), Vec::new());
        for ji in 0..n {
            let orig = cg.junctions[ji].0;
            if self.raptor.transit_node_to_stop[orig] != u32::MAX {
                continue;
            }
            for se in &cg.adjacency[ji] {
                if let Some(secs) = cg.walk_secs(self, se) {
                    tail.push(ji as u32);
                    head.push(se.to);
                    weight.push(secs);
                }
            }
        }

        // Targets: every stop junction, paired with its compact stop id.
        let mut targets = Vec::new();
        let mut stop_compact = Vec::new();
        for ji in 0..n {
            let compact = self.raptor.transit_node_to_stop[cg.junctions[ji].0];
            if compact != u32::MAX {
                targets.push(ji as u32);
                stop_compact.push(compact);
            }
        }

        FootGraph { n, tail, head, weight, lat, lng, targets, stop_compact }
    }

    /// Compute JUST the metric-independent nested-dissection ORDER (a `Vec<u32>` of node
    /// ranks, length `n`) from the extracted foot graph. This is the expensive (~56 s on
    /// Belgium) part and the only thing worth caching to `cch.bin`: it is a pure function of
    /// the foot-graph topology, independent of the walk-second weights. Feed the result to
    /// [`Graph::build_cch_access_with_order`].
    pub fn compute_cch_order(&self) -> Vec<u32> {
        let fg = self.extract_foot_graph();
        compute_order_inertial(fg.n as u32, &fg.tail, &fg.head, &fg.lat, &fg.lng)
    }

    /// Number of foot-graph vertices the order must permute — the contracted graph's
    /// junction count. A cached order whose length differs is stale and must be recomputed.
    pub fn cch_vertex_count(&self) -> usize {
        self.contracted
            .as_ref()
            .expect("cch_vertex_count: contracted graph must be present")
            .junctions
            .len()
    }

    /// Build a [`CchAccess`] from a precomputed `order`: re-extract the foot graph, build the
    /// CCH structure (`CCH::new`, ~1 s) and customize the walk-second metric (~0.26 s). Fast
    /// relative to [`Graph::compute_cch_order`]; run on every startup with the cached order.
    ///
    /// Panics if `order.len() != self.cch_vertex_count()` (a stale/foreign order).
    pub fn build_cch_access_with_order(&self, order: &[u32]) -> CchAccess {
        let fg = self.extract_foot_graph();
        assert_eq!(
            order.len(),
            fg.n,
            "build_cch_access_with_order: order length {} != vertex count {}",
            order.len(),
            fg.n
        );
        Self::assemble_cch(order, fg)
    }

    /// All-in-one build (compute order + assemble). Kept for tests and any caller that does
    /// not want to cache the order.
    ///
    /// Panics if no contracted graph is present.
    pub fn build_cch_access(&self) -> CchAccess {
        let fg = self.extract_foot_graph();
        let order = compute_order_inertial(fg.n as u32, &fg.tail, &fg.head, &fg.lat, &fg.lng);
        Self::assemble_cch(&order, fg)
    }

    /// order -> structure -> metric (customization binds the walk-second weights).
    fn assemble_cch(order: &[u32], fg: FootGraph) -> CchAccess {
        let FootGraph { tail, head, weight, targets, stop_compact, .. } = fg;
        let cch = Box::new(CCH::new(order, &tail, &head, |_| {}, false));
        // SAFETY: `metric` borrows `*cch`, which lives on the heap behind the box stored
        // alongside it. The box gives a stable address across moves of `CchAccess`, the
        // `metric` field drops before `_cch` (declaration order), and `CchAccess` is never
        // cloned, so the borrow never dangles or outlives the box.
        let cch_ref: &'static CCH = unsafe { std::mem::transmute::<&CCH, &'static CCH>(&*cch) };
        let metric = CCHMetric::new(cch_ref, weight);

        CchAccess { metric, _cch: cch, targets, stop_compact }
    }

    /// Install a prebuilt foot-access CCH. Runtime-only (`#[serde(skip)]`); chunk 2 wires
    /// this into startup.
    pub fn set_cch(&mut self, cch: CchAccess) {
        self.cch = Some(cch);
    }

    /// Exact foot **access**: stops reachable on foot from `origin`, as
    /// `(compact stop id, walk secs)` sorted by stop id, unreachable omitted. Same shape
    /// as [`ContractedGraph::nearby_stops_union`]/`nearby_stops_arena`, computed via the
    /// CCH one-to-many instead of a Dijkstra flood.
    pub fn cch_access(&self, cch: &CchAccess, origin: LatLng) -> Vec<(usize, u32)> {
        self.cch_one_to_many(cch, origin)
    }

    /// Exact foot **egress**: stops from which `dest` is reachable on foot. Foot cost is
    /// direction-symmetric, so this is the same one-to-many sweep from `dest` against the
    /// same stop targets. Same shape/sort as [`Graph::cch_access`].
    pub fn cch_egress(&self, cch: &CchAccess, dest: LatLng) -> Vec<(usize, u32)> {
        self.cch_one_to_many(cch, dest)
    }

    /// Shared one-to-many core: snap `coord` to its bounding-junction seeds, then sweep to
    /// every pinned stop target.
    fn cch_one_to_many(&self, cch: &CchAccess, coord: LatLng) -> Vec<(usize, u32)> {
        if cch.targets.is_empty() {
            return Vec::new();
        }
        let cg = self
            .contracted
            .as_ref()
            .expect("cch_one_to_many: contracted graph must be present");
        let radius = self.raptor.edge_snap_radius_m;
        let Some(entries) = cg.foot_snap_entries(self, coord.latitude, coord.longitude, radius)
        else {
            return Vec::new();
        };
        if entries.is_empty() {
            return Vec::new();
        }
        let sources: Vec<(u32, u32)> = entries.iter().map(|&(ji, s)| (ji as u32, s)).collect();

        let mut otm = CCHOneToMany::new(&cch.metric, &cch.targets);
        let dists = otm.distances_from_multi(&sources);

        let mut stops: Vec<(usize, u32)> = dists
            .into_iter()
            .enumerate()
            .filter_map(|(i, d)| d.map(|secs| (cch.stop_compact[i] as usize, secs)))
            .collect();
        stops.sort_unstable_by_key(|&(s, _)| s);
        stops
    }
}
