//! Exact foot access/egress via a Customizable Contraction Hierarchy (CCH).
//!
//! Foot cost is direction-symmetric, so the same one-to-many sweep answers both
//! access (origin → stops) and egress (dest → stops).

use routingkit_cch::{CCH, CCHMetric, CCHOneToMany, compute_order_inertial};

use super::Graph;
use crate::structures::LatLng;

/// Self-referential: `metric` borrows the heap-stable `*cch`. `metric` MUST be
/// declared before `cch` so it drops first (fields drop in declaration order),
/// releasing the borrow before the box is freed. `CchAccess` must NEVER be
/// `Clone`/`Copy` (a clone's metric would dangle to the original box).
pub struct CchAccess {
    metric: CCHMetric<'static>,
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

/// Extracted foot super-edge graph plus its pinned stop-target set: everything the
/// CCH needs that is a pure function of the contracted graph.
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
    /// Extract the foot super-edge graph + stop-target set. Stop junctions are sinks
    /// (outgoing arcs dropped) so a shortest path never walks *through* a stop.
    /// Panics if no contracted graph is present.
    fn extract_foot_graph(&self) -> FootGraph {
        let cg = self
            .contracted
            .as_ref()
            .expect("extract_foot_graph: contracted graph must be present");
        let n = cg.junctions.len();
        let lat: Vec<f32> = cg.junction_coord.iter().map(|c| c.latitude as f32).collect();
        let lng: Vec<f32> = cg.junction_coord.iter().map(|c| c.longitude as f32).collect();

        // Stop junctions are sinks (skip their outgoing arcs).
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

    /// Compute the metric-independent nested-dissection order (length `n`). Expensive
    /// (~56 s on Belgium), a pure function of foot-graph topology, so it is the only
    /// thing worth caching to `cch.bin`.
    pub fn compute_cch_order(&self) -> Vec<u32> {
        let fg = self.extract_foot_graph();
        compute_order_inertial(fg.n as u32, &fg.tail, &fg.head, &fg.lat, &fg.lng)
    }

    /// Foot-graph vertex count (junction count); a cached order of different length is stale.
    pub fn cch_vertex_count(&self) -> usize {
        self.contracted
            .as_ref()
            .expect("cch_vertex_count: contracted graph must be present")
            .junctions
            .len()
    }

    /// Build a [`CchAccess`] from a precomputed `order` (fast; run on every startup
    /// with the cached order). Panics if `order.len() != self.cch_vertex_count()`.
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

    /// All-in-one build (compute order + assemble). Panics if no contracted graph is present.
    pub fn build_cch_access(&self) -> CchAccess {
        let fg = self.extract_foot_graph();
        let order = compute_order_inertial(fg.n as u32, &fg.tail, &fg.head, &fg.lat, &fg.lng);
        Self::assemble_cch(&order, fg)
    }

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

    pub fn set_cch(&mut self, cch: CchAccess) {
        self.cch = Some(cch);
    }

    /// Exact foot access: stops reachable on foot from `origin`, as `(compact stop id,
    /// walk secs)` sorted by stop id, unreachable omitted.
    pub fn cch_access(&self, cch: &CchAccess, origin: LatLng) -> Vec<(usize, u32)> {
        self.cch_one_to_many(cch, origin)
    }

    /// Exact foot egress; same direction-symmetric sweep from `dest`.
    pub fn cch_egress(&self, cch: &CchAccess, dest: LatLng) -> Vec<(usize, u32)> {
        self.cch_one_to_many(cch, dest)
    }

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
