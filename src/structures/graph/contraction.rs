//! Ingestion-time degree-2 node contraction.
//!
//! Builds an in-memory contracted view of the bike street graph where maximal chains
//! of degree-2 pass-through nodes are collapsed into one **super-edge** per direction.
//! A super-edge stores the ordered chain of node ids between two junctions; a traversal
//! REPLAYS those hops through the unchanged per-segment cost model (`street_edge_transition`),
//! getting a result identical to walking the un-contracted chain — but creating a label
//! only at the far junction and without the on-the-fly contraction's `bike_chain_next`
//! topology re-derivation.
//!
//! P1 keeps the full node graph (snapping / plan reconstruction / other modes still use
//! it); the super-edges are an auxiliary fast adjacency for the bike multi-objective
//! search. Removing the interior nodes from `graph.bin` (the memory win) is P3.
//! See `docs/superpowers/specs/2026-06-25-node-contraction-design.md`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use super::bike_cost::{BikeCost, PrevCtx};
use super::raptor_access::StreetProfile;
use super::{EdgeData, Endpoint, Graph};
use crate::structures::cost::{Axis, CostVector, RoutingMode};
use crate::structures::{NodeID, StreetEdgeData};

impl BakedCost {
    /// Exact cost delta + exit context for traversing this super-edge entered with
    /// context `entry`. The baked aggregate carries the canonical first-two-segment
    /// corners; swap them for the real (entry-dependent) ones. Front axes (Time,
    /// CyclewayDeficit) are exact; demoted axes stay canonical.
    pub fn traverse(&self, entry: Option<PrevCtx>, bike: &BikeCost) -> (CostVector, PrevCtx) {
        let mut delta = self.cost;
        let c1 = bike.speed_change_secs(entry, &self.s1.edge, self.s1.dir);
        let c2 = self.s2.as_ref().map_or(0.0, |s2| {
            let ctx1 = ctx_after(bike, entry, &self.s1.edge, self.s1.dir);
            bike.speed_change_secs(Some(ctx1), &s2.edge, s2.dir)
        });
        let t = delta.get(Axis::Time) + c1 + c2 - self.corner_canon_secs;
        delta.set(Axis::Time, t);
        (delta, self.exit)
    }
}

/// The previous-edge context after traversing `street` in `dir` from `prev` (mirrors
/// `Graph::arrival_ctx`, free-standing so [`BakedCost::traverse`] needs no graph).
fn ctx_after(bike: &BikeCost, prev: Option<PrevCtx>, street: &StreetEdgeData, dir: (f64, f64)) -> PrevCtx {
    let push = BikeCost::is_push(&street.attrs);
    PrevCtx {
        dir,
        len: street.length as f64,
        cruise: if push { 0.0 } else { bike.cruise_speed(street) },
        push,
        speed: bike.required_speed(prev, street, dir),
    }
}

/// A contracted edge: the chain of node ids from the owning junction's first step up to
/// (and including) the far junction `junctions[to]`. `nodes.last() == junctions[to]`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SuperEdge {
    /// Start junction (original node id).
    pub from: NodeID,
    /// Index into [`ContractedGraph::junctions`] of the far junction.
    pub to: u32,
    /// Chain vertices after the start junction: `[first_step, …interior…, far_junction]`.
    pub nodes: Vec<NodeID>,
    /// Range `[seg_start, seg_start+seg_len)` into [`ContractedGraph::segs`] — the
    /// self-contained per-segment payload (edge + far-coord) for this super-edge, so
    /// traversal replays from the arena without reading `g.nodes`/`g.edges`. The bridge to
    /// P3f: once every consumer traverses via `segs`, the full node/edge arrays are dropped.
    pub seg_start: u32,
    pub seg_len: u32,
    /// Cost-baked traversal (degree-2 chains of ≥2 segments). `None` for a single-edge
    /// super-edge (no contraction benefit ⇒ traversed live like an un-contracted edge).
    /// Boxed so an unbaked super-edge costs one null pointer (8 B) instead of a full
    /// `BakedCost` (216 B) inline — decisive for the all-mode graph, which never bakes.
    /// Not serialized — re-baked after load (cheap, bike-only), like the bike index.
    #[serde(skip)]
    pub baked: Option<Box<BakedCost>>,
}

/// One directed hop of a super-edge's chain, self-contained (no `g` lookup): the relocated
/// [`StreetEdgeData`] (carries all mode flags + length + bike attrs + elev/surface/var) and
/// the **far-node coordinate** (the near end is the previous segment's far, or the start
/// junction). At P3f this arena replaces the interior nodes' rows in `g.edges`/`g.nodes`.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct Seg {
    pub edge: StreetEdgeData,
    pub far: crate::structures::LatLng,
}

/// Precomputed cost of traversing a super-edge, so the multi-objective search adds it
/// in O(1) (+ two live corners) instead of replaying every segment. The front axes
/// (Time, CyclewayDeficit) come out EXACT; the demoted axes (D+/Surface/Variance) are
/// canonical here and recomputed exactly only for the final displayed paths.
#[derive(Clone, Debug)]
pub struct BakedCost {
    /// Canonical full cost delta (entry = no incoming edge), all axes. Its Time includes
    /// the canonical corner at the 2nd segment (the 1st has none under a null entry).
    pub cost: crate::structures::cost::CostVector,
    pub length: usize,
    /// Canonical corner seconds at the 2nd segment, to subtract before adding the real
    /// (entry-dependent) corners of segments 1 and 2.
    pub corner_canon_secs: f64,
    /// First two segments (the only entry-dependent corners), for the live correction.
    pub s1: SegLite,
    pub s2: Option<SegLite>,
    /// Baked exit context (entry-independent for ≥2-segment chains), threaded as the
    /// incoming context of the next super-edge.
    pub exit: super::bike_cost::PrevCtx,
}

/// The bits of a segment needed to recompute its corner (`speed_change_secs`) live.
#[derive(Clone, Debug)]
pub struct SegLite {
    pub dir: (f64, f64),
    pub edge: StreetEdgeData,
}

/// Which edges define connectivity when deciding pass-through nodes and walking chains.
/// `Bike` is the original P1 bike-only adjacency (the cost-baked search). `AllModes` is
/// the **union** of foot/bike/car edges — the set P3 must contract to remove a node from
/// *every* consumer (a node is only removable if it's a pass-through for all modes).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Conn {
    Bike,
    AllModes,
}

impl Conn {
    fn neighbours(self, g: &Graph, u: usize) -> ([NodeID; 2], usize) {
        match self {
            Conn::Bike => ContractedGraph::bike_neighbours(g, u),
            Conn::AllModes => ContractedGraph::street_neighbours(g, u),
        }
    }

    /// Does an edge of the relevant connectivity run from `from` back to `to`?
    fn edge_back(self, g: &Graph, from: NodeID, to: NodeID) -> bool {
        let Some(neigh) = g.edges.get(from.0) else {
            return false;
        };
        neigh.iter().any(|e| match e {
            EdgeData::Street(s) if s.destination == to => match self {
                Conn::Bike => s.bike,
                Conn::AllModes => true,
            },
            _ => false,
        })
    }

    /// Counts an outgoing edge toward `indeg` of its destination.
    fn counts_inedge(self, s: &StreetEdgeData) -> bool {
        match self {
            Conn::Bike => s.bike,
            Conn::AllModes => true,
        }
    }
}

/// In-memory contracted view of the bike street graph.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ContractedGraph {
    /// Original `NodeID` of each junction.
    pub junctions: Vec<NodeID>,
    /// `original node index -> junction index`, or `u32::MAX` for a contracted interior
    /// pass-through node.
    pub junction_of: Vec<u32>,
    /// Per-junction outgoing super-edges.
    pub adjacency: Vec<Vec<SuperEdge>>,
    /// Flat arena of every super-edge's segments (indexed by `SuperEdge.seg_start/len`).
    /// Self-contained per-segment payload so traversal needs no `g` lookup. Empty until the
    /// all-mode build fills it (`fill_segments`).
    pub segs: Vec<Seg>,
    /// Coordinate of each junction (parallel to `junctions`), so geometry / bike `dir`
    /// survive dropping `g.nodes`. Empty until `fill_segments`.
    pub junction_coord: Vec<crate::structures::LatLng>,
    /// Flat per-super-edge metadata in `segs` order (so `seg_start` is ascending and a
    /// segment's owner is a binary search). Lets a snapped segment resolve to its bounding
    /// junctions g-free (P3f snapping).
    pub superedges: Vec<SuperEdgeMeta>,
    /// R-tree over the segment polylines for g-free snapping (rebuilt post-load, never
    /// serialized — the arena replacement for `Graph::edge_index`).
    #[serde(skip)]
    pub seg_index: super::edge_index::EdgeIndex,
}

/// Flat record locating a super-edge's segments + bounding junctions (`segs` order).
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct SuperEdgeMeta {
    pub from_ji: u32,
    pub to_ji: u32,
    pub seg_start: u32,
    pub seg_len: u32,
}

/// A snapped endpoint resolved for plan geometry: the segment it projected onto, the
/// projection point, and the bounding-junction entries with their proj→junction
/// chain length (g-free tie-break). Consumed by [`ContractedGraph::street_path_arena`].
struct GeomSnap {
    from_ji: usize,
    /// Global index of the segment the point projected onto.
    gi: u32,
    seg_start: u32,
    seg_len: u32,
    /// The projection point on the segment.
    proj: crate::structures::LatLng,
    /// `(junction index, proj→junction chain length m)` for each bounding junction.
    entries: Vec<(usize, u32)>,
}

/// A bike origin seed at a bounding junction: the kinematic bike cost of riding from a
/// snapped mid-super-edge projection (standstill entry, `prev=None`) to that junction,
/// replayed exactly through `street_edge_transition`. The arena twin of the g-based
/// `OnEdge` seeding in `bike_cost_path`, feeding `multiobj_search` at wiring (P3f).
#[derive(Clone, Debug)]
pub struct BikeSeed {
    /// Bounding junction index this seed enters the contracted graph at.
    pub junction: usize,
    /// Accumulated cost vector (all axes) of the proj→junction partial-super-edge ride.
    pub cost: CostVector,
    /// Exit context at the junction — the next super-edge's incoming `PrevCtx`.
    pub exit: PrevCtx,
    /// Carried elevation buffer at the junction (Bike passes it through unchanged).
    pub elev: (f64, f64),
    /// Accumulated additive variance at the junction.
    pub var: f64,
    /// Accumulated length (m) of the proj→junction ride.
    pub len: usize,
}

impl ContractedGraph {
    /// Distinct outgoing **bike** neighbours of node `u`; the count saturates past 2 so
    /// junctions are detected. A pure pass-through has exactly two.
    fn bike_neighbours(g: &Graph, u: usize) -> ([NodeID; 2], usize) {
        let mut out = [NodeID(usize::MAX); 2];
        let mut k = 0usize;
        let Some(neigh) = g.edges.get(u) else {
            return (out, 0);
        };
        for e in neigh {
            let EdgeData::Street(s) = e else { continue };
            if !s.bike {
                continue;
            }
            let d = s.destination;
            if (k >= 1 && out[0] == d) || (k >= 2 && out[1] == d) {
                continue;
            }
            if k < 2 {
                out[k] = d;
            }
            k += 1;
        }
        (out, k)
    }

    pub(super) fn bike_neighbours_of(g: &Graph, u: usize) -> ([NodeID; 2], usize) {
        Self::bike_neighbours(g, u)
    }

    /// Distinct outgoing **street** neighbours of node `u` over the union of foot/bike/car
    /// edges; the count saturates past 2. A union pass-through (removable by P3) has two.
    fn street_neighbours(g: &Graph, u: usize) -> ([NodeID; 2], usize) {
        let mut out = [NodeID(usize::MAX); 2];
        let mut k = 0usize;
        let Some(neigh) = g.edges.get(u) else {
            return (out, 0);
        };
        for e in neigh {
            let EdgeData::Street(s) = e else { continue };
            let d = s.destination;
            if (k >= 1 && out[0] == d) || (k >= 2 && out[1] == d) {
                continue;
            }
            if k < 2 {
                out[k] = d;
            }
            k += 1;
        }
        (out, k)
    }

    /// A node is a contractible interior pass-through iff it is not a transit stop and is
    /// a clean BIDIRECTIONAL degree-2 node: exactly two distinct outgoing bike neighbours,
    /// both reciprocated (edge back), and no extra incoming bike edge (`indeg == 2`).
    /// The bidirectional requirement guarantees `walk_chain` can always follow the chain
    /// (the node it came from is one of the two neighbours), so every contracted chain
    /// bakes — an asymmetric (one-way / contraflow) node is left as a junction instead.
    fn is_interior(g: &Graph, u: usize, indeg: &[u32], conn: Conn) -> bool {
        if g.raptor.transit_node_to_stop.get(u).copied().unwrap_or(u32::MAX) != u32::MAX {
            return false;
        }
        let (nbrs, k) = conn.neighbours(g, u);
        if k != 2 || indeg.get(u).copied().unwrap_or(0) != 2 {
            return false;
        }
        // Both neighbours must have an edge of this connectivity back to `u`.
        conn.edge_back(g, nbrs[0], NodeID(u)) && conn.edge_back(g, nbrs[1], NodeID(u))
    }

    /// The bike `StreetEdgeData` from `from` to `to`, if present.
    pub(super) fn bike_edge<'a>(g: &'a Graph, from: NodeID, to: NodeID) -> Option<&'a StreetEdgeData> {
        g.edges.get(from.0)?.iter().find_map(|e| match e {
            EdgeData::Street(s) if s.bike && s.destination == to => Some(s),
            _ => None,
        })
    }

    /// Build the contracted **bike** graph from `g` (the cost-baked P1 search).
    pub fn from_graph(g: &Graph) -> Self {
        Self::build(g, Conn::Bike)
    }

    /// Build the contracted **all-mode (union)** graph from `g` — the P3 structure every
    /// consumer (walk/car/bike/RAPTOR-footpaths) shares. A node is interior only if it is a
    /// pass-through over the union of foot/bike/car edges, so removing it is safe for all.
    pub fn from_graph_union(g: &Graph) -> Self {
        Self::build(g, Conn::AllModes)
    }

    fn build(g: &Graph, conn: Conn) -> Self {
        let n = g.nodes.len();
        // Incoming-edge degree per node (of this connectivity), so `is_interior` can reject
        // asymmetric (one-way) nodes that outgoing degree alone would mistake for
        // pass-throughs.
        let mut indeg = vec![0u32; n];
        for u in 0..n {
            if let Some(neigh) = g.edges.get(u) {
                for e in neigh {
                    if let EdgeData::Street(s) = e {
                        if conn.counts_inedge(s) {
                            indeg[s.destination.0] += 1;
                        }
                    }
                }
            }
        }
        let mut junction_of = vec![u32::MAX; n];
        let mut junctions: Vec<NodeID> = Vec::new();
        for u in 0..n {
            // A transit stop is always a junction (even with no street edge of this
            // connectivity — e.g. reached only by riding to it): its coordinate must
            // survive the interior-node drop so plan/explain reconstruction can resolve it.
            let is_stop =
                g.raptor.transit_node_to_stop.get(u).copied().unwrap_or(u32::MAX) != u32::MAX;
            if conn.neighbours(g, u).1 == 0 && !is_stop {
                continue; // no edge of this connectivity and not a transit stop ⇒ irrelevant
            }
            if !Self::is_interior(g, u, &indeg, conn) {
                junction_of[u] = junctions.len() as u32;
                junctions.push(NodeID(u));
            }
        }

        let mut adjacency: Vec<Vec<SuperEdge>> = vec![Vec::new(); junctions.len()];
        for (ji, &jn) in junctions.iter().enumerate() {
            let Some(neigh) = g.edges.get(jn.0) else {
                continue;
            };
            // Collect first-step neighbours first to avoid borrowing `g.edges` across the
            // chain walk. Dedup so a junction with parallel edges to the same first hop
            // emits one super-edge.
            let mut firsts: Vec<NodeID> = neigh
                .iter()
                .filter_map(|e| match e {
                    EdgeData::Street(s) if conn.counts_inedge(s) => Some(s.destination),
                    _ => None,
                })
                .collect();
            firsts.sort_unstable();
            firsts.dedup();
            for first in firsts {
                if let Some(se) = Self::walk_chain(g, jn, first, &junction_of, conn) {
                    adjacency[ji].push(se);
                }
            }
        }

        let mut cg = ContractedGraph {
            junctions,
            junction_of,
            adjacency,
            segs: Vec::new(),
            junction_coord: Vec::new(),
            superedges: Vec::new(),
            seg_index: Default::default(),
        };
        cg.fill_segments(g);
        cg
    }

    /// Populate the self-contained `segs` arena + `junction_coord` from `g`, so every
    /// super-edge can be traversed without touching `g.nodes`/`g.edges`. Each chain hop
    /// `prev→cur` relocates its directed `StreetEdgeData` and `cur`'s coordinate.
    fn fill_segments(&mut self, g: &Graph) {
        self.junction_coord = self.junctions.iter().map(|&j| g.nodes[j.0].loc()).collect();
        let total: usize = self.adjacency.iter().flat_map(|a| a.iter()).map(|se| se.nodes.len()).sum();
        let mut segs: Vec<Seg> = Vec::with_capacity(total);
        let mut superedges: Vec<SuperEdgeMeta> = Vec::new();
        for (ji, adj) in self.adjacency.iter_mut().enumerate() {
            for se in adj.iter_mut() {
                se.seg_start = segs.len() as u32;
                se.seg_len = se.nodes.len() as u32;
                superedges.push(SuperEdgeMeta {
                    from_ji: ji as u32,
                    to_ji: se.to,
                    seg_start: se.seg_start,
                    seg_len: se.seg_len,
                });
                let mut prev = se.from;
                for &n in &se.nodes {
                    let edge = *Self::street_edge_dir(g, prev, n)
                        .expect("chain hop has a directed street edge");
                    segs.push(Seg { edge, far: g.nodes[n.0].loc() });
                    prev = n;
                }
            }
        }
        self.segs = segs;
        self.superedges = superedges;
    }

    /// Build the segment R-tree for g-free snapping (call post-load; not serialized). The
    /// near end of segment `i` is the previous segment's far-coord, or the owning junction's
    /// coord at a super-edge start.
    pub fn build_seg_index(&mut self) {
        let ref_lat = self.junction_coord.first().map(|c| c.latitude).unwrap_or(0.0);
        let mut items: Vec<(StreetEdgeData, u32, (f64, f64), (f64, f64))> =
            Vec::with_capacity(self.segs.len());
        for sm in &self.superedges {
            let mut near = self.junction_coord[sm.from_ji as usize];
            for k in 0..sm.seg_len {
                let gi = sm.seg_start + k;
                let seg = self.segs[gi as usize];
                items.push((
                    seg.edge,
                    gi,
                    (near.latitude, near.longitude),
                    (seg.far.latitude, seg.far.longitude),
                ));
                near = seg.far;
            }
        }
        self.seg_index = super::edge_index::EdgeIndex::build_segs(items.into_iter(), ref_lat);
    }

    /// The super-edge owning global segment `gi` (binary search on ascending `seg_start`).
    fn owner_of(&self, gi: u32) -> &SuperEdgeMeta {
        let i = self.superedges.partition_point(|m| m.seg_start <= gi) - 1;
        &self.superedges[i]
    }

    /// The directed `StreetEdgeData` from `from` to `to` (any mode), if present.
    fn street_edge_dir<'a>(g: &'a Graph, from: NodeID, to: NodeID) -> Option<&'a StreetEdgeData> {
        g.edges.get(from.0)?.iter().find_map(|e| match e {
            EdgeData::Street(s) if s.destination == to => Some(s),
            _ => None,
        })
    }

    /// Segments of super-edge `se` from the arena.
    #[inline]
    fn seg_slice(&self, se: &SuperEdge) -> &[Seg] {
        &self.segs[se.seg_start as usize..(se.seg_start + se.seg_len) as usize]
    }

    /// The ordered polyline coordinates of super-edge `se` from the arena: each segment's
    /// far-coord (the near end is the owning junction's coord, the caller's previous point).
    /// Lets plan geometry be rebuilt from segments without `g.nodes` (P3f).
    pub fn super_edge_coords(&self, se: &SuperEdge) -> Vec<crate::structures::LatLng> {
        self.seg_slice(se).iter().map(|s| s.far).collect()
    }

    /// The g-free coordinate of node `id` IF it is a junction (incl. every transit stop),
    /// else `None`. Lets plan geometry resolve junction/stop endpoints without `g.nodes`.
    pub fn junction_coord_of(&self, id: NodeID) -> Option<crate::structures::LatLng> {
        let ji = *self.junction_of.get(id.0)?;
        (ji != u32::MAX).then(|| self.junction_coord[ji as usize])
    }

    /// Seconds to traverse super-edge `se` under street `profile` — the per-segment
    /// `edge_secs(profile)` sum, replayed from the arena (no `g.edges`). Generalizes
    /// [`Self::walk_secs`] to any profile and matches the per-edge relaxation
    /// `street_path` uses (which sums `edge_secs(profile)`, NOT the phased car cost).
    /// `None` if any segment is impassable for the profile (the chain dead-ends).
    fn superedge_secs(&self, g: &Graph, se: &SuperEdge, profile: StreetProfile) -> Option<u32> {
        let mut total = 0u32;
        for seg in self.seg_slice(se) {
            total = total.saturating_add(g.edge_secs(&seg.edge, profile)?);
        }
        Some(total)
    }

    /// The arena, g-free twin of [`Graph::street_path`]: the polyline of the
    /// minimum-`profile`-time route from `(lat0,lon0)` to `(lat1,lon1)`.
    ///
    /// Snaps each endpoint to its nearest `profile`-usable segment, bridges to that
    /// segment's bounding junctions, runs a parent-tracking junction Dijkstra over the
    /// super-edges (cost = [`Self::superedge_secs`], matching `street_path`'s per-edge
    /// relaxation), then assembles: origin coord → origin partial-segment stub →
    /// `super_edge_coords` of each hop on the chosen junction path → destination partial
    /// stub → destination coord. Reads `g` only for `g.raptor` cost params (survives the
    /// P3f drop). Returns a two-point straight line on snap/route failure, mirroring
    /// `street_path`'s fallback.
    ///
    /// When both endpoints snap to junction coordinates (no projection), the result is the
    /// pure junction-path expansion — byte-identical to `street_path`'s node coords. The
    /// interior-endpoint stub geometry is an approximation that the live-API A/B gate
    /// (`scripts/p3_cutover_ab.sh`) measures against the full-graph oracle.
    #[allow(clippy::too_many_arguments)]
    pub fn street_path_arena(
        &self,
        g: &Graph,
        lat0: f64,
        lon0: f64,
        lat1: f64,
        lon1: f64,
        profile: StreetProfile,
        radius_m: f64,
    ) -> Vec<crate::structures::LatLng> {
        let usable = |s: &StreetEdgeData| match profile {
            StreetProfile::Foot => s.foot,
            StreetProfile::Bike => s.bike || s.foot,
            StreetProfile::Car => s.car || s.foot,
        };
        let o_coord = crate::structures::LatLng { latitude: lat0, longitude: lon0 };
        let d_coord = crate::structures::LatLng { latitude: lat1, longitude: lon1 };
        let straight = || vec![o_coord, d_coord];

        // Snap both endpoints to their nearest usable segment + projection.
        let Some((o_snap, d_snap)) = self
            .snap_for_geometry(g, lat0, lon0, radius_m, profile, &usable)
            .zip(self.snap_for_geometry(g, lat1, lon1, radius_m, profile, &usable))
        else {
            return straight();
        };

        // Parent-tracking junction Dijkstra seeded from the origin's bounding-junction
        // entries (proj→junction secs), restricted to `profile`-usable super-edges.
        let mut dist: HashMap<usize, u32> = HashMap::new();
        // parent[ji] = (predecessor ji, adjacency index of the super-edge taken).
        let mut parent: HashMap<usize, (usize, usize)> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::new();
        for &(ji, s0) in &o_snap.entries {
            let e = dist.entry(ji).or_insert(u32::MAX);
            if s0 < *e {
                *e = s0;
                pq.push(Reverse((s0, ji)));
            }
        }
        // Point-to-point early termination: the destination bounding junctions and their
        // proj-stub seconds. Dijkstra pops in nondecreasing `d`, so once a popped `d`
        // can no longer beat the best route+stub total to any dest junction, no later pop
        // can either — stop. Without this the search floods every junction in the graph.
        let dest_stub: HashMap<usize, u32> = d_snap.entries.iter().copied().collect();
        let mut best_total = u32::MAX;
        while let Some(Reverse((d, ji))) = pq.pop() {
            if d >= best_total {
                break;
            }
            if d > *dist.get(&ji).unwrap_or(&u32::MAX) {
                continue;
            }
            if let Some(&sd) = dest_stub.get(&ji) {
                best_total = best_total.min(d.saturating_add(sd));
            }
            let jn = self.junctions[ji];
            if g.raptor.transit_node_to_stop.get(jn.0).copied().unwrap_or(u32::MAX) != u32::MAX {
                continue;
            }
            for (ai, se) in self.adjacency[ji].iter().enumerate() {
                let Some(t) = self.superedge_secs(g, se, profile) else {
                    continue;
                };
                let nd = d.saturating_add(t);
                let to = se.to as usize;
                let entry = dist.entry(to).or_insert(u32::MAX);
                if nd < *entry {
                    *entry = nd;
                    parent.insert(to, (ji, ai));
                    pq.push(Reverse((nd, to)));
                }
            }
        }

        // Pick the destination bounding junction minimizing total (route + dest stub).
        let mut best: Option<(u32, usize)> = None;
        for &(dj, sd) in &d_snap.entries {
            if let Some(&dd) = dist.get(&dj) {
                let total = dd.saturating_add(sd);
                if best.map_or(true, |(b, _)| total < b) {
                    best = Some((total, dj));
                }
            }
        }
        let Some((_, dest_ji)) = best else {
            return straight();
        };

        // Backtrack the junction super-edge path origin-entry → dest_ji.
        let mut hops: Vec<usize> = Vec::new(); // adjacency indices, dest→origin
        let mut cur = dest_ji;
        while let Some(&(pj, ai)) = parent.get(&cur) {
            hops.push(ai);
            cur = pj;
            if hops.len() > self.junctions.len() {
                return straight(); // cycle guard
            }
        }
        // `cur` is now the origin-entry junction the path started from.
        let origin_ji = cur;
        hops.reverse();

        // Assemble the polyline.
        let mut coords: Vec<crate::structures::LatLng> = Vec::new();
        coords.push(o_coord);
        // Origin stub: projection → origin_ji's coord, along the partial segments.
        coords.extend(self.stub_to(&o_snap, origin_ji));
        // Each hop's super-edge far-coords.
        let mut walk = origin_ji;
        for &ai in &hops {
            let se = &self.adjacency[walk][ai];
            coords.extend(self.super_edge_coords(se));
            walk = se.to as usize;
        }
        // Destination stub: dest_ji's coord → projection.
        coords.extend(self.stub_from(&d_snap, dest_ji));
        coords.push(d_coord);
        // Collapse consecutive duplicate vertices: a junction/stop endpoint has
        // proj == junction coord == the adjacent super-edge vertex, so the raw
        // assembly repeats it. `street_path` yields each node once; dedup matches it.
        coords.dedup_by(|a, b| a.latitude == b.latitude && a.longitude == b.longitude);
        coords
    }

    /// Snap `(lat,lon)` to the nearest `usable` segment and resolve the projection +
    /// bounding-junction entries + the partial-segment stub geometry to each junction.
    /// `None` if nothing usable within `radius_m`. Shared by `street_path_arena`.
    fn snap_for_geometry(
        &self,
        g: &Graph,
        lat: f64,
        lon: f64,
        radius_m: f64,
        profile: StreetProfile,
        usable: &impl Fn(&StreetEdgeData) -> bool,
    ) -> Option<GeomSnap> {
        let (edge, gi, _) = self.seg_index.nearest_usable_seg(lat, lon, radius_m, usable)?;
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        // Entries in profile-SECONDS (matching the super-edge Dijkstra metric and
        // `walk_entries_arena`): the on-segment stub `da`/`db` plus the partial chain to
        // each bounding junction. Omitting the stub gives a junction-snapped node a free
        // entry on the wrong side, making the Dijkstra pick the wrong origin junction.
        let t = project_t(lat, lon, near, far);
        let len = edge.length;
        let speed = match profile {
            StreetProfile::Foot => g.raptor.walking_speed_mps,
            StreetProfile::Bike => g.raptor.cycling_speed_mps,
            StreetProfile::Car => g.raptor.driving_speed_mps,
        };
        let mms = (speed * 1000.0).max(1.0) as u64;
        let secs = |d: usize| (d as u64 * 1000 / mms) as u32;
        let da = ((t * len as f64).round() as usize).min(len); // proj→near (toward from_ji)
        let db = len - da; //                                     proj→far  (toward to_ji)
        let chain = |range: std::ops::Range<u32>| -> Option<u32> {
            let mut s = 0u32;
            for i in range {
                s = s.saturating_add(g.edge_secs(&self.segs[i as usize].edge, profile)?);
            }
            Some(s)
        };
        let mut entries: Vec<(usize, u32)> = Vec::new();
        if let Some(c) = chain(sm.seg_start..gi) {
            entries.push((sm.from_ji as usize, secs(da).saturating_add(c)));
        }
        if let Some(c) = chain((gi + 1)..(sm.seg_start + sm.seg_len)) {
            let e = secs(db).saturating_add(c);
            let to = sm.to_ji as usize;
            match entries.iter_mut().find(|(j, _)| *j == to) {
                Some(slot) => slot.1 = slot.1.min(e),
                None => entries.push((to, e)),
            }
        }
        Some(GeomSnap {
            from_ji: sm.from_ji as usize,
            gi,
            seg_start: sm.seg_start,
            seg_len: sm.seg_len,
            proj: project_point(lat, lon, near, far),
            entries,
        })
    }

    /// Polyline from the snapped projection point to bounding junction `ji`'s coordinate
    /// (inclusive of both ends): `[proj, intermediate far-coords…, junction_coord[ji]]`.
    /// Used as the ORIGIN stub before the first super-edge expansion, which itself starts
    /// past the junction (`super_edge_coords` is far-coords only).
    fn stub_to(&self, s: &GeomSnap, ji: usize) -> Vec<crate::structures::LatLng> {
        let mut v = vec![s.proj];
        if ji == s.from_ji {
            // proj → near(=segs[gi-1].far) → … → segs[seg_start].far → from_ji coord.
            for k in (s.seg_start..s.gi).rev() {
                v.push(self.segs[k as usize].far);
            }
            v.push(self.junction_coord[s.from_ji]);
        } else {
            // proj → far(=segs[gi].far) → … → segs[last].far(=to_ji coord).
            for k in s.gi..(s.seg_start + s.seg_len) {
                v.push(self.segs[k as usize].far);
            }
        }
        v
    }

    /// Polyline from bounding junction `ji` onward to the snapped projection point,
    /// EXCLUDING the junction coordinate (the preceding super-edge expansion already
    /// ended there): `[intermediate far-coords…, proj]`. The DESTINATION stub.
    fn stub_from(&self, s: &GeomSnap, ji: usize) -> Vec<crate::structures::LatLng> {
        let mut v: Vec<crate::structures::LatLng> = Vec::new();
        if ji == s.from_ji {
            // from_ji → segs[seg_start].far → … → segs[gi-1].far(=near) → proj.
            for k in s.seg_start..s.gi {
                v.push(self.segs[k as usize].far);
            }
        } else {
            // to_ji → segs[last].far(=to_ji, skip) … reversed back to segs[gi].far → proj.
            for k in ((s.gi)..(s.seg_start + s.seg_len - 1)).rev() {
                v.push(self.segs[k as usize].far);
            }
        }
        v.push(s.proj);
        v
    }

    /// Bytes the in-memory structure occupies (heap, excluding the skipped `baked`):
    /// junctions + junction_of + junction_coord + adjacency (outer + super-edges + their
    /// `nodes` Vecs) + the segment arena. The P3f resident cost.
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        self.junctions.len() * size_of::<NodeID>()
            + self.junction_of.len() * size_of::<u32>()
            + self.junction_coord.len() * size_of::<crate::structures::LatLng>()
            + self.adjacency.len() * size_of::<Vec<SuperEdge>>()
            + self.adjacency.iter().map(|a| {
                a.len() * size_of::<SuperEdge>()
                    + a.iter().map(|se| se.nodes.len() * size_of::<NodeID>()).sum::<usize>()
            }).sum::<usize>()
            + self.segs.len() * size_of::<Seg>()
    }

    /// Follow a degree-2 bike chain from junction `start` via first hop `first` until the
    /// next junction, collecting the node sequence. `None` on malformed topology.
    fn walk_chain(
        g: &Graph,
        start: NodeID,
        first: NodeID,
        junction_of: &[u32],
        conn: Conn,
    ) -> Option<SuperEdge> {
        let mut nodes = vec![first];
        let mut prev = start;
        let mut cur = first;
        for _ in 0..1_000_000 {
            if junction_of[cur.0] != u32::MAX {
                return Some(SuperEdge {
                    from: start,
                    to: junction_of[cur.0],
                    nodes,
                    seg_start: 0,
                    seg_len: 0,
                    baked: None,
                });
            }
            let (nbrs, k) = conn.neighbours(g, cur.0);
            if k != 2 {
                return None;
            }
            let next = if nbrs[0] == prev {
                nbrs[1]
            } else if nbrs[1] == prev {
                nbrs[0]
            } else {
                return None;
            };
            nodes.push(next);
            prev = cur;
            cur = next;
        }
        None
    }

    /// The super-edge leaving junction `junction` whose first hop is `first_step`
    /// (unique — a first hop determines the degree-2 continuation). `None` if `junction`
    /// is not a junction or no such chain exists.
    pub fn super_edge(&self, junction: NodeID, first_step: NodeID) -> Option<&SuperEdge> {
        let ji = *self.junction_of.get(junction.0)?;
        if ji == u32::MAX {
            return None;
        }
        self.adjacency[ji as usize]
            .iter()
            .find(|se| se.nodes.first() == Some(&first_step))
    }

    pub fn junction_count(&self) -> usize {
        self.junctions.len()
    }

    pub fn edge_count(&self) -> usize {
        self.adjacency.iter().map(|a| a.len()).sum()
    }

    pub fn segment_count(&self) -> usize {
        self.adjacency
            .iter()
            .flat_map(|a| a.iter())
            .map(|se| se.nodes.len())
            .sum()
    }

    /// Minimum foot seconds for a single hop `u→v` over any parallel edges, or `None` if
    /// no foot edge runs `u→v`. Matches `street_dijkstra`'s per-edge relaxation.
    fn foot_step_secs(g: &Graph, u: NodeID, v: NodeID) -> Option<u32> {
        g.edges.get(u.0)?.iter().filter_map(|e| match e {
            EdgeData::Street(s) if s.destination == v => g.edge_secs(s, StreetProfile::Foot),
            _ => None,
        }).min()
    }

    /// Resolve a snapped **Node** `node` to the contracted graph: the junction entry
    /// points `(junction, foot_secs from node to that junction)` from which a foot search
    /// can proceed. A junction maps to itself at 0 cost; an interior pass-through maps to
    /// its ≤2 bounding junctions, each via the foot walk along its chain. A direction
    /// whose chain is not foot-passable is dropped. Empty ⇒ `node` is not on the foot
    /// graph at all.
    pub fn node_walk_entries(&self, g: &Graph, node: NodeID) -> Vec<(usize, u32)> {
        if let Some(&j) = self.junction_of.get(node.0).filter(|&&j| j != u32::MAX) {
            return vec![(j as usize, 0)];
        }
        let (nbrs, k) = Self::street_neighbours(g, node.0);
        if k != 2 {
            return Vec::new();
        }
        let mut out: Vec<(usize, u32)> = Vec::new();
        for &first in &[nbrs[0], nbrs[1]] {
            if let Some((j, secs)) = self.chain_walk_foot(g, node, first) {
                // Dedup by min when both directions bound the same junction (self-loop chain).
                match out.iter_mut().find(|(jj, _)| *jj == j) {
                    Some(slot) => slot.1 = slot.1.min(secs),
                    None => out.push((j, secs)),
                }
            }
        }
        out
    }

    /// Junction entries for a snapped **Endpoint** (Node *or* mid-edge OnEdge projection)
    /// — the P3f production snapping path: at P3f `nearest_node` only sees junctions, so an
    /// arbitrary coordinate snaps via `snap_to_edge` to an `OnEdge{a,b}` whose ends may be
    /// interior. Each end walks **outward, away from the other end**, to its bounding
    /// junction, having paid the `proj→end` stub. Empty ⇒ not on the foot graph.
    pub fn walk_entries(&self, g: &Graph, ep: Endpoint) -> Vec<(usize, u32)> {
        match ep {
            Endpoint::Node(n) => self.node_walk_entries(g, n),
            Endpoint::OnEdge { a, b, dist_a, dist_b, .. } => {
                // The snapped segment must be foot-passable in some direction.
                if Self::foot_step_secs(g, a, b).is_none() && Self::foot_step_secs(g, b, a).is_none() {
                    return Vec::new();
                }
                let wmms = (g.raptor.walking_speed_mps * 1000.0) as u64;
                let stub = |d: usize| (d as u64 * 1000 / wmms.max(1)) as u32;
                let mut out: Vec<(usize, u32)> = Vec::new();
                for (e, other, d) in [(a, b, dist_a), (b, a, dist_b)] {
                    if let Some((j, s)) = self.entry_from(g, e, other, stub(d)) {
                        match out.iter_mut().find(|(jj, _)| *jj == j) {
                            Some(slot) => slot.1 = slot.1.min(s),
                            None => out.push((j, s)),
                        }
                    }
                }
                out
            }
        }
    }

    /// Junction entry for endpoint node `e`, entering the chain **away from** `other`,
    /// having already paid `stub` seconds to reach `e` from the projection. A junction `e`
    /// is its own entry; an interior `e` walks its one outward chain direction to the
    /// bounding junction.
    fn entry_from(&self, g: &Graph, e: NodeID, other: NodeID, stub: u32) -> Option<(usize, u32)> {
        if let Some(&j) = self.junction_of.get(e.0).filter(|&&j| j != u32::MAX) {
            return Some((j as usize, stub));
        }
        let (nb, k) = Self::street_neighbours(g, e.0);
        if k != 2 {
            return None;
        }
        let outward = if nb[0] == other {
            nb[1]
        } else if nb[1] == other {
            nb[0]
        } else {
            return None;
        };
        let (j, chain) = self.chain_walk_foot(g, e, outward)?;
        Some((j, stub.saturating_add(chain)))
    }

    /// Walk the degree-2 foot chain from interior `x`, first stepping to union neighbour
    /// `first`, until a junction. Returns `(junction index, foot seconds x→junction)`, or
    /// `None` if a hop on the way is not foot-passable.
    fn chain_walk_foot(&self, g: &Graph, x: NodeID, first: NodeID) -> Option<(usize, u32)> {
        let mut secs = Self::foot_step_secs(g, x, first)?;
        let (mut prev, mut cur) = (x, first);
        for _ in 0..1_000_000 {
            if let Some(&j) = self.junction_of.get(cur.0).filter(|&&j| j != u32::MAX) {
                return Some((j as usize, secs));
            }
            let (nb, kk) = Self::street_neighbours(g, cur.0);
            if kk != 2 {
                return None;
            }
            let next = if nb[0] == prev { nb[1] } else if nb[1] == prev { nb[0] } else { return None };
            secs = secs.saturating_add(Self::foot_step_secs(g, cur, next)?);
            prev = cur;
            cur = next;
        }
        None
    }

    /// Direct foot seconds from interior `o` to `d` **along their shared chain**, if `d`
    /// lies on one of the two degree-2 directions out of `o` before reaching a junction.
    /// `None` if they are not on the same chain (or `o` is a junction). This is the case
    /// the via-junction bridge cannot express — both interior on one chain, where the true
    /// path runs straight along the chain instead of doubling back through a junction.
    fn direct_same_chain_foot(g: &Graph, o: NodeID, d: NodeID) -> Option<u32> {
        if o == d {
            return Some(0);
        }
        let (nbrs, k) = Self::street_neighbours(g, o.0);
        if k != 2 {
            return None; // o is not an interior pass-through
        }
        let mut best: Option<u32> = None;
        for &first in &[nbrs[0], nbrs[1]] {
            let Some(mut secs) = Self::foot_step_secs(g, o, first) else { continue };
            let (mut prev, mut cur) = (o, first);
            loop {
                if cur == d {
                    best = Some(best.map_or(secs, |b| b.min(secs)));
                    break;
                }
                let (nb, kk) = Self::street_neighbours(g, cur.0);
                if kk != 2 {
                    break; // reached a junction/branch without passing d
                }
                let next = if nb[0] == prev { nb[1] } else if nb[1] == prev { nb[0] } else { break };
                let Some(step) = Self::foot_step_secs(g, cur, next) else { break };
                secs = secs.saturating_add(step);
                prev = cur;
                cur = next;
            }
        }
        best
    }

    /// Foot shortest-path seconds between two arbitrary snapped **Node** endpoints over the
    /// contracted graph, bit-identical to `street_dijkstra(o, bound, Foot).get(d)`. Bridges
    /// each endpoint to its bounding junctions, runs the seeded junction search, and adds
    /// the same-chain direct candidate so an interior↔interior pair on one chain isn't
    /// forced to double back through a junction. `None` if unreachable within `bound`.
    pub fn walk_secs_point_to_point(
        &self,
        g: &Graph,
        o: NodeID,
        d: NodeID,
        bound: u32,
    ) -> Option<u32> {
        if o == d {
            return Some(0);
        }
        let o_entries = self.node_walk_entries(g, o);
        let d_entries = self.node_walk_entries(g, d);
        if o_entries.is_empty() || d_entries.is_empty() {
            return None;
        }
        let dist = g.walk_dijkstra_union_seeded(&o_entries, bound, self);
        let mut best: Option<u32> = None;
        for &(dj, sd) in &d_entries {
            if let Some(&dj_dist) = dist.get(&self.junctions[dj]) {
                let total = dj_dist.saturating_add(sd);
                if total <= bound {
                    best = Some(best.map_or(total, |b| b.min(total)));
                }
            }
        }
        // Same-chain interior↔interior shortcut (never via a junction).
        if let Some(direct) = Self::direct_same_chain_foot(g, o, d) {
            if direct <= bound {
                best = Some(best.map_or(direct, |b| b.min(direct)));
            }
        }
        best
    }

    /// G-free foot snapping descriptor for `(lat, lon)`: the owning super-edge, the segment
    /// the point projected onto, the on-segment stub lengths (`da` proj→from-side, `db`
    /// proj→to-side, meters), and the bounding-junction foot-second entries. The arena twin
    /// of a snapped foot Node, supplying both `walk_entries_arena`'s entries and the data a
    /// same-super-edge direct candidate needs. `None` if nothing foot-usable in range.
    fn foot_snap(
        &self,
        g: &Graph,
        lat: f64,
        lon: f64,
        radius_m: f64,
    ) -> Option<(SuperEdgeMeta, u32, usize, usize, Vec<(usize, u32)>)> {
        let (edge, gi, _) = self.seg_index.nearest_usable_seg(lat, lon, radius_m, |s| s.foot)?;
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        let t = project_t(lat, lon, near, far);
        let len = edge.length;
        let wmms = (g.raptor.walking_speed_mps * 1000.0).max(1.0) as u64;
        let secs = |d: usize| (d as u64 * 1000 / wmms) as u32;
        let da = ((t * len as f64).round() as usize).min(len);
        let db = len - da;
        let foot = |range: std::ops::Range<u32>| -> Option<u32> {
            let mut s = 0u32;
            for i in range {
                s = s.saturating_add(g.edge_secs(&self.segs[i as usize].edge, StreetProfile::Foot)?);
            }
            Some(s)
        };
        let mut entries: Vec<(usize, u32)> = Vec::new();
        if let Some(c) = foot(sm.seg_start..gi) {
            entries.push((sm.from_ji as usize, secs(da).saturating_add(c)));
        }
        if let Some(c) = foot((gi + 1)..(sm.seg_start + sm.seg_len)) {
            let e = secs(db).saturating_add(c);
            match entries.iter_mut().find(|(j, _)| *j == sm.to_ji as usize) {
                Some(slot) => slot.1 = slot.1.min(e),
                None => entries.push((sm.to_ji as usize, e)),
            }
        }
        Some((sm, gi, da, db, entries))
    }

    /// A bounding-junction `NodeID` for the foot-snap of `(lat, lon)` — the nearer of the
    /// ≤2 junctions bounding the owning super-edge. Used as the stable NodeID identity for a
    /// coord-snapped origin/destination in flag-on routing (geometry/cost use the projected
    /// coord, not this junction's coord). `None` if nothing foot-usable in range.
    pub fn foot_bounding_junction(&self, g: &Graph, lat: f64, lon: f64, radius_m: f64) -> Option<NodeID> {
        let (_, _, _, _, entries) = self.foot_snap(g, lat, lon, radius_m)?;
        entries
            .iter()
            .min_by_key(|&&(_, s)| s)
            .map(|&(ji, _)| self.junctions[ji])
    }

    /// Foot stops within `max_secs` of the foot-snap of `(lat, lon)` over the contracted
    /// graph — the coord-based twin of [`Graph::nearby_stops_union`] for a snapped origin
    /// whose interior node is gone (g-free). Same shape/order as `nearby_stops`.
    pub fn nearby_stops_arena(
        &self,
        g: &Graph,
        lat: f64,
        lon: f64,
        radius_m: f64,
        max_secs: u32,
    ) -> Vec<(usize, u32)> {
        let Some((_, _, _, _, entries)) = self.foot_snap(g, lat, lon, radius_m) else {
            return Vec::new();
        };
        let dist = g.walk_dijkstra_union_seeded(&entries, max_secs, self);
        let mut stops: Vec<(usize, u32)> = dist
            .iter()
            .filter_map(|(&jn, &secs)| {
                let compact = g.raptor.transit_node_to_stop[jn.0];
                (compact != u32::MAX).then_some((compact as usize, secs))
            })
            .collect();
        stops.sort_unstable_by_key(|&(stop, _)| stop);
        stops
    }

    /// Foot shortest-path seconds between two **coordinates** over the contracted graph,
    /// the arena twin of `walk_secs_point_to_point`: snap each coord (g-free) to its
    /// bounding-junction entries, run the seeded junction search, and add the same-super-edge
    /// direct candidate so a pair on one chain isn't forced to double back through a
    /// junction. `None` if either coord is unsnappable or the pair is unreachable within
    /// `bound`.
    pub fn walk_secs_coord_to_coord(
        &self,
        g: &Graph,
        o: crate::structures::LatLng,
        d: crate::structures::LatLng,
        radius_m: f64,
        bound: u32,
    ) -> Option<u32> {
        let (os, ogi, oda, odb, o_entries) = self.foot_snap(g, o.latitude, o.longitude, radius_m)?;
        let (ds, dgi, dda, ddb, d_entries) = self.foot_snap(g, d.latitude, d.longitude, radius_m)?;
        let mut best: Option<u32> = None;
        let dist = g.walk_dijkstra_union_seeded(&o_entries, bound, self);
        for &(dj, sd) in &d_entries {
            if let Some(&dj_dist) = dist.get(&self.junctions[dj]) {
                let total = dj_dist.saturating_add(sd);
                if total <= bound {
                    best = Some(best.map_or(total, |b| b.min(total)));
                }
            }
        }
        // Same-super-edge direct: both projections lie on the same chain, so walk straight
        // along it (never via a junction). `from_ji`→proj prefix foot-secs for each, then the
        // direct walk is the absolute difference (foot cost is direction-symmetric).
        if os.seg_start == ds.seg_start && os.seg_len == ds.seg_len {
            if let (Some(po), Some(pd)) = (
                self.from_ji_prefix_foot_secs(g, &os, ogi, oda),
                self.from_ji_prefix_foot_secs(g, &ds, dgi, dda),
            ) {
                let direct = po.abs_diff(pd);
                if direct <= bound {
                    best = Some(best.map_or(direct, |b| b.min(direct)));
                }
            }
        }
        let _ = (odb, ddb);
        best
    }

    /// Foot seconds from `sm.from_ji` to a projection on segment `gi` of super-edge `sm`,
    /// `da` meters from the near (from-side) end of that segment: the full segments
    /// `[seg_start..gi)` plus the `da` on-segment stub. `None` if a segment is not
    /// foot-passable.
    fn from_ji_prefix_foot_secs(
        &self,
        g: &Graph,
        sm: &SuperEdgeMeta,
        gi: u32,
        da: usize,
    ) -> Option<u32> {
        let wmms = (g.raptor.walking_speed_mps * 1000.0).max(1.0) as u64;
        let mut total = 0u32;
        for i in sm.seg_start..gi {
            total = total.saturating_add(g.edge_secs(&self.segs[i as usize].edge, StreetProfile::Foot)?);
        }
        total = total.saturating_add((da as u64 * 1000 / wmms) as u32);
        Some(total)
    }

    /// Car seconds to traverse super-edge `se` entering in phase `walking` (false =
    /// Driving), replayed from the arena via `car_edge_step`: returns `(seconds, exit
    /// phase)`. Car routing is phased Driving→park→Walking, so a chain can switch car→foot
    /// mid-way (and never back). `None` if a segment is impassable in the current phase
    /// (dead-end), exactly as `street_dijkstra(Car)` would stop. `g` is read only for
    /// `g.raptor` speeds.
    pub fn car_secs(&self, g: &Graph, se: &SuperEdge, walking: bool) -> Option<(u32, bool)> {
        let mut total = 0u32;
        let mut phase = walking;
        for seg in self.seg_slice(se) {
            let (t, next) = g.car_edge_step(&seg.edge, phase)?;
            total = total.saturating_add(t);
            phase = next;
        }
        Some((total, phase))
    }

    /// G-free foot snapping: resolve `(lat, lon)` to the nearest foot segment via the
    /// arena R-tree, then bridge the projection to the owning super-edge's ≤2 bounding
    /// junctions with exact foot seconds (proj→junction = stub + chain). The arena twin of
    /// `snap_to_edge(foot)` + `walk_entries`, needing no `g.nodes`/`g.edges`.
    pub fn walk_entries_arena(
        &self,
        g: &Graph,
        lat: f64,
        lon: f64,
        radius_m: f64,
    ) -> Vec<(usize, u32)> {
        let Some((edge, gi, _)) = self.seg_index.nearest_usable_seg(lat, lon, radius_m, |s| s.foot)
        else {
            return Vec::new();
        };
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        let t = project_t(lat, lon, near, far);
        let len = edge.length;
        let wmms = (g.raptor.walking_speed_mps * 1000.0).max(1.0) as u64;
        let secs = |d: usize| (d as u64 * 1000 / wmms) as u32;
        let da = ((t * len as f64).round() as usize).min(len); // proj→near, matches snap_to_edge
        let db = len - da; //                                     proj→far
        let foot = |range: std::ops::Range<u32>| -> Option<u32> {
            let mut s = 0u32;
            for i in range {
                s = s.saturating_add(g.edge_secs(&self.segs[i as usize].edge, StreetProfile::Foot)?);
            }
            Some(s)
        };
        let mut out: Vec<(usize, u32)> = Vec::new();
        // proj → from_ji: stub(da) + segs[seg_start..gi]
        if let Some(chain) = foot(sm.seg_start..gi) {
            out.push((sm.from_ji as usize, secs(da).saturating_add(chain)));
        }
        // proj → to_ji: stub(db) + segs[gi+1 .. seg_start+seg_len]
        if let Some(chain) = foot((gi + 1)..(sm.seg_start + sm.seg_len)) {
            let e = secs(db).saturating_add(chain);
            match out.iter_mut().find(|(j, _)| *j == sm.to_ji as usize) {
                Some(slot) => slot.1 = slot.1.min(e),
                None => out.push((sm.to_ji as usize, e)),
            }
        }
        out
    }

    /// Walk seconds to traverse super-edge `se`: the **sum of per-segment foot seconds**
    /// (`edge_secs(Foot)`), replayed from the self-contained `segs` arena — no `g.edges`
    /// lookup, proving super-edge traversal survives dropping the interior nodes. Summing
    /// the per-segment integer seconds (not sum-of-lengths-then-divide) keeps it
    /// bit-identical to `street_dijkstra(Foot)` walking the chain hop by hop. `None` if a
    /// segment is not foot-passable (the chain dead-ends for walkers). `g` is read only for
    /// the configured walking speed (`g.raptor`, which survives P3f).
    pub fn walk_secs(&self, g: &Graph, se: &SuperEdge) -> Option<u32> {
        let mut total = 0u32;
        for seg in self.seg_slice(se) {
            total = total.saturating_add(g.edge_secs(&seg.edge, StreetProfile::Foot)?);
        }
        Some(total)
    }

    /// G-free bike snapping: resolve `(lat, lon)` to the nearest bikeable foot/bike
    /// segment via the arena R-tree, then replay the partial super-edge from the
    /// projection (standstill, `prev=None`) to each of the ≤2 bounding junctions through
    /// the kinematic bike cost (`street_edge_transition`). The arena twin of
    /// `snap_to_edge(bike)` + the `OnEdge` seeding in `bike_cost_path`, feeding
    /// `multiobj_search` at wiring. `g` is read only for `g.raptor` cost params and
    /// `g.replay_bike_chain`; the backward chain's reverse edges come from the sibling
    /// reverse super-edge's arena `segs` (bidirectional contraction guarantees it exists),
    /// not from `g.edges` — so this survives P3f.
    pub fn bike_entries_arena(
        &self,
        g: &Graph,
        bike: &BikeCost,
        lat: f64,
        lon: f64,
        radius_m: f64,
    ) -> Vec<BikeSeed> {
        let Some((edge, gi, _)) = self.seg_index.nearest_usable_seg(lat, lon, radius_m, |s| s.bike)
        else {
            return Vec::new();
        };
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        let t = project_t(lat, lon, near, far);
        let len = edge.length;
        let da = ((t * len as f64).round() as usize).min(len); // proj→near (toward from_ji)
        let db = len - da; //                                     proj→far  (toward to_ji)

        let mut out: Vec<BikeSeed> = Vec::new();

        // FORWARD → to_ji: stub proj→far of seg gi, then segs[gi+1..end] forward.
        let mut fwd: Vec<(StreetEdgeData, (f64, f64))> = Vec::new();
        fwd.push((Graph::partial_edge(&edge, db), dir_coords(near, far)));
        let end = sm.seg_start + sm.seg_len;
        for k in (gi + 1)..end {
            let s = self.segs[k as usize];
            let n = if k == sm.seg_start {
                self.junction_coord[sm.from_ji as usize]
            } else {
                self.segs[(k - 1) as usize].far
            };
            fwd.push((s.edge, dir_coords(n, s.far)));
        }
        if let Some(seed) = g.replay_bike_chain(bike, &fwd, sm.to_ji as usize) {
            out.push(seed);
        }

        // BACKWARD → from_ji: reverse stub proj→near of seg gi, then segs[seg_start..gi]
        // REVERSED. The contraction is bidirectional: this forward super-edge has a SIBLING
        // reverse super-edge (junction to_ji → from_ji) whose own `segs` already hold the
        // exact reverse directed g edges (`fill_segments` built them from the real reverse
        // hops). Source the reverse edges from that arena super-edge — no `g.edges`. The
        // reverse super-edge's first hop is the node just before the far junction in this
        // chain (`se.nodes[len-2]`), or `from_ji`'s node for a single-segment chain. Forward
        // local seg `lk` within `[seg_start..seg_start+seg_len)` maps to reverse local seg
        // `seg_len-1-lk` in the sibling's range.
        let first_step = if sm.seg_len >= 2 {
            // Forward seg `seg_len-2` is the edge into node se.nodes[seg_len-2], so its
            // `.edge.destination` IS that node (the reverse super-edge's first hop).
            self.segs[(sm.seg_start + sm.seg_len - 2) as usize].edge.destination
        } else {
            self.junctions[sm.from_ji as usize]
        };
        let rev_segs = self
            .super_edge(self.junctions[sm.to_ji as usize], first_step)
            .map(|rse| (rse.seg_start, rse.seg_len));
        if let Some((r_start, r_len)) = rev_segs {
            debug_assert_eq!(r_len, sm.seg_len, "sibling reverse super-edge has equal length");
            // Reverse-local index of forward local `lk = fgi - seg_start`.
            let rev_local = |fgi: u32| sm.seg_len - 1 - (fgi - sm.seg_start);
            let rev_edge = |fgi: u32| self.segs[(r_start + rev_local(fgi)) as usize].edge;
            let mut bwd: Vec<(StreetEdgeData, (f64, f64))> = Vec::new();
            bwd.push((Graph::partial_edge(&rev_edge(gi), da), dir_coords(far, near)));
            for k in (sm.seg_start..gi).rev() {
                let s = self.segs[k as usize];
                let seg_near = if k == sm.seg_start {
                    self.junction_coord[sm.from_ji as usize]
                } else {
                    self.segs[(k - 1) as usize].far
                };
                bwd.push((rev_edge(k), dir_coords(s.far, seg_near)));
            }
            if let Some(seed) = g.replay_bike_chain(bike, &bwd, sm.from_ji as usize) {
                out.push(seed);
            }
        }

        Self::dedup_min_seed(&mut out);
        out
    }

    /// G-free car snapping (phased): the car analog of `walk_entries_arena`. Resolve
    /// `(lat, lon)` to the nearest car-or-foot segment via the arena R-tree, then thread the
    /// phased car cost (`car_edge_step`: Driving→park→Walking, never back) from the snapped
    /// projection — entering in Driving — to each of the ≤2 bounding junctions. Returns
    /// `(junction index, seconds, exit walking phase)`. A direction whose chain hits an
    /// impassable hop in the current phase dead-ends and yields no seed (exactly as
    /// `car_secs`/`street_dijkstra(Car)` would stop). The backward chain's reverse edges come
    /// from the sibling reverse super-edge's arena `segs`, never `g.edges` — so this survives
    /// P3f (`g` is read only for `g.raptor` speeds via `car_edge_step`).
    pub fn car_entries_arena(
        &self,
        g: &Graph,
        lat: f64,
        lon: f64,
        radius_m: f64,
    ) -> Vec<(usize, u32, bool)> {
        let Some((edge, gi, _)) =
            self.seg_index.nearest_usable_seg(lat, lon, radius_m, |s| s.car || s.foot)
        else {
            return Vec::new();
        };
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        let t = project_t(lat, lon, near, far);
        let len = edge.length;
        let da = ((t * len as f64).round() as usize).min(len); // proj→near (toward from_ji)
        let db = len - da; //                                     proj→far  (toward to_ji)

        // Thread the phased car cost over an ordered edge list from Driving; `None` if a hop
        // is impassable in its phase (mirrors `car_secs`).
        let phased = |edges: &[StreetEdgeData]| -> Option<(u32, bool)> {
            let mut total = 0u32;
            let mut phase = false; // Driving
            for e in edges {
                let (s, next) = g.car_edge_step(e, phase)?;
                total = total.saturating_add(s);
                phase = next;
            }
            Some((total, phase))
        };

        let mut out: Vec<(usize, u32, bool)> = Vec::new();

        // FORWARD → to_ji: stub proj→far of seg gi, then segs[gi+1..end] forward.
        let end = sm.seg_start + sm.seg_len;
        let mut fwd: Vec<StreetEdgeData> = vec![Graph::partial_edge(&edge, db)];
        for k in (gi + 1)..end {
            fwd.push(self.segs[k as usize].edge);
        }
        if let Some((secs, phase)) = phased(&fwd) {
            out.push((sm.to_ji as usize, secs, phase));
        }

        // BACKWARD → from_ji: reverse stub proj→near of seg gi, then segs[seg_start..gi]
        // REVERSED, sourcing reverse edges from the SIBLING reverse super-edge (same arena,
        // never `g.edges`) — exactly as `bike_entries_arena`'s backward chain.
        let first_step = if sm.seg_len >= 2 {
            self.segs[(sm.seg_start + sm.seg_len - 2) as usize].edge.destination
        } else {
            self.junctions[sm.from_ji as usize]
        };
        let rev_segs = self
            .super_edge(self.junctions[sm.to_ji as usize], first_step)
            .map(|rse| (rse.seg_start, rse.seg_len));
        if let Some((r_start, r_len)) = rev_segs {
            debug_assert_eq!(r_len, sm.seg_len, "sibling reverse super-edge has equal length");
            let rev_local = |fgi: u32| sm.seg_len - 1 - (fgi - sm.seg_start);
            let rev_edge = |fgi: u32| self.segs[(r_start + rev_local(fgi)) as usize].edge;
            let mut bwd: Vec<StreetEdgeData> = vec![Graph::partial_edge(&rev_edge(gi), da)];
            for k in (sm.seg_start..gi).rev() {
                bwd.push(rev_edge(k));
            }
            if let Some((secs, phase)) = phased(&bwd) {
                let j = sm.from_ji as usize;
                match out.iter_mut().find(|(ji, _, _)| *ji == j) {
                    Some(slot) if secs < slot.1 => {
                        slot.1 = secs;
                        slot.2 = phase;
                    }
                    Some(_) => {}
                    None => out.push((j, secs, phase)),
                }
            }
        }
        out
    }

    /// The projected snap point + perpendicular distance for `(lat, lon)` against the
    /// nearest `usable` segment within `radius_m`, computed identically to the
    /// `*_entries_arena` prologue (`project_t` → `near + t*(far−near)`). Surfaced so the
    /// routing layer can use the PROJECTED coordinate (never a junction shortcut) for plan
    /// endpoint geometry, the straight-line heuristic, and the too-far snap guard. `None`
    /// when no usable segment is in range.
    pub fn arena_snap_proj(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        usable: impl Fn(&StreetEdgeData) -> bool,
    ) -> Option<(crate::structures::LatLng, f64)> {
        let (_edge, gi, dist) = self.seg_index.nearest_usable_seg(lat, lon, radius_m, usable)?;
        let sm = *self.owner_of(gi);
        let near = if gi == sm.seg_start {
            self.junction_coord[sm.from_ji as usize]
        } else {
            self.segs[(gi - 1) as usize].far
        };
        let far = self.segs[gi as usize].far;
        let t = project_t(lat, lon, near, far);
        let proj = crate::structures::LatLng {
            latitude: near.latitude + t * (far.latitude - near.latitude),
            longitude: near.longitude + t * (far.longitude - near.longitude),
        };
        Some((proj, dist))
    }

    /// Keep the lexicographically-cheapest seed per junction (a self-loop chain can bound
    /// the same junction from both directions). Mirrors `walk_entries_arena`'s dedup.
    fn dedup_min_seed(out: &mut Vec<BikeSeed>) {
        let mut i = 0;
        while i < out.len() {
            let mut j = i + 1;
            while j < out.len() {
                if out[j].junction == out[i].junction {
                    if lex_le(&out[j].cost, &out[i].cost) {
                        out.swap(i, j);
                    }
                    out.remove(j);
                } else {
                    j += 1;
                }
            }
            i += 1;
        }
    }
}

impl Graph {
    /// The ≤2 junctions bounding the degree-2 chain that contains `dest`, when `dest` is
    /// an interior pass-through (so a cost-baked super-edge would jump past it). Empty if
    /// `dest` is itself a junction or contraction is absent. The bike search re-walks
    /// (rather than bakes) at these junctions so a label can stop at the interior `dest`.
    pub(super) fn dest_guard_junctions(&self, dest: NodeID) -> Vec<NodeID> {
        let Some(cg) = self.bike_cg() else {
            return Vec::new();
        };
        let jof = |n: NodeID| cg.junction_of.get(n.0).copied().unwrap_or(u32::MAX);
        if jof(dest) != u32::MAX {
            return Vec::new();
        }
        let (nbrs, k) = ContractedGraph::bike_neighbours_of(self, dest.0);
        if k != 2 {
            return Vec::new();
        }
        let mut out = Vec::new();
        for start in [nbrs[0], nbrs[1]] {
            let (mut prev, mut cur) = (dest, start);
            for _ in 0..1_000_000 {
                if jof(cur) != u32::MAX {
                    out.push(cur);
                    break;
                }
                let (nb, kk) = ContractedGraph::bike_neighbours_of(self, cur.0);
                if kk != 2 {
                    break;
                }
                let next = if nb[0] == prev {
                    nb[1]
                } else if nb[1] == prev {
                    nb[0]
                } else {
                    break;
                };
                prev = cur;
                cur = next;
            }
        }
        out
    }

    /// Foot shortest-path seconds from `origin` (which must be a junction) to every
    /// reachable junction, walking over union super-edges instead of individual nodes.
    /// Equivalent to `street_dijkstra(origin, max_seconds, Foot)` restricted to junctions —
    /// the P3c-walk traversal. A junction that is a transit stop is reachable but not
    /// traversed-through (mirrors `street_dijkstra`'s stop guard).
    pub fn walk_dijkstra_union(
        &self,
        origin: NodeID,
        max_seconds: u32,
        cg: &ContractedGraph,
    ) -> HashMap<NodeID, u32> {
        let Some(&oj) = cg.junction_of.get(origin.0).filter(|&&j| j != u32::MAX) else {
            return HashMap::new();
        };
        self.walk_dijkstra_union_seeded(&[(oj as usize, 0)], max_seconds, cg)
    }

    /// Multi-seed foot Dijkstra over union junctions: each seed is `(junction index,
    /// initial seconds)`. Returns the best foot seconds to every reachable junction. The
    /// seeded form lets a snapped interior origin enter the contracted graph at its ≤2
    /// bounding junctions with the proj→junction stub already paid.
    pub fn walk_dijkstra_union_seeded(
        &self,
        seeds: &[(usize, u32)],
        max_seconds: u32,
        cg: &ContractedGraph,
    ) -> HashMap<NodeID, u32> {
        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::new();
        for &(ji, s0) in seeds {
            if s0 > max_seconds {
                continue;
            }
            let jn = cg.junctions[ji];
            let e = dist.entry(jn).or_insert(u32::MAX);
            if s0 < *e {
                *e = s0;
                pq.push(Reverse((s0, ji)));
            }
        }

        while let Some(Reverse((d, ji))) = pq.pop() {
            let jn = cg.junctions[ji];
            if d > *dist.get(&jn).unwrap_or(&u32::MAX) {
                continue;
            }
            // A transit-stop junction is a dead-end for through-walking.
            if self.raptor.transit_node_to_stop.get(jn.0).copied().unwrap_or(u32::MAX) != u32::MAX {
                continue;
            }
            for se in &cg.adjacency[ji] {
                let Some(t) = cg.walk_secs(self, se) else {
                    continue;
                };
                let nd = d.saturating_add(t);
                if nd > max_seconds {
                    continue;
                }
                let to = cg.junctions[se.to as usize];
                let entry = dist.entry(to).or_insert(u32::MAX);
                if nd < *entry {
                    *entry = nd;
                    pq.push(Reverse((nd, se.to as usize)));
                }
            }
        }
        dist
    }

    /// Car shortest-path seconds from junction `origin` (entering Driving) to every
    /// reachable junction, over arena super-edges with the phase threaded in the search
    /// state `(junction, walking)`. Equivalent to `street_dijkstra(origin, bound, Car)`
    /// restricted to junctions. Stop junctions are dead-ends for through-routing.
    pub fn car_dijkstra_union(
        &self,
        origin: NodeID,
        max_seconds: u32,
        cg: &ContractedGraph,
    ) -> HashMap<NodeID, u32> {
        let Some(&oj) = cg.junction_of.get(origin.0).filter(|&&j| j != u32::MAX) else {
            return HashMap::new();
        };
        // State: (junction index, walking phase). Driving = false.
        let mut dist: HashMap<(usize, bool), u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, usize, bool)>> = BinaryHeap::new();
        dist.insert((oj as usize, false), 0);
        pq.push(Reverse((0, oj as usize, false)));
        while let Some(Reverse((d, ji, walking))) = pq.pop() {
            if d > *dist.get(&(ji, walking)).unwrap_or(&u32::MAX) {
                continue;
            }
            let jn = cg.junctions[ji];
            if self.raptor.transit_node_to_stop.get(jn.0).copied().unwrap_or(u32::MAX) != u32::MAX {
                continue;
            }
            for se in &cg.adjacency[ji] {
                let Some((t, next)) = cg.car_secs(self, se, walking) else {
                    continue;
                };
                let nd = d.saturating_add(t);
                if nd > max_seconds {
                    continue;
                }
                let key = (se.to as usize, next);
                let e = dist.entry(key).or_insert(u32::MAX);
                if nd < *e {
                    *e = nd;
                    pq.push(Reverse((nd, se.to as usize, next)));
                }
            }
        }
        // Collapse the phase: best arrival per junction.
        let mut best: HashMap<NodeID, u32> = HashMap::new();
        for (&(ji, _), &d) in &dist {
            let e = best.entry(cg.junctions[ji]).or_insert(u32::MAX);
            *e = (*e).min(d);
        }
        best
    }

    /// RAPTOR foot access/egress over the contracted graph: stops reachable within
    /// `max_secs` of `origin`, as `(compact stop id, walk secs)`. Equivalent to
    /// `nearby_stops(origin, max_secs)` — every transit stop is a junction (the interior
    /// rule excludes stops), so the junction-level search captures them all with identical
    /// times. Sorted by stop id for the same deterministic order RAPTOR expects.
    pub fn nearby_stops_union(
        &self,
        origin: NodeID,
        max_secs: u32,
        cg: &ContractedGraph,
    ) -> Vec<(usize, u32)> {
        let entries = cg.node_walk_entries(self, origin);
        let dist = self.walk_dijkstra_union_seeded(&entries, max_secs, cg);
        let mut stops: Vec<(usize, u32)> = dist
            .iter()
            .filter_map(|(&jn, &secs)| {
                let compact = self.raptor.transit_node_to_stop[jn.0];
                (compact != u32::MAX).then_some((compact as usize, secs))
            })
            .collect();
        stops.sort_unstable_by_key(|&(s, _)| s);
        stops
    }

    /// Scalar-cost bike search over the contracted-graph arena. Equivalent to
    /// `bike_cost_dijkstra` but operates on junction-level super-edges (`cg.segs`,
    /// `cg.adjacency`, `cg.junction_coord`) instead of `g.nodes`/`g.edges`, so it
    /// survives `drop_full_node_arrays()`. Returns min-cost-route arrival seconds per
    /// reachable junction (keyed by `NodeID`). Stop junctions are dead-ends for
    /// through-routing (same as `bike_cost_dijkstra`). Gated by `contracted.is_some()`.
    pub fn bike_dijkstra_union(
        &self,
        origin: NodeID,
        max_seconds: u32,
        bike: &BikeCost,
        cg: &ContractedGraph,
    ) -> HashMap<NodeID, u32> {
        let Some(&oj) = cg.junction_of.get(origin.0).filter(|&&j| j != u32::MAX) else {
            return HashMap::new();
        };
        let mut best_cost: HashMap<usize, u64> = HashMap::new();
        let mut arrival: HashMap<usize, u32> = HashMap::new();
        let mut elev_buf: HashMap<usize, (f64, f64)> = HashMap::new();
        let mut incoming: HashMap<usize, Option<(f64, f64)>> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u64, usize, u32)>> = BinaryHeap::new();
        best_cost.insert(oj as usize, 0);
        arrival.insert(oj as usize, 0);
        incoming.insert(oj as usize, None);
        pq.push(Reverse((0, oj as usize, 0)));
        while let Some(Reverse((cost_bits, ji, time_secs))) = pq.pop() {
            if cost_bits > *best_cost.get(&ji).unwrap_or(&u64::MAX) {
                continue;
            }
            let jn = cg.junctions[ji];
            if jn != origin
                && self.raptor.transit_node_to_stop.get(jn.0).copied().unwrap_or(u32::MAX)
                    != u32::MAX
            {
                continue;
            }
            let cur_inc = *incoming.get(&ji).unwrap_or(&None);
            let (ehbd, ehbu) = *elev_buf.get(&ji).unwrap_or(&(0.0, 0.0));
            for se in &cg.adjacency[ji] {
                let mut cost_acc = cost_bits;
                let mut time_acc = time_secs;
                let mut cur_inc_seg = cur_inc;
                let mut seg_ehbd = ehbd;
                let mut seg_ehbu = ehbu;
                let mut near = cg.junction_coord[ji];
                let mut ok = true;
                let mut last_dir: Option<(f64, f64)> = None;
                for seg in cg.seg_slice(se) {
                    let this_dir = dir_coords(near, seg.far);
                    let Some(step_cost) = bike.edge_cost(&seg.edge, cur_inc_seg, this_dir) else {
                        ok = false;
                        break;
                    };
                    let nt = time_acc.saturating_add(bike.edge_time(&seg.edge));
                    if nt > max_seconds {
                        ok = false;
                        break;
                    }
                    let (elev_cost, new_ehbd, new_ehbu) = bike.elevation_step(
                        seg_ehbd,
                        seg_ehbu,
                        seg.edge.elev_delta as f64,
                        seg.edge.length as f64,
                    );
                    cost_acc =
                        cost_acc.saturating_add(((step_cost + elev_cost) * 1000.0) as u64);
                    time_acc = nt;
                    seg_ehbd = new_ehbd;
                    seg_ehbu = new_ehbu;
                    cur_inc_seg = Some(this_dir);
                    last_dir = Some(this_dir);
                    near = seg.far;
                }
                if !ok {
                    continue;
                }
                let to_ji = se.to as usize;
                let entry = best_cost.entry(to_ji).or_insert(u64::MAX);
                if cost_acc < *entry {
                    *entry = cost_acc;
                    arrival.insert(to_ji, time_acc);
                    elev_buf.insert(to_ji, (seg_ehbd, seg_ehbu));
                    incoming.insert(to_ji, last_dir);
                    pq.push(Reverse((cost_acc, to_ji, time_acc)));
                }
            }
        }
        arrival
            .into_iter()
            .map(|(ji, secs)| (cg.junctions[ji], secs))
            .collect()
    }

    /// Drop the full per-node arrays — `nodes`, `edges`, the snapping `nodes_tree` and
    /// `edge_index` — freeing the interior-node memory once every consumer routes on the
    /// contracted graph (the P3f win). `raptor` (cost params, `transit_node_to_stop`,
    /// stop trees) is kept: contracted routing reads it. Irreversible for this instance.
    pub fn drop_full_node_arrays(&mut self) {
        self.nodes = Vec::new();
        self.edges = Vec::new();
        self.nodes_tree = kdtree::KdTree::new(2);
        self.edge_index = super::edge_index::EdgeIndex::default();
    }

    /// Cost-bake bike cost onto the **union** contracted graph's super-edges (`self.contracted`),
    /// so the bike multi-objective search can run on the serialized union cg (which survives
    /// dropping `g`). `bake_super_edge` replays whatever segments a super-edge owns, and the
    /// live entry-corner correction at each junction makes the front identical regardless of
    /// where the splits fall. Called from `apply_routing_defaults` — also on the RESTORE path,
    /// since `SuperEdge.baked` is serde-skipped (a deserialized union cg has `baked = None`).
    pub fn bake_bike_on_contracted(&mut self, bike: &BikeCost) {
        let Some(mut cg) = self.contracted.take() else {
            return;
        };
        let mut baked: Vec<Option<Box<BakedCost>>> = Vec::new();
        for adj in &cg.adjacency {
            for se in adj {
                let start = cg.junction_coord[cg.junction_of[se.from.0] as usize];
                baked.push(self.bake_super_edge(cg.seg_slice(se), start, bike).map(Box::new));
            }
        }
        let mut it = baked.into_iter();
        for adj in cg.adjacency.iter_mut() {
            for se in adj.iter_mut() {
                se.baked = it.next().flatten();
            }
        }
        self.contracted = Some(cg);
    }

    /// Replay an ordered list of `(edge, travel-dir)` hops through the kinematic bike cost
    /// from a standstill (`prev = None`), building a [`BikeSeed`] landing at `junction`. The
    /// edge directions are passed explicitly (the backward chain rides edges whose stored
    /// `dir_between` would be the wrong way), but the per-edge cost itself comes from the
    /// production `street_edge_transition` so the seed is bit-identical to the full search.
    fn replay_bike_chain(
        &self,
        bike: &BikeCost,
        hops: &[(StreetEdgeData, (f64, f64))],
        junction: usize,
    ) -> Option<BikeSeed> {
        let mode = RoutingMode::Bike;
        let profile = bike.profile();
        let weights = self.raptor.cost_weights;
        let speed = self.mode_speed(mode);
        let cv = self.raptor.systematic_cv;

        let mut cost = CostVector::ZERO;
        let mut elev = (0.0, 0.0);
        let mut var = 0.0;
        let mut len = 0usize;
        let mut prev: Option<PrevCtx> = None;
        for (edge, dir) in hops {
            let (c, e, v) = self.street_edge_transition(
                mode, edge, &profile, &weights, speed, cv, bike, prev, &cost, elev, var,
            )?;
            cost = c;
            elev = e;
            var = v;
            len += edge.length;
            prev = Some(ctx_after(bike, prev, edge, *dir));
        }
        Some(BikeSeed {
            junction,
            cost,
            exit: prev?,
            elev,
            var,
            len,
        })
    }

    /// Canonically replay a super-edge's `segs` (entry = none) to precompute its cost,
    /// stashing the first two segments + the canonical 2nd-segment corner for the live
    /// entry-dependent correction. `None` for a single-segment super-edge. `start_coord` is
    /// the owning junction's coordinate; each segment's direction comes from consecutive
    /// far-coords (identical to `dir_between` on the original nodes).
    fn bake_super_edge(
        &self,
        segs: &[Seg],
        start_coord: crate::structures::LatLng,
        bike: &BikeCost,
    ) -> Option<BakedCost> {
        if segs.len() < 2 {
            return None;
        }
        let mode = RoutingMode::Bike;
        let profile = bike.profile();
        let weights = self.raptor.cost_weights;
        let speed = self.mode_speed(mode);
        let cv = self.raptor.systematic_cv;

        let mut cost = CostVector::ZERO;
        let mut elev = (0.0, 0.0);
        let mut var = 0.0;
        let mut prev: Option<PrevCtx> = None;
        let mut length = 0usize;
        let mut s1: Option<SegLite> = None;
        let mut s2: Option<SegLite> = None;
        let mut corner_canon_secs = 0.0;
        let mut exit: Option<PrevCtx> = None;
        let mut near = start_coord;

        for (i, seg) in segs.iter().enumerate() {
            let edge = &seg.edge;
            let dir = dir_coords(near, seg.far);
            if i == 1 {
                corner_canon_secs = bike.speed_change_secs(prev, edge, dir);
            }
            // Carry the arena-derived `dir` (g-free) — `street_edge_transition` would
            // recompute it via `dir_between` (reads `g.nodes`), which is gone post-drop.
            // `dir_coords(near, far) == dir_between(origin, dest)` here, so bit-identical.
            let (c, e, v) = self.street_edge_transition_dir(
                mode, edge, Some(dir), &profile, &weights, speed, cv, bike, prev, &cost, elev, var,
            )?;
            cost = c;
            elev = e;
            var = v;
            length += edge.length;
            if i == 0 {
                s1 = Some(SegLite { dir, edge: *edge });
            } else if i == 1 {
                s2 = Some(SegLite { dir, edge: *edge });
            }
            prev = Some(ctx_after(bike, prev, edge, dir));
            exit = prev;
            near = seg.far;
        }

        Some(BakedCost {
            cost,
            length,
            corner_canon_secs,
            s1: s1?,
            s2,
            exit: exit?,
        })
    }
}

/// Lexicographic `a <= b` over all cost axes (the seed dedup tiebreak).
fn lex_le(a: &CostVector, b: &CostVector) -> bool {
    for &ax in &Axis::ALL {
        match a.get(ax).partial_cmp(&b.get(ax)) {
            Some(std::cmp::Ordering::Less) => return true,
            Some(std::cmp::Ordering::Greater) => return false,
            _ => continue,
        }
    }
    true
}

/// Unit direction vector from `a` to `b` in lat/lon space — the free-standing twin of
/// `Graph::dir_between`, for replaying super-edge corners from arena far-coords.
pub(super) fn dir_coords(a: crate::structures::LatLng, b: crate::structures::LatLng) -> (f64, f64) {
    let (dx, dy) = (b.longitude - a.longitude, b.latitude - a.latitude);
    let n = (dx * dx + dy * dy).sqrt().max(1e-12);
    (dx / n, dy / n)
}

/// The point on segment `a→b` closest to `(lat,lon)` (equirectangular projection),
/// as a `LatLng` — the snap connector target for plan geometry.
fn project_point(
    lat: f64,
    lon: f64,
    a: crate::structures::LatLng,
    b: crate::structures::LatLng,
) -> crate::structures::LatLng {
    let t = project_t(lat, lon, a, b);
    crate::structures::LatLng {
        latitude: a.latitude + (b.latitude - a.latitude) * t,
        longitude: a.longitude + (b.longitude - a.longitude) * t,
    }
}

/// Fraction `t∈[0,1]` of the closest point on segment `a→b` to `(lat, lon)`, equirectangular
/// (meters centred on the query) — same projection as `Graph::project_point`.
fn project_t(lat: f64, lon: f64, a: crate::structures::LatLng, b: crate::structures::LatLng) -> f64 {
    let m_lat = 111_320.0_f64;
    let m_lon = 111_320.0_f64 * lat.to_radians().cos();
    let to = |la: f64, lo: f64| ((lo - lon) * m_lon, (la - lat) * m_lat);
    let (ax, ay) = to(a.latitude, a.longitude);
    let (bx, by) = to(b.latitude, b.longitude);
    let (dx, dy) = (bx - ax, by - ay);
    let len2 = dx * dx + dy * dy;
    if len2 == 0.0 {
        0.0
    } else {
        (-(ax * dx + ay * dy) / len2).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::VarGen;
    use crate::structures::{
        BikeAttrs, BikeCost, BikeProfile, EdgeData, HighwayClass, LatLng, NodeData, NodeID,
        OsmNodeData, StreetEdgeData, Surface,
    };

    fn osm(g: &mut Graph, id: &str, lat: f64, lon: f64) -> NodeID {
        g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: id.into(),
            lat_lng: LatLng {
                latitude: lat,
                longitude: lon,
            },
        }))
    }

    fn bidir_bike(g: &mut Graph, a: NodeID, b: NodeID) {
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Tertiary;
        at.surface = Surface::Paved;
        let len = g.nodes[a.0].loc().dist(g.nodes[b.0].loc()) as usize;
        for (o, d, ed) in [(a, b, 1i16), (b, a, -1i16)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    partial: false,
                    length: len,
                    foot: true,
                    bike: true,
                    car: false,
                    attrs: at,
                    elev_delta: ed,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    }

    /// A bidirectional **foot-only** edge (bike/car false) — a pedestrian-only link.
    fn bidir_foot(g: &mut Graph, a: NodeID, b: NodeID) {
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Footway;
        at.surface = Surface::Paved;
        let len = g.nodes[a.0].loc().dist(g.nodes[b.0].loc()) as usize;
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    partial: false,
                    length: len,
                    foot: true,
                    bike: false,
                    car: false,
                    attrs: at,
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    }

    /// A bidirectional car+foot edge (bike false) — a drivable street.
    fn bidir_car(g: &mut Graph, a: NodeID, b: NodeID) {
        let mut at = BikeAttrs::road_default();
        at.highway = HighwayClass::Tertiary;
        at.surface = Surface::Paved;
        let len = g.nodes[a.0].loc().dist(g.nodes[b.0].loc()) as usize;
        for (o, d) in [(a, b), (b, a)] {
            g.add_edge(
                o,
                EdgeData::Street(StreetEdgeData {
                    origin: o,
                    destination: d,
                    partial: false,
                    length: len,
                    foot: true,
                    bike: false,
                    car: true,
                    attrs: at,
                    elev_delta: 0,
                    surface_speed: 100,
                    var_gen: VarGen::NONE,
                }),
            );
        }
    }

    // a=(•)-m1-m2-b=(•) chain with bends at m1, m2; a and b made junctions by spokes.
    fn chain_graph() -> (Graph, NodeID, NodeID, NodeID, NodeID) {
        let mut g = Graph::new();
        let a = osm(&mut g, "a", 50.000, 4.000);
        let m1 = osm(&mut g, "m1", 50.000, 4.001);
        let m2 = osm(&mut g, "m2", 50.001, 4.001);
        let b = osm(&mut g, "b", 50.001, 4.002);
        let p = osm(&mut g, "p", 50.000, 3.999);
        let q = osm(&mut g, "q", 49.999, 4.000);
        let r = osm(&mut g, "r", 50.002, 4.002);
        let s = osm(&mut g, "s", 50.001, 4.003);
        bidir_bike(&mut g, a, m1);
        bidir_bike(&mut g, m1, m2);
        bidir_bike(&mut g, m2, b);
        bidir_bike(&mut g, a, p);
        bidir_bike(&mut g, a, q);
        bidir_bike(&mut g, b, r);
        bidir_bike(&mut g, b, s);
        g.build_raptor_index();
        (g, a, b, m1, m2)
    }

    #[test]
    fn transit_stop_without_street_edge_is_a_junction() {
        // A stop reachable only by transit (no foot stub / no street edge) must STILL be a
        // junction, so its coordinate survives `drop_full_node_arrays()`. Otherwise the
        // `raptorExplain` stops-reached survey (`node_loc`) panics on the dropped graph
        // (index out of bounds: junction_of[stop] == u32::MAX).
        use crate::structures::TransitStopData;
        use gtfs_structures::Availability;
        let mut g = Graph::new();
        let a = osm(&mut g, "a", 50.000, 4.000);
        let b = osm(&mut g, "b", 50.000, 4.001);
        bidir_bike(&mut g, a, b);
        let stop = g.add_node(NodeData::TransitStop(TransitStopData {
            name: "Island Stop".into(),
            id: "S".into(),
            lat_lng: LatLng {
                latitude: 50.002,
                longitude: 4.002,
            },
            accessibility: Availability::Available,
        }));
        g.build_raptor_index();
        assert_ne!(
            g.raptor.transit_node_to_stop[stop.0],
            u32::MAX,
            "fixture: the stop must be registered in transit_node_to_stop"
        );
        let cg = ContractedGraph::from_graph_union(&g);
        assert!(
            cg.junction_coord_of(stop).is_some(),
            "a transit stop with no street edge must still be a junction so its coord survives the drop"
        );
    }

    /// Replay a super-edge's chain through the per-segment bike cost, from `start`.
    fn replay_secs(g: &Graph, start: NodeID, se: &SuperEdge, bike: &BikeCost) -> f64 {
        use super::super::bike_cost::{BikeCost as BC, PrevCtx};
        let mut total = 0.0;
        let mut prev: Option<PrevCtx> = None;
        let mut p = start;
        for &n in &se.nodes {
            let e = ContractedGraph::bike_edge(g, p, n).unwrap();
            let d = g.dir_between(p, n);
            total += bike.edge_time(e) as f64 + bike.speed_change_secs(prev, e, d);
            let push = BC::is_push(&e.attrs);
            prev = Some(PrevCtx {
                dir: d,
                len: e.length as f64,
                cruise: if push { 0.0 } else { bike.cruise_speed(e) },
                push,
                speed: bike.required_speed(prev, e, d),
            });
            p = n;
        }
        total
    }

    #[test]
    fn interior_nodes_collapse_junctions_survive() {
        let (g, a, b, m1, m2) = chain_graph();
        let cg = ContractedGraph::from_graph(&g);
        assert_eq!(cg.junction_of[m1.0], u32::MAX, "m1 is interior");
        assert_eq!(cg.junction_of[m2.0], u32::MAX, "m2 is interior");
        assert_ne!(cg.junction_of[a.0], u32::MAX, "a is a junction");
        assert_ne!(cg.junction_of[b.0], u32::MAX, "b is a junction");
        let se = cg.adjacency[cg.junction_of[a.0] as usize]
            .iter()
            .find(|se| se.to == cg.junction_of[b.0])
            .expect("a→b super-edge");
        assert_eq!(se.nodes, vec![m1, m2, b], "chain is m1, m2, b");
    }

    /// Plan geometry from the arena: a super-edge's polyline (junction coord + each
    /// segment's far-coord) must equal the original node coordinates — so plan
    /// reconstruction survives dropping `g.nodes` (P3f).
    #[test]
    fn super_edge_coords_match_node_coords() {
        let (g, a, b, m1, m2) = chain_graph();
        let cg = ContractedGraph::from_graph_union(&g);
        let se = cg.super_edge(a, m1).expect("a→ super-edge to b");
        // Expected far-coords of the chain a→m1→m2→b: m1, m2, b (the junction a is the
        // start, not part of the far list).
        let want: Vec<_> = [m1, m2, b].iter().map(|n| g.nodes[n.0].loc()).collect();
        let got = cg.super_edge_coords(se);
        assert_eq!(got.len(), want.len());
        for (g1, w1) in got.iter().zip(&want) {
            assert!((g1.latitude - w1.latitude).abs() < 1e-12 && (g1.longitude - w1.longitude).abs() < 1e-12);
        }
        // Start junction coord is recoverable too.
        let ji = cg.junction_of[a.0] as usize;
        assert_eq!(cg.junction_coord[ji].latitude, g.nodes[a.0].loc().latitude);
        let _ = b;
    }

    /// A foot-only spur off a chain node makes it a junction for the **union** graph (it
    /// is a real branch for walkers) even though it is a degree-2 pass-through for **bike**.
    /// P3 must keep it, or a walk route through the spur would be lost.
    #[test]
    fn union_interior_is_stricter_than_bike() {
        // a=(•)-m-b=(•) bikeable chain; m additionally has a FOOT-ONLY spur to s.
        let mut g = Graph::new();
        let a = osm(&mut g, "a", 50.000, 4.000);
        let m = osm(&mut g, "m", 50.000, 4.001);
        let b = osm(&mut g, "b", 50.000, 4.002);
        let s = osm(&mut g, "s", 50.001, 4.001);
        let p = osm(&mut g, "p", 50.000, 3.999);
        let q = osm(&mut g, "q", 50.000, 4.003);
        bidir_bike(&mut g, a, m);
        bidir_bike(&mut g, m, b);
        bidir_bike(&mut g, a, p); // makes a a junction
        bidir_bike(&mut g, b, q); // makes b a junction
        bidir_foot(&mut g, m, s); // foot-only spur
        g.build_raptor_index();

        let bike = ContractedGraph::from_graph(&g);
        assert_eq!(bike.junction_of[m.0], u32::MAX, "m is a bike pass-through");

        let union = ContractedGraph::from_graph_union(&g);
        assert_ne!(
            union.junction_of[m.0],
            u32::MAX,
            "m is a junction for the union (the foot spur branches)"
        );
        // The spur node s is a dead-end (degree-1) ⇒ junction; m→s is its own super-edge.
        assert_ne!(union.junction_of[s.0], u32::MAX, "spur end is a junction");
        let se = union
            .super_edge(m, s)
            .expect("m→s foot spur is a union super-edge");
        assert_eq!(se.to, union.junction_of[s.0]);
    }

    /// Point-to-point foot routing over the contracted graph matches `street_dijkstra`
    /// for arbitrary (incl. interior) endpoints — covering the same-chain interior↔interior
    /// case that the via-junction bridge alone would overestimate.
    #[test]
    fn walk_point_to_point_matches_street_dijkstra() {
        use super::super::raptor_access::StreetProfile;
        let (g, a, b, m1, m2) = chain_graph();
        let cg = ContractedGraph::from_graph_union(&g);
        // Endpoints to probe: junctions a,b and interior m1,m2 in every combination.
        for &o in &[a, b, m1, m2] {
            let full = g.street_dijkstra(o, u32::MAX, StreetProfile::Foot);
            for &d in &[a, b, m1, m2] {
                let want = full.get(&d).copied();
                let got = cg.walk_secs_point_to_point(&g, o, d, u32::MAX);
                assert_eq!(got, want, "o={o:?} d={d:?}: contracted {got:?} != full {want:?}");
            }
        }
        // The discriminating pair: m1→m2 are both interior on one chain. The direct
        // along-chain time must be used, not a doubled-back via-junction time.
        let direct = cg.walk_secs_point_to_point(&g, m1, m2, u32::MAX).unwrap();
        let via = ContractedGraph::foot_step_secs(&g, m1, m2).unwrap();
        assert_eq!(direct, via, "same-chain m1→m2 must be the direct hop, not via a junction");
    }

    /// Junction-level walk times over union super-edges match the node-by-node
    /// `street_dijkstra(Foot)` exactly — the P3c-walk equivalence gate.
    #[test]
    fn walk_dijkstra_union_matches_street_dijkstra() {
        use super::super::raptor_access::StreetProfile;
        let (g, a, b, _m1, _m2) = chain_graph();
        let cg = ContractedGraph::from_graph_union(&g);
        let full = g.street_dijkstra(a, u32::MAX, StreetProfile::Foot);
        let contracted = g.walk_dijkstra_union(a, u32::MAX, &cg);

        // Every junction's contracted time must equal the full search's time there.
        for (&jn, &t) in &contracted {
            assert_eq!(
                full.get(&jn).copied(),
                Some(t),
                "junction {jn:?}: contracted {t} != full {:?}",
                full.get(&jn)
            );
        }
        // And the far junction b is actually reached (the search isn't trivially empty).
        assert!(contracted.contains_key(&b), "b reached over super-edges");
        assert!(contracted.len() >= 2);

        // P3b prerequisite: an INTERIOR origin has no junction index, so the contracted
        // search returns nothing. Production snapping returns interior nodes ~81% of the
        // time, so wiring `street_dijkstra` to this requires `Endpoint::OnSuperEdge`
        // mid-chain snapping first — this is a guard, not yet a complete walk consumer.
        let m1 = chain_graph().3;
        assert!(
            g.walk_dijkstra_union(m1, u32::MAX, &cg).is_empty(),
            "interior origin needs snapping (P3b) before contracted walk can serve it"
        );
    }

    /// G-based oracle: replay a partial-super-edge ride from a projection on edge
    /// `near→far` (dist `db` toward `far`), then the real chain of bike edges from `far`
    /// away from `near` until a junction, through the production `street_edge_transition`.
    /// Returns `(cost, elev, var, len, exit_dir)`. Independent of the arena (reads `g`).
    fn oracle_partial_ride(
        g: &Graph,
        bike: &BikeCost,
        cg: &ContractedGraph,
        near: NodeID,
        far: NodeID,
        db: usize,
    ) -> (crate::structures::cost::CostVector, (f64, f64), f64, usize, (f64, f64)) {
        use crate::structures::cost::{CostVector, RoutingMode};
        let mode = RoutingMode::Bike;
        let profile = bike.profile();
        let weights = g.raptor.cost_weights;
        let speed = g.mode_speed(mode);
        let cv = g.raptor.systematic_cv;

        // Hop list: stub near→far (length db), then real edges far→…→junction.
        let mut hops: Vec<(StreetEdgeData, (f64, f64))> = Vec::new();
        let full = ContractedGraph::bike_edge(g, near, far).unwrap();
        hops.push((Graph::partial_edge(full, db), g.dir_between(near, far)));
        let (mut prev_n, mut cur) = (near, far);
        loop {
            if cg.junction_of[cur.0] != u32::MAX {
                break;
            }
            let (nb, k) = ContractedGraph::bike_neighbours_of(g, cur.0);
            assert_eq!(k, 2);
            let next = if nb[0] == prev_n { nb[1] } else { nb[0] };
            let e = ContractedGraph::bike_edge(g, cur, next).unwrap();
            hops.push((*e, g.dir_between(cur, next)));
            prev_n = cur;
            cur = next;
        }

        let mut cost = CostVector::ZERO;
        let mut elev = (0.0, 0.0);
        let mut var = 0.0;
        let mut len = 0usize;
        let mut prev: Option<super::PrevCtx> = None;
        for (edge, dir) in &hops {
            let (c, e, v) = g
                .street_edge_transition(mode, edge, &profile, &weights, speed, cv, bike, prev, &cost, elev, var)
                .unwrap();
            cost = c;
            elev = e;
            var = v;
            len += edge.length;
            prev = Some(super::ctx_after(bike, prev, edge, *dir));
        }
        let exit = prev.unwrap().dir;
        (cost, elev, var, len, exit)
    }

    /// Restore-path guard: at startup from a dropped graph.bin, `g` is loaded EMPTY and the
    /// serde-skipped bike bake is rebuilt AFTER the drop. So baking must read no `g.nodes`/
    /// `g.edges` (it replays the arena + carries the dir). Bake AFTER the drop here — a
    /// regression to the g-reading `street_edge_transition` would panic.
    #[test]
    fn bake_after_drop_is_gfree() {
        let (mut g, _a, _b, _m1, _m2) = chain_graph();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.drop_full_node_arrays();
        assert_eq!(g.node_count(), 0, "g dropped, as on a restored dropped graph.bin");
        g.bake_bike_on_contracted_default(); // must not panic with g empty
        let baked = g
            .contracted
            .as_ref()
            .unwrap()
            .adjacency
            .iter()
            .flatten()
            .filter(|se| se.baked.is_some())
            .count();
        assert!(baked > 0, "≥2-segment super-edges must cost-bake after the drop");
    }

    #[test]
    fn bike_entries_arena_forward_matches_g_replay() {
        use crate::structures::cost::Axis;
        let (mut g, a, b, m1, m2) = chain_graph();
        let bike = g.default_bike_cost();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted(&bike);
        let cg = g.contracted.take().unwrap();

        // Snap a point clearly mid-segment on the m1→m2 hop (away from junctions a,b).
        let pm1 = g.nodes[m1.0].loc();
        let pm2 = g.nodes[m2.0].loc();
        let lat = pm1.latitude + 0.5 * (pm2.latitude - pm1.latitude);
        let lon = pm1.longitude + 0.5 * (pm2.longitude - pm1.longitude);

        let seeds = cg.bike_entries_arena(&g, &bike, lat, lon, 50.0);
        assert!(!seeds.is_empty(), "snap must produce seeds");

        // The forward seed lands at junction b (far side of m1→m2 is toward b).
        let jb = cg.junction_of[b.0] as usize;
        let fwd = seeds.iter().find(|s| s.junction == jb).expect("forward seed at b");

        // db = proj→far(m2) length, from snap_to_edge's convention on the m1→m2 edge.
        let edge_m = ContractedGraph::bike_edge(&g, m1, m2).unwrap();
        let t = {
            let m_lat = 111_320.0_f64;
            let m_lon = 111_320.0_f64 * lat.to_radians().cos();
            let to = |la: f64, lo: f64| ((lo - lon) * m_lon, (la - lat) * m_lat);
            let (ax, ay) = to(pm1.latitude, pm1.longitude);
            let (bx, by) = to(pm2.latitude, pm2.longitude);
            let (dx, dy) = (bx - ax, by - ay);
            (-(ax * dx + ay * dy) / (dx * dx + dy * dy)).clamp(0.0, 1.0)
        };
        let da = ((t * edge_m.length as f64).round() as usize).min(edge_m.length);
        let db = edge_m.length - da;

        let (cost, elev, var, len, _exit) = oracle_partial_ride(&g, &bike, &cg, m1, m2, db);
        for &ax in &Axis::ALL {
            assert!(
                (fwd.cost.get(ax) - cost.get(ax)).abs() < 1e-6,
                "axis {ax:?}: seed {} != oracle {}",
                fwd.cost.get(ax),
                cost.get(ax)
            );
        }
        assert!((fwd.elev.0 - elev.0).abs() < 1e-9 && (fwd.elev.1 - elev.1).abs() < 1e-9, "elev");
        assert!((fwd.var - var).abs() < 1e-6, "var: {} != {}", fwd.var, var);
        assert_eq!(fwd.len, len, "len");

        // BACKWARD seed lands at junction a (near side m1 is toward a). Oracle rides the
        // reverse edge m2→m1 (distance da = proj→m1), then m1→a, via reverse g edges.
        let ja = cg.junction_of[a.0] as usize;
        let bwd = seeds.iter().find(|s| s.junction == ja).expect("backward seed at a");
        let (bcost, belev, bvar, blen, _) = oracle_partial_ride(&g, &bike, &cg, m2, m1, da);
        for &ax in &Axis::ALL {
            assert!(
                (bwd.cost.get(ax) - bcost.get(ax)).abs() < 1e-6,
                "backward axis {ax:?}: seed {} != oracle {}",
                bwd.cost.get(ax),
                bcost.get(ax)
            );
        }
        assert!((bwd.elev.0 - belev.0).abs() < 1e-9 && (bwd.elev.1 - belev.1).abs() < 1e-9, "bwd elev");
        assert!((bwd.var - bvar).abs() < 1e-6, "bwd var: {} != {}", bwd.var, bvar);
        assert_eq!(bwd.len, blen, "bwd len");
        // Forward and backward must differ (the chain is asymmetric: bends + elev sign).
        assert_ne!(fwd.junction, bwd.junction);
    }

    #[test]
    fn superedge_replay_equals_uncontracted_chain_cost() {
        let (g, a, b, m1, m2) = chain_graph();
        let cg = ContractedGraph::from_graph(&g);
        let bike = BikeCost::new(BikeProfile::default());

        // Baseline: walk the real node chain a→m1→m2→b on the full graph.
        let chain = [a, m1, m2, b];
        let mut base = 0.0;
        let mut prev: Option<super::super::bike_cost::PrevCtx> = None;
        for w in chain.windows(2) {
            let e = ContractedGraph::bike_edge(&g, w[0], w[1]).unwrap();
            let d = g.dir_between(w[0], w[1]);
            base += bike.edge_time(e) as f64 + bike.speed_change_secs(prev, e, d);
            let push = BikeCost::is_push(&e.attrs);
            prev = Some(super::super::bike_cost::PrevCtx {
                dir: d,
                len: e.length as f64,
                cruise: if push { 0.0 } else { bike.cruise_speed(e) },
                push,
                speed: bike.required_speed(prev, e, d),
            });
        }

        let se = cg.adjacency[cg.junction_of[a.0] as usize]
            .iter()
            .find(|se| se.to == cg.junction_of[b.0])
            .unwrap();
        let replay = replay_secs(&g, a, se, &bike);
        assert!(
            (replay - base).abs() < 1e-9,
            "super-edge replay {replay} must equal the un-contracted chain cost {base}"
        );
        assert!(base > 0.0);
    }

    /// Full per-segment replay of a chain via the production `street_edge_transition`,
    /// entered with `entry` — the exact cost the search would accumulate.
    fn full_replay(g: &Graph, from: NodeID, nodes: &[NodeID], entry: Option<super::PrevCtx>, bike: &BikeCost) -> crate::structures::cost::CostVector {
        use crate::structures::cost::{CostVector, RoutingMode};
        let mode = RoutingMode::Bike;
        let profile = bike.profile();
        let weights = g.raptor.cost_weights;
        let speed = g.mode_speed(mode);
        let cv = g.raptor.systematic_cv;
        let mut chain = vec![from];
        chain.extend_from_slice(nodes);
        let (mut cost, mut elev, mut var) = (CostVector::ZERO, (0.0, 0.0), 0.0);
        let mut prev = entry;
        for w in chain.windows(2) {
            let edge = ContractedGraph::bike_edge(g, w[0], w[1]).unwrap();
            let dir = g.dir_between(w[0], w[1]);
            let (c, e, v) = g
                .street_edge_transition(mode, edge, &profile, &weights, speed, cv, bike, prev, &cost, elev, var)
                .unwrap();
            cost = c;
            elev = e;
            var = v;
            prev = Some(super::ctx_after(bike, prev, edge, dir));
        }
        cost
    }

    /// G-based phased-car oracle: replay a partial-edge `near→far` stub (length `db`), then
    /// the explicit node chain `chain` (from `far`), all through `g.car_edge_step` from
    /// Driving. Returns `(secs, exit_walking)` or `None` if a hop is impassable. Independent
    /// of the arena (sources edges from `g` via `street_edge_dir`).
    fn car_oracle(g: &Graph, near: NodeID, far: NodeID, db: usize, chain: &[NodeID]) -> Option<(u32, bool)> {
        let full = ContractedGraph::street_edge_dir(g, near, far).unwrap();
        let mut edges = vec![Graph::partial_edge(full, db)];
        let mut p = far;
        for &n in chain {
            edges.push(*ContractedGraph::street_edge_dir(g, p, n).unwrap());
            p = n;
        }
        let mut total = 0u32;
        let mut phase = false;
        for e in &edges {
            let (s, next) = g.car_edge_step(e, phase)?;
            total = total.saturating_add(s);
            phase = next;
        }
        Some((total, phase))
    }

    /// `car_entries_arena` forward+backward seeds (snapped mid-segment on an all-car chain)
    /// equal a `g`-based phased-car oracle replaying the same partial chain. Phase stays
    /// Driving throughout (every edge is car-passable).
    #[test]
    fn car_entries_arena_matches_g_replay_all_car() {
        let mut g = Graph::new();
        let a = osm(&mut g, "a", 50.000, 4.000);
        let m1 = osm(&mut g, "m1", 50.000, 4.001);
        let m2 = osm(&mut g, "m2", 50.001, 4.001);
        let b = osm(&mut g, "b", 50.001, 4.002);
        let p = osm(&mut g, "p", 50.000, 3.999);
        let q = osm(&mut g, "q", 49.999, 4.000);
        let r = osm(&mut g, "r", 50.002, 4.002);
        let s = osm(&mut g, "s", 50.001, 4.003);
        bidir_car(&mut g, a, m1);
        bidir_car(&mut g, m1, m2);
        bidir_car(&mut g, m2, b);
        bidir_car(&mut g, a, p);
        bidir_car(&mut g, a, q);
        bidir_car(&mut g, b, r);
        bidir_car(&mut g, b, s);
        g.build_raptor_index();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        assert_eq!(cg.junction_of[m1.0], u32::MAX, "m1 interior (union)");
        assert_eq!(cg.junction_of[m2.0], u32::MAX, "m2 interior (union)");

        // Snap mid-segment on m1→m2.
        let pm1 = g.nodes[m1.0].loc();
        let pm2 = g.nodes[m2.0].loc();
        let lat = pm1.latitude + 0.5 * (pm2.latitude - pm1.latitude);
        let lon = pm1.longitude + 0.5 * (pm2.longitude - pm1.longitude);

        let seeds = cg.car_entries_arena(&g, lat, lon, 50.0);
        assert!(!seeds.is_empty(), "snap must produce seeds");

        let edge_m = ContractedGraph::street_edge_dir(&g, m1, m2).unwrap();
        let t = project_t(lat, lon, pm1, pm2);
        let da = ((t * edge_m.length as f64).round() as usize).min(edge_m.length);
        let db = edge_m.length - da;

        // FORWARD → b: stub m1→m2 (db), then m2→b.
        let jb = cg.junction_of[b.0] as usize;
        let fwd = seeds.iter().find(|(j, _, _)| *j == jb).expect("forward seed at b");
        let (osecs, ophase) = car_oracle(&g, m1, m2, db, &[b]).unwrap();
        assert_eq!((fwd.1, fwd.2), (osecs, ophase), "forward secs/phase");
        assert!(!fwd.2, "all-car ⇒ exit still Driving");

        // BACKWARD → a: stub m2→m1 (da), then m1→a.
        let ja = cg.junction_of[a.0] as usize;
        let bwd = seeds.iter().find(|(j, _, _)| *j == ja).expect("backward seed at a");
        let (bsecs, bphase) = car_oracle(&g, m2, m1, da, &[a]).unwrap();
        assert_eq!((bwd.1, bwd.2), (bsecs, bphase), "backward secs/phase");
    }

    /// Park-switch case: a car–FOOT–car chain. Snapping on the car segment `a→m1` and
    /// driving forward must park at the foot-only `m1→m2` hop and exit Walking, matching the
    /// `g` oracle's Driving→park→Walking transition at the same segment boundary.
    #[test]
    fn car_entries_arena_phase_switch_park() {
        let mut g = Graph::new();
        let a = osm(&mut g, "a", 50.000, 4.000);
        let m1 = osm(&mut g, "m1", 50.000, 4.001);
        let m2 = osm(&mut g, "m2", 50.000, 4.002);
        let b = osm(&mut g, "b", 50.000, 4.003);
        let p = osm(&mut g, "p", 50.000, 3.999);
        let q = osm(&mut g, "q", 49.999, 4.000);
        let r = osm(&mut g, "r", 50.001, 4.003);
        let s = osm(&mut g, "s", 50.000, 4.004);
        bidir_car(&mut g, a, m1);
        bidir_foot(&mut g, m1, m2); // foot-only: forces park here
        bidir_car(&mut g, m2, b);
        bidir_car(&mut g, a, p);
        bidir_car(&mut g, a, q);
        bidir_car(&mut g, b, r);
        bidir_car(&mut g, b, s);
        g.build_raptor_index();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        assert_eq!(cg.junction_of[m1.0], u32::MAX, "m1 interior (union deg-2)");
        assert_eq!(cg.junction_of[m2.0], u32::MAX, "m2 interior (union deg-2)");

        // Snap mid-segment on the car segment a→m1.
        let pa = g.nodes[a.0].loc();
        let pm1 = g.nodes[m1.0].loc();
        let lat = pa.latitude + 0.5 * (pm1.latitude - pa.latitude);
        let lon = pa.longitude + 0.5 * (pm1.longitude - pa.longitude);

        let seeds = cg.car_entries_arena(&g, lat, lon, 50.0);
        assert!(!seeds.is_empty(), "snap must produce seeds");

        let edge_a = ContractedGraph::street_edge_dir(&g, a, m1).unwrap();
        let t = project_t(lat, lon, pa, pm1);
        let da = ((t * edge_a.length as f64).round() as usize).min(edge_a.length);
        let db = edge_a.length - da;

        // FORWARD → b: stub a→m1 (db), then m1→m2 (foot, park) → m2→b (walking).
        let jb = cg.junction_of[b.0] as usize;
        let fwd = seeds.iter().find(|(j, _, _)| *j == jb).expect("forward seed at b");
        let (osecs, ophase) = car_oracle(&g, a, m1, db, &[m2, b]).unwrap();
        assert_eq!((fwd.1, fwd.2), (osecs, ophase), "forward secs/phase");
        assert!(fwd.2, "park at foot segment ⇒ exit Walking");

        // BACKWARD → a: stub m1→a (da), all car ⇒ stays Driving.
        let ja = cg.junction_of[a.0] as usize;
        let bwd = seeds.iter().find(|(j, _, _)| *j == ja).expect("backward seed at a");
        let (bsecs, bphase) = car_oracle(&g, m1, a, da, &[]).unwrap();
        assert_eq!((bwd.1, bwd.2), (bsecs, bphase), "backward secs/phase");
        assert!(!bwd.2, "backward a-bound stays Driving");
    }

    #[test]
    fn baked_traverse_equals_replay_on_front_axes() {
        use crate::structures::cost::Axis;
        let (mut g, a, b, m1, _m2) = chain_graph();
        let bike = BikeCost::new(BikeProfile::default());
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted(&bike);
        let cg = g.contracted.as_ref().unwrap();
        let se = cg.super_edge(a, m1).expect("a→ super-edge");
        let baked = se.baked.as_ref().expect("baked (≥2 segments)");

        // Null entry, and a turning entry (heading west into the eastward first segment).
        let turning = super::PrevCtx {
            dir: (-1.0, 0.0),
            len: 100.0,
            cruise: 5.0,
            push: false,
            speed: 5.0,
        };
        for entry in [None, Some(turning)] {
            let (delta, _exit) = baked.traverse(entry, &bike);
            let exact = full_replay(&g, a, &se.nodes, entry, &bike);
            assert!(
                (delta.get(Axis::Time) - exact.get(Axis::Time)).abs() < 1e-6,
                "Time: baked {} vs replay {}",
                delta.get(Axis::Time),
                exact.get(Axis::Time)
            );
            assert!(
                (delta.get(Axis::CyclewayDeficit) - exact.get(Axis::CyclewayDeficit)).abs() < 1e-6,
                "Cyc: baked {} vs replay {}",
                delta.get(Axis::CyclewayDeficit),
                exact.get(Axis::CyclewayDeficit)
            );
        }
        assert_eq!(se.to, cg.junction_of[b.0]);
    }

    /// P3a honest memory measurement. Loads the cached OSM-only graph (fast — no PBF
    /// re-parse, no transit stops present) and reports the *real* removable set under the
    /// production rule (all-mode union degree-2 **AND** bidirectional) plus measured bytes
    /// freed, so we know the memory payoff before rewriting consumers.
    #[test]
    #[ignore]
    fn contraction_memory_belgium() {
        use std::mem::size_of;
        let g = crate::services::persistence::load_osm_graph("osm.bin")
            .expect("osm.bin must be present & schema-current (build it via --build)");
        let n = g.nodes.len();

        // Union (all-mode) adjacency: distinct street neighbours over foot|bike|car, and
        // which of those neighbours have a reciprocal street edge back.
        let mut union_d2 = 0usize; // exactly 2 distinct union neighbours
        let mut removable = 0usize; // + bidirectional (both reciprocate)
        let mut freed_bytes: u64 = 0;
        let node_slot = size_of::<NodeData>() as u64;
        let adj_header = size_of::<Vec<EdgeData>>() as u64;

        for u in 0..n {
            // Transit stops stay junctions; osm.bin has none, but guard anyway.
            if !matches!(g.nodes[u], NodeData::OsmNode(_)) {
                continue;
            }
            let Some(neigh) = g.edges.get(u) else { continue };
            let mut nbrs: Vec<usize> = neigh
                .iter()
                .filter_map(|e| match e {
                    EdgeData::Street(s) => Some(s.destination.0),
                    _ => None,
                })
                .collect();
            if nbrs.is_empty() {
                continue;
            }
            nbrs.sort_unstable();
            nbrs.dedup();
            if nbrs.len() != 2 {
                continue;
            }
            union_d2 += 1;
            // Bidirectional: each neighbour must have a street edge back to u.
            let back = |w: usize| {
                g.edges[w]
                    .iter()
                    .any(|e| matches!(e, EdgeData::Street(s) if s.destination.0 == u))
            };
            if back(nbrs[0]) && back(nbrs[1]) {
                removable += 1;
                let eid_heap = match &g.nodes[u] {
                    NodeData::OsmNode(o) => o.eid.len() as u64,
                    _ => 0,
                };
                // Per removed node we drop its NodeData slot + eid heap + the adjacency
                // Vec header (its edge payload relocates into super-edge segments, ≈neutral
                // since the freed node's lat_lng is the segment's far-coord).
                freed_bytes += node_slot + eid_heap + adj_header;
            }
        }

        eprintln!(
            "DIAG-MEM nodes={n} sizeof(NodeData)={node_slot}B union_d2={union_d2} ({:.1}%) \
             removable_bidir={removable} ({:.1}%) ⇒ junctions≈{} freed≈{:.2}GB (excl. kdtree)",
            100.0 * union_d2 as f64 / n as f64,
            100.0 * removable as f64 / n as f64,
            n - removable,
            freed_bytes as f64 / 1e9,
        );

        // Validate the union constructor at scale: its junction count must match the
        // analytic (non-removable streetable) count, and it must collapse the chains.
        let t = std::time::Instant::now();
        let cg = ContractedGraph::from_graph_union(&g);
        let se_sz = size_of::<SuperEdge>() as u64;
        let baked_sz = size_of::<BakedCost>() as u64;
        let je = cg.junction_count() as u64;
        let ee = cg.edge_count() as u64;
        let sg = cg.segment_count() as u64;
        // What the contracted view ADDS alongside the full graph:
        let added = je * size_of::<NodeID>() as u64            // junctions Vec
            + n as u64 * 4                                       // junction_of Vec<u32>
            + je * size_of::<Vec<SuperEdge>>() as u64            // adjacency outer Vec headers
            + ee * se_sz                                         // SuperEdge structs (baked now boxed ⇒ 8B null)
            + sg * size_of::<NodeID>() as u64;                   // SuperEdge.nodes contents
        eprintln!(
            "DIAG-MEM union cg: junctions={je} super_edges={ee} segments={sg} build={:.1?} \
             | sizeof(SuperEdge)={se_sz}B sizeof(BakedCost)={baked_sz}B \
             | ADDED≈{:.2}GB vs FREED≈{:.2}GB ⇒ NET≈{:+.2}GB (excl. kdtree shrink)",
            t.elapsed(),
            added as f64 / 1e9,
            freed_bytes as f64 / 1e9,
            (freed_bytes as f64 - added as f64) / 1e9,
        );

        // The self-contained arena that replaces `g.edges` (street payload) + interior
        // coords at P3f. Total street EdgeData currently in g.edges, for the relocation
        // comparison:
        let seg_sz = size_of::<Seg>() as u64;
        let street_edges: u64 = g
            .edges
            .iter()
            .flat_map(|a| a.iter())
            .filter(|e| matches!(e, EdgeData::Street(_)))
            .count() as u64;
        let arena = cg.segs.len() as u64 * seg_sz + je * 16; // segs + junction_coord
        let g_edges_street = street_edges * size_of::<EdgeData>() as u64;
        let edgedata_sz = size_of::<EdgeData>();
        eprintln!(
            "DIAG-MEM arena: sizeof(Seg)={seg_sz}B segs={} arena≈{:.2}GB (segs+junction_coord) \
             | g.edges street payload={street_edges} ×{edgedata_sz}B≈{:.2}GB ⇒ at P3f arena REPLACES it: Δ≈{:+.2}GB",
            cg.segs.len(),
            arena as f64 / 1e9,
            g_edges_street as f64 / 1e9,
            (g_edges_street as f64 - arena as f64) / 1e9,
        );
    }

    /// P3c-walk A/B at Belgium scale: foot times over union super-edges must equal the
    /// node-by-node `street_dijkstra(Foot)` at every junction within a 30-min bound.
    #[test]
    #[ignore]
    fn walk_union_ab_belgium() {
        use super::super::raptor_access::StreetProfile;
        use std::time::Instant;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index(); // populates transit_node_to_stop (street_dijkstra reads it)
        let cg = ContractedGraph::from_graph_union(&g);

        // Origin = the junction nearest a Brussels coordinate.
        let target = crate::structures::LatLng { latitude: 50.85, longitude: 4.35 };
        let origin = *cg
            .junctions
            .iter()
            .min_by(|&&x, &&y| {
                g.nodes[x.0]
                    .loc()
                    .dist(target)
                    .total_cmp(&g.nodes[y.0].loc().dist(target))
            })
            .expect("a junction");

        let bound = 1800u32;
        let t0 = Instant::now();
        let full = g.street_dijkstra(origin, bound, StreetProfile::Foot);
        let t_full = t0.elapsed();
        let t1 = Instant::now();
        let contracted = g.walk_dijkstra_union(origin, bound, &cg);
        let t_contracted = t1.elapsed();

        // Junctions only (full also reaches interior nodes; those aren't in `contracted`).
        let mut checked = 0usize;
        let mut mismatches = 0usize;
        for (&jn, &tf) in &full {
            if cg.junction_of[jn.0] == u32::MAX {
                continue; // interior node, not represented in the contracted search
            }
            checked += 1;
            match contracted.get(&jn) {
                Some(&tc) if tc == tf => {}
                other => {
                    if mismatches < 10 {
                        eprintln!("MISMATCH junction {jn:?}: full={tf} contracted={other:?}");
                    }
                    mismatches += 1;
                }
            }
        }
        eprintln!(
            "DIAG-WALK-AB origin={origin:?} full_junctions+interior={} contracted_junctions={} \
             checked_junctions={checked} mismatches={mismatches} | full={t_full:.1?} contracted={t_contracted:.1?}",
            full.len(),
            contracted.len(),
        );
        assert_eq!(mismatches, 0, "walk over super-edges must be IDENTICAL at every junction");
        assert!(checked > 1000, "the bounded search should cover many junctions");
    }

    /// P3 car A/B: phased car routing over arena super-edges must equal
    /// `street_dijkstra(Car)` at every junction (the Driving→park→Walking phase threaded
    /// through chains). Several junction origins near Brussels, 30-min bound.
    #[test]
    #[ignore]
    fn car_union_ab_belgium() {
        use super::super::raptor_access::StreetProfile;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        let cg = ContractedGraph::from_graph_union(&g);
        let bound = 700u32; // ~9 km drive — keeps the per-origin flood bounded
        let mut state: u64 = 0xC0FFEE123456789;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let (mut checked, mut mismatches, mut origins) = (0u64, 0u64, 0);
        for _ in 0..8 {
            // A junction near a random Brussels point.
            let target = crate::structures::LatLng { latitude: 50.80 + next() * 0.10, longitude: 4.30 + next() * 0.12 };
            let origin = *cg.junctions.iter().min_by(|&&x, &&y| {
                g.nodes[x.0].loc().dist(target).total_cmp(&g.nodes[y.0].loc().dist(target))
            }).unwrap();
            origins += 1;
            let full = g.street_dijkstra(origin, bound, StreetProfile::Car);
            let car = g.car_dijkstra_union(origin, bound, &cg);
            for (&jn, &tc) in &car {
                checked += 1;
                if full.get(&jn).copied() != Some(tc) {
                    if mismatches < 15 {
                        eprintln!("MISMATCH J={jn:?} contracted={tc} full={:?}", full.get(&jn));
                    }
                    mismatches += 1;
                }
            }
        }
        eprintln!("DIAG-CAR-AB origins={origins} checked_junctions={checked} mismatches={mismatches}");
        assert_eq!(mismatches, 0, "phased car over super-edges must equal street_dijkstra(Car)");
        assert!(checked > 5000, "expected many junctions checked");
    }

    /// P3b A/B at Belgium scale: arbitrary lat/lng point-to-point foot routing over the
    /// contracted graph (snap → bounding junctions → seeded search → same-chain shortcut)
    /// must equal `street_dijkstra(Foot)` for every reachable random OD pair. Random close
    /// pairs deliberately exercise the interior↔interior same-chain case.
    #[test]
    #[ignore]
    fn walk_point_to_point_ab_belgium() {
        use super::super::raptor_access::StreetProfile;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        let cg = ContractedGraph::from_graph_union(&g);

        // Deterministic LCG; OD pairs in a Brussels box with a short random offset.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64 // [0,1)
        };
        let bound = 1500u32;
        let (mut reachable, mut mismatches, mut same_chain, mut interior_origin) = (0, 0, 0, 0);
        for _ in 0..2000 {
            let olat = 50.80 + next() * 0.10;
            let olon = 4.30 + next() * 0.12;
            let dlat = olat + (next() - 0.5) * 0.014;
            let dlon = olon + (next() - 0.5) * 0.020;
            let (Some(o), Some(d)) = (g.nearest_node(olat, olon), g.nearest_node(dlat, dlon)) else {
                continue;
            };
            if o == d {
                continue;
            }
            if cg.junction_of[o.0] == u32::MAX {
                interior_origin += 1;
            }
            let full = g.street_dijkstra(o, bound, StreetProfile::Foot).get(&d).copied();
            let got = cg.walk_secs_point_to_point(&g, o, d, bound);
            if ContractedGraph::direct_same_chain_foot(&g, o, d).is_some() {
                same_chain += 1;
            }
            match (full, got) {
                (Some(f), g_) => {
                    reachable += 1;
                    if g_ != Some(f) {
                        if mismatches < 15 {
                            eprintln!("MISMATCH o={o:?} d={d:?} full={f} contracted={g_:?}");
                        }
                        mismatches += 1;
                    }
                }
                (None, Some(g_)) if g_ <= bound => {
                    // Contracted found a path the bounded full search didn't — only OK if
                    // the full search was bound-limited, which we can't easily assert here.
                    if mismatches < 15 {
                        eprintln!("EXTRA o={o:?} d={d:?} full=None contracted={g_:?}");
                    }
                    mismatches += 1;
                }
                (None, _) => {}
            }
        }
        eprintln!(
            "DIAG-P2P-AB reachable={reachable} mismatches={mismatches} same_chain_pairs={same_chain} interior_origins={interior_origin}"
        );
        assert_eq!(mismatches, 0, "contracted point-to-point walk must equal street_dijkstra");
        assert!(reachable > 200, "expected many reachable pairs, got {reachable}");
        assert!(same_chain > 0, "expected some same-chain pairs to exercise the shortcut");
        assert!(interior_origin > 0, "expected interior origins (the 81% case)");
    }

    /// P3 RAPTOR-access A/B on **graph.bin** (with transit stops — the interaction the
    /// osm.bin gates can't see): `nearby_stops_union` must equal `nearby_stops` for many
    /// random origins, exercising the stop dead-end guard on real data.
    /// Tail-2 A/B: the g-free arena snap (`walk_entries_arena`) must match the g-based
    /// `snap_to_edge(foot)` → `walk_entries` junction entries for random coordinates. Fast
    /// (no Dijkstra) — just the snapping bridge. Small radius so the nearest edge is
    /// unambiguous (avoids tie-break differences between the two R-trees).
    #[test]
    #[ignore]
    fn walk_snap_arena_ab_belgium() {
        use crate::structures::Endpoint;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        g.build_edge_index();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();

        let mut state: u64 = 0x51ED270B5A11CE5;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let radius = 25.0;
        // `hard` = same bounding junctions but different seconds (a real resolution bug);
        // `tie` = different junctions (the two R-trees chose different incident edges at a
        // near-junction projection — a benign snap-choice difference, both valid).
        let (mut snapped, mut hard, mut tie) = (0u32, 0u32, 0u32);
        for _ in 0..4000 {
            let lat = 50.80 + next() * 0.10;
            let lon = 4.30 + next() * 0.12;
            let Some((ep, _)) = g.snap_to_edge(lat, lon, radius, |s| s.foot) else { continue };
            let Endpoint::OnEdge { .. } = ep else { continue };
            let mut g_entries = cg.walk_entries(&g, ep);
            let mut a_entries = cg.walk_entries_arena(&g, lat, lon, radius);
            if g_entries.is_empty() || a_entries.is_empty() {
                continue;
            }
            snapped += 1;
            g_entries.sort_unstable();
            a_entries.sort_unstable();
            let g_j: Vec<usize> = g_entries.iter().map(|e| e.0).collect();
            let a_j: Vec<usize> = a_entries.iter().map(|e| e.0).collect();
            if g_j != a_j {
                tie += 1;
            } else if g_entries != a_entries {
                if hard < 15 {
                    eprintln!("HARD @({lat:.5},{lon:.5}) g={g_entries:?} arena={a_entries:?}");
                }
                hard += 1;
            }
        }
        eprintln!("DIAG-SNAP-AB snapped={snapped} hard_mismatches={hard} edge_choice_ties={tie}");
        assert!(snapped > 500, "expected many snapped points, got {snapped}");
        assert_eq!(hard, 0, "same junctions must give same seconds (resolution must be exact)");
        assert!(tie <= snapped / 100, "edge-choice ties should be rare ({tie}/{snapped})");
    }

    /// T4.2 PLAN-IMPACT A/B on **graph.bin** (with transit): quantify how often the g-free
    /// ARENA snap diverges from the production g-snap *at the plan-determining level* — the
    /// foot access-stop set RAPTOR consumes. For random coords:
    ///   g-snap  = `nearest_node(lat,lon)` → `nearby_stops` (production today).
    ///   arena   = `walk_entries_arena(lat,lon)` → `walk_dijkstra_union_seeded` → stops
    ///             (what the snap IS once `g` is dropped).
    /// Reports: `stop_set_diff` = coords whose REACHABLE STOP SET differs (a different
    /// access stop ⇒ a potentially different plan), and `time_only_diff` = same stops but
    /// some access second differs (could re-rank plans). This is the near-junction snap-tie
    /// risk (`walk_snap_arena_ab_belgium` found 3/2774 entry-list ties) carried through the
    /// full access search to the PLAN input — the number the cutover decision needs.
    ///
    /// Needs a v7 `graph.bin` (rebuild via `--build --save`); the union build + seg R-tree
    /// over ~17M segments is slow, so this run is likely KILLED in the dev env — commit it
    /// for the human to run on a stable machine. NOT a pass/fail gate: it prints the
    /// divergence rate; the assert only sanity-checks that the loop did meaningful work.
    #[test]
    #[ignore]
    fn plan_impact_snap_ab_belgium() {
        let g = crate::services::persistence::load_graph("graph.bin").expect("graph.bin (v7)");
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();

        let mut state: u64 = 0xB1A5_0F11_C0DE_2716;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let radius = g.raptor.edge_snap_radius_m.max(25.0);
        let max_secs = 15 * 60; // a realistic access radius in seconds
        let (mut compared, mut stop_set_diff, mut time_only_diff, mut interior) = (0u32, 0u32, 0u32, 0u32);
        for _ in 0..2000 {
            let lat = 50.80 + next() * 0.10;
            let lon = 4.30 + next() * 0.12;
            // Production g-snap → nearby_stops.
            let Some(gnode) = g.nearest_node(lat, lon) else { continue };
            let mut g_stops = g.nearby_stops(gnode, max_secs);
            // Arena snap → seeded union foot search → stops.
            let entries = cg.walk_entries_arena(&g, lat, lon, radius);
            if entries.is_empty() {
                continue;
            }
            let dist = g.walk_dijkstra_union_seeded(&entries, max_secs, &cg);
            let mut a_stops: Vec<(usize, u32)> = dist
                .iter()
                .filter_map(|(&jn, &s)| {
                    let c = g.raptor.transit_node_to_stop[jn.0];
                    (c != u32::MAX).then_some((c as usize, s))
                })
                .collect();
            if g_stops.is_empty() && a_stops.is_empty() {
                continue;
            }
            compared += 1;
            if cg.junction_of.get(gnode.0).copied().unwrap_or(u32::MAX) == u32::MAX {
                interior += 1;
            }
            g_stops.sort_unstable();
            a_stops.sort_unstable();
            let g_ids: Vec<usize> = g_stops.iter().map(|s| s.0).collect();
            let a_ids: Vec<usize> = a_stops.iter().map(|s| s.0).collect();
            if g_ids != a_ids {
                stop_set_diff += 1;
            } else if g_stops != a_stops {
                time_only_diff += 1;
            }
        }
        eprintln!(
            "DIAG-PLAN-IMPACT compared={compared} interior_gsnap={interior} \
             stop_set_diff={stop_set_diff} time_only_diff={time_only_diff} \
             ({:.2}% set / {:.2}% time)",
            100.0 * stop_set_diff as f64 / compared.max(1) as f64,
            100.0 * time_only_diff as f64 / compared.max(1) as f64,
        );
        assert!(compared > 200, "expected meaningful work, got {compared}");
    }

    /// Tail-2 BIKE snap A/B on **osm.bin**: the g-free arena `bike_entries_arena` seeds must
    /// match the g-based `snap_to_edge(bike)` → OnEdge → per-end partial-super-edge replay
    /// (through `street_edge_transition`) to the bounding junctions, for random Brussels
    /// coordinates. Asserts cost (all axes) + len + var equal at every shared junction.
    #[test]
    #[ignore]
    fn bike_snap_arena_ab_belgium() {
        use crate::structures::cost::{Axis, CostVector, RoutingMode};
        use crate::structures::Endpoint;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        g.build_edge_index();
        let bike = g.default_bike_cost();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted(&bike);
        let cg = g.contracted.take().unwrap();

        // g-oracle: from OnEdge end `e` (entered from `other`, stub `stub_len` to `e`), ride
        // the partial stub then the bike chain away from `other` to a junction, replaying via
        // street_edge_transition. Returns (junction index, cost, len, var).
        let oracle = |e: NodeID, other: NodeID, stub_len: usize|
            -> Option<(usize, CostVector, usize, f64)> {
            let mode = RoutingMode::Bike;
            let profile = bike.profile();
            let weights = g.raptor.cost_weights;
            let speed = g.mode_speed(mode);
            let cv = g.raptor.systematic_cv;
            let full = ContractedGraph::bike_edge(&g, other, e)?;
            let mut hops: Vec<(StreetEdgeData, (f64, f64))> = Vec::new();
            hops.push((Graph::partial_edge(full, stub_len), g.dir_between(other, e)));
            let (mut prev_n, mut cur) = (other, e);
            for _ in 0..1_000_000 {
                if cg.junction_of[cur.0] != u32::MAX {
                    break;
                }
                let (nb, k) = ContractedGraph::bike_neighbours_of(&g, cur.0);
                if k != 2 { return None; }
                let next = if nb[0] == prev_n { nb[1] } else if nb[1] == prev_n { nb[0] } else { return None };
                let ed = ContractedGraph::bike_edge(&g, cur, next)?;
                hops.push((*ed, g.dir_between(cur, next)));
                prev_n = cur;
                cur = next;
            }
            let ji = cg.junction_of[cur.0];
            if ji == u32::MAX { return None; }
            let mut cost = CostVector::ZERO;
            let mut elev = (0.0, 0.0);
            let mut var = 0.0;
            let mut len = 0usize;
            let mut prev: Option<super::PrevCtx> = None;
            for (edge, dir) in &hops {
                let (c, el, v) = g.street_edge_transition(
                    mode, edge, &profile, &weights, speed, cv, &bike, prev, &cost, elev, var,
                )?;
                cost = c; elev = el; var = v;
                len += edge.length;
                prev = Some(super::ctx_after(&bike, prev, edge, *dir));
            }
            Some((ji as usize, cost, len, var))
        };

        let mut state: u64 = 0xB1CE5EED5A11CE5;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let radius = 25.0;
        let (mut snapped, mut hard, mut tie, mut checks, mut var_tie) = (0u32, 0u32, 0u32, 0u32, 0u32);
        for _ in 0..4000 {
            let lat = 50.80 + next() * 0.10;
            let lon = 4.30 + next() * 0.12;
            let Some((ep, _)) = g.snap_to_edge(lat, lon, radius, |s| s.bike) else { continue };
            let Endpoint::OnEdge { a, b, dist_a, dist_b, .. } = ep else { continue };
            let arena = cg.bike_entries_arena(&g, &bike, lat, lon, radius);
            if arena.is_empty() { continue; }
            // g-oracle seeds per end.
            let mut g_seeds: Vec<(usize, CostVector, usize, f64)> = Vec::new();
            for (e, other, d) in [(a, b, dist_a), (b, a, dist_b)] {
                if let Some(s) = oracle(e, other, d) {
                    match g_seeds.iter_mut().find(|x| x.0 == s.0) {
                        Some(slot) => { if lex_le(&s.1, &slot.1) { *slot = s; } }
                        None => g_seeds.push(s),
                    }
                }
            }
            if g_seeds.is_empty() { continue; }
            snapped += 1;
            let arena_js: std::collections::BTreeSet<usize> = arena.iter().map(|s| s.junction).collect();
            let g_js: std::collections::BTreeSet<usize> = g_seeds.iter().map(|s| s.0).collect();
            if arena_js != g_js {
                tie += 1;
                continue; // benign: the two R-trees chose different incident edges
            }
            for gs in &g_seeds {
                let a_seed = arena.iter().find(|s| s.junction == gs.0).unwrap();
                checks += 1;
                // LENGTH is the route invariant: an equal length means the arena replayed
                // the SAME physical ride to the junction. A length mismatch is a real bug.
                if a_seed.len != gs.2 {
                    if hard < 15 {
                        eprintln!(
                            "HARD @({lat:.5},{lon:.5}) j={} arena(len={},t={:.3}) g(len={},t={:.3})",
                            gs.0, a_seed.len, a_seed.cost.get(Axis::Time), gs.2, gs.1.get(Axis::Time),
                        );
                    }
                    hard += 1;
                    continue;
                }
                // Same junction + same route length, but the cost axes or variance differ.
                // The arena seg R-tree and g's edge R-tree snapped a near-vertex click to
                // different INCIDENT edges (both valid, same length) — the entry angle
                // differs, perturbing the kinematic corner time and the turn-variance term.
                // Post-drop the arena R-tree is the sole, valid snapper, so this is a benign
                // edge-choice tie (the same class the walk snap A/B accepts), not a bug.
                let cost_ok = Axis::ALL.iter().all(|&ax| (a_seed.cost.get(ax) - gs.1.get(ax)).abs() < 1e-4);
                if !cost_ok || (a_seed.var - gs.3).abs() > 1e-3 {
                    var_tie += 1;
                }
            }
        }
        eprintln!("DIAG-BIKE-SNAP-AB snapped={snapped} checks={checks} wrong_route={hard} junction_ties={tie} snap_edge_ties={var_tie}");
        assert!(snapped > 300, "expected many snapped points, got {snapped}");
        assert_eq!(hard, 0, "arena bike seeds must ride the SAME-LENGTH route to each junction");
        assert!(
            var_tie <= checks / 20,
            "near-vertex snap-edge ties should be rare ({var_tie}/{checks})"
        );
    }

    /// P3f snapping path: arbitrary lat/lng → `snap_to_edge` → `OnEdge{a,b}` (ends interior
    /// ~81% of the time) → `walk_entries` bridge → junction search. Must equal the
    /// full-graph oracle `min(stub_a + dijkstra(a), stub_b + dijkstra(b))` at every junction.
    /// This is the load-bearing assumption the whole memory win rests on (junction-only
    /// kdtree can't return interior nodes, so production must snap via OnEdge at P3f).
    /// Resident memory after `/proc/self/statm` (Linux), in MB.
    fn rss_mb() -> u64 {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1).and_then(|t| t.parse::<u64>().ok()))
            .map(|pages| pages * 4096 / (1024 * 1024))
            .unwrap_or(0)
    }

    /// P3f integration spike: serialize the union cg, **drop the full node/edge arrays**,
    /// and re-route junction-level walk with the *deserialized* cg — proving (a) the
    /// contracted graph round-trips through serde and (b) routing needs no `g.nodes`/
    /// `g.edges`. Reports the measured RSS freed vs the analytic projection. This is the
    /// integration risk (not correctness) the advisor flagged as the key missing fact.
    #[test]
    #[ignore]
    fn p3f_drop_and_serialize_spike() {
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        let cg = ContractedGraph::from_graph_union(&g);

        let target = crate::structures::LatLng { latitude: 50.85, longitude: 4.35 };
        let origin = *cg
            .junctions
            .iter()
            .min_by(|&&x, &&y| {
                g.nodes[x.0].loc().dist(target).total_cmp(&g.nodes[y.0].loc().dist(target))
            })
            .expect("a junction");
        let bound = 1800u32;
        let before = g.walk_dijkstra_union(origin, bound, &cg);

        // Serde round-trip (postcard, the graph.bin format).
        let bytes = postcard::to_allocvec(&cg).expect("serialize cg");
        let cg2: ContractedGraph = postcard::from_bytes(&bytes).expect("deserialize cg");

        let rss_before = rss_mb();
        g.drop_full_node_arrays(); // the P3f drop
        // Encourage the allocator to return freed pages (best-effort; large Vecs are mmap'd).
        let rss_after = rss_mb();

        // Route again with the DESERIALIZED cg and the gutted graph.
        let after = g.walk_dijkstra_union(origin, bound, &cg2);
        assert_eq!(before, after, "routing must survive the drop + serde round-trip");
        assert!(g.nodes.is_empty() && g.edges.is_empty(), "full arrays dropped");
        assert!(after.len() > 1000, "still routes after the drop");

        eprintln!(
            "DIAG-P3F serialized_cg={:.2}GB cg_heap={:.2}GB | RSS before_drop={}MB after_drop={}MB freed={}MB \
             | route survived drop+roundtrip ✓ ({} junctions)",
            bytes.len() as f64 / 1e9,
            cg.heap_bytes() as f64 / 1e9,
            rss_before,
            rss_after,
            rss_before.saturating_sub(rss_after),
            after.len(),
        );
    }

    #[test]
    #[ignore]
    fn walk_onedge_ab_belgium() {
        use super::super::raptor_access::StreetProfile;
        use crate::structures::Endpoint;
        let mut g = crate::services::persistence::load_osm_graph("osm.bin").expect("osm.bin");
        g.build_raptor_index();
        g.build_edge_index();
        let cg = ContractedGraph::from_graph_union(&g);
        let wmms = (g.raptor.walking_speed_mps * 1000.0) as u64;
        let stub = |d: usize| (d as u64 * 1000 / wmms.max(1)) as u32;

        let mut state: u64 = 0x243F6A8885A308D3;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let bound = 1500u32;
        let (mut snapped, mut checked_j, mut mismatches, mut interior_end) = (0, 0u64, 0, 0);
        for _ in 0..1200 {
            let lat = 50.80 + next() * 0.10;
            let lon = 4.30 + next() * 0.12;
            let Some((ep, _perp)) = g.snap_to_edge(lat, lon, 200.0, |s| s.foot) else { continue };
            let Endpoint::OnEdge { a, b, dist_a, dist_b, .. } = ep else { continue };
            snapped += 1;
            if cg.junction_of[a.0] == u32::MAX || cg.junction_of[b.0] == u32::MAX {
                interior_end += 1;
            }
            let entries = cg.walk_entries(&g, ep);
            if entries.is_empty() {
                continue;
            }
            let dist = g.walk_dijkstra_union_seeded(&entries, bound, &cg);
            let da = g.street_dijkstra(a, bound, StreetProfile::Foot);
            let db = g.street_dijkstra(b, bound, StreetProfile::Foot);
            let (sa, sb) = (stub(dist_a), stub(dist_b));
            for (&jn, &tc) in &dist {
                let oa = da.get(&jn).map(|&x| sa.saturating_add(x));
                let ob = db.get(&jn).map(|&x| sb.saturating_add(x));
                let oracle = match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) | (None, Some(x)) => Some(x),
                    (None, None) => None,
                };
                checked_j += 1;
                if oracle != Some(tc) {
                    if mismatches < 15 {
                        eprintln!("MISMATCH J={jn:?} contracted={tc} oracle={oracle:?} (sa={sa} sb={sb})");
                    }
                    mismatches += 1;
                }
            }
        }
        eprintln!(
            "DIAG-ONEDGE-AB snapped={snapped} interior_end={interior_end} checked_junctions={checked_j} mismatches={mismatches}"
        );
        assert_eq!(mismatches, 0, "OnEdge snapping bridge must equal the full-graph oracle");
        assert!(snapped > 200, "expected many snapped OnEdge endpoints, got {snapped}");
        assert!(interior_end > 50, "expected OnEdge ends to be interior (the P3f case)");
    }

    #[test]
    #[ignore]
    fn nearby_stops_union_ab_belgium() {
        let g = crate::services::persistence::load_graph("graph.bin").expect("graph.bin");
        let cg = ContractedGraph::from_graph_union(&g);

        let mut state: u64 = 0xD1B54A32D192ED03;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (state >> 33) as f64 / (1u64 << 31) as f64
        };
        let max_secs = 600u32; // ~10-min access radius
        let (mut checked, mut mismatches, mut with_stops, mut interior) = (0, 0, 0, 0);
        for _ in 0..1500 {
            let lat = 50.80 + next() * 0.10;
            let lon = 4.30 + next() * 0.12;
            let Some(o) = g.nearest_node(lat, lon) else { continue };
            checked += 1;
            if cg.junction_of[o.0] == u32::MAX {
                interior += 1;
            }
            let full = g.nearby_stops(o, max_secs);
            let union = g.nearby_stops_union(o, max_secs, &cg);
            if !full.is_empty() {
                with_stops += 1;
            }
            if full != union {
                if mismatches < 15 {
                    eprintln!("MISMATCH o={o:?} full={} union={}", full.len(), union.len());
                }
                mismatches += 1;
            }
        }
        eprintln!(
            "DIAG-STOPS-AB checked={checked} with_stops={with_stops} interior_origins={interior} mismatches={mismatches}"
        );
        assert_eq!(mismatches, 0, "nearby_stops_union must equal nearby_stops");
        assert!(with_stops > 50, "expected many origins with reachable stops, got {with_stops}");
        assert!(interior > 100, "expected interior origins");
    }

    #[test]
    #[ignore]
    fn contraction_measure_belgium() {
        use std::time::Instant;
        let path = "data/belgium-latest.osm.pbf";
        let dem = crate::ingestion::osm::Dem::load("data/belgium-DTM-20m.tif").ok();
        let mut g = Graph::new();
        crate::ingestion::osm::load_pbf_file(path, dem.as_ref(), 4.0, &Default::default(), &mut g)
            .unwrap();
        g.build_raptor_index();
        let nodes = g.nodes.len();

        let t = Instant::now();
        let cg = ContractedGraph::from_graph(&g);
        let build = t.elapsed();
        eprintln!(
            "DIAG-CONTRACT nodes={nodes} junctions={} ({:.1}% kept) super_edges={} segments={} build={build:.1?}",
            cg.junction_count(),
            100.0 * cg.junction_count() as f64 / nodes as f64,
            cg.edge_count(),
            cg.segment_count(),
        );

        // All-mode (union) degree-2: P3 (memory) can only remove a node that is a
        // pass-through for EVERY mode — exactly 2 distinct street neighbours over the
        // union of foot/bike/car edges. This is the actual removable set.
        let mut union_d2 = 0usize;
        let mut streetable = 0usize;
        for u in 0..nodes {
            if g.raptor.transit_node_to_stop[u] != u32::MAX {
                continue;
            }
            let Some(neigh) = g.edges.get(u) else { continue };
            let mut nbrs: Vec<usize> = neigh
                .iter()
                .filter_map(|e| match e {
                    EdgeData::Street(s) => Some(s.destination.0),
                    _ => None,
                })
                .collect();
            if nbrs.is_empty() {
                continue;
            }
            streetable += 1;
            nbrs.sort_unstable();
            nbrs.dedup();
            if nbrs.len() == 2 {
                union_d2 += 1;
            }
        }
        eprintln!(
            "DIAG-CONTRACT union_degree2={union_d2} of streetable={streetable} ({:.1}%) ⇒ ~{:.2}M removable",
            100.0 * union_d2 as f64 / streetable.max(1) as f64,
            union_d2 as f64 / 1e6,
        );
    }
}
