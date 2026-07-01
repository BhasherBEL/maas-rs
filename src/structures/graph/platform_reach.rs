//! Stage B1 connector-coverage measurement (the B2 go/no-go instrument).
//!
//! For each Stage-A-matched OSM platform, decide whether its platform polyline is
//! reachable from the surrounding ground-level street graph via a **level-continuous
//! pedestrian path** that crosses at least one vertical connector (stairs / elevator /
//! ramp). In well-mapped OSM the only edges joining a `level=1` platform to a `level=0`
//! concourse are connectors, so plain foot connectivity from the platform to a ground
//! street node already implies a connector was crossed — we additionally require a
//! connector edge on the path so a flat "teleport" footway sharing a platform node is
//! NOT counted (it lands in `no_vertical_path`, where it belongs).
//!
//! This module only READS the graph (foot edges + auxiliary `node_levels` /
//! `connector_edges`). It relocates nothing and changes no routing state.

use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::structures::{EdgeData, LatLng, NodeID};

use super::Graph;

/// Outcome of the connector-reach search for a single platform.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConnectorReach {
    /// A ground-level street node was reached on a path crossing ≥1 connector edge.
    pub reachable_via_connector: bool,
    /// Walk distance (m) of that connector path to the nearest such ground node.
    pub path_dist_m: Option<f64>,
    /// Straight-line distance (m) platform centroid → that ground node.
    pub straight_m: Option<f64>,
}

impl ConnectorReach {
    fn none() -> Self {
        ConnectorReach {
            reachable_via_connector: false,
            path_dist_m: None,
            straight_m: None,
        }
    }
}

/// Min-heap entry for the 2-state (node, used-connector) Dijkstra.
#[derive(PartialEq, Eq)]
struct QEntry {
    dist: usize,
    node: NodeID,
    used: bool,
}
impl Ord for QEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.dist.cmp(&self.dist) // reversed → min-heap
    }
}
impl PartialOrd for QEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Graph {
    fn osm_loc(&self, id: NodeID) -> Option<LatLng> {
        self.get_node(id).map(|n| n.loc())
    }

    /// True when `id` is a ground-level OSM node usable as a reach target: not a
    /// platform node, its `level` absent or 0, and it carries at least one foot
    /// street edge (i.e. it is part of the walkable street graph).
    fn is_ground_street_node(&self, id: NodeID, platform_nodes: &HashSet<NodeID>) -> bool {
        if platform_nodes.contains(&id) {
            return false;
        }
        if self.node_level(id).is_some_and(|l| l != 0) {
            return false;
        }
        self.edges[id.0]
            .iter()
            .any(|e| matches!(e, EdgeData::Street(s) if s.foot))
    }

    /// Bounded multi-source foot Dijkstra from a platform's `start` nodes. Returns
    /// the nearest ground street node reachable on a path that crosses ≥1 connector
    /// edge, within `budget_m`. The 2-state label `(node, used_connector)` lets a
    /// flat path and a vertical path to the same node coexist, so a connector path
    /// is found even when a shorter flat one exists.
    pub fn platform_connector_reach(
        &self,
        start: &[NodeID],
        centroid: LatLng,
        platform_nodes: &HashSet<NodeID>,
        budget_m: usize,
    ) -> ConnectorReach {
        if start.is_empty() {
            return ConnectorReach::none();
        }
        let mut best: HashMap<(NodeID, bool), usize> = HashMap::new();
        let mut heap: BinaryHeap<QEntry> = BinaryHeap::new();
        for &s in start {
            if best.insert((s, false), 0).is_none() {
                heap.push(QEntry { dist: 0, node: s, used: false });
            }
        }

        while let Some(QEntry { dist, node, used }) = heap.pop() {
            if dist > budget_m {
                break;
            }
            if best.get(&(node, used)).is_some_and(|&d| d < dist) {
                continue;
            }
            // Target test: reached a ground street node via a connector path.
            if used && self.is_ground_street_node(node, platform_nodes) {
                let straight = self.osm_loc(node).map(|l| centroid.dist(l));
                return ConnectorReach {
                    reachable_via_connector: true,
                    path_dist_m: Some(dist as f64),
                    straight_m: straight,
                };
            }
            for e in &self.edges[node.0] {
                let EdgeData::Street(s) = e else { continue };
                if !s.foot {
                    continue;
                }
                let v = s.destination;
                let nd = dist + s.length;
                if nd > budget_m {
                    continue;
                }
                let nused = used || self.connector_kind(node, v).is_some();
                let key = (v, nused);
                if best.get(&key).is_none_or(|&d| nd < d) {
                    best.insert(key, nd);
                    heap.push(QEntry { dist: nd, node: v, used: nused });
                }
            }
        }
        ConnectorReach::none()
    }

    /// Bounded foot-only Dijkstra from `start`, returning the lowest-cost reachable
    /// node in `targets` and its raw-metre cost, or `None` if no target is reachable
    /// within `budget` metres. Exits as soon as the first target node is popped from
    /// the heap — Dijkstra settles in non-decreasing order, so that is the global
    /// minimum. Uses raw edge lengths (metres), not baked stair-time lengths, because
    /// this is called during the GTFS phase before `bake_connector_lengths` runs.
    pub fn foot_reach_to_targets(
        &self,
        start: NodeID,
        targets: &HashSet<NodeID>,
        budget: usize,
    ) -> Option<(NodeID, usize)> {
        if targets.is_empty() {
            return None;
        }
        let mut best: HashMap<NodeID, usize> = HashMap::new();
        let mut heap: BinaryHeap<QEntry> = BinaryHeap::new();
        best.insert(start, 0);
        heap.push(QEntry { dist: 0, node: start, used: false });

        while let Some(QEntry { dist, node, used: _ }) = heap.pop() {
            if dist > budget {
                break;
            }
            if best.get(&node).is_some_and(|&d| d < dist) {
                continue;
            }
            if targets.contains(&node) {
                return Some((node, dist));
            }
            for e in &self.edges[node.0] {
                let EdgeData::Street(s) = e else { continue };
                if !s.foot {
                    continue;
                }
                let v = s.destination;
                let nd = dist + s.length;
                if nd > budget {
                    continue;
                }
                if best.get(&v).is_none_or(|&d| nd < d) {
                    best.insert(v, nd);
                    heap.push(QEntry { dist: nd, node: v, used: false });
                }
            }
        }
        None
    }

    /// Union of all platform polyline node IDs across the index — the exclusion set
    /// for ground-node identification.
    pub fn all_platform_nodes(&self) -> HashSet<NodeID> {
        let mut set = HashSet::new();
        let idx = self.platform_index();
        for i in 0..idx.len() {
            if let Some(p) = idx.platform(i) {
                set.extend(p.node_ids.iter().copied());
            }
        }
        set
    }

    /// Stage B1 coarse accessibility / sanity indicator. Returns
    /// `(transit_stops, reachable_after, reachable_before)`:
    /// - `reachable_after`: stop has ≥1 foot edge into the OSM graph (its snap edge).
    /// - `reachable_before`: same, ignoring the newly-imported platform polyline
    ///   nodes as edge targets.
    ///
    /// The two counts are NOT equal (a real build measured after=5483, before=4047):
    /// some stops legitimately snap to nodes shared by a street and a platform, which
    /// `before` discounts. So this is only a rough sanity/accessibility signal, never a
    /// non-regression proof — `after >= before` holds for ANY graph and proves nothing.
    /// The REAL non-regression guarantee is structural: the GTFS stop-snap loop and
    /// `validate_way` are unchanged, and platform-only nodes are excluded from the snap
    /// KD-tree, so B1 only adds foot edges and removes none — no stop snap can move.
    pub fn transit_stops_reachable(&self, platform_nodes: &HashSet<NodeID>) -> (usize, usize, usize) {
        let mut total = 0usize;
        let mut after = 0usize;
        let mut before = 0usize;
        for (i, node) in self.nodes.iter().enumerate() {
            if !matches!(node, crate::structures::NodeData::TransitStop(_)) {
                continue;
            }
            total += 1;
            let edges = &self.edges[i];
            let has_after = edges
                .iter()
                .any(|e| matches!(e, EdgeData::Street(s) if s.foot));
            let has_before = edges.iter().any(|e| {
                matches!(e, EdgeData::Street(s) if s.foot && !platform_nodes.contains(&s.destination))
            });
            if has_after {
                after += 1;
            }
            if has_before {
                before += 1;
            }
        }
        (total, after, before)
    }
}
