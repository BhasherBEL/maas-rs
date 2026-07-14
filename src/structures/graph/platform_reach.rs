//! Platform reachability from ground-level street graph. A path must cross a
//! vertical connector (stairs/elevator/ramp) to count as reachable.

use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::structures::{EdgeData, LatLng, NodeID};

use super::Graph;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConnectorReach {
    pub reachable_via_connector: bool,
    pub path_dist_m: Option<f64>,
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

    /// 2-state label `(node, used_connector)` so a connector path is found even
    /// when a shorter flat path to the same node exists.
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

    /// Uses raw edge lengths (metres), not baked stair-time lengths: runs before
    /// `bake_connector_lengths`.
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

    /// Returns `(transit_stops, reachable_after, reachable_before)`. Rough sanity
    /// signal only, not a non-regression proof.
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
