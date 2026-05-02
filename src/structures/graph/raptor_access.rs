use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

use kdtree::distance::squared_euclidean;

use crate::structures::{EdgeData, NodeID, degrees_to_meters, plan::PlanCoordinate};

use super::Graph;

impl Graph {
    pub(super) fn node_coord(&self, id: NodeID) -> PlanCoordinate {
        let loc = self.nodes[id.0].loc();
        PlanCoordinate { lat: loc.latitude, lon: loc.longitude }
    }

    /// Returns the sequence of OSM nodes forming the shortest walking path
    /// from `origin` to `destination`, converted to lat/lon coordinates.
    ///
    /// Falls back to a two-point straight line if no path is found.
    pub(super) fn walk_path(&self, origin: NodeID, destination: NodeID) -> Vec<PlanCoordinate> {
        if origin == destination {
            let c = self.node_coord(origin);
            return vec![c];
        }

        let walk_mms = (self.raptor.walking_speed_mps * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut parent: HashMap<NodeID, NodeID> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        'outer: while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }
            if node == destination {
                break 'outer;
            }
            // Do not expand through transit stop nodes (except the origin which
            // may itself be a transit-stop-snapped OSM node).
            if node != origin && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / walk_mms as u64) as u32;
                            let nd = d.saturating_add(t);
                            let entry = dist.entry(street.destination).or_insert(u32::MAX);
                            if nd < *entry {
                                *entry = nd;
                                parent.insert(street.destination, node);
                                pq.push(Reverse((nd, street.destination)));
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                                parent.entry(transit.destination).or_insert(node);
                            }
                        }
                    }
                }
            }
        }

        if !dist.contains_key(&destination) {
            return vec![self.node_coord(origin), self.node_coord(destination)];
        }

        // Backtrack from destination to origin.
        let mut path_nodes = vec![destination];
        let mut current = destination;
        while current != origin {
            match parent.get(&current) {
                Some(&p) => {
                    path_nodes.push(p);
                    current = p;
                }
                None => break,
            }
        }
        path_nodes.reverse();
        path_nodes.iter().map(|&n| self.node_coord(n)).collect()
    }

    pub fn walk_dijkstra(&self, origin: NodeID, max_seconds: u32) -> HashMap<NodeID, u32> {
        let walk_mms = (self.raptor.walking_speed_mps * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }

            if self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }

            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / walk_mms as u64) as u32;
                            let nd = d.saturating_add(t);
                            if nd <= max_seconds {
                                let entry = dist.entry(street.destination).or_insert(u32::MAX);
                                if nd < *entry {
                                    *entry = nd;
                                    pq.push(Reverse((nd, street.destination)));
                                }
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                            }
                        }
                    }
                }
            }
        }

        dist
    }

    pub(super) fn nearest_stop_secs(&self, node: NodeID, straight_line_secs: u32) -> u32 {
        let loc = self.nodes[node.0].loc();
        self.raptor.transit_stops_tree
            .nearest(&[loc.latitude, loc.longitude], 1, &squared_euclidean)
            .ok()
            .and_then(|v| v.into_iter().next())
            .map(|(dist_sq, _)| {
                let dist_m = degrees_to_meters(dist_sq, loc.latitude);
                (dist_m / self.raptor.walking_speed_mps) as u32
            })
            .unwrap_or(straight_line_secs)
    }

    pub fn nearby_stops(&self, origin: NodeID, max_walk_secs: u32) -> Vec<(usize, u32)> {
        let walk_times = self.walk_dijkstra(origin, max_walk_secs);

        let mut stops = Vec::new();
        for (&node, &walk_secs) in &walk_times {
            let compact = self.raptor.transit_node_to_stop[node.0];
            if compact != u32::MAX {
                stops.push((compact as usize, walk_secs));
            }
        }
        stops
    }
}
