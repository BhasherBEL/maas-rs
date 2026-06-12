use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

use kdtree::distance::squared_euclidean;

use crate::structures::{
    BikeCost, EdgeData, NodeID, StreetEdgeData, degrees_to_meters, plan::PlanCoordinate,
};

use super::Graph;

/// Street traversal profile for access/egress/direct routing.
/// `Bike` rides `bike` edges at cycling speed and falls back to `foot` edges
/// at walking speed (dismount and push), so pedestrian-only shortcuts stay usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreetProfile {
    Foot,
    Bike,
    Car,
}

impl Graph {
    /// Traversal time of a street edge under `profile` in integer milliseconds
    /// per meter math (same arithmetic as the historical walk path), or `None`
    /// when the profile cannot use the edge.
    #[inline]
    fn edge_secs(&self, street: &StreetEdgeData, profile: StreetProfile) -> Option<u32> {
        let speed_mps = match profile {
            StreetProfile::Foot if street.foot => self.raptor.walking_speed_mps,
            StreetProfile::Foot => return None,
            StreetProfile::Bike if street.bike => self.raptor.cycling_speed_mps,
            StreetProfile::Bike if street.foot => self.raptor.walking_speed_mps,
            StreetProfile::Bike => return None,
            // Car drives car edges, and falls back to foot edges at walking speed
            // (park near the station and walk the last stretch) — the stop→street
            // snap connectors are foot-only, so without this a car could never
            // reach a platform for park & ride / kiss & ride.
            StreetProfile::Car if street.car => self.raptor.driving_speed_mps,
            StreetProfile::Car if street.foot => self.raptor.walking_speed_mps,
            StreetProfile::Car => return None,
        };
        let speed_mms = (speed_mps * 1000.0) as u32;
        Some((street.length as u64 * 1000 / speed_mms as u64) as u32)
    }

    pub(super) fn node_coord(&self, id: NodeID) -> PlanCoordinate {
        let loc = self.nodes[id.0].loc();
        PlanCoordinate { lat: loc.latitude, lon: loc.longitude }
    }

    /// Returns the sequence of OSM nodes forming the shortest walking path
    /// from `origin` to `destination`, converted to lat/lon coordinates.
    ///
    /// Falls back to a two-point straight line if no path is found.
    pub(super) fn walk_path(&self, origin: NodeID, destination: NodeID) -> Vec<PlanCoordinate> {
        self.street_path(origin, destination, StreetProfile::Foot)
    }

    pub(super) fn street_path(
        &self,
        origin: NodeID,
        destination: NodeID,
        profile: StreetProfile,
    ) -> Vec<PlanCoordinate> {
        if origin == destination {
            let c = self.node_coord(origin);
            return vec![c];
        }

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
                            let Some(t) = self.edge_secs(street, profile) else {
                                continue;
                            };
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
        self.street_dijkstra(origin, max_seconds, StreetProfile::Foot)
    }

    pub fn street_dijkstra(
        &self,
        origin: NodeID,
        max_seconds: u32,
        profile: StreetProfile,
    ) -> HashMap<NodeID, u32> {
        // Car routing is phased: a car may go Driving → (park) → Walking, but
        // never Walking → Driving (a car left at the kerb can't be picked back
        // up). The phase is carried in the search state; `walking == false` means
        // still in the car. Foot/Bike are single-phase (the flag stays false).
        let car = matches!(profile, StreetProfile::Car);
        let mut dist: HashMap<(NodeID, bool), u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, (NodeID, bool))>> = BinaryHeap::new();

        dist.insert((origin, false), 0);
        pq.push(Reverse((0, (origin, false))));

        while let Some(Reverse((d, (node, walking)))) = pq.pop() {
            if d > *dist.get(&(node, walking)).unwrap_or(&u32::MAX) {
                continue;
            }

            if self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }

            let Some(neighbors) = self.edges.get(node.0) else { continue };
            for edge in neighbors {
                match edge {
                    EdgeData::Street(street) => {
                        // (time, next-phase) for this edge under the profile.
                        let step = if car {
                            self.car_edge_step(street, walking)
                        } else {
                            self.edge_secs(street, profile).map(|t| (t, false))
                        };
                        let Some((t, next_walking)) = step else { continue };
                        let nd = d.saturating_add(t);
                        if nd <= max_seconds {
                            let entry =
                                dist.entry((street.destination, next_walking)).or_insert(u32::MAX);
                            if nd < *entry {
                                *entry = nd;
                                pq.push(Reverse((nd, (street.destination, next_walking))));
                            }
                        }
                    }
                    EdgeData::Transit(transit) => {
                        let entry = dist.entry((transit.destination, walking)).or_insert(u32::MAX);
                        if d < *entry {
                            *entry = d;
                        }
                    }
                }
            }
        }

        // Collapse the (node, phase) distances to the best arrival per node.
        let mut best: HashMap<NodeID, u32> = HashMap::new();
        for (&(node, _), &d) in &dist {
            let e = best.entry(node).or_insert(u32::MAX);
            *e = (*e).min(d);
        }
        best
    }

    /// One car step: `(seconds, next-phase)` or `None` if impassable. Driving may
    /// stay on car edges or *park and walk* onto a foot edge (→ Walking); once
    /// Walking, only foot edges are usable (the car has been left behind).
    #[inline]
    fn car_edge_step(&self, street: &StreetEdgeData, walking: bool) -> Option<(u32, bool)> {
        let secs = |speed_mps: f64| {
            let speed_mms = (speed_mps * 1000.0) as u32;
            (street.length as u64 * 1000 / speed_mms as u64) as u32
        };
        if !walking && street.car {
            Some((secs(self.raptor.driving_speed_mps), false))
        } else if street.foot {
            Some((secs(self.raptor.walking_speed_mps), true))
        } else {
            None
        }
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

    /// Unit direction vector from `from` to `to` in lat/lon space (adequate for
    /// turn-angle dot products at Belgian latitudes; not great-circle exact).
    fn dir_between(&self, from: NodeID, to: NodeID) -> (f64, f64) {
        let a = self.nodes[from.0].loc();
        let b = self.nodes[to.0].loc();
        let (dx, dy) = (b.longitude - a.longitude, b.latitude - a.latitude);
        let n = (dx * dx + dy * dy).sqrt().max(1e-12);
        (dx / n, dy / n)
    }

    /// Cost-minimizing bike search. Each label carries accumulated kinematic time
    /// (for the access-radius budget + reported ETA) but the priority is the
    /// BRouter-style weighted cost, so routes prefer nicer/safer ways. Returns the
    /// min-cost-route arrival time (seconds) per reachable node.
    pub fn bike_cost_dijkstra(
        &self,
        origin: NodeID,
        max_seconds: u32,
        bike: &BikeCost,
    ) -> HashMap<NodeID, u32> {
        // Cost is scaled to integer bits for a total order in the heap.
        let mut best_cost: HashMap<NodeID, u64> = HashMap::new();
        let mut arrival: HashMap<NodeID, u32> = HashMap::new();
        // heap tuple: (cost_bits, node, time_secs, prev_node_index_or_MAX)
        let mut pq: BinaryHeap<Reverse<(u64, NodeID, u32, u64)>> = BinaryHeap::new();
        best_cost.insert(origin, 0);
        arrival.insert(origin, 0);
        pq.push(Reverse((0, origin, 0, u64::MAX)));

        while let Some(Reverse((cost_bits, node, time_secs, prev))) = pq.pop() {
            if cost_bits > *best_cost.get(&node).unwrap_or(&u64::MAX) {
                continue;
            }
            // Do not expand through transit stop nodes (except the origin).
            if node != origin && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            let incoming = (prev != u64::MAX).then(|| self.dir_between(NodeID(prev as usize), node));
            let Some(neighbors) = self.edges.get(node.0) else { continue };
            for edge in neighbors {
                let EdgeData::Street(street) = edge else { continue };
                let this_dir = self.dir_between(node, street.destination);
                let Some(step_cost) = bike.edge_cost(street, incoming, this_dir) else { continue };
                let nt = time_secs.saturating_add(bike.edge_time(street));
                if nt > max_seconds {
                    continue;
                }
                let nc = cost_bits.saturating_add((step_cost * 1000.0) as u64);
                let entry = best_cost.entry(street.destination).or_insert(u64::MAX);
                if nc < *entry {
                    *entry = nc;
                    arrival.insert(street.destination, nt);
                    pq.push(Reverse((nc, street.destination, nt, node.0 as u64)));
                }
            }
        }
        arrival
    }

    /// Cost-routed bike path `origin → destination`: returns the node sequence on
    /// the minimum-cost route, its accumulated kinematic time (seconds) and total
    /// length (meters), or `None` if the destination is unreachable within the
    /// time budget. Mirrors `bike_cost_dijkstra` but tracks parents for backtrack.
    pub(super) fn bike_cost_path(
        &self,
        origin: NodeID,
        destination: NodeID,
        max_seconds: u32,
        bike: &BikeCost,
    ) -> Option<(Vec<NodeID>, u32, usize)> {
        if origin == destination {
            return Some((vec![origin], 0, 0));
        }
        let mut best_cost: HashMap<NodeID, u64> = HashMap::new();
        let mut arrival: HashMap<NodeID, u32> = HashMap::new();
        let mut length: HashMap<NodeID, usize> = HashMap::new();
        let mut parent: HashMap<NodeID, NodeID> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u64, NodeID, u32, u64)>> = BinaryHeap::new();
        best_cost.insert(origin, 0);
        arrival.insert(origin, 0);
        length.insert(origin, 0);
        pq.push(Reverse((0, origin, 0, u64::MAX)));

        while let Some(Reverse((cost_bits, node, time_secs, prev))) = pq.pop() {
            if cost_bits > *best_cost.get(&node).unwrap_or(&u64::MAX) {
                continue;
            }
            if node == destination {
                break;
            }
            // Stop expansion at intermediate transit stops (except origin/destination).
            if node != origin && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            let incoming = (prev != u64::MAX).then(|| self.dir_between(NodeID(prev as usize), node));
            let Some(neighbors) = self.edges.get(node.0) else { continue };
            for edge in neighbors {
                let EdgeData::Street(street) = edge else { continue };
                let this_dir = self.dir_between(node, street.destination);
                let Some(step_cost) = bike.edge_cost(street, incoming, this_dir) else { continue };
                let nt = time_secs.saturating_add(bike.edge_time(street));
                if nt > max_seconds {
                    continue;
                }
                let nc = cost_bits.saturating_add((step_cost * 1000.0) as u64);
                let entry = best_cost.entry(street.destination).or_insert(u64::MAX);
                if nc < *entry {
                    *entry = nc;
                    arrival.insert(street.destination, nt);
                    length.insert(street.destination, length[&node] + street.length);
                    parent.insert(street.destination, node);
                    pq.push(Reverse((nc, street.destination, nt, node.0 as u64)));
                }
            }
        }

        best_cost.get(&destination)?;
        let mut path = vec![destination];
        let mut cur = destination;
        while cur != origin {
            let p = *parent.get(&cur)?;
            path.push(p);
            cur = p;
        }
        path.reverse();
        Some((path, arrival[&destination], length[&destination]))
    }

    /// Bike variant of `nearby_stops`, cost-routed (carries kinematic time).
    pub fn bike_nearby_stops(&self, origin: NodeID, max_secs: u32, bike: &BikeCost) -> Vec<(usize, u32)> {
        let times = self.bike_cost_dijkstra(origin, max_secs, bike);
        let mut stops = Vec::new();
        for (&node, &secs) in &times {
            let compact = self.raptor.transit_node_to_stop[node.0];
            if compact != u32::MAX {
                stops.push((compact as usize, secs));
            }
        }
        // Stable order (see `nearby_stops_profile`).
        stops.sort_unstable_by_key(|&(stop, _)| stop);
        stops
    }

    pub fn nearby_stops(&self, origin: NodeID, max_walk_secs: u32) -> Vec<(usize, u32)> {
        self.nearby_stops_profile(origin, max_walk_secs, StreetProfile::Foot)
    }

    pub fn nearby_stops_profile(
        &self,
        origin: NodeID,
        max_secs: u32,
        profile: StreetProfile,
    ) -> Vec<(usize, u32)> {
        let walk_times = self.street_dijkstra(origin, max_secs, profile);

        let mut stops = Vec::new();
        for (&node, &walk_secs) in &walk_times {
            let compact = self.raptor.transit_node_to_stop[node.0];
            if compact != u32::MAX {
                stops.push((compact as usize, walk_secs));
            }
        }
        // `walk_times` is a HashMap (random per-process seed), so its iteration order
        // varies between runs. RAPTOR seeds sources in this order and `LabelSet::insert`
        // keeps the first label on ties, so unsorted output makes routing results
        // nondeterministic across processes. Sort by stop id for a stable order.
        stops.sort_unstable_by_key(|&(stop, _)| stop);
        stops
    }
}
