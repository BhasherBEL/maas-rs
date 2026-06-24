use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
};

use kdtree::distance::squared_euclidean;

use crate::structures::{
    BikeCost, EdgeData, Endpoint, LatLng, NodeID, StreetEdgeData, degrees_to_meters,
    plan::PlanCoordinate,
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

/// One traversed edge on a cost-routed bike path.
#[derive(Debug, Clone, Copy)]
pub(super) struct BikeEdge {
    /// True when the edge is walked with the bike pushed (foot-only, not bike-accessible).
    pub push: bool,
    pub length: usize,
    pub time: u32,
}

/// Result of [`Graph::bike_cost_path`]: the chosen node sequence plus aggregate
/// totals and per-edge metadata (aligned with `nodes.windows(2)`).
#[derive(Debug, Clone)]
pub(super) struct BikePath {
    pub nodes: Vec<NodeID>,
    pub secs: u32,
    pub length: usize,
    pub cycleroute_length: usize,
    pub ascent: usize,
    pub edges: Vec<BikeEdge>,
    /// Partial stub from an edge-projected origin to `nodes[0]` (coordinate + edge),
    /// when the origin was snapped to the interior of an edge. `None` for a node origin.
    pub lead: Option<(LatLng, BikeEdge)>,
    /// Partial stub from `nodes[last]` to an edge-projected destination. `None` for
    /// a node destination.
    pub tail: Option<(LatLng, BikeEdge)>,
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
        PlanCoordinate {
            lat: loc.latitude,
            lon: loc.longitude,
        }
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

            let Some(neighbors) = self.edges.get(node.0) else {
                continue;
            };
            for edge in neighbors {
                match edge {
                    EdgeData::Street(street) => {
                        // (time, next-phase) for this edge under the profile.
                        let step = if car {
                            self.car_edge_step(street, walking)
                        } else {
                            self.edge_secs(street, profile).map(|t| (t, false))
                        };
                        let Some((t, next_walking)) = step else {
                            continue;
                        };
                        let nd = d.saturating_add(t);
                        if nd <= max_seconds {
                            let entry = dist
                                .entry((street.destination, next_walking))
                                .or_insert(u32::MAX);
                            if nd < *entry {
                                *entry = nd;
                                pq.push(Reverse((nd, (street.destination, next_walking))));
                            }
                        }
                    }
                    EdgeData::Transit(transit) => {
                        let entry = dist
                            .entry((transit.destination, walking))
                            .or_insert(u32::MAX);
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
        self.raptor
            .transit_stops_tree
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
    pub(super) fn dir_between(&self, from: NodeID, to: NodeID) -> (f64, f64) {
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
        // Signed elevation hysteresis buffer (meters) of the min-cost path to each
        // node; threaded so elevation cost reflects sustained net climbs/descents.
        let mut elev_buf: HashMap<NodeID, (f64, f64)> = HashMap::new();
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
            let incoming =
                (prev != u64::MAX).then(|| self.dir_between(NodeID(prev as usize), node));
            let Some(neighbors) = self.edges.get(node.0) else {
                continue;
            };
            for edge in neighbors {
                let EdgeData::Street(street) = edge else {
                    continue;
                };
                let this_dir = self.dir_between(node, street.destination);
                let Some(step_cost) = bike.edge_cost(street, incoming, this_dir) else {
                    continue;
                };
                let nt = time_secs.saturating_add(bike.edge_time(street));
                if nt > max_seconds {
                    continue;
                }
                let (ehbd, ehbu) = elev_buf.get(&node).copied().unwrap_or((0.0, 0.0));
                let (elev_cost, new_ehbd, new_ehbu) =
                    bike.elevation_step(ehbd, ehbu, street.elev_delta as f64, street.length as f64);
                let nc = cost_bits.saturating_add(((step_cost + elev_cost) * 1000.0) as u64);
                let entry = best_cost.entry(street.destination).or_insert(u64::MAX);
                if nc < *entry {
                    *entry = nc;
                    arrival.insert(street.destination, nt);
                    elev_buf.insert(street.destination, (new_ehbd, new_ehbu));
                    pq.push(Reverse((nc, street.destination, nt, node.0 as u64)));
                }
            }
        }
        arrival
    }

    /// The directed street edge `from → to`, if one exists.
    fn street_edge_between(&self, from: NodeID, to: NodeID) -> Option<&StreetEdgeData> {
        self.edges.get(from.0)?.iter().find_map(|e| match e {
            EdgeData::Street(s) if s.destination == to => Some(s),
            _ => None,
        })
    }

    /// A synthetic partial-length copy of `e` (a stub of `len` meters along it),
    /// elevation prorated. Used to charge the bit of an edge between a projected
    /// endpoint and the edge's node.
    fn partial_edge(e: &StreetEdgeData, len: usize) -> StreetEdgeData {
        let frac = if e.length == 0 {
            0.0
        } else {
            len as f64 / e.length as f64
        };
        StreetEdgeData {
            origin: e.origin,
            destination: e.destination,
            length: len,
            partial: true,
            foot: e.foot,
            bike: e.bike,
            car: e.car,
            attrs: e.attrs,
            elev_delta: (e.elev_delta as f64 * frac).round() as i16,
            surface_speed: 100,
            var_gen: e.var_gen,
        }
    }

    /// Cost-routed bike path between two [`Endpoint`]s. A `Node` endpoint is the
    /// usual graph node; an `OnEdge` endpoint is a point projected onto an edge's
    /// interior — handled by seeding the search from *both* edge ends with the
    /// partial-stub cost (origin) and by adding the partial stub when the search
    /// reaches either end (destination). No graph mutation. Returns the node path,
    /// totals (stubs included) and per-edge metadata, or `None` if unreachable.
    pub(super) fn bike_cost_path(
        &self,
        origin: Endpoint,
        destination: Endpoint,
        max_seconds: u32,
        bike: &BikeCost,
    ) -> Option<BikePath> {
        let mut best_cost: HashMap<NodeID, u64> = HashMap::new();
        let mut arrival: HashMap<NodeID, u32> = HashMap::new();
        let mut length: HashMap<NodeID, usize> = HashMap::new();
        let mut cycleroute_length: HashMap<NodeID, usize> = HashMap::new();
        let mut ascent: HashMap<NodeID, usize> = HashMap::new();
        let mut parent: HashMap<NodeID, NodeID> = HashMap::new();
        let mut step_meta: HashMap<NodeID, BikeEdge> = HashMap::new();
        let mut elev_buf: HashMap<NodeID, (f64, f64)> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u64, NodeID, u32, u64)>> = BinaryHeap::new();

        // ── Seed the search ────────────────────────────────────────────────────
        // For a node origin: one zero-cost seed. For an edge origin: seed both ends
        // with the cost/time of the stub from the projection to that end.
        let mut seed_nodes: HashSet<NodeID> = HashSet::new();
        let mut lead_stub: HashMap<NodeID, BikeEdge> = HashMap::new();
        let origin_proj = match origin {
            Endpoint::Node(n) => {
                best_cost.insert(n, 0);
                arrival.insert(n, 0);
                length.insert(n, 0);
                cycleroute_length.insert(n, 0);
                ascent.insert(n, 0);
                elev_buf.insert(n, (0.0, 0.0));
                seed_nodes.insert(n);
                pq.push(Reverse((0, n, 0, u64::MAX)));
                None
            }
            Endpoint::OnEdge {
                a,
                b,
                dist_a,
                dist_b,
                proj,
            } => {
                // To reach end `e` from the projection, ride the stub `other→e`.
                for (e, other, d) in [(a, b, dist_a), (b, a, dist_b)] {
                    let Some(edge) = self.street_edge_between(other, e) else {
                        continue;
                    };
                    let stub = Self::partial_edge(edge, d);
                    let Some(c) = bike.edge_cost(&stub, None, (1.0, 0.0)) else {
                        continue;
                    };
                    let (ec, ehbd, ehbu) =
                        bike.elevation_step(0.0, 0.0, stub.elev_delta as f64, stub.length as f64);
                    let bits = ((c + ec) * 1000.0) as u64;
                    let t = bike.edge_time(&stub);
                    if t > max_seconds {
                        continue;
                    }
                    if bits < *best_cost.get(&e).unwrap_or(&u64::MAX) {
                        best_cost.insert(e, bits);
                        arrival.insert(e, t);
                        length.insert(e, d);
                        cycleroute_length.insert(e, if stub.attrs.cycleroute { d } else { 0 });
                        ascent.insert(e, stub.elev_delta.max(0) as usize);
                        elev_buf.insert(e, (ehbd, ehbu));
                        lead_stub.insert(
                            e,
                            BikeEdge {
                                push: !stub.attrs.bikeaccess && stub.attrs.footaccess,
                                length: d,
                                time: t,
                            },
                        );
                        pq.push(Reverse((bits, e, t, u64::MAX)));
                    }
                    seed_nodes.insert(e);
                }
                Some(proj)
            }
        };
        if pq.is_empty() {
            return None;
        }

        // ── Targets ────────────────────────────────────────────────────────────
        // For a node destination: settle that node. For an edge destination: settle
        // both ends, each with a tail stub `end→projection` added on arrival.
        let dest_proj = match destination {
            Endpoint::OnEdge { proj, .. } => Some(proj),
            Endpoint::Node(_) => None,
        };
        // target end -> Option<(directed edge end→other, stub length)>
        let mut target_stub: HashMap<NodeID, Option<(NodeID, usize)>> = HashMap::new();
        match destination {
            Endpoint::Node(n) => {
                target_stub.insert(n, None);
            }
            Endpoint::OnEdge {
                a,
                b,
                dist_a,
                dist_b,
                ..
            } => {
                for (e, other, d) in [(a, b, dist_a), (b, a, dist_b)] {
                    if self.street_edge_between(e, other).is_some() {
                        target_stub.insert(e, Some((other, d)));
                    }
                }
            }
        }
        if target_stub.is_empty() {
            return None;
        }
        let mut remaining: HashSet<NodeID> = target_stub.keys().copied().collect();

        // ── Dijkstra ───────────────────────────────────────────────────────────
        while let Some(Reverse((cost_bits, node, time_secs, prev))) = pq.pop() {
            if cost_bits > *best_cost.get(&node).unwrap_or(&u64::MAX) {
                continue;
            }
            if remaining.remove(&node) && remaining.is_empty() {
                break;
            }
            // Stop expansion at intermediate transit stops (except seeds).
            if !seed_nodes.contains(&node) && self.raptor.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            let incoming =
                (prev != u64::MAX).then(|| self.dir_between(NodeID(prev as usize), node));
            let Some(neighbors) = self.edges.get(node.0) else {
                continue;
            };
            for edge in neighbors {
                let EdgeData::Street(street) = edge else {
                    continue;
                };
                let this_dir = self.dir_between(node, street.destination);
                let Some(step_cost) = bike.edge_cost(street, incoming, this_dir) else {
                    continue;
                };
                let nt = time_secs.saturating_add(bike.edge_time(street));
                if nt > max_seconds {
                    continue;
                }
                let (ehbd, ehbu) = elev_buf.get(&node).copied().unwrap_or((0.0, 0.0));
                let (elev_cost, new_ehbd, new_ehbu) =
                    bike.elevation_step(ehbd, ehbu, street.elev_delta as f64, street.length as f64);
                let nc = cost_bits.saturating_add(((step_cost + elev_cost) * 1000.0) as u64);
                let entry = best_cost.entry(street.destination).or_insert(u64::MAX);
                if nc < *entry {
                    *entry = nc;
                    arrival.insert(street.destination, nt);
                    elev_buf.insert(street.destination, (new_ehbd, new_ehbu));
                    step_meta.insert(
                        street.destination,
                        BikeEdge {
                            push: !street.attrs.bikeaccess && street.attrs.footaccess,
                            length: street.length,
                            time: bike.edge_time(street),
                        },
                    );
                    let parent_cycleroute = cycleroute_length[&node];
                    cycleroute_length.insert(
                        street.destination,
                        if street.attrs.cycleroute {
                            parent_cycleroute + street.length
                        } else {
                            parent_cycleroute
                        },
                    );
                    length.insert(street.destination, length[&node] + street.length);
                    ascent.insert(
                        street.destination,
                        ascent[&node] + street.elev_delta.max(0) as usize,
                    );
                    parent.insert(street.destination, node);
                    pq.push(Reverse((nc, street.destination, nt, node.0 as u64)));
                }
            }
        }

        // ── Pick the best settled target (path cost + tail stub) ────────────────
        // tail data per reachable target: (total_bits, BikeEdge, time, len, cyc, asc)
        let tail_of = |end: NodeID,
                       stub: &Option<(NodeID, usize)>|
         -> Option<(u64, Option<BikeEdge>, u32, usize, usize, usize)> {
            let base = *best_cost.get(&end)?;
            match stub {
                None => Some((base, None, 0, 0, 0, 0)),
                Some((other, d)) => {
                    let e = self.street_edge_between(end, *other)?;
                    let s = Self::partial_edge(e, *d);
                    let c = bike.edge_cost(&s, None, (1.0, 0.0))?;
                    let (ehbd, ehbu) = elev_buf.get(&end).copied().unwrap_or((0.0, 0.0));
                    let (ec, _, _) =
                        bike.elevation_step(ehbd, ehbu, s.elev_delta as f64, s.length as f64);
                    let t = bike.edge_time(&s);
                    let be = BikeEdge {
                        push: !s.attrs.bikeaccess && s.attrs.footaccess,
                        length: *d,
                        time: t,
                    };
                    let cyc = if s.attrs.cycleroute { *d } else { 0 };
                    Some((
                        base.saturating_add(((c + ec) * 1000.0) as u64),
                        Some(be),
                        t,
                        *d,
                        cyc,
                        s.elev_delta.max(0) as usize,
                    ))
                }
            }
        };
        let (end, _total, tail_be, tail_t, tail_len, tail_cyc, tail_asc) = target_stub
            .iter()
            .filter_map(|(&e, stub)| {
                tail_of(e, stub).map(|(tot, be, t, l, cy, asc)| (e, tot, be, t, l, cy, asc))
            })
            .min_by_key(|x| x.1)?;

        // ── Reconstruct ─────────────────────────────────────────────────────────
        let mut path = vec![end];
        let mut cur = end;
        while !seed_nodes.contains(&cur) {
            let p = *parent.get(&cur)?;
            path.push(p);
            cur = p;
        }
        path.reverse();
        let edges: Vec<BikeEdge> = path[1..].iter().map(|n| step_meta[n]).collect();
        let lead = origin_proj.and_then(|p| lead_stub.get(&path[0]).map(|be| (p, *be)));
        let tail = match (dest_proj, tail_be) {
            (Some(p), Some(be)) => Some((p, be)),
            _ => None,
        };
        Some(BikePath {
            nodes: path,
            secs: arrival[&end] + tail_t,
            length: length[&end] + tail_len,
            cycleroute_length: cycleroute_length[&end] + tail_cyc,
            ascent: ascent[&end] + tail_asc,
            edges,
            lead,
            tail,
        })
    }

    /// Bike variant of `nearby_stops`, cost-routed (carries kinematic time).
    pub fn bike_nearby_stops(
        &self,
        origin: NodeID,
        max_secs: u32,
        bike: &BikeCost,
    ) -> Vec<(usize, u32)> {
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

#[cfg(test)]
mod tests {
    use crate::structures::{
        BikeAttrs, BikeCost, BikeProfile, EdgeData, Endpoint, Graph, HighwayClass, LatLng,
        NodeData, NodeID, OsmNodeData, StreetEdgeData, Surface,
    };

    fn osm(id: &str, lat: f64, lon: f64) -> NodeData {
        NodeData::OsmNode(OsmNodeData {
            eid: id.to_string(),
            lat_lng: LatLng {
                latitude: lat,
                longitude: lon,
            },
        })
    }

    fn cyc_attrs(cycleroute: bool) -> BikeAttrs {
        let mut a = BikeAttrs::road_default();
        a.highway = HighwayClass::Cycleway;
        a.surface = Surface::Paved;
        a.isbike = true;
        a.cycleroute = cycleroute;
        a
    }

    /// A door-to-door bike plan over `ride 100m → push 60m → ride 100m` must split
    /// into three steps, with the middle marked `dismount` and timed at walk speed.
    #[test]
    fn build_bike_plan_segments_ride_and_push() {
        use crate::structures::plan::{PlanLeg, PlanLegStep};
        let mut g = Graph::new();
        let a = g.add_node(osm("a", 50.000, 4.0000));
        let b = g.add_node(osm("b", 50.000, 4.0010));
        let c = g.add_node(osm("c", 50.000, 4.0016));
        let d = g.add_node(osm("d", 50.000, 4.0026));

        let mut edge = |from: NodeID, to: NodeID, len: usize, push: bool| {
            let mut attrs = cyc_attrs(false);
            if push {
                attrs.bikeaccess = false;
                attrs.footaccess = true;
                attrs.highway = HighwayClass::Footway;
            }
            for (o2, d2) in [(from, to), (to, from)] {
                g.add_edge(
                    o2,
                    EdgeData::Street(StreetEdgeData {
                        origin: o2,
                        destination: d2,
                        length: len,
                        partial: false,
                        foot: true,
                        bike: !push,
                        car: false,
                        attrs,
                        elev_delta: 0,
                        surface_speed: 100,
                        var_gen: crate::structures::cost::VarGen::NONE,
                    }),
                );
            }
        };
        edge(a, b, 100, false);
        edge(b, c, 60, true);
        edge(c, d, 100, false);
        g.build_raptor_index();

        let walk = 1.2;
        let bc = BikeCost::new(BikeProfile::default(), walk);
        let plan = g
            .build_bike_plan(Endpoint::Node(a), Endpoint::Node(d), 0, u32::MAX, &bc)
            .expect("d reachable from a");
        let leg = match &plan.legs[0] {
            PlanLeg::Walk(w) => w,
            _ => panic!("expected a walk leg"),
        };
        let runs: Vec<(bool, usize, u32)> = leg
            .steps
            .iter()
            .map(|s| match s {
                PlanLegStep::Walk(w) => (w.dismount, w.length, w.time),
                _ => panic!("expected walk steps"),
            })
            .collect();
        assert_eq!(runs.len(), 3, "ride / push / ride runs");
        assert_eq!(runs[0].0, false, "first run is ridden");
        assert_eq!(runs[1].0, true, "middle run is a dismount");
        assert_eq!(runs[2].0, false, "last run is ridden");
        assert_eq!(runs[1].1, 60, "push run length");
        assert_eq!(
            runs[1].2,
            (60.0_f64 / BikeProfile::default().push_speed_mps).round() as u32,
            "push run timed at the (slow) push speed, not free walking"
        );
    }

    /// A destination projected onto the interior of a long rideable cycleway must
    /// be ridden to (no dismount), with the partial stub stitched onto the plan so
    /// the geometry ends at the projection point.
    #[test]
    fn build_bike_plan_edge_destination_rides_to_projection() {
        use crate::structures::plan::{PlanLeg, PlanLegStep};
        let mut g = Graph::new();
        let s = g.add_node(osm("s", 50.000, 4.0000));
        let a = g.add_node(osm("a", 50.000, 4.0010));
        let b = g.add_node(osm("b", 50.000, 4.0030));
        let mut edge = |from: NodeID, to: NodeID, len: usize| {
            for (o2, d2) in [(from, to), (to, from)] {
                g.add_edge(
                    o2,
                    EdgeData::Street(StreetEdgeData {
                        origin: o2,
                        destination: d2,
                        length: len,
                        partial: false,
                        foot: true,
                        bike: true,
                        car: false,
                        attrs: cyc_attrs(true), // rideable cycleway
                        elev_delta: 0,
                        surface_speed: 100,
                        var_gen: crate::structures::cost::VarGen::NONE,
                    }),
                );
            }
        };
        edge(s, a, 100);
        edge(a, b, 200); // long cycleway; destination sits 120 m along it from a
        g.build_raptor_index();

        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let proj = LatLng {
            latitude: 50.000,
            longitude: 4.0010 + 0.6 * (4.0030 - 4.0010),
        };
        let dest = Endpoint::OnEdge {
            a,
            b,
            dist_a: 120,
            dist_b: 80,
            proj,
        };
        let plan = g
            .build_bike_plan(Endpoint::Node(s), dest, 0, u32::MAX, &bc)
            .expect("projection reachable");
        let leg = match &plan.legs[0] {
            PlanLeg::Walk(w) => w,
            _ => panic!("expected a walk leg"),
        };
        assert_eq!(leg.length, 220, "s->a (100) + a->proj (120)");
        assert!(
            leg.steps
                .iter()
                .all(|st| matches!(st, PlanLegStep::Walk(w) if !w.dismount)),
            "no dismount riding the cycleway"
        );
        let last = leg.geometry.last().unwrap();
        assert!(
            (last.lat - proj.latitude).abs() < 1e-9 && (last.lon - proj.longitude).abs() < 1e-9,
            "geometry ends at the projection point"
        );
    }

    /// `cycleroute_length` must be the sum of *only* the cycleroute edges on the
    /// chosen path — not the running total length. Regression test for the bug
    /// where the accumulator read `length[&node]` (the total so far) instead of
    /// the parent's `cycleroute_length`, reporting nearly the whole path as
    /// cycleroute regardless of the actual tags.
    #[test]
    fn cycleroute_length_counts_only_cycleroute_edges() {
        let mut g = Graph::new();
        // A straight corridor O–A–B–C with one cycleroute segment (A–B = 100 m)
        // sandwiched between two plain cycleway segments (200 m + 300 m).
        let o = g.add_node(osm("o", 50.000, 4.000));
        let a = g.add_node(osm("a", 50.000, 4.0010));
        let b = g.add_node(osm("b", 50.000, 4.0020));
        let c = g.add_node(osm("c", 50.000, 4.0030));

        let mut edge = |from: NodeID, to: NodeID, len: usize, cycleroute: bool| {
            for (o2, d2) in [(from, to), (to, from)] {
                g.add_edge(
                    o2,
                    EdgeData::Street(StreetEdgeData {
                        origin: o2,
                        destination: d2,
                        length: len,
                        partial: false,
                        foot: true,
                        bike: true,
                        car: false,
                        attrs: cyc_attrs(cycleroute),
                        elev_delta: 0,
                        surface_speed: 100,
                        var_gen: crate::structures::cost::VarGen::NONE,
                    }),
                );
            }
        };
        edge(o, a, 200, false);
        edge(a, b, 100, true);
        edge(b, c, 300, false);
        g.build_raptor_index();

        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let p = g
            .bike_cost_path(Endpoint::Node(o), Endpoint::Node(c), u32::MAX, &bc)
            .expect("c reachable from o");

        assert_eq!(p.length, 600, "total path length");
        assert_eq!(
            p.cycleroute_length, 100,
            "only the 100 m A–B segment is a cycleroute"
        );
    }

    /// D+ (total ascent) sums only the positive elevation deltas along the chosen
    /// path — a descent does not reduce it.
    #[test]
    fn bike_path_sums_positive_ascent() {
        let mut g = Graph::new();
        let o = g.add_node(osm("o", 50.000, 4.000));
        let a = g.add_node(osm("a", 50.000, 4.0010));
        let b = g.add_node(osm("b", 50.000, 4.0020));
        let mut edge = |from: NodeID, to: NodeID, len: usize, elev: i16| {
            g.add_edge(
                from,
                EdgeData::Street(StreetEdgeData {
                    origin: from,
                    destination: to,
                    length: len,
                    partial: false,
                    foot: true,
                    bike: true,
                    car: false,
                    attrs: cyc_attrs(false),
                    elev_delta: elev,
                    surface_speed: 100,
                    var_gen: crate::structures::cost::VarGen::NONE,
                }),
            );
        };
        // Forced corridor O→A (+10 m climb) →B (−5 m descent); reverse edges negate.
        edge(o, a, 100, 10);
        edge(a, o, 100, -10);
        edge(a, b, 100, -5);
        edge(b, a, 100, 5);
        g.build_raptor_index();

        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let p = g
            .bike_cost_path(Endpoint::Node(o), Endpoint::Node(b), u32::MAX, &bc)
            .expect("b reachable from o");
        assert_eq!(
            p.ascent, 10,
            "D+ counts the +10 m climb only, not the −5 m descent"
        );
    }
}
