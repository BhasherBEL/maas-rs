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
    pub(super) fn edge_secs(&self, street: &StreetEdgeData, profile: StreetProfile) -> Option<u32> {
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
        let loc = self.node_loc(id);
        PlanCoordinate {
            lat: loc.latitude,
            lon: loc.longitude,
        }
    }

    /// Coordinate of a node, g-free once the interior arrays are dropped. Every NodeID
    /// reachable post-drop (query endpoints, transit stops, junction path nodes) is a
    /// junction, so its position survives in the contracted graph's `junction_coord`.
    /// With `g` present (flag-off or pre-drop) this is byte-identical to
    /// `self.nodes[id].loc()`.
    pub(super) fn node_loc(&self, id: NodeID) -> crate::structures::LatLng {
        if self.nodes.is_empty() {
            let cg = self
                .contracted
                .as_ref()
                .expect("contracted graph present after the interior-node drop");
            cg.junction_coord[cg.junction_of[id.0] as usize]
        } else {
            self.nodes[id.0].loc()
        }
    }

    /// Inter-stop segment length (meters) for a transit leg, g-free post-drop. Mirrors
    /// [`Graph::nodes_distance`]'s `* 0.99` haversine discount.
    pub(super) fn transit_seg_length(&self, a: NodeID, b: NodeID) -> usize {
        (self.node_loc(a).dist(self.node_loc(b)) * 0.99) as usize
    }

    /// Profile-aware leg geometry that routes over the contracted graph when present,
    /// else the full-graph `street_path`.
    ///
    /// When `contracted.is_some()`, the polyline is rebuilt from super-edge segment coords
    /// (`street_path_arena`) — g-free traversal. The endpoint COORDS still come from
    /// `node_coord` here (a `g.nodes` read, valid until the P3f drop); the snapping
    /// cutover (T2) swaps that coord source to the arena snap. A transit-stop endpoint's
    /// coord comes from its junction coord (stops are junctions), so it survives the drop.
    pub(super) fn street_path_geom(
        &self,
        origin: NodeID,
        destination: NodeID,
        profile: StreetProfile,
    ) -> Vec<PlanCoordinate> {
        let cg = self.contracted.as_ref().unwrap();
        let o = self.geom_node_coord(origin, cg);
        let d = self.geom_node_coord(destination, cg);
        let radius = self.raptor.edge_snap_radius_m;
        cg.street_path_arena(self, o.lat, o.lon, d.lat, d.lon, profile, radius)
            .into_iter()
            .map(|c| PlanCoordinate { lat: c.latitude, lon: c.longitude })
            .collect()
    }

    /// Profile-aware leg geometry between two PROJECTED snap coordinates over the
    /// contracted graph (`street_path_arena`). Used for a coord-snapped origin/destination
    /// whose interior node is gone, so the polyline survives the drop. The endpoints are
    /// the exact projection points (never a junction shortcut).
    pub(super) fn street_path_geom_coords(
        &self,
        origin: crate::structures::LatLng,
        destination: crate::structures::LatLng,
        profile: StreetProfile,
    ) -> Vec<PlanCoordinate> {
        let cg = self.contracted.as_ref().unwrap();
        let radius = self.raptor.edge_snap_radius_m;
        cg.street_path_arena(
            self,
            origin.latitude,
            origin.longitude,
            destination.latitude,
            destination.longitude,
            profile,
            radius,
        )
        .into_iter()
        .map(|c| PlanCoordinate { lat: c.latitude, lon: c.longitude })
        .collect()
    }

    /// Coordinate of a snapped endpoint for contracted geometry: a junction (incl. every
    /// transit stop) resolves to its g-free `junction_coord`; any other node falls back to
    /// `node_coord` (valid until the P3f drop — T2 replaces interior-node coords with the
    /// arena snap point). Keeps geometry endpoints stable across the flag.
    fn geom_node_coord(
        &self,
        id: NodeID,
        cg: &super::contraction::ContractedGraph,
    ) -> PlanCoordinate {
        if let Some(c) = cg.junction_coord_of(id) {
            return PlanCoordinate { lat: c.latitude, lon: c.longitude };
        }
        self.node_coord(id)
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
    pub(super) fn car_edge_step(&self, street: &StreetEdgeData, walking: bool) -> Option<(u32, bool)> {
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
        self.nearest_stop_secs_coord(self.node_loc(node), straight_line_secs)
    }

    /// `nearest_stop_secs` keyed directly on a coordinate (the projected arena snap),
    /// avoiding a `g.nodes` read so it survives the interior-node drop.
    pub(super) fn nearest_stop_secs_coord(
        &self,
        loc: crate::structures::LatLng,
        straight_line_secs: u32,
    ) -> u32 {
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

    /// A synthetic partial-length copy of `e` (a stub of `len` meters along it),
    /// elevation prorated. Used to charge the bit of an edge between a projected
    /// endpoint and the edge's node.
    pub(super) fn partial_edge(e: &StreetEdgeData, len: usize) -> StreetEdgeData {
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

    /// Bike variant of `nearby_stops`, cost-routed (carries kinematic time).
    pub fn bike_nearby_stops(
        &self,
        origin: NodeID,
        max_secs: u32,
        bike: &BikeCost,
    ) -> Vec<(usize, u32)> {
        let cg = self.contracted.as_ref().unwrap();
        let times = self.bike_dijkstra_union(origin, max_secs, bike, cg);
        let mut stops: Vec<(usize, u32)> = times
            .iter()
            .filter_map(|(&jn, &secs)| {
                let compact = self.raptor.transit_node_to_stop[jn.0];
                (compact != u32::MAX).then_some((compact as usize, secs))
            })
            .collect();
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
