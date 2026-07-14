use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
};

use kdtree::distance::squared_euclidean;

use crate::structures::{
    BikeCost, EdgeData, NodeID, StreetEdgeData, degrees_to_meters, plan::PlanCoordinate,
};

use super::Graph;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreetProfile {
    Foot,
    Bike,
    Car,
}

impl Graph {
    #[inline]
    pub(super) fn edge_secs(&self, street: &StreetEdgeData, profile: StreetProfile) -> Option<u32> {
        let speed_mps = match profile {
            StreetProfile::Foot if street.foot => self.raptor.walking_speed_mps,
            StreetProfile::Foot => return None,
            StreetProfile::Bike if street.bike => self.raptor.cycling_speed_mps,
            StreetProfile::Bike if street.foot => self.raptor.walking_speed_mps,
            StreetProfile::Bike => return None,
            // Car falls back to foot edges (snap connectors are foot-only).
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

    /// Must mirror [`Graph::nodes_distance`]'s `* 0.99` haversine discount.
    pub(super) fn transit_seg_length(&self, a: NodeID, b: NodeID) -> usize {
        (self.node_loc(a).dist(self.node_loc(b)) * 0.99) as usize
    }

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
        // Car is phased Drive → (park) → Walk, never reversed; the state `bool`
        // is `walking` (`false` = still in the car). Foot/Bike stay `false`.
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

        let mut best: HashMap<NodeID, u32> = HashMap::new();
        for (&(node, _), &d) in &dist {
            let e = best.entry(node).or_insert(u32::MAX);
            *e = (*e).min(d);
        }
        best
    }

    /// Once `walking`, only foot edges are usable (the car is left behind).
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

    pub(super) fn dir_between(&self, from: NodeID, to: NodeID) -> (f64, f64) {
        let a = self.nodes[from.0].loc();
        let b = self.nodes[to.0].loc();
        let (dx, dy) = (b.longitude - a.longitude, b.latitude - a.latitude);
        let n = (dx * dx + dy * dy).sqrt().max(1e-12);
        (dx / n, dy / n)
    }

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
        // Stable sort by stop id: RAPTOR keeps the first label on ties, so HashMap
        // iteration order would make results nondeterministic across processes.
        stops.sort_unstable_by_key(|&(stop, _)| stop);
        stops
    }
}
