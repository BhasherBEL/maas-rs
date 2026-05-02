use kdtree::{KdTree, distance::squared_euclidean};

use crate::structures::{
    NodeData, NodeID, meters_to_degrees,
    raptor::{Lookup, PatternID},
};

use super::{Graph, MAX_TRANSFER_DISTANCE_M};

impl Graph {
    pub fn build_raptor_index(&mut self) {
        self.build_compact_stop_index();
        self.build_stop_patterns();
        self.build_stop_transfers();
        self.build_reverse_transfers();
    }

    fn build_compact_stop_index(&mut self) {
        self.raptor.transit_node_to_stop = vec![u32::MAX; self.nodes.len()];
        self.raptor.transit_stop_to_node.clear();
        self.raptor.transit_stops_tree = KdTree::new(2);

        for (i, node) in self.nodes.iter().enumerate() {
            if matches!(node, NodeData::TransitStop(_)) {
                let compact = self.raptor.transit_stop_to_node.len();
                self.raptor.transit_node_to_stop[i] = compact as u32;
                self.raptor.transit_stop_to_node.push(NodeID(i));
                let loc = node.loc();
                let _ = self
                    .raptor.transit_stops_tree
                    .add([loc.latitude, loc.longitude], compact);
            }
        }
    }

    fn build_stop_patterns(&mut self) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let mut per_stop: Vec<Vec<(PatternID, u32)>> = vec![Vec::new(); n_stops];

        for (p, lookup) in self.raptor.transit_idx_pattern_stops.iter().enumerate() {
            let stops = lookup.of(&self.raptor.transit_pattern_stops);
            for (pos, &node_id) in stops.iter().enumerate() {
                let compact = self.raptor.transit_node_to_stop[node_id.0];
                if compact == u32::MAX {
                    continue;
                }
                per_stop[compact as usize].push((PatternID(p as u32), pos as u32));
            }
        }

        self.raptor.transit_stop_patterns.clear();
        self.raptor.transit_idx_stop_patterns = Vec::with_capacity(n_stops);

        for pairs in &per_stop {
            let start = self.raptor.transit_stop_patterns.len();
            self.raptor.transit_stop_patterns.extend_from_slice(pairs);
            self.raptor.transit_idx_stop_patterns.push(Lookup {
                start,
                len: pairs.len(),
            });
        }
    }

    /// Inverts `transit_stop_transfers` to produce `transit_stop_reverse_transfers`:
    /// for each compact target stop `t`, the list of `(source_compact, walk_secs)`
    /// pairs such that walking from `source` reaches `t` in `walk_secs` seconds.
    /// Used by the backward RAPTOR pass for reverse footpath relaxation.
    fn build_reverse_transfers(&mut self) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let mut reverse_map: Vec<Vec<(usize, u32)>> = vec![Vec::new(); n_stops];

        for source in 0..n_stops {
            let transfers =
                self.raptor.transit_idx_stop_transfers[source].of(&self.raptor.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.raptor.transit_node_to_stop[target_node.0];
                if target != u32::MAX {
                    reverse_map[target as usize].push((source, walk));
                }
            }
        }

        self.raptor.transit_stop_reverse_transfers.clear();
        self.raptor.transit_idx_stop_reverse_transfers = Vec::with_capacity(n_stops);

        for pairs in &reverse_map {
            let start = self.raptor.transit_stop_reverse_transfers.len();
            self.raptor.transit_stop_reverse_transfers.extend_from_slice(pairs);
            self.raptor.transit_idx_stop_reverse_transfers.push(Lookup {
                start,
                len: pairs.len(),
            });
        }
    }

    fn build_stop_transfers(&mut self) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        self.raptor.transit_stop_transfers.clear();
        self.raptor.transit_idx_stop_transfers = Vec::with_capacity(n_stops);

        let max_walk_secs = (MAX_TRANSFER_DISTANCE_M / self.raptor.walking_speed_mps) as u32;

        for i in 0..n_stops {
            let start = self.raptor.transit_stop_transfers.len();
            let stop_node = self.raptor.transit_stop_to_node[i];
            let loc = self.nodes[stop_node.0].loc();

            let origin_osm = match self.nearest_node(loc.latitude, loc.longitude) {
                Some(n) => n,
                None => {
                    self.raptor.transit_idx_stop_transfers
                        .push(Lookup { start, len: 0 });
                    continue;
                }
            };

            let walk_times = self.walk_dijkstra(origin_osm, max_walk_secs);

            let nearby = self
                .raptor.transit_stops_tree
                .within(
                    &[loc.latitude, loc.longitude],
                    meters_to_degrees(MAX_TRANSFER_DISTANCE_M),
                    &squared_euclidean,
                )
                .unwrap_or_default();

            for &(_, &compact_neighbor) in &nearby {
                if compact_neighbor == i {
                    continue;
                }
                let neighbor_node = self.raptor.transit_stop_to_node[compact_neighbor];
                if let Some(&walk_secs) = walk_times.get(&neighbor_node) {
                    self.raptor.transit_stop_transfers.push((neighbor_node, walk_secs));
                }
            }

            self.raptor.transit_idx_stop_transfers.push(Lookup {
                start,
                len: self.raptor.transit_stop_transfers.len() - start,
            });
        }
    }
}
