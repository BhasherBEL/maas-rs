use kdtree::{KdTree, distance::squared_euclidean};

use std::collections::{BTreeSet, HashMap};

use crate::structures::{
    LatLng, NodeData, NodeID, meters_to_degrees,
    raptor::{Lookup, PatternID, PatternInfo},
};

use super::{Graph, MAX_TRANSFER_DISTANCE_M, StationInfo, StationLine};

/// Display ordering rank for a transport-mode label: Rail, then Subway/Metro,
/// Tram, Bus, then everything else (resolved alphabetically by the label itself).
fn mode_rank(mode: &str) -> u8 {
    match mode {
        "Rail" => 0,
        "Subway" => 1,
        "Tramway" => 2,
        "Bus" => 3,
        _ => 4,
    }
}

/// Natural/numeric-aware sort key for a line `short_name`: all-digit names sort
/// numerically and before any name containing non-digits ("5" < "61" < "81" <
/// "M1"); non-numeric names sort lexically among themselves. Fully deterministic.
fn natural_key(short_name: &str) -> (u8, u64, String) {
    if !short_name.is_empty() && short_name.bytes().all(|b| b.is_ascii_digit()) {
        (0, short_name.parse::<u64>().unwrap_or(u64::MAX), String::new())
    } else {
        (1, 0, short_name.to_string())
    }
}

impl Graph {
    pub fn build_raptor_index(&mut self) {
        self.split_overtaking_patterns();
        self.build_compact_stop_index();
        self.build_stop_patterns();
        self.build_station_index();
        self.build_pattern_segment_timetables();
        self.build_stop_transfers();
        self.build_reverse_transfers();
        self.raptor.build_runtime_indices();
        self.build_edge_index();
    }

    /// Restore the FIFO (non-overtaking) precondition that `scan_route`'s
    /// `partition_point` boarding cutoff relies on. Patterns are grouped purely by
    /// stop sequence and sorted by their stop-0 departure, but when an express
    /// overtakes a stopping trip between two stops a mid-stop departure column
    /// becomes non-monotonic, and `partition_point` (a binary search assuming a
    /// sorted column) can skip a feasible/optimal trip.
    ///
    /// This splits each pattern into the minimum-ish number of sub-routes such that
    /// every per-stop DEPARTURE column is non-decreasing within a sub-route, using a
    /// greedy first-fit chain decomposition over trips in stop-0 order: append a trip
    /// to the first chain whose last trip is `<=` it at EVERY stop (so the column
    /// stays non-decreasing), else open a new chain. Each chain becomes its own
    /// pattern. Non-overtaking patterns yield a single chain → identical output, so
    /// the transform is idempotent and transparent to everything downstream
    /// (scan_route, stop→pattern membership, segment timetables, shapes, realtime
    /// trip resolution): a trip still lives in exactly one pattern, with its columns
    /// intact. Must run before the other build steps, which read the split patterns.
    fn split_overtaking_patterns(&mut self) {
        let r = &mut self.raptor;
        let n_patterns = r.transit_patterns.len();
        if n_patterns == 0 {
            return;
        }
        if r.transit_idx_pattern_stops.len() != n_patterns
            || r.transit_idx_pattern_trips.len() != n_patterns
            || r.transit_idx_pattern_stop_times.len() != n_patterns
        {
            return;
        }
        let has_shapes = r.transit_pattern_shapes.len() == n_patterns
            && r.transit_pattern_shape_stop_idx.len() == n_patterns;

        let mut new_patterns: Vec<PatternInfo> = Vec::with_capacity(n_patterns);
        let mut new_pattern_stops: Vec<NodeID> = Vec::with_capacity(r.transit_pattern_stops.len());
        let mut new_idx_pattern_stops: Vec<Lookup> = Vec::with_capacity(n_patterns);
        let mut new_pattern_trips: Vec<crate::ingestion::gtfs::TripId> =
            Vec::with_capacity(r.transit_pattern_trips.len());
        let mut new_idx_pattern_trips: Vec<Lookup> = Vec::with_capacity(n_patterns);
        let mut new_stop_times: Vec<crate::ingestion::gtfs::StopTime> =
            Vec::with_capacity(r.transit_pattern_stop_times.len());
        let mut new_idx_stop_times: Vec<Lookup> = Vec::with_capacity(n_patterns);
        let mut new_shapes: Vec<Vec<crate::structures::LatLng>> = Vec::new();
        let mut new_shape_stop_idx: Vec<Vec<u32>> = Vec::new();

        for p in 0..n_patterns {
            let stops = r.transit_idx_pattern_stops[p].of(&r.transit_pattern_stops);
            let trips = r.transit_idx_pattern_trips[p].of(&r.transit_pattern_trips);
            let times = r.transit_idx_pattern_stop_times[p].of(&r.transit_pattern_stop_times);
            let route = r.transit_patterns[p].route;
            let n_stops = stops.len();
            let n_trips = trips.len();

            let dep = |s: usize, t: usize| times[s * n_trips + t].departure;
            let dominates = |a: usize, b: usize| (0..n_stops).all(|s| dep(s, a) <= dep(s, b));

            let mut chains: Vec<Vec<usize>> = Vec::new();
            for t in 0..n_trips {
                let mut placed = false;
                for chain in chains.iter_mut() {
                    if dominates(*chain.last().unwrap(), t) {
                        chain.push(t);
                        placed = true;
                        break;
                    }
                }
                if !placed {
                    chains.push(vec![t]);
                }
            }

            for chain in &chains {
                new_patterns.push(PatternInfo {
                    route,
                    num_trips: chain.len() as u32,
                });

                let ss = new_pattern_stops.len();
                new_pattern_stops.extend_from_slice(stops);
                new_idx_pattern_stops.push(Lookup {
                    start: ss,
                    len: n_stops,
                });

                let ts = new_pattern_trips.len();
                for &t in chain {
                    new_pattern_trips.push(trips[t]);
                }
                new_idx_pattern_trips.push(Lookup {
                    start: ts,
                    len: chain.len(),
                });

                let sts = new_stop_times.len();
                for s in 0..n_stops {
                    for &t in chain {
                        new_stop_times.push(times[s * n_trips + t]);
                    }
                }
                new_idx_stop_times.push(Lookup {
                    start: sts,
                    len: n_stops * chain.len(),
                });

                if has_shapes {
                    new_shapes.push(r.transit_pattern_shapes[p].clone());
                    new_shape_stop_idx.push(r.transit_pattern_shape_stop_idx[p].clone());
                }
            }
        }

        let split_count = new_patterns.len().saturating_sub(n_patterns);
        if split_count > 0 {
            tracing::info!(
                "overtaking split: {n_patterns} patterns → {} routes (+{split_count} sub-routes)",
                new_patterns.len(),
            );
        }

        r.transit_patterns = new_patterns;
        r.transit_pattern_stops = new_pattern_stops;
        r.transit_idx_pattern_stops = new_idx_pattern_stops;
        r.transit_pattern_trips = new_pattern_trips;
        r.transit_idx_pattern_trips = new_idx_pattern_trips;
        r.transit_pattern_stop_times = new_stop_times;
        r.transit_idx_pattern_stop_times = new_idx_stop_times;
        if has_shapes {
            r.transit_pattern_shapes = new_shapes;
            r.transit_pattern_shape_stop_idx = new_shape_stop_idx;
        }
    }

    /// Precompute, per pattern and per inter-stop segment, the transit edge's
    /// `timetable_segment` by scanning `g.edges` while the full graph is present. Plan
    /// reconstruction reads this instead of `self.edges[..]`, so transit legs survive the
    /// node-contraction drop of the interior-node arrays.
    fn build_pattern_segment_timetables(&mut self) {
        use crate::ingestion::gtfs::TimetableSegment;
        use crate::structures::EdgeData;

        let mut per_pattern: Vec<Vec<TimetableSegment>> =
            Vec::with_capacity(self.raptor.transit_idx_pattern_stops.len());
        for (p, lookup) in self.raptor.transit_idx_pattern_stops.iter().enumerate() {
            let stops = lookup.of(&self.raptor.transit_pattern_stops);
            let route_id = self.raptor.transit_patterns[p].route;
            let mut segs: Vec<TimetableSegment> = Vec::with_capacity(stops.len().saturating_sub(1));
            for w in stops.windows(2) {
                let (from, to) = (w[0], w[1]);
                let tt = self.edges[from.0]
                    .iter()
                    .find_map(|e| match e {
                        EdgeData::Transit(te)
                            if te.destination == to && te.route_id == route_id =>
                        {
                            Some(te.timetable_segment)
                        }
                        _ => None,
                    })
                    .unwrap_or(TimetableSegment { start: 0, len: 0 });
                segs.push(tt);
            }
            per_pattern.push(segs);
        }
        self.raptor.transit_pattern_segment_timetables = per_pattern;
    }

    fn build_compact_stop_index(&mut self) {
        self.raptor.transit_node_to_stop = vec![u32::MAX; self.nodes.len()];
        self.raptor.transit_stop_to_node.clear();
        self.raptor.transit_stop_ids.clear();
        self.raptor.transit_stop_names.clear();
        self.raptor.transit_stop_platform_codes.clear();
        self.raptor.transit_stops_tree = KdTree::new(2);

        for (i, node) in self.nodes.iter().enumerate() {
            if let NodeData::TransitStop(stop) = node {
                let compact = self.raptor.transit_stop_to_node.len();
                self.raptor.transit_node_to_stop[i] = compact as u32;
                self.raptor.transit_stop_to_node.push(NodeID(i));
                self.raptor.transit_stop_ids.push(stop.id.clone());
                self.raptor
                    .transit_stop_names
                    .push(crate::ingestion::gtfs::harmonize_display_name(&stop.name));
                self.raptor.transit_stop_platform_codes.push(stop.platform_code.clone());
                let loc = node.loc();
                let _ = self
                    .raptor
                    .transit_stops_tree
                    .add([loc.latitude, loc.longitude], compact);
            }
        }
    }

    /// Group compact stops into physical stations by their (non-empty)
    /// `parent_station`; stops without one each form a standalone station. The
    /// station id is the shared parent value (or the lone stop's `stop_id`),
    /// coord is the member centroid (mean), and operators are the distinct agency
    /// names of the patterns serving any member. Reads `parent_station` from the
    /// (still-present) interior nodes, so it must run at build time before any
    /// node-array drop. Requires `build_stop_patterns` first (operators read
    /// `transit_idx_stop_patterns`).
    fn build_station_index(&mut self) {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let mut stations: Vec<StationInfo> = Vec::new();
        let mut key_to_idx: HashMap<String, usize> = HashMap::new();
        let mut op_sets: Vec<BTreeSet<String>> = Vec::new();
        let mut mode_sets: Vec<BTreeSet<String>> = Vec::new();
        let mut line_seen: Vec<std::collections::HashSet<(String, String, Option<String>)>> =
            Vec::new();
        let mut line_lists: Vec<Vec<StationLine>> = Vec::new();
        let mut sums: Vec<(f64, f64)> = Vec::new();

        for compact in 0..n_stops {
            let node_id = self.raptor.transit_stop_to_node[compact];
            let parent = match &self.nodes[node_id.0] {
                NodeData::TransitStop(s) => s.parent_station.clone().filter(|p| !p.is_empty()),
                _ => None,
            };
            let stop_id = self.raptor.transit_stop_ids[compact].clone();
            let key = parent.unwrap_or_else(|| stop_id.clone());

            let idx = *key_to_idx.entry(key.clone()).or_insert_with(|| {
                let i = stations.len();
                stations.push(StationInfo {
                    id: key.clone(),
                    name: self.raptor.transit_stop_names[compact].clone(),
                    lat_lng: LatLng {
                        latitude: 0.0,
                        longitude: 0.0,
                    },
                    operators: Vec::new(),
                    modes: Vec::new(),
                    lines: Vec::new(),
                    platform_stop_indices: Vec::new(),
                });
                op_sets.push(BTreeSet::new());
                mode_sets.push(BTreeSet::new());
                line_seen.push(std::collections::HashSet::new());
                line_lists.push(Vec::new());
                sums.push((0.0, 0.0));
                i
            });

            if stop_id == stations[idx].id {
                stations[idx].name = self.raptor.transit_stop_names[compact].clone();
            }

            stations[idx].platform_stop_indices.push(compact);
            let loc = self.nodes[node_id.0].loc();
            sums[idx].0 += loc.latitude;
            sums[idx].1 += loc.longitude;

            let pats = self.raptor.transit_idx_stop_patterns[compact]
                .of(&self.raptor.transit_stop_patterns);
            for &(pattern_id, _) in pats {
                let route = self.raptor.transit_patterns[pattern_id.0 as usize].route;
                let route_info = &self.raptor.transit_routes[route.0 as usize];
                let agency_id = route_info.agency_id;
                let mode = crate::ingestion::gtfs::display_route_type(route_info.route_type)
                    .to_string();
                mode_sets[idx].insert(mode.clone());
                if let Some(agency) = self.raptor.transit_agencies.get(agency_id.0 as usize) {
                    op_sets[idx].insert(agency.name.clone());
                }
                let color = route_info
                    .route_color
                    .map(|(r, g, b)| crate::structures::plan::rgb_to_hex(r, g, b));
                let text_color = route_info
                    .route_text_color
                    .map(|(r, g, b)| crate::structures::plan::rgb_to_hex(r, g, b));
                let dedup_key = (mode.clone(), route_info.route_short_name.clone(), color.clone());
                if line_seen[idx].insert(dedup_key) {
                    line_lists[idx].push(StationLine {
                        mode,
                        short_name: route_info.route_short_name.clone(),
                        color,
                        text_color,
                    });
                }
            }
        }

        for (i, st) in stations.iter_mut().enumerate() {
            let n = st.platform_stop_indices.len() as f64;
            st.lat_lng = LatLng {
                latitude: sums[i].0 / n,
                longitude: sums[i].1 / n,
            };
            st.operators = op_sets[i].iter().cloned().collect();
            st.modes = mode_sets[i].iter().cloned().collect();
            let mut lines = std::mem::take(&mut line_lists[i]);
            lines.sort_by(|a, b| {
                (mode_rank(&a.mode), &a.mode, natural_key(&a.short_name)).cmp(&(
                    mode_rank(&b.mode),
                    &b.mode,
                    natural_key(&b.short_name),
                ))
            });
            st.lines = lines;
        }

        self.raptor.transit_stations = stations;
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
            let transfers = self.raptor.transit_idx_stop_transfers[source]
                .of(&self.raptor.transit_stop_transfers);
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
            self.raptor
                .transit_stop_reverse_transfers
                .extend_from_slice(pairs);
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
                    self.raptor
                        .transit_idx_stop_transfers
                        .push(Lookup { start, len: 0 });
                    continue;
                }
            };

            let walk_times = self.walk_dijkstra(origin_osm, max_walk_secs);

            let nearby = self
                .raptor
                .transit_stops_tree
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
                    self.raptor
                        .transit_stop_transfers
                        .push((neighbor_node, walk_secs));
                }
            }

            self.raptor.transit_idx_stop_transfers.push(Lookup {
                start,
                len: self.raptor.transit_stop_transfers.len() - start,
            });
        }
    }
}
