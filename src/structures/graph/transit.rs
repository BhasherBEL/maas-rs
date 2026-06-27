use std::collections::HashMap;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteId, RouteInfo, ServicePattern, StopTime, TimetableSegment, TripId,
        TripInfo, TripSegment, display_route_type,
    },
    structures::{
        DelayCDF, EdgeData, LatLng, NodeID,
        raptor::{Lookup, PatternInfo},
    },
};

use super::Graph;

impl Graph {
    pub fn get_transit_departures_size(&self) -> usize {
        self.raptor.transit_departures.len()
    }

    pub fn add_transit_departures(&mut self, segments: Vec<TripSegment>) {
        self.raptor.transit_departures.extend(segments);
    }

    pub fn get_transit_services_size(&self) -> usize {
        self.raptor.transit_services.len()
    }

    pub fn add_transit_services(&mut self, services: Vec<ServicePattern>) {
        self.raptor.transit_services.extend(services);
    }

    pub fn get_transit_trips_size(&self) -> usize {
        self.raptor.transit_trips.len()
    }

    pub fn add_transit_trips(&mut self, trips: Vec<TripInfo>) {
        self.raptor.transit_trips.extend(trips);
    }

    /// Append GTFS `trip_id` strings, aligned 1:1 with `add_transit_trips` so the
    /// nth appended string corresponds to the nth appended `TripInfo`.
    pub fn add_transit_trip_ids(&mut self, ids: Vec<String>) {
        self.raptor.transit_trip_ids.extend(ids);
    }

    /// Original GTFS `trip_id` for an internal `TripId`, if known.
    pub fn trip_id_str(&self, trip: TripId) -> Option<&str> {
        self.raptor.trip_id_str(trip)
    }

    /// Internal `TripId` for a GTFS `trip_id` string, if known.
    pub fn trip_index_of(&self, trip_id: &str) -> Option<TripId> {
        self.raptor.trip_index_of(trip_id)
    }

    /// Compact stop index for a GTFS `stop_id` string, if known.
    pub fn stop_index_of(&self, stop_id: &str) -> Option<usize> {
        self.raptor.stop_index_of(stop_id)
    }

    /// GTFS `stop_id` string for a compact stop index, if known.
    pub fn stop_id_str(&self, stop: usize) -> Option<&str> {
        self.raptor.transit_stop_ids.get(stop).map(|s| s.as_str())
    }

    pub fn get_transit_routes_size(&self) -> usize {
        self.raptor.transit_routes.len()
    }

    pub fn add_transit_routes(&mut self, routes: Vec<RouteInfo>) {
        self.raptor.transit_routes.extend(routes);
    }

    pub fn get_transit_agencies_size(&self) -> usize {
        self.raptor.transit_agencies.len()
    }

    pub fn add_transit_agencies(&mut self, agencies: Vec<AgencyInfo>) {
        self.raptor.transit_agencies.extend(agencies);
    }

    /// Returns all transit stops as (stop_index, name, lat, lon, mode) tuples.
    /// Mode is derived from the RAPTOR pattern index: each stop is assigned the
    /// route type of the first pattern that serves it.
    pub fn gtfs_stops(&self) -> Vec<(usize, String, f64, f64, String)> {
        // Build compact_stop_idx → RouteType from the pattern index.
        let mut stop_mode: Vec<Option<RouteType>> =
            vec![None; self.raptor.transit_stop_to_node.len()];
        for (pattern_idx, lookup) in self.raptor.transit_idx_pattern_stops.iter().enumerate() {
            let route_type = self.raptor.transit_routes
                [self.raptor.transit_patterns[pattern_idx].route.0 as usize]
                .route_type;
            for &node_id in lookup.of(&self.raptor.transit_pattern_stops) {
                let compact = self.raptor.transit_node_to_stop[node_id.0];
                if compact != u32::MAX {
                    stop_mode[compact as usize].get_or_insert(route_type);
                }
            }
        }

        self.raptor
            .transit_stop_to_node
            .iter()
            .enumerate()
            .map(|(stop_idx, &node_id)| {
                let loc = self.node_loc(node_id);
                (
                    stop_idx,
                    self.raptor.transit_stop_names[stop_idx].clone(),
                    loc.latitude,
                    loc.longitude,
                    display_route_type(stop_mode[stop_idx].unwrap_or(RouteType::Bus)).to_string(),
                )
            })
            .collect()
    }

    /// G-free plan-node resolution for a `NodeID`: its coordinate (via `node_loc`,
    /// so it survives the interior-node drop) and, when the node is a transit stop,
    /// its display name (from the serialized `transit_stop_names`, not `g.nodes`).
    /// With `g` present this is byte-identical to reading `NodeData` directly.
    pub fn plan_node_info(&self, id: NodeID) -> Option<(crate::structures::LatLng, Option<String>)> {
        if self.nodes.is_empty() {
            self.contracted.as_ref()?;
        } else {
            self.nodes.get(id.0)?;
        }
        let loc = self.node_loc(id);
        let compact = self.raptor.transit_node_to_stop[id.0];
        let name = if compact != u32::MAX {
            Some(self.raptor.transit_stop_names[compact as usize].clone())
        } else {
            None
        };
        Some((loc, name))
    }

    /// Returns all agencies with their routes as owned data.
    /// Each entry: (agency_idx, name, url, routes)
    /// Each route: (route_idx, short_name, long_name, mode_string, color_hex, text_color_hex)
    pub fn gtfs_agencies_with_routes(
        &self,
    ) -> Vec<(
        usize,
        String,
        String,
        Vec<(
            usize,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
        )>,
    )> {
        let mut agency_routes: Vec<
            Vec<(
                usize,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
            )>,
        > = vec![vec![]; self.raptor.transit_agencies.len()];

        for (route_idx, route) in self.raptor.transit_routes.iter().enumerate() {
            let agency_idx = route.agency_id.0 as usize;
            if agency_idx < agency_routes.len() {
                let color = route
                    .route_color
                    .map(|(r, g, b)| crate::structures::plan::rgb_to_hex(r, g, b));
                let text_color = route
                    .route_text_color
                    .map(|(r, g, b)| crate::structures::plan::rgb_to_hex(r, g, b));
                agency_routes[agency_idx].push((
                    route_idx,
                    route.route_short_name.clone(),
                    route.route_long_name.clone(),
                    display_route_type(route.route_type).to_string(),
                    color,
                    text_color,
                ));
            }
        }

        self.raptor
            .transit_agencies
            .iter()
            .enumerate()
            .map(|(i, agency)| {
                (
                    i,
                    agency.name.clone(),
                    agency.url.clone(),
                    agency_routes[i].clone(),
                )
            })
            .collect()
    }

    pub fn next_transit_departure(
        &self,
        tt: TimetableSegment,
        time: u32,
        date: u32,
        weekday: u8,
    ) -> Option<(usize, &TripSegment)> {
        let slice = &self.raptor.transit_departures[tt.start..tt.start + tt.len];

        let start_idx = slice.partition_point(|d| d.departure < time);

        for (i, dep) in slice[start_idx..].iter().enumerate() {
            if self.raptor.transit_services[dep.service_id.0 as usize].is_active(date, weekday) {
                return Some((tt.start + start_idx + i, dep));
            }
        }

        None
    }

    pub fn get_transit_departure_slice(&self, tt: TimetableSegment) -> &[TripSegment] {
        &self.raptor.transit_departures[tt.start..tt.start + tt.len]
    }

    pub fn previous_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.raptor.transit_departures[tt.start..tt.start + tt.len];
        // Guard against an out-of-segment index (saturating, never underflows): an
        // inconsistent `initial_index` yields no previous departures rather than panicking.
        let relative_index = initial_index.saturating_sub(tt.start).min(slice.len());
        let base = tt.start + relative_index;

        slice[..relative_index]
            .iter()
            .rev()
            .enumerate()
            .filter(move |(_, dep)| {
                self.raptor.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (base - 1 - i, dep))
    }

    pub fn next_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.raptor.transit_departures[tt.start..tt.start + tt.len];
        // Guard against an out-of-segment index (saturating, never underflows).
        let relative_index = initial_index.saturating_sub(tt.start).min(slice.len());
        let base = tt.start + relative_index;

        slice
            .get(relative_index + 1..)
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .filter(move |(_, dep)| {
                self.raptor.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (base + 1 + i, dep))
    }

    /// Returns up to `count` departures from RAPTOR patterns that serve both
    /// `boarding_node` and `alighting_node` (boarding before alighting), excluding
    /// trips already covered by `exclude_timetable` (the same-route timetable
    /// segment of the original leg's first hop).
    ///
    /// `after = true`  → next departures (departure >= reference_time)
    /// `after = false` → previous departures (departure < reference_time)
    ///
    /// Returns `(TripId, boarding_departure_secs, alighting_arrival_secs)` tuples.
    pub fn cross_route_departures(
        &self,
        boarding_node: NodeID,
        alighting_node: NodeID,
        exclude_timetable: TimetableSegment,
        reference_time: u32,
        date: u32,
        weekday: u8,
        after: bool,
        count: usize,
    ) -> Vec<(TripId, u32, u32)> {
        use std::collections::HashSet;

        let boarding_compact = self.raptor.transit_node_to_stop[boarding_node.0];
        if boarding_compact == u32::MAX {
            return vec![];
        }

        // Build the set of trip_ids already covered by the same-route timetable.
        let exclude_slice = &self.raptor.transit_departures
            [exclude_timetable.start..exclude_timetable.start + exclude_timetable.len];
        let excluded_trips: HashSet<TripId> = exclude_slice.iter().map(|s| s.trip_id).collect();

        let mut seen_trips: HashSet<TripId> = HashSet::new();
        let mut candidates: Vec<(TripId, u32, u32)> = Vec::new();

        let pats = self.raptor.transit_idx_stop_patterns[boarding_compact as usize]
            .of(&self.raptor.transit_stop_patterns);

        for &(pattern_id, boarding_pos) in pats {
            let n_trips = self.raptor.transit_patterns[pattern_id.0 as usize].num_trips as usize;
            if n_trips == 0 {
                continue;
            }
            let pat_stops = self.raptor.transit_idx_pattern_stops[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_stops);

            // Find alighting_node in the stops that come after boarding_pos.
            let alighting_offset = match pat_stops[boarding_pos as usize + 1..]
                .iter()
                .position(|&n| n == alighting_node)
            {
                Some(off) => off,
                None => continue,
            };
            let alighting_pos = boarding_pos as usize + 1 + alighting_offset;

            let all_times = self.raptor.transit_idx_pattern_stop_times[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_stop_times);
            let trip_ids = self.raptor.transit_idx_pattern_trips[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_trips);

            // Stop-time columns: layout is [stop_pos * n_trips + trip_idx].
            let boarding_col =
                &all_times[boarding_pos as usize * n_trips..(boarding_pos as usize + 1) * n_trips];
            let alighting_col = &all_times[alighting_pos * n_trips..(alighting_pos + 1) * n_trips];

            if after {
                let start = boarding_col.partition_point(|st| st.departure < reference_time);
                for t in start..n_trips {
                    let trip_id = trip_ids[t];
                    if excluded_trips.contains(&trip_id) || seen_trips.contains(&trip_id) {
                        continue;
                    }
                    let service_id = self.raptor.transit_trips[trip_id.0 as usize].service_id;
                    if self.raptor.transit_services[service_id.0 as usize].is_active(date, weekday)
                    {
                        seen_trips.insert(trip_id);
                        candidates.push((
                            trip_id,
                            boarding_col[t].departure,
                            alighting_col[t].arrival,
                        ));
                    }
                }
            } else {
                let end = boarding_col.partition_point(|st| st.departure < reference_time);
                for t in (0..end).rev() {
                    let trip_id = trip_ids[t];
                    if excluded_trips.contains(&trip_id) || seen_trips.contains(&trip_id) {
                        continue;
                    }
                    let service_id = self.raptor.transit_trips[trip_id.0 as usize].service_id;
                    if self.raptor.transit_services[service_id.0 as usize].is_active(date, weekday)
                    {
                        seen_trips.insert(trip_id);
                        candidates.push((
                            trip_id,
                            boarding_col[t].departure,
                            alighting_col[t].arrival,
                        ));
                    }
                }
            }
        }

        if after {
            candidates.sort_by_key(|&(_, dep, _)| dep);
        } else {
            candidates.sort_by_key(|&(_, dep, _)| std::cmp::Reverse(dep));
        }
        candidates.truncate(count);
        candidates
    }

    // RAPTOR pattern management

    pub fn push_transit_pattern(&mut self, p: PatternInfo) {
        self.raptor.transit_patterns.push(p);
    }

    pub fn transit_pattern_stops_len(&self) -> usize {
        self.raptor.transit_pattern_stops.len()
    }

    pub fn extend_transit_pattern_stops(&mut self, s: &[NodeID]) {
        self.raptor.transit_pattern_stops.extend_from_slice(s);
    }

    pub fn push_transit_idx_pattern_stops(&mut self, l: Lookup) {
        self.raptor.transit_idx_pattern_stops.push(l);
    }

    pub fn transit_pattern_trips_len(&self) -> usize {
        self.raptor.transit_pattern_trips.len()
    }

    pub fn push_transit_pattern_trip(&mut self, t: TripId) {
        self.raptor.transit_pattern_trips.push(t);
    }

    pub fn push_transit_idx_pattern_trips(&mut self, l: Lookup) {
        self.raptor.transit_idx_pattern_trips.push(l);
    }

    pub fn transit_pattern_stop_times_len(&self) -> usize {
        self.raptor.transit_pattern_stop_times.len()
    }

    pub fn push_transit_pattern_stop_time(&mut self, st: StopTime) {
        self.raptor.transit_pattern_stop_times.push(st);
    }

    pub fn push_transit_idx_pattern_stop_times(&mut self, l: Lookup) {
        self.raptor.transit_idx_pattern_stop_times.push(l);
    }

    pub fn set_transit_delay_models(&mut self, models: HashMap<RouteType, DelayCDF>) {
        self.raptor.transit_delay_models = models;
    }

    pub fn get_delay_model(&self, route_type: RouteType) -> Option<&DelayCDF> {
        self.raptor.transit_delay_models.get(&route_type)
    }

    /// Probability that an alternative for one transit leg still makes the *next*
    /// (unchanged) transit leg of the journey — the "outbound" half of an
    /// alternative's marginal swap reliability.
    ///
    /// `following_margin_secs` is the original plan's outbound slack (next
    /// boarding − this leg's scheduled arrival at the boarding stop). `arrival_shift`
    /// is `original_scheduled_end − alternative_end`: positive when the alternative
    /// arrives earlier (more slack), negative when it arrives later (less slack).
    /// The feeder is this alternative's own vehicle (`leg_route_type`); the boarding
    /// vehicle is the next leg's (`following_route_type`).
    ///
    /// Returns `1.0` when there is no following transit leg (last leg of the
    /// journey) or no delay model for the alternative's own route type.
    pub(crate) fn outbound_reliability(
        &self,
        leg_route_type: Option<RouteType>,
        following_route_type: Option<RouteType>,
        following_margin_secs: Option<i32>,
        arrival_shift: i32,
    ) -> f32 {
        let (Some(_), Some(base_margin)) = (following_route_type, following_margin_secs) else {
            return 1.0;
        };
        let board = following_route_type.and_then(|rt| self.get_delay_model(rt));
        match leg_route_type.and_then(|rt| self.get_delay_model(rt)) {
            Some(feeder) => feeder.prob_on_time_vs(board, base_margin + arrival_shift),
            None => 1.0,
        }
    }

    pub fn route_type_of_trip(&self, trip_id: TripId) -> Option<RouteType> {
        let route_id = self.get_trip(trip_id)?.route_id;
        self.get_route(route_id).map(|r| r.route_type)
    }

    /// Returns the latest trip across all RAPTOR patterns serving both
    /// `boarding_node` → `alighting_node` (in that order) where:
    ///   - boarding departure ≥ min_boarding
    ///   - alighting arrival  ≤ max_alighting
    ///   - trip is active on date/weekday
    ///
    /// Returns (dep_abs_idx_in_transit_departures, boarding_dep, alighting_arr).
    pub fn latest_departure_before_arrival(
        &self,
        boarding_node: NodeID,
        alighting_node: NodeID,
        min_boarding: u32,
        max_alighting: u32,
        date: u32,
        weekday: u8,
    ) -> Option<(usize, u32, u32)> {
        let boarding_compact = self.raptor.transit_node_to_stop[boarding_node.0];
        if boarding_compact == u32::MAX {
            return None;
        }

        let pats = self.raptor.transit_idx_stop_patterns[boarding_compact as usize]
            .of(&self.raptor.transit_stop_patterns);

        let mut best: Option<(usize, u32, u32)> = None;

        for &(pattern_id, boarding_pos) in pats {
            let n_trips = self.raptor.transit_patterns[pattern_id.0 as usize].num_trips as usize;
            if n_trips == 0 {
                continue;
            }
            let pat_stops = self.raptor.transit_idx_pattern_stops[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_stops);

            // Find alighting_node after boarding_pos.
            let alighting_offset = match pat_stops[boarding_pos as usize + 1..]
                .iter()
                .position(|&n| n == alighting_node)
            {
                Some(off) => off,
                None => continue,
            };
            let alighting_pos = boarding_pos as usize + 1 + alighting_offset;

            let all_times = self.raptor.transit_idx_pattern_stop_times[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_stop_times);
            let trip_ids = self.raptor.transit_idx_pattern_trips[pattern_id.0 as usize]
                .of(&self.raptor.transit_pattern_trips);

            let boarding_col =
                &all_times[boarding_pos as usize * n_trips..(boarding_pos as usize + 1) * n_trips];
            let alighting_col = &all_times[alighting_pos * n_trips..(alighting_pos + 1) * n_trips];

            let start_t = boarding_col.partition_point(|st| st.departure < min_boarding);

            // Locate the timetable_segment for the first hop of this pattern so we
            // can translate trip_id → absolute dep_idx in transit_departures.
            let boarding_stop_node = pat_stops[boarding_pos as usize];
            let next_stop_node = pat_stops[boarding_pos as usize + 1];
            let route_id: RouteId = self.raptor.transit_patterns[pattern_id.0 as usize].route;
            // Edge timetable from `g.edges`. Unavailable once the interior arrays are
            // dropped — the contracted path reads the precomputed side-table instead.
            let scan = || {
                self.edges[boarding_stop_node.0].iter().find_map(|e| match e {
                    EdgeData::Transit(te)
                        if te.destination == next_stop_node && te.route_id == route_id =>
                    {
                        Some(te.timetable_segment)
                    }
                    _ => None,
                })
            };
            let from_table = || {
                self.raptor
                    .transit_pattern_segment_timetables
                    .get(pattern_id.0 as usize)
                    .and_then(|segs| segs.get(boarding_pos as usize).copied())
            };
            let resolved = if self.use_contracted() {
                let t = from_table();
                debug_assert!(
                    t.is_some(),
                    "contracted segment-timetable side-table miss (pattern {})",
                    pattern_id.0
                );
                t.or_else(|| (!self.nodes.is_empty()).then(scan).flatten())
            } else {
                scan()
            };
            let ts = match resolved {
                Some(ts) => ts,
                None => continue,
            };

            // Scan backwards from the latest trip to find the latest feasible one.
            for t in (start_t..n_trips).rev() {
                let arr = alighting_col[t].arrival;
                if arr > max_alighting {
                    continue;
                }

                let trip_id = trip_ids[t];
                let svc = self.raptor.transit_trips[trip_id.0 as usize].service_id;
                if !self.raptor.transit_services[svc.0 as usize].is_active(date, weekday) {
                    continue;
                }

                let dep = boarding_col[t].departure;
                let dep_abs_idx = match self.raptor.transit_departures[ts.start..ts.start + ts.len]
                    .iter()
                    .position(|seg| seg.trip_id == trip_id)
                {
                    Some(i) => ts.start + i,
                    None => continue,
                };

                if best.is_none_or(|(_, best_dep, _)| dep > best_dep) {
                    best = Some((dep_abs_idx, dep, arr));
                }
                break; // Latest feasible trip for this pattern found.
            }
        }

        best
    }

    /// Push shape data for the next pattern (call once per pattern, in order).
    pub fn push_transit_pattern_shape(&mut self, points: Vec<LatLng>, stop_idx: Vec<u32>) {
        self.raptor.transit_pattern_shapes.push(points);
        self.raptor.transit_pattern_shape_stop_idx.push(stop_idx);
    }

    /// Returns `(shape_points, stop_indices)` for pattern `p`, or `None` if
    /// no shape data was stored (pattern was pushed without a shape, or vecs are
    /// shorter than `p` for backward-compat reasons).
    pub fn get_pattern_shape(&self, p: usize) -> Option<(&[LatLng], &[u32])> {
        let pts = self.raptor.transit_pattern_shapes.get(p)?;
        let idx = self.raptor.transit_pattern_shape_stop_idx.get(p)?;
        if pts.is_empty() {
            None
        } else {
            Some((pts, idx))
        }
    }

    /// Number of transit patterns currently in the graph.
    pub fn transit_pattern_count(&self) -> usize {
        self.raptor.transit_patterns.len()
    }

    /// Returns the ordered stop-node sequence for pattern `p`.
    pub fn get_pattern_stop_nodes(&self, p: usize) -> &[NodeID] {
        self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops)
    }

    /// Overwrites the shape for pattern `p` (must already exist — generic loader
    /// always pushes an empty shape, so this is safe for any pattern it loaded).
    pub fn set_pattern_shape(&mut self, p: usize, pts: Vec<LatLng>, stop_idx: Vec<u32>) {
        if p < self.raptor.transit_pattern_shapes.len() {
            self.raptor.transit_pattern_shapes[p] = pts;
            self.raptor.transit_pattern_shape_stop_idx[p] = stop_idx;
        }
    }
}

#[cfg(test)]
mod outbound_reliability_tests {
    use super::*;

    /// A bus-mode CDF: ~62% on time (delay ≤ 0), rising with slack.
    fn bus_cdf() -> DelayCDF {
        DelayCDF {
            bins: vec![
                (-120, 0.10),
                (0, 0.62),
                (120, 0.80),
                (300, 0.95),
                (600, 1.00),
            ],
        }
    }

    fn graph_with_bus_model() -> Graph {
        let mut g = Graph::new();
        let mut models = HashMap::new();
        models.insert(RouteType::Bus, bus_cdf());
        g.set_transit_delay_models(models);
        g
    }

    #[test]
    fn last_leg_has_no_following_so_reliability_is_one() {
        let g = graph_with_bus_model();
        // No following route type / margin ⇒ outbound term is identity.
        assert_eq!(
            g.outbound_reliability(Some(RouteType::Bus), None, None, 0),
            1.0
        );
    }

    #[test]
    fn missed_downstream_connection_collapses_to_near_zero() {
        let g = graph_with_bus_model();
        // Original slack 60s, but the alternative arrives 600s later than planned
        // ⇒ effective margin −540s ⇒ essentially impossible to make the next leg.
        let rel =
            g.outbound_reliability(Some(RouteType::Bus), Some(RouteType::Bus), Some(60), -600);
        assert!(rel < 0.05, "expected near-zero, got {rel}");
    }

    #[test]
    fn earlier_arrival_loosens_the_connection() {
        let g = graph_with_bus_model();
        // Arriving 300s earlier than planned adds slack ⇒ more reliable than the
        // same connection at the original margin.
        let base = g.outbound_reliability(Some(RouteType::Bus), Some(RouteType::Bus), Some(60), 0);
        let earlier =
            g.outbound_reliability(Some(RouteType::Bus), Some(RouteType::Bus), Some(60), 300);
        assert!(
            earlier > base,
            "earlier ({earlier}) should beat base ({base})"
        );
    }

    #[test]
    fn unknown_leg_route_type_is_treated_as_certain() {
        let g = graph_with_bus_model();
        // No delay model for the alternative's own vehicle ⇒ outbound term = 1.0.
        assert_eq!(
            g.outbound_reliability(Some(RouteType::Ferry), Some(RouteType::Bus), Some(60), -600),
            1.0
        );
    }
}
