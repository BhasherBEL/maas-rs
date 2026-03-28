use std::collections::HashMap;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteInfo, ServicePattern, StopTime, TimetableSegment, TripId, TripInfo,
        TripSegment, display_route_type,
    },
    structures::{
        DelayCDF, NodeData, NodeID,
        raptor::{Lookup, PatternInfo},
    },
};

use super::Graph;

impl Graph {
    pub fn get_transit_departures_size(&self) -> usize {
        self.transit_departures.len()
    }

    pub fn add_transit_departures(&mut self, segments: Vec<TripSegment>) {
        self.transit_departures.extend(segments);
    }

    pub fn get_transit_services_size(&self) -> usize {
        self.transit_services.len()
    }

    pub fn add_transit_services(&mut self, services: Vec<ServicePattern>) {
        self.transit_services.extend(services);
    }

    pub fn get_transit_trips_size(&self) -> usize {
        self.transit_trips.len()
    }

    pub fn add_transit_trips(&mut self, trips: Vec<TripInfo>) {
        self.transit_trips.extend(trips);
    }

    pub fn get_transit_routes_size(&self) -> usize {
        self.transit_routes.len()
    }

    pub fn add_transit_routes(&mut self, routes: Vec<RouteInfo>) {
        self.transit_routes.extend(routes);
    }

    pub fn get_transit_agencies_size(&self) -> usize {
        self.transit_agencies.len()
    }

    pub fn add_transit_agencies(&mut self, agencies: Vec<AgencyInfo>) {
        self.transit_agencies.extend(agencies);
    }

    /// Returns all transit stops as (stop_index, name, lat, lon, mode) tuples.
    /// Mode is derived from the RAPTOR pattern index: each stop is assigned the
    /// route type of the first pattern that serves it.
    pub fn gtfs_stops(&self) -> Vec<(usize, String, f64, f64, String)> {
        // Build compact_stop_idx → RouteType from the pattern index.
        let mut stop_mode: Vec<Option<RouteType>> =
            vec![None; self.transit_stop_to_node.len()];
        for (pattern_idx, lookup) in self.transit_idx_pattern_stops.iter().enumerate() {
            let route_type =
                self.transit_routes[self.transit_patterns[pattern_idx].route.0 as usize]
                    .route_type;
            for &node_id in lookup.of(&self.transit_pattern_stops) {
                let compact = self.transit_node_to_stop[node_id.0];
                if compact != u32::MAX {
                    stop_mode[compact as usize].get_or_insert(route_type);
                }
            }
        }

        self.transit_stop_to_node
            .iter()
            .enumerate()
            .filter_map(|(stop_idx, &node_id)| match &self.nodes[node_id.0] {
                NodeData::TransitStop(stop) => Some((
                    stop_idx,
                    stop.name.clone(),
                    stop.lat_lng.latitude,
                    stop.lat_lng.longitude,
                    display_route_type(
                        stop_mode[stop_idx].unwrap_or(RouteType::Bus),
                    )
                    .to_string(),
                )),
                _ => None,
            })
            .collect()
    }

    /// Returns all agencies with their routes as owned data.
    /// Each entry: (agency_idx, name, url, routes)
    /// Each route: (route_idx, short_name, long_name, mode_string, color_hex, text_color_hex)
    pub fn gtfs_agencies_with_routes(
        &self,
    ) -> Vec<(usize, String, String, Vec<(usize, String, String, String, Option<String>, Option<String>)>)> {
        let mut agency_routes: Vec<Vec<(usize, String, String, String, Option<String>, Option<String>)>> =
            vec![vec![]; self.transit_agencies.len()];

        for (route_idx, route) in self.transit_routes.iter().enumerate() {
            let agency_idx = route.agency_id.0 as usize;
            if agency_idx < agency_routes.len() {
                let color = route.route_color.map(|(r, g, b)| format!("{:02X}{:02X}{:02X}", r, g, b));
                let text_color = route.route_text_color.map(|(r, g, b)| format!("{:02X}{:02X}{:02X}", r, g, b));
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

        self.transit_agencies
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
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];

        let start_idx = slice.partition_point(|d| d.departure < time);

        for (i, dep) in slice[start_idx..].iter().enumerate() {
            if self.transit_services[dep.service_id.0 as usize].is_active(date, weekday) {
                return Some((tt.start + start_idx + i, dep));
            }
        }

        None
    }

    pub fn get_transit_departure_slice(&self, tt: TimetableSegment) -> &[TripSegment] {
        &self.transit_departures[tt.start..tt.start + tt.len]
    }

    pub fn previous_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];
        let relative_index = initial_index - tt.start;

        debug_assert!(
            initial_index >= tt.start && initial_index < tt.start + tt.len,
            "initial_index {} out of timetable segment [{}, {}]",
            initial_index,
            tt.start,
            tt.start + tt.len
        );

        slice[..relative_index]
            .iter()
            .rev()
            .enumerate()
            .filter(move |(_, dep)| {
                self.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (initial_index - 1 - i, dep))
    }

    pub fn next_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];
        let relative_index = initial_index - tt.start;

        debug_assert!(
            initial_index >= tt.start && initial_index < tt.start + tt.len,
            "initial_index {} out of timetable segment [{}, {}]",
            initial_index,
            tt.start,
            tt.start + tt.len
        );

        slice
            .get(relative_index + 1..)
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .filter(move |(_, dep)| {
                self.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (initial_index + 1 + i, dep))
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

        let boarding_compact = self.transit_node_to_stop[boarding_node.0];
        if boarding_compact == u32::MAX {
            return vec![];
        }

        // Build the set of trip_ids already covered by the same-route timetable.
        let exclude_slice = &self.transit_departures
            [exclude_timetable.start..exclude_timetable.start + exclude_timetable.len];
        let excluded_trips: HashSet<TripId> =
            exclude_slice.iter().map(|s| s.trip_id).collect();

        let mut seen_trips: HashSet<TripId> = HashSet::new();
        let mut candidates: Vec<(TripId, u32, u32)> = Vec::new();

        let pats = self.transit_idx_stop_patterns[boarding_compact as usize]
            .of(&self.transit_stop_patterns);

        for &(pattern_id, boarding_pos) in pats {
            let n_trips = self.transit_patterns[pattern_id.0 as usize].num_trips as usize;
            if n_trips == 0 {
                continue;
            }
            let pat_stops = self.transit_idx_pattern_stops[pattern_id.0 as usize]
                .of(&self.transit_pattern_stops);

            // Find alighting_node in the stops that come after boarding_pos.
            let alighting_offset = match pat_stops[boarding_pos as usize + 1..]
                .iter()
                .position(|&n| n == alighting_node)
            {
                Some(off) => off,
                None => continue,
            };
            let alighting_pos = boarding_pos as usize + 1 + alighting_offset;

            let all_times = self.transit_idx_pattern_stop_times[pattern_id.0 as usize]
                .of(&self.transit_pattern_stop_times);
            let trip_ids = self.transit_idx_pattern_trips[pattern_id.0 as usize]
                .of(&self.transit_pattern_trips);

            // Stop-time columns: layout is [stop_pos * n_trips + trip_idx].
            let boarding_col =
                &all_times[boarding_pos as usize * n_trips..(boarding_pos as usize + 1) * n_trips];
            let alighting_col =
                &all_times[alighting_pos * n_trips..(alighting_pos + 1) * n_trips];

            if after {
                let start = boarding_col.partition_point(|st| st.departure < reference_time);
                for t in start..n_trips {
                    let trip_id = trip_ids[t];
                    if excluded_trips.contains(&trip_id) || seen_trips.contains(&trip_id) {
                        continue;
                    }
                    let service_id =
                        self.transit_trips[trip_id.0 as usize].service_id;
                    if self.transit_services[service_id.0 as usize].is_active(date, weekday) {
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
                    let service_id =
                        self.transit_trips[trip_id.0 as usize].service_id;
                    if self.transit_services[service_id.0 as usize].is_active(date, weekday) {
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
        self.transit_patterns.push(p);
    }

    pub fn transit_pattern_stops_len(&self) -> usize {
        self.transit_pattern_stops.len()
    }

    pub fn extend_transit_pattern_stops(&mut self, s: &[NodeID]) {
        self.transit_pattern_stops.extend_from_slice(s);
    }

    pub fn push_transit_idx_pattern_stops(&mut self, l: Lookup) {
        self.transit_idx_pattern_stops.push(l);
    }

    pub fn transit_pattern_trips_len(&self) -> usize {
        self.transit_pattern_trips.len()
    }

    pub fn push_transit_pattern_trip(&mut self, t: TripId) {
        self.transit_pattern_trips.push(t);
    }

    pub fn push_transit_idx_pattern_trips(&mut self, l: Lookup) {
        self.transit_idx_pattern_trips.push(l);
    }

    pub fn transit_pattern_stop_times_len(&self) -> usize {
        self.transit_pattern_stop_times.len()
    }

    pub fn push_transit_pattern_stop_time(&mut self, st: StopTime) {
        self.transit_pattern_stop_times.push(st);
    }

    pub fn push_transit_idx_pattern_stop_times(&mut self, l: Lookup) {
        self.transit_idx_pattern_stop_times.push(l);
    }

    pub fn set_transit_delay_models(&mut self, models: HashMap<RouteType, DelayCDF>) {
        self.transit_delay_models = models;
    }
}
