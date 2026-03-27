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
    /// Each route: (route_idx, short_name, long_name, mode_string)
    pub fn gtfs_agencies_with_routes(
        &self,
    ) -> Vec<(usize, String, String, Vec<(usize, String, String, String)>)> {
        let mut agency_routes: Vec<Vec<(usize, String, String, String)>> =
            vec![vec![]; self.transit_agencies.len()];

        for (route_idx, route) in self.transit_routes.iter().enumerate() {
            let agency_idx = route.agency_id.0 as usize;
            if agency_idx < agency_routes.len() {
                agency_routes[agency_idx].push((
                    route_idx,
                    route.route_short_name.clone(),
                    route.route_long_name.clone(),
                    display_route_type(route.route_type).to_string(),
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
