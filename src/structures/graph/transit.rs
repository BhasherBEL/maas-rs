use std::collections::HashMap;

use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteInfo, ServicePattern, StopTime, TimetableSegment, TripId, TripInfo,
        TripSegment,
    },
    structures::{
        DelayCDF, NodeID,
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

        slice[relative_index + 1..]
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
