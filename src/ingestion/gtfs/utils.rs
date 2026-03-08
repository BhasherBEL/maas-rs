use std::{collections::HashMap, hash::Hash};

use gtfs_structures::RouteType;

pub struct IdMapper<T, U> {
    to_index: HashMap<T, U>,
    to_string: Vec<T>,
}

impl<T: Eq + Hash + Clone> IdMapper<T, usize> {
    pub fn new() -> Self {
        Self {
            to_index: HashMap::new(),
            to_string: Vec::new(),
        }
    }

    pub fn get_or_insert(&mut self, gtfs_id: T) -> usize {
        if let Some(&idx) = self.to_index.get(&gtfs_id) {
            return idx;
        }
        let idx = self.to_string.len() as usize;
        self.to_string.push(gtfs_id.clone());
        self.to_index.insert(gtfs_id, idx);
        idx
    }

    pub fn get(&mut self, gtfs_id: T) -> Option<usize> {
        if let Some(&idx) = self.to_index.get(&gtfs_id) {
            return Some(idx);
        }
        None
    }

    pub fn to_gtfs_id(&self, idx: u32) -> &T {
        &self.to_string[idx as usize]
    }

    pub fn get_reversed(&self) -> &Vec<T> {
        &self.to_string
    }
}

pub fn display_route_type(route_type: RouteType) -> &'static str {
    match route_type {
        RouteType::Bus => "Bus",
        RouteType::Air => "Air",
        RouteType::Rail => "Rail",
        RouteType::Taxi => "Taxi",
        RouteType::Ferry => "Ferry",
        RouteType::Coach => "Coach",
        RouteType::Subway => "Subway",
        RouteType::Funicular => "Funicular",
        RouteType::Tramway => "Tramway",
        RouteType::Gondola => "Gondola",
        RouteType::CableCar => "CableCar",
        RouteType::Other(_) => "Other",
    }
}

pub fn sec_to_time(sec: u32) -> String {
    let hours = sec / 3600;
    let minutes = (sec % 3600) / 60;
    let seconds = sec % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}
