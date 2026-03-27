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

#[cfg(test)]
mod tests {
    use super::*;
    use gtfs_structures::RouteType;

    // ── IdMapper ──────────────────────────────────────────────────────────────

    #[test]
    fn idmapper_first_insert_returns_zero() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        assert_eq!(m.get_or_insert("alpha".to_string()), 0);
    }

    #[test]
    fn idmapper_second_insert_returns_one() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        m.get_or_insert("a".to_string());
        assert_eq!(m.get_or_insert("b".to_string()), 1);
    }

    #[test]
    fn idmapper_insert_is_idempotent() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        let id1 = m.get_or_insert("dup".to_string());
        let id2 = m.get_or_insert("dup".to_string());
        assert_eq!(id1, id2);
    }

    #[test]
    fn idmapper_get_existing_key() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        m.get_or_insert("x".to_string());
        assert_eq!(m.get("x".to_string()), Some(0));
    }

    #[test]
    fn idmapper_get_missing_key_returns_none() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        assert_eq!(m.get("missing".to_string()), None);
    }

    #[test]
    fn idmapper_to_gtfs_id_roundtrip() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        m.get_or_insert("route_1".to_string());
        m.get_or_insert("route_2".to_string());
        assert_eq!(m.to_gtfs_id(0), "route_1");
        assert_eq!(m.to_gtfs_id(1), "route_2");
    }

    #[test]
    fn idmapper_get_reversed_returns_insertion_order() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        m.get_or_insert("first".to_string());
        m.get_or_insert("second".to_string());
        m.get_or_insert("third".to_string());
        assert_eq!(m.get_reversed(), &vec!["first".to_string(), "second".to_string(), "third".to_string()]);
    }

    // ── display_route_type ────────────────────────────────────────────────────

    #[test]
    fn display_route_type_all_variants() {
        assert_eq!(display_route_type(RouteType::Bus), "Bus");
        assert_eq!(display_route_type(RouteType::Air), "Air");
        assert_eq!(display_route_type(RouteType::Rail), "Rail");
        assert_eq!(display_route_type(RouteType::Taxi), "Taxi");
        assert_eq!(display_route_type(RouteType::Ferry), "Ferry");
        assert_eq!(display_route_type(RouteType::Coach), "Coach");
        assert_eq!(display_route_type(RouteType::Subway), "Subway");
        assert_eq!(display_route_type(RouteType::Funicular), "Funicular");
        assert_eq!(display_route_type(RouteType::Tramway), "Tramway");
        assert_eq!(display_route_type(RouteType::Gondola), "Gondola");
        assert_eq!(display_route_type(RouteType::CableCar), "CableCar");
        assert_eq!(display_route_type(RouteType::Other(-1)), "Other");
        assert_eq!(display_route_type(RouteType::Other(999)), "Other");
    }

    // ── sec_to_time ───────────────────────────────────────────────────────────

    #[test]
    fn sec_to_time_midnight() {
        assert_eq!(sec_to_time(0), "00:00:00");
    }

    #[test]
    fn sec_to_time_noon() {
        assert_eq!(sec_to_time(43200), "12:00:00");
    }

    #[test]
    fn sec_to_time_end_of_day() {
        assert_eq!(sec_to_time(86399), "23:59:59");
    }

    #[test]
    fn sec_to_time_one_hour() {
        assert_eq!(sec_to_time(3600), "01:00:00");
    }

    #[test]
    fn sec_to_time_mixed() {
        assert_eq!(sec_to_time(3661), "01:01:01");
    }

    #[test]
    fn sec_to_time_after_midnight_gtfs() {
        // GTFS allows times > 24h for trips after midnight
        assert_eq!(sec_to_time(86400), "24:00:00");
        assert_eq!(sec_to_time(90000), "25:00:00");
    }
}
