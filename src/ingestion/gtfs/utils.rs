use std::{collections::HashMap, hash::Hash};

use gtfs_structures::RouteType;

pub struct IdMapper<T, U> {
    to_index: HashMap<T, U>,
    to_string: Vec<T>,
}

impl<T: Eq + Hash + Clone> Default for IdMapper<T, usize> {
    fn default() -> Self {
        Self::new()
    }
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
        let idx = self.to_string.len();
        self.to_string.push(gtfs_id.clone());
        self.to_index.insert(gtfs_id, idx);
        idx
    }

    pub fn get(&self, gtfs_id: &T) -> Option<usize> {
        self.to_index.get(gtfs_id).copied()
    }

    pub fn len(&self) -> usize {
        self.to_string.len()
    }

    pub fn is_empty(&self) -> bool {
        self.to_string.is_empty()
    }

    pub fn strings(&self) -> &[T] {
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

/// Title-cases all-caps feed display names (STIB "GARE DU NORD" -> "Gare du Nord") to
/// match mixed-case feeds. Names already containing a lowercase letter are left unchanged
/// (keeps this idempotent). Display only; never feeds dedup/grouping normalization.
pub fn harmonize_display_name(name: &str) -> String {
    const PARTICLES: &[&str] = &[
        "de", "du", "des", "d", "la", "le", "les", "l", "un", "une", "a", "au", "aux", "et", "en",
        "sur", "sous", "lez", "van", "der", "den", "op", "ten", "ter", "t", "ver",
    ];

    if name.chars().any(|c| c.is_ascii_lowercase()) {
        return name.to_string();
    }

    fn is_roman_numeral(part: &str) -> bool {
        part.chars().count() >= 2
            && part
                .chars()
                .all(|c| matches!(c.to_ascii_uppercase(), 'I' | 'V' | 'X' | 'L' | 'C' | 'D' | 'M'))
    }

    fn titlecase_part(part: &str) -> String {
        if is_roman_numeral(part) {
            return part.to_uppercase();
        }
        let mut out = String::with_capacity(part.len());
        let mut seen_alpha = false;
        for c in part.chars() {
            if c.is_alphabetic() && !seen_alpha {
                out.extend(c.to_uppercase());
                seen_alpha = true;
            } else {
                out.extend(c.to_lowercase());
            }
        }
        out
    }

    let titlecase_token = |token: &str| -> String {
        let mut out = String::with_capacity(token.len());
        let mut part = String::new();
        for c in token.chars() {
            if c == '-' || c == '\'' {
                out.push_str(&titlecase_part(&part));
                out.push(c);
                part.clear();
            } else {
                part.push(c);
            }
        }
        out.push_str(&titlecase_part(&part));
        out
    };

    name.split_whitespace()
        .enumerate()
        .map(|(i, word)| {
            let lower = word.to_lowercase();
            if i != 0 && PARTICLES.contains(&lower.as_str()) {
                lower
            } else {
                titlecase_token(word)
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
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
        assert_eq!(m.get(&"x".to_string()), Some(0));
    }

    #[test]
    fn idmapper_get_missing_key_returns_none() {
        let m: IdMapper<String, usize> = IdMapper::new();
        assert_eq!(m.get(&"missing".to_string()), None);
    }

    #[test]
    fn idmapper_get_does_not_require_mut() {
        let mut m: IdMapper<String, usize> = IdMapper::new();
        m.get_or_insert("a".to_string());
        let r: &IdMapper<String, usize> = &m;
        assert_eq!(r.get(&"a".to_string()), Some(0));
    }

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

    #[test]
    fn harmonize_caps_with_particle() {
        assert_eq!(harmonize_display_name("GARE DU NORD"), "Gare du Nord");
    }

    #[test]
    fn harmonize_single_word() {
        assert_eq!(harmonize_display_name("MERODE"), "Merode");
    }

    #[test]
    fn harmonize_abbreviation_with_period() {
        assert_eq!(harmonize_display_name("PRINC. ELISABETH"), "Princ. Elisabeth");
    }

    #[test]
    fn harmonize_hyphenated_part() {
        assert_eq!(harmonize_display_name("EGLISE ST-JULIEN"), "Eglise St-Julien");
    }

    #[test]
    fn harmonize_first_word_particle_is_capitalized() {
        assert_eq!(harmonize_display_name("DE BROUCKERE"), "De Brouckere");
    }

    #[test]
    fn harmonize_leaves_mixed_case_unchanged() {
        assert_eq!(harmonize_display_name("Bruxelles-Nord"), "Bruxelles-Nord");
        assert_eq!(harmonize_display_name("Morkhoven Station"), "Morkhoven Station");
    }

    #[test]
    fn harmonize_is_idempotent() {
        let once = harmonize_display_name("GARE DU NORD");
        assert_eq!(harmonize_display_name(&once), once);
    }

    #[test]
    fn harmonize_apostrophe_part() {
        assert_eq!(harmonize_display_name("PLACE D'ARMES"), "Place D'Armes");
    }

    #[test]
    fn harmonize_keeps_roman_numerals_uppercase() {
        assert_eq!(harmonize_display_name("ALBERT II"), "Albert II");
        assert_eq!(harmonize_display_name("LEOPOLD III"), "Leopold III");
        assert_eq!(harmonize_display_name("ALPHONSE XIII"), "Alphonse XIII");
    }

    #[test]
    fn harmonize_single_i_is_titlecased() {
        assert_eq!(harmonize_display_name("ALBERT I"), "Albert I");
    }

    #[test]
    fn harmonize_non_numeral_caps_word_unaffected() {
        assert_eq!(harmonize_display_name("GARE DU NORD"), "Gare du Nord");
        assert_eq!(harmonize_display_name("MERODE"), "Merode");
    }

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
