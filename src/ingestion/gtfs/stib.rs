use gtfs_structures::RouteType;

use crate::structures::Graph;

pub fn load_gtfs_stib(path: &str, g: &mut Graph) -> Result<(), gtfs_structures::Error> {
    tracing::info!("applying STIB bike-allowance rules");
    super::load_gtfs_with_hook(path, g, super::GtfsProvider::Stib, bikes_allowed_stib)
}

fn bikes_allowed_stib(trip: &gtfs_structures::Trip, route_type: RouteType) -> Option<bool> {
    // Priority 1: explicit GTFS data
    match trip.bikes_allowed {
        gtfs_structures::BikesAllowedType::AtLeastOneBike => return Some(true),
        gtfs_structures::BikesAllowedType::NoBikesAllowed => return Some(false),
        _ => {}
    }
    // Priority 2: STIB fallback rules
    match route_type {
        RouteType::Subway | RouteType::Tramway => {
            let dep = trip
                .stop_times
                .iter()
                .filter_map(|st| st.departure_time)
                .min()?;
            Some(!is_stib_peak_hour(dep))
        }
        RouteType::Bus | RouteType::Coach => Some(false),
        _ => None,
    }
}

/// Returns `true` if `seconds_since_midnight` falls in a STIB peak window
/// where bikes are not allowed on trams/metro:
///   07:00–09:00  (25 200 – 32 400 s)
///   16:00–18:30  (57 600 – 66 600 s)
fn is_stib_peak_hour(secs: u32) -> bool {
    const H7: u32 = 7 * 3600;
    const H9: u32 = 9 * 3600;
    const H16: u32 = 16 * 3600;
    const H1830: u32 = 16 * 3600 + 2 * 3600 + 30 * 60; // 66 600
    (H7..H9).contains(&secs) || (H16..H1830).contains(&secs)
}

// ── Time-window fare model (STIB / De Lijn operator-specific interpretation) ────

/// Build a `time_window_flat` `OperatorModel` (STIB or De Lijn) from its config
/// block. Both STIB and De Lijn are flat-ticket, transfer-window operators; this
/// selects the independent ticket-window state (`TimeWindowOperator`, absent ⇒
/// STIB) and carries the single-ticket + optional N-journey card price. It only
/// composes the operator-agnostic `TimeWindowFlat` PRIMITIVE that lives in
/// `structures::cost::fares`; the STIB/De Lijn choice of window operator and card
/// price is co-located here with these operators' ingestor. `cents` converts an
/// optional euro amount to integer cents.
pub fn build_time_window_operator(
    op: &crate::structures::FareOperatorConfig,
    cents: impl Fn(Option<f64>) -> u32,
) -> crate::structures::cost::OperatorModel {
    use crate::structures::cost::{OperatorModel, TimeWindowOperator};
    let operator = match op.time_window_operator.as_deref() {
        Some(s) if s.eq_ignore_ascii_case("delijn") || s.eq_ignore_ascii_case("de lijn") => {
            TimeWindowOperator::Delijn
        }
        _ => TimeWindowOperator::Stib,
    };
    OperatorModel::TimeWindowFlat {
        ticket_cents: cents(op.ticket_euros),
        card_cents: op.card_euros.map(|e| cents(Some(e))),
        validity_secs: op.validity_secs.unwrap_or(0),
        operator,
    }
}

#[cfg(test)]
mod tests {
    use super::is_stib_peak_hour;

    // 07:00 = 25 200 s, 09:00 = 32 400 s
    // 16:00 = 57 600 s, 18:30 = 66 600 s

    #[test]
    fn before_morning_peak() {
        assert!(!is_stib_peak_hour(25_199));
    }

    #[test]
    fn at_morning_peak_start() {
        assert!(is_stib_peak_hour(25_200));
    }

    #[test]
    fn just_before_morning_peak_end() {
        assert!(is_stib_peak_hour(32_399));
    }

    #[test]
    fn at_morning_peak_end() {
        assert!(!is_stib_peak_hour(32_400));
    }

    #[test]
    fn before_evening_peak() {
        assert!(!is_stib_peak_hour(57_599));
    }

    #[test]
    fn at_evening_peak_start() {
        assert!(is_stib_peak_hour(57_600));
    }

    #[test]
    fn just_before_evening_peak_end() {
        assert!(is_stib_peak_hour(66_599));
    }

    #[test]
    fn at_evening_peak_end() {
        assert!(!is_stib_peak_hour(66_600));
    }

    #[test]
    fn midday_off_peak() {
        assert!(!is_stib_peak_hour(12 * 3600)); // noon
    }
}
