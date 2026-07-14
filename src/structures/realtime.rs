//! Live realtime delays applied to RAPTOR routing as an additive offset; an empty
//! index yields 0 everywhere, reproducing schedule-only behavior.

use std::collections::{HashMap, HashSet};

use crate::ingestion::gtfs::TripId;
use crate::ingestion::realtime::ServiceAlert;

/// Resolved vehicle position for one trip. `timestamp` is the unix epoch second of
/// the observation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehiclePos {
    pub lat: f32,
    pub lng: f32,
    pub bearing: Option<f32>,
    pub current_stop_sequence: Option<u32>,
    pub timestamp: Option<u64>,
}

/// Live status of a transit trip at a stop. `NoData` is the inert default (empty
/// index reports it everywhere).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TripStatus {
    NoData,
    OnTime,
    /// `secs` off schedule (positive = late).
    Delayed(i32),
    Canceled,
}

#[derive(Debug, Clone, Default)]
pub struct RealtimeIndex {
    /// Delay (secs, positive = late) per `(trip, stop)`. Stop key is the RAPTOR
    /// compact stop index, NOT the GTFS `stop_sequence`.
    delays: HashMap<(TripId, u32), i32>,
    canceled: HashSet<TripId>,
    /// Excluded from `is_empty`/`len`: positions alone are "empty" for routing.
    positions: HashMap<TripId, VehiclePos>,
    /// Excluded from `is_empty`: alerts alone do not change routing.
    alerts: Vec<ServiceAlert>,
    /// Actual compact stop per `(trip, parent_station)` on a platform-level RT
    /// stop_id. `parent_station` is the stop_id prefix before the last `_`.
    platform_swaps: HashMap<(TripId, String), u32>,
    /// `(trip, compact_stop)` pairs the feed marked SKIPPED; routing must not board
    /// or alight here, as at a CANCELED trip. Empty on the inert default.
    skipped: HashSet<(TripId, u32)>,
    /// Unix seconds this snapshot was produced (0 for the empty index).
    pub generated_at: i64,
    /// Staleness TTL (secs): routing ignores this snapshot once
    /// `now - generated_at > max_age_secs`. `0` on the empty index.
    max_age_secs: i64,
    /// Sticky last-known delays, `(trip, stop) → (delay_secs, last_seen_unix)`. Read
    /// ONLY via `delay_with_sticky`/`status_with_sticky`, NEVER by routing; excluded
    /// from `is_empty`/`len` so a sticky-only index is invisible to planning.
    sticky_delays: HashMap<(TripId, u32), (i32, i64)>,
}

impl RealtimeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_delays(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            canceled: HashSet::new(),
            positions: HashMap::new(),
            alerts: Vec::new(),
            platform_swaps: HashMap::new(),
            generated_at,
            max_age_secs: 0,
            skipped: HashSet::new(),
            sticky_delays: HashMap::new(),
        }
    }

    pub fn from_updates(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
        canceled: impl IntoIterator<Item = TripId>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            canceled: canceled.into_iter().collect(),
            positions: HashMap::new(),
            alerts: Vec::new(),
            platform_swaps: HashMap::new(),
            generated_at,
            max_age_secs: 0,
            skipped: HashSet::new(),
            sticky_delays: HashMap::new(),
        }
    }

    pub fn with_positions(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
        canceled: impl IntoIterator<Item = TripId>,
        positions: impl IntoIterator<Item = (TripId, VehiclePos)>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            canceled: canceled.into_iter().collect(),
            positions: positions.into_iter().collect(),
            alerts: Vec::new(),
            platform_swaps: HashMap::new(),
            generated_at,
            max_age_secs: 0,
            skipped: HashSet::new(),
            sticky_delays: HashMap::new(),
        }
    }

    pub fn with_alerts(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
        canceled: impl IntoIterator<Item = TripId>,
        positions: impl IntoIterator<Item = (TripId, VehiclePos)>,
        alerts: impl IntoIterator<Item = ServiceAlert>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            canceled: canceled.into_iter().collect(),
            positions: positions.into_iter().collect(),
            alerts: alerts.into_iter().collect(),
            platform_swaps: HashMap::new(),
            generated_at,
            max_age_secs: 0,
            skipped: HashSet::new(),
            sticky_delays: HashMap::new(),
        }
    }

    pub fn with_all(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
        canceled: impl IntoIterator<Item = TripId>,
        positions: impl IntoIterator<Item = (TripId, VehiclePos)>,
        alerts: impl IntoIterator<Item = ServiceAlert>,
        platform_swaps: HashMap<(TripId, String), u32>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            canceled: canceled.into_iter().collect(),
            positions: positions.into_iter().collect(),
            alerts: alerts.into_iter().collect(),
            platform_swaps,
            generated_at,
            max_age_secs: 0,
            skipped: HashSet::new(),
            sticky_delays: HashMap::new(),
        }
    }

    #[inline]
    pub fn max_age_secs(&self) -> i64 {
        self.max_age_secs
    }

    pub fn with_max_age_secs(mut self, secs: i64) -> Self {
        self.max_age_secs = secs;
        self
    }

    pub fn with_skipped(mut self, skipped: HashSet<(TripId, u32)>) -> Self {
        self.skipped = skipped;
        self
    }

    #[inline]
    pub fn is_skipped(&self, trip: TripId, stop: u32) -> bool {
        self.skipped.contains(&(trip, stop))
    }

    pub fn skipped_len(&self) -> usize {
        self.skipped.len()
    }

    pub fn with_sticky_delays(mut self, sticky: HashMap<(TripId, u32), (i32, i64)>) -> Self {
        self.sticky_delays = sticky;
        self
    }

    pub fn iter_delays(&self) -> impl Iterator<Item = ((TripId, u32), i32)> + '_ {
        self.delays.iter().map(|(&k, &v)| (k, v))
    }

    pub fn sticky_len(&self) -> usize {
        self.sticky_delays.len()
    }

    /// Live value preferred, sticky fallback. A live entry wins even at 0.
    /// Live-refresh overlay only.
    #[inline]
    pub fn delay_with_sticky(&self, trip: TripId, stop: u32) -> i32 {
        match self.delay_opt(trip, stop) {
            Some(d) => d,
            None => self
                .sticky_delays
                .get(&(trip, stop))
                .map(|(d, _)| *d)
                .unwrap_or(0),
        }
    }

    /// Live preferred, sticky fallback. Cancellation outranks any delay; live
    /// outranks sticky (even live 0 → `OnTime`). Live-refresh overlay only.
    pub fn status_with_sticky(&self, trip: TripId, stop: u32) -> TripStatus {
        if self.is_canceled(trip) {
            return TripStatus::Canceled;
        }
        let d = match self.delay_opt(trip, stop) {
            Some(d) => Some(d),
            None => self.sticky_delays.get(&(trip, stop)).map(|(d, _)| *d),
        };
        match d {
            None => TripStatus::NoData,
            Some(0) => TripStatus::OnTime,
            Some(secs) => TripStatus::Delayed(secs),
        }
    }

    pub fn vehicle(&self, trip: TripId) -> Option<&VehiclePos> {
        self.positions.get(&trip)
    }

    /// Delay (secs, positive = late) at a compact stop index; 0 when unknown.
    #[inline]
    pub fn delay(&self, trip: TripId, stop: u32) -> i32 {
        self.delays.get(&(trip, stop)).copied().unwrap_or(0)
    }

    /// Like [`delay`], but `None` distinguishes "known on time (0)" from "no data".
    #[inline]
    pub fn delay_opt(&self, trip: TripId, stop: u32) -> Option<i32> {
        self.delays.get(&(trip, stop)).copied()
    }

    #[inline]
    pub fn is_canceled(&self, trip: TripId) -> bool {
        self.canceled.contains(&trip)
    }

    /// Realtime status at compact `stop`. Cancellation outranks any per-stop delay.
    pub fn status(&self, trip: TripId, stop: u32) -> TripStatus {
        if self.is_canceled(trip) {
            return TripStatus::Canceled;
        }
        match self.delay_opt(trip, stop) {
            None => TripStatus::NoData,
            Some(0) => TripStatus::OnTime,
            Some(secs) => TripStatus::Delayed(secs),
        }
    }

    /// Number of known per-stop delays (cancellations counted separately).
    pub fn len(&self) -> usize {
        self.delays.len()
    }

    pub fn canceled_len(&self) -> usize {
        self.canceled.len()
    }

    /// Informational only; positions do not affect `is_empty`.
    pub fn positions_len(&self) -> usize {
        self.positions.len()
    }

    /// Informational only.
    pub fn alerts_len(&self) -> usize {
        self.alerts.len()
    }

    /// Compact stop index for `(trip, parent_station)` on a platform-level RT stop.
    /// `parent_station` is the stop_id prefix before the last `_`.
    #[inline]
    pub fn platform_swap(&self, trip: TripId, parent_station: &str) -> Option<u32> {
        self.platform_swaps.get(&(trip, parent_station.to_string())).copied()
    }

    /// Service alerts matching a leg and active at `now_unix_secs`. Matches when an
    /// `informed_entity` has `trip_id == trip_id_str`, `stop_id ∈ {board, alight}`,
    /// or `route_id == route_id_str` (both `Some`). Active if `active_period` is empty
    /// or some period has `start ≤ now < end` (missing bound = open).
    pub fn alerts_for_leg<'a>(
        &'a self,
        trip_id_str: &str,
        board_stop_id: &str,
        alight_stop_id: &str,
        route_id_str: Option<&'a str>,
        now_unix_secs: u64,
    ) -> impl Iterator<Item = &'a ServiceAlert> {
        self.alerts.iter().filter(move |alert| {
            let active = if alert.active_period.is_empty() {
                true
            } else {
                alert.active_period.iter().any(|(start, end)| {
                    start.map_or(true, |s| now_unix_secs >= s)
                        && end.map_or(true, |e| now_unix_secs < e)
                })
            };
            if !active {
                return false;
            }
            alert.informed_entity.iter().any(|e| {
                e.trip_id.as_deref() == Some(trip_id_str)
                    || e.stop_id
                        .as_deref()
                        .map(|s| s == board_stop_id || s == alight_stop_id)
                        .unwrap_or(false)
                    || matches!(
                        (e.route_id.as_deref(), route_id_str),
                        (Some(a), Some(b)) if a == b
                    )
            })
        })
    }

    pub fn is_empty(&self) -> bool {
        self.delays.is_empty() && self.canceled.is_empty() && self.skipped.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::realtime::{AlertEntitySelector, ServiceAlert};

    fn make_alert(
        header: &str,
        trip_id: Option<&str>,
        stop_id: Option<&str>,
        active_period: Vec<(Option<u64>, Option<u64>)>,
    ) -> ServiceAlert {
        ServiceAlert {
            header: Some(header.to_string()),
            description: None,
            cause: Some(1),
            effect: Some(1),
            active_period,
            informed_entity: vec![AlertEntitySelector {
                trip_id: trip_id.map(|s| s.to_string()),
                route_id: None,
                stop_id: stop_id.map(|s| s.to_string()),
            }],
        }
    }

    #[test]
    fn skipped_is_inert_by_default_and_visible_when_set() {
        let idx = RealtimeIndex::new();
        assert!(!idx.is_skipped(TripId(1), 3));
        assert_eq!(idx.skipped_len(), 0);
        assert!(idx.is_empty());

        let mut skipped = HashSet::new();
        skipped.insert((TripId(1), 3));
        let idx = RealtimeIndex::new().with_skipped(skipped);
        assert!(idx.is_skipped(TripId(1), 3));
        assert!(!idx.is_skipped(TripId(1), 4), "other stop not skipped");
        assert!(!idx.is_skipped(TripId(2), 3), "other trip not skipped");
        assert_eq!(idx.skipped_len(), 1);
        assert!(!idx.is_empty(), "a skip alters routing, so the index is non-empty");
    }

    #[test]
    fn sticky_absent_delay_with_sticky_is_zero_matching_no_feed() {
        let idx = RealtimeIndex::new();
        assert_eq!(idx.delay(TripId(1), 0), 0);
        assert_eq!(idx.delay_with_sticky(TripId(1), 0), 0);
        assert_eq!(idx.status_with_sticky(TripId(1), 0), TripStatus::NoData);
        assert!(idx.is_empty());
    }

    #[test]
    fn sticky_is_invisible_to_routing_accessors_and_is_empty() {
        let mut sticky = HashMap::new();
        sticky.insert((TripId(7), 3), (120, 1_000));
        let idx = RealtimeIndex::new().with_sticky_delays(sticky);

        assert!(idx.is_empty(), "sticky-only index must be empty for routing");
        assert_eq!(idx.len(), 0, "len counts live delays only");
        assert_eq!(idx.delay(TripId(7), 3), 0);
        assert_eq!(idx.delay_opt(TripId(7), 3), None);
        assert_eq!(idx.status(TripId(7), 3), TripStatus::NoData);

        assert_eq!(idx.delay_with_sticky(TripId(7), 3), 120);
        assert_eq!(idx.status_with_sticky(TripId(7), 3), TripStatus::Delayed(120));
        assert_eq!(idx.sticky_len(), 1);
    }

    #[test]
    fn live_delay_overrides_sticky_even_when_live_is_zero() {
        let mut sticky = HashMap::new();
        sticky.insert((TripId(1), 0), (120, 1_000));
        let idx = RealtimeIndex::from_delays(2_000, [((TripId(1), 0), 0)])
            .with_sticky_delays(sticky);
        assert_eq!(idx.delay_with_sticky(TripId(1), 0), 0, "live 0 overrides sticky");
        assert_eq!(idx.status_with_sticky(TripId(1), 0), TripStatus::OnTime);
    }

    #[test]
    fn sticky_fallback_only_where_live_is_absent() {
        let mut sticky = HashMap::new();
        sticky.insert((TripId(1), 5), (90, 1_000));
        let idx = RealtimeIndex::from_delays(2_000, [((TripId(1), 0), 30)])
            .with_sticky_delays(sticky);
        assert_eq!(idx.delay_with_sticky(TripId(1), 0), 30, "live stop wins");
        assert_eq!(idx.delay_with_sticky(TripId(1), 5), 90, "sticky fills the gap");
        assert_eq!(idx.delay_with_sticky(TripId(1), 9), 0, "unknown → 0");
        assert!(!idx.is_empty(), "has a live delay");
        assert_eq!(idx.len(), 1, "len counts the single live delay, not sticky");
    }

    #[test]
    fn cancellation_outranks_sticky_delay() {
        let mut sticky = HashMap::new();
        sticky.insert((TripId(4), 2), (300, 1_000));
        let idx = RealtimeIndex::from_updates(1_000, [], [TripId(4)])
            .with_sticky_delays(sticky);
        assert_eq!(idx.status_with_sticky(TripId(4), 2), TripStatus::Canceled);
    }

    #[test]
    fn max_age_secs_defaults_zero_and_builder_stamps_it() {
        assert_eq!(RealtimeIndex::new().max_age_secs(), 0);
        let idx = RealtimeIndex::from_updates(100, [((TripId(1), 0), 30)], [])
            .with_max_age_secs(600);
        assert_eq!(idx.max_age_secs(), 600);
        assert_eq!(idx.generated_at, 100, "builder leaves generated_at intact");
        assert!(!idx.is_empty(), "builder leaves delays/cancels intact");
    }

    #[test]
    fn empty_index_returns_none_for_vehicle() {
        let idx = RealtimeIndex::new();
        assert!(idx.vehicle(TripId(0)).is_none());
        assert!(idx.vehicle(TripId(42)).is_none());
    }

    #[test]
    fn with_positions_stores_and_retrieves_vehicle_pos() {
        let pos = VehiclePos {
            lat: 50.845_f32,
            lng: 4.352_f32,
            bearing: Some(90.0),
            current_stop_sequence: Some(3),
            timestamp: Some(1_751_000_000),
        };
        let idx = RealtimeIndex::with_positions(
            1_751_000_000,
            [],
            [],
            [(TripId(7), pos)],
        );
        let got = idx.vehicle(TripId(7)).expect("position should be stored");
        assert_eq!(got.lat, 50.845_f32);
        assert_eq!(got.lng, 4.352_f32);
        assert_eq!(got.bearing, Some(90.0));
        assert_eq!(got.current_stop_sequence, Some(3));
        assert_eq!(got.timestamp, Some(1_751_000_000));
        assert!(idx.vehicle(TripId(0)).is_none(), "unknown trip → None");
    }

    #[test]
    fn positions_do_not_affect_delay_or_cancel_semantics() {
        let pos = VehiclePos {
            lat: 50.0_f32,
            lng: 4.0_f32,
            bearing: None,
            current_stop_sequence: None,
            timestamp: None,
        };
        let idx = RealtimeIndex::with_positions(
            1,
            [((TripId(1), 0), 60)],
            [TripId(2)],
            [(TripId(3), pos)],
        );
        assert_eq!(idx.delay(TripId(1), 0), 60);
        assert!(idx.is_canceled(TripId(2)));
        assert!(idx.vehicle(TripId(3)).is_some());
        assert!(!idx.is_empty(), "delays+canceled make the index non-empty");
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.canceled_len(), 1);
    }

    #[test]
    fn is_empty_ignores_positions_preserving_inert_default() {
        let pos = VehiclePos {
            lat: 50.0_f32,
            lng: 4.0_f32,
            bearing: None,
            current_stop_sequence: None,
            timestamp: None,
        };
        let idx = RealtimeIndex::with_positions(0, [], [], [(TripId(5), pos)]);
        assert!(
            idx.is_empty(),
            "positions alone do not make the index non-empty"
        );
        assert!(idx.vehicle(TripId(5)).is_some());
    }

    #[test]
    fn from_delays_has_no_positions() {
        let idx = RealtimeIndex::from_delays(1, [((TripId(3), 2), 60)]);
        assert!(idx.vehicle(TripId(3)).is_none());
        assert!(idx.vehicle(TripId(0)).is_none());
    }

    #[test]
    fn empty_index_has_zero_delay_everywhere() {
        let idx = RealtimeIndex::new();
        assert_eq!(idx.delay(TripId(0), 0), 0);
        assert_eq!(idx.delay(TripId(42), 7), 0);
        assert!(!idx.is_canceled(TripId(0)));
        assert_eq!(idx.status(TripId(0), 0), TripStatus::NoData);
        assert_eq!(idx.canceled_len(), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn index_reports_status_for_canceled_delayed_and_unknown_trips() {
        let idx = RealtimeIndex::from_updates(
            1_700_000_000,
            [((TripId(1), 4), 120), ((TripId(2), 0), 0)],
            [TripId(9)],
        );

        assert!(idx.is_canceled(TripId(9)));
        assert_eq!(idx.status(TripId(9), 0), TripStatus::Canceled);
        assert_eq!(idx.status(TripId(9), 7), TripStatus::Canceled);

        assert_eq!(idx.status(TripId(1), 4), TripStatus::Delayed(120));
        assert_eq!(idx.status(TripId(2), 0), TripStatus::OnTime);

        assert!(!idx.is_canceled(TripId(1)));
        assert_eq!(idx.status(TripId(1), 0), TripStatus::NoData);
        assert_eq!(idx.status(TripId(5), 5), TripStatus::NoData);

        assert_eq!(idx.canceled_len(), 1);
        assert_eq!(idx.len(), 2);
        assert!(!idx.is_empty());
    }

    #[test]
    fn cancellation_outranks_delay_for_same_trip() {
        let idx = RealtimeIndex::from_updates(
            1_700_000_000,
            [((TripId(7), 3), 300)],
            [TripId(7)],
        );

        assert_eq!(idx.delay(TripId(7), 3), 300);
        assert!(idx.is_canceled(TripId(7)));
        assert_eq!(idx.status(TripId(7), 3), TripStatus::Canceled);
        assert_eq!(idx.status(TripId(7), 0), TripStatus::Canceled);
    }

    #[test]
    fn from_delays_has_no_cancellations() {
        let idx = RealtimeIndex::from_delays(1, [((TripId(3), 2), 60)]);
        assert!(!idx.is_canceled(TripId(3)));
        assert_eq!(idx.status(TripId(3), 2), TripStatus::Delayed(60));
        assert_eq!(idx.canceled_len(), 0);
    }

    #[test]
    fn populated_index_returns_known_delays() {
        let idx = RealtimeIndex::from_delays(
            1_700_000_000,
            [((TripId(3), 2), 120), ((TripId(3), 3), -30)],
        );
        assert_eq!(idx.delay(TripId(3), 2), 120);
        assert_eq!(idx.delay(TripId(3), 3), -30);
        assert_eq!(idx.delay(TripId(3), 9), 0);
        assert_eq!(idx.delay(TripId(4), 2), 0);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.generated_at, 1_700_000_000);
    }

    #[test]
    fn with_alerts_stores_and_retrieves_alerts_by_trip() {
        let alert = make_alert("Disruption", Some("T42"), None, vec![]);
        let idx = RealtimeIndex::with_alerts(1_000, [], [], [], [alert.clone()]);
        assert_eq!(idx.alerts_len(), 1);
        let matches: Vec<_> = idx.alerts_for_leg("T42", "SA", "SB", None, 0).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].header.as_deref(), Some("Disruption"));
    }

    #[test]
    fn alerts_for_leg_matches_board_stop() {
        let alert = make_alert("Stop alert", None, Some("SA"), vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, 0).collect();
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn alerts_for_leg_matches_alight_stop() {
        let alert = make_alert("Alight stop alert", None, Some("SB"), vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, 0).collect();
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn alerts_for_leg_no_match_on_different_trip_and_stop() {
        let alert = make_alert("Other line alert", Some("T99"), None, vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, 0).collect();
        assert!(matches.is_empty());
    }

    #[test]
    fn alerts_for_leg_active_period_filters_expired_alert() {
        let now = 1_750_100_000u64;
        let expired_alert = make_alert(
            "Expired",
            Some("T0"),
            None,
            vec![(Some(1_749_000_000), Some(1_749_999_999))],
        );
        let active_alert = make_alert(
            "Active",
            Some("T0"),
            None,
            vec![(Some(1_750_000_000), Some(1_751_000_000))],
        );
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [expired_alert, active_alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, now).collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].header.as_deref(), Some("Active"));
    }

    #[test]
    fn alerts_for_leg_empty_active_period_always_active() {
        let alert = make_alert("Always active", Some("T0"), None, vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, 9_999_999_999).collect();
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn alerts_for_leg_open_ended_period_is_active() {
        let now = 1_750_000_000u64;
        let no_end = make_alert("No end", Some("T0"), None, vec![(Some(1_749_000_000), None)]);
        let no_start = make_alert(
            "No start",
            Some("T0"),
            None,
            vec![(None, Some(1_751_000_000))],
        );
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [no_end, no_start]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, now).collect();
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn alerts_do_not_affect_is_empty() {
        let alert = make_alert("Alert", Some("T0"), None, vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        assert!(idx.is_empty(), "alerts alone must not make the index non-empty");
        assert_eq!(idx.alerts_len(), 1);
    }

    #[test]
    fn with_positions_has_no_alerts() {
        let pos = VehiclePos {
            lat: 50.0,
            lng: 4.0,
            bearing: None,
            current_stop_sequence: None,
            timestamp: None,
        };
        let idx = RealtimeIndex::with_positions(0, [], [], [(TripId(0), pos)]);
        assert_eq!(idx.alerts_len(), 0);
    }

    fn make_route_alert(header: &str, route_id: &str) -> ServiceAlert {
        ServiceAlert {
            header: Some(header.to_string()),
            description: None,
            cause: None,
            effect: None,
            active_period: vec![],
            informed_entity: vec![crate::ingestion::realtime::AlertEntitySelector {
                trip_id: None,
                route_id: Some(route_id.to_string()),
                stop_id: None,
            }],
        }
    }

    #[test]
    fn alerts_for_leg_matches_route_id() {
        let alert = make_route_alert("Line alert", "R1");
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", Some("R1"), 0).collect();
        assert_eq!(matches.len(), 1, "route-level alert must surface for matching route");
        assert_eq!(matches[0].header.as_deref(), Some("Line alert"));
    }

    #[test]
    fn alerts_for_leg_no_match_route_id_mismatch() {
        let alert = make_route_alert("Line alert", "R1");
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", Some("R2"), 0).collect();
        assert!(matches.is_empty(), "alert for R1 must not surface for route R2");
    }

    #[test]
    fn alerts_for_leg_no_match_when_route_id_str_is_none() {
        let alert = make_route_alert("Line alert", "R1");
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", None, 0).collect();
        assert!(
            matches.is_empty(),
            "route alert must not match when leg route_id is unavailable (None)"
        );
    }

    #[test]
    fn alerts_for_leg_route_and_trip_alerts_both_surface() {
        let trip_alert = make_alert("Trip alert", Some("T0"), None, vec![]);
        let route_alert = make_route_alert("Route alert", "R1");
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [trip_alert, route_alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", Some("R1"), 0).collect();
        assert_eq!(matches.len(), 2, "both trip-level and route-level alerts must surface");
    }

    #[test]
    fn alerts_for_leg_route_match_does_not_affect_trip_only_match() {
        let trip_alert = make_alert("Trip alert", Some("T0"), None, vec![]);
        let idx = RealtimeIndex::with_alerts(0, [], [], [], [trip_alert]);
        let matches: Vec<_> = idx.alerts_for_leg("T0", "SA", "SB", Some("R99"), 0).collect();
        assert_eq!(matches.len(), 1, "trip match must still work when route does not match");
    }
}
