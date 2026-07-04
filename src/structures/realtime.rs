//! Live realtime delays applied to RAPTOR routing.
//!
//! A [`RealtimeIndex`] maps `(trip, stop_sequence)` to a signed delay in seconds
//! (positive = late). It is produced by the realtime poller from one or more
//! feeds and hot-swapped behind an `ArcSwap`, independently of the graph.
//!
//! The router consults [`RealtimeIndex::delay`] as an *additive* offset: an
//! empty index yields 0 everywhere, exactly reproducing schedule-only behavior.

use std::collections::{HashMap, HashSet};

use crate::ingestion::gtfs::TripId;
use crate::ingestion::realtime::ServiceAlert;

/// Resolved vehicle position for one trip in a realtime snapshot.
///
/// `lat`/`lng` are WGS84 degrees; `bearing` is optional degrees clockwise from
/// north; `timestamp` is the unix epoch second of the observation (feed-level for
/// STIB, per-vehicle for GTFS-RT). Derives `Copy` — no heap allocation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VehiclePos {
    pub lat: f32,
    pub lng: f32,
    pub bearing: Option<f32>,
    pub current_stop_sequence: Option<u32>,
    pub timestamp: Option<u64>,
}

/// Live status of a transit trip (at a given stop) according to realtime data.
/// `NoData` is the inert default: an empty index reports it everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TripStatus {
    /// No realtime information is known for this trip/stop.
    NoData,
    /// Running and reported exactly on schedule (delay 0) at this stop.
    OnTime,
    /// Running but reported `secs` off schedule (positive = late).
    Delayed(i32),
    /// Reported CANCELED — the trip exists in the schedule but will not run.
    Canceled,
}

#[derive(Debug, Clone, Default)]
pub struct RealtimeIndex {
    /// Delay in seconds per `(trip, compact_stop_index)`; positive = late.
    /// The stop key is the RAPTOR compact stop index (what `scan_route` uses),
    /// not the GTFS `stop_sequence`.
    delays: HashMap<(TripId, u32), i32>,
    /// Trips reported CANCELED for this snapshot (whole-trip, stop-independent).
    canceled: HashSet<TripId>,
    /// Latest resolved vehicle position per trip, keyed by internal `TripId`.
    /// `is_empty()` / `len()` / `canceled_len()` count delays and cancellations
    /// only, keeping the inert-default invariant: an index with positions but no
    /// delays/cancellations is still considered "empty" for routing purposes.
    positions: HashMap<TripId, VehiclePos>,
    /// Service alerts from all realtime feeds, kept as-is (not indexed by
    /// trip/route/stop) for small-to-medium alert counts. Callers filter via
    /// [`alerts_for_leg`]. Not counted in `is_empty` — alerts alone do not
    /// change routing behaviour.
    alerts: Vec<ServiceAlert>,
    /// Actual compact stop per `(trip, parent_station)` when the RT feed reported
    /// a platform-level stop_id. Key uses the stop_id prefix before the last `_`
    /// as the parent station identity. Populated only when the actual stop_id
    /// contains `_` (is a platform-level stop, e.g. `gs:nmbssncb:8814001_8`).
    /// In `live_refresh` this is compared against the scheduled compact stop to
    /// detect a platform reassignment.
    platform_swaps: HashMap<(TripId, String), u32>,
    /// `(trip, compact_stop)` pairs the feed marked SKIPPED (a stop the trip no
    /// longer serves this run — a partial cancellation). Routing must not board
    /// or alight here, exactly as it must not at a CANCELED trip. Empty on the
    /// inert default, so with no feed every `is_skipped` query is `false` and the
    /// scan collapses to schedule-only behaviour. Not serialized (mirrors
    /// `positions`/`platform_swaps`).
    skipped: HashSet<(TripId, u32)>,
    /// Unix seconds when this snapshot was produced (0 for the empty index).
    pub generated_at: i64,
    /// Staleness TTL (seconds) stamped by the poller from config
    /// (`realtime.index_max_age_secs`). The routing consumer boundary ignores this
    /// snapshot once `now - generated_at > max_age_secs`, so a feed outage cannot
    /// serve hours-old delays/cancellations forever. `0` on the inert empty index
    /// (which is short-circuited before the staleness check ever reads it).
    max_age_secs: i64,
    /// Sticky last-known delays for TRACKED journeys, keyed `(trip, compact_stop)`
    /// with value `(delay_secs, last_seen_unix)`. Populated by the poller from a
    /// persistent cross-cycle cache so a boarded trip's observed delay survives
    /// after the live feed drops it from the upcoming-departures window (STIB
    /// waiting-times only predict future departures). Read ONLY through
    /// [`RealtimeIndex::delay_with_sticky`]/[`RealtimeIndex::status_with_sticky`]
    /// (the live-refresh overlay), NEVER by routing. Deliberately excluded from
    /// `is_empty`/`len` so a sticky-only index is invisible to planning:
    /// retention for tracking, never for routing. Not serialized (mirrors
    /// `positions`/`max_age_secs`).
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

    /// Build an index from per-stop delays plus the set of CANCELED trips.
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

    /// Build a full index from delays, cancellations, and vehicle positions.
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

    /// Build a full index from delays, cancellations, vehicle positions, and
    /// service alerts. Used by the poller when the feed carries all four.
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

    /// Build a complete index from all fields including platform swaps detected
    /// by the realtime poller. Used by [`build_index`] in the poller.
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

    /// Staleness TTL (seconds) for this snapshot, stamped by the poller from
    /// config. Consumed at the routing boundary: the snapshot is ignored once
    /// `now - generated_at > max_age_secs`. Returns 0 for the empty default.
    #[inline]
    pub fn max_age_secs(&self) -> i64 {
        self.max_age_secs
    }

    /// Builder: stamp this snapshot's staleness TTL (seconds). Used by the poller
    /// to attach `realtime.index_max_age_secs` to every published index.
    pub fn with_max_age_secs(mut self, secs: i64) -> Self {
        self.max_age_secs = secs;
        self
    }

    /// Builder: attach the set of SKIPPED `(trip, compact_stop)` pairs to this
    /// snapshot. Used by [`build_index`] in the poller after resolving the feed's
    /// `skipped_stops` to internal ids. An empty set leaves the index inert.
    pub fn with_skipped(mut self, skipped: HashSet<(TripId, u32)>) -> Self {
        self.skipped = skipped;
        self
    }

    /// True if the feed marked `trip` as SKIPPING compact `stop` this run (the
    /// trip does not serve that stop). The inert default reports `false`
    /// everywhere, so with no feed routing never sees a skip.
    #[inline]
    pub fn is_skipped(&self, trip: TripId, stop: u32) -> bool {
        self.skipped.contains(&(trip, stop))
    }

    /// Number of skipped `(trip, stop)` pairs in this snapshot. Informational.
    pub fn skipped_len(&self) -> usize {
        self.skipped.len()
    }

    /// Builder: attach the poller's sticky last-known-delay cache to this snapshot.
    /// The map is keyed `(trip, compact_stop)` with value `(delay_secs,
    /// last_seen_unix)`. Visible ONLY to [`delay_with_sticky`]/[`status_with_sticky`]
    /// — never counted by `is_empty`/`len`, never read by routing.
    pub fn with_sticky_delays(mut self, sticky: HashMap<(TripId, u32), (i32, i64)>) -> Self {
        self.sticky_delays = sticky;
        self
    }

    /// Iterate the live per-stop delays as `((trip, compact_stop), delay_secs)`.
    /// Used by the poller to fold each cycle's live delays into its persistent
    /// sticky cache.
    pub fn iter_delays(&self) -> impl Iterator<Item = ((TripId, u32), i32)> + '_ {
        self.delays.iter().map(|(&k, &v)| (k, v))
    }

    /// Number of sticky (retained) delays. Informational only — never affects
    /// `is_empty`/`len`.
    pub fn sticky_len(&self) -> usize {
        self.sticky_delays.len()
    }

    /// Delay (seconds, positive = late) for a trip at a compact stop, preferring
    /// the LIVE value and falling back to the sticky last-known delay when the
    /// feed no longer reports this `(trip, stop)`. A live entry always wins even
    /// when it is 0 (vehicle now reported on-time overrides a stale sticky value).
    /// Returns 0 when neither is known. Read by the live-refresh overlay only.
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

    /// Realtime status of `trip` at compact `stop`, preferring LIVE data and
    /// falling back to the sticky last-known delay. Cancellation outranks any
    /// delay; a live per-stop entry outranks sticky (even live 0 → `OnTime`);
    /// otherwise the sticky delay yields `OnTime`/`Delayed`; else `NoData`.
    /// Read by the live-refresh overlay only.
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

    /// Latest resolved vehicle position for a trip, if known.
    pub fn vehicle(&self, trip: TripId) -> Option<&VehiclePos> {
        self.positions.get(&trip)
    }

    /// Delay (seconds, positive = late) for a trip at a compact stop index.
    /// Returns 0 when no realtime information is known — the inert default.
    #[inline]
    pub fn delay(&self, trip: TripId, stop: u32) -> i32 {
        self.delays.get(&(trip, stop)).copied().unwrap_or(0)
    }

    /// Like [`delay`], but `None` when no realtime info exists for `(trip, stop)`
    /// — lets callers distinguish "known on time (0)" from "no data".
    #[inline]
    pub fn delay_opt(&self, trip: TripId, stop: u32) -> Option<i32> {
        self.delays.get(&(trip, stop)).copied()
    }

    /// True if `trip` is reported CANCELED in this snapshot. The inert default
    /// (empty index) reports `false` for every trip.
    #[inline]
    pub fn is_canceled(&self, trip: TripId) -> bool {
        self.canceled.contains(&trip)
    }

    /// Realtime status of `trip` at compact `stop`. Cancellation outranks any
    /// per-stop delay; otherwise the verdict comes from [`delay_opt`]. An empty
    /// index returns [`TripStatus::NoData`] everywhere.
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

    /// Number of known per-stop delays. Used to flag a leg as realtime-backed and
    /// for poller logging; cancellations are counted separately.
    pub fn len(&self) -> usize {
        self.delays.len()
    }

    /// Number of trips reported CANCELED in this snapshot.
    pub fn canceled_len(&self) -> usize {
        self.canceled.len()
    }

    /// Number of resolved vehicle positions in this snapshot. Informational only —
    /// positions do not affect `is_empty` (they do not contribute to the inert-default
    /// invariant; an index with positions but no delays/cancellations is still empty
    /// for routing purposes).
    pub fn positions_len(&self) -> usize {
        self.positions.len()
    }

    /// Number of service alerts in this snapshot. Informational only.
    pub fn alerts_len(&self) -> usize {
        self.alerts.len()
    }

    /// Actual compact stop index for `(trip, parent_station)` when the RT feed
    /// signalled a platform-level stop. `parent_station` is the stop_id prefix
    /// before the last `_` (e.g. `"gs:nmbssncb:8814001"` for a platform stop).
    /// Returns `None` when no RT platform information is known for this pair.
    #[inline]
    pub fn platform_swap(&self, trip: TripId, parent_station: &str) -> Option<u32> {
        self.platform_swaps.get(&(trip, parent_station.to_string())).copied()
    }

    /// Returns service alerts relevant to a transit leg, filtered to those
    /// currently active at `now_unix_secs`.
    ///
    /// An alert matches the leg if at least one `informed_entity` satisfies:
    /// - `trip_id` equals `trip_id_str`, OR
    /// - `stop_id` equals `board_stop_id` or `alight_stop_id`, OR
    /// - `route_id` equals `route_id_str` (when both are `Some`).
    ///
    /// `route_id_str` is the raw GTFS route_id string for the leg's trip. Pass
    /// `None` when the trip→route mapping is unavailable (e.g. old graph without
    /// `transit_route_ids`); route-level alerts will then be silently skipped.
    ///
    /// An alert is active at `now` if its `active_period` list is empty (always
    /// active) or at least one period contains `now`: `start ≤ now < end` where
    /// a missing bound is treated as open (no start = always started; no end =
    /// never expires).
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
        // No-feed: empty index reports no skips and stays empty.
        let idx = RealtimeIndex::new();
        assert!(!idx.is_skipped(TripId(1), 3));
        assert_eq!(idx.skipped_len(), 0);
        assert!(idx.is_empty());

        // A skip-only index is non-empty (routing must consult it) and reports
        // exactly the pairs it was given.
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
        // No-feed byte-identical: empty index, sticky-blind and sticky-aware both 0.
        let idx = RealtimeIndex::new();
        assert_eq!(idx.delay(TripId(1), 0), 0);
        assert_eq!(idx.delay_with_sticky(TripId(1), 0), 0);
        assert_eq!(idx.status_with_sticky(TripId(1), 0), TripStatus::NoData);
        assert!(idx.is_empty());
    }

    #[test]
    fn sticky_is_invisible_to_routing_accessors_and_is_empty() {
        // An index whose ONLY realtime content is sticky must look empty to routing:
        // is_empty()/len() ignore it, and the sticky-blind accessors return the inert
        // default — while the sticky-aware accessors surface the retained delay.
        let mut sticky = HashMap::new();
        sticky.insert((TripId(7), 3), (120, 1_000));
        let idx = RealtimeIndex::new().with_sticky_delays(sticky);

        // Routing-facing (sticky-blind) view: inert.
        assert!(idx.is_empty(), "sticky-only index must be empty for routing");
        assert_eq!(idx.len(), 0, "len counts live delays only");
        assert_eq!(idx.delay(TripId(7), 3), 0);
        assert_eq!(idx.delay_opt(TripId(7), 3), None);
        assert_eq!(idx.status(TripId(7), 3), TripStatus::NoData);

        // Live-refresh (sticky-aware) view: retained delay is visible.
        assert_eq!(idx.delay_with_sticky(TripId(7), 3), 120);
        assert_eq!(idx.status_with_sticky(TripId(7), 3), TripStatus::Delayed(120));
        assert_eq!(idx.sticky_len(), 1);
    }

    #[test]
    fn live_delay_overrides_sticky_even_when_live_is_zero() {
        // A fresh live Some(0) (vehicle now on-time) must win over a stale sticky 120.
        let mut sticky = HashMap::new();
        sticky.insert((TripId(1), 0), (120, 1_000));
        let idx = RealtimeIndex::from_delays(2_000, [((TripId(1), 0), 0)])
            .with_sticky_delays(sticky);
        assert_eq!(idx.delay_with_sticky(TripId(1), 0), 0, "live 0 overrides sticky");
        assert_eq!(idx.status_with_sticky(TripId(1), 0), TripStatus::OnTime);
    }

    #[test]
    fn sticky_fallback_only_where_live_is_absent() {
        // Live delay for one stop; sticky retains another stop the feed dropped.
        let mut sticky = HashMap::new();
        sticky.insert((TripId(1), 5), (90, 1_000)); // dropped by feed → sticky only
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
        assert_eq!(idx.delay(TripId(3), 9), 0); // unknown stop on a known trip
        assert_eq!(idx.delay(TripId(4), 2), 0); // unknown trip
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
