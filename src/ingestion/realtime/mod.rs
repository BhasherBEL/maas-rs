//! Realtime transit feeds: generic GTFS-Realtime (protobuf trip-updates) and a
//! custom STIB waiting-times parser. Each feed produces [`TripDelay`]s that the
//! poller folds into a `RealtimeIndex` applied to RAPTOR routing.

pub mod fetcher;
pub mod gtfs_rt;
pub mod proto;
pub mod stib;

/// One realtime delay observation: a trip is `delay` seconds off schedule at a
/// stop (positive = late). `trip_id` is the GTFS string id; the poller resolves
/// it to an internal `TripId`, and the stop via `stop_id` (preferred) — both
/// against the graph's reverse maps. `stop_sequence` is kept for diagnostics and
/// future stop-sequence-based resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TripDelay {
    pub trip_id: String,
    pub stop_id: Option<String>,
    pub stop_sequence: Option<u32>,
    pub delay: i32,
}

/// A resolved vehicle position observation from any feed: the feed-specific
/// encoding has already been normalised to a GTFS `trip_id` string and WGS84
/// coordinates before this point. The poller resolves `trip_id` → `TripId` and
/// folds into `RealtimeIndex.positions`.
#[derive(Debug, Clone, PartialEq)]
pub struct VehicleObservation {
    pub trip_id: String,
    pub lat: f32,
    pub lng: f32,
    pub bearing: Option<f32>,
    pub current_stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub timestamp: Option<u64>,
}

/// One informed entity within a service alert: identifies which routes, trips,
/// or stops the alert applies to. Fields use raw GTFS string ids from the feed.
/// Route matching via `route_id` requires a GTFS route_id→internal mapping that
/// does not yet exist; it is stored but not resolved in this increment (follow-up).
#[derive(Debug, Clone, PartialEq)]
pub struct AlertEntitySelector {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub stop_id: Option<String>,
}

/// A parsed GTFS-RT service alert. `active_period` is a list of Unix-second
/// `(start, end)` ranges; an empty list means always active. `None` on either
/// bound means open-ended (no start = started at minus-infinity; no end = never
/// expires). `cause`/`effect` are the raw protobuf `i32` enum values.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceAlert {
    pub header: Option<String>,
    pub description: Option<String>,
    pub cause: Option<i32>,
    pub effect: Option<i32>,
    pub active_period: Vec<(Option<u64>, Option<u64>)>,
    pub informed_entity: Vec<AlertEntitySelector>,
}

/// One actual stop location reported by a realtime feed for a given trip stop:
/// the RT `stop_id` at a stop position, captured regardless of whether a delay
/// is present. Used to detect platform reassignments when the RT `stop_id`
/// differs from the scheduled one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActualStopId {
    pub trip_id: String,
    pub stop_id: String,
}

/// One poll cycle's observations from a feed: per-stop [`TripDelay`]s plus the
/// GTFS string `trip_id`s the feed reports as CANCELED (the trip exists in the
/// schedule but will not run). Cancellations carry no stop, so they live
/// alongside the delays rather than as a degenerate `TripDelay`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeedUpdate {
    pub delays: Vec<TripDelay>,
    pub canceled: Vec<String>,
    pub positions: Vec<VehicleObservation>,
    /// Service alerts from GTFS-RT `Alert` entities. STIB-specific alert
    /// sources are out of scope for this increment.
    pub alerts: Vec<ServiceAlert>,
    /// Actual RT stop_id per trip stop, captured without delay gating so
    /// platform assignments are recorded even when a stop runs on time.
    pub actual_stops: Vec<ActualStopId>,
}

/// A pollable realtime data source. Implementations fetch and parse their feed,
/// returning the delays and cancellations they observed. Network/parse errors
/// are returned, not panicked — the poller isolates failures per feed.
pub trait RealtimeFeed: Send + Sync {
    fn name(&self) -> &str;
    fn poll(&self, fetcher: &fetcher::Fetcher) -> Result<FeedUpdate, String>;
}
