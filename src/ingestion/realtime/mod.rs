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

/// A pollable realtime data source. Implementations fetch and parse their feed,
/// returning the delays they observed. Network/parse errors are returned, not
/// panicked — the poller isolates failures per feed.
pub trait RealtimeFeed: Send + Sync {
    fn name(&self) -> &str;
    fn poll(&self, fetcher: &fetcher::Fetcher) -> Result<Vec<TripDelay>, String>;
}
