pub mod fetcher;
pub mod gtfs_rt;
pub mod proto;
pub mod stib;

/// `delay` is seconds off schedule (positive = late). `trip_id`/`stop_id` are raw
/// GTFS string ids the poller resolves against the graph's reverse maps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TripDelay {
    pub trip_id: String,
    pub stop_id: Option<String>,
    pub stop_sequence: Option<u32>,
    pub delay: i32,
}

/// `trip_id` is a raw GTFS string; `lat`/`lng` are WGS84.
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

/// Which routes/trips/stops an alert applies to; raw GTFS string ids.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertEntitySelector {
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub stop_id: Option<String>,
}

/// `active_period`: Unix-second `(start, end)` ranges; empty list = always active,
/// `None` bound = open-ended. `cause`/`effect` are raw protobuf enum values.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceAlert {
    pub header: Option<String>,
    pub description: Option<String>,
    pub cause: Option<i32>,
    pub effect: Option<i32>,
    pub active_period: Vec<(Option<u64>, Option<u64>)>,
    pub informed_entity: Vec<AlertEntitySelector>,
}

/// RT `stop_id` for a trip stop, captured regardless of delay; used to detect
/// platform reassignments against the scheduled stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActualStopId {
    pub trip_id: String,
    pub stop_id: String,
}

/// One poll cycle's observations. `canceled` holds GTFS `trip_id`s that will not
/// run; `skipped_stops` holds `(trip_id, stop_id)` pairs the feed marked SKIPPED.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeedUpdate {
    pub delays: Vec<TripDelay>,
    pub canceled: Vec<String>,
    pub positions: Vec<VehicleObservation>,
    pub alerts: Vec<ServiceAlert>,
    pub actual_stops: Vec<ActualStopId>,
    pub skipped_stops: Vec<(String, String)>,
}

pub trait RealtimeFeed: Send + Sync {
    fn name(&self) -> &str;
    fn poll(&self, fetcher: &fetcher::Fetcher) -> Result<FeedUpdate, fetcher::FetchError>;
}
