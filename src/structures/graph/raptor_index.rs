use std::collections::HashMap;

use gtfs_structures::RouteType;
use kdtree::KdTree;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteInfo, ServicePattern, StopTime, TripId, TripInfo, TripSegment,
    },
    structures::{
        DelayCDF, LatLng, NodeID,
        raptor::{Lookup, PatternID, PatternInfo},
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct RaptorIndex {
    pub transit_departures: Vec<TripSegment>,
    pub transit_services: Vec<ServicePattern>,
    pub transit_trips: Vec<TripInfo>,
    pub transit_routes: Vec<RouteInfo>,
    pub transit_agencies: Vec<AgencyInfo>,
    pub transit_patterns: Vec<PatternInfo>,

    pub transit_pattern_stops: Vec<NodeID>,
    pub transit_stop_patterns: Vec<(PatternID, u32)>,
    pub transit_stop_transfers: Vec<(NodeID, u32)>,
    pub transit_pattern_stop_times: Vec<StopTime>,
    pub transit_pattern_trips: Vec<TripId>,

    pub transit_idx_pattern_stops: Vec<Lookup>,
    pub transit_idx_stop_patterns: Vec<Lookup>,
    pub transit_idx_stop_transfers: Vec<Lookup>,
    pub transit_idx_pattern_stop_times: Vec<Lookup>,
    pub transit_idx_pattern_trips: Vec<Lookup>,

    pub transit_delay_models: HashMap<RouteType, DelayCDF>,

    pub transit_node_to_stop: Vec<u32>,
    pub transit_stop_to_node: Vec<NodeID>,
    pub transit_stops_tree: KdTree<f64, usize, [f64; 2]>,

    #[serde(default)]
    pub transit_stop_reverse_transfers: Vec<(usize, u32)>,
    #[serde(default)]
    pub transit_idx_stop_reverse_transfers: Vec<Lookup>,

    #[serde(default)]
    pub transit_pattern_shapes: Vec<Vec<LatLng>>,
    #[serde(default)]
    pub transit_pattern_shape_stop_idx: Vec<Vec<u32>>,

    #[serde(default)]
    pub railway_nodes: Vec<(f64, f64)>,
    #[serde(default)]
    pub railway_adj: Vec<Vec<(usize, u32)>>,

    #[serde(default = "RaptorIndex::default_min_access_secs")]
    pub min_access_secs: u32,

    #[serde(default = "RaptorIndex::default_walking_speed_mps")]
    pub walking_speed_mps: f64,
}

impl Default for RaptorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl RaptorIndex {
    pub fn new() -> Self {
        RaptorIndex {
            transit_departures: Vec::new(),
            transit_services: Vec::new(),
            transit_trips: Vec::new(),
            transit_routes: Vec::new(),
            transit_agencies: Vec::new(),
            transit_patterns: Vec::new(),

            transit_pattern_stops: Vec::new(),
            transit_stop_patterns: Vec::new(),
            transit_stop_transfers: Vec::new(),
            transit_pattern_stop_times: Vec::new(),
            transit_pattern_trips: Vec::new(),

            transit_idx_pattern_stops: Vec::new(),
            transit_idx_stop_patterns: Vec::new(),
            transit_idx_stop_transfers: Vec::new(),
            transit_idx_pattern_stop_times: Vec::new(),
            transit_idx_pattern_trips: Vec::new(),

            transit_delay_models: HashMap::new(),

            transit_node_to_stop: Vec::new(),
            transit_stop_to_node: Vec::new(),
            transit_stops_tree: KdTree::new(2),

            transit_stop_reverse_transfers: Vec::new(),
            transit_idx_stop_reverse_transfers: Vec::new(),

            transit_pattern_shapes: Vec::new(),
            transit_pattern_shape_stop_idx: Vec::new(),

            railway_nodes: Vec::new(),
            railway_adj: Vec::new(),

            min_access_secs: Self::default_min_access_secs(),
            walking_speed_mps: Self::default_walking_speed_mps(),
        }
    }

    pub fn default_min_access_secs() -> u32 {
        10 * 60
    }

    pub fn default_walking_speed_mps() -> f64 {
        1.2
    }

    /// Cross-reference check run after deserialization.  Returns an error if any index is
    /// out-of-bounds, which indicates a stale or corrupt `graph.bin`.
    pub fn validate(&self) -> Result<(), String> {
        let n_services = self.transit_services.len();
        let n_routes   = self.transit_routes.len();
        let n_trips    = self.transit_trips.len();
        let n_patterns = self.transit_patterns.len();

        for (i, trip) in self.transit_trips.iter().enumerate() {
            if trip.service_id.0 as usize >= n_services {
                return Err(format!(
                    "transit_trips[{i}].service_id={} out of bounds (transit_services.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip.service_id.0, n_services
                ));
            }
            if trip.route_id.0 as usize >= n_routes {
                return Err(format!(
                    "transit_trips[{i}].route_id={} out of bounds (transit_routes.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip.route_id.0, n_routes
                ));
            }
        }

        for (i, &trip_id) in self.transit_pattern_trips.iter().enumerate() {
            if trip_id.0 as usize >= n_trips {
                return Err(format!(
                    "transit_pattern_trips[{i}]={} out of bounds (transit_trips.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip_id.0, n_trips
                ));
            }
        }

        for (i, &(pat_id, _)) in self.transit_stop_patterns.iter().enumerate() {
            if pat_id.0 as usize >= n_patterns {
                return Err(format!(
                    "transit_stop_patterns[{i}].pattern_id={} out of bounds (transit_patterns.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    pat_id.0, n_patterns
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::gtfs::{RouteId, ServiceId, TripInfo};
    use crate::structures::raptor::PatternID;

    fn make_trip(route_id: u32, service_id: u32) -> TripInfo {
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(route_id),
            service_id: ServiceId(service_id),
            bikes_allowed: None,
        }
    }

    #[test]
    fn validate_empty_index_ok() {
        assert!(RaptorIndex::new().validate().is_ok());
    }

    #[test]
    fn validate_consistent_trips_ok() {
        let mut idx = RaptorIndex::new();
        idx.transit_services.push(crate::ingestion::gtfs::ServicePattern {
            days_of_week: 0x1f,
            start_date: 0,
            end_date: u32::MAX,
            added_dates: vec![],
            removed_dates: vec![],
        });
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Line 1".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_trips.push(make_trip(0, 0));
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn validate_bad_service_id_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Line 1".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_trips.push(make_trip(0, 9999));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("service_id"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_route_id_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_services.push(crate::ingestion::gtfs::ServicePattern {
            days_of_week: 0x1f,
            start_date: 0,
            end_date: u32::MAX,
            added_dates: vec![],
            removed_dates: vec![],
        });
        idx.transit_trips.push(make_trip(9999, 0));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("route_id"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_pattern_trip_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_pattern_trips.push(crate::ingestion::gtfs::TripId(9999));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("pattern_trips"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_stop_pattern_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_stop_patterns.push((PatternID(9999), 0));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("stop_patterns"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn raptor_index_new_is_empty() {
        let idx = RaptorIndex::new();
        assert!(idx.transit_departures.is_empty());
        assert!(idx.transit_services.is_empty());
        assert!(idx.transit_trips.is_empty());
        assert!(idx.transit_routes.is_empty());
        assert!(idx.transit_agencies.is_empty());
        assert!(idx.transit_patterns.is_empty());
        assert!(idx.transit_pattern_stops.is_empty());
        assert!(idx.transit_stop_patterns.is_empty());
        assert!(idx.transit_stop_transfers.is_empty());
        assert!(idx.transit_pattern_stop_times.is_empty());
        assert!(idx.transit_pattern_trips.is_empty());
        assert!(idx.transit_idx_pattern_stops.is_empty());
        assert!(idx.transit_idx_stop_patterns.is_empty());
        assert!(idx.transit_idx_stop_transfers.is_empty());
        assert!(idx.transit_idx_pattern_stop_times.is_empty());
        assert!(idx.transit_idx_pattern_trips.is_empty());
        assert!(idx.transit_delay_models.is_empty());
        assert!(idx.transit_node_to_stop.is_empty());
        assert!(idx.transit_stop_to_node.is_empty());
        assert!(idx.transit_stop_reverse_transfers.is_empty());
        assert!(idx.transit_idx_stop_reverse_transfers.is_empty());
        assert!(idx.transit_pattern_shapes.is_empty());
        assert!(idx.transit_pattern_shape_stop_idx.is_empty());
        assert!(idx.railway_nodes.is_empty());
        assert!(idx.railway_adj.is_empty());
        assert_eq!(idx.min_access_secs, 600);
        assert_eq!(idx.walking_speed_mps, 1.2);
    }
}
