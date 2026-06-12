use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{RouteId, TimetableSegment},
    structures::NodeID,
};

#[derive(Clone, Debug, Copy, Serialize, Deserialize)]
pub enum EdgeData {
    Street(StreetEdgeData),
    Transit(TransitEdgeData),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StreetEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub partial: bool,
    pub length: usize,
    pub foot: bool,
    pub bike: bool,
    pub car: bool,
    /// Bike-routing classification (BRouter-style). See `BikeAttrs`.
    pub attrs: crate::structures::BikeAttrs,
    /// Elevation change origin→destination in meters (signed). 0 when no DEM.
    pub elev_delta: i16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TransitEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub route_id: RouteId,
    pub timetable_segment: TimetableSegment,
    pub length: usize,
}
