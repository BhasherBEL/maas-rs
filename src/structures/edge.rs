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

/// Classified from the OSM way tag, NOT derived from node levels.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Connector {
    Steps,
    Elevator,
    Ramp,
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
    pub attrs: crate::structures::BikeAttrs,
    /// Signed origin→destination elevation change in meters. 0 when no DEM.
    pub elev_delta: i16,
    /// Bike cruise-speed multiplier as `round(factor·100)` (100 = asphalt). `0`
    /// means unset and is read as the default 90.
    pub surface_speed: u8,
    pub var_gen: crate::structures::cost::VarGen,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TransitEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub route_id: RouteId,
    pub timetable_segment: TimetableSegment,
    pub length: usize,
}
