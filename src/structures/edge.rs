use crate::{
    ingestion::gtfs::{RouteId, TimetableSegment},
    structures::NodeID,
};

#[derive(Clone, Debug)]
pub enum EdgeData {
    Street(StreetEdgeData),
    Transit(TransitEdgeData),
}

#[derive(Debug, Clone, Copy)]
pub struct StreetEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub partial: bool,
    pub length: usize,
    pub foot: bool,
    pub bike: bool,
    pub car: bool,
}

#[derive(Debug, Clone)]
pub struct TransitEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub route_id: RouteId,
    pub timetable_segment: TimetableSegment,
    pub length: usize,
}
