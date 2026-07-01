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

/// A pedestrian vertical connector that bridges OSM `level`s — the
/// infrastructure that makes a platform (`level=1`) reachable from a `level=0`
/// concourse. Classified from the OSM way tag (NOT derived from node levels: a
/// flat concourse footway can share a node with a platform yet is *not* a
/// connector). Stored in `Graph::connector_edges` (auxiliary OSM data, osm.bin
/// only). Used by the Stage B1 connector-coverage measurement; B1 does not yet
/// charge it in routing (see `ConnectorCost`).
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
    /// Bike-routing classification (BRouter-style). See `BikeAttrs`.
    pub attrs: crate::structures::BikeAttrs,
    /// Elevation change origin→destination in meters (signed). 0 when no DEM.
    pub elev_delta: i16,
    /// Per-edge bike cruise-speed multiplier baked from OSM `surface=*`, as
    /// `round(factor·100)` (100 = asphalt baseline 1.0, 60 = gravel 0.60). A
    /// SPEED factor only — distinct from the Surface comfort Pareto axis. `0`
    /// means "unset" (old cache / non-bike edge) and is read as the unknown
    /// default (90). Baked at ingest, so re-tuning the table needs a rebuild.
    pub surface_speed: u8,
    /// Variance-generating features (signals, elevators, uncontrolled crossings)
    /// classified at ingest from the segment's endpoint nodes. Consumed only
    /// post-hoc and by the deadline variance-proxy axis — never in the search.
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
