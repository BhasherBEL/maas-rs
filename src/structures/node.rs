use std::fmt::Display;

use gtfs_structures::Availability;

use crate::structures::LatLng;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NodeID(pub usize);

impl Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return self.0.fmt(f);
    }
}

#[derive(Debug, Clone)]
pub enum NodeData {
    OsmNode(OsmNodeData),
    TransitStop(TransitStopData),
}

impl NodeData {
    pub fn loc(&self) -> LatLng {
        match self {
            Self::OsmNode(node) => node.lat_lng,
            Self::TransitStop(node) => node.lat_lng,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OsmNodeData {
    pub eid: String,
    pub lat_lng: LatLng,
}

#[derive(Debug, Clone)]
pub struct TransitStopData {
    pub name: String,
    pub lat_lng: LatLng,
    pub accessibility: Availability,
}
