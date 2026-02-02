use std::fmt::Display;

use crate::structures::LatLng;

#[derive(Debug, Clone)]
pub struct NodeData {
    pub eid: String,
    pub lat_lng: LatLng,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct NodeID(pub usize);

impl Display for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return self.0.fmt(f);
    }
}
