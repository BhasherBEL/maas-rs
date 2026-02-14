use async_graphql::{Enum, SimpleObject};

use crate::structures::{Graph, NodeData, NodeID};

#[derive(Debug, Enum, Clone, Copy, PartialEq, PartialOrd, Ord, Eq)]
pub enum PlanNodeType {
    Osm,
    TransitStop,
}

#[derive(Debug, SimpleObject)]
pub struct PlanNode {
    lat: f64,
    lon: f64,
    mode: PlanNodeType,
}

impl PlanNode {
    pub fn from_node_id(g: &Graph, id: NodeID) -> Option<PlanNode> {
        let node = g.get_node(id)?;

        match node {
            NodeData::OsmNode(node) => Some(PlanNode {
                lat: node.lat_lng.latitude,
                lon: node.lat_lng.longitude,
                mode: PlanNodeType::Osm,
            }),
            NodeData::TransitStop(node) => Some(PlanNode {
                lat: node.lat_lng.latitude,
                lon: node.lat_lng.longitude,
                mode: PlanNodeType::TransitStop,
            }),
        }
    }
}
