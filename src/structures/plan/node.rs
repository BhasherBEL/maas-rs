use async_graphql::{Enum, SimpleObject};

use crate::structures::{Graph, NodeID};

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
    name: Option<String>,
}

impl PlanNode {
    pub fn from_node_id(g: &Graph, id: NodeID) -> Option<PlanNode> {
        let (loc, name) = g.plan_node_info(id)?;
        let mode = if name.is_some() {
            PlanNodeType::TransitStop
        } else {
            PlanNodeType::Osm
        };
        Some(PlanNode {
            lat: loc.latitude,
            lon: loc.longitude,
            mode,
            name,
        })
    }
}
