use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Result, SimpleObject};

use crate::structures::{Graph, NodeID, plan::PlanNode};

#[derive(Debug, SimpleObject, Clone, Copy)]
#[graphql(complex)]
pub struct PlanPlace {
    pub stop_position: Option<usize>,
    pub arrival: Option<u32>,
    pub departure: Option<u32>,

    #[graphql(skip)]
    pub node_id: NodeID,
}

#[ComplexObject]
impl PlanPlace {
    pub async fn node(&self, ctx: &Context<'_>) -> Result<Option<PlanNode>> {
        let graph = ctx.data::<Arc<Graph>>()?;

        Ok(PlanNode::from_node_id(graph, self.node_id))
    }
}
