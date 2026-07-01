use async_graphql::{ComplexObject, Context, Result, SimpleObject};

use crate::structures::{NodeID, plan::PlanNode};

#[derive(Debug, SimpleObject, Clone, Copy)]
#[graphql(complex)]
pub struct PlanPlace {
    pub stop_position: Option<u32>,
    pub arrival: Option<u32>,
    pub departure: Option<u32>,

    #[graphql(skip)]
    pub node_id: NodeID,
}

#[ComplexObject]
impl PlanPlace {
    pub async fn node(&self, ctx: &Context<'_>) -> Result<Option<PlanNode>> {
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();

        Ok(PlanNode::from_node_id(graph.as_ref(), self.node_id))
    }

    pub async fn stop_id(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();

        Ok(graph.stop_id_of_node(self.node_id).map(str::to_string))
    }

    pub async fn platform(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();

        Ok(graph.platform_code_of_node(self.node_id).map(str::to_string))
    }
}
