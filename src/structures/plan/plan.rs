use async_graphql::SimpleObject;

use crate::structures::plan::PlanLeg;

#[derive(Debug, SimpleObject)]
pub struct Plan {
    pub legs: Vec<PlanLeg>,
    pub start: u32,
    pub end: u32,
}
