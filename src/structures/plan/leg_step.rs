use async_graphql::SimpleObject;

use crate::structures::plan::PlanPlace;

#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanLegStep {
    pub length: usize,
    pub time: u32,
    pub place: PlanPlace,
}
