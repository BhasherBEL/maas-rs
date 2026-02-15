use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Enum, Result, SimpleObject};

use crate::{
    ingestion::gtfs::TripId,
    structures::{
        Graph,
        plan::{PlanLegStep, PlanPlace, PlanTrip},
    },
};

#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PlanLegType {
    WALK,
    TRANSIT,
    OTHER,
}

#[derive(Debug, SimpleObject, Clone)]
#[graphql(complex)]
pub struct PlanLeg {
    pub mode: PlanLegType,
    pub length: usize,
    pub start: u32,
    pub end: u32,
    pub duration: u32,

    pub from: PlanPlace,
    pub to: PlanPlace,

    pub steps: Vec<PlanLegStep>,

    #[graphql(skip)]
    pub trip_id: Option<TripId>,
}

#[ComplexObject]
impl PlanLeg {
    async fn trip(&self, ctx: &Context<'_>) -> Result<Option<PlanTrip>> {
        let graph = ctx.data::<Arc<Graph>>()?;

        Ok(PlanTrip::from_trip_id(graph, self.trip_id))
    }
}
