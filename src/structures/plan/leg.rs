use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Result, SimpleObject};

use crate::{
    ingestion::gtfs::TripId,
    structures::{
        Graph,
        plan::{PlanPlace, PlanTrip},
    },
};

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct PlanLeg {
    pub mode: String,
    pub length: usize,
    pub start: u32,
    pub end: u32,
    pub duration: u32,

    pub from: PlanPlace,
    pub to: PlanPlace,

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
