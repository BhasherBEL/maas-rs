
use async_graphql::{ComplexObject, Context, Result, SimpleObject};

use crate::{
    ingestion::gtfs::{RouteId, TripId},
    structures::{Graph, plan::PlanRoute},
};

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct PlanTrip {
    pub headsign: Option<String>,

    #[graphql(skip)]
    pub route_id: RouteId,
}

#[ComplexObject]
impl PlanTrip {
    pub async fn route(&self, ctx: &Context<'_>) -> Result<Option<PlanRoute>> {
        let graph = ctx.data::<crate::services::scheduler::SharedGraph>()?.load_full();

        Ok(PlanRoute::from_route_id(graph.as_ref(), Some(self.route_id)))
    }
}

impl PlanTrip {
    pub fn from_trip_id(g: &Graph, id: TripId) -> Option<PlanTrip> {
        let trip = g.get_trip(id)?;

        Some(PlanTrip {
            headsign: trip.trip_headsign.clone(),
            route_id: trip.route_id,
        })
    }
}
