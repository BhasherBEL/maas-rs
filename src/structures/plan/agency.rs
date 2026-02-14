use async_graphql::SimpleObject;

use crate::{ingestion::gtfs::AgencyId, structures::Graph};

#[derive(Debug, SimpleObject)]
pub struct PlanAgency {
    pub name: String,
    pub url: String,
    pub timezone: String,
}

impl PlanAgency {
    pub fn from_agency_id(g: &Graph, id: Option<AgencyId>) -> Option<PlanAgency> {
        let agency = g.get_agency(id?)?;

        Some(PlanAgency {
            name: agency.name.clone(),
            url: agency.url.clone(),
            timezone: agency.timezone.clone(),
        })
    }
}
