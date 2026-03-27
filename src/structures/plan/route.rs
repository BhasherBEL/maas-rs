use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Enum, Result, SimpleObject};
use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{AgencyId, RouteId},
    structures::{Graph, plan::PlanAgency},
};

#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PlanRouteType {
    Tramway,
    Subway,
    Rail,
    Bus,
    Ferry,
    CableCar,
    Gondola,
    Funicular,
    Coach,
    Air,
    Taxi,
    Other,
}

impl PlanRouteType {
    pub fn from_gtfs_route_type(route_type: RouteType) -> PlanRouteType {
        match route_type {
            RouteType::Bus => PlanRouteType::Bus,
            RouteType::Air => PlanRouteType::Air,
            RouteType::Rail => PlanRouteType::Rail,
            RouteType::Taxi => PlanRouteType::Taxi,
            RouteType::Ferry => PlanRouteType::Ferry,
            RouteType::Coach => PlanRouteType::Coach,
            RouteType::Subway => PlanRouteType::Subway,
            RouteType::Tramway => PlanRouteType::Tramway,
            RouteType::Gondola => PlanRouteType::Gondola,
            RouteType::CableCar => PlanRouteType::CableCar,
            RouteType::Funicular => PlanRouteType::Funicular,
            RouteType::Other(_) => PlanRouteType::Other,
        }
    }
}

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct PlanRoute {
    pub short_name: String,
    pub long_name: String,
    pub mode: PlanRouteType,
    /// GTFS route colour as a 6-character hex string (e.g. `"ADD8E6"`), or
    /// `null` when the GTFS feed does not define a colour for this route.
    pub color: Option<String>,
    /// GTFS route text colour as a 6-character hex string, or `null`.
    pub text_color: Option<String>,

    #[graphql(skip)]
    pub agency_id: AgencyId,
}

#[ComplexObject]
impl PlanRoute {
    pub async fn agency(&self, ctx: &Context<'_>) -> Result<Option<PlanAgency>> {
        let graph = ctx.data::<Arc<Graph>>()?;

        Ok(PlanAgency::from_agency_id(graph, Some(self.agency_id)))
    }
}

fn rgb_to_hex(r: u8, g: u8, b: u8) -> String {
    format!("{:02X}{:02X}{:02X}", r, g, b)
}

impl PlanRoute {
    pub fn from_route_id(g: &Graph, id: Option<RouteId>) -> Option<PlanRoute> {
        let route = g.get_route(id?)?;

        Some(PlanRoute {
            short_name: route.route_short_name.clone(),
            long_name: route.route_long_name.clone(),
            mode: PlanRouteType::from_gtfs_route_type(route.route_type),
            color: route.route_color.map(|(r, g, b)| rgb_to_hex(r, g, b)),
            text_color: route.route_text_color.map(|(r, g, b)| rgb_to_hex(r, g, b)),
            agency_id: route.agency_id,
        })
    }
}
