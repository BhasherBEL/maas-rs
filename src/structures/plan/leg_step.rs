use async_graphql::{Interface, SimpleObject};

use crate::{ingestion::gtfs::TimetableSegment, structures::plan::PlanPlace};

#[derive(Clone, Copy, Debug, Interface)]
#[graphql(field(name = "length", ty = "&usize"))]
#[graphql(field(name = "time", ty = "&u32"))]
#[graphql(field(name = "place", ty = "&PlanPlace"))]
pub enum PlanLegStep {
    Walk(PlanWalkLegStep),
    Transit(PlanTransitLegStep),
}

#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanWalkLegStep {
    pub length: usize,
    pub time: u32,
    pub place: PlanPlace,
}

#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanTransitLegStep {
    pub length: usize,
    pub time: u32,
    pub place: PlanPlace,

    #[graphql(skip)]
    pub timetable_segment: TimetableSegment,
    #[graphql(skip)]
    pub departure_index: usize,
    #[graphql(skip)]
    pub date: u32,
    #[graphql(skip)]
    pub weekday: u8,
}
