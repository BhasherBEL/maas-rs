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

    /// True when this step must be walked with the bike pushed (a foot-only
    /// "dismount" stretch on a bike journey). Always false on plain walk legs.
    pub dismount: bool,
    /// Inclusive `[geom_start, geom_end]` index range into the parent leg's
    /// `geometry`, delimiting this step's coordinate run so the client can draw it
    /// distinctly. Both `0` on a single-step leg (the client draws the whole
    /// `geometry` and ignores the range).
    pub geom_start: usize,
    pub geom_end: usize,
}

impl PlanWalkLegStep {
    /// A single-step walk leg covering its whole geometry (no dismount, no range).
    pub fn plain(length: usize, time: u32, place: PlanPlace) -> Self {
        PlanWalkLegStep {
            length,
            time,
            place,
            dismount: false,
            geom_start: 0,
            geom_end: 0,
        }
    }
}

#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanTransitLegStep {
    pub length: usize,
    pub time: u32,
    pub place: PlanPlace,

    pub scheduled_arrival: Option<u32>,
    pub scheduled_departure: Option<u32>,

    #[graphql(skip)]
    pub timetable_segment: TimetableSegment,
    #[graphql(skip)]
    pub departure_index: usize,
    #[graphql(skip)]
    pub date: u32,
    #[graphql(skip)]
    pub weekday: u8,
}
