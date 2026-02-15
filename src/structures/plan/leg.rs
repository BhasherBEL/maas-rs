use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Enum, Interface, Result, SimpleObject};

use crate::{
    ingestion::gtfs::{TripId, TripSegment},
    structures::{
        Graph,
        plan::{PlanLegStep, PlanPlace, PlanTransitLegStep, PlanTrip},
    },
};

#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PlanLegType {
    WALK,
    TRANSIT,
    OTHER,
}

#[derive(Debug, Interface, Clone)]
#[graphql(field(name = "length", ty = "&usize"))]
#[graphql(field(name = "start", ty = "&u32"))]
#[graphql(field(name = "end", ty = "&u32"))]
#[graphql(field(name = "duration", ty = "&u32"))]
#[graphql(field(name = "from", ty = "&PlanPlace"))]
#[graphql(field(name = "to", ty = "&PlanPlace"))]
#[graphql(field(name = "steps", ty = "&Vec<PlanLegStep>"))]
pub enum PlanLeg {
    Transit(PlanTransitLeg),
    Walk(PlanWalkLeg),
}

#[derive(Debug, SimpleObject, Clone)]
pub struct PlanWalkLeg {
    pub length: usize,
    pub start: u32,
    pub end: u32,
    pub duration: u32,

    pub from: PlanPlace,
    pub to: PlanPlace,

    pub steps: Vec<PlanLegStep>,
}

#[derive(Debug, SimpleObject, Clone)]
#[graphql(complex)]
pub struct PlanTransitLeg {
    pub length: usize,
    pub start: u32,
    pub end: u32,
    pub duration: u32,

    pub from: PlanPlace,
    pub to: PlanPlace,

    pub steps: Vec<PlanLegStep>,

    #[graphql(skip)]
    pub trip_id: TripId,
}

#[ComplexObject]
impl PlanTransitLeg {
    async fn trip(&self, ctx: &Context<'_>) -> Result<Option<PlanTrip>> {
        let graph = ctx.data::<Arc<Graph>>()?;
        Ok(PlanTrip::from_trip_id(graph, self.trip_id))
    }

    async fn previous_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        if count == 0 {
            return Ok(vec![]);
        }
        let graph = ctx.data::<Arc<Graph>>()?;
        let first = match self.steps[0] {
            PlanLegStep::Walk(_) => {
                return Err(async_graphql::Error::new(
                    "Found a walk step in a transit leg",
                ));
            }
            PlanLegStep::Transit(first) => first,
        };
        self.find_alternatives(
            &graph,
            graph.previous_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )
    }

    async fn next_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        if count == 0 {
            return Ok(vec![]);
        }
        let graph = ctx.data::<Arc<Graph>>()?;
        let first = match self.steps[0] {
            PlanLegStep::Walk(_) => {
                return Err(async_graphql::Error::new(
                    "Found a walk step in a transit leg",
                ));
            }
            PlanLegStep::Transit(first) => first,
        };
        self.find_alternatives(
            &graph,
            graph.next_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )
    }
}

impl PlanTransitLeg {
    fn find_alternatives<'a>(
        &self,
        graph: &'a Graph,
        candidates: impl Iterator<Item = (usize, &'a TripSegment)>,
        count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        let first = match self.steps[0] {
            PlanLegStep::Walk(_) => return Err(async_graphql::Error::new("")),
            PlanLegStep::Transit(first) => first,
        };

        let remaining_steps: Vec<&PlanTransitLegStep> = self
            .steps
            .iter()
            .skip(1)
            .filter_map(|s| match s {
                PlanLegStep::Transit(ts) => Some(ts),
                _ => None,
            })
            .collect();

        Ok(candidates
            .filter_map(|(idx, segment)| {
                let trip_id = segment.trip_id;
                let mut current_arrival = segment.arrival;
                let mut new_steps = Vec::with_capacity(self.steps.len());

                new_steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    departure_index: idx,
                    weekday: first.weekday,
                    date: first.date,
                    timetable_segment: first.timetable_segment,
                    time: segment.departure,
                    place: first.place,
                    length: first.length,
                }));

                for step in &remaining_steps {
                    let tt = step.timetable_segment;
                    let slice = graph.get_transit_departure_slice(tt);

                    let (local_idx, seg) = slice
                        .iter()
                        .enumerate()
                        .find(|(_, dep)| dep.trip_id == trip_id)?;

                    current_arrival = seg.arrival;
                    new_steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                        length: step.length,
                        time: seg.departure,
                        place: step.place,
                        timetable_segment: step.timetable_segment,
                        departure_index: tt.start + local_idx,
                        date: step.date,
                        weekday: step.weekday,
                    }));
                }

                Some(PlanTransitLeg {
                    steps: new_steps,
                    trip_id,
                    start: segment.departure,
                    end: current_arrival,
                    length: 0,
                    to: self.to,
                    from: self.from,
                    duration: current_arrival - segment.departure,
                })
            })
            .take(count)
            .collect())
    }
}
