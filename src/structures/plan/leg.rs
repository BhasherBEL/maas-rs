use std::sync::Arc;

use async_graphql::{ComplexObject, Context, Enum, Interface, Result, SimpleObject};
use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{TripId, TripSegment},
    structures::{
        Graph, NodeID,
        plan::{PlanLegStep, PlanPlace, PlanTransitLegStep, PlanTrip},
    },
};

#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PlanLegType {
    WALK,
    TRANSIT,
    OTHER,
}

/// A single lat/lon coordinate point in a leg's geometry.
#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanCoordinate {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Interface, Clone)]
#[graphql(field(name = "length", ty = "&usize"))]
#[graphql(field(name = "start", ty = "&u32"))]
#[graphql(field(name = "end", ty = "&u32"))]
#[graphql(field(name = "duration", ty = "&u32"))]
#[graphql(field(name = "from", ty = "&PlanPlace"))]
#[graphql(field(name = "to", ty = "&PlanPlace"))]
#[graphql(field(name = "steps", ty = "&Vec<PlanLegStep>"))]
#[graphql(field(name = "geometry", ty = "&Vec<PlanCoordinate>"))]
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

    /// Ordered sequence of coordinates tracing the walking path.
    pub geometry: Vec<PlanCoordinate>,
}

#[derive(Debug, SimpleObject, Clone)]
pub struct TransferRisk {
    /// Probability (0.0–1.0) that the user boards this vehicle on time.
    /// 1.0 = deterministic (no delay model for this route type).
    pub reliability: f32,
    /// Scheduled departure time of the boarded trip (seconds since midnight).
    pub scheduled_departure: u32,
    /// Departure time of the next available trip at the boarding stop, if any.
    /// The client computes `wait_if_missed = next_departure - scheduled_departure`.
    pub next_departure: Option<u32>,
    /// Probability (0.0–1.0) of boarding the *next* trip (if the scheduled one
    /// is missed).  Computed as `cdf.prob_on_time(next_departure −
    /// arrival_at_boarding_stop)`.  `None` when there is no next departure or
    /// no delay model for this route type.
    pub next_reliability: Option<f32>,
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

    /// Ordered sequence of stop coordinates along the transit route.
    pub geometry: Vec<PlanCoordinate>,

    /// Transfer risk for boarding this vehicle on time.
    /// `None` for the first transit leg (walked directly from journey origin).
    pub transfer_risk: Option<TransferRisk>,

    #[graphql(skip)]
    pub trip_id: TripId,

    /// Arrival time (seconds since midnight) of the preceding transit vehicle at
    /// this leg's boarding stop.  Used to compute `transfer_risk` for alternative
    /// departures returned by `previousDepartures`/`nextDepartures`.
    #[graphql(skip)]
    pub preceding_arrival: Option<u32>,

    /// Route type of the preceding transit vehicle (the one that delivered the
    /// user to this leg's boarding stop).  Combined with `preceding_arrival` to
    /// select the correct delay-CDF when populating `transfer_risk` on alternatives.
    #[graphql(skip)]
    pub preceding_route_type: Option<RouteType>,

    /// Whether bikes are allowed on this transit leg.
    /// `None` = no information available.
    pub bikes_allowed: Option<bool>,
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
        let mut results = self.find_alternatives(
            &graph,
            graph.previous_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )?;
        let cross = graph.cross_route_departures(
            self.from.node_id,
            self.to.node_id,
            first.timetable_segment,
            self.start,
            first.date,
            first.weekday,
            false,
            count,
        );
        results.extend(self.build_cross_route_legs(&graph, cross, self.from.node_id, self.to.node_id));
        results.sort_by_key(|l| l.start);
        results.reverse();
        results.truncate(count);
        Ok(results)
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
        let mut results = self.find_alternatives(
            &graph,
            graph.next_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )?;
        let cross = graph.cross_route_departures(
            self.from.node_id,
            self.to.node_id,
            first.timetable_segment,
            self.start,
            first.date,
            first.weekday,
            true,
            count,
        );
        results.extend(self.build_cross_route_legs(&graph, cross, self.from.node_id, self.to.node_id));
        results.sort_by_key(|l| l.start);
        results.truncate(count);
        Ok(results)
    }
}

impl PlanTransitLeg {
    fn build_cross_route_legs(
        &self,
        graph: &Graph,
        candidates: Vec<(TripId, u32, u32)>,
        boarding_node: NodeID,
        alighting_node: NodeID,
    ) -> Vec<PlanTransitLeg> {
        candidates
            .into_iter()
            .map(|(trip_id, dep, arr)| {
                let transfer_risk =
                    if let (Some(pa), Some(prt)) =
                        (self.preceding_arrival, self.preceding_route_type)
                    {
                        let margin = dep as i32 - pa as i32;
                        let rel = match graph.get_delay_model(prt) {
                            Some(cdf) => cdf.prob_on_time(margin),
                            None => 1.0,
                        };
                        Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: dep,
                            next_departure: None,
                            next_reliability: None,
                        })
                    } else {
                        None
                    };
                PlanTransitLeg {
                    steps: vec![],
                    trip_id,
                    start: dep,
                    end: arr,
                    length: 0,
                    from: PlanPlace {
                        departure: Some(dep),
                        arrival: self.from.arrival,
                        stop_position: self.from.stop_position,
                        node_id: boarding_node,
                    },
                    to: PlanPlace {
                        arrival: Some(arr),
                        departure: self.to.departure,
                        stop_position: self.to.stop_position,
                        node_id: alighting_node,
                    },
                    duration: arr - dep,
                    geometry: vec![],
                    transfer_risk,
                    preceding_arrival: self.preceding_arrival,
                    preceding_route_type: self.preceding_route_type,
                    bikes_allowed: graph.get_trip(trip_id).and_then(|t| t.bikes_allowed),
                }
            })
            .collect()
    }

    pub(crate) fn find_alternatives<'a>(
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

                let transfer_risk =
                    if let (Some(pa), Some(prt)) =
                        (self.preceding_arrival, self.preceding_route_type)
                    {
                        let margin = segment.departure as i32 - pa as i32;
                        let next_dep = graph
                            .next_departures(
                                first.timetable_segment,
                                first.date,
                                first.weekday,
                                idx,
                            )
                            .next()
                            .map(|(_, seg)| seg.departure);
                        let (rel, next_rel) = match graph.get_delay_model(prt) {
                            Some(cdf) => (
                                cdf.prob_on_time(margin),
                                next_dep.map(|nd| cdf.prob_on_time(nd as i32 - pa as i32)),
                            ),
                            None => (1.0, None),
                        };
                        Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: segment.departure,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                        })
                    } else {
                        None
                    };

                Some(PlanTransitLeg {
                    steps: new_steps,
                    trip_id,
                    start: segment.departure,
                    end: current_arrival,
                    length: 0,
                    from: PlanPlace {
                        departure: Some(segment.departure),
                        arrival: self.from.arrival,
                        stop_position: self.from.stop_position,
                        node_id: self.from.node_id,
                    },
                    to: PlanPlace {
                        arrival: Some(current_arrival),
                        departure: self.to.departure,
                        stop_position: self.to.stop_position,
                        node_id: self.to.node_id,
                    },
                    duration: current_arrival - segment.departure,
                    geometry: self.geometry.clone(),
                    transfer_risk,
                    preceding_arrival: self.preceding_arrival,
                    preceding_route_type: self.preceding_route_type,
                    bikes_allowed: graph.get_trip(trip_id).and_then(|t| t.bikes_allowed),
                })
            })
            .take(count)
            .collect())
    }
}
