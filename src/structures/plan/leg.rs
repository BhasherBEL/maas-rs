
use async_graphql::{ComplexObject, Context, Interface, Result, SimpleObject};
use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{TripId, TripSegment},
    structures::{
        Graph, NodeID,
        plan::{PlanLegStep, PlanPlace, PlanTransitLegStep, PlanTrip},
    },
};

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
    /// is missed), convolving the feeder and boarding delay distributions over
    /// the `next_departure − arrival_at_boarding_stop` margin.  `None` when there
    /// is no next departure or no delay model for the feeder route type.
    pub next_reliability: Option<f32>,
    /// `scheduled_departure − arrival_at_boarding_stop` in seconds.
    /// Negative = transfer is physically impossible (arrive after the train departs).
    /// `None` for the first leg (no preceding vehicle).
    pub margin_secs: Option<i32>,
}

#[derive(Debug, SimpleObject, Clone)]
#[graphql(complex)]
pub struct PlanTransitLeg {
    pub length: usize,
    /// Effective boarding time (seconds since midnight). Equals `scheduled_start`
    /// unless realtime data shifts it.
    pub start: u32,
    /// Effective alighting time. Equals `scheduled_end` unless realtime shifts it.
    pub end: u32,
    pub duration: u32,

    /// Scheduled (timetable) boarding time, before any realtime delay.
    pub scheduled_start: u32,
    /// Scheduled (timetable) alighting time, before any realtime delay.
    pub scheduled_end: u32,
    /// True when realtime data informs this leg's times (UI shows it as "live").
    pub realtime: bool,

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
    /// this leg's boarding stop.  When combined with `transfer_risk.scheduled_departure`,
    /// gives the actual transfer window (`scheduled_departure - preceding_arrival`).
    pub preceding_arrival: Option<u32>,

    /// Route type of the preceding transit vehicle (the one that delivered the
    /// user to this leg's boarding stop).  Combined with `preceding_arrival` to
    /// select the correct delay-CDF when populating `transfer_risk` on alternatives.
    #[graphql(skip)]
    pub preceding_route_type: Option<RouteType>,

    /// This leg's own route type (the vehicle being boarded).  Combined with
    /// `preceding_route_type` to convolve both delay-CDFs when scoring the
    /// transfer onto this leg.
    #[graphql(skip)]
    pub route_type: Option<RouteType>,

    /// Route type of the *following* transit leg (the next vehicle the user
    /// boards after this one).  `None` when this is the last transit leg.  Used,
    /// with `following_margin_secs`, to score whether an alternative for this leg
    /// still makes the downstream connection.
    #[graphql(skip)]
    pub following_route_type: Option<RouteType>,

    /// Outbound connection slack of the original plan, in seconds: the next transit
    /// leg's scheduled boarding minus this leg's scheduled arrival at that boarding
    /// stop (i.e. minus the intervening transfer walk).  `None` when this is the
    /// last transit leg.  An alternative arriving `d` seconds later than scheduled
    /// has effective slack `following_margin_secs − d`.
    #[graphql(skip)]
    pub following_margin_secs: Option<i32>,

    /// Whether bikes are allowed on this transit leg.
    /// `None` = no information available.
    pub bikes_allowed: Option<bool>,

    /// Seconds subtracted from raw timetable times when this leg was built from an
    /// overnight RAPTOR pass (86400 for prev-day trips, 0 otherwise). Used by the
    /// `previousDepartures`/`nextDepartures` resolvers to normalize returned times.
    #[graphql(skip)]
    pub time_shift: u32,
}

#[ComplexObject]
impl PlanTransitLeg {
    async fn trip(&self, ctx: &Context<'_>) -> Result<Option<PlanTrip>> {
        let graph = ctx.data::<crate::services::scheduler::SharedGraph>()?.load_full();
        Ok(PlanTrip::from_trip_id(graph.as_ref(), self.trip_id))
    }

    async fn previous_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        let graph = ctx.data::<crate::services::scheduler::SharedGraph>()?.load_full();
        self.previous_departures_on(&graph, count)
    }

    async fn next_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        let graph = ctx.data::<crate::services::scheduler::SharedGraph>()?.load_full();
        self.next_departures_on(&graph, count)
    }
}

impl PlanTransitLeg {
    /// Earlier same-service + cross-route departures for this leg, scored for swap
    /// reliability. Shared by the `previousDepartures` resolver and the lazy
    /// `legAlternatives` query.
    pub(crate) fn previous_departures_on(
        &self,
        graph: &Graph,
        count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        if count == 0 {
            return Ok(vec![]);
        }
        let first = match self.steps[0] {
            PlanLegStep::Walk(_) => {
                return Err(async_graphql::Error::new("Found a walk step in a transit leg"));
            }
            PlanLegStep::Transit(first) => first,
        };
        let mut results = self.find_alternatives(
            graph,
            graph.previous_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )?;
        // Use raw timetable reference time for cross-route search.
        let cross = graph.cross_route_departures(
            self.from.node_id,
            self.to.node_id,
            first.timetable_segment,
            self.start + self.time_shift,
            first.date,
            first.weekday,
            false,
            count,
        );
        results.extend(self.build_cross_route_legs(graph, cross, self.from.node_id, self.to.node_id));
        if self.time_shift > 0 {
            results = results.into_iter().map(|l| shift_transit_leg(l, self.time_shift)).collect();
        }
        results.sort_by_key(|l| l.start);
        results.reverse();
        results.truncate(count);
        Ok(results)
    }

    /// Later same-service + cross-route departures for this leg, scored for swap
    /// reliability. Shared by the `nextDepartures` resolver and `legAlternatives`.
    pub(crate) fn next_departures_on(
        &self,
        graph: &Graph,
        count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        if count == 0 {
            return Ok(vec![]);
        }
        let first = match self.steps[0] {
            PlanLegStep::Walk(_) => {
                return Err(async_graphql::Error::new("Found a walk step in a transit leg"));
            }
            PlanLegStep::Transit(first) => first,
        };
        let mut results = self.find_alternatives(
            graph,
            graph.next_departures(
                first.timetable_segment,
                first.date,
                first.weekday,
                first.departure_index,
            ),
            count,
        )?;
        // Use raw timetable reference time for cross-route search.
        let cross = graph.cross_route_departures(
            self.from.node_id,
            self.to.node_id,
            first.timetable_segment,
            self.start + self.time_shift,
            first.date,
            first.weekday,
            true,
            count,
        );
        results.extend(self.build_cross_route_legs(graph, cross, self.from.node_id, self.to.node_id));
        if self.time_shift > 0 {
            results = results.into_iter().map(|l| shift_transit_leg(l, self.time_shift)).collect();
        }
        results.sort_by_key(|l| l.start);
        results.truncate(count);
        Ok(results)
    }
}

/// Subtract `s` from raw-timetable times in a result leg to normalize it to
/// wall-clock time. Used when returning alternatives for an overnight-shifted leg.
fn shift_transit_leg(mut l: PlanTransitLeg, s: u32) -> PlanTransitLeg {
    l.start = l.start.saturating_sub(s);
    l.end = l.end.saturating_sub(s);
    l.scheduled_start = l.scheduled_start.saturating_sub(s);
    l.scheduled_end = l.scheduled_end.saturating_sub(s);
    l.from.departure = l.from.departure.map(|d| d.saturating_sub(s));
    l.to.arrival = l.to.arrival.map(|a| a.saturating_sub(s));
    if let Some(tr) = &mut l.transfer_risk {
        tr.scheduled_departure = tr.scheduled_departure.saturating_sub(s);
        tr.next_departure = tr.next_departure.map(|d| d.saturating_sub(s));
    }
    for step in &mut l.steps {
        if let PlanLegStep::Transit(ts) = step {
            ts.time = ts.time.saturating_sub(s);
        }
    }
    l
}

impl PlanTransitLeg {
    /// Marginal swap reliability for an *alternative* of this leg — the chance the
    /// whole journey still works if only this leg is replaced by the alternative,
    /// everything else fixed:
    ///
    /// `P(inbound) × P(outbound)`
    ///   - `P(inbound)`  — board the alternative given the fixed preceding leg's
    ///     arrival; `1.0` when this is the first transit leg.
    ///   - `P(outbound)` — the alternative still makes the fixed following leg;
    ///     `1.0` when this is the last transit leg.
    ///
    /// Returns `None` only for a lone transit leg (neither preceding nor following),
    /// where there is no connection to score.
    fn alternative_transfer_risk(
        &self,
        graph: &Graph,
        alt_trip_id: TripId,
        alt_dep: u32,
        alt_end: u32,
    ) -> Option<TransferRisk> {
        let alt_rt = graph.route_type_of_trip(alt_trip_id);
        // `self` times were shifted by -time_shift during overnight normalization.
        // Add it back to compare in the same raw timetable domain as alt_dep/alt_end.
        let s = self.time_shift;

        let (p_in, in_margin) = match (self.preceding_arrival, self.preceding_route_type) {
            (Some(pa), Some(prt)) => {
                let margin = alt_dep as i32 - (pa + s) as i32;
                let board = alt_rt.and_then(|rt| graph.get_delay_model(rt));
                let p = match graph.get_delay_model(prt) {
                    Some(cdf) => cdf.prob_on_time_vs(board, margin),
                    None => 1.0,
                };
                (p, Some(margin))
            }
            _ => (1.0, None),
        };

        let has_following = self.following_margin_secs.is_some();
        if in_margin.is_none() && !has_following {
            return None;
        }

        let p_out = graph.outbound_reliability(
            alt_rt,
            self.following_route_type,
            self.following_margin_secs,
            (self.scheduled_end + s) as i32 - alt_end as i32,
        );

        Some(TransferRisk {
            reliability: p_in * p_out,
            scheduled_departure: alt_dep,
            next_departure: None,
            next_reliability: None,
            margin_secs: in_margin,
        })
    }

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
                let transfer_risk = self.alternative_transfer_risk(graph, trip_id, dep, arr);
                PlanTransitLeg {
                    steps: vec![],
                    trip_id,
                    start: dep,
                    end: arr,
                    scheduled_start: dep,
                    scheduled_end: arr,
                    realtime: false,
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
                    route_type: graph.route_type_of_trip(trip_id),
                    following_route_type: self.following_route_type,
                    following_margin_secs: self.following_margin_secs,
                    bikes_allowed: graph.get_trip(trip_id).and_then(|t| t.bikes_allowed),
                    time_shift: 0,
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
            .filter_map(|(_idx, segment)| {
                let trip_id = segment.trip_id;
                let mut current_arrival = segment.arrival;
                let mut new_steps = Vec::with_capacity(self.steps.len());

                // Derive the first step's departure index from THIS leg's first
                // timetable segment (not the caller's index, which may come from a
                // different segment/pattern — e.g. the backward-tightening path). If
                // the trip isn't on this segment it isn't a valid alternative here.
                let first_slice = graph.get_transit_departure_slice(first.timetable_segment);
                let (first_local, first_seg) =
                    first_slice.iter().enumerate().find(|(_, d)| d.trip_id == trip_id)?;

                new_steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    departure_index: first.timetable_segment.start + first_local,
                    weekday: first.weekday,
                    date: first.date,
                    timetable_segment: first.timetable_segment,
                    time: first_seg.departure,
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
                    self.alternative_transfer_risk(graph, trip_id, segment.departure, current_arrival);

                Some(PlanTransitLeg {
                    steps: new_steps,
                    trip_id,
                    start: segment.departure,
                    end: current_arrival,
                    scheduled_start: segment.departure,
                    scheduled_end: current_arrival,
                    realtime: false,
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
                    route_type: graph.route_type_of_trip(trip_id),
                    following_route_type: self.following_route_type,
                    following_margin_secs: self.following_margin_secs,
                    bikes_allowed: graph.get_trip(trip_id).and_then(|t| t.bikes_allowed),
                    time_shift: 0,
                })
            })
            .take(count)
            .collect())
    }
}
