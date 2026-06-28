use async_graphql::{ComplexObject, Context, Interface, Result, SimpleObject};
use gtfs_structures::RouteType;

use crate::{
    ingestion::gtfs::{TripId, TripSegment},
    structures::{
        Graph, Mode, NodeID,
        plan::{LegOption, PlanLegStep, PlanPlace, PlanTransitLegStep, PlanTrip, PlanWalkLegStep},
    },
};

/// A single lat/lon coordinate point in a leg's geometry.
#[derive(Debug, SimpleObject, Clone, Copy)]
pub struct PlanCoordinate {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Interface, Clone)]
// clippy false positive: each #[graphql(field(...))] defines a distinct field,
// but the lint keys on the repeated `ty` values.
#[allow(clippy::duplicated_attributes)]
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
#[graphql(complex)]
pub struct PlanWalkLeg {
    pub length: usize,
    pub cycleroute_length: Option<usize>,
    /// Total ascent (D+) in meters along the leg, summed over the path's positive
    /// elevation deltas. `None` when not computed (only the cost-routed bike leg sets it).
    pub elevation_gain: Option<usize>,
    pub start: u32,
    pub end: u32,
    pub duration: u32,

    /// How this street leg is traversed: `Walk` or `Bike`.
    pub street_mode: Mode,

    pub from: PlanPlace,
    pub to: PlanPlace,

    pub steps: Vec<PlanLegStep>,

    /// Ordered sequence of coordinates tracing the walking path.
    pub geometry: Vec<PlanCoordinate>,

    pub alternatives: Vec<LegOption>,

    /// "Leave by" time (seconds since midnight) for an access leg with a downstream
    /// boarding deadline: depart by this to make the connection with 95% confidence
    /// (`board − p95`). `None` for legs without a deadline (egress, direct).
    pub leave_by: Option<u32>,
}

impl PlanWalkLeg {
    fn reselect_checked(&self, option_index: i32) -> Result<PlanWalkLeg, &'static str> {
        if option_index < 0 {
            return Err("option_index out of range");
        }
        self.reselect_to(option_index as usize)
            .ok_or("option_index out of range")
    }

    /// A view of this leg with option `i` highlighted: leg metrics/geometry/steps
    /// mirror `alternatives[i]`, the option set is preserved. O(1), no re-search.
    pub fn reselect_to(&self, i: usize) -> Option<PlanWalkLeg> {
        let o = self.alternatives.get(i)?;
        let mut leg = self.clone();
        leg.length = o.length;
        leg.duration = o.p50;
        leg.elevation_gain = o.elevation_gain;
        leg.cycleroute_length = o.cycleroute_length;
        leg.geometry = o.geometry.clone();
        if self.leave_by.is_some() {
            leg.start = self.end.saturating_sub(o.p50);
            leg.leave_by = Some(self.end.saturating_sub(o.p95));
        } else {
            leg.end = self.start + o.p50;
        }
        leg.steps = vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
            o.length, o.p50, leg.to,
        ))];
        Some(leg)
    }
}

#[ComplexObject]
impl PlanWalkLeg {
    /// Switch the highlighted alternative without re-running the engine.
    async fn reselect(&self, option_index: i32) -> Result<PlanWalkLeg> {
        self.reselect_checked(option_index)
            .map_err(async_graphql::Error::new)
    }
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
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();
        Ok(PlanTrip::from_trip_id(graph.as_ref(), self.trip_id))
    }

    async fn previous_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();
        self.previous_departures_on(&graph, count)
    }

    async fn next_departures(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 0)] count: usize,
    ) -> Result<Vec<PlanTransitLeg>> {
        let graph = ctx
            .data::<crate::services::scheduler::SharedGraph>()?
            .load_full();
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
                return Err(async_graphql::Error::new(
                    "Found a walk step in a transit leg",
                ));
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
        results.extend(self.build_cross_route_legs(
            graph,
            cross,
            self.from.node_id,
            self.to.node_id,
        ));
        if self.time_shift > 0 {
            results = results
                .into_iter()
                .map(|l| shift_transit_leg(l, self.time_shift))
                .collect();
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
                return Err(async_graphql::Error::new(
                    "Found a walk step in a transit leg",
                ));
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
        results.extend(self.build_cross_route_legs(
            graph,
            cross,
            self.from.node_id,
            self.to.node_id,
        ));
        if self.time_shift > 0 {
            results = results
                .into_iter()
                .map(|l| shift_transit_leg(l, self.time_shift))
                .collect();
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
                let (first_local, first_seg) = first_slice
                    .iter()
                    .enumerate()
                    .find(|(_, d)| d.trip_id == trip_id)?;

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

                let transfer_risk = self.alternative_transfer_risk(
                    graph,
                    trip_id,
                    segment.departure,
                    current_arrival,
                );

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{Mode, NodeID};

    fn sample_walk_leg() -> PlanWalkLeg {
        let place = PlanPlace {
            stop_position: None,
            arrival: None,
            departure: None,
            node_id: NodeID(0),
        };
        PlanWalkLeg {
            length: 50,
            cycleroute_length: None,
            elevation_gain: None,
            start: 1000,
            end: 1060,
            duration: 60,
            street_mode: Mode::Walk,
            from: place,
            to: place,
            steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(50, 60, place))],
            geometry: vec![],
            alternatives: vec![],
            leave_by: None,
        }
    }

    #[test]
    fn walk_leg_leave_by_defaults_none_and_roundtrips() {
        let mut leg = sample_walk_leg();
        assert_eq!(leg.leave_by, None, "non-access legs carry no leave-by");
        leg.leave_by = Some(28_800);
        assert_eq!(leg.leave_by, Some(28_800));
    }

    #[test]
    fn reselect_checked_rejects_negative_and_out_of_range() {
        use crate::structures::plan::LegOption;
        let opt = |len: usize| LegOption {
            time: len as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50: len as u32,
            p95: len as u32,
            length: len,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![],
            edges: vec![],
        };
        let mut leg = sample_walk_leg();
        leg.alternatives = vec![opt(100), opt(250)];
        leg.length = 100;
        assert!(leg.reselect_checked(-1).is_err(), "negative index rejected");
        assert!(leg.reselect_checked(9).is_err(), "out-of-range rejected");
        assert_eq!(
            leg.reselect_checked(1).unwrap().length,
            250,
            "valid index mirrors option"
        );
    }

    #[test]
    fn reselect_swaps_highlight_from_precomputed_options_without_research() {
        use crate::structures::plan::LegOption;
        let opt = |len: usize| LegOption {
            time: len as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50: len as u32,
            p95: len as u32,
            length: len,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![],
            edges: vec![],
        };
        let mut leg = sample_walk_leg();
        leg.alternatives = vec![opt(100), opt(250)];
        leg.length = 100;
        let swapped = leg.reselect_to(1).expect("valid index");
        assert_eq!(swapped.length, 250, "leg now mirrors option 1");
        assert_eq!(
            swapped.end,
            leg.start + 250,
            "non-deadline: end = start + p50"
        );
        assert_eq!(swapped.alternatives.len(), 2, "option set unchanged");
        assert!(leg.reselect_to(9).is_none(), "out-of-range rejected");
    }

    #[test]
    fn reselect_access_leg_holds_board_end_fixed() {
        use crate::structures::plan::LegOption;
        let opt = |p50: u32, p95: u32| LegOption {
            time: p50 as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50,
            p95,
            length: p50 as usize,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![],
            edges: vec![],
        };
        let board = 30_000u32;
        let p50_0 = 200u32;
        let p95_0 = 260u32;
        let p50_1 = 350u32;
        let p95_1 = 420u32;
        let mut leg = sample_walk_leg();
        leg.end = board;
        leg.start = board - p50_0;
        leg.duration = p50_0;
        leg.leave_by = Some(board - p95_0);
        leg.alternatives = vec![opt(p50_0, p95_0), opt(p50_1, p95_1)];

        let reselected = leg.reselect_to(1).expect("valid index");
        assert_eq!(
            reselected.end, board,
            "boarding time (end) must be unchanged"
        );
        assert_eq!(reselected.start, board - p50_1, "start = board - p50");
        assert_eq!(
            reselected.leave_by,
            Some(board - p95_1),
            "leave_by = board - p95 of selected option"
        );
        assert_eq!(reselected.duration, p50_1);
    }

    /// P3f drop gate: `previous_departures_on` / `next_departures_on` must (a) not panic
    /// and (b) return byte-identical results before and after `drop_full_node_arrays()`.
    /// Uses a 2-trip fixture so `next_departures_on` returns a non-empty list, proving the
    /// path actually traverses timetable data rather than short-circuiting to empty.
    #[test]
    fn leg_alternatives_drop_gate_identical() {
        use crate::{
            ingestion::gtfs::{
                AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime,
                TimetableSegment, TripId, TripInfo, TripSegment,
            },
            structures::{
                BikeAttrs, EdgeData, Graph, LatLng, NodeData, OsmNodeData,
                StreetEdgeData, TransitEdgeData, TransitStopData,
                contraction::ContractedGraph,
                cost::VarGen,
                raptor::{Lookup, PatternInfo},
            },
        };
        use gtfs_structures::{Availability, RouteType};

        let mut g = Graph::new();

        let origin = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "origin".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.000 },
        }));
        let j_a = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "jA".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.003 },
        }));
        let j_b = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "jB".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.030 },
        }));
        let dest = g.add_node(NodeData::OsmNode(OsmNodeData {
            eid: "dest".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.033 },
        }));
        let stop_a = g.add_node(NodeData::TransitStop(TransitStopData {
            name: "Stop A".into(),
            id: "A".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.003 },
            accessibility: Availability::Available,
        }));
        let stop_b = g.add_node(NodeData::TransitStop(TransitStopData {
            name: "Stop B".into(),
            id: "B".into(),
            lat_lng: LatLng { latitude: 50.000, longitude: 4.030 },
            accessibility: Availability::Available,
        }));

        let bidir = |g: &mut Graph, a: crate::structures::NodeID, b: crate::structures::NodeID, len: usize| {
            for (o, d) in [(a, b), (b, a)] {
                g.add_edge(o, EdgeData::Street(StreetEdgeData {
                    origin: o, destination: d, length: len, partial: false,
                    foot: true, bike: true, car: true,
                    attrs: BikeAttrs::road_default(), elev_delta: 0,
                    surface_speed: 100, var_gen: VarGen::NONE,
                }));
            }
        };
        bidir(&mut g, origin, j_a, 300);
        bidir(&mut g, j_a, j_b, 500);
        bidir(&mut g, j_b, dest, 300);

        for (stop, junc) in [(stop_a, j_a), (stop_b, j_b)] {
            for (o, d) in [(stop, junc), (junc, stop)] {
                g.add_edge(o, EdgeData::Street(StreetEdgeData {
                    origin: o, destination: d, length: 5, partial: true,
                    foot: true, bike: false, car: false,
                    attrs: BikeAttrs::road_default(), elev_delta: 0,
                    surface_speed: 100, var_gen: VarGen::NONE,
                }));
            }
        }

        g.add_edge(stop_a, EdgeData::Transit(TransitEdgeData {
            origin: stop_a, destination: stop_b,
            route_id: RouteId(0),
            timetable_segment: TimetableSegment { start: 0, len: 2 },
            length: 1900,
        }));

        g.add_transit_services(vec![ServicePattern {
            days_of_week: 0x7F, start_date: 0, end_date: 9999,
            added_dates: vec![], removed_dates: vec![],
        }]);
        g.add_transit_routes(vec![RouteInfo {
            route_short_name: "1".into(), route_long_name: "Bus 1".into(),
            route_type: RouteType::Bus, agency_id: AgencyId(0),
            route_color: None, route_text_color: None,
        }]);
        g.add_transit_trips(vec![
            TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
            TripInfo { trip_headsign: None, route_id: RouteId(0), service_id: ServiceId(0), bikes_allowed: None },
        ]);
        g.add_transit_departures(vec![
            TripSegment { trip_id: TripId(0), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 8 * 3600, arrival: 8 * 3600 + 600, service_id: ServiceId(0) },
            TripSegment { trip_id: TripId(1), origin_stop_sequence: 0, destination_stop_sequence: 1, departure: 9 * 3600, arrival: 9 * 3600 + 600, service_id: ServiceId(0) },
        ]);

        {
            let ss = g.transit_pattern_stops_len();
            g.extend_transit_pattern_stops(&[stop_a, stop_b]);
            g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 2 });
            let ts = g.transit_pattern_trips_len();
            g.push_transit_pattern_trip(TripId(0));
            g.push_transit_pattern_trip(TripId(1));
            g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 2 });
            let sts = g.transit_pattern_stop_times_len();
            // Column-major: stop 0 (stop_a): trip 0 at 8:00, trip 1 at 9:00
            g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600, departure: 8 * 3600 });
            g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600, departure: 9 * 3600 });
            // Column-major: stop 1 (stop_b): trip 0 at 8:10, trip 1 at 9:10
            g.push_transit_pattern_stop_time(StopTime { arrival: 8 * 3600 + 600, departure: 8 * 3600 + 600 });
            g.push_transit_pattern_stop_time(StopTime { arrival: 9 * 3600 + 600, departure: 9 * 3600 + 600 });
            g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 4 });
            g.push_transit_pattern(PatternInfo { route: RouteId(0), num_trips: 2 });
        }

        g.build_raptor_index();
        let mut cg = ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        g.contracted = Some(cg);
        g.bake_bike_on_contracted_default();

        // Route directly by NodeID (bypasses coordinate snapping), using the same
        // parameters as `t4_drop_g_then_route_identical` — date=0 (2000-01-01),
        // weekday=0x7F (all days), querying at 7:50 to board the 8:00 trip.
        let plans_before = g.raptor(origin, dest, 7 * 3600 + 50 * 60, 0, 0x7F, 10 * 60);

        let transit_leg_before = plans_before
            .iter()
            .find_map(|p| {
                p.legs.iter().find_map(|l| {
                    if let PlanLeg::Transit(t) = l { Some(t.clone()) } else { None }
                })
            })
            .expect("pre-drop plan must contain a transit leg");

        let prev_before = transit_leg_before.previous_departures_on(&g, 3).expect("pre-drop previous_departures_on");
        let next_before = transit_leg_before.next_departures_on(&g, 3).expect("pre-drop next_departures_on");
        assert!(
            !next_before.is_empty(),
            "next_departures_on must return the 9:00 alternative pre-drop (2-trip fixture)"
        );

        g.drop_full_node_arrays();
        assert_eq!(g.node_count(), 0, "g arrays dropped");

        let plans_after = g.raptor(origin, dest, 7 * 3600 + 50 * 60, 0, 0x7F, 10 * 60);

        let transit_leg_after = plans_after
            .iter()
            .find_map(|p| {
                p.legs.iter().find_map(|l| {
                    if let PlanLeg::Transit(t) = l { Some(t.clone()) } else { None }
                })
            })
            .expect("post-drop plan must contain a transit leg");

        let prev_after = transit_leg_after.previous_departures_on(&g, 3).expect("post-drop previous_departures_on");
        let next_after = transit_leg_after.next_departures_on(&g, 3).expect("post-drop next_departures_on");

        let starts_ends = |legs: &[PlanTransitLeg]| -> Vec<(u32, u32)> {
            legs.iter().map(|l| (l.start, l.end)).collect()
        };

        assert_eq!(
            starts_ends(&prev_before),
            starts_ends(&prev_after),
            "previous_departures_on must be byte-identical pre/post drop"
        );
        assert_eq!(
            starts_ends(&next_before),
            starts_ends(&next_after),
            "next_departures_on must be byte-identical pre/post drop"
        );
    }
}
