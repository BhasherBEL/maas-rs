use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
};

use async_graphql::Result;
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::TripSegment,
    structures::{
        EdgeData, NodeID, RoutingParameters,
        plan::{
            ArrivalScenario, Plan, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg,
            PlanTransitLegStep, PlanWalkLeg, PlanWalkLegStep,
        },
    },
};

use super::Graph;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct AStarPriority {
    estimated_weight: usize,
    weight: usize,
    time: u32,
}

#[derive(Debug, Serialize, Copy, Deserialize, Clone)]
struct AStarOrigins {
    destination: NodeID,
    edge: EdgeData,
    next_departure_index: Option<usize>,
    time: u32,
}

/// Try to insert or update `dest` in the priority queue. If `dest` is already
/// in the queue with a higher weight, update it; if absent, insert it.
fn pq_update(
    pq: &mut PriorityQueue<NodeID, Reverse<AStarPriority>>,
    origins: &mut HashMap<NodeID, AStarOrigins>,
    dest: NodeID,
    weight: usize,
    estimated_weight: usize,
    time: u32,
    from: NodeID,
    edge: EdgeData,
    departure_index: Option<usize>,
) {
    match pq.get_priority(&dest) {
        Some(current) if current.0.weight <= weight => {}
        Some(_) => {
            pq.change_priority(
                &dest,
                Reverse(AStarPriority { estimated_weight, weight, time }),
            );
            origins.insert(dest, AStarOrigins {
                destination: from,
                edge,
                next_departure_index: departure_index,
                time,
            });
        }
        None => {
            pq.push(dest, Reverse(AStarPriority { estimated_weight, weight, time }));
            origins.insert(dest, AStarOrigins {
                destination: from,
                edge,
                next_departure_index: departure_index,
                time,
            });
        }
    }
}

fn new_transit_leg(
    step: PlanTransitLegStep,
    from: PlanPlace,
    to: PlanPlace,
    trip_segment: &TripSegment,
    edge_length: usize,
) -> PlanLeg {
    PlanLeg::Transit(PlanTransitLeg {
        steps: vec![PlanLegStep::Transit(step)],
        from,
        to,
        length: edge_length,
        start: trip_segment.departure,
        end: trip_segment.arrival,
        duration: trip_segment.arrival - trip_segment.departure,
        trip_id: trip_segment.trip_id,
        geometry: vec![],
        transfer_risk: None,
        preceding_arrival: None,
        preceding_route_type: None,
        bikes_allowed: None,
    })
}

impl Graph {
    pub fn a_star(
        &self,
        a: NodeID,
        b: NodeID,
        start_time: u32,
        start_day: u32,
        weekday: u8,
        params: RoutingParameters,
    ) -> Result<Plan, async_graphql::Error> {
        let mut pq = PriorityQueue::<NodeID, Reverse<AStarPriority>>::new();
        let mut origins = HashMap::<NodeID, AStarOrigins>::new();
        let mut visited = HashSet::<NodeID>::new();
        pq.push(
            a,
            Reverse(AStarPriority {
                estimated_weight: self.nodes_distance(a, b) * 1000 / params.estimator_speed,
                weight: 0,
                time: start_time,
            }),
        );

        while !pq.is_empty() {
            let (id, p) = match pq.pop() {
                Some(x) => x,
                None => return Err(async_graphql::Error::new("No plan found")),
            };

            if id == b {
                let legs =
                    self.reconstruct_path(start_time, start_day, weekday, &origins, id, params)?;
                return Ok(Plan {
                    start: start_time,
                    end: p.0.time,
                    legs,
                    arrival_distribution: vec![ArrivalScenario {
                        time: p.0.time,
                        probability: 1.0,
                    }],
                });
            }
            visited.insert(id);

            if let Some(neighbors) = self.edges.get(id.0) {
                for neighbor in neighbors {
                    match neighbor {
                        EdgeData::Street(street) => {
                            if visited.contains(&street.destination) {
                                continue;
                            }
                            let weight = p.0.weight + street.length * 1000 / params.walking_speed;
                            let time =
                                p.0.time + (street.length * 1000 / params.walking_speed) as u32;
                            let estimated = weight
                                + self.nodes_distance(street.destination, b) * 1000
                                    / params.estimator_speed;

                            pq_update(
                                &mut pq,
                                &mut origins,
                                street.destination,
                                weight,
                                estimated,
                                time,
                                id,
                                *neighbor,
                                None,
                            );
                        }
                        EdgeData::Transit(transit) => {
                            if visited.contains(&transit.destination) {
                                continue;
                            }

                            let (next_departure_index, next_departure) = match self
                                .next_transit_departure(
                                    transit.timetable_segment,
                                    p.0.time,
                                    start_day,
                                    weekday,
                                ) {
                                Some(departure) => departure,
                                None => continue,
                            };

                            let weight =
                                p.0.weight + (next_departure.arrival - p.0.time) as usize;
                            let estimated = weight
                                + self.nodes_distance(transit.destination, b) * 1000
                                    / params.estimator_speed;

                            pq_update(
                                &mut pq,
                                &mut origins,
                                transit.destination,
                                weight,
                                estimated,
                                next_departure.arrival,
                                id,
                                *neighbor,
                                Some(next_departure_index),
                            );
                        }
                    }
                }
            }
        }

        Err(async_graphql::Error::new("No plan found"))
    }

    fn reconstruct_path(
        &self,
        start_time: u32,
        start_day: u32,
        weekday: u8,
        origins: &HashMap<NodeID, AStarOrigins>,
        mut current: NodeID,
        params: RoutingParameters,
    ) -> async_graphql::Result<Vec<PlanLeg>> {
        let mut path: Vec<&AStarOrigins> = Vec::new();

        while let Some(next) = origins.get(&current) {
            path.push(next);
            current = next.destination;
        }

        path.reverse();

        if path.is_empty() {
            return Ok(Vec::new());
        }

        let mut legs = Vec::<PlanLeg>::new();
        let mut current: Option<PlanLeg> = None;

        for origin in path {
            match &origin.edge {
                EdgeData::Street(edge) => {
                    let from = PlanPlace {
                        node_id: edge.origin,
                        arrival: None,
                        departure: None,
                        stop_position: None,
                    };
                    let to = PlanPlace {
                        node_id: edge.destination,
                        arrival: Some(origin.time),
                        departure: Some(origin.time),
                        stop_position: None,
                    };

                    let step = PlanWalkLegStep {
                        length: edge.length,
                        time: (edge.length * 1000 / params.walking_speed) as u32,
                        place: to,
                    };

                    match current {
                        None => {
                            current = Some(PlanLeg::Walk(PlanWalkLeg {
                                steps: vec![PlanLegStep::Walk(step)],
                                from,
                                to,
                                length: edge.length,
                                start: start_time,
                                end: origin.time,
                                duration: origin.time - start_time,
                                geometry: vec![],
                            }));
                        }
                        Some(ref mut c) => match c {
                            PlanLeg::Walk(cw) => {
                                cw.steps.push(PlanLegStep::Walk(step));
                                cw.to = to;
                                cw.end += step.time;
                                cw.length += step.length;
                            }
                            PlanLeg::Transit(_) => {
                                legs.push(c.clone());
                                current = Some(PlanLeg::Walk(PlanWalkLeg {
                                    steps: vec![PlanLegStep::Walk(step)],
                                    from,
                                    to,
                                    length: edge.length,
                                    start: start_time,
                                    end: origin.time,
                                    duration: origin.time - start_time,
                                    geometry: vec![],
                                }));
                            }
                        },
                    }
                }
                EdgeData::Transit(edge) => {
                    let departure_index =
                        origin
                            .next_departure_index
                            .ok_or(async_graphql::Error::new(
                                "Found a transit edge without departure",
                            ))?;
                    let trip_segment = self.transit_departures[departure_index];

                    let from = PlanPlace {
                        node_id: edge.origin,
                        arrival: None,
                        departure: Some(trip_segment.departure),
                        stop_position: Some(trip_segment.origin_stop_sequence),
                    };
                    let to = PlanPlace {
                        node_id: edge.destination,
                        arrival: Some(trip_segment.arrival),
                        departure: None,
                        stop_position: Some(trip_segment.destination_stop_sequence),
                    };

                    let step = PlanTransitLegStep {
                        length: edge.length,
                        time: trip_segment.arrival - trip_segment.departure,
                        place: to,
                        date: start_day,
                        weekday,
                        timetable_segment: edge.timetable_segment,
                        departure_index,
                    };

                    match current {
                        None => {
                            current = Some(new_transit_leg(step, from, to, &trip_segment, edge.length));
                        }
                        Some(ref mut c) => match c {
                            PlanLeg::Transit(ct) => {
                                if ct.trip_id == trip_segment.trip_id {
                                    ct.steps.push(PlanLegStep::Transit(step));
                                    ct.to = to;
                                    ct.end = trip_segment.arrival;
                                    ct.length += edge.length;
                                } else {
                                    legs.push(c.clone());
                                    current = Some(new_transit_leg(step, from, to, &trip_segment, edge.length));
                                }
                            }
                            PlanLeg::Walk(_) => {
                                legs.push(c.clone());
                                current = Some(new_transit_leg(step, from, to, &trip_segment, edge.length));
                            }
                        },
                    }
                }
            }
        }

        if let Some(current) = current {
            legs.push(current);
        }

        Ok(legs)
    }
}
