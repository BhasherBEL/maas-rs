use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    usize,
};

use async_graphql::Result;
use kdtree::{KdTree, distance::squared_euclidean};
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{
        AgencyId, AgencyInfo, RouteId, RouteInfo, ServicePattern, TimetableSegment, TripId,
        TripInfo, TripSegment,
    },
    structures::{
        EdgeData, LatLng, NodeData, NodeID, RoutingParameters,
        plan::{
            Plan, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg, PlanTransitLegStep, PlanWalkLeg,
            PlanWalkLegStep,
        },
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub enum GraphError {
    NodeNotFoundError(NodeID),
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Graph {
    nodes: Vec<NodeData>,
    edges: Vec<Vec<EdgeData>>,
    nodes_tree: KdTree<f64, NodeID, [f64; 2]>,
    id_mapper: HashMap<String, NodeID>,
    transit_departures: Vec<TripSegment>,
    transit_services: Vec<ServicePattern>,
    transit_trips: Vec<TripInfo>,
    transit_routes: Vec<RouteInfo>,
    transit_agencies: Vec<AgencyInfo>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_tree: KdTree::new(2),
            id_mapper: HashMap::new(),
            transit_departures: Vec::<TripSegment>::new(),
            transit_services: Vec::<ServicePattern>::new(),
            transit_trips: Vec::<TripInfo>::new(),
            transit_routes: Vec::<RouteInfo>::new(),
            transit_agencies: Vec::<AgencyInfo>::new(),
        }
    }

    pub fn add_node(&mut self, node: NodeData) -> NodeID {
        let id = NodeID(self.nodes.len());

        self.nodes.push(node.clone());
        self.edges.push(Vec::new());

        match node {
            NodeData::OsmNode(osm_node) => {
                let lat = osm_node.lat_lng.latitude;
                let lon = osm_node.lat_lng.longitude;
                let eid = osm_node.eid.clone();

                let _ = self.nodes_tree.add([lat, lon], id);
                self.id_mapper.insert(eid, id);
            }
            _ => {}
        }
        id
    }

    pub fn add_edge(&mut self, from: NodeID, edge: EdgeData) {
        self.edges[from.0].push(edge);
    }

    pub fn get_id(&self, eid: &str) -> Option<&NodeID> {
        self.id_mapper.get(eid)
    }

    pub fn get_node(&self, id: NodeID) -> Option<&NodeData> {
        self.nodes.get(id.0)
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn get_trip(&self, id: TripId) -> Option<&TripInfo> {
        self.transit_trips.get(id.0 as usize)
    }

    pub fn get_route(&self, id: RouteId) -> Option<&RouteInfo> {
        self.transit_routes.get(id.0 as usize)
    }

    pub fn get_agency(&self, id: AgencyId) -> Option<&AgencyInfo> {
        self.transit_agencies.get(id.0 as usize)
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn nearest_node(&self, lat: f64, lon: f64) -> Option<NodeID> {
        match self
            .nodes_tree
            .iter_nearest(&[lat, lon], &squared_euclidean)
        {
            Ok(mut it) => match it.next() {
                Some(v) => Some(*v.1),
                None => None,
            },
            Err(_) => {
                eprintln!("Failed to find a close node");
                None
            }
        }
    }

    pub fn nearest_node_dist(&self, lat: f64, lon: f64) -> Option<(f64, &NodeID)> {
        match self.nodes_tree.iter_nearest(&[lat, lon], &LatLng::distance) {
            Ok(mut it) => match it.next() {
                Some(v) => return Some(v),
                None => None,
            },
            Err(_) => {
                eprintln!("Failed to find a close node");
                None
            }
        }
    }

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
                estimated_weight: 0 + self.nodes_distance(a, b) * 1000 / params.estimator_speed,
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
                let legs = self.reconstruct_path(start_time, start_day, weekday, &origins, id)?;
                return Ok(Plan {
                    start: start_time,
                    end: p.0.time,
                    legs,
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

                            match pq.get_priority(&street.destination) {
                                Some(current) => {
                                    if current.0.weight > weight {
                                        let time = p.0.time
                                            + (street.length * 1000 / params.walking_speed) as u32;
                                        pq.change_priority(
                                            &street.destination,
                                            Reverse(AStarPriority {
                                                estimated_weight: weight
                                                    + self.nodes_distance(street.destination, b)
                                                        * 1000
                                                        / params.estimator_speed,
                                                weight,
                                                time,
                                            }),
                                        );
                                        origins.insert(
                                            street.destination,
                                            AStarOrigins {
                                                destination: id,
                                                edge: neighbor.clone(),
                                                next_departure_index: None,
                                                time,
                                            },
                                        );
                                    }
                                }
                                None => {
                                    let time = p.0.time
                                        + (street.length * 1000 / params.walking_speed) as u32;
                                    pq.push(
                                        street.destination,
                                        Reverse(AStarPriority {
                                            estimated_weight: weight
                                                + self.nodes_distance(street.destination, b) * 1000
                                                    / params.estimator_speed,
                                            weight,
                                            time,
                                        }),
                                    );
                                    origins.insert(
                                        street.destination,
                                        AStarOrigins {
                                            destination: id,
                                            edge: neighbor.clone(),
                                            next_departure_index: None,
                                            time,
                                        },
                                    );
                                }
                            }
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

                            let edge_weight = next_departure.arrival - p.0.time;

                            let weight = p.0.weight + edge_weight as usize;

                            match pq.get_priority(&transit.destination) {
                                Some(current) => {
                                    if current.0.weight > weight {
                                        pq.change_priority(
                                            &transit.destination,
                                            Reverse(AStarPriority {
                                                estimated_weight: weight
                                                    + self.nodes_distance(transit.destination, b)
                                                        * 1000
                                                        / params.estimator_speed,
                                                weight,
                                                time: next_departure.arrival,
                                            }),
                                        );
                                        origins.insert(
                                            transit.destination,
                                            AStarOrigins {
                                                destination: id,
                                                edge: neighbor.clone(),
                                                next_departure_index: Some(next_departure_index),
                                                time: next_departure.arrival,
                                            },
                                        );
                                    }
                                }
                                None => {
                                    pq.push(
                                        transit.destination,
                                        Reverse(AStarPriority {
                                            estimated_weight: weight
                                                + self.nodes_distance(transit.destination, b)
                                                    * 1000
                                                    / params.estimator_speed,
                                            weight,
                                            time: next_departure.arrival,
                                        }),
                                    );
                                    origins.insert(
                                        transit.destination,
                                        AStarOrigins {
                                            destination: id,
                                            edge: neighbor.clone(),
                                            next_departure_index: Some(next_departure_index),
                                            time: next_departure.arrival,
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        return Err(async_graphql::Error::new("No plan found"));
    }

    fn reconstruct_path(
        &self,
        start_time: u32,
        start_day: u32,
        weekday: u8,
        origins: &HashMap<NodeID, AStarOrigins>,
        mut current: NodeID,
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
                        time: 0,
                        place: to,
                    };

                    match current {
                        None => {
                            let mut steps = Vec::<PlanLegStep>::new();
                            steps.push(PlanLegStep::Walk(step));
                            current = Some(PlanLeg::Walk(PlanWalkLeg {
                                steps,
                                from,
                                to,
                                length: edge.length,
                                start: start_time,
                                end: origin.time,
                                duration: origin.time - start_time,
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
                                let mut steps = Vec::<PlanLegStep>::new();
                                steps.push(PlanLegStep::Walk(step));
                                current = Some(PlanLeg::Walk(PlanWalkLeg {
                                    steps,
                                    from,
                                    to,
                                    length: edge.length,
                                    start: start_time,
                                    end: origin.time,
                                    duration: origin.time - start_time,
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
                            let mut steps = Vec::<PlanLegStep>::new();
                            steps.push(PlanLegStep::Transit(step));
                            current = Some(PlanLeg::Transit(PlanTransitLeg {
                                steps,
                                from,
                                to,
                                length: edge.length,
                                start: trip_segment.departure,
                                end: trip_segment.arrival,
                                duration: trip_segment.arrival - trip_segment.departure,
                                trip_id: trip_segment.trip_id,
                            }));
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
                                    let mut steps = Vec::<PlanLegStep>::new();
                                    steps.push(PlanLegStep::Transit(step));
                                    current = Some(PlanLeg::Transit(PlanTransitLeg {
                                        steps,
                                        from,
                                        to,
                                        length: edge.length,
                                        start: trip_segment.departure,
                                        end: trip_segment.arrival,
                                        duration: trip_segment.arrival - trip_segment.departure,
                                        trip_id: trip_segment.trip_id,
                                    }));
                                }
                            }
                            PlanLeg::Walk(_) => {
                                legs.push(c.clone());
                                let mut steps = Vec::<PlanLegStep>::new();
                                steps.push(PlanLegStep::Transit(step));
                                current = Some(PlanLeg::Transit(PlanTransitLeg {
                                    steps,
                                    from,
                                    to,
                                    length: edge.length,
                                    start: trip_segment.departure,
                                    end: trip_segment.arrival,
                                    duration: trip_segment.arrival - trip_segment.departure,
                                    trip_id: trip_segment.trip_id,
                                }));
                            }
                        },
                    }
                }
            }
        }

        match current {
            Some(current) => legs.push(current),
            None => {}
        }

        Ok(legs)
    }

    pub fn nodes_distance(&self, a: NodeID, b: NodeID) -> usize {
        let node_a = &self.nodes[a.0];
        let node_b = &self.nodes[b.0];

        (node_a.loc().dist(node_b.loc()) * 0.99) as usize
    }

    pub fn get_transit_departures_size(&self) -> usize {
        self.transit_departures.len()
    }

    pub fn add_transit_departures(&mut self, segments: Vec<TripSegment>) {
        self.transit_departures.extend(segments);
    }

    pub fn get_transit_services_size(&self) -> usize {
        self.transit_services.len()
    }

    pub fn add_transit_services(&mut self, services: Vec<ServicePattern>) {
        self.transit_services.extend(services);
    }

    pub fn get_transit_trips_size(&self) -> usize {
        self.transit_trips.len()
    }

    pub fn add_transit_trips(&mut self, trips: Vec<TripInfo>) {
        self.transit_trips.extend(trips);
    }

    pub fn get_transit_routes_size(&self) -> usize {
        self.transit_routes.len()
    }

    pub fn add_transit_routes(&mut self, routes: Vec<RouteInfo>) {
        self.transit_routes.extend(routes);
    }

    pub fn get_transit_agencies_size(&self) -> usize {
        self.transit_agencies.len()
    }

    pub fn add_transit_agencies(&mut self, agencies: Vec<AgencyInfo>) {
        self.transit_agencies.extend(agencies);
    }

    pub fn next_transit_departure(
        &self,
        tt: TimetableSegment,
        time: u32,
        date: u32,
        weekday: u8,
    ) -> Option<(usize, &TripSegment)> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];

        let start_idx = slice.partition_point(|d| d.departure < time);

        for (i, dep) in slice[start_idx..].iter().enumerate() {
            if self.transit_services[dep.service_id.0 as usize].is_active(date, weekday) {
                return Some((tt.start + start_idx + i, dep));
            }
        }

        None
    }

    pub fn get_transit_departure_slice(&self, tt: TimetableSegment) -> &[TripSegment] {
        &self.transit_departures[tt.start..tt.start + tt.len]
    }

    pub fn previous_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];
        let relative_index = initial_index - tt.start;

        debug_assert!(
            initial_index >= tt.start && initial_index < tt.start + tt.len,
            "initial_index {} out of timetable segment [{}, {}]",
            initial_index,
            tt.start,
            tt.start + tt.len
        );

        slice[..relative_index]
            .iter()
            .rev()
            .enumerate()
            .filter(move |(_, dep)| {
                self.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (initial_index - 1 - i, dep))
    }

    pub fn next_departures(
        &self,
        tt: TimetableSegment,
        date: u32,
        weekday: u8,
        initial_index: usize,
    ) -> impl Iterator<Item = (usize, &TripSegment)> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];
        let relative_index = initial_index - tt.start;

        debug_assert!(
            initial_index >= tt.start && initial_index < tt.start + tt.len,
            "initial_index {} out of timetable segment [{}, {}]",
            initial_index,
            tt.start,
            tt.start + tt.len
        );

        slice[relative_index + 1..]
            .iter()
            .enumerate()
            .filter(move |(_, dep)| {
                self.transit_services[dep.service_id.0 as usize].is_active(date, weekday)
            })
            .map(move |(i, dep)| (initial_index + 1 + i, dep))
    }
}
