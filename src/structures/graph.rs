use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet},
    usize,
};

use async_graphql::Result;
use gtfs_structures::RouteType;
use kdtree::{KdTree, distance::squared_euclidean};
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{
        AgencyId, AgencyInfo, RouteId, RouteInfo, ServicePattern, StopTime, TimetableSegment,
        TripId, TripInfo, TripSegment,
    },
    structures::{
        DelayCDF, EdgeData, LatLng, NodeData, NodeID, RoutingParameters, ScenarioBag,
        meters_to_degrees,
        plan::{
            Plan, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg, PlanTransitLegStep, PlanWalkLeg,
            PlanWalkLegStep,
        },
        raptor::{Lookup, PatternID, PatternInfo, Trace},
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
    transit_patterns: Vec<PatternInfo>,

    transit_pattern_stops: Vec<NodeID>,
    transit_stop_patterns: Vec<(PatternID, u32)>,
    transit_stop_transfers: Vec<(NodeID, u32)>,
    transit_pattern_stop_times: Vec<StopTime>,
    transit_pattern_trips: Vec<TripId>,

    transit_idx_pattern_stops: Vec<Lookup>,
    transit_idx_stop_patterns: Vec<Lookup>,
    transit_idx_stop_transfers: Vec<Lookup>,
    transit_idx_pattern_stop_times: Vec<Lookup>,
    transit_idx_pattern_trips: Vec<Lookup>,

    transit_delay_models: HashMap<RouteType, DelayCDF>,

    transit_node_to_stop: Vec<u32>,
    transit_stop_to_node: Vec<NodeID>,

    transit_stops_tree: KdTree<f64, usize, [f64; 2]>,
}

static MAX_TRANSFER_DISTANCE_M: f64 = 1000.0;
static WALKING_SPEED_MS: f64 = 1.2;
pub const MAX_SCENARIOS: usize = 1;
pub const MAX_ROUNDS: usize = 6;
static MAX_ACCESS_DISTANCE_M: f64 = 5000.0;

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
            transit_patterns: Vec::<PatternInfo>::new(),

            transit_pattern_stops: Vec::<NodeID>::new(),
            transit_stop_patterns: Vec::<(PatternID, u32)>::new(),
            transit_stop_transfers: Vec::<(NodeID, u32)>::new(),
            transit_pattern_stop_times: Vec::new(),
            transit_pattern_trips: Vec::new(),

            transit_idx_pattern_stops: Vec::<Lookup>::new(),
            transit_idx_stop_patterns: Vec::<Lookup>::new(),
            transit_idx_stop_transfers: Vec::<Lookup>::new(),
            transit_idx_pattern_stop_times: Vec::new(),
            transit_idx_pattern_trips: Vec::new(),

            transit_delay_models: HashMap::new(),

            transit_node_to_stop: Vec::new(),
            transit_stop_to_node: Vec::new(),

            transit_stops_tree: KdTree::new(2),
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
                let legs =
                    self.reconstruct_path(start_time, start_day, weekday, &origins, id, params)?;
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

    // RAPTOR

    pub fn push_transit_pattern(&mut self, p: PatternInfo) {
        self.transit_patterns.push(p);
    }

    pub fn transit_pattern_stops_len(&self) -> usize {
        self.transit_pattern_stops.len()
    }
    pub fn extend_transit_pattern_stops(&mut self, s: &[NodeID]) {
        self.transit_pattern_stops.extend_from_slice(s);
    }
    pub fn push_transit_idx_pattern_stops(&mut self, l: Lookup) {
        self.transit_idx_pattern_stops.push(l);
    }

    pub fn transit_pattern_trips_len(&self) -> usize {
        self.transit_pattern_trips.len()
    }
    pub fn push_transit_pattern_trip(&mut self, t: TripId) {
        self.transit_pattern_trips.push(t);
    }
    pub fn push_transit_idx_pattern_trips(&mut self, l: Lookup) {
        self.transit_idx_pattern_trips.push(l);
    }

    pub fn transit_pattern_stop_times_len(&self) -> usize {
        self.transit_pattern_stop_times.len()
    }
    pub fn push_transit_pattern_stop_time(&mut self, st: StopTime) {
        self.transit_pattern_stop_times.push(st);
    }
    pub fn push_transit_idx_pattern_stop_times(&mut self, l: Lookup) {
        self.transit_idx_pattern_stop_times.push(l);
    }

    pub fn build_raptor_index(&mut self) {
        self.build_compact_stop_index();
        self.build_stop_patterns();
        self.build_stop_transfers();
    }

    fn build_compact_stop_index(&mut self) {
        self.transit_node_to_stop = vec![u32::MAX; self.nodes.len()];
        self.transit_stop_to_node.clear();
        self.transit_stops_tree = KdTree::new(2);

        for (i, node) in self.nodes.iter().enumerate() {
            if matches!(node, NodeData::TransitStop(_)) {
                let compact = self.transit_stop_to_node.len();
                self.transit_node_to_stop[i] = compact as u32;
                self.transit_stop_to_node.push(NodeID(i));
                let loc = node.loc();
                let _ = self
                    .transit_stops_tree
                    .add([loc.latitude, loc.longitude], compact);
            }
        }
    }

    fn build_stop_patterns(&mut self) {
        let n_stops = self.transit_stop_to_node.len();
        let mut per_stop: Vec<Vec<(PatternID, u32)>> = vec![Vec::new(); n_stops];

        for (p, lookup) in self.transit_idx_pattern_stops.iter().enumerate() {
            let stops = lookup.of(&self.transit_pattern_stops);
            for (pos, &node_id) in stops.iter().enumerate() {
                let compact = self.transit_node_to_stop[node_id.0];
                if compact == u32::MAX {
                    continue;
                }
                per_stop[compact as usize].push((PatternID(p as u32), pos as u32));
            }
        }

        self.transit_stop_patterns.clear();
        self.transit_idx_stop_patterns = Vec::with_capacity(n_stops);

        for pairs in &per_stop {
            let start = self.transit_stop_patterns.len();
            self.transit_stop_patterns.extend_from_slice(pairs);
            self.transit_idx_stop_patterns.push(Lookup {
                start,
                len: pairs.len(),
            });
        }
    }

    fn build_stop_transfers(&mut self) {
        let n_stops = self.transit_stop_to_node.len();
        self.transit_stop_transfers.clear();
        self.transit_idx_stop_transfers = Vec::with_capacity(n_stops);

        let max_walk_secs = (MAX_TRANSFER_DISTANCE_M / WALKING_SPEED_MS) as u32;

        for i in 0..n_stops {
            let start = self.transit_stop_transfers.len();
            let stop_node = self.transit_stop_to_node[i];
            let loc = self.nodes[stop_node.0].loc();

            let origin_osm = match self.nearest_node(loc.latitude, loc.longitude) {
                Some(n) => n,
                None => {
                    self.transit_idx_stop_transfers
                        .push(Lookup { start, len: 0 });
                    continue;
                }
            };

            let walk_times = self.walk_dijkstra(origin_osm, max_walk_secs);

            let nearby = self
                .transit_stops_tree
                .within(
                    &[loc.latitude, loc.longitude],
                    meters_to_degrees(MAX_TRANSFER_DISTANCE_M),
                    &squared_euclidean,
                )
                .unwrap_or_default();

            for &(_, &compact_neighbor) in &nearby {
                if compact_neighbor == i {
                    continue;
                }
                let neighbor_node = self.transit_stop_to_node[compact_neighbor];
                if let Some(&walk_secs) = walk_times.get(&neighbor_node) {
                    self.transit_stop_transfers.push((neighbor_node, walk_secs));
                }
            }

            self.transit_idx_stop_transfers.push(Lookup {
                start,
                len: self.transit_stop_transfers.len() - start,
            });
        }
    }

    pub fn raptor(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<Plan> {
        let n_stops = self.transit_stop_to_node.len();
        let n_patterns = self.transit_patterns.len();

        let mut best = vec![ScenarioBag::EMPTY; n_stops];
        let mut labels = vec![vec![ScenarioBag::EMPTY; n_stops]; MAX_ROUNDS + 1];
        let mut traces = vec![vec![Trace::NONE; n_stops]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_stops];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];

        for &(stop, time) in sources {
            let bag = ScenarioBag::single(time);
            labels[0][stop] = bag;
            best[stop] = bag;
            Self::mark(stop, &mut marked, &mut is_marked);
        }

        self.apply_transfers(
            &mut labels[0],
            &mut best,
            &mut traces[0],
            &mut marked,
            &mut is_marked,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                rest[0].copy_from_slice(&prev[k - 1]);
            }

            self.collect_routes(&marked, &mut queue, &mut queue_pos);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(&best, targets);

            {
                let (prev, rest) = labels.split_at_mut(k);
                let prev = &prev[k - 1];
                let curr = &mut rest[0];

                for &pat in &queue {
                    self.scan_route(
                        pat,
                        queue_pos[pat],
                        date,
                        weekday,
                        cutoff,
                        prev,
                        curr,
                        &mut best,
                        &mut traces[k],
                        &mut marked,
                        &mut is_marked,
                    );
                }
            }

            for &p in &queue {
                queue_pos[p] = u32::MAX;
            }
            queue.clear();

            self.apply_transfers(
                &mut labels[k],
                &mut best,
                &mut traces[k],
                &mut marked,
                &mut is_marked,
            );

            if marked.is_empty() {
                break;
            }
        }

        self.extract(
            sources, targets, start_time, date, weekday, &labels, &traces,
        )
    }

    fn collect_routes(&self, marked: &[usize], queue: &mut Vec<usize>, queue_pos: &mut [u32]) {
        for &stop in marked {
            let pats = self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
            for &(pat_id, pos) in pats {
                let p = pat_id.0 as usize;
                if queue_pos[p] == u32::MAX {
                    queue.push(p);
                }
                queue_pos[p] = queue_pos[p].min(pos);
            }
        }
    }

    fn scan_route(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: u32,
        prev: &[ScenarioBag],
        curr: &mut [ScenarioBag],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
    ) {
        let pat_stops = self.transit_idx_pattern_stops[pattern].of(&self.transit_pattern_stops);
        let n_trips = self.transit_patterns[pattern].num_trips as usize;
        if n_trips == 0 {
            return;
        }

        let all_times =
            self.transit_idx_pattern_stop_times[pattern].of(&self.transit_pattern_stop_times);
        let trip_ids = self.transit_idx_pattern_trips[pattern].of(&self.transit_pattern_trips);

        let mut boarded: Option<(usize, u32)> = None;

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            if let Some((t, bp)) = boarded {
                let arr = col[t].arrival;
                if arr < cutoff {
                    let bag = ScenarioBag::single(arr);
                    if bag.improves_on(&best[stop]) {
                        curr[stop].try_improve(&bag);
                        best[stop].try_improve(&bag);
                        traces[stop] = Trace {
                            pattern: pattern as u32,
                            trip: t as u32,
                            boarded_at: bp,
                            alighted_at: pos as u32,
                            from_stop: u32::MAX,
                        };
                        Self::mark(stop, marked, is_marked);
                    }
                }
            }

            if prev[stop].is_reached() {
                let min_dep = prev[stop].earliest();
                let t_start = col.partition_point(|st| st.departure < min_dep);
                for t in t_start..n_trips {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        if boarded.map_or(true, |(ct, _)| t < ct) {
                            boarded = Some((t, pos as u32));
                        }
                        break;
                    }
                }
            }
        }
    }

    fn apply_transfers(
        &self,
        labels: &mut [ScenarioBag],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
    ) {
        let n = marked.len();
        for i in 0..n {
            let stop = marked[i];
            let time = labels[stop].earliest();
            if time == u32::MAX {
                continue;
            }

            let transfers = self.transit_idx_stop_transfers[stop].of(&self.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.transit_node_to_stop[target_node.0] as usize;
                let bag = ScenarioBag::single(time + walk);

                if bag.improves_on(&best[target]) {
                    labels[target].try_improve(&bag);
                    best[target].try_improve(&bag);
                    traces[target] = Trace {
                        pattern: u32::MAX,
                        trip: u32::MAX,
                        boarded_at: u32::MAX,
                        alighted_at: u32::MAX,
                        from_stop: stop as u32,
                    };
                    Self::mark(target, marked, is_marked);
                }
            }
        }
    }

    fn extract(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        traces: &[Vec<Trace>],
    ) -> Vec<Plan> {
        let mut results = Vec::new();
        let mut pareto_best = u32::MAX;

        for k in 0..=MAX_ROUNDS {
            let mut best_arr = u32::MAX;
            let mut best_stop = 0usize;
            let mut best_walk = 0u32;

            for &(s, w) in targets {
                let total = labels[k][s].earliest().saturating_add(w);
                if total < best_arr {
                    best_arr = total;
                    best_stop = s;
                    best_walk = w;
                }
            }

            if best_arr >= pareto_best {
                continue;
            }

            pareto_best = best_arr;

            let (mut legs, origin_stop) =
                self.reconstruct(k, best_stop, date, weekday, labels, traces);

            if legs.is_empty() {
                continue;
            }

            if let Some(&(_, stop_arrival)) = sources.iter().find(|&&(s, _)| s == origin_stop) {
                let first_walk = stop_arrival.saturating_sub(start_time);
                if first_walk > 0 {
                    let stop_node = self.transit_stop_to_node[origin_stop];
                    let length = (first_walk as f64 * WALKING_SPEED_MS) as usize;
                    let to_place = PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: Some(stop_arrival),
                        departure: None,
                    };
                    legs.insert(
                        0,
                        PlanLeg::Walk(PlanWalkLeg {
                            from: PlanPlace {
                                node_id: stop_node,
                                stop_position: None,
                                arrival: None,
                                departure: Some(start_time),
                            },
                            to: to_place,
                            start: start_time,
                            end: stop_arrival,
                            duration: first_walk,
                            length,
                            steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                                length,
                                time: first_walk,
                                place: to_place,
                            })],
                        }),
                    );
                }
            }

            if best_walk > 0 {
                let walk_start = labels[k][best_stop].earliest();
                let stop_node = self.transit_stop_to_node[best_stop];
                let length = (best_walk as f64 * WALKING_SPEED_MS) as usize;
                let to_place = PlanPlace {
                    node_id: stop_node,
                    stop_position: None,
                    arrival: Some(best_arr),
                    departure: None,
                };
                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: None,
                        departure: Some(walk_start),
                    },
                    to: to_place,
                    start: walk_start,
                    end: best_arr,
                    duration: best_walk,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: best_walk,
                        place: to_place,
                    })],
                }));
            }

            let departure = legs
                .first()
                .map(|l| match l {
                    PlanLeg::Walk(w) => w.start,
                    PlanLeg::Transit(t) => t.start,
                })
                .unwrap_or(start_time);

            results.push(Plan {
                legs,
                start: departure,
                end: best_arr,
            });
        }

        results
    }

    fn reconstruct(
        &self,
        round: usize,
        target_stop: usize,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        traces: &[Vec<Trace>],
    ) -> (Vec<PlanLeg>, usize) {
        let mut legs = Vec::new();
        let mut stop = target_stop;
        let mut k = round;

        loop {
            let trace = traces[k][stop];
            if !trace.is_transit() && !trace.is_transfer() {
                break;
            }

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let start = labels[k][from].earliest();
                let end = labels[k][stop].earliest();
                let duration = end - start;
                let from_node = self.transit_stop_to_node[from];
                let to_node = self.transit_stop_to_node[stop];
                let length = (duration as f64 * WALKING_SPEED_MS) as usize;

                let to_place = PlanPlace {
                    stop_position: None,
                    arrival: Some(end),
                    departure: None,
                    node_id: to_node,
                };

                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        stop_position: None,
                        arrival: None,
                        departure: Some(start),
                        node_id: from_node,
                    },
                    to: to_place,
                    start,
                    end,
                    duration,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: duration,
                        place: to_place,
                    })],
                }));
                stop = from;
                continue;
            }

            let p = trace.pattern as usize;
            let t = trace.trip as usize;
            let bp = trace.boarded_at as usize;
            let ap = trace.alighted_at as usize;

            let pat_stops = self.transit_idx_pattern_stops[p].of(&self.transit_pattern_stops);
            let n_trips = self.transit_patterns[p].num_trips as usize;
            let times = self.transit_idx_pattern_stop_times[p].of(&self.transit_pattern_stop_times);
            let trip_ids = self.transit_idx_pattern_trips[p].of(&self.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let mut steps = Vec::with_capacity(ap - bp);
            let mut total_length = 0usize;
            for s in (bp + 1)..=ap {
                let seg_len = self.nodes_distance(pat_stops[s - 1], pat_stops[s]);
                total_length += seg_len;

                let arr = times[s * n_trips + t].arrival;
                let prev_dep = times[(s - 1) * n_trips + t].departure;

                steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    length: seg_len,
                    time: arr - prev_dep,
                    place: PlanPlace {
                        node_id: pat_stops[s],
                        stop_position: Some(s as u32),
                        arrival: Some(arr),
                        departure: if s < ap {
                            Some(times[s * n_trips + t].departure)
                        } else {
                            None
                        },
                    },
                    date,
                    weekday,
                    timetable_segment: TimetableSegment { start: 0, len: 0 },
                    departure_index: 0,
                }));
            }

            legs.push(PlanLeg::Transit(PlanTransitLeg {
                from: PlanPlace {
                    stop_position: Some(bp as u32),
                    arrival: None,
                    departure: Some(board_dep),
                    node_id: pat_stops[bp],
                },
                to: PlanPlace {
                    stop_position: Some(ap as u32),
                    arrival: Some(alight_arr),
                    departure: None,
                    node_id: pat_stops[ap],
                },
                start: board_dep,
                end: alight_arr,
                trip_id: trip_ids[t],
                length: total_length,
                duration: alight_arr - board_dep,
                steps,
            }));

            stop = self.transit_node_to_stop[pat_stops[bp].0] as usize;
            k -= 1;
        }

        legs.reverse();
        (legs, stop)
    }

    #[inline]
    fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.transit_trips[trip_id.0 as usize].service_id;
        self.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    #[inline]
    fn target_cutoff(best: &[ScenarioBag], targets: &[(usize, u32)]) -> u32 {
        targets
            .iter()
            .map(|&(s, w)| best[s].earliest().saturating_add(w))
            .min()
            .unwrap_or(u32::MAX)
    }

    #[inline]
    fn mark(stop: usize, marked: &mut Vec<usize>, is_marked: &mut [bool]) {
        if !is_marked[stop] {
            is_marked[stop] = true;
            marked.push(stop);
        }
    }

    fn walk_dijkstra(&self, origin: NodeID, max_seconds: u32) -> HashMap<NodeID, u32> {
        const WALK_MMS: u32 = (WALKING_SPEED_MS * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }

            if self.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }

            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / WALK_MMS as u64) as u32;
                            let nd = d.saturating_add(t);
                            if nd <= max_seconds {
                                let entry = dist.entry(street.destination).or_insert(u32::MAX);
                                if nd < *entry {
                                    *entry = nd;
                                    pq.push(Reverse((nd, street.destination)));
                                }
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                            }
                        }
                    }
                }
            }
        }

        dist
    }

    pub fn nearby_stops(&self, lat: f64, lng: f64) -> Vec<(usize, u32)> {
        let nearby = self
            .transit_stops_tree
            .within(
                &[lat, lng],
                meters_to_degrees(MAX_ACCESS_DISTANCE_M),
                &squared_euclidean,
            )
            .unwrap_or_default();

        if nearby.is_empty() {
            return Vec::new();
        }

        let origin_node = match self.nearest_node(lat, lng) {
            Some(n) => n,
            None => return Vec::new(),
        };

        let max_walk_secs = (MAX_ACCESS_DISTANCE_M / WALKING_SPEED_MS) as u32;
        let walk_times = self.walk_dijkstra(origin_node, max_walk_secs);

        let mut stops = Vec::new();
        for &(_, &compact) in &nearby {
            let stop_node = self.transit_stop_to_node[compact];
            if let Some(&walk_secs) = walk_times.get(&stop_node) {
                stops.push((compact, walk_secs));
            }
        }
        stops
    }
}
