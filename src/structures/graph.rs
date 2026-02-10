use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    usize,
};

use kdtree::{KdTree, distance::squared_euclidean};
use priority_queue::PriorityQueue;

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteInfo, ServicePattern, TimetableSegment, TripInfo, TripSegment,
    },
    structures::{EdgeData, LatLng, NodeData, NodeID, RoutingParameters},
};

#[derive(Debug)]
pub enum GraphError {
    NodeNotFoundError(NodeID),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AStarPriority {
    estimated_weight: usize,
    weight: usize,
    time: u32,
}

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
    ) {
        let mut pq = PriorityQueue::<NodeID, Reverse<AStarPriority>>::new();
        let mut origins = HashMap::<NodeID, (NodeID, EdgeData)>::new();
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
                None => return,
            };

            if id == b {
                println!("Found a path after visiting {} nodes!", visited.len());
                let path = Graph::reconstruct_path(origins, id);
                println!("Nodes: {}", path.len());
                let dist = path.iter().fold(0, |acc, e| {
                    acc + match e {
                        EdgeData::Street(e) => e.length,
                        EdgeData::Transit(e) => e.length,
                    }
                });
                println!("Length: {}", dist);
                println!("Duration: {}", p.0.time - start_time);
                let count_transit = path.iter().fold(0, |acc, e| {
                    acc + match e {
                        EdgeData::Street(_) => 0,
                        EdgeData::Transit(_) => 1,
                    }
                });
                println!("Took {} transits", count_transit);
                return;
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
                                        pq.change_priority(
                                            &street.destination,
                                            Reverse(AStarPriority {
                                                estimated_weight: weight
                                                    + self.nodes_distance(street.destination, b)
                                                        * 1000
                                                        / params.estimator_speed,
                                                weight,
                                                time: p.0.time
                                                    + (street.length * 1000 / params.walking_speed)
                                                        as u32,
                                            }),
                                        );
                                        origins.insert(street.destination, (id, neighbor.clone()));
                                    }
                                }
                                None => {
                                    pq.push(
                                        street.destination,
                                        Reverse(AStarPriority {
                                            estimated_weight: weight
                                                + self.nodes_distance(street.destination, b) * 1000
                                                    / params.estimator_speed,
                                            weight,
                                            time: p.0.time
                                                + (street.length * 1000 / params.walking_speed)
                                                    as u32,
                                        }),
                                    );
                                    origins.insert(street.destination, (id, neighbor.clone()));
                                }
                            }
                        }
                        EdgeData::Transit(transit) => {
                            if visited.contains(&transit.destination) {
                                continue;
                            }

                            let next_departure = match self.next_transit_departure(
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
                                        origins.insert(transit.destination, (id, neighbor.clone()));
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
                                    origins.insert(transit.destination, (id, neighbor.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }

        println!("Didn't found a path after visiting {} nodes", visited.len());
    }

    fn reconstruct_path(
        origins: HashMap<NodeID, (NodeID, EdgeData)>,
        mut current: NodeID,
    ) -> Vec<EdgeData> {
        let mut path = Vec::<EdgeData>::new();

        while let Some(next) = origins.get(&current) {
            path.push(next.1.clone());
            current = next.0;
        }

        return path;
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
    ) -> Option<&TripSegment> {
        let slice = &self.transit_departures[tt.start..tt.start + tt.len];

        let start_idx = slice.partition_point(|d| d.departure < time);

        for dep in &slice[start_idx..] {
            if self.transit_services[dep.service_id.0 as usize].is_active(date, weekday) {
                return Some(dep);
            }
        }

        None
    }
}
