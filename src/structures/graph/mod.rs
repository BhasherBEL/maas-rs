use std::collections::HashMap;

use gtfs_structures::RouteType;
use kdtree::{KdTree, distance::squared_euclidean};
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{
        AgencyId, AgencyInfo, RouteId, RouteInfo, ServicePattern, StopTime, TripId, TripInfo,
        TripSegment,
    },
    structures::{
        DelayCDF, EdgeData, LatLng, NodeData, NodeID,
        raptor::{Lookup, PatternID, PatternInfo},
    },
};

mod astar;
mod raptor_build;
mod raptor_route;
mod transit;

#[derive(Debug, Serialize, Deserialize)]
pub enum GraphError {
    NodeNotFoundError(NodeID),
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

    /// Minimum walk-radius for access/egress stop discovery (seconds).
    /// Stored in the graph so config.yaml changes take effect after a rebuild.
    /// Defaults to 600 (10 min) when absent from a serialized graph.
    #[serde(default = "Graph::default_min_access_secs")]
    pub min_access_secs: u32,
}

static MAX_TRANSFER_DISTANCE_M: f64 = 1000.0;
static WALKING_SPEED_MS: f64 = 1.2;
pub const MAX_SCENARIOS: usize = 2;
pub const MAX_ROUNDS: usize = 20;

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

            min_access_secs: Self::default_min_access_secs(),
        }
    }

    fn default_min_access_secs() -> u32 {
        10 * 60
    }

    pub fn set_min_access_secs(&mut self, secs: u32) {
        self.min_access_secs = secs;
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

    pub fn nodes_distance(&self, a: NodeID, b: NodeID) -> usize {
        let node_a = &self.nodes[a.0];
        let node_b = &self.nodes[b.0];

        (node_a.loc().dist(node_b.loc()) * 0.99) as usize
    }
}
