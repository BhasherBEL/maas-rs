use std::collections::HashMap;

use kdtree::{KdTree, distance::squared_euclidean};
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{AgencyId, AgencyInfo, RouteId, RouteInfo, TripId, TripInfo},
    structures::{EdgeData, LatLng, NodeData, NodeID},
};

use raptor_index::RaptorIndex;

mod raptor_access;
mod raptor_backward;
mod raptor_build;
mod raptor_index;
mod raptor_plan;
mod raptor_route;
mod railway;
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
    pub raptor: RaptorIndex,
}

pub static MAX_TRANSFER_DISTANCE_M: f64 = 1000.0;
pub const MAX_SCENARIOS: usize = 2;
pub const MAX_ROUNDS: usize = 20;

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_tree: KdTree::new(2),
            id_mapper: HashMap::new(),
            raptor: RaptorIndex::new(),
        }
    }

    pub fn set_min_access_secs(&mut self, secs: u32) {
        self.raptor.min_access_secs = secs;
    }

    pub fn set_walking_speed_mps(&mut self, mps: f64) {
        self.raptor.walking_speed_mps = mps;
    }

    pub fn add_node(&mut self, node: NodeData) -> NodeID {
        let id = NodeID(self.nodes.len());

        if let NodeData::OsmNode(ref osm_node) = node {
            let _ = self.nodes_tree.add([osm_node.lat_lng.latitude, osm_node.lat_lng.longitude], id);
            self.id_mapper.insert(osm_node.eid.clone(), id);
        }

        self.nodes.push(node);
        self.edges.push(Vec::new());
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
        self.raptor.transit_trips.get(id.0 as usize)
    }

    pub fn get_route(&self, id: RouteId) -> Option<&RouteInfo> {
        self.raptor.transit_routes.get(id.0 as usize)
    }

    pub fn get_agency(&self, id: AgencyId) -> Option<&AgencyInfo> {
        self.raptor.transit_agencies.get(id.0 as usize)
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Finds the nearest OSM node using squared Euclidean distance in the
    /// lat/lon plane. Fast but not metrically accurate — suitable for finding
    /// the closest node when the exact distance doesn't matter.
    /// See also: `nearest_node_dist` which returns the Haversine distance in meters.
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
                tracing::warn!("KD-tree query failed (empty tree?)");
                None
            }
        }
    }

    /// Finds the nearest OSM node and returns the Haversine distance in meters.
    /// More expensive than `nearest_node` but gives an accurate metric distance,
    /// needed when the actual distance matters (e.g. GTFS stop snapping).
    pub fn nearest_node_dist(&self, lat: f64, lon: f64) -> Option<(f64, &NodeID)> {
        match self.nodes_tree.iter_nearest(&[lat, lon], &LatLng::distance) {
            Ok(mut it) => match it.next() {
                Some(v) => Some(v),
                None => None,
            },
            Err(_) => {
                tracing::warn!("KD-tree query failed (empty tree?)");
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
