use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    usize,
};

use kdtree::{KdTree, distance::squared_euclidean};
use priority_queue::PriorityQueue;

use crate::structures::{EdgeData, LatLng, NodeData, NodeID};

#[derive(Debug)]
pub enum GraphError {
    NodeNotFoundError(NodeID),
}

pub struct Graph {
    nodes: Vec<NodeData>,
    edges: Vec<Vec<EdgeData>>,
    nodes_tree: KdTree<f64, NodeID, [f64; 2]>,
    id_mapper: HashMap<String, NodeID>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_tree: KdTree::new(2),
            id_mapper: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: NodeData) {
        let id = NodeID(self.nodes.len());

        let lat = node.lat_lng.latitude;
        let lon = node.lat_lng.longitude;
        let eid = node.eid.clone();

        self.nodes.push(node);
        self.edges.push(Vec::new());
        let _ = self.nodes_tree.add([lat, lon], id);

        self.id_mapper.insert(eid, id);
    }

    pub fn add_edge(&mut self, from: NodeID, edge: EdgeData) {
        self.edges[from.0].push(edge);
    }

    pub fn get_id(&self, eid: String) -> Option<&NodeID> {
        self.id_mapper.get(&eid)
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

    pub fn a_star(&self, a: NodeID, b: NodeID) {
        let mut pq = PriorityQueue::<NodeID, Reverse<usize>>::new();
        let mut origins = HashMap::<NodeID, (NodeID, EdgeData)>::new();
        let mut visited = HashSet::<NodeID>::new();
        pq.push(a, Reverse(0));

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
                    }
                });
                println!("Length: {}", dist);
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
                            let cost = p.0 + street.length;

                            match pq.get_priority(&street.destination) {
                                Some(current) => {
                                    if current.0 > cost {
                                        pq.change_priority(&street.destination, Reverse(cost));
                                        origins.insert(street.destination, (id, neighbor.clone()));
                                    }
                                }
                                None => {
                                    pq.push(street.destination, Reverse(cost));
                                    origins.insert(street.destination, (id, neighbor.clone()));
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
}
