use std::collections::HashMap;

use kdtree::{KdTree, distance::squared_euclidean};
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{AgencyId, AgencyInfo, RouteId, RouteInfo, TripId, TripInfo},
    ingestion::osm::{ConnectorCost, PlatformIndex},
    structures::{Connector, EdgeData, LatLng, NodeData, NodeID, OsmNodeData, StreetEdgeData},
};

pub use raptor_index::{RaptorIndex, StationInfo, StationLine};

mod bike_cost;
pub mod contraction;
mod edge_index;
pub mod latency_profile;
mod multiobj;
mod multiobj_plan;
mod path_distribution;
mod platform_reach;
mod railway;
mod raptor_access;
mod raptor_backward;
mod raptor_build;
mod raptor_cch;
mod raptor_index;
mod raptor_plan;
mod raptor_route;
mod realtime_match;
mod representatives;
mod street_enrich;
mod transit;

pub use bike_cost::{BikeCost, PrevCtx};
pub use platform_reach::ConnectorReach;
pub use raptor_access::StreetProfile;
pub use raptor_cch::CchAccess;
pub use raptor_route::{OnboardRide, OnboardSeed, QueryEndpoints};
pub use realtime_match::{MatchParams, ScheduledArrival, best_match};
pub use transit::StationBackup;

/// A routing terminal: either an exact graph node, or a point projected onto the
/// interior of an edge between nodes `a` and `b` (used by edge-aware snapping so a
/// pin sitting mid-way along a long edge isn't forced onto a distant end node).
/// `dist_a`/`dist_b` are the meters from the projection to each endpoint, summing
/// to the edge length.
#[derive(Debug, Clone, Copy)]
pub enum Endpoint {
    Node(NodeID),
    OnEdge {
        a: NodeID,
        b: NodeID,
        dist_a: usize,
        dist_b: usize,
        proj: LatLng,
    },
}

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
    #[serde(skip, default)]
    edge_index: edge_index::EdgeIndex,
    /// All-mode (union) contracted graph driving all routing. **Serialized** into
    /// `graph.bin` (its serde-skipped `seg_index` R-tree is rebuilt post-load). `None` ⇒
    /// no contracted graph (only on a partially-built graph; production always builds it).
    #[serde(default)]
    pub contracted: Option<contraction::ContractedGraph>,
    /// OSM-derived platform index (Stage A platform matching). Serialized only via
    /// the OSM view (lives in `osm.bin`, gated by `OSM_SCHEMA_VERSION`); skipped in
    /// the full-graph (`graph.bin`) serialization since routing never reads it.
    #[serde(skip, default)]
    platforms: PlatformIndex,
    /// OSM `level` (semantic storey) per node, sparse — only nodes on a leveled way
    /// (platforms, leveled footways). Stage B1 auxiliary OSM data, lives in `osm.bin`
    /// only (mirrors `platforms`); routing never reads it in B1.
    #[serde(skip, default)]
    node_levels: HashMap<NodeID, i16>,
    /// Pedestrian vertical connectors (stairs/elevator/ramp) keyed by directed node
    /// pair, classified from the OSM highway tag. Stage B1 auxiliary OSM data,
    /// `osm.bin` only. Used by the connector-coverage measurement.
    #[serde(skip, default)]
    connector_edges: HashMap<(NodeID, NodeID), Connector>,
    /// Pedestrian connector cost model (config `default_routing.connector_cost`).
    /// Runtime-only knob for the B1 measurement's added-time stat; not serialized.
    #[serde(skip, default)]
    connector_cost: ConnectorCost,
    /// Foot access/egress CCH (runtime-only, never serialized — adds no bytes to the
    /// postcard layout, so `GRAPH_SCHEMA_VERSION` need not change). Built on demand;
    /// chunk 2 wires it into startup. `None` until [`Graph::set_cch`] installs it.
    #[serde(skip, default)]
    pub cch: Option<raptor_cch::CchAccess>,
}

#[derive(Serialize)]
struct OsmView<'a> {
    nodes: &'a Vec<NodeData>,
    edges: &'a Vec<Vec<EdgeData>>,
    nodes_tree: &'a KdTree<f64, NodeID, [f64; 2]>,
    id_mapper: &'a HashMap<String, NodeID>,
    platforms: &'a PlatformIndex,
    node_levels: &'a HashMap<NodeID, i16>,
    connector_edges: &'a HashMap<(NodeID, NodeID), Connector>,
}

#[derive(Deserialize)]
struct OsmOwned {
    nodes: Vec<NodeData>,
    edges: Vec<Vec<EdgeData>>,
    nodes_tree: KdTree<f64, NodeID, [f64; 2]>,
    id_mapper: HashMap<String, NodeID>,
    platforms: PlatformIndex,
    #[serde(default)]
    node_levels: HashMap<NodeID, i16>,
    #[serde(default)]
    connector_edges: HashMap<(NodeID, NodeID), Connector>,
}

pub static MAX_TRANSFER_DISTANCE_M: f64 = 1000.0;
pub const MAX_SCENARIOS: usize = 2;
pub const MAX_ROUNDS: usize = 20;

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_tree: KdTree::new(2),
            id_mapper: HashMap::new(),
            raptor: RaptorIndex::new(),
            edge_index: edge_index::EdgeIndex::default(),
            contracted: None,
            platforms: PlatformIndex::default(),
            node_levels: HashMap::new(),
            connector_edges: HashMap::new(),
            connector_cost: ConnectorCost::default(),
            cch: None,
        }
    }

    /// The contracted graph the bike multi-objective search reads — the **serialized union**
    /// cg (`self.contracted`, which survives the P3 drop of `g`).
    pub(super) fn bike_cg(&self) -> Option<&contraction::ContractedGraph> {
        self.contracted.as_ref()
    }

    /// Cost-bake bike onto the already-built union contracted graph (`self.contracted`) using
    /// the configured bike profile. Call after `g.contracted` exists, on every startup
    /// (the bake is serde-skipped). No-op if `self.contracted` is None.
    pub fn bake_bike_on_contracted_default(&mut self) {
        let bike = self.default_bike_cost();
        self.bake_bike_on_contracted(&bike);
    }

    pub fn to_osm_postcard(&self) -> Result<Vec<u8>, String> {
        let view = OsmView {
            nodes: &self.nodes,
            edges: &self.edges,
            nodes_tree: &self.nodes_tree,
            id_mapper: &self.id_mapper,
            platforms: &self.platforms,
            node_levels: &self.node_levels,
            connector_edges: &self.connector_edges,
        };
        postcard::to_allocvec(&view).map_err(|e| format!("Failed to serialize OSM graph: {e}"))
    }

    pub fn from_osm_postcard(bytes: &[u8]) -> Result<Graph, String> {
        let o: OsmOwned = postcard::from_bytes(bytes)
            .map_err(|e| format!("Failed to deserialize OSM graph: {e}"))?;
        Ok(Graph {
            nodes: o.nodes,
            edges: o.edges,
            nodes_tree: o.nodes_tree,
            id_mapper: o.id_mapper,
            raptor: RaptorIndex::new(),
            edge_index: edge_index::EdgeIndex::default(),
            contracted: None,
            platforms: o.platforms,
            node_levels: o.node_levels,
            connector_edges: o.connector_edges,
            connector_cost: ConnectorCost::default(),
            cch: None,
        })
    }

    pub fn set_platform_index(&mut self, idx: PlatformIndex) {
        self.platforms = idx;
    }

    pub fn platform_index(&self) -> &PlatformIndex {
        &self.platforms
    }

    /// Install the Stage B1 auxiliary OSM level/connector data parsed during the
    /// PBF pass (sparse `node → level` and `connector edge` maps).
    pub fn set_osm_level_data(
        &mut self,
        node_levels: HashMap<NodeID, i16>,
        connector_edges: HashMap<(NodeID, NodeID), Connector>,
    ) {
        self.node_levels = node_levels;
        self.connector_edges = connector_edges;
    }

    /// OSM `level` (semantic storey) of a node, if it sits on a leveled way.
    /// `None` is read as ground level.
    pub fn node_level(&self, id: NodeID) -> Option<i16> {
        self.node_levels.get(&id).copied()
    }

    /// Set a node's `level` (Stage B2a relocation pins a stop to its platform
    /// storey). Sparse — overrides any existing entry.
    pub fn set_node_level(&mut self, id: NodeID, level: i16) {
        self.node_levels.insert(id, level);
    }

    /// Move a **transit stop**'s anchor coordinate (Stage B2a relocation onto its
    /// matched OSM platform node). Safe only for transit stops, which are NOT in the
    /// snap KD-tree, so no `nodes_tree` resync is needed; a no-op on OSM nodes.
    pub fn relocate_transit_stop(&mut self, id: NodeID, loc: LatLng) {
        if let Some(NodeData::TransitStop(stop)) = self.nodes.get_mut(id.0) {
            stop.lat_lng = loc;
        }
    }

    /// The pedestrian connector kind of the directed edge `a→b`, if any.
    pub fn connector_kind(&self, a: NodeID, b: NodeID) -> Option<Connector> {
        self.connector_edges.get(&(a, b)).copied()
    }

    pub fn set_connector_cost(&mut self, cost: ConnectorCost) {
        self.connector_cost = cost;
    }

    pub fn connector_cost(&self) -> ConnectorCost {
        self.connector_cost
    }

    /// Bake connector-specific traversal costs into edge **lengths** so that
    /// `edge_secs(Foot)` — which always computes `length / walking_speed` — yields
    /// the correct time even after `connector_edges` is serde-skipped on restore and
    /// after contraction copies the lengths into super-edge segments.
    ///
    /// Must be called after `connector_edges` is populated (post-OSM phase) and
    /// after `walking_speed_mps` is configured, but **before** contraction builds the
    /// contracted graph (which bakes the lengths into super-edge segments).
    ///
    /// Formula:
    /// - Stairs/ramp: `new_len = old_len * walk_speed / connector_speed`
    ///   (so `new_len / walk_speed = old_len / connector_speed`, the correct time)
    /// - Elevator: `new_len = elevator_secs * walk_speed`
    ///   (fixed time regardless of physical run length)
    ///
    /// No-op when `connector_edges` or `edges` is empty (restore path, pre-OSM, tests).
    pub fn bake_connector_lengths(&mut self, cost: ConnectorCost) {
        if self.connector_edges.is_empty() || self.edges.is_empty() {
            return;
        }
        let walk_speed = self.raptor.walking_speed_mps;
        let pairs: Vec<((NodeID, NodeID), Connector)> =
            self.connector_edges.iter().map(|(&k, &v)| (k, v)).collect();
        for ((a, b), kind) in pairs {
            let Some(edges) = self.edges.get_mut(a.0) else {
                continue;
            };
            for edge in edges.iter_mut() {
                if let EdgeData::Street(s) = edge {
                    if s.destination == b {
                        let old_len = s.length as f64;
                        let new_len = match kind {
                            Connector::Steps => {
                                (old_len * walk_speed / cost.stairs_speed_mps).round() as usize
                            }
                            Connector::Ramp => {
                                (old_len * walk_speed / cost.ramp_speed_mps).round() as usize
                            }
                            Connector::Elevator => {
                                (cost.elevator_secs * walk_speed).round() as usize
                            }
                        };
                        s.length = new_len.max(1);
                        break;
                    }
                }
            }
        }
    }

    pub fn set_min_access_secs(&mut self, secs: u32) {
        self.raptor.min_access_secs = secs;
    }

    pub fn set_walking_speed_mps(&mut self, mps: f64) {
        self.raptor.walking_speed_mps = mps;
    }

    pub fn walking_speed_mps(&self) -> f64 {
        self.raptor.walking_speed_mps
    }

    pub fn set_station_merge_radius_m(&mut self, m: f64) {
        self.raptor.station_merge_radius_m = m;
    }

    pub fn station_merge_radius_m(&self) -> f64 {
        self.raptor.station_merge_radius_m
    }

    pub fn set_cycling_speed_mps(&mut self, mps: f64) {
        self.raptor.cycling_speed_mps = mps;
    }

    pub fn set_bike_profile(&mut self, p: crate::structures::BikeProfile) {
        self.raptor.bike_profile = p;
    }

    pub fn set_street_time(&mut self, m: crate::structures::StreetTimeModel) {
        self.raptor.street_time = m;
    }

    pub fn set_distance_budget(&mut self, v: f64) {
        self.raptor.distance_budget = v;
    }

    pub fn set_epsilon(&mut self, e: crate::structures::cost::Epsilon) {
        self.raptor.epsilon = e;
    }

    pub fn set_bike_bucket_cyc_k(&mut self, k: f64) {
        self.raptor.bike_bucket_cyc_k = k;
    }

    pub fn set_bike_bucket_dpl_k(&mut self, k: f64) {
        self.raptor.bike_bucket_dpl_k = k;
    }

    pub fn set_drive_bucket_var_k(&mut self, k: f64) {
        self.raptor.drive_bucket_var_k = k;
    }

    pub fn set_walk_bucket_surf_k(&mut self, k: f64) {
        self.raptor.walk_bucket_surf_k = k;
    }

    pub fn set_variance_model(&mut self, m: crate::structures::cost::VarianceModel) {
        self.raptor.variance_model = m;
    }

    pub fn set_cost_weights(&mut self, w: crate::structures::cost::CostWeights) {
        self.raptor.cost_weights = w;
    }

    pub fn set_representatives_k(&mut self, k: usize) {
        self.raptor.representatives_k = k;
    }

    pub fn set_alt_max_share_factor(&mut self, f: f64) {
        self.raptor.alt_max_share_factor = f;
    }

    pub fn set_systematic_cv(&mut self, cv: f64) {
        self.raptor.systematic_cv = cv;
    }

    pub fn set_balance(&mut self, b: crate::structures::cost::BalanceWeights) {
        self.raptor.balance = b;
    }

    /// `BikeCost` built from the graph's configured default profile.
    pub(super) fn default_bike_cost(&self) -> BikeCost {
        BikeCost::new(self.raptor.bike_profile)
    }

    pub fn set_driving_speed_mps(&mut self, mps: f64) {
        self.raptor.driving_speed_mps = mps;
    }

    pub fn set_vehicle_access_secs(&mut self, secs: u32) {
        self.raptor.vehicle_access_secs = secs;
    }

    pub fn set_vehicle_access_fraction(&mut self, f: f64) {
        self.raptor.vehicle_access_fraction = f;
    }

    pub fn set_vehicle_access_max_secs(&mut self, secs: u32) {
        self.raptor.vehicle_access_max_secs = secs;
    }

    /// Sets reliability bucket edges after validating they are sorted, strictly
    /// increasing, and each in `(0.0, 1.0)`. Invalid input is ignored (keeps default).
    pub fn set_reliability_bucket_edges(&mut self, edges: Vec<f32>) {
        if crate::structures::valid_reliability_edges(&edges) {
            self.raptor.reliability_bucket_edges = edges;
        } else {
            tracing::warn!("ignoring invalid reliability_bucket_edges: {:?}", edges);
        }
    }

    pub fn set_arrival_slack_secs(&mut self, secs: u32) {
        self.raptor.arrival_slack_secs = secs;
    }

    pub fn set_unrestricted_transfers(&mut self, on: bool) {
        self.raptor.unrestricted_transfers = on;
    }

    pub fn set_use_cch_access(&mut self, on: bool) {
        self.raptor.use_cch_access = on;
    }

    /// Sets the graph-level default for the query-latency decomposition profiler
    /// (`GraphQL raptor(profileLatency: ...)` overrides this per-query).
    pub fn set_profile_latency(&mut self, on: bool) {
        self.raptor.profile_latency = on;
    }

    pub fn set_max_window_secs(&mut self, secs: u32) {
        self.raptor.max_window_secs = secs;
    }

    pub fn set_max_snap_distance_m(&mut self, meters: u32) {
        self.raptor.max_snap_distance_m = meters;
    }

    pub fn add_node(&mut self, node: NodeData) -> NodeID {
        let id = NodeID(self.nodes.len());

        if let NodeData::OsmNode(ref osm_node) = node {
            let _ = self
                .nodes_tree
                .add([osm_node.lat_lng.latitude, osm_node.lat_lng.longitude], id);
            self.id_mapper.insert(osm_node.eid.clone(), id);
        }

        self.nodes.push(node);
        self.edges.push(Vec::new());
        id
    }

    /// Add an OSM node **without** inserting it into the snap KD-tree
    /// (`nodes_tree`). Used for Stage B1 platform-way nodes: they must be routable
    /// (edges resolve via `id_mapper`) but must NOT become candidates for GTFS
    /// stop snapping — otherwise a stop near a newly-imported platform would snap
    /// to it, silently relocating the stop. Keeping them out of the tree preserves
    /// today's stop→nearest-street-node snapping exactly.
    pub fn add_osm_node_unindexed(&mut self, node: OsmNodeData) -> NodeID {
        let id = NodeID(self.nodes.len());
        self.id_mapper.insert(node.eid.clone(), id);
        self.nodes.push(NodeData::OsmNode(node));
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

    /// Outgoing edges of a node (empty slice if out of bounds).
    pub fn out_edges(&self, id: NodeID) -> &[EdgeData] {
        self.edges.get(id.0).map(|v| v.as_slice()).unwrap_or(&[])
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
            Ok(mut it) => it.next().map(|v| *v.1),
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
            Ok(mut it) => it.next(),
            Err(_) => {
                tracing::warn!("KD-tree query failed (empty tree?)");
                None
            }
        }
    }

    /// Project a query coordinate onto the segment `pa→pb`, returning
    /// `(perpendicular_distance_meters, t)` where `t∈[0,1]` is the fraction from
    /// `pa` to the closest point. Equirectangular meters centred on the query.
    fn project_point(lat: f64, lon: f64, pa: LatLng, pb: LatLng) -> (f64, f64) {
        let m_lat = 111_320.0_f64;
        let m_lon = 111_320.0_f64 * lat.to_radians().cos();
        let to = |la: f64, lo: f64| ((lo - lon) * m_lon, (la - lat) * m_lat);
        let (ax, ay) = to(pa.latitude, pa.longitude);
        let (bx, by) = to(pb.latitude, pb.longitude);
        let (dx, dy) = (bx - ax, by - ay);
        let len2 = dx * dx + dy * dy;
        let t = if len2 == 0.0 {
            0.0
        } else {
            (-(ax * dx + ay * dy) / len2).clamp(0.0, 1.0)
        };
        let (px, py) = (ax + t * dx, ay + t * dy);
        ((px * px + py * py).sqrt(), t)
    }

    /// Rebuild the spatial edge index over every street segment. Cheap, bulk-loaded,
    /// and never serialized — call after a build or after deserialization so
    /// edge-aware snapping works without a graph rebuild. No-op shape when there are
    /// no OSM nodes (empty graph).
    pub fn build_edge_index(&mut self) {
        let ref_lat = self
            .nodes
            .iter()
            .find_map(|n| match n {
                NodeData::OsmNode(o) => Some(o.lat_lng.latitude),
                _ => None,
            })
            .unwrap_or(0.0);
        let edges = self.edges.iter().flatten().filter_map(|e| {
            let EdgeData::Street(s) = e else { return None };
            let pa = self.nodes[s.origin.0].loc();
            let pb = self.nodes[s.destination.0].loc();
            Some((
                *s,
                (pa.latitude, pa.longitude),
                (pb.latitude, pb.longitude),
            ))
        });
        self.edge_index = edge_index::EdgeIndex::build(edges, ref_lat);
    }

    /// Snap a coordinate to the nearest *edge* (by perpendicular body distance)
    /// whose street data satisfies `usable`, within `radius_m`, using the spatial
    /// edge index (so a point mid-way along a long edge with distant endpoints is
    /// still found). Returns the projected [`Endpoint`] and its perpendicular
    /// distance in meters, or `None` if no usable edge is in range (caller falls
    /// back to `nearest_node`).
    pub fn snap_to_edge(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        usable: impl Fn(&StreetEdgeData) -> bool,
    ) -> Option<(Endpoint, f64)> {
        let (s, _) = self.edge_index.nearest_usable(lat, lon, radius_m, usable)?;
        let pa = self.nodes[s.origin.0].loc();
        let pb = self.nodes[s.destination.0].loc();
        let (perp, t) = Self::project_point(lat, lon, pa, pb);
        let da = ((t * s.length as f64).round() as usize).min(s.length);
        let proj = LatLng {
            latitude: pa.latitude + t * (pb.latitude - pa.latitude),
            longitude: pa.longitude + t * (pb.longitude - pa.longitude),
        };
        Some((
            Endpoint::OnEdge {
                a: s.origin,
                b: s.destination,
                dist_a: da,
                dist_b: s.length - da,
                proj,
            },
            perp,
        ))
    }

    pub fn nodes_distance(&self, a: NodeID, b: NodeID) -> usize {
        (self.node_loc(a).dist(self.node_loc(b)) * 0.99) as usize
    }
}
