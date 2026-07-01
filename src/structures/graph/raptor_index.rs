use std::collections::HashMap;

use gtfs_structures::RouteType;
use kdtree::KdTree;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::{
        AgencyInfo, RouteInfo, ServicePattern, StopTime, TimetableSegment, TripId, TripInfo,
        TripSegment,
    },
    structures::{
        DelayCDF, LatLng, NodeID,
        raptor::{Lookup, PatternID, PatternInfo},
    },
};

/// One transit line (GTFS route) serving a station, with its display mode and
/// colours. `color`/`text_color` are 6-character hex strings (no leading `#`),
/// or `None` when the feed omits them. Distinct per `(mode, short_name, color)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StationLine {
    pub mode: String,
    pub short_name: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

/// One physical transit station: a group of GTFS platforms collapsed by their
/// shared (non-empty) `parent_station`. Stops lacking a parent each form their
/// own standalone station.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationInfo {
    /// Station id: the shared `parent_station` value, or the lone stop's `stop_id`.
    pub id: String,
    pub name: String,
    pub lat_lng: LatLng,
    /// Distinct operator (agency) names serving any member platform, sorted.
    pub operators: Vec<String>,
    /// Distinct transport-mode labels (e.g. "Bus", "Tramway", "Subway") served by
    /// any member platform, sorted. Derived after grouping from each member's
    /// stop→pattern→route route_type via `display_route_type`.
    #[serde(default)]
    pub modes: Vec<String>,
    /// Distinct lines (routes) serving any member platform, deduped by
    /// `(mode, short_name, color)`. Grouped by mode (Rail, Subway, Tramway, Bus,
    /// then others alphabetically) and naturally sorted by `short_name` within a mode.
    #[serde(default)]
    pub lines: Vec<StationLine>,
    /// Compact stop indices of the member platforms, ascending.
    pub platform_stop_indices: Vec<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaptorIndex {
    pub transit_departures: Vec<TripSegment>,
    pub transit_services: Vec<ServicePattern>,
    pub transit_trips: Vec<TripInfo>,
    pub transit_routes: Vec<RouteInfo>,
    pub transit_agencies: Vec<AgencyInfo>,
    pub transit_patterns: Vec<PatternInfo>,

    pub transit_pattern_stops: Vec<NodeID>,
    pub transit_stop_patterns: Vec<(PatternID, u32)>,
    pub transit_stop_transfers: Vec<(NodeID, u32)>,
    pub transit_pattern_stop_times: Vec<StopTime>,
    pub transit_pattern_trips: Vec<TripId>,

    pub transit_idx_pattern_stops: Vec<Lookup>,
    pub transit_idx_stop_patterns: Vec<Lookup>,
    pub transit_idx_stop_transfers: Vec<Lookup>,
    pub transit_idx_pattern_stop_times: Vec<Lookup>,
    pub transit_idx_pattern_trips: Vec<Lookup>,

    pub transit_delay_models: HashMap<RouteType, DelayCDF>,

    pub transit_node_to_stop: Vec<u32>,
    pub transit_stop_to_node: Vec<NodeID>,
    pub transit_stops_tree: KdTree<f64, usize, [f64; 2]>,

    /// Original GTFS `route_id` string per internal `RouteId` (index = `RouteId.0`).
    /// Serialized — required to match realtime alert `route_id` fields, which carry
    /// the raw GTFS string id. Parallel to `transit_routes`.
    #[serde(default)]
    pub transit_route_ids: Vec<String>,

    /// Original GTFS `trip_id` string per internal `TripId` (index = `TripId.0`).
    /// Serialized — required to match realtime feeds, which key by string id.
    #[serde(default)]
    pub transit_trip_ids: Vec<String>,
    /// Reverse of `transit_trip_ids`, derived at build/load time (not serialized).
    #[serde(skip)]
    pub trip_id_to_index: HashMap<String, TripId>,

    /// Original GTFS `stop_id` string per compact stop index. Serialized so the
    /// reverse map can be rebuilt at load without re-reading node data.
    #[serde(default)]
    pub transit_stop_ids: Vec<String>,
    /// Reverse of `transit_stop_ids` (stop_id → compact stop index), derived.
    #[serde(skip)]
    pub stop_id_to_index: HashMap<String, usize>,

    /// Display name per compact stop index (parallel to `transit_stop_ids`). Names
    /// live only in `NodeData::TransitStop`, so this serialized copy is what plan and
    /// explain reconstruction read after the interior-node drop empties `g.nodes`.
    #[serde(default)]
    pub transit_stop_names: Vec<String>,

    /// GTFS `platform_code` per compact stop index (parallel to `transit_stop_ids`).
    #[serde(default)]
    pub transit_stop_platform_codes: Vec<Option<String>>,

    /// Deduped physical stations (grouped by `parent_station`). Source of truth;
    /// the lookup maps below are derived in `build_runtime_indices`.
    #[serde(default)]
    pub transit_stations: Vec<StationInfo>,
    /// compact stop index → station index, derived from `transit_stations`.
    #[serde(skip)]
    pub transit_stop_to_station: Vec<u32>,
    /// station id → station index, derived from `transit_stations`.
    #[serde(skip)]
    pub station_id_to_index: HashMap<String, usize>,

    #[serde(default)]
    pub transit_stop_reverse_transfers: Vec<(usize, u32)>,
    #[serde(default)]
    pub transit_idx_stop_reverse_transfers: Vec<Lookup>,

    #[serde(default)]
    pub transit_pattern_shapes: Vec<Vec<LatLng>>,
    #[serde(default)]
    pub transit_pattern_shape_stop_idx: Vec<Vec<u32>>,

    /// Per pattern, per inter-stop segment (index `s-1` for the edge stop `s-1`→`s`),
    /// the transit edge's `timetable_segment`. Precomputed at build time from `g.edges`
    /// so transit-leg plan reconstruction needs no `g` once the interior arrays are
    /// dropped (node contraction). Empty unless built; reconstruction falls back to the
    /// `g.edges` scan when empty (flag-off / pre-cutover graphs).
    #[serde(default)]
    pub transit_pattern_segment_timetables: Vec<Vec<TimetableSegment>>,

    #[serde(default)]
    pub railway_nodes: Vec<(f64, f64)>,
    #[serde(default)]
    pub railway_adj: Vec<Vec<(usize, u32)>>,

    #[serde(default = "RaptorIndex::default_min_access_secs")]
    pub min_access_secs: u32,

    #[serde(default = "RaptorIndex::default_walking_speed_mps")]
    pub walking_speed_mps: f64,

    /// Build-time radius (meters) for merging parent-less GTFS stops into a
    /// same-named physical station. Read during the GTFS ingestion phase by the
    /// per-provider orphan-absorption preprocessor. Tuning param — not serialized,
    /// set before the GTFS phase from config.yaml.
    #[serde(skip, default = "RaptorIndex::default_station_merge_radius_m")]
    pub station_merge_radius_m: f64,

    #[serde(skip, default = "RaptorIndex::default_cycling_speed_mps")]
    pub cycling_speed_mps: f64,

    #[serde(skip, default = "RaptorIndex::default_driving_speed_mps")]
    pub driving_speed_mps: f64,

    // Access-radius floor (seconds) used when a bike/car access or egress mode is
    // active, so the search reaches a better-connected hub farther than the
    // nearest stops instead of stopping at the first local result.
    #[serde(skip, default = "RaptorIndex::default_vehicle_access_secs")]
    pub vehicle_access_secs: u32,

    // The vehicle (bike/car) access budget scales with trip length: a longer journey
    // justifies riding farther to reach a better hub. Budget = crow-flies time ×
    // `vehicle_access_fraction`, clamped to [`vehicle_access_secs`, `vehicle_access_max_secs`].
    #[serde(skip, default = "RaptorIndex::default_vehicle_access_fraction")]
    pub vehicle_access_fraction: f64,
    #[serde(skip, default = "RaptorIndex::default_vehicle_access_max_secs")]
    pub vehicle_access_max_secs: u32,

    // Runtime tuning params, applied from config.yaml at startup — not serialized,
    // so adding them does not change the `graph.bin` (postcard) layout.
    #[serde(skip, default = "RaptorIndex::default_reliability_bucket_edges")]
    pub reliability_bucket_edges: Vec<f32>,

    #[serde(skip, default = "RaptorIndex::default_arrival_slack_secs")]
    pub arrival_slack_secs: u32,

    #[serde(skip, default = "RaptorIndex::default_max_window_secs")]
    pub max_window_secs: u32,

    #[serde(skip, default = "RaptorIndex::default_max_snap_distance_m")]
    pub max_snap_distance_m: u32,

    // Edge-aware snapping: project a query coordinate onto the nearest *edge*
    // (within `edge_snap_radius_m`) instead of the nearest node, so a point mid-way
    // along a long straight edge isn't forced onto a distant end node. Runtime
    // tuning — not serialized.
    #[serde(skip, default = "RaptorIndex::default_edge_snap")]
    pub edge_snap: bool,

    #[serde(skip, default = "RaptorIndex::default_edge_snap_radius_m")]
    pub edge_snap_radius_m: f64,

    // Default bike cost profile (BRouter trekking). Not serialized — applied from
    // config.yaml at startup; a per-request override merges over it.
    #[serde(skip, default)]
    pub bike_profile: crate::structures::BikeProfile,

    /// Stochastic street-time model for access/egress legs. Tuning, not derived
    /// data — `#[serde(skip)]` like `bike_profile`, so it is NOT in graph.bin and
    /// needs no schema bump; set from config at build time.
    #[serde(skip, default = "RaptorIndex::default_street_time")]
    pub street_time: crate::structures::StreetTimeModel,

    /// RCSP distance budget multiplier δ: paths up to (1+δ)·shortest are explored.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_distance_budget")]
    pub distance_budget: f64,

    /// Per-axis ε-dominance thresholds for the multi-objective Pareto filter.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_epsilon")]
    pub epsilon: crate::structures::cost::Epsilon,

    /// Bike grid-bucketing cell-size coefficients per meter of origin→dest
    /// straight-line distance, on the CyclewayDeficit and Dplus diversity axes.
    /// Cell size = k·D; `0.0` disables bucketing on that axis (strict no-op).
    /// Bounds the per-node Pareto frontier while preserving the cycleway/climb
    /// span. Tuning params — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_bike_bucket_cyc_k")]
    pub bike_bucket_cyc_k: f64,
    #[serde(skip, default = "RaptorIndex::default_bike_bucket_dpl_k")]
    pub bike_bucket_dpl_k: f64,

    /// Whether D+ (ascent) is a bike SELECTION/dominance axis. Default false: with the
    /// gradient-aware power model climbing is already priced in Time, so a separate
    /// "minimize D+ at any cost" axis only manufactures absurd extremes (a long walk to
    /// shave a few m of ascent) and triples search cost. D+ stays a displayed stat.
    #[serde(skip, default = "RaptorIndex::default_bike_select_dplus")]
    pub bike_select_dplus: bool,

    /// Tunable σ model for signal/elevator/crossing variance generators.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_variance_model")]
    pub variance_model: crate::structures::cost::VarianceModel,

    /// Per-axis surface roughness and comfort-stress weights for the cost vector.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_cost_weights")]
    pub cost_weights: crate::structures::cost::CostWeights,

    /// Number of diverse representatives kept from the multi-objective front.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_representatives_k")]
    pub representatives_k: usize,

    /// Whether multi-objective street routing is enabled (opt-in; see config).
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_multiobj_street")]
    pub multiobj_street: bool,

    /// Max scalar leg length (metres) to enrich with multi-objective alternatives
    /// for non-walk street modes (bike/car). Bike/car alternatives come from
    /// `multiobj_leg_options` (Pareto front, corridor-budgeted), so this is a high
    /// safety net against pathological cross-country legs rather than a hard perf cliff.
    /// Walk legs are never gated. Tuning param — applied at startup.
    #[serde(skip, default = "RaptorIndex::default_multiobj_street_max_len_m")]
    pub multiobj_street_max_len_m: usize,

    /// Secondary weight on Time in a single-axis "champion" scalarization (the
    /// fastest champion is pure Time). Breaks ties toward shorter routes when an
    /// objective (e.g. D+) is otherwise near-degenerate. Tuning param — applied at
    /// startup, not serialized.
    #[serde(skip, default = "RaptorIndex::default_champion_time_tiebreak")]
    pub champion_time_tiebreak: f64,

    /// ADGW limited-sharing threshold: an alternative bike/car leg is dropped if it
    /// shares more than this fraction of its length with a higher-ranked one. Tuning
    /// param — applied at startup, not serialized. 0.6 mirrors GraphHopper's default.
    #[serde(skip, default = "RaptorIndex::default_alt_max_share_factor")]
    pub alt_max_share_factor: f64,

    /// Systematic coefficient of variation for post-hoc path-time variance.
    /// Tuning param — not serialized, applied from config at startup.
    #[serde(skip, default = "RaptorIndex::default_systematic_cv")]
    pub systematic_cv: f64,

    /// Per-axis balanced-default weights. Tuning param — not serialized, applied
    /// from config at startup.
    #[serde(skip, default = "RaptorIndex::default_balance")]
    pub balance: crate::structures::cost::BalanceWeights,
}

impl Default for RaptorIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl RaptorIndex {
    pub fn new() -> Self {
        RaptorIndex {
            transit_departures: Vec::new(),
            transit_services: Vec::new(),
            transit_trips: Vec::new(),
            transit_routes: Vec::new(),
            transit_agencies: Vec::new(),
            transit_patterns: Vec::new(),

            transit_pattern_stops: Vec::new(),
            transit_stop_patterns: Vec::new(),
            transit_stop_transfers: Vec::new(),
            transit_pattern_stop_times: Vec::new(),
            transit_pattern_trips: Vec::new(),

            transit_idx_pattern_stops: Vec::new(),
            transit_idx_stop_patterns: Vec::new(),
            transit_idx_stop_transfers: Vec::new(),
            transit_idx_pattern_stop_times: Vec::new(),
            transit_idx_pattern_trips: Vec::new(),

            transit_delay_models: HashMap::new(),

            transit_node_to_stop: Vec::new(),
            transit_stop_to_node: Vec::new(),
            transit_stops_tree: KdTree::new(2),

            transit_route_ids: Vec::new(),
            transit_trip_ids: Vec::new(),
            trip_id_to_index: HashMap::new(),
            transit_stop_ids: Vec::new(),
            stop_id_to_index: HashMap::new(),
            transit_stop_names: Vec::new(),
            transit_stop_platform_codes: Vec::new(),

            transit_stations: Vec::new(),
            transit_stop_to_station: Vec::new(),
            station_id_to_index: HashMap::new(),

            transit_stop_reverse_transfers: Vec::new(),
            transit_idx_stop_reverse_transfers: Vec::new(),

            transit_pattern_shapes: Vec::new(),
            transit_pattern_shape_stop_idx: Vec::new(),
            transit_pattern_segment_timetables: Vec::new(),

            railway_nodes: Vec::new(),
            railway_adj: Vec::new(),

            min_access_secs: Self::default_min_access_secs(),
            walking_speed_mps: Self::default_walking_speed_mps(),
            station_merge_radius_m: Self::default_station_merge_radius_m(),
            cycling_speed_mps: Self::default_cycling_speed_mps(),
            driving_speed_mps: Self::default_driving_speed_mps(),
            vehicle_access_secs: Self::default_vehicle_access_secs(),
            vehicle_access_fraction: Self::default_vehicle_access_fraction(),
            vehicle_access_max_secs: Self::default_vehicle_access_max_secs(),
            reliability_bucket_edges: Self::default_reliability_bucket_edges(),
            arrival_slack_secs: Self::default_arrival_slack_secs(),
            max_window_secs: Self::default_max_window_secs(),
            max_snap_distance_m: Self::default_max_snap_distance_m(),
            edge_snap: Self::default_edge_snap(),
            edge_snap_radius_m: Self::default_edge_snap_radius_m(),
            bike_profile: crate::structures::BikeProfile::default(),
            street_time: Self::default_street_time(),
            distance_budget: Self::default_distance_budget(),
            epsilon: Self::default_epsilon(),
            bike_bucket_cyc_k: Self::default_bike_bucket_cyc_k(),
            bike_bucket_dpl_k: Self::default_bike_bucket_dpl_k(),
            bike_select_dplus: Self::default_bike_select_dplus(),
            variance_model: Self::default_variance_model(),
            cost_weights: Self::default_cost_weights(),
            representatives_k: Self::default_representatives_k(),
            multiobj_street: Self::default_multiobj_street(),
            multiobj_street_max_len_m: Self::default_multiobj_street_max_len_m(),
            champion_time_tiebreak: Self::default_champion_time_tiebreak(),
            alt_max_share_factor: Self::default_alt_max_share_factor(),
            systematic_cv: Self::default_systematic_cv(),
            balance: Self::default_balance(),
        }
    }

    pub fn default_min_access_secs() -> u32 {
        10 * 60
    }

    pub fn default_walking_speed_mps() -> f64 {
        1.2
    }

    /// Merge radius (m) for the EXACT normalized-name + SAME-operator/feed case
    /// only. That match is a strong signal, so it tolerates the spread of big
    /// interchanges (Gare du Nord surface↔metro ~95-123 m, Merode ~111 m) while
    /// keeping genuinely distinct same-named STIB stops (>250 m apart) separate. A
    /// future fuzzy or cross-operator matcher should use its own, tighter value.
    pub fn default_station_merge_radius_m() -> f64 {
        250.0
    }

    pub fn default_cycling_speed_mps() -> f64 {
        4.2
    }

    pub fn default_driving_speed_mps() -> f64 {
        11.0 // ~40 km/h urban driving
    }

    pub fn default_vehicle_access_secs() -> u32 {
        20 * 60 // 20 min floor: ~5 km by bike, ~13 km by car, to reach a real hub
    }

    pub fn default_vehicle_access_fraction() -> f64 {
        0.06 // ~6% of the crow-flies (walk-time) trip: only long journeys grow past the floor
    }

    pub fn default_vehicle_access_max_secs() -> u32 {
        45 * 60 // hard ceiling so a very long trip's access Dijkstra stays bounded
    }

    pub fn default_reliability_bucket_edges() -> Vec<f32> {
        vec![0.50, 0.80, 0.95]
    }

    pub fn default_arrival_slack_secs() -> u32 {
        900
    }

    pub fn default_max_window_secs() -> u32 {
        24 * 3600
    }

    pub fn default_max_snap_distance_m() -> u32 {
        10_000
    }

    pub fn default_edge_snap() -> bool {
        true
    }

    pub fn default_edge_snap_radius_m() -> f64 {
        300.0
    }

    pub fn default_street_time() -> crate::structures::StreetTimeModel {
        crate::structures::StreetTimeModel::default()
    }

    pub fn default_distance_budget() -> f64 {
        0.5
    }

    pub fn default_epsilon() -> crate::structures::cost::Epsilon {
        crate::structures::EpsilonConfig::default().to_epsilon()
    }

    pub fn default_bike_bucket_cyc_k() -> f64 {
        0.11
    }

    pub fn default_bike_bucket_dpl_k() -> f64 {
        0.013
    }

    pub fn default_bike_select_dplus() -> bool {
        // Off: A/B showed demoting D+ removes the long-walk-for-flatness extreme
        // (push 1114→44 m) and makes the search 3–4× faster, with comparable diversity.
        false
    }

    pub fn set_bike_select_dplus(&mut self, v: bool) {
        self.bike_select_dplus = v;
    }

    pub fn default_variance_model() -> crate::structures::cost::VarianceModel {
        crate::structures::cost::VarianceModel::default()
    }

    pub fn default_cost_weights() -> crate::structures::cost::CostWeights {
        crate::structures::cost::CostWeights::default()
    }

    pub fn default_representatives_k() -> usize {
        6
    }

    pub fn default_multiobj_street() -> bool {
        false
    }

    pub fn default_multiobj_street_max_len_m() -> usize {
        // 25 km comfortably covers urban bike legs and the ~5 km vehicle access radius;
        // the scalar search is corridor-bounded so this is a guard, not a perf cliff.
        25_000
    }

    pub fn default_champion_time_tiebreak() -> f64 {
        0.1
    }

    pub fn default_alt_max_share_factor() -> f64 {
        0.6
    }

    pub fn default_systematic_cv() -> f64 {
        0.05
    }

    pub fn default_balance() -> crate::structures::cost::BalanceWeights {
        crate::structures::cost::BalanceWeights::default()
    }

    /// Rebuild non-serialized runtime indices from serialized data. Must be
    /// called after construction (build) and after deserialization (load), since
    /// `trip_id_to_index` is `#[serde(skip)]`.
    pub fn build_runtime_indices(&mut self) {
        self.trip_id_to_index = self
            .transit_trip_ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), TripId(i as u32)))
            .collect();
        self.stop_id_to_index = self
            .transit_stop_ids
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty())
            .map(|(i, s)| (s.clone(), i))
            .collect();
        self.rebuild_station_lookups();
    }

    /// Derive the station lookup maps from the serialized `transit_stations`.
    /// Called on both build and load, mirroring the other runtime-index rebuilds.
    pub fn rebuild_station_lookups(&mut self) {
        self.station_id_to_index = self
            .transit_stations
            .iter()
            .enumerate()
            .map(|(i, st)| (st.id.clone(), i))
            .collect();

        self.transit_stop_to_station = vec![u32::MAX; self.transit_stop_to_node.len()];
        for (station_idx, st) in self.transit_stations.iter().enumerate() {
            for &compact in &st.platform_stop_indices {
                if compact < self.transit_stop_to_station.len() {
                    self.transit_stop_to_station[compact] = station_idx as u32;
                }
            }
        }
    }

    /// Compact platform stop indices for a GTFS station id, if known.
    pub fn station_platforms(&self, station_id: &str) -> Option<Vec<usize>> {
        let idx = *self.station_id_to_index.get(station_id)?;
        Some(self.transit_stations[idx].platform_stop_indices.clone())
    }

    /// Compact stop index for a GTFS `stop_id` string, if known.
    pub fn stop_index_of(&self, stop_id: &str) -> Option<usize> {
        self.stop_id_to_index.get(stop_id).copied()
    }

    /// Original GTFS `route_id` string for the route of an internal `TripId`, if known.
    /// Returns `None` when either the trip or the route-id mapping is absent (e.g. on
    /// old `graph.bin` files loaded before `transit_route_ids` was added).
    pub fn route_id_of_trip(&self, trip: TripId) -> Option<&str> {
        let route_idx = self.transit_trips.get(trip.0 as usize)?.route_id.0 as usize;
        self.transit_route_ids.get(route_idx).map(|s| s.as_str())
    }

    /// Original GTFS `trip_id` string for an internal `TripId`, if known.
    pub fn trip_id_str(&self, trip: TripId) -> Option<&str> {
        self.transit_trip_ids
            .get(trip.0 as usize)
            .map(|s| s.as_str())
    }

    /// Internal `TripId` for a GTFS `trip_id` string, if known.
    pub fn trip_index_of(&self, trip_id: &str) -> Option<TripId> {
        self.trip_id_to_index.get(trip_id).copied()
    }

    /// Cross-reference check run after deserialization.  Returns an error if any index is
    /// out-of-bounds, which indicates a stale or corrupt `graph.bin`.
    pub fn validate(&self) -> Result<(), String> {
        let n_services = self.transit_services.len();
        let n_routes = self.transit_routes.len();
        let n_trips = self.transit_trips.len();
        let n_patterns = self.transit_patterns.len();

        for (i, trip) in self.transit_trips.iter().enumerate() {
            if trip.service_id.0 as usize >= n_services {
                return Err(format!(
                    "transit_trips[{i}].service_id={} out of bounds (transit_services.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip.service_id.0, n_services
                ));
            }
            if trip.route_id.0 as usize >= n_routes {
                return Err(format!(
                    "transit_trips[{i}].route_id={} out of bounds (transit_routes.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip.route_id.0, n_routes
                ));
            }
        }

        for (i, &trip_id) in self.transit_pattern_trips.iter().enumerate() {
            if trip_id.0 as usize >= n_trips {
                return Err(format!(
                    "transit_pattern_trips[{i}]={} out of bounds (transit_trips.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    trip_id.0, n_trips
                ));
            }
        }

        for (i, &(pat_id, _)) in self.transit_stop_patterns.iter().enumerate() {
            if pat_id.0 as usize >= n_patterns {
                return Err(format!(
                    "transit_stop_patterns[{i}].pattern_id={} out of bounds (transit_patterns.len={}); \
                     graph.bin is stale — rebuild with --build --save",
                    pat_id.0, n_patterns
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::gtfs::{RouteId, ServiceId, TripInfo};
    use crate::structures::raptor::PatternID;

    #[test]
    fn new_index_has_default_query_caps() {
        let idx = RaptorIndex::new();
        assert_eq!(idx.max_window_secs, 24 * 3600);
        assert_eq!(idx.max_snap_distance_m, 10_000);
    }

    fn make_trip(route_id: u32, service_id: u32) -> TripInfo {
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(route_id),
            service_id: ServiceId(service_id),
            bikes_allowed: None,
        }
    }

    #[test]
    fn validate_empty_index_ok() {
        assert!(RaptorIndex::new().validate().is_ok());
    }

    #[test]
    fn validate_consistent_trips_ok() {
        let mut idx = RaptorIndex::new();
        idx.transit_services
            .push(crate::ingestion::gtfs::ServicePattern {
                days_of_week: 0x1f,
                start_date: 0,
                end_date: u32::MAX,
                added_dates: vec![],
                removed_dates: vec![],
            });
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Line 1".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_trips.push(make_trip(0, 0));
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn validate_bad_service_id_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "1".into(),
            route_long_name: "Line 1".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_trips.push(make_trip(0, 9999));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("service_id"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_route_id_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_services
            .push(crate::ingestion::gtfs::ServicePattern {
                days_of_week: 0x1f,
                start_date: 0,
                end_date: u32::MAX,
                added_dates: vec![],
                removed_dates: vec![],
            });
        idx.transit_trips.push(make_trip(9999, 0));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("route_id"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_pattern_trip_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_pattern_trips
            .push(crate::ingestion::gtfs::TripId(9999));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("pattern_trips"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn validate_bad_stop_pattern_returns_error() {
        let mut idx = RaptorIndex::new();
        idx.transit_stop_patterns.push((PatternID(9999), 0));
        let err = idx.validate().unwrap_err();
        assert!(err.contains("stop_patterns"), "unexpected error: {err}");
        assert!(err.contains("rebuild"), "no rebuild hint: {err}");
    }

    #[test]
    fn trip_id_round_trips_through_runtime_index() {
        let mut idx = RaptorIndex::new();
        idx.transit_trip_ids = vec!["trip_a".into(), "trip_b".into(), "trip_c".into()];
        idx.build_runtime_indices();

        assert_eq!(
            idx.trip_id_str(crate::ingestion::gtfs::TripId(1)),
            Some("trip_b")
        );
        assert_eq!(
            idx.trip_index_of("trip_c"),
            Some(crate::ingestion::gtfs::TripId(2))
        );
        assert_eq!(
            idx.trip_index_of("trip_a"),
            Some(crate::ingestion::gtfs::TripId(0))
        );
        assert_eq!(idx.trip_index_of("nope"), None);
        assert_eq!(idx.trip_id_str(crate::ingestion::gtfs::TripId(99)), None);
    }

    #[test]
    fn route_id_of_trip_resolves_via_trip_info() {
        use crate::ingestion::gtfs::{RouteId, ServiceId, TripId, TripInfo};
        let mut idx = RaptorIndex::new();
        idx.transit_route_ids = vec!["gtfs-route-A".into(), "gtfs-route-B".into()];
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "A".into(),
            route_long_name: "Route A".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_routes.push(crate::ingestion::gtfs::RouteInfo {
            route_short_name: "B".into(),
            route_long_name: "Route B".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        });
        idx.transit_services
            .push(crate::ingestion::gtfs::ServicePattern {
                days_of_week: 0x7F,
                start_date: 0,
                end_date: 9999,
                added_dates: vec![],
                removed_dates: vec![],
            });
        idx.transit_trips.push(TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        });
        idx.transit_trips.push(TripInfo {
            trip_headsign: None,
            route_id: RouteId(1),
            service_id: ServiceId(0),
            bikes_allowed: None,
        });

        assert_eq!(idx.route_id_of_trip(TripId(0)), Some("gtfs-route-A"));
        assert_eq!(idx.route_id_of_trip(TripId(1)), Some("gtfs-route-B"));
        assert_eq!(idx.route_id_of_trip(TripId(99)), None, "out-of-bounds trip → None");
    }

    #[test]
    fn route_id_of_trip_none_when_transit_route_ids_empty() {
        use crate::ingestion::gtfs::{RouteId, ServiceId, TripId, TripInfo};
        let mut idx = RaptorIndex::new();
        idx.transit_trips.push(TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        });
        assert_eq!(
            idx.route_id_of_trip(TripId(0)),
            None,
            "empty transit_route_ids → None (old graph.bin graceful degradation)"
        );
    }

    #[test]
    fn station_lookups_rebuild_from_serialized_stations() {
        let mut idx = RaptorIndex::new();
        idx.transit_stop_to_node = vec![NodeID(0), NodeID(1), NodeID(2)];
        idx.transit_stations = vec![
            StationInfo {
                id: "HUB".into(),
                name: "Hub".into(),
                lat_lng: LatLng {
                    latitude: 51.0,
                    longitude: 3.7,
                },
                operators: vec!["Op".into()],
                modes: vec!["Bus".into()],
                lines: Vec::new(),
                platform_stop_indices: vec![0, 1],
            },
            StationInfo {
                id: "SOLO".into(),
                name: "Solo".into(),
                lat_lng: LatLng {
                    latitude: 50.0,
                    longitude: 4.0,
                },
                operators: vec![],
                modes: vec![],
                lines: Vec::new(),
                platform_stop_indices: vec![2],
            },
        ];

        idx.rebuild_station_lookups();

        assert_eq!(idx.station_id_to_index["HUB"], 0);
        assert_eq!(idx.station_id_to_index["SOLO"], 1);
        assert_eq!(idx.transit_stop_to_station, vec![0, 0, 1]);
        assert_eq!(idx.station_platforms("HUB"), Some(vec![0, 1]));
        assert_eq!(idx.station_platforms("nope"), None);
    }

    #[test]
    fn representatives_k_defaults_to_six() {
        assert_eq!(RaptorIndex::new().representatives_k, 6);
    }

    #[test]
    fn systematic_cv_defaults_to_five_percent() {
        assert_eq!(RaptorIndex::new().systematic_cv, 0.05);
    }

    #[test]
    fn balance_defaults_present() {
        assert_eq!(
            RaptorIndex::new().balance,
            crate::structures::cost::BalanceWeights::default()
        );
    }

    #[test]
    fn raptor_index_new_is_empty() {
        let idx = RaptorIndex::new();
        assert!(idx.transit_departures.is_empty());
        assert!(idx.transit_services.is_empty());
        assert!(idx.transit_trips.is_empty());
        assert!(idx.transit_routes.is_empty());
        assert!(idx.transit_agencies.is_empty());
        assert!(idx.transit_patterns.is_empty());
        assert!(idx.transit_pattern_stops.is_empty());
        assert!(idx.transit_stop_patterns.is_empty());
        assert!(idx.transit_stop_transfers.is_empty());
        assert!(idx.transit_pattern_stop_times.is_empty());
        assert!(idx.transit_pattern_trips.is_empty());
        assert!(idx.transit_idx_pattern_stops.is_empty());
        assert!(idx.transit_idx_stop_patterns.is_empty());
        assert!(idx.transit_idx_stop_transfers.is_empty());
        assert!(idx.transit_idx_pattern_stop_times.is_empty());
        assert!(idx.transit_idx_pattern_trips.is_empty());
        assert!(idx.transit_delay_models.is_empty());
        assert!(idx.transit_node_to_stop.is_empty());
        assert!(idx.transit_stop_to_node.is_empty());
        assert!(idx.transit_trip_ids.is_empty());
        assert!(idx.trip_id_to_index.is_empty());
        assert!(idx.transit_stop_ids.is_empty());
        assert!(idx.stop_id_to_index.is_empty());
        assert!(idx.transit_stop_reverse_transfers.is_empty());
        assert!(idx.transit_idx_stop_reverse_transfers.is_empty());
        assert!(idx.transit_pattern_shapes.is_empty());
        assert!(idx.transit_pattern_shape_stop_idx.is_empty());
        assert!(idx.railway_nodes.is_empty());
        assert!(idx.railway_adj.is_empty());
        assert_eq!(idx.min_access_secs, 600);
        assert_eq!(idx.walking_speed_mps, 1.2);
        assert_eq!(idx.cycling_speed_mps, 4.2);
        assert_eq!(idx.driving_speed_mps, 11.0);
        assert_eq!(idx.vehicle_access_secs, 1200);
        assert_eq!(idx.reliability_bucket_edges, vec![0.50, 0.80, 0.95]);
        assert_eq!(idx.arrival_slack_secs, 900);
    }
}
