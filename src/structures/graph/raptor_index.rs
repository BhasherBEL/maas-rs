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

/// `color`/`text_color` are 6-char hex (no leading `#`), or `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StationLine {
    pub mode: String,
    pub short_name: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

/// One physical transit station: GTFS platforms collapsed by shared non-empty
/// `parent_station`; parent-less stops each form their own station.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationInfo {
    /// Shared `parent_station`, or the lone stop's `stop_id`.
    pub id: String,
    pub name: String,
    pub lat_lng: LatLng,
    pub operators: Vec<String>,
    #[serde(default)]
    pub modes: Vec<String>,
    #[serde(default)]
    pub lines: Vec<StationLine>,
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

    /// Must carry the raw GTFS string id to match realtime alert `route_id` fields.
    #[serde(default)]
    pub transit_route_ids: Vec<String>,

    #[serde(default)]
    pub transit_trip_ids: Vec<String>,
    #[serde(skip)]
    pub trip_id_to_index: HashMap<String, TripId>,

    #[serde(default)]
    pub transit_stop_ids: Vec<String>,
    #[serde(skip)]
    pub stop_id_to_index: HashMap<String, usize>,

    /// Serialized copy of stop names (originals live only in `NodeData::TransitStop`,
    /// dropped by interior-node contraction).
    #[serde(default)]
    pub transit_stop_names: Vec<String>,

    #[serde(default)]
    pub transit_stop_platform_codes: Vec<Option<String>>,

    #[serde(default)]
    pub transit_stations: Vec<StationInfo>,
    #[serde(skip)]
    pub transit_stop_to_station: Vec<u32>,
    #[serde(skip)]
    pub station_id_to_index: HashMap<String, usize>,

    #[serde(default)]
    pub transit_stop_reverse_transfers: Vec<(usize, u32)>,
    #[serde(default)]
    pub transit_idx_stop_reverse_transfers: Vec<Lookup>,

    /// When `true`, a long inter-leg transfer (> `MAX_TRANSFER_DISTANCE_M`, off the
    /// capped reverse-footpath table) is tightened using the plan's own reconstructed
    /// walk. Default `false` keeps the chain sweep byte-identical to the backward pass.
    #[serde(skip)]
    pub tighten_long_transfers: bool,

    #[serde(default)]
    pub transit_pattern_shapes: Vec<Vec<LatLng>>,
    #[serde(default)]
    pub transit_pattern_shape_stop_idx: Vec<Vec<u32>>,

    /// Per pattern, per inter-stop segment (index `s-1` for edge `s-1`→`s`), the
    /// transit edge's `timetable_segment`. Empty unless built; reconstruction falls
    /// back to the `g.edges` scan when empty.
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

    /// Radius (m) for merging parent-less GTFS stops into a same-named station.
    /// Read during GTFS ingestion, so must be set before that phase.
    #[serde(skip, default = "RaptorIndex::default_station_merge_radius_m")]
    pub station_merge_radius_m: f64,

    #[serde(skip, default = "RaptorIndex::default_cycling_speed_mps")]
    pub cycling_speed_mps: f64,

    #[serde(skip, default = "RaptorIndex::default_driving_speed_mps")]
    pub driving_speed_mps: f64,

    #[serde(skip, default = "RaptorIndex::default_vehicle_access_secs")]
    pub vehicle_access_secs: u32,

    // Budget = crow-flies time × `vehicle_access_fraction`, clamped to
    // [`vehicle_access_secs`, `vehicle_access_max_secs`].
    #[serde(skip, default = "RaptorIndex::default_vehicle_access_fraction")]
    pub vehicle_access_fraction: f64,
    #[serde(skip, default = "RaptorIndex::default_vehicle_access_max_secs")]
    pub vehicle_access_max_secs: u32,

    #[serde(skip, default = "RaptorIndex::default_reliability_bucket_edges")]
    pub reliability_bucket_edges: Vec<f32>,

    #[serde(skip, default = "RaptorIndex::default_arrival_slack_secs")]
    pub arrival_slack_secs: u32,

    /// When true, inter-stop transfers use a live per-round MCR foot-Dijkstra instead
    /// of the precomputed ≤`MAX_TRANSFER_DISTANCE_M` table, discovering >1 km transfers.
    #[serde(skip, default = "RaptorIndex::default_unrestricted_transfers")]
    pub unrestricted_transfers: bool,

    /// When true, foot access/egress uses the exact CCH instead of the two-pass foot
    /// Dijkstra. Requires a built `cch`; falls back to two-pass when absent.
    #[serde(skip, default = "RaptorIndex::default_use_cch_access")]
    pub use_cch_access: bool,

    #[serde(skip, default = "RaptorIndex::default_profile_latency")]
    pub profile_latency: bool,

    #[serde(skip, default = "RaptorIndex::default_max_window_secs")]
    pub max_window_secs: u32,

    /// Isochrone sampling grid step, in METRES.
    #[serde(skip, default = "RaptorIndex::default_travel_map_grid_step_m")]
    pub travel_map_grid_step_m: f64,

    /// Safety cap on isochrone grid cells; the step is coarsened when exceeded.
    #[serde(skip, default = "RaptorIndex::default_travel_map_max_cells")]
    pub travel_map_max_cells: u64,

    /// Isochrone departure-window sample interval, in SECONDS.
    #[serde(skip, default = "RaptorIndex::default_travel_map_window_sample_secs")]
    pub travel_map_window_sample_secs: u32,

    #[serde(skip, default = "RaptorIndex::default_max_snap_distance_m")]
    pub max_snap_distance_m: u32,

    #[serde(skip, default = "RaptorIndex::default_edge_snap_radius_m")]
    pub edge_snap_radius_m: f64,

    #[serde(skip, default)]
    pub bike_profile: crate::structures::BikeProfile,

    #[serde(skip, default = "RaptorIndex::default_street_time")]
    pub street_time: crate::structures::StreetTimeModel,

    /// RCSP distance budget multiplier δ: paths up to (1+δ)·shortest are explored.
    #[serde(skip, default = "RaptorIndex::default_distance_budget")]
    pub distance_budget: f64,

    #[serde(skip, default = "RaptorIndex::default_epsilon")]
    pub epsilon: crate::structures::cost::Epsilon,

    /// Bike bucketing cell-size coefficients per metre of origin→dest distance, on the
    /// CyclewayDeficit and Dplus axes. Cell size = k·D; `0.0` disables bucketing.
    #[serde(skip, default = "RaptorIndex::default_bike_bucket_cyc_k")]
    pub bike_bucket_cyc_k: f64,
    #[serde(skip, default = "RaptorIndex::default_bike_bucket_dpl_k")]
    pub bike_bucket_dpl_k: f64,

    /// Drive bucketing cell-size coefficient per metre of origin→dest distance, on the
    /// Variance axis. Cell size = k·D; `0.0` disables bucketing.
    #[serde(skip, default = "RaptorIndex::default_drive_bucket_var_k")]
    pub drive_bucket_var_k: f64,

    /// Walk bucketing cell-size coefficient per metre of origin→dest distance, on the
    /// Surface axis. Cell size = k·D; `0.0` disables bucketing.
    #[serde(skip, default = "RaptorIndex::default_walk_bucket_surf_k")]
    pub walk_bucket_surf_k: f64,

    /// Whether D+ (ascent) is a bike SELECTION/dominance axis. Default false.
    #[serde(skip, default = "RaptorIndex::default_bike_select_dplus")]
    pub bike_select_dplus: bool,

    #[serde(skip, default = "RaptorIndex::default_variance_model")]
    pub variance_model: crate::structures::cost::VarianceModel,

    #[serde(skip, default = "RaptorIndex::default_cost_weights")]
    pub cost_weights: crate::structures::cost::CostWeights,

    #[serde(skip, default = "RaptorIndex::default_representatives_k")]
    pub representatives_k: usize,

    /// ADGW limited-sharing threshold: an alternative bike/car leg is dropped if it
    /// shares more than this fraction of its length with a higher-ranked one.
    #[serde(skip, default = "RaptorIndex::default_alt_max_share_factor")]
    pub alt_max_share_factor: f64,

    #[serde(skip, default = "RaptorIndex::default_systematic_cv")]
    pub systematic_cv: f64,

    #[serde(skip, default = "RaptorIndex::default_balance")]
    pub balance: crate::structures::cost::BalanceWeights,

    /// `enabled = false` (the default) makes every fare code path a no-op,
    /// byte-identical to pre-feature routing.
    #[serde(skip, default)]
    pub fare_model: crate::structures::cost::FareModel,

    /// RouteId → `OperatorFareId`, parallel to `transit_routes`. Empty when fares
    /// are disabled or the index is unbuilt.
    #[serde(skip, default)]
    pub operator_fare_of_route: Vec<crate::structures::cost::OperatorFareId>,

    /// `unknown_operator_names[slot]` is the normalized agency name for that
    /// unmodeled-operator slot.
    #[serde(skip, default)]
    pub unknown_operator_names: Vec<String>,

    /// Per-pattern cumulative railway distance (m) from the pattern's first stop to
    /// each stop, parallel to `transit_idx_pattern_stops`. Per-km SNCB fare is
    /// `per_km × (cum[alight] - cum[board])`. Rebuilt on load.
    #[serde(skip, default)]
    pub sncb_pattern_cum_railway_m: Vec<Vec<f64>>,

    /// SNCB flat-agglomeration membership per compact stop, parallel to
    /// `transit_stop_to_node`. Two stops in the SAME non-`None` zone have zero
    /// chargeable railway distance between them. Rebuilt on load.
    #[serde(skip, default)]
    pub sncb_stop_zone: Vec<crate::structures::cost::Agglomeration>,

    /// Airport special-OD membership per compact stop: an SNCB journey boarding OR
    /// alighting at an airport prices at fixed `airport_od_cents`. Rebuilt on load.
    #[serde(skip, default)]
    pub sncb_airport_stop: Vec<bool>,

    /// `sncb_ref_to_stop[z]`: railway distance (m) from zone `z`'s reference node to
    /// every compact SNCB stop, in `fare_model.agglomerations` order; `INFINITY` if
    /// unreachable. Rebuilt on load.
    #[serde(skip, default)]
    pub sncb_ref_to_stop: Vec<Vec<f64>>,

    /// `sncb_ref_to_ref[zi][zj]`, both in `fare_model.agglomerations` order: fixed
    /// per-km distance between any two zone reference nodes. `INFINITY` if unreachable.
    #[serde(skip, default)]
    pub sncb_ref_to_ref: Vec<Vec<f64>>,

    /// Chosen reference railway-node index per agglomeration; `None` if unresolved.
    #[serde(skip, default)]
    pub sncb_zone_ref_node: Vec<Option<usize>>,
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
            tighten_long_transfers: false,

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
            unrestricted_transfers: Self::default_unrestricted_transfers(),
            use_cch_access: Self::default_use_cch_access(),
            profile_latency: Self::default_profile_latency(),
            max_window_secs: Self::default_max_window_secs(),
            travel_map_grid_step_m: Self::default_travel_map_grid_step_m(),
            travel_map_max_cells: Self::default_travel_map_max_cells(),
            travel_map_window_sample_secs: Self::default_travel_map_window_sample_secs(),
            max_snap_distance_m: Self::default_max_snap_distance_m(),
            edge_snap_radius_m: Self::default_edge_snap_radius_m(),
            bike_profile: crate::structures::BikeProfile::default(),
            street_time: Self::default_street_time(),
            distance_budget: Self::default_distance_budget(),
            epsilon: Self::default_epsilon(),
            bike_bucket_cyc_k: Self::default_bike_bucket_cyc_k(),
            bike_bucket_dpl_k: Self::default_bike_bucket_dpl_k(),
            drive_bucket_var_k: Self::default_drive_bucket_var_k(),
            walk_bucket_surf_k: Self::default_walk_bucket_surf_k(),
            bike_select_dplus: Self::default_bike_select_dplus(),
            variance_model: Self::default_variance_model(),
            cost_weights: Self::default_cost_weights(),
            representatives_k: Self::default_representatives_k(),
            alt_max_share_factor: Self::default_alt_max_share_factor(),
            systematic_cv: Self::default_systematic_cv(),
            balance: Self::default_balance(),
            fare_model: crate::structures::cost::FareModel::default(),
            operator_fare_of_route: Vec::new(),
            unknown_operator_names: Vec::new(),
            sncb_pattern_cum_railway_m: Vec::new(),
            sncb_stop_zone: Vec::new(),
            sncb_airport_stop: Vec::new(),
            sncb_ref_to_stop: Vec::new(),
            sncb_ref_to_ref: Vec::new(),
            sncb_zone_ref_node: Vec::new(),
        }
    }

    pub fn default_min_access_secs() -> u32 {
        10 * 60
    }

    pub fn default_walking_speed_mps() -> f64 {
        1.2
    }

    pub fn default_station_merge_radius_m() -> f64 {
        250.0
    }

    pub fn default_cycling_speed_mps() -> f64 {
        4.2
    }

    pub fn default_driving_speed_mps() -> f64 {
        11.0
    }

    pub fn default_vehicle_access_secs() -> u32 {
        20 * 60
    }

    pub fn default_vehicle_access_fraction() -> f64 {
        0.06
    }

    pub fn default_vehicle_access_max_secs() -> u32 {
        45 * 60
    }

    pub fn default_reliability_bucket_edges() -> Vec<f32> {
        vec![0.50, 0.80, 0.95]
    }

    pub fn default_arrival_slack_secs() -> u32 {
        900
    }

    pub fn default_unrestricted_transfers() -> bool {
        false
    }

    pub fn default_use_cch_access() -> bool {
        false
    }

    pub fn default_profile_latency() -> bool {
        false
    }

    pub fn default_max_window_secs() -> u32 {
        24 * 3600
    }

    pub fn default_travel_map_grid_step_m() -> f64 {
        300.0
    }

    pub fn default_travel_map_max_cells() -> u64 {
        150_000
    }

    pub fn default_travel_map_window_sample_secs() -> u32 {
        600
    }

    pub fn default_max_snap_distance_m() -> u32 {
        10_000
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

    pub fn default_drive_bucket_var_k() -> f64 {
        0.05
    }

    pub fn default_walk_bucket_surf_k() -> f64 {
        0.025
    }

    pub fn default_bike_select_dplus() -> bool {
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

    pub fn default_alt_max_share_factor() -> f64 {
        0.6
    }

    pub fn default_systematic_cv() -> f64 {
        0.05
    }

    pub fn default_balance() -> crate::structures::cost::BalanceWeights {
        crate::structures::cost::BalanceWeights::default()
    }

    /// Rebuild non-serialized runtime indices. Call after build and after load.
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
        self.rebuild_operator_fare_lookup();
    }

    fn normalize_agency_name(name: &str) -> String {
        name.trim().to_ascii_uppercase()
    }

    /// Token-based match (both args pre-normalized): feeds spell some operators as
    /// slash/whitespace-joined multi-brand strings (`NMBS/SNCB` for config's `SNCB`),
    /// so an exact compare silently fails.
    fn agency_matches_operator(agency_name: &str, op_token: &str) -> bool {
        if agency_name == op_token {
            return true;
        }
        agency_name
            .split(|c: char| c == '/' || c.is_whitespace())
            .any(|tok| tok == op_token)
    }

    /// Build the RouteId → `OperatorFareId` lookup. A route whose agency matches a
    /// modeled operator resolves to its model; others get a stable `unknown[]` slot
    /// (`0..N_OP`). No-op when fares are disabled.
    pub fn rebuild_operator_fare_lookup(&mut self) {
        use crate::structures::cost::{N_OP, OperatorFareId};
        if !self.fare_model.enabled {
            self.operator_fare_of_route = Vec::new();
            self.unknown_operator_names = Vec::new();
            return;
        }
        let modeled: Vec<(String, crate::structures::cost::OperatorModel)> = self
            .fare_model
            .operators
            .iter()
            .map(|op| (Self::normalize_agency_name(&op.name), op.model))
            .collect();
        let mut op_matched = vec![false; modeled.len()];
        let mut unknown_slot: HashMap<String, usize> = HashMap::new();
        let mut next_slot = 0usize;
        let mut slot_names: Vec<String> = vec![String::new(); N_OP];

        let mut lookup = Vec::with_capacity(self.transit_routes.len());
        for route in &self.transit_routes {
            let display_name = self
                .transit_agencies
                .get(route.agency_id.0 as usize)
                .map(|a| a.name.trim().to_string())
                .unwrap_or_default();
            let agency_name = Self::normalize_agency_name(&display_name);
            let matched = modeled
                .iter()
                .position(|(tok, _)| Self::agency_matches_operator(&agency_name, tok));
            let id = if let Some(i) = matched {
                op_matched[i] = true;
                let model = match modeled[i].1 {
                    crate::structures::cost::OperatorModel::TimeWindowFlatTiered {
                        single_cents,
                        card6_cents,
                        card6_reduced_cents,
                        ..
                    } => {
                        let opf = &self.fare_model.operators[i];
                        let is_express = crate::ingestion::gtfs::tec_route_is_express(
                            route,
                            &opf.express_route_names,
                            &opf.express_route_prefixes,
                        );
                        let (single_cents, card6_cents, card6_reduced_cents) = if is_express {
                            (
                                opf.express_single_cents,
                                opf.express_card6_cents,
                                opf.express_card6_reduced_cents,
                            )
                        } else {
                            (single_cents, card6_cents, card6_reduced_cents)
                        };
                        crate::structures::cost::OperatorModel::TimeWindowFlatTiered {
                            is_express,
                            single_cents,
                            card6_cents,
                            card6_reduced_cents,
                        }
                    }
                    other => other,
                };
                OperatorFareId::Modeled { model }
            } else {
                let name_for_slot = display_name.clone();
                let slot = *unknown_slot.entry(agency_name).or_insert_with(|| {
                    let s = next_slot.min(N_OP - 1);
                    next_slot += 1;
                    if slot_names[s].is_empty() {
                        slot_names[s] = name_for_slot;
                    }
                    s
                });
                OperatorFareId::Unknown { slot }
            };
            lookup.push(id);
        }
        self.operator_fare_of_route = lookup;
        self.unknown_operator_names = slot_names;

        // Warn on any configured fare operator that matched ZERO agencies.
        let unmatched: Vec<&str> = modeled
            .iter()
            .zip(&op_matched)
            .filter(|(_, m)| !**m)
            .map(|((tok, _), _)| tok.as_str())
            .collect();
        if !unmatched.is_empty() {
            let available: Vec<String> = self
                .transit_agencies
                .iter()
                .map(|a| a.name.clone())
                .collect();
            tracing::warn!(
                "fares: configured operator(s) {:?} matched NO agency — their fares \
                 are inert (routes price as unknown). Available agency names: {:?}",
                unmatched,
                available,
            );
        }
    }

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

    pub fn station_platforms(&self, station_id: &str) -> Option<Vec<usize>> {
        let idx = *self.station_id_to_index.get(station_id)?;
        Some(self.transit_stations[idx].platform_stop_indices.clone())
    }

    pub fn stop_index_of(&self, stop_id: &str) -> Option<usize> {
        self.stop_id_to_index.get(stop_id).copied()
    }

    pub fn route_id_of_trip(&self, trip: TripId) -> Option<&str> {
        let route_idx = self.transit_trips.get(trip.0 as usize)?.route_id.0 as usize;
        self.transit_route_ids.get(route_idx).map(|s| s.as_str())
    }

    pub fn trip_id_str(&self, trip: TripId) -> Option<&str> {
        self.transit_trip_ids
            .get(trip.0 as usize)
            .map(|s| s.as_str())
    }

    pub fn trip_index_of(&self, trip_id: &str) -> Option<TripId> {
        self.trip_id_to_index.get(trip_id).copied()
    }

    /// Post-deserialization cross-reference check; an out-of-bounds index means a
    /// stale or corrupt `graph.bin`.
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

    fn route(agency: u16) -> crate::ingestion::gtfs::RouteInfo {
        crate::ingestion::gtfs::RouteInfo {
            route_short_name: "r".into(),
            route_long_name: "route".into(),
            route_type: gtfs_structures::RouteType::Bus,
            agency_id: crate::ingestion::gtfs::AgencyId(agency),
            route_color: None,
            route_text_color: None,
        }
    }

    fn agency(name: &str) -> crate::ingestion::gtfs::AgencyInfo {
        crate::ingestion::gtfs::AgencyInfo {
            name: name.into(),
            url: String::new(),
            timezone: String::new(),
        }
    }

    #[test]
    fn operator_fare_lookup_resolves_modeled_and_unknown() {
        use crate::structures::cost::{
            FareModel, KnownEurosEpsilon, OperatorFare, OperatorFareId, OperatorModel,
        };
        let mut idx = RaptorIndex::new();
        idx.transit_agencies = vec![agency("STIB"), agency("De Lijn"), agency("TEC")];
        idx.transit_routes = vec![route(0), route(1), route(2), route(1)];
        idx.fare_model = FareModel {
            enabled: true,
            known_euros_epsilon: KnownEurosEpsilon::default(),
            operators: vec![OperatorFare {
                name: "stib".into(),
                model: OperatorModel::TimeWindowFlat {
                    ticket_cents: 210,
                    card_cents: None,
                    validity_secs: 5400,
                    operator: crate::structures::cost::TimeWindowOperator::Stib,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            }],
            agglomerations: Vec::new(),
            ..FareModel::default()
        };
        idx.rebuild_operator_fare_lookup();

        assert!(matches!(
            idx.operator_fare_of_route[0],
            OperatorFareId::Modeled { .. }
        ));
        let (s1, s2, s3) = match (
            idx.operator_fare_of_route[1],
            idx.operator_fare_of_route[2],
            idx.operator_fare_of_route[3],
        ) {
            (
                OperatorFareId::Unknown { slot: a },
                OperatorFareId::Unknown { slot: b },
                OperatorFareId::Unknown { slot: c },
            ) => (a, b, c),
            _ => panic!("unmodeled routes must resolve to Unknown"),
        };
        assert_ne!(s1, s2, "distinct unmodeled operators get distinct slots");
        assert_eq!(s1, s3, "same unmodeled operator reuses its slot");
        assert_eq!(idx.unknown_operator_names[s1], "De Lijn");
        assert_eq!(idx.unknown_operator_names[s2], "TEC");
    }

    #[test]
    fn sncb_operator_matches_slashed_agency_name() {
        use crate::structures::cost::{
            FareModel, KnownEurosEpsilon, OperatorFare, OperatorFareId, OperatorModel,
        };
        let mut idx = RaptorIndex::new();
        idx.transit_agencies = vec![agency("NMBS/SNCB")];
        idx.transit_routes = vec![route(0)];
        idx.fare_model = FareModel {
            enabled: true,
            known_euros_epsilon: KnownEurosEpsilon::default(),
            operators: vec![OperatorFare {
                name: "SNCB".into(),
                model: OperatorModel::DistanceBasePerKm {
                    tariff: crate::structures::cost::DistanceTariff::Band {
                        per_km_rate_cents: 12.40,
                        thresholds: [36, 51],
                        coeffs: [1.40, 1.50, 1.60],
                        min_km: 3,
                        max_km: 118,
                        floor_cents: 260,
                    },
                    rules: crate::structures::cost::SncbTimeRules {
                        peak_windows: [(0, 0); 2],
                        n_peak_windows: 0,
                        weekend_discount_adult: 0.0,
                        weekend_discount_reduced: 0.0,
                        train_plus_offpeak_discount: 0.0,
                        train_plus_peak_cap_adult: u32::MAX,
                        train_plus_peak_cap_reduced: u32::MAX,
                    },
                    airport_od_cents: 0,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            }],
            agglomerations: Vec::new(),
            ..FareModel::default()
        };
        idx.rebuild_operator_fare_lookup();
        assert!(
            matches!(
                idx.operator_fare_of_route[0],
                OperatorFareId::Modeled {
                    model: OperatorModel::DistanceBasePerKm { .. }
                }
            ),
            "SNCB fare must resolve against the 'NMBS/SNCB' agency (not treated as unknown)"
        );
    }

    #[test]
    fn agency_matches_operator_token_rules() {
        assert!(RaptorIndex::agency_matches_operator("SNCB", "SNCB"));
        assert!(RaptorIndex::agency_matches_operator("NMBS/SNCB", "SNCB"));
        assert!(RaptorIndex::agency_matches_operator("NMBS/SNCB", "NMBS"));
        assert!(RaptorIndex::agency_matches_operator("DE LIJN", "LIJN"));
        assert!(!RaptorIndex::agency_matches_operator("SNCBX", "SNCB"));
    }

    #[test]
    fn operator_fare_lookup_empty_when_disabled() {
        let mut idx = RaptorIndex::new();
        idx.transit_agencies = vec![agency("STIB")];
        idx.transit_routes = vec![route(0)];
        idx.rebuild_operator_fare_lookup();
        assert!(
            idx.operator_fare_of_route.is_empty(),
            "disabled fares build no lookup (zero hot-loop overhead)"
        );
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
