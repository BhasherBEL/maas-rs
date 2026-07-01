use std::collections::HashMap;
use std::fs;

use gtfs_structures::RouteType;
use serde::Deserialize;

use crate::ingestion::cache::SourceLocation;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub build: BuildConfig,
    pub default_routing: RoutingDefaultConfig,
    #[serde(default)]
    pub server: ServerConfig,
    /// Background auto-update of remote GTFS feeds. Absent ⇒ disabled.
    #[serde(default)]
    pub auto_update: Option<AutoUpdateConfig>,
    /// Realtime delay feeds (GTFS-RT + custom STIB). Absent ⇒ disabled.
    #[serde(default)]
    pub realtime: Option<RealtimeConfig>,
    /// Minimum log level: trace | debug | info | warn | error  (default: info)
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RealtimeConfig {
    /// When false (or the section is absent) no realtime polling runs.
    #[serde(default)]
    pub enabled: bool,
    /// Seconds between poll cycles across all feeds.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    /// Per-request HTTP timeout for realtime polls.
    #[serde(default = "default_rt_timeout_secs")]
    pub request_timeout_secs: u64,
    /// A vehicle position is considered stale after this many seconds (unix time
    /// comparison). Routing and the live UI fall back to schedule interpolation
    /// when the position is absent or older than this window.
    #[serde(default = "default_vehicle_position_max_age_secs")]
    pub vehicle_position_max_age_secs: u64,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub feeds: Vec<RealtimeFeedConfig>,
}

fn default_poll_interval_secs() -> u64 {
    30
}

fn default_rt_timeout_secs() -> u64 {
    20
}

fn default_vehicle_position_max_age_secs() -> u64 {
    120
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default = "default_429_threshold")]
    pub consecutive_429_threshold: u32,
    #[serde(default = "default_throttled_interval_secs")]
    pub throttled_min_interval_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            consecutive_429_threshold: default_429_threshold(),
            throttled_min_interval_secs: default_throttled_interval_secs(),
        }
    }
}

fn default_429_threshold() -> u32 {
    3
}

fn default_throttled_interval_secs() -> u64 {
    60
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum RealtimeFeedConfig {
    /// Generic GTFS-Realtime protobuf trip-update feed (SNCB, TEC).
    #[serde(rename = "gtfs-rt")]
    GtfsRt {
        name: String,
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
    /// Custom STIB / MIVB waiting-times feed.
    #[serde(rename = "stib")]
    Stib {
        name: String,
        waiting_time_url: String,
        #[serde(default)]
        vehicle_position_url: Option<String>,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

#[derive(Debug, Deserialize, Clone)]
pub struct AutoUpdateConfig {
    /// When false (or the section is absent) no background updates run.
    #[serde(default)]
    pub enabled: bool,
    /// Cron schedule. Standard 5-field (e.g. "0 5 * * *") or 6-field (leading seconds).
    pub schedule: String,
    /// Directory for downloaded feeds and the hash sidecar.
    #[serde(default = "default_cache_dir")]
    pub cache_dir: String,
}

fn default_cache_dir() -> String {
    "cache".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: default_host(),
            port: default_port(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    3000
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize)]
pub struct BuildConfig {
    pub inputs: Vec<Ingestor>,
    pub output: String,
    #[serde(default = "default_osm_output")]
    pub osm_output: String,
    /// Optional GeoTIFF DEM for bike elevation cost/time (e.g. "path:data/dem.tif").
    #[serde(default)]
    pub elevation: Option<String>,
    /// Ramer–Douglas–Peucker vertical tolerance (meters) used to denoise each way's
    /// DEM elevation profile at ingestion, before deriving per-segment ascent. Sub-ε
    /// blips collapse to zero ascent; real climbs ≥ ε are preserved. Default 4.0 m.
    #[serde(default = "default_elevation_smoothing_epsilon")]
    pub elevation_smoothing_epsilon: f64,
    /// OSM `surface=*` → bike cruise-speed factor (relative to asphalt = 1.0),
    /// baked per-edge at ingest. Absent / sparse ⇒ the compiled-in table; an
    /// unlisted or untagged surface uses the unknown default (0.90). Re-tuning
    /// requires a graph rebuild (the factor is baked, like elevation smoothing).
    #[serde(default)]
    pub surface_speed_factors: crate::structures::SurfaceSpeedFactors,
    #[serde(default)]
    pub delay_models: Vec<DelayModelConfig>,
}

fn default_osm_output() -> String {
    "osm.bin".to_string()
}

fn default_elevation_smoothing_epsilon() -> f64 {
    4.0
}

#[derive(Debug, Deserialize)]
pub struct DelayModelConfig {
    pub mode: String,
    pub bins: Vec<(i32, f32)>,
}

impl DelayModelConfig {
    pub fn route_type(&self) -> Option<RouteType> {
        match self.mode.as_str() {
            "tram" => Some(RouteType::Tramway),
            "subway" | "metro" => Some(RouteType::Subway),
            "rail" | "train" => Some(RouteType::Rail),
            "bus" => Some(RouteType::Bus),
            "ferry" => Some(RouteType::Ferry),
            "cable_car" | "cablecar" => Some(RouteType::CableCar),
            "gondola" => Some(RouteType::Gondola),
            "funicular" => Some(RouteType::Funicular),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "ingestor")]
pub enum Ingestor {
    #[serde(rename = "osm/pbf")]
    OsmPbf(OsmPbfIngestor),
    #[serde(rename = "gtfs/generic")]
    GtfsGeneric(GtfsGenericIngestor),
    #[serde(rename = "gtfs/stib")]
    GtfsStib(GtfsGenericIngestor),
    #[serde(rename = "gtfs/sncb")]
    GtfsSncb(GtfsSncbIngestor),
    #[serde(rename = "best/add")]
    BeStAdd(BeStAddIngestor),
}

#[derive(Debug, Deserialize)]
pub struct OsmPbfIngestor {
    pub url: String,
    pub phase: Option<u8>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct GtfsGenericIngestor {
    pub name: String,
    pub url: String,
    pub phase: Option<u8>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct GtfsSncbIngestor {
    pub name: String,
    pub url: String,
    pub osm_url: String,
    pub phase: Option<u8>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

/// BeST-Add Belgian address feed (FULL XML zip from FPS BOSA). Ingested into a
/// sibling [`crate::structures::AddressIndex`] (not the routing graph), so it runs
/// on its own phase (default 2) outside the OSM/GTFS graph build. The download is
/// age-gated and safely skippable — see `services::build::load_or_build_address_index`.
#[derive(Debug, Deserialize)]
pub struct BeStAddIngestor {
    #[serde(default = "default_bestadd_name")]
    pub name: String,
    pub url: String,
    pub phase: Option<u8>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_bestadd_name() -> String {
    "bestadd".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoutingDefaultConfig {
    /// Minimum walk-radius (seconds) used for access/egress stop search.
    /// When absent, the compiled-in default (600 s = 10 min) is used.
    #[serde(default)]
    pub min_access_secs: Option<u32>,
    /// Pedestrian walking speed in m/s. When absent, defaults to 1.2 m/s (4.32 km/h).
    #[serde(default)]
    pub walking_speed_mps: Option<f64>,
    /// Radius (meters) within which a parent-less ("orphan") GTFS stop may be merged
    /// into a physical station during ingestion when their normalized names match
    /// EXACTLY and they belong to the SAME operator/feed. That exact-name + same-feed
    /// match is a strong signal, so it tolerates a larger spread (big interchanges)
    /// while genuinely distinct same-named stops (>250 m apart) stay separate. A
    /// future fuzzy/cross-operator matcher should use its own, tighter value. When
    /// absent, defaults to 250 m.
    #[serde(default)]
    pub station_merge_radius_m: Option<f64>,
    /// Cycling speed in m/s. When absent, defaults to 4.2 m/s (~15 km/h).
    #[serde(default)]
    pub cycling_speed_mps: Option<f64>,
    /// Driving speed in m/s. When absent, defaults to 11.0 m/s (~40 km/h).
    #[serde(default)]
    pub driving_speed_mps: Option<f64>,
    /// Access-radius floor (seconds) for bike/car modes. When absent, 1200 s.
    #[serde(default)]
    pub vehicle_access_secs: Option<u32>,
    /// Fraction of the crow-flies (walk-time) trip the bike/car access budget grows to,
    /// above the floor. When absent, 0.06.
    #[serde(default)]
    pub vehicle_access_fraction: Option<f64>,
    /// Ceiling (seconds) on the dynamic bike/car access budget. When absent, 2700 s.
    #[serde(default)]
    pub vehicle_access_max_secs: Option<u32>,
    /// Reliability bucket edges (sorted, strictly increasing, each in (0,1)) used to
    /// quantize plan reliability. When absent, defaults to [0.50, 0.80, 0.95].
    #[serde(default)]
    pub reliability_bucket_edges: Option<Vec<f32>>,
    /// Arrival-time slack (seconds) added to the fastest expected arrival when pruning,
    /// widening the explored band so safer-but-slower plans survive. Default 900 s.
    #[serde(default)]
    pub arrival_slack_secs: Option<u32>,
    /// Upper bound on the `windowMinutes` query argument (Range-RAPTOR window).
    /// Requests above it are clamped. When absent, defaults to 1440 (24 h).
    #[serde(default)]
    pub max_window_minutes: Option<u32>,
    /// Maximum distance (meters) a query coordinate may snap to the street
    /// network; farther queries are rejected. When absent, defaults to 10000.
    #[serde(default)]
    pub max_snap_distance_m: Option<u32>,
    /// Default bike cost profile. Absent ⇒ compiled-in BRouter trekking defaults.
    #[serde(default)]
    pub bike_profile: Option<crate::structures::BikeProfile>,
    /// Stochastic street-time model for access/egress. Absent ⇒ compiled-in defaults.
    #[serde(default)]
    pub street_time: Option<crate::structures::StreetTimeModel>,
    /// RCSP distance budget multiplier δ: the search may explore paths up to
    /// (1+δ)·shortest-distance. Absent ⇒ compiled-in default (0.5).
    #[serde(default)]
    pub distance_budget: Option<f64>,
    /// Per-axis ε-dominance tuning. Absent ⇒ compiled-in defaults.
    #[serde(default)]
    pub epsilon: Option<EpsilonConfig>,
    /// Bike grid-bucketing cell-size coefficients per metre of origin→dest
    /// straight-line distance, on the CyclewayDeficit and Dplus diversity axes
    /// (cell = k·D). Bound the per-node Pareto frontier while preserving the
    /// cycleway/climb span. `0` disables bucketing on that axis. Absent ⇒ defaults.
    #[serde(default)]
    pub bike_bucket_cyc_k: Option<f64>,
    #[serde(default)]
    pub bike_bucket_dpl_k: Option<f64>,
    /// Whether D+ is a bike selection axis. Absent ⇒ compiled default (false: D+ is
    /// displayed-only; Time already prices climbing via the gradient power model).
    #[serde(default)]
    pub bike_select_dplus: Option<bool>,
    /// Variance-generator σ model (signals/elevators/crossings). Absent ⇒ defaults.
    #[serde(default)]
    pub variance_model: Option<crate::structures::cost::VarianceModel>,
    /// Per-axis surface roughness and comfort-stress weights. Absent ⇒ defaults.
    #[serde(default)]
    pub cost_weights: Option<crate::structures::cost::CostWeights>,
    /// Number of diverse representatives kept from the multi-objective front.
    /// Absent ⇒ compiled-in default (6).
    #[serde(default)]
    pub representatives_k: Option<usize>,
    /// Enable multi-objective street routing (per-leg alternatives + brackets on
    /// direct and access/egress walk/bike legs). Absent ⇒ disabled: the per-leg
    /// Pareto search is currently too slow on a country-sized graph (needs an
    /// A*-bounded search), so it is opt-in until that lands.
    #[serde(default)]
    pub multiobj_street: Option<bool>,
    /// Max scalar leg length (metres) to enrich with multi-objective alternatives
    /// for non-walk street modes (bike/car); longer legs stay scalar because the
    /// state-expanded search is too slow / memory-heavy past a few km. Absent ⇒
    /// default (1500).
    #[serde(default)]
    pub multiobj_street_max_len_m: Option<usize>,
    /// Secondary Time weight in a single-axis bike/car "champion" scalarization, to
    /// break ties toward shorter routes. Absent ⇒ default (0.1).
    #[serde(default)]
    pub champion_time_tiebreak: Option<f64>,
    /// ADGW limited-sharing threshold for bike/car alternatives. Absent ⇒ default (0.6).
    #[serde(default)]
    pub alt_max_share_factor: Option<f64>,
    /// Systematic coefficient of variation added to a chosen path's time variance
    /// so long legs do not collapse to false precision. Absent ⇒ default (0.05).
    #[serde(default)]
    pub systematic_cv: Option<f64>,
    /// Per-axis weights selecting the highlighted "balanced" representative.
    /// Absent ⇒ compiled-in defaults. Touches presentation only, never the search.
    #[serde(default)]
    pub balance: Option<crate::structures::cost::BalanceWeights>,
    /// Pedestrian vertical-connector (stairs/elevator/ramp) cost model, used by the
    /// Stage B1 connector-coverage measurement to report the extra walk time a
    /// vertical-access path adds. Absent ⇒ compiled-in defaults. (B1 does not charge
    /// this in routing.)
    #[serde(default)]
    pub connector_cost: Option<ConnectorCostConfig>,
    /// Address search: distance (km) within which a candidate keeps its full geo
    /// score around the map focus point. Absent ⇒ 2.0.
    #[serde(default)]
    pub address_geo_offset_km: Option<f64>,
    /// Address search: distance (km) at which the geo score has decayed to half;
    /// the exponential scale is derived as `(half - offset)/ln(2)`. Absent ⇒ 5.0.
    #[serde(default)]
    pub address_geo_half_score_km: Option<f64>,
    /// Address search: floor on the geo decay so a far but exact text match is
    /// never fully buried. Absent ⇒ 0.1.
    #[serde(default)]
    pub address_geo_floor: Option<f64>,
    /// Address search: text factor for a prefix-only token match relative to an
    /// exact alias token (exact = 1.0). Absent ⇒ 0.6.
    #[serde(default)]
    pub address_prefix_token_weight: Option<f64>,
    /// Address search: multiplicative boost when a number token exactly equals a
    /// record's house number, ranking it above a prefix house-number match.
    /// Absent ⇒ 1.5.
    #[serde(default)]
    pub address_house_number_boost: Option<f64>,
    /// Address search: run the typo-tolerant fuzzy fallback only when the exact /
    /// prefix pass covered fewer than this many streets. Absent ⇒ 5.
    #[serde(default)]
    pub address_fuzzy_trigger_k: Option<usize>,
    /// Address search: minimum query-token length (chars) to allow 1 edit of fuzzy
    /// tolerance; below it a token is never fuzzed. Absent ⇒ 3.
    #[serde(default)]
    pub address_fuzzy_min_len_1typo: Option<usize>,
    /// Address search: minimum query-token length (chars) to allow 2 edits of fuzzy
    /// tolerance. Absent ⇒ 8.
    #[serde(default)]
    pub address_fuzzy_min_len_2typos: Option<usize>,
    /// Address search: text factor for a token matched only via the fuzzy fallback,
    /// below the prefix weight so corrected matches rank under literal ones.
    /// Absent ⇒ 0.4.
    #[serde(default)]
    pub address_fuzzy_token_weight: Option<f64>,
    /// Address index build: divergence epsilon (meters) for a building's box
    /// coordinates. BeST stores each apartment/box as its own address row at one
    /// house number; when those rows sit within this radius they are one entrance,
    /// so the box coordinates collapse to the building centroid. Beyond it (a rare
    /// multi-entrance building) each box keeps its own coordinate so box-level
    /// precision is not lost. Absent ⇒ 5.0.
    #[serde(default)]
    pub address_box_coord_epsilon_m: Option<f64>,
}

impl RoutingDefaultConfig {
    /// Build the address-search tuning, starting from the researched compiled-in
    /// defaults and overriding only the fields present in config.
    pub fn to_address_search_params(&self) -> crate::structures::AddressSearchParams {
        let mut p = crate::structures::AddressSearchParams::default();
        if let Some(v) = self.address_geo_offset_km {
            p.geo_offset_km = v;
        }
        if let Some(v) = self.address_geo_half_score_km {
            p.geo_half_score_km = v;
        }
        if let Some(v) = self.address_geo_floor {
            p.geo_floor = v;
        }
        if let Some(v) = self.address_prefix_token_weight {
            p.prefix_token_weight = v;
        }
        if let Some(v) = self.address_house_number_boost {
            p.house_number_boost = v;
        }
        if let Some(v) = self.address_fuzzy_trigger_k {
            p.fuzzy_trigger_k = v;
        }
        if let Some(v) = self.address_fuzzy_min_len_1typo {
            p.fuzzy_min_len_1typo = v;
        }
        if let Some(v) = self.address_fuzzy_min_len_2typos {
            p.fuzzy_min_len_2typos = v;
        }
        if let Some(v) = self.address_fuzzy_token_weight {
            p.fuzzy_token_weight = v;
        }
        p
    }

    /// Build-time representative-coordinate divergence epsilon (meters) for the
    /// address index, from `address_box_coord_epsilon_m`. Absent ⇒ 5.0.
    pub fn address_box_coord_epsilon_m(&self) -> f64 {
        self.address_box_coord_epsilon_m
            .unwrap_or(crate::structures::DEFAULT_BOX_COORD_EPSILON_M)
    }
}

/// Config view of the pedestrian connector cost model. Absent fields fall back to
/// the compiled-in `ConnectorCost::default()` values.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConnectorCostConfig {
    #[serde(default)]
    pub stairs_speed_mps: Option<f64>,
    #[serde(default)]
    pub ramp_speed_mps: Option<f64>,
    #[serde(default)]
    pub elevator_secs: Option<f64>,
    #[serde(default)]
    pub relocation_fallback_secs: Option<f64>,
}

/// Per-axis ε-dominance tuning: `ε_i = a_i + b_i·value`. Absent fields keep these
/// defaults. Converted to `cost::Epsilon` for the multi-objective search.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EpsilonConfig {
    pub time_a: f64,
    pub time_b: f64,
    pub dplus_a: f64,
    pub dplus_b: f64,
    pub surface_a: f64,
    pub surface_b: f64,
    pub cycleway_deficit_a: f64,
    pub cycleway_deficit_b: f64,
    pub variance_a: f64,
    pub variance_b: f64,
}

impl Default for EpsilonConfig {
    fn default() -> Self {
        EpsilonConfig {
            time_a: 2.0,
            time_b: 0.01,
            dplus_a: 3.0,
            dplus_b: 0.02,
            surface_a: 10.0,
            surface_b: 0.06,
            cycleway_deficit_a: 10.0,
            cycleway_deficit_b: 0.02,
            variance_a: 150.0,
            variance_b: 0.1,
        }
    }
}

impl EpsilonConfig {
    pub fn to_epsilon(&self) -> crate::structures::cost::Epsilon {
        use crate::structures::cost::{AXIS_COUNT, Axis};
        let mut a = [0.0; AXIS_COUNT];
        let mut b = [0.0; AXIS_COUNT];
        a[Axis::Time.index()] = self.time_a;
        b[Axis::Time.index()] = self.time_b;
        a[Axis::Dplus.index()] = self.dplus_a;
        b[Axis::Dplus.index()] = self.dplus_b;
        a[Axis::Surface.index()] = self.surface_a;
        b[Axis::Surface.index()] = self.surface_b;
        a[Axis::CyclewayDeficit.index()] = self.cycleway_deficit_a;
        b[Axis::CyclewayDeficit.index()] = self.cycleway_deficit_b;
        a[Axis::Variance.index()] = self.variance_a;
        b[Axis::Variance.index()] = self.variance_b;
        crate::structures::cost::Epsilon::new(a, b)
    }
}

impl Ingestor {
    pub fn label(&self) -> &str {
        match self {
            Ingestor::OsmPbf(_) => "osm/pbf",
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.name,
            Ingestor::GtfsSncb(c) => &c.name,
            Ingestor::BeStAdd(c) => &c.name,
        }
    }

    pub fn url(&self) -> &str {
        match self {
            Ingestor::OsmPbf(c) => &c.url,
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.url,
            Ingestor::GtfsSncb(c) => &c.url,
            Ingestor::BeStAdd(c) => &c.url,
        }
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        match self {
            Ingestor::OsmPbf(c) => &c.headers,
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.headers,
            Ingestor::GtfsSncb(c) => &c.headers,
            Ingestor::BeStAdd(c) => &c.headers,
        }
    }

    pub fn location(&self) -> Result<SourceLocation, String> {
        let url = self.url();
        if let Some(path) = url.strip_prefix("path:") {
            Ok(SourceLocation::Local(path.to_string()))
        } else if url.starts_with("http://") || url.starts_with("https://") {
            Ok(SourceLocation::Remote(url.to_string()))
        } else {
            Err(format!("Unknown URL scheme for '{}': {url}", self.label()))
        }
    }

    pub fn phase(&self) -> u8 {
        match self {
            Ingestor::OsmPbf(i) => i.phase.unwrap_or(0),
            Ingestor::GtfsGeneric(i) | Ingestor::GtfsStib(i) => i.phase.unwrap_or(1),
            Ingestor::GtfsSncb(i) => i.phase.unwrap_or(1),
            Ingestor::BeStAdd(i) => i.phase.unwrap_or(2),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, String> {
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read config: {e}"))?;
        serde_yaml_ng::from_str(&content).map_err(|e| format!("Failed to parse config: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::BalanceWeights;

    #[test]
    fn server_config_defaults() {
        let cfg: ServerConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
    }

    #[test]
    fn server_config_custom_values() {
        let yaml = "host: 127.0.0.1\nport: 8080";
        let cfg: ServerConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 8080);
    }

    #[test]
    fn config_without_server_section_uses_defaults() {
        let yaml = r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:data/test.pbf"
  output: graph.bin
default_routing: {}
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 3000);
        assert_eq!(cfg.build.elevation_smoothing_epsilon, 4.0);
    }

    #[test]
    fn elevation_smoothing_epsilon_parses_and_defaults() {
        let with = r#"
build:
  inputs: []
  output: graph.bin
  elevation_smoothing_epsilon: 3.0
default_routing: {}
"#;
        let cfg: Config = serde_yaml_ng::from_str(with).unwrap();
        assert_eq!(cfg.build.elevation_smoothing_epsilon, 3.0);

        let without = r#"
build:
  inputs: []
  output: graph.bin
default_routing: {}
"#;
        let cfg: Config = serde_yaml_ng::from_str(without).unwrap();
        assert_eq!(cfg.build.elevation_smoothing_epsilon, 4.0);
    }

    #[test]
    fn surface_speed_factors_absent_uses_default_table() {
        let yaml = r#"
build:
  inputs: []
  output: graph.bin
default_routing: {}
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let f = &cfg.build.surface_speed_factors;
        assert_eq!(f.quantize(Some("asphalt")), 100);
        assert_eq!(f.quantize(Some("gravel")), 60);
        assert_eq!(f.quantize(Some("mud")), 20);
        assert_eq!(f.quantize(None), 90, "untagged → unknown default");
    }

    #[test]
    fn surface_speed_factors_sparse_override_replaces_table() {
        // A provided map replaces the table wholesale (it is not field-merged), so an
        // unlisted surface falls through to the unknown default rather than the
        // compiled-in value. Listed entries take the configured factor.
        let yaml = r#"
build:
  inputs: []
  output: graph.bin
  surface_speed_factors:
    asphalt: 1.00
    gravel: 0.50
default_routing: {}
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        let f = &cfg.build.surface_speed_factors;
        assert_eq!(f.quantize(Some("asphalt")), 100);
        assert_eq!(f.quantize(Some("gravel")), 50, "configured override wins");
        assert_eq!(f.quantize(Some("mud")), 90, "unlisted surface → unknown default");
    }

    #[test]
    fn config_with_server_section_overrides_defaults() {
        let yaml = r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:data/test.pbf"
  output: graph.bin
default_routing: {}
server:
  host: "127.0.0.1"
  port: 9090
"#;
        let cfg: Config = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 9090);
    }

    #[test]
    fn routing_default_config_walking_speed_absent_is_none() {
        let yaml = "default_routing: {}";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.walking_speed_mps.is_none());
        assert!(cfg.min_access_secs.is_none());
    }

    #[test]
    fn routing_default_config_walking_speed_parses() {
        let yaml = "walking_speed_mps: 1.4";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.walking_speed_mps, Some(1.4));
    }

    #[test]
    fn routing_default_config_station_merge_radius_parses_and_defaults() {
        let with = "station_merge_radius_m: 75.0";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(with).unwrap();
        assert_eq!(cfg.station_merge_radius_m, Some(75.0));

        let without: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(without.station_merge_radius_m.is_none());
    }

    #[test]
    fn routing_default_config_cycling_speed_parses() {
        let yaml = "cycling_speed_mps: 5.0";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.cycling_speed_mps, Some(5.0));
    }

    #[test]
    fn routing_default_config_cycling_speed_absent_is_none() {
        let yaml = "default_routing: {}";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.cycling_speed_mps.is_none());
    }

    #[test]
    fn routing_default_config_bike_profile_absent_is_none() {
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(cfg.bike_profile.is_none());
    }

    #[test]
    fn routing_default_config_bike_profile_partial_parses() {
        // A sparse profile keeps BikeProfile's serde-defaulted fields.
        let yaml = "bike_profile:\n  allow_steps: false\n  biker_power: 150";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let bp = cfg.bike_profile.expect("bike_profile present");
        assert!(!bp.allow_steps);
        assert_eq!(bp.biker_power, 150.0);
        assert_eq!(bp.downhillcost, 100.0); // untouched default
    }

    #[test]
    fn parses_street_time_block() {
        let yaml = "street_time:\n  access_percentile: 0.9\n  sigma_floor: 0.1";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let st = cfg.street_time.unwrap();
        assert_eq!(st.access_percentile, 0.9);
        assert_eq!(st.sigma_floor, 0.1);
        assert_eq!(st.sigma_cap, 0.5);
        assert_eq!(st.sigma_alpha, 3.8);
    }

    #[test]
    fn street_time_absent_is_none() {
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(cfg.street_time.is_none());
    }

    #[test]
    fn routing_default_config_caps_parse() {
        let yaml = "max_window_minutes: 120\nmax_snap_distance_m: 5000";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.max_window_minutes, Some(120));
        assert_eq!(cfg.max_snap_distance_m, Some(5000));
    }

    #[test]
    fn routing_default_config_caps_absent_are_none() {
        let yaml = "default_routing: {}";
        let cfg: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.max_window_minutes.is_none());
        assert!(cfg.max_snap_distance_m.is_none());
    }

    #[test]
    fn gtfs_ingestor_parses_headers() {
        let yaml = r#"
ingestor: gtfs/generic
name: stib
url: "https://example.com/gtfs.zip"
headers:
  Authorization: "Bearer ${TOKEN}"
  X-Api-Key: "${file:/run/secrets/key}"
"#;
        let ing: Ingestor = serde_yaml_ng::from_str(yaml).unwrap();
        let h = ing.headers();
        assert_eq!(
            h.get("Authorization").map(|s| s.as_str()),
            Some("Bearer ${TOKEN}")
        );
        assert_eq!(
            h.get("X-Api-Key").map(|s| s.as_str()),
            Some("${file:/run/secrets/key}")
        );
    }

    #[test]
    fn ingestor_without_headers_is_empty() {
        let yaml = "ingestor: gtfs/generic\nname: x\nurl: \"path:data/x.zip\"";
        let ing: Ingestor = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(ing.headers().is_empty());
    }

    #[test]
    fn auto_update_section_parses() {
        let yaml = "enabled: true\nschedule: \"0 5 * * *\"\ncache_dir: \"mycache\"";
        let au: AutoUpdateConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(au.enabled);
        assert_eq!(au.schedule, "0 5 * * *");
        assert_eq!(au.cache_dir, "mycache");
    }

    #[test]
    fn auto_update_cache_dir_defaults() {
        let yaml = "enabled: false\nschedule: \"0 5 * * *\"";
        let au: AutoUpdateConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(au.cache_dir, "cache");
    }

    #[test]
    fn realtime_config_parses_both_feed_kinds() {
        let yaml = r#"
enabled: true
poll_interval_secs: 45
feeds:
  - type: gtfs-rt
    name: sncb
    url: "https://example.com/rt?format=protobuf"
    headers:
      bmc-partner-key: "${BMC_PARTNER_KEY}"
  - type: stib
    name: stib
    waiting_time_url: "https://example.com/WaitingTimes/"
"#;
        let rt: RealtimeConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(rt.enabled);
        assert_eq!(rt.poll_interval_secs, 45);
        assert_eq!(rt.request_timeout_secs, 20); // default
        assert_eq!(rt.feeds.len(), 2);
        match &rt.feeds[0] {
            RealtimeFeedConfig::GtfsRt { name, url, headers } => {
                assert_eq!(name, "sncb");
                assert!(url.contains("protobuf"));
                assert_eq!(
                    headers.get("bmc-partner-key").unwrap(),
                    "${BMC_PARTNER_KEY}"
                );
            }
            _ => panic!("expected gtfs-rt feed first"),
        }
        match &rt.feeds[1] {
            RealtimeFeedConfig::Stib {
                name,
                waiting_time_url,
                ..
            } => {
                assert_eq!(name, "stib");
                assert!(waiting_time_url.ends_with("WaitingTimes/"));
            }
            _ => panic!("expected stib feed second"),
        }
    }

    #[test]
    fn stib_feed_vehicle_position_url_parses_and_defaults_to_none() {
        let with_url = r#"
enabled: true
feeds:
  - type: stib
    name: stib
    waiting_time_url: "https://example.com/WaitingTimes/"
    vehicle_position_url: "https://example.com/VehiclePositions/"
"#;
        let rt: RealtimeConfig = serde_yaml_ng::from_str(with_url).unwrap();
        match &rt.feeds[0] {
            RealtimeFeedConfig::Stib {
                vehicle_position_url,
                ..
            } => assert_eq!(
                vehicle_position_url.as_deref(),
                Some("https://example.com/VehiclePositions/")
            ),
            _ => panic!("expected stib feed"),
        }

        let without_url = r#"
enabled: true
feeds:
  - type: stib
    name: stib
    waiting_time_url: "https://example.com/WaitingTimes/"
"#;
        let rt2: RealtimeConfig = serde_yaml_ng::from_str(without_url).unwrap();
        match &rt2.feeds[0] {
            RealtimeFeedConfig::Stib {
                vehicle_position_url,
                ..
            } => assert!(
                vehicle_position_url.is_none(),
                "vehicle_position_url should default to None"
            ),
            _ => panic!("expected stib feed"),
        }
    }

    #[test]
    fn realtime_rate_limit_defaults() {
        let yaml = "enabled: true";
        let rt: RealtimeConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(rt.rate_limit.consecutive_429_threshold, 3);
        assert_eq!(rt.rate_limit.throttled_min_interval_secs, 60);
        assert_eq!(rt.poll_interval_secs, 30);
        assert!(rt.feeds.is_empty());
    }

    #[test]
    fn vehicle_position_max_age_secs_parses_and_defaults() {
        let with_age = "enabled: true\nvehicle_position_max_age_secs: 60";
        let rt: RealtimeConfig = serde_yaml_ng::from_str(with_age).unwrap();
        assert_eq!(rt.vehicle_position_max_age_secs, 60);

        let without_age = "enabled: true";
        let rt2: RealtimeConfig = serde_yaml_ng::from_str(without_age).unwrap();
        assert_eq!(rt2.vehicle_position_max_age_secs, 120, "default should be 120");
    }

    #[test]
    fn parses_distance_budget_and_epsilon() {
        let yaml = "distance_budget: 1.4\nepsilon:\n  time_a: 3.0\n  time_b: 0.05";
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(r.distance_budget, Some(1.4));
        let e = r.epsilon.unwrap();
        assert_eq!(e.time_a, 3.0);
        assert_eq!(e.time_b, 0.05);
        assert_eq!(e.surface_a, 10.0, "unspecified epsilon fields keep defaults");
    }

    #[test]
    fn distance_budget_and_variance_absent_are_none() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.distance_budget.is_none());
        assert!(r.epsilon.is_none());
        assert!(r.variance_model.is_none());
    }

    #[test]
    fn parses_variance_model_delays_and_defaults_sparse_fields() {
        let yaml = "variance_model:\n  signal_delay_major: 20.0\n  push_sigma: 6.0";
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let vm = r.variance_model.unwrap();
        assert_eq!(vm.signal_delay_major, 20.0);
        assert_eq!(vm.push_sigma, 6.0);
        assert_eq!(
            vm.signal_delay_secondary, 10.0,
            "unspecified delay fields keep compiled-in defaults"
        );
        assert_eq!(vm.signal_delay_minor, 7.0);
        assert_eq!(
            vm.signal_sigma_major, 25.0,
            "the variance sigmas are independent of the new mean delays"
        );
    }

    #[test]
    fn cost_weights_absent_is_none() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.cost_weights.is_none());
    }

    #[test]
    fn parses_representatives_k() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("representatives_k: 8").unwrap();
        assert_eq!(r.representatives_k, Some(8));
    }

    #[test]
    fn representatives_k_absent_is_none() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.representatives_k.is_none());
    }

    #[test]
    fn parses_systematic_cv() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("systematic_cv: 0.1").unwrap();
        assert_eq!(r.systematic_cv, Some(0.1));
    }

    #[test]
    fn systematic_cv_absent_is_none() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.systematic_cv.is_none());
    }

    #[test]
    fn parses_cost_weights() {
        let yaml = "cost_weights:\n  surface_unpaved: 4.0";
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let cw = r.cost_weights.expect("cost_weights present");
        assert_eq!(cw.surface_unpaved, 4.0);
        assert_eq!(cw.surface_paved, 1.0, "unspecified field keeps default");
        assert_eq!(cw.surface_unknown, 1.3, "unspecified field keeps default");
    }

    #[test]
    fn parses_balance_weights() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("balance:\n  time: 2.0").unwrap();
        let b = r.balance.unwrap();
        assert_eq!(b.time, 2.0);
        assert_eq!(
            b.variance,
            BalanceWeights::default().variance,
            "unspecified fields keep defaults"
        );
    }

    #[test]
    fn balance_absent_is_none() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.balance.is_none());
    }

    #[test]
    fn address_search_params_default_when_absent() {
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str("{}").unwrap();
        assert!(r.address_geo_offset_km.is_none());
        assert!(r.address_box_coord_epsilon_m.is_none());
        assert_eq!(r.address_box_coord_epsilon_m(), 5.0);
        let p = r.to_address_search_params();
        assert_eq!(p.geo_offset_km, 2.0);
        assert_eq!(p.geo_half_score_km, 5.0);
        assert_eq!(p.geo_floor, 0.1);
        assert_eq!(p.prefix_token_weight, 0.6);
        assert_eq!(p.house_number_boost, 1.5);
        assert_eq!(p.fuzzy_trigger_k, 5);
        assert_eq!(p.fuzzy_min_len_1typo, 3);
        assert_eq!(p.fuzzy_min_len_2typos, 8);
        assert_eq!(p.fuzzy_token_weight, 0.4);
    }

    #[test]
    fn address_search_params_override_from_config() {
        let yaml = r#"
address_geo_offset_km: 1.0
address_geo_half_score_km: 8.0
address_geo_floor: 0.05
address_prefix_token_weight: 0.5
address_house_number_boost: 2.0
address_fuzzy_trigger_k: 3
address_fuzzy_min_len_1typo: 4
address_fuzzy_min_len_2typos: 9
address_fuzzy_token_weight: 0.3
address_box_coord_epsilon_m: 12.0
"#;
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(r.address_box_coord_epsilon_m(), 12.0);
        let p = r.to_address_search_params();
        assert_eq!(p.geo_offset_km, 1.0);
        assert_eq!(p.geo_half_score_km, 8.0);
        assert_eq!(p.geo_floor, 0.05);
        assert_eq!(p.prefix_token_weight, 0.5);
        assert_eq!(p.house_number_boost, 2.0);
        assert_eq!(p.fuzzy_trigger_k, 3);
        assert_eq!(p.fuzzy_min_len_1typo, 4);
        assert_eq!(p.fuzzy_min_len_2typos, 9);
        assert_eq!(p.fuzzy_token_weight, 0.3);
    }

    #[test]
    fn epsilon_config_maps_to_per_axis_arrays() {
        use crate::structures::cost::{Axis, CostVector};
        let ec = EpsilonConfig {
            time_a: 2.0,
            time_b: 0.0,
            ..Default::default()
        };
        let eps = ec.to_epsilon();
        let a = CostVector::from_active(&[Axis::Time], &[100.0]);
        let b = CostVector::from_active(&[Axis::Time], &[101.0]);
        assert!(a.eps_dominates(&b, &eps), "time slack 2.0 covers a 1.0 gap");
    }
}
