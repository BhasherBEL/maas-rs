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
    /// Seconds between poll cycles across all feeds. The steady-state request
    /// rate is `(Σ feed.requests_per_poll) / poll_interval_secs`; the shipped
    /// default keeps it under the gateway's ~8 req/min and ~12k req/day quota
    /// (see [`RealtimeConfig::request_rate`]).
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
    /// A realtime snapshot older than this many seconds is considered STALE and is
    /// NOT applied to routing — the router falls back to schedule-only for that
    /// query. Guards against a feed outage (the poller keeps the last good index
    /// with no TTL of its own) serving hours-old delays and cancellations forever.
    /// The poller stamps every published snapshot with this value; the routing
    /// consumer boundary enforces it against wall-clock `now`.
    #[serde(default = "default_index_max_age_secs")]
    pub index_max_age_secs: u64,
    /// How long (seconds) the poller retains a TRACKED journey's last-known live
    /// delay after the feed stops reporting it. STIB waiting-times only predict
    /// upcoming departures, so once a user boards, their vehicle leaves the feed
    /// window and its delay would vanish. The poller keeps a cross-cycle sticky
    /// cache of `(trip, stop) → delay` and evicts entries older than this TTL.
    /// The sticky delay is exposed ONLY to the live-refresh overlay (a tracked
    /// journey), never to routing/planning. Defaults to ~24h.
    #[serde(default = "default_tracked_delay_ttl_secs")]
    pub tracked_delay_ttl_secs: u64,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub feeds: Vec<RealtimeFeedConfig>,
}

fn default_poll_interval_secs() -> u64 {
    60
}

fn default_rt_timeout_secs() -> u64 {
    20
}

fn default_vehicle_position_max_age_secs() -> u64 {
    120
}

fn default_index_max_age_secs() -> u64 {
    600
}

fn default_tracked_delay_ttl_secs() -> u64 {
    86_400
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    /// Consecutive throttle responses (HTTP 403 "out of quota" or 429) before the
    /// fetcher backs off and all feeds skip until a probe succeeds.
    #[serde(
        default = "default_failure_threshold",
        alias = "consecutive_429_threshold"
    )]
    pub consecutive_failure_threshold: u32,
    /// While backed off, at most one probe request is issued per this interval.
    #[serde(default = "default_throttled_interval_secs")]
    pub throttled_min_interval_secs: u64,
    /// Documented gateway quota ceilings, used to warn at startup (and gate in
    /// tests) if the configured cadence would exceed them.
    #[serde(default = "default_max_requests_per_min")]
    pub max_requests_per_min: u32,
    #[serde(default = "default_max_requests_per_day")]
    pub max_requests_per_day: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            consecutive_failure_threshold: default_failure_threshold(),
            throttled_min_interval_secs: default_throttled_interval_secs(),
            max_requests_per_min: default_max_requests_per_min(),
            max_requests_per_day: default_max_requests_per_day(),
        }
    }
}

fn default_failure_threshold() -> u32 {
    3
}

fn default_throttled_interval_secs() -> u64 {
    60
}

fn default_max_requests_per_min() -> u32 {
    8
}

fn default_max_requests_per_day() -> u32 {
    12_000
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

impl RealtimeFeedConfig {
    /// Number of HTTP requests one poll of this feed issues to the gateway. A
    /// GTFS-RT feed is a single GET; a STIB feed fetches waiting-times plus, when
    /// configured, vehicle-positions — two requests. Miscounting the STIB feed as
    /// one was the arithmetic root of the original quota overshoot.
    pub fn requests_per_poll(&self) -> u32 {
        match self {
            RealtimeFeedConfig::GtfsRt { .. } => 1,
            RealtimeFeedConfig::Stib {
                vehicle_position_url,
                ..
            } => {
                if vehicle_position_url.is_some() {
                    2
                } else {
                    1
                }
            }
        }
    }
}

impl RealtimeConfig {
    /// Steady-state request rate implied by the configured cadence, as
    /// `(requests_per_minute, requests_per_day)`, counting each feed's per-poll
    /// request count. Used for the startup quota warning and the quota test.
    pub fn request_rate(&self) -> (f64, f64) {
        let interval = self.poll_interval_secs.max(1) as f64;
        let per_cycle: u32 = self.feeds.iter().map(|f| f.requests_per_poll()).sum();
        let per_sec = per_cycle as f64 / interval;
        (per_sec * 60.0, per_sec * 86_400.0)
    }

    /// True when the configured cadence stays within the documented gateway
    /// quota (`rate_limit.max_requests_per_min` and `_per_day`).
    pub fn within_quota(&self) -> bool {
        let (per_min, per_day) = self.request_rate();
        per_min <= self.rate_limit.max_requests_per_min as f64
            && per_day <= self.rate_limit.max_requests_per_day as f64
    }
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
    /// When true, inter-stop transfers are found by a live per-round multi-source
    /// bounded foot-Dijkstra (MCR) over the contracted graph instead of the precomputed
    /// ≤1 km table, so >1 km inter-stop walks are discovered. Default false.
    #[serde(default)]
    pub unrestricted_transfers: Option<bool>,
    /// When true, foot access/egress stop discovery uses the exact CCH one-to-many
    /// instead of the radius-bounded two-pass foot Dijkstra. Requires a built `cch`;
    /// falls back to the two-pass path when absent. Default false.
    #[serde(default)]
    pub use_cch_access: Option<bool>,
    /// When true, a range/window query emits a per-phase wall-clock decomposition
    /// (discovery/grid_alloc/forward/extract/backward, plus per-pass probe/range/
    /// departure counts) as one structured log line. Purely additive observability
    /// — never changes routing behavior or results. Per-query `profileLatency`
    /// (GraphQL `raptor` argument) overrides this default. Absent ⇒ false.
    #[serde(default)]
    pub profile_latency: Option<bool>,
    /// When true (default), the exact foot-access CCH is built (or loaded from
    /// `cch.bin`) at startup so the per-query `useCchAccess` flag has a live index to
    /// dispatch to. When false, `g.cch` stays `None` and the CCH seam always falls back
    /// to the two-pass foot Dijkstra. Absent ⇒ true.
    #[serde(default)]
    pub prepare_cch_access: Option<bool>,
    /// Upper bound on the `windowMinutes` query argument (Range-RAPTOR window).
    /// Requests above it are clamped. When absent, defaults to 1440 (24 h).
    #[serde(default)]
    pub max_window_minutes: Option<u32>,
    /// Maximum distance (meters) a query coordinate may snap to the street
    /// network; farther queries are rejected. When absent, defaults to 10000.
    #[serde(default)]
    pub max_snap_distance_m: Option<u32>,
    /// Travel-time-map (isochrone) sampling grid step, in metres. Smaller = finer
    /// heatmap, more cells, slower. When absent, defaults to 300.
    #[serde(default)]
    pub travel_map_grid_step_m: Option<f64>,
    /// Travel-time-map safety cap on total grid cells. When a (possibly
    /// per-query overridden) grid step would produce more cells than this over
    /// the reachable bounding box, the step is coarsened so the output stays
    /// bounded. When absent, defaults to 150000.
    #[serde(default)]
    pub travel_map_max_cells: Option<u64>,
    /// Travel-time-map departure-window sample interval, in seconds. When a
    /// `travelTimeMap` query supplies a window, the isochrone is evaluated at
    /// departures spaced this many seconds apart. When absent, defaults to 600.
    #[serde(default)]
    pub travel_map_window_sample_secs: Option<u32>,
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
    /// Drive grid-bucketing cell-size coefficient per metre of origin→dest
    /// straight-line distance, on the Variance selection axis (cell = k·D). Bounds
    /// the per-node Pareto frontier on long direct drive legs. `0` disables
    /// bucketing. Absent ⇒ compiled-in default.
    #[serde(default)]
    pub drive_bucket_var_k: Option<f64>,
    /// Walk grid-bucketing cell-size coefficient per metre of origin→dest
    /// straight-line distance, on the Surface selection axis (cell = k·D). Bounds
    /// the per-node Pareto frontier on long direct walk legs. `0` disables
    /// bucketing. Absent ⇒ compiled-in default.
    #[serde(default)]
    pub walk_bucket_surf_k: Option<f64>,
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
    /// Transit-pricing model (price as an in-search Pareto dominance axis).
    /// Absent ⇒ feature off (byte-identical to pre-feature routing). The single
    /// master switch is `fares.enabled`; there are no per-piece feature flags.
    #[serde(default)]
    pub fares: Option<FaresConfig>,
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

/// ε-bucket params for the euro (known-cents) price axis, shaped like `epsilon.*`
/// (`a + b * value`). `a` is an absolute cent width, `b` a relative fraction.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct KnownEurosEpsilonConfig {
    #[serde(default)]
    pub a: f64,
    #[serde(default)]
    pub b: f64,
}

/// One fare-modeled operator, keyed by normalized `agency.name`. `model` selects
/// the marginal-fare function. Fields not used by the selected model deserialize
/// leniently and are ignored.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FareOperatorConfig {
    /// Normalized agency name key (e.g. "STIB").
    pub name: String,
    /// Model tag: `time_window_flat` (STIB / De Lijn), `time_window_flat_tiered`
    /// (TEC classic/express), or `distance_base_per_km` (SNCB).
    pub model: String,
    // --- time_window_flat (STIB / De Lijn) ---
    #[serde(default)]
    pub ticket_euros: Option<f64>,
    #[serde(default)]
    pub validity_secs: Option<u32>,
    /// Per-journey price of a held N-journey card (De Lijn 10-journey). Selected
    /// by the `delijn10Journey` profile flag; absent ⇒ card == single price.
    #[serde(default)]
    pub card_euros: Option<f64>,
    /// Which time-window operator (`stib` | `delijn`) — selects the independent
    /// ticket-window state. Absent ⇒ `stib`.
    #[serde(default)]
    pub time_window_operator: Option<String>,
    // --- time_window_flat_tiered (TEC) ---
    #[serde(default)]
    pub classic_single_euros: Option<f64>,
    #[serde(default)]
    pub express_single_euros: Option<f64>,
    #[serde(default)]
    pub classic_card6_euros: Option<f64>,
    #[serde(default)]
    pub express_card6_euros: Option<f64>,
    #[serde(default)]
    pub classic_card6_reduced_euros: Option<f64>,
    #[serde(default)]
    pub express_card6_reduced_euros: Option<f64>,
    /// Route-name tokens marking an EXPRESS route (matched as an uppercased
    /// substring of `route_short_name`/`route_long_name`). Config-driven
    /// classification rule.
    #[serde(default)]
    pub express_route_names: Vec<String>,
    /// Route-name PREFIXES marking an EXPRESS route (an uppercased
    /// `route_short_name` ONLY that starts with any of these; long-name matching is
    /// intentionally excluded to avoid misclassifying E-initial destination names
    /// like Eupen/Eghezée/Esneux). TEC express lines are those whose route number
    /// starts with "E" (e.g. E12), which this rule expresses without hardcoding the
    /// letter in Rust.
    #[serde(default)]
    pub express_route_prefixes: Vec<String>,
    // --- distance_base_per_km (SNCB) ---
    /// Base single-fare-of-distance model. `bracketed` (default, the EXACT published
    /// 2026 SNCB 2nd-class tariff), `band` (a legacy piecewise placeholder), or
    /// `linear` (an inert linear-fit alternative). Absent ⇒ `bracketed`.
    #[serde(default)]
    pub distance_tariff: Option<String>,
    // bracketed model (default, exact 2026 2nd-class): raw = a*d_eff + b, floored,
    // then rounded to the 0.10-EUR grid (half up); d_eff snaps to the SNCB bracket
    // midpoints (structure fixed in code).
    /// Slope `a` (EUR per effective km) of the exact 2nd-class linear formula.
    #[serde(default)]
    pub a_euros_per_km: Option<f64>,
    /// Intercept `b` (EUR) of the exact 2nd-class linear formula.
    #[serde(default)]
    pub b_euros: Option<f64>,
    /// Distance (km) at/above which the fare caps (`d_eff = cap_km`). Default 116.
    #[serde(default)]
    pub cap_from_km: Option<u32>,
    /// The capped effective distance (km) used at/above `cap_from_km`. Default 118.
    #[serde(default)]
    pub cap_km: Option<u32>,
    /// 1st-class multiplier km thresholds over `d_eff` (default [36, 51]): coeff[0]
    /// applies for `d_eff <= t0`, coeff[1] for `t0 < d_eff <= t1`, else coeff[2].
    #[serde(default)]
    pub first_class_thresholds: Vec<u32>,
    /// 1st-class multipliers applied to the UNROUNDED 2nd-class raw (default
    /// [1.40, 1.50, 1.60]).
    #[serde(default)]
    pub first_class_coefficients: Vec<f64>,
    /// 1st-class rounding tier boundaries (EUR, ascending, default [25, 50]): base
    /// fares below `first_class_round_thresholds[0]` round to
    /// `first_class_round_grids[0]`, in `[t0, t1]` to grid[1], above `t1` to grid[2].
    #[serde(default)]
    pub first_class_round_thresholds: Vec<f64>,
    /// 1st-class rounding grid (EUR) per tier (default [0.10, 0.50, 1.00]). Rounding
    /// is half-up onto the grid.
    #[serde(default)]
    pub first_class_round_grids: Vec<f64>,
    /// Absolute fare floor (euros) — the minimum any SNCB single costs, and the
    /// amount charged at board (default 2.60).
    #[serde(default)]
    pub floor_euros: Option<f64>,
    /// Tariff-distance clamp (whole km): trips shorter than `min_km` bill as `min_km`,
    /// longer than `max_km` bill as `max_km` (the advertised "120 km" cap → 118).
    #[serde(default)]
    pub min_km: Option<u32>,
    #[serde(default)]
    pub max_km: Option<u32>,
    // band model (default): fare = per_km_rate * band_coeff(tariff_km) * tariff_km.
    /// SNCB base per-km rate (euros/km) — the tariff constant. PLACEHOLDER pending
    /// the exact value from SNCB's tariff PDF / fare API.
    #[serde(default)]
    pub per_km_rate: Option<f64>,
    /// Two ascending km thresholds delimiting the three band coefficients (e.g. [36, 51]).
    #[serde(default)]
    pub band_thresholds: Vec<u32>,
    /// The three band coefficients (e.g. [1.40, 1.50, 1.60]).
    #[serde(default)]
    pub band_coefficients: Vec<f64>,
    // linear model (inert alternative): fare = intercept_euros + slope_euros_per_km * tariff_km.
    #[serde(default)]
    pub intercept_euros: Option<f64>,
    #[serde(default)]
    pub slope_euros_per_km: Option<f64>,
    #[serde(default)]
    pub railway_distance_source: Option<String>,
    /// SNCB peak windows as `[[start_hhmm_secs, end_hhmm_secs], ...]` (seconds
    /// since midnight), weekdays only. Up to 2 windows are used.
    #[serde(default)]
    pub peak_windows: Vec<(u32, u32)>,
    /// Weekend discount (fraction removed) without Train+: adult / reduced.
    #[serde(default)]
    pub weekend_discount_adult: Option<f64>,
    #[serde(default)]
    pub weekend_discount_reduced: Option<f64>,
    /// Off-peak (incl. weekend) discount WITH Train+, all categories.
    #[serde(default)]
    pub train_plus_offpeak_discount: Option<f64>,
    /// Train+ peak per-journey cap (euros): adult / reduced.
    #[serde(default)]
    pub train_plus_peak_cap_adult_euros: Option<f64>,
    #[serde(default)]
    pub train_plus_peak_cap_reduced_euros: Option<f64>,
    /// Fixed airport special-OD fare (euros); overrides base+per-km for an OD
    /// touching an airport station.
    #[serde(default)]
    pub airport_od_euros: Option<f64>,
    /// Substrings identifying an airport station name (e.g. "Airport",
    /// "Luchthaven", "Aeroport"). Config-driven.
    #[serde(default)]
    pub airport_station_names: Vec<String>,
}

/// Display-only day/journey cap for an operator (spec §9). Deserializes leniently;
/// caps are applied only at plan output in a later increment, never in dominance.
#[derive(Debug, Clone, Deserialize)]
pub struct FareCapConfig {
    pub name: String,
    #[serde(default)]
    pub day_cap_euros: Option<f64>,
}

/// Transit-pricing config. `enabled` is THE master switch for the whole feature.
#[derive(Debug, Clone, Deserialize)]
pub struct FaresConfig {
    /// Master switch. When false the price axis, dominance clause, and price
    /// output are all absent and the hot loop is untouched.
    #[serde(default)]
    pub enabled: bool,
    /// ε-bucket on the known-cents axis. Absent ⇒ compiled-in default.
    #[serde(default)]
    pub known_euros_epsilon: Option<KnownEurosEpsilonConfig>,
    /// Fare-modeled operators. Operators absent here are unmodeled (contribute an
    /// incomparable `unknown` token).
    #[serde(default)]
    pub operators: Vec<FareOperatorConfig>,
    /// Display-only caps (inert this increment; spec §9).
    #[serde(default)]
    pub caps: Vec<FareCapConfig>,
    /// SNCB flat agglomeration zones (Brussels / Antwerpen). Each is a config-driven
    /// bounding polygon; a stop inside a zone is collapsed to that zone's single fare
    /// node so railway distance within the zone is not charged (spec Appendix A.2).
    /// Absent ⇒ no zones ⇒ plain full-km SNCB pricing.
    #[serde(default)]
    pub agglomerations: Vec<FareAgglomerationConfig>,
    /// Brupass single-journey price (euros); absent/`None` ⇒ Brupass unavailable.
    /// PLACEHOLDER value — see the config comment. Brupass is NOT a user option: it is
    /// applied automatically as a post-hoc CAP on the Brussels multi-operator fare
    /// (paid in-zone boardings spanning 2+ distinct operators are capped at this price
    /// when cheaper than the individual tickets).
    #[serde(default)]
    pub brupass_euros: Option<f64>,
    /// Brupass coverage window (seconds). Retained for config back-compat; the
    /// post-hoc cap is window-agnostic. Absent ⇒ 3600.
    #[serde(default)]
    pub brupass_validity_secs: Option<u32>,
}

/// One SNCB flat agglomeration zone: an identity plus a bounding polygon. The
/// polygon is a documented approximation of the exact OSM admin boundary
/// (Brussels-Capital Region admin_level 4; City of Antwerp admin_level 8 for the
/// Antwerpen fare zone). `admin_level` and `osm_relation` are recorded for
/// provenance/future exact-boundary refinement; only `polygon` is used at runtime.
#[derive(Debug, Clone, Deserialize)]
pub struct FareAgglomerationConfig {
    /// Zone identifier: "brussels" or "antwerpen" (case-insensitive). An unknown
    /// name is skipped with a warning.
    pub name: String,
    /// OSM admin level the polygon approximates (provenance only; e.g. 4 or 8).
    #[serde(default)]
    pub admin_level: Option<u32>,
    /// OSM boundary relation id the polygon approximates (provenance only).
    #[serde(default)]
    pub osm_relation: Option<u64>,
    /// Bounding polygon as `[[lat, lng], ...]` vertices in order, implicitly closed.
    #[serde(default)]
    pub polygon: Vec<(f64, f64)>,
    /// Canonical central-station name for this zone's fare reference node (spec
    /// Appendix A.2 zone collapse), e.g. "Bruxelles-Central". Matched
    /// (case-insensitive substring) against SNCB stop names to pick the reference
    /// railway node; unset/unmatched falls back to the polygon centroid's nearest
    /// railway node. Provenance/tuning only.
    #[serde(default)]
    pub reference: Option<String>,
}

impl FaresConfig {
    /// Compile into the runtime `FareModel`. Each operator's model is a data-driven
    /// marginal-fare function (`time_window_flat` STIB/De Lijn, `time_window_flat_tiered`
    /// TEC, `distance_base_per_km` SNCB); unknown tags are skipped with a warning.
    /// Euro amounts are converted to integer cents (rounded). Profile-dependent
    /// selection (subscription/card/reduced) happens at charge time, so every price
    /// variant the profile can pick is carried in the model. The TEC express tier is
    /// resolved per route later (`rebuild_operator_fare_lookup`), so the compiled
    /// template starts `is_express = false`.
    pub fn to_fare_model(&self) -> crate::structures::cost::FareModel {
        use crate::structures::cost::{FareModel, KnownEurosEpsilon, OperatorFare};
        // Per-operator fare interpretation is OWNED BY EACH OPERATOR'S INGESTOR, not
        // this generic engine (operator-agnostic policy): `to_fare_model` only maps
        // the config's model tag to the matching ingestor builder and assembles the
        // shared, operator-agnostic wrapper (ε-bucket, zones, Brupass). The builders
        // compose the reusable fare primitives that live in `structures::cost::fares`.
        //   - `time_window_flat`        → STIB/De Lijn ingestor (`gtfs::stib`)
        //   - `time_window_flat_tiered` → generic/TEC ingestor (`gtfs::gtfs`)
        //   - `distance_base_per_km`    → SNCB ingestor (`gtfs::sncb`)
        use crate::ingestion::gtfs::{
            build_sncb_operator, build_tec_operator, build_time_window_operator,
        };
        let cents = |e: Option<f64>| (e.unwrap_or(0.0) * 100.0).round() as u32;
        let known_euros_epsilon = self
            .known_euros_epsilon
            .map(|e| KnownEurosEpsilon { a: e.a, b: e.b })
            .unwrap_or_default();
        let mut operators = Vec::new();
        for op in &self.operators {
            let mut express_route_names = Vec::new();
            let mut express_route_prefixes = Vec::new();
            let mut express_single_cents = 0;
            let mut express_card6_cents = 0;
            let mut express_card6_reduced_cents = 0;
            let mut airport_station_names = Vec::new();
            let model = match op.model.as_str() {
                "time_window_flat" => build_time_window_operator(op, cents),
                "time_window_flat_tiered" => {
                    let tec = build_tec_operator(op, cents);
                    express_route_names = tec.express_route_names;
                    express_route_prefixes = tec.express_route_prefixes;
                    express_single_cents = tec.express_single_cents;
                    express_card6_cents = tec.express_card6_cents;
                    express_card6_reduced_cents = tec.express_card6_reduced_cents;
                    tec.model
                }
                "distance_base_per_km" => {
                    let (model, names) = build_sncb_operator(op, cents);
                    airport_station_names = names;
                    model
                }
                other => {
                    tracing::warn!(
                        "fares: unknown operator model '{}' for '{}' — ignored",
                        other,
                        op.name
                    );
                    continue;
                }
            };
            operators.push(OperatorFare {
                name: op.name.clone(),
                model,
                express_route_names,
                express_route_prefixes,
                express_single_cents,
                express_card6_cents,
                express_card6_reduced_cents,
                airport_station_names,
            });
        }
        // SNCB flat agglomeration zones (spec Appendix A.2). Config-driven bounding
        // polygons; an unknown name or a degenerate (<3-vertex) polygon is skipped
        // with a warning so a malformed entry never silently mis-tags stops.
        use crate::structures::LatLng;
        use crate::structures::cost::{Agglomeration, AgglomerationZone};
        let mut agglomerations = Vec::new();
        for a in &self.agglomerations {
            let zone = match a.name.trim().to_ascii_lowercase().as_str() {
                "brussels" | "bruxelles" | "brussel" => Agglomeration::Brussels,
                "antwerpen" | "antwerp" | "anvers" => Agglomeration::Antwerpen,
                other => {
                    tracing::warn!("fares: unknown agglomeration '{}' — ignored", other);
                    continue;
                }
            };
            if a.polygon.len() < 3 {
                tracing::warn!(
                    "fares: agglomeration '{}' has a degenerate polygon ({} vertices) — ignored",
                    a.name,
                    a.polygon.len()
                );
                continue;
            }
            let polygon = a
                .polygon
                .iter()
                .map(|&(latitude, longitude)| LatLng { latitude, longitude })
                .collect();
            let reference = a
                .reference
                .as_ref()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            agglomerations.push(AgglomerationZone { zone, polygon, reference });
        }

        FareModel {
            enabled: self.enabled,
            known_euros_epsilon,
            operators,
            agglomerations,
            brupass_cents: self.brupass_euros.map(|e| cents(Some(e))).unwrap_or(0),
            brupass_validity_secs: self.brupass_validity_secs.unwrap_or(3600),
        }
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
        assert_eq!(rt.rate_limit.consecutive_failure_threshold, 3);
        assert_eq!(rt.rate_limit.throttled_min_interval_secs, 60);
        assert_eq!(rt.rate_limit.max_requests_per_min, 8);
        assert_eq!(rt.rate_limit.max_requests_per_day, 12_000);
        assert_eq!(rt.poll_interval_secs, 60);
        assert!(rt.feeds.is_empty());
    }

    #[test]
    fn rate_limit_threshold_accepts_legacy_429_alias() {
        let yaml = "enabled: true\nrate_limit:\n  consecutive_429_threshold: 5";
        let rt: RealtimeConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(rt.rate_limit.consecutive_failure_threshold, 5);
    }

    #[test]
    fn requests_per_poll_counts_stib_vehicle_positions() {
        let gtfs = RealtimeFeedConfig::GtfsRt {
            name: "sncb".into(),
            url: "https://x/rt".into(),
            headers: HashMap::new(),
        };
        assert_eq!(gtfs.requests_per_poll(), 1);

        let stib_vp = RealtimeFeedConfig::Stib {
            name: "stib".into(),
            waiting_time_url: "https://x/WaitingTimes/".into(),
            vehicle_position_url: Some("https://x/VehiclePositions/".into()),
            headers: HashMap::new(),
        };
        assert_eq!(stib_vp.requests_per_poll(), 2, "waiting-times + vehicle-positions");

        let stib_no_vp = RealtimeFeedConfig::Stib {
            name: "stib".into(),
            waiting_time_url: "https://x/WaitingTimes/".into(),
            vehicle_position_url: None,
            headers: HashMap::new(),
        };
        assert_eq!(stib_no_vp.requests_per_poll(), 1);
    }

    fn production_like_feeds() -> Vec<RealtimeFeedConfig> {
        let gtfs = |name: &str| RealtimeFeedConfig::GtfsRt {
            name: name.into(),
            url: "https://x/rt".into(),
            headers: HashMap::new(),
        };
        vec![
            gtfs("sncb"),
            gtfs("sncb-alerts"),
            gtfs("delijn"),
            gtfs("delijn-alerts"),
            RealtimeFeedConfig::Stib {
                name: "stib".into(),
                waiting_time_url: "https://x/WaitingTimes/".into(),
                vehicle_position_url: Some("https://x/VehiclePositions/".into()),
                headers: HashMap::new(),
            },
        ]
    }

    #[test]
    fn default_cadence_request_rate_fits_gateway_quota() {
        let yaml = "enabled: true";
        let mut rt: RealtimeConfig = serde_yaml_ng::from_str(yaml).unwrap();
        rt.feeds = production_like_feeds();

        let (per_min, per_day) = rt.request_rate();
        assert!((per_min - 6.0).abs() < 1e-9, "6 req/cycle at 60s = 6/min, got {per_min}");
        assert!((per_day - 8_640.0).abs() < 1e-6, "6/min = 8,640/day, got {per_day}");
        assert!(per_min <= 8.0, "must be <= 8 req/min quota");
        assert!(per_day <= 12_000.0, "must be <= 12k req/day quota");
        assert!(rt.within_quota());
    }

    #[test]
    fn old_30s_cadence_would_have_exceeded_quota() {
        let yaml = "enabled: true\npoll_interval_secs: 30";
        let mut rt: RealtimeConfig = serde_yaml_ng::from_str(yaml).unwrap();
        rt.feeds = production_like_feeds();
        let (per_min, per_day) = rt.request_rate();
        assert!(per_min > 8.0, "30s cadence = 12/min, exceeds 8/min");
        assert!(per_day > 12_000.0, "30s cadence = 17,280/day, exceeds 12k");
        assert!(!rt.within_quota());
    }

    #[test]
    fn shipped_config_yaml_has_stib_fares_enabled() {
        let cfg = Config::load("config.yaml").expect("config.yaml must parse");
        let fares = cfg
            .default_routing
            .fares
            .expect("shipped config.yaml carries a fares block");
        assert!(fares.enabled, "fares are live on the shipped config");
        let model = fares.to_fare_model();
        // STIB must compile to an active time_window_flat operator.
        assert!(
            model.operators.iter().any(|op| op.name.eq_ignore_ascii_case("STIB")
                && matches!(op.model, crate::structures::cost::OperatorModel::TimeWindowFlat { .. })),
            "shipped config models STIB as a time-window flat ticket"
        );
        // SNCB must compile to the EXACT 2026 2nd-class BRACKETED distance tariff.
        let sncb = model
            .operators
            .iter()
            .find(|op| op.name.eq_ignore_ascii_case("SNCB"))
            .expect("shipped config models SNCB");
        match sncb.model {
            crate::structures::cost::OperatorModel::DistanceBasePerKm { tariff, .. } => {
                match tariff {
                    crate::structures::cost::DistanceTariff::Bracketed {
                        a_cents_per_km,
                        b_cents,
                        floor_cents,
                        min_km,
                        cap_from_km,
                        cap_km,
                        ..
                    } => {
                        assert!((a_cents_per_km - 16.8546).abs() < 1e-9);
                        assert!((b_cents - 145.1226).abs() < 1e-9);
                        assert_eq!(floor_cents, 262, "2.6151 EUR → 262 c");
                        assert_eq!(min_km, 3);
                        assert_eq!(cap_from_km, 116);
                        assert_eq!(cap_km, 118);
                        // Spot-check the exact published samples through the shipped tariff.
                        assert_eq!(tariff.fare_cents(47.0), 940, "d_eff=47 → 9.40 EUR");
                        assert_eq!(tariff.fare_cents(118.0), 2130, "d_eff=118 → 21.30 EUR");
                        assert_eq!(tariff.fare_cents(1.0), 260, "d=1 → floor 2.60 EUR");
                    }
                    _ => panic!("shipped SNCB must be the bracketed tariff"),
                }
            }
            _ => panic!("shipped SNCB must be distance_base_per_km"),
        }
    }

    #[test]
    fn shipped_config_yaml_cadence_is_quota_safe() {
        let cfg = Config::load("config.yaml").expect("config.yaml must parse");
        if let Some(rt) = cfg.realtime.filter(|r| r.enabled && !r.feeds.is_empty()) {
            let (per_min, per_day) = rt.request_rate();
            assert!(
                rt.within_quota(),
                "shipped config.yaml realtime cadence exceeds quota: \
                 {per_min:.2} req/min (max {}), {per_day:.0} req/day (max {})",
                rt.rate_limit.max_requests_per_min,
                rt.rate_limit.max_requests_per_day,
            );
        }
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
    fn index_max_age_secs_parses_and_defaults() {
        let with_age = "enabled: true\nindex_max_age_secs: 90";
        let rt: RealtimeConfig = serde_yaml_ng::from_str(with_age).unwrap();
        assert_eq!(rt.index_max_age_secs, 90);

        let without_age = "enabled: true";
        let rt2: RealtimeConfig = serde_yaml_ng::from_str(without_age).unwrap();
        assert_eq!(rt2.index_max_age_secs, 600, "default should be 600");
    }

    #[test]
    fn tracked_delay_ttl_secs_parses_and_defaults() {
        let with_ttl = "enabled: true\ntracked_delay_ttl_secs: 3600";
        let rt: RealtimeConfig = serde_yaml_ng::from_str(with_ttl).unwrap();
        assert_eq!(rt.tracked_delay_ttl_secs, 3600);

        let without_ttl = "enabled: true";
        let rt2: RealtimeConfig = serde_yaml_ng::from_str(without_ttl).unwrap();
        assert_eq!(rt2.tracked_delay_ttl_secs, 86_400, "default should be ~24h");
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
    fn fares_config_parses_and_compiles_to_fare_model() {
        use crate::structures::cost::OperatorModel;
        let yaml = r#"
walking_speed_mps: 1.2
fares:
  enabled: true
  known_euros_epsilon: { a: 10.0, b: 0.0 }
  operators:
    - name: STIB
      model: time_window_flat
      ticket_euros: 2.10
      validity_secs: 5400
    - name: SNCB
      model: distance_base_per_km
      a_euros_per_km: 0.168546
      b_euros: 1.451226
      min_km: 3
      cap_from_km: 116
      cap_km: 118
      floor_euros: 2.6151
      first_class_thresholds: [36, 51]
      first_class_coefficients: [1.40, 1.50, 1.60]
      railway_distance_source: topology
  caps:
    - name: STIB
      day_cap_euros: 7.50
"#;
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let fares = r.fares.expect("fares block parsed");
        assert!(fares.enabled);
        assert_eq!(fares.operators.len(), 2);
        assert_eq!(fares.caps.len(), 1);

        let model = fares.to_fare_model();
        assert!(model.enabled);
        assert_eq!(model.known_euros_epsilon.a, 10.0);
        assert_eq!(model.operators.len(), 2);
        // STIB: euros -> cents, active model.
        match model.operators[0].model {
            OperatorModel::TimeWindowFlat { ticket_cents, validity_secs, .. } => {
                assert_eq!(ticket_cents, 210);
                assert_eq!(validity_secs, 5400);
            }
            _ => panic!("STIB should be time_window_flat"),
        }
        // SNCB: default BRACKETED (exact 2026 2nd-class) tariff; a/b/floor/caps + the
        // inert 1st-class fields compiled from config.
        match model.operators[1].model {
            OperatorModel::DistanceBasePerKm { tariff, .. } => match tariff {
                crate::structures::cost::DistanceTariff::Bracketed {
                    a_cents_per_km,
                    b_cents,
                    floor_cents,
                    min_km,
                    cap_from_km,
                    cap_km,
                    first_class_thresholds,
                    first_class_coeffs,
                    first_class_round_thresholds,
                    first_class_round_grids,
                } => {
                    assert!((a_cents_per_km - 16.8546).abs() < 1e-9, "0.168546 EUR/km → 16.8546 c/km");
                    assert!((b_cents - 145.1226).abs() < 1e-9, "1.451226 EUR → 145.1226 c");
                    assert_eq!(min_km, 3);
                    assert_eq!(cap_from_km, 116);
                    assert_eq!(cap_km, 118);
                    // 2.6151 EUR → 262 cents (rounded to whole cents at compile).
                    assert_eq!(floor_cents, 262);
                    assert_eq!(first_class_thresholds, [36, 51]);
                    assert_eq!(first_class_coeffs, [1.40, 1.50, 1.60]);
                    // 1st-class rounding defaults: 0.10 below 25 EUR, 0.50 in [25,50],
                    // 1 EUR above 50 (thresholds/grids in cents).
                    assert_eq!(first_class_round_thresholds, [2500, 5000]);
                    assert_eq!(first_class_round_grids, [10, 50, 100]);
                }
                _ => panic!("SNCB should default to the bracketed tariff"),
            },
            _ => panic!("SNCB should be distance_base_per_km"),
        }
    }

    #[test]
    fn fares_config_compiles_agglomeration_zones() {
        use crate::structures::LatLng;
        use crate::structures::cost::Agglomeration;
        let yaml = r#"
fares:
  enabled: true
  operators:
    - name: SNCB
      model: distance_base_per_km
      base_euros: 2.60
      per_km_euros: 0.25
  agglomerations:
    - name: brussels
      admin_level: 4
      osm_relation: 54094
      polygon:
        - [50.797, 4.242]
        - [50.764, 4.312]
        - [50.905, 4.402]
        - [50.860, 4.245]
    - name: antwerpen
      admin_level: 8
      polygon:
        - [51.160, 4.352]
        - [51.160, 4.485]
        - [51.275, 4.485]
        - [51.275, 4.352]
    - name: nowhere
      polygon:
        - [0.0, 0.0]
        - [0.0, 1.0]
    - name: unknownzone
      polygon:
        - [1.0, 1.0]
        - [1.0, 2.0]
        - [2.0, 2.0]
"#;
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let model = r.fares.unwrap().to_fare_model();
        // Only the two valid, known-name, >=3-vertex zones survive; the degenerate
        // "nowhere" (2 vertices) and the unknown-name "unknownzone" are skipped.
        assert_eq!(model.agglomerations.len(), 2, "degenerate/unknown zones dropped");
        assert_eq!(model.agglomerations[0].zone, Agglomeration::Brussels);
        assert_eq!(model.agglomerations[1].zone, Agglomeration::Antwerpen);
        // A central-Brussels coordinate falls inside the Brussels polygon.
        assert!(
            model.agglomerations[0].contains(LatLng { latitude: 50.83, longitude: 4.30 }),
            "central Brussels is inside the compiled Brussels polygon"
        );
    }

    #[test]
    fn fares_config_compiles_delijn_tec_and_sncb_rules() {
        use crate::structures::cost::{OperatorModel, SncbTimeRules, TimeWindowOperator};
        let yaml = r#"
fares:
  enabled: true
  operators:
    - name: De Lijn
      model: time_window_flat
      time_window_operator: delijn
      ticket_euros: 3.00
      card_euros: 2.20
      validity_secs: 3600
    - name: TEC
      model: time_window_flat_tiered
      classic_single_euros: 2.80
      express_single_euros: 5.50
      classic_card6_euros: 2.23
      express_card6_euros: 4.40
      classic_card6_reduced_euros: 1.80
      express_card6_reduced_euros: 3.52
      express_route_names: ["EXPRESS", "E"]
    - name: SNCB
      model: distance_base_per_km
      per_km_rate: 0.1240
      floor_euros: 2.60
      peak_windows: [[21600, 32400], [57600, 64800]]
      weekend_discount_adult: 0.30
      weekend_discount_reduced: 0.40
      train_plus_offpeak_discount: 0.40
      train_plus_peak_cap_adult_euros: 14.00
      train_plus_peak_cap_reduced_euros: 5.50
      airport_od_euros: 7.90
      airport_station_names: ["Airport", "Luchthaven"]
  brupass_euros: 2.60
  brupass_validity_secs: 3600
"#;
        let r: RoutingDefaultConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let model = r.fares.unwrap().to_fare_model();
        assert_eq!(model.operators.len(), 3);
        // Brupass placeholder compiles into cents + window.
        assert_eq!(model.brupass_cents, 260, "brupass_euros → cents");
        assert_eq!(model.brupass_validity_secs, 3600);
        match model.operators[0].model {
            OperatorModel::TimeWindowFlat { ticket_cents, card_cents, operator, .. } => {
                assert_eq!(ticket_cents, 300);
                assert_eq!(card_cents, Some(220));
                assert_eq!(operator, TimeWindowOperator::Delijn);
            }
            _ => panic!("De Lijn should be time_window_flat/delijn"),
        }
        match model.operators[1].model {
            OperatorModel::TimeWindowFlatTiered { single_cents, card6_cents, card6_reduced_cents, is_express } => {
                assert!(!is_express, "template tier is classic; per-route express resolved later");
                assert_eq!(single_cents, 280);
                assert_eq!(card6_cents, 223);
                assert_eq!(card6_reduced_cents, 180);
            }
            _ => panic!("TEC should be time_window_flat_tiered"),
        }
        assert_eq!(model.operators[1].express_route_names, vec!["EXPRESS", "E"]);
        match model.operators[2].model {
            OperatorModel::DistanceBasePerKm { tariff, rules, airport_od_cents } => {
                assert_eq!(tariff.floor_cents(), 260, "floor 2.60 compiled");
                assert_eq!(airport_od_cents, 790);
                let SncbTimeRules {
                    n_peak_windows,
                    weekend_discount_adult,
                    train_plus_peak_cap_adult,
                    train_plus_peak_cap_reduced,
                    ..
                } = rules;
                assert_eq!(n_peak_windows, 2);
                assert_eq!(weekend_discount_adult, 0.30);
                assert_eq!(train_plus_peak_cap_adult, 1400);
                assert_eq!(train_plus_peak_cap_reduced, 550);
            }
            _ => panic!("SNCB should be distance_base_per_km"),
        }
        assert_eq!(model.operators[2].airport_station_names, vec!["AIRPORT", "LUCHTHAVEN"]);
    }

    #[test]
    fn fares_absent_means_feature_off() {
        // No `fares` block ⇒ None ⇒ the graph keeps FareModel::default() (disabled),
        // so routing is byte-identical to pre-feature.
        let r: RoutingDefaultConfig =
            serde_yaml_ng::from_str("walking_speed_mps: 1.2\n").unwrap();
        assert!(r.fares.is_none());
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
