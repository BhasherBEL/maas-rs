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
    #[serde(default)]
    pub delay_models: Vec<DelayModelConfig>,
}

fn default_osm_output() -> String {
    "osm.bin".to_string()
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

#[derive(Debug, Clone, Deserialize)]
pub struct RoutingDefaultConfig {
    /// Minimum walk-radius (seconds) used for access/egress stop search.
    /// When absent, the compiled-in default (600 s = 10 min) is used.
    #[serde(default)]
    pub min_access_secs: Option<u32>,
    /// Pedestrian walking speed in m/s. When absent, defaults to 1.2 m/s (4.32 km/h).
    #[serde(default)]
    pub walking_speed_mps: Option<f64>,
    /// Reliability bucket edges (sorted, strictly increasing, each in (0,1)) used to
    /// quantize plan reliability. When absent, defaults to [0.50, 0.80, 0.95].
    #[serde(default)]
    pub reliability_bucket_edges: Option<Vec<f32>>,
    /// Arrival-time slack (seconds) added to the fastest expected arrival when pruning,
    /// widening the explored band so safer-but-slower plans survive. Default 900 s.
    #[serde(default)]
    pub arrival_slack_secs: Option<u32>,
}

impl Ingestor {
    pub fn label(&self) -> &str {
        match self {
            Ingestor::OsmPbf(_) => "osm/pbf",
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.name,
            Ingestor::GtfsSncb(c) => &c.name,
        }
    }

    pub fn url(&self) -> &str {
        match self {
            Ingestor::OsmPbf(c) => &c.url,
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.url,
            Ingestor::GtfsSncb(c) => &c.url,
        }
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        match self {
            Ingestor::OsmPbf(c) => &c.headers,
            Ingestor::GtfsGeneric(c) | Ingestor::GtfsStib(c) => &c.headers,
            Ingestor::GtfsSncb(c) => &c.headers,
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
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, String> {
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read config: {e}"))?;
        serde_yml::from_str(&content).map_err(|e| format!("Failed to parse config: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_defaults() {
        let cfg: ServerConfig = serde_yml::from_str("{}").unwrap();
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 3000);
    }

    #[test]
    fn server_config_custom_values() {
        let yaml = "host: 127.0.0.1\nport: 8080";
        let cfg: ServerConfig = serde_yml::from_str(yaml).unwrap();
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
        let cfg: Config = serde_yml::from_str(yaml).unwrap();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 3000);
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
        let cfg: Config = serde_yml::from_str(yaml).unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 9090);
    }

    #[test]
    fn routing_default_config_walking_speed_absent_is_none() {
        let yaml = "default_routing: {}";
        let cfg: RoutingDefaultConfig = serde_yml::from_str(yaml).unwrap();
        assert!(cfg.walking_speed_mps.is_none());
        assert!(cfg.min_access_secs.is_none());
    }

    #[test]
    fn routing_default_config_walking_speed_parses() {
        let yaml = "walking_speed_mps: 1.4";
        let cfg: RoutingDefaultConfig = serde_yml::from_str(yaml).unwrap();
        assert_eq!(cfg.walking_speed_mps, Some(1.4));
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
        let ing: Ingestor = serde_yml::from_str(yaml).unwrap();
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
        let ing: Ingestor = serde_yml::from_str(yaml).unwrap();
        assert!(ing.headers().is_empty());
    }

    #[test]
    fn auto_update_section_parses() {
        let yaml = "enabled: true\nschedule: \"0 5 * * *\"\ncache_dir: \"mycache\"";
        let au: AutoUpdateConfig = serde_yml::from_str(yaml).unwrap();
        assert!(au.enabled);
        assert_eq!(au.schedule, "0 5 * * *");
        assert_eq!(au.cache_dir, "mycache");
    }

    #[test]
    fn auto_update_cache_dir_defaults() {
        let yaml = "enabled: false\nschedule: \"0 5 * * *\"";
        let au: AutoUpdateConfig = serde_yml::from_str(yaml).unwrap();
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
        let rt: RealtimeConfig = serde_yml::from_str(yaml).unwrap();
        assert!(rt.enabled);
        assert_eq!(rt.poll_interval_secs, 45);
        assert_eq!(rt.request_timeout_secs, 20); // default
        assert_eq!(rt.feeds.len(), 2);
        match &rt.feeds[0] {
            RealtimeFeedConfig::GtfsRt { name, url, headers } => {
                assert_eq!(name, "sncb");
                assert!(url.contains("protobuf"));
                assert_eq!(headers.get("bmc-partner-key").unwrap(), "${BMC_PARTNER_KEY}");
            }
            _ => panic!("expected gtfs-rt feed first"),
        }
        match &rt.feeds[1] {
            RealtimeFeedConfig::Stib { name, waiting_time_url, .. } => {
                assert_eq!(name, "stib");
                assert!(waiting_time_url.ends_with("WaitingTimes/"));
            }
            _ => panic!("expected stib feed second"),
        }
    }

    #[test]
    fn realtime_rate_limit_defaults() {
        let yaml = "enabled: true";
        let rt: RealtimeConfig = serde_yml::from_str(yaml).unwrap();
        assert_eq!(rt.rate_limit.consecutive_429_threshold, 3);
        assert_eq!(rt.rate_limit.throttled_min_interval_secs, 60);
        assert_eq!(rt.poll_interval_secs, 30);
        assert!(rt.feeds.is_empty());
    }
}
