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
    /// Minimum log level: trace | debug | info | warn | error  (default: info)
    #[serde(default = "default_log_level")]
    pub log_level: String,
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
}

#[derive(Debug, Deserialize)]
pub struct GtfsGenericIngestor {
    pub name: String,
    pub url: String,
    pub phase: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct GtfsSncbIngestor {
    pub name: String,
    pub url: String,
    pub osm_url: String,
    pub phase: Option<u8>,
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
}
