//! Fingerprinting only covers BUILD-BAKED inputs. Most query-time params re-applied
//! by `apply_routing_defaults` on every startup (bike physics, epsilons, …) are NOT
//! baked into an artifact and so must NOT enter its fingerprint. The exception is any
//! speed that is baked during the build: `walking_speed_mps`, for instance, is written
//! into stairs/ramp/elevator connector edge lengths and so DOES enter the graph
//! fingerprint.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;

use sha2::{Digest, Sha256};

use crate::ingestion::cache::{gtfs_content_hash, resolve_source};
use crate::structures::{BuildConfig, Config, Ingestor, RoutingDefaultConfig};

use super::persistence::Fingerprint;

fn file_content_hash(path: &str) -> Result<String, String> {
    let mut f = fs::File::open(path).map_err(|e| format!("failed to open '{path}': {e}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1 << 16];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| format!("failed to read '{path}': {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

type FileHashStore = BTreeMap<String, String>;

fn file_hash_store_path(cache_dir: &str) -> String {
    format!("{cache_dir}/file_hashes.yml")
}

fn load_file_hash_store(cache_dir: &str) -> FileHashStore {
    fs::read_to_string(file_hash_store_path(cache_dir))
        .ok()
        .and_then(|s| serde_yaml_ng::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_file_hash_store(cache_dir: &str, store: &FileHashStore) {
    if let Ok(s) = serde_yaml_ng::to_string(store) {
        let _ = fs::create_dir_all(cache_dir);
        let _ = fs::write(file_hash_store_path(cache_dir), s);
    }
}

struct FileHashCache {
    store: FileHashStore,
    dirty: bool,
}

impl FileHashCache {
    fn load(cache_dir: &str) -> Self {
        Self {
            store: load_file_hash_store(cache_dir),
            dirty: false,
        }
    }

    fn flush(&self, cache_dir: &str) {
        if self.dirty {
            save_file_hash_store(cache_dir, &self.store);
        }
    }

    fn raw(&mut self, path: &str) -> String {
        let key = match fs::metadata(path) {
            Ok(m) => {
                let mtime = m
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                format!("{path}|{mtime}|{}", m.len())
            }
            Err(_) => return String::new(),
        };
        if let Some(h) = self.store.get(&key) {
            return h.clone();
        }
        let h = file_content_hash(path).unwrap_or_default();
        self.store.insert(key, h.clone());
        self.dirty = true;
        h
    }
}

fn push_f64(h: &mut Sha256, v: f64) {
    h.update(v.to_bits().to_le_bytes());
}

/// Tags presence so `None` and `Some(0.0)` never collide.
fn push_opt_f64(h: &mut Sha256, v: Option<f64>) {
    match v {
        Some(x) => {
            h.update([1u8]);
            push_f64(h, x);
        }
        None => h.update([0u8]),
    }
}

/// A field separator so concatenated fields cannot alias one another.
fn sep(h: &mut Sha256) {
    h.update([0xffu8]);
}

fn ingestor_tag(input: &Ingestor) -> &'static str {
    match input {
        Ingestor::OsmPbf(_) => "osm/pbf",
        Ingestor::GtfsGeneric(_) => "gtfs/generic",
        Ingestor::GtfsStib(_) => "gtfs/stib",
        Ingestor::GtfsSncb(_) => "gtfs/sncb",
        Ingestor::AddressBestAdd(_) => "address/bestadd",
        Ingestor::DemBelgianLambert2008(_) => "dem/belgian-lambert-2008",
    }
}

/// A REMOTE source keys on the PRE-interpolation url (so `${VAR}`/`${file:}` secrets
/// never enter the hash) plus content hash; a `path:` LOCAL source keys on the content
/// hash ONLY, so the same bytes at a different path yield the same fingerprint.
fn hash_input_identity(
    h: &mut Sha256,
    input: &Ingestor,
    cache_dir: &str,
    hash_content: &mut dyn FnMut(&str) -> String,
) {
    h.update(ingestor_tag(input).as_bytes());
    sep(h);
    let content = match resolve_source(input, cache_dir, false) {
        Ok(path) => hash_content(&path),
        Err(_) => String::new(),
    };
    match input.location() {
        Ok(crate::ingestion::cache::SourceLocation::Local(_)) => {
            h.update(b"local");
            sep(h);
            h.update(content.as_bytes());
        }
        Ok(crate::ingestion::cache::SourceLocation::Remote(_)) => {
            h.update(b"remote");
            sep(h);
            h.update(input.url().as_bytes());
            sep(h);
            h.update(content.as_bytes());
        }
        Err(_) => {
            h.update(b"bad");
            sep(h);
            h.update(input.url().as_bytes());
        }
    }
    sep(h);
}

fn hash_osm_params(h: &mut Sha256, build: &BuildConfig) {
    push_f64(h, build.elevation_smoothing_epsilon);
    sep(h);
    for (surface, factor) in build.surface_speed_factors.sorted_entries() {
        h.update(surface.as_bytes());
        h.update([b'=']);
        push_f64(h, factor);
        sep(h);
    }
}

pub fn osm_fingerprint(config: &Config, cache_dir: &str) -> Fingerprint {
    let mut cache = FileHashCache::load(cache_dir);
    let fp = osm_fingerprint_inner(config, cache_dir, &mut cache);
    cache.flush(cache_dir);
    fp
}

fn osm_fingerprint_inner(config: &Config, cache_dir: &str, cache: &mut FileHashCache) -> Fingerprint {
    let build = &config.build;
    let mut h = Sha256::new();
    h.update(b"maas-osm-fp-v1");
    sep(&mut h);

    let mut osm_inputs: Vec<&Ingestor> = build
        .inputs
        .iter()
        .filter(|i| matches!(i, Ingestor::OsmPbf(_)) || i.dem_projection().is_some())
        .collect();
    osm_inputs.sort_by(|a, b| {
        ingestor_tag(a)
            .cmp(ingestor_tag(b))
            .then_with(|| a.label().cmp(b.label()))
    });
    for input in osm_inputs {
        hash_input_identity(&mut h, input, cache_dir, &mut |p| cache.raw(p));
    }

    hash_osm_params(&mut h, build);
    h.finalize().into()
}

pub fn graph_fingerprint(config: &Config, cache_dir: &str) -> Fingerprint {
    let mut cache = FileHashCache::load(cache_dir);
    let fp = graph_fingerprint_inner(config, cache_dir, &mut cache);
    cache.flush(cache_dir);
    fp
}

fn graph_fingerprint_inner(
    config: &Config,
    cache_dir: &str,
    cache: &mut FileHashCache,
) -> Fingerprint {
    let build = &config.build;
    let mut h = Sha256::new();
    h.update(b"maas-graph-fp-v1");
    sep(&mut h);

    let osm = osm_fingerprint_inner(config, cache_dir, cache);
    h.update(osm);
    sep(&mut h);

    let mut gtfs_inputs: Vec<&Ingestor> = build
        .inputs
        .iter()
        .filter(|i| {
            i.phase() >= 1
                && i.address_kind().is_none()
                && matches!(
                    i,
                    Ingestor::GtfsGeneric(_) | Ingestor::GtfsStib(_) | Ingestor::GtfsSncb(_)
                )
        })
        .collect();
    gtfs_inputs.sort_by(|a, b| {
        ingestor_tag(a)
            .cmp(ingestor_tag(b))
            .then_with(|| a.label().cmp(b.label()))
    });
    for input in gtfs_inputs {
        hash_input_identity(&mut h, input, cache_dir, &mut |p| {
            gtfs_content_hash(p).unwrap_or_default()
        });
        // SNCB's companion OSM railway topology (`osm_url`) is baked into stop-snapping.
        if let Ingestor::GtfsSncb(c) = input {
            let osm_path = c
                .osm_url
                .strip_prefix("path:")
                .map(|s| s.to_string())
                .unwrap_or_else(|| c.osm_url.clone());
            h.update(b"sncb-osm");
            sep(&mut h);
            h.update(c.osm_url.as_bytes());
            sep(&mut h);
            h.update(cache.raw(&osm_path).as_bytes());
            sep(&mut h);
        }
    }

    hash_graph_params(&mut h, &config.default_routing, build);
    h.finalize().into()
}

fn hash_graph_params(h: &mut Sha256, routing: &RoutingDefaultConfig, build: &BuildConfig) {
    push_opt_f64(h, routing.station_merge_radius_m);
    sep(h);
    // Baked into stairs/ramp/elevator connector edge LENGTHS during the build (see
    // `Graph::bake_connector_lengths`), so it is a real graph-build input.
    push_opt_f64(h, routing.walking_speed_mps);
    sep(h);
    match routing.connector_cost {
        Some(c) => {
            h.update([1u8]);
            push_opt_f64(h, c.stairs_speed_mps);
            push_opt_f64(h, c.ramp_speed_mps);
            push_opt_f64(h, c.elevator_secs);
            // Baked into fallback relocation connectors during the GTFS phase (see
            // `build_gtfs_phase`), so it is a real graph-build input.
            push_opt_f64(h, c.relocation_fallback_secs);
        }
        None => h.update([0u8]),
    }
    sep(h);
    let mut models: Vec<&crate::structures::DelayModelConfig> = build.delay_models.iter().collect();
    models.sort_by(|a, b| a.mode.cmp(&b.mode));
    for m in models {
        h.update(m.mode.as_bytes());
        h.update([b'=']);
        for (delay, prob) in &m.bins {
            h.update(delay.to_le_bytes());
            h.update((*prob as f64).to_bits().to_le_bytes());
        }
        sep(h);
    }
}

pub fn address_fingerprint(config: &Config, cache_dir: &str) -> Fingerprint {
    let mut h = Sha256::new();
    h.update(b"maas-address-fp-v1");
    sep(&mut h);

    let mut addr_inputs: Vec<&Ingestor> = config
        .build
        .inputs
        .iter()
        .filter(|i| i.address_kind().is_some())
        .collect();
    addr_inputs.sort_by(|a, b| a.label().cmp(b.label()));
    for input in addr_inputs {
        hash_input_identity(&mut h, input, cache_dir, &mut |p| {
            gtfs_content_hash(p).unwrap_or_default()
        });
    }

    push_f64(&mut h, config.default_routing.address_box_coord_epsilon_m());
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn base_config() -> Config {
        let yaml = r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:PBF_PATH"
    - ingestor: gtfs/generic
      name: bus
      url: "path:GTFS_PATH"
    - ingestor: address/bestadd
      url: "path:ADDR_PATH"
default_routing: {}
"#;
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    fn config_with_files(dir: &std::path::Path, pbf: &[u8], gtfs: &[(&str, &str)], addr: &[(&str, &str)]) -> (Config, String) {
        std::fs::create_dir_all(dir).unwrap();
        let pbf_path = dir.join("test.pbf");
        std::fs::write(&pbf_path, pbf).unwrap();
        let gtfs_path = dir.join("gtfs.zip");
        make_zip(&gtfs_path, gtfs);
        let addr_path = dir.join("addr.zip");
        make_zip(&addr_path, addr);

        let yaml = format!(
            r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
    - ingestor: gtfs/generic
      name: bus
      url: "path:{}"
    - ingestor: address/bestadd
      url: "path:{}"
default_routing: {{}}
"#,
            pbf_path.display(),
            gtfs_path.display(),
            addr_path.display()
        );
        let cfg: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        (cfg, dir.to_str().unwrap().to_string())
    }

    fn make_zip(path: &std::path::Path, entries: &[(&str, &str)]) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::SimpleFileOptions::default();
        for (name, content) in entries {
            zip.start_file(*name, opts).unwrap();
            zip.write_all(content.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("maas_fp_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn base_config_parses() {
        let _ = base_config();
    }

    #[test]
    fn fingerprint_is_stable_across_calls() {
        let dir = tmp("stable");
        let (cfg, cache) =
            config_with_files(&dir, b"PBF", &[("stops.txt", "A")], &[("a.xml", "1")]);
        assert_eq!(osm_fingerprint(&cfg, &cache), osm_fingerprint(&cfg, &cache));
        assert_eq!(graph_fingerprint(&cfg, &cache), graph_fingerprint(&cfg, &cache));
        assert_eq!(
            address_fingerprint(&cfg, &cache),
            address_fingerprint(&cfg, &cache)
        );
    }

    #[test]
    fn no_network_needed_for_local_path_source() {
        let dir = tmp("local");
        let (cfg, cache) =
            config_with_files(&dir, b"PBF", &[("stops.txt", "A")], &[("a.xml", "1")]);
        let _ = graph_fingerprint(&cfg, &cache);
        let _ = address_fingerprint(&cfg, &cache);
    }

    #[test]
    fn float_and_map_order_independence() {
        let dir = tmp("order");
        let pbf = dir.join("a.pbf");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |body: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
  surface_speed_factors:
{body}
default_routing: {{}}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let c1 = mk("    asphalt: 1.0\n    gravel: 0.6\n");
        let c2 = mk("    gravel: 0.6\n    asphalt: 1.0\n");
        let cache = dir.to_str().unwrap();
        assert_eq!(osm_fingerprint(&c1, cache), osm_fingerprint(&c2, cache));
    }

    #[test]
    fn gtfs_change_rebuilds_graph_not_address() {
        let dir = tmp("gtfs");
        let (cfg, cache) =
            config_with_files(&dir, b"PBF", &[("stops.txt", "A")], &[("a.xml", "1")]);
        let g0 = graph_fingerprint(&cfg, &cache);
        let a0 = address_fingerprint(&cfg, &cache);
        let o0 = osm_fingerprint(&cfg, &cache);

        make_zip(&dir.join("gtfs.zip"), &[("stops.txt", "DIFFERENT")]);
        let g1 = graph_fingerprint(&cfg, &cache);
        let a1 = address_fingerprint(&cfg, &cache);
        let o1 = osm_fingerprint(&cfg, &cache);

        assert_ne!(g0, g1, "gtfs change must change graph fingerprint");
        assert_eq!(a0, a1, "gtfs change must NOT change address fingerprint");
        assert_eq!(o0, o1, "gtfs change must NOT change osm fingerprint");
    }

    #[test]
    fn osm_content_change_cascades_to_graph_only() {
        let dir = tmp("osmchange");
        let (cfg, cache) =
            config_with_files(&dir, b"PBF", &[("stops.txt", "A")], &[("a.xml", "1")]);
        let o0 = osm_fingerprint(&cfg, &cache);
        let g0 = graph_fingerprint(&cfg, &cache);
        let a0 = address_fingerprint(&cfg, &cache);
        std::fs::write(dir.join("test.pbf"), b"PBF-CHANGED").unwrap();
        let o1 = osm_fingerprint(&cfg, &cache);
        let g1 = graph_fingerprint(&cfg, &cache);
        let a1 = address_fingerprint(&cfg, &cache);

        assert_ne!(o0, o1, "osm content change must change osm fingerprint");
        assert_ne!(g0, g1, "osm content change must cascade to graph fingerprint");
        assert_eq!(a0, a1, "osm content change must NOT change address fingerprint");
    }

    #[test]
    fn elevation_epsilon_change_invalidates_osm_and_graph_not_address() {
        let dir = tmp("eps");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let addr = dir.join("a.zip");
        make_zip(&addr, &[("a.xml", "1")]);
        let mk = |eps: f64| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
    - ingestor: address/bestadd
      url: "path:{}"
  elevation_smoothing_epsilon: {eps}
default_routing: {{}}
"#,
                pbf.display(),
                addr.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let c0 = mk(4.0);
        let c1 = mk(6.0);
        let cache = dir.to_str().unwrap();
        assert_ne!(osm_fingerprint(&c0, cache), osm_fingerprint(&c1, cache));
        assert_ne!(graph_fingerprint(&c0, cache), graph_fingerprint(&c1, cache));
        assert_eq!(
            address_fingerprint(&c0, cache),
            address_fingerprint(&c1, cache)
        );
    }

    #[test]
    fn surface_factor_change_invalidates_osm() {
        let dir = tmp("surf");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |factor: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
  surface_speed_factors:
    gravel: {factor}
default_routing: {{}}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            osm_fingerprint(&mk("0.6"), cache),
            osm_fingerprint(&mk("0.4"), cache)
        );
    }

    #[test]
    fn delay_models_change_invalidates_graph() {
        let dir = tmp("delay");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |prob: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
  delay_models:
    - mode: bus
      bins: [[0, {prob}], [60, 0.5]]
default_routing: {{}}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        let g0 = graph_fingerprint(&mk("0.5"), cache);
        let g1 = graph_fingerprint(&mk("0.7"), cache);
        assert_ne!(g0, g1, "delay_models change must change graph fingerprint");
        assert_eq!(
            osm_fingerprint(&mk("0.5"), cache),
            osm_fingerprint(&mk("0.7"), cache)
        );
    }

    #[test]
    fn station_merge_radius_change_invalidates_graph_not_osm() {
        let dir = tmp("merge");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |r: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
default_routing:
  station_merge_radius_m: {r}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            graph_fingerprint(&mk("250"), cache),
            graph_fingerprint(&mk("300"), cache)
        );
        assert_eq!(
            osm_fingerprint(&mk("250"), cache),
            osm_fingerprint(&mk("300"), cache)
        );
    }

    #[test]
    fn connector_cost_change_invalidates_graph() {
        let dir = tmp("conn");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |s: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
default_routing:
  connector_cost:
    stairs_speed_mps: {s}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            graph_fingerprint(&mk("0.5"), cache),
            graph_fingerprint(&mk("0.7"), cache)
        );
    }

    #[test]
    fn walking_speed_change_invalidates_graph() {
        let dir = tmp("walkspeed");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |s: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
default_routing:
  walking_speed_mps: {s}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            graph_fingerprint(&mk("1.4"), cache),
            graph_fingerprint(&mk("1.0"), cache)
        );
        assert_eq!(
            osm_fingerprint(&mk("1.4"), cache),
            osm_fingerprint(&mk("1.0"), cache)
        );
    }

    #[test]
    fn relocation_fallback_secs_change_invalidates_graph() {
        let dir = tmp("fallback");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let mk = |s: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
default_routing:
  connector_cost:
    relocation_fallback_secs: {s}
"#,
                pbf.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            graph_fingerprint(&mk("30"), cache),
            graph_fingerprint(&mk("90"), cache)
        );
    }

    #[test]
    fn address_epsilon_change_invalidates_address_only() {
        let dir = tmp("addreps");
        std::fs::create_dir_all(&dir).unwrap();
        let pbf = dir.join("p.pbf");
        std::fs::write(&pbf, b"P").unwrap();
        let addr = dir.join("a.zip");
        make_zip(&addr, &[("a.xml", "1")]);
        let mk = |eps: &str| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
    - ingestor: address/bestadd
      url: "path:{}"
default_routing:
  address_box_coord_epsilon_m: {eps}
"#,
                pbf.display(),
                addr.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let cache = dir.to_str().unwrap();
        assert_ne!(
            address_fingerprint(&mk("5.0"), cache),
            address_fingerprint(&mk("8.0"), cache)
        );
        assert_eq!(
            osm_fingerprint(&mk("5.0"), cache),
            osm_fingerprint(&mk("8.0"), cache)
        );
        assert_eq!(
            graph_fingerprint(&mk("5.0"), cache),
            graph_fingerprint(&mk("8.0"), cache)
        );
    }

    #[test]
    fn same_content_different_path_same_fingerprint() {
        let dir1 = tmp("path1");
        let dir2 = tmp("path2");
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir1.join("x.pbf"), b"SAME").unwrap();
        std::fs::write(dir2.join("y_different_name.pbf"), b"SAME").unwrap();
        let mk = |p: std::path::PathBuf| -> Config {
            let yaml = format!(
                r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{}"
default_routing: {{}}
"#,
                p.display()
            );
            serde_yaml_ng::from_str(&yaml).unwrap()
        };
        let c1 = mk(dir1.join("x.pbf"));
        let c2 = mk(dir2.join("y_different_name.pbf"));
        assert_eq!(
            osm_fingerprint(&c1, dir1.to_str().unwrap()),
            osm_fingerprint(&c2, dir2.to_str().unwrap())
        );
    }
}
