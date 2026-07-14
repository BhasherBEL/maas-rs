//! Dependency-aware rebuild planner: decides which cached artifacts (osm.bin,
//! graph.bin, address.bin) are still valid. The graph fingerprint embeds the osm
//! fingerprint, so an OSM change cascades to graph automatically.

use crate::structures::Config;

use super::fingerprint::{address_fingerprint, graph_fingerprint, osm_fingerprint};
use super::persistence::{
    ADDRESS_SCHEMA_VERSION, Fingerprint, GRAPH_SCHEMA_VERSION, OSM_SCHEMA_VERSION, header_valid,
};

#[derive(Debug, Clone)]
pub struct RebuildPlan {
    pub osm_fp: Fingerprint,
    pub graph_fp: Fingerprint,
    pub address_fp: Fingerprint,
    pub osm_valid: bool,
    pub graph_valid: bool,
    pub address_valid: bool,
}

pub fn plan_rebuild(config: &Config, cache_dir: &str) -> RebuildPlan {
    let build = &config.build;
    let osm_fp = osm_fingerprint(config, cache_dir);
    let graph_fp = graph_fingerprint(config, cache_dir);
    let address_fp = address_fingerprint(config, cache_dir);

    RebuildPlan {
        osm_valid: header_valid(&build.osm_output, OSM_SCHEMA_VERSION, &osm_fp),
        graph_valid: header_valid(&build.output, GRAPH_SCHEMA_VERSION, &graph_fp),
        address_valid: header_valid(&build.address_output, ADDRESS_SCHEMA_VERSION, &address_fp),
        osm_fp,
        graph_fp,
        address_fp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::Graph;
    use std::io::Write;

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

    fn setup(dir: &std::path::Path) -> Config {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("test.pbf"), b"PBF").unwrap();
        make_zip(&dir.join("gtfs.zip"), &[("stops.txt", "A")]);
        make_zip(&dir.join("addr.zip"), &[("a.xml", "1")]);
        let yaml = format!(
            r#"
build:
  inputs:
    - ingestor: osm/pbf
      url: "path:{d}/test.pbf"
    - ingestor: gtfs/generic
      name: bus
      url: "path:{d}/gtfs.zip"
    - ingestor: address/bestadd
      url: "path:{d}/addr.zip"
  output: "{d}/graph.bin"
  osm_output: "{d}/osm.bin"
  address_output: "{d}/address.bin"
default_routing: {{}}
"#,
            d = dir.display()
        );
        serde_yaml_ng::from_str(&yaml).unwrap()
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("maas_rebuild_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn all_invalid_when_no_caches_exist() {
        let dir = tmp("empty");
        let cfg = setup(&dir);
        let plan = plan_rebuild(&cfg, dir.to_str().unwrap());
        assert!(!plan.osm_valid);
        assert!(!plan.graph_valid);
        assert!(!plan.address_valid);
    }

    #[test]
    fn valid_after_saving_with_matching_fingerprints() {
        use super::super::persistence::{save_address_index, save_graph, save_osm_graph};
        use crate::structures::AddressIndex;
        let dir = tmp("valid");
        let cfg = setup(&dir);
        let cache = dir.to_str().unwrap();
        let plan = plan_rebuild(&cfg, cache);

        save_osm_graph(&Graph::new(), &plan.osm_fp, &cfg.build.osm_output).unwrap();
        save_graph(&Graph::new(), &plan.graph_fp, &cfg.build.output).unwrap();
        save_address_index(&AddressIndex::default(), &plan.address_fp, &cfg.build.address_output)
            .unwrap();

        let plan2 = plan_rebuild(&cfg, cache);
        assert!(plan2.osm_valid, "osm valid after matching save");
        assert!(plan2.graph_valid, "graph valid after matching save");
        assert!(plan2.address_valid, "address valid after matching save");
    }

    #[test]
    fn gtfs_change_invalidates_graph_only() {
        use super::super::persistence::{save_address_index, save_graph, save_osm_graph};
        use crate::structures::AddressIndex;
        let dir = tmp("granular");
        let cfg = setup(&dir);
        let cache = dir.to_str().unwrap();
        let plan = plan_rebuild(&cfg, cache);
        save_osm_graph(&Graph::new(), &plan.osm_fp, &cfg.build.osm_output).unwrap();
        save_graph(&Graph::new(), &plan.graph_fp, &cfg.build.output).unwrap();
        save_address_index(&AddressIndex::default(), &plan.address_fp, &cfg.build.address_output)
            .unwrap();

        make_zip(&dir.join("gtfs.zip"), &[("stops.txt", "CHANGED")]);
        let plan2 = plan_rebuild(&cfg, cache);
        assert!(plan2.osm_valid, "GTFS change: osm.bin still valid (reuse)");
        assert!(!plan2.graph_valid, "GTFS change: graph.bin invalid (rebuild GTFS phase)");
        assert!(plan2.address_valid, "GTFS change: address.bin still valid");
    }

    #[test]
    fn osm_change_cascades_to_graph_but_not_address() {
        use super::super::persistence::{save_address_index, save_graph, save_osm_graph};
        use crate::structures::AddressIndex;
        let dir = tmp("cascade");
        let cfg = setup(&dir);
        let cache = dir.to_str().unwrap();
        let plan = plan_rebuild(&cfg, cache);
        save_osm_graph(&Graph::new(), &plan.osm_fp, &cfg.build.osm_output).unwrap();
        save_graph(&Graph::new(), &plan.graph_fp, &cfg.build.output).unwrap();
        save_address_index(&AddressIndex::default(), &plan.address_fp, &cfg.build.address_output)
            .unwrap();

        std::fs::write(dir.join("test.pbf"), b"PBF-CHANGED").unwrap();
        let plan2 = plan_rebuild(&cfg, cache);
        assert!(!plan2.osm_valid, "OSM change: osm.bin invalid");
        assert!(!plan2.graph_valid, "OSM change: cascades to graph.bin");
        assert!(plan2.address_valid, "OSM change: address.bin untouched");
    }
}
