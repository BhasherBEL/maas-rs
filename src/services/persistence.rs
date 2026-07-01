use std::fs;

use postcard::{from_bytes, to_allocvec};

use crate::structures::{AddressIndex, Graph};

/// Magic prefix identifying a maas-rs cache file.
const MAGIC: &[u8; 4] = b"MAAS";
/// Bump when any OSM-side `Graph` field (nodes/edges/kdtree/id_mapper) changes layout.
/// v3: bike-route membership now propagated from OSM relations onto edges' `cycleroute`.
/// v4: `StreetEdgeData` gained a `var_gen` variance-generator field.
/// v5: `elev_delta` is now DEM-denoised per-way at ingestion (RDP smoothing), so
///     stale caches carry raw (noisy) ascent and must be rebuilt.
/// v6: `StreetEdgeData` gained a baked `surface_speed` bike speed factor.
/// v7: OSM view gained a `PlatformIndex` (Stage A platform matching) serialized
///     into `osm.bin` so the GTFS phase can match platform stops on `--update-gtfs`.
/// v8: Stage B1 — OSM view gained `node_levels` + `connector_edges` (level/connector
///     maps); `OsmPlatform` gained `node_ids`; platform ways are now imported as
///     walkable foot edges (so node/edge counts changed). GRAPH_SCHEMA is NOT bumped:
///     all of this is serde-skipped on `Graph` and lives only in the OSM view, mirroring
///     `PlatformIndex`; routing never reads it in B1, so graph.bin's layout is unchanged.
/// v9: `build_platform_index` now also indexes `public_transport=platform` /
///     `railway=platform` OSM **nodes** (with `local_ref`/`ref`); `load_pbf_file`
///     adds those nodes to the graph as unindexed entries. GRAPH stays 12 — these
///     unindexed nodes are serde-skipped and invisible to the routing core.
/// v10: `build_platform_index` now indexes `railway=platform` /
///     `public_transport=platform` OSM **relations** (multipolygon platforms where
///     member ways are untagged). `load_pbf_file` registers member-way nodes as
///     unindexed entries so the resolver can find them. Old osm.bin is missing
///     relation-derived platforms → rebuild needed. GRAPH stays 16: PlatformIndex
///     is OSM-view-only and unindexed nodes remain serde-skipped.
/// v11: `build_platform_index` now excludes bus/tram-only platforms from the index
///     (`highway=bus_stop` or `bus=yes`/`tram=yes`/`trolleybus=yes` without a rail
///     signal). Old osm.bin may contain bus-terminal nodes (e.g. Namur TECN Gare des
///     Bus) that falsely match SNCB rail platform codes → rebuild to purge them.
///     GRAPH stays 17: PlatformIndex is OSM-view-only.
/// v12: `validate_way` now treats `virtual:highway=footway/steps/path/pedestrian`
///     as a fallback when `highway` is absent, importing OSM platform-stair
///     connector ways that were previously silently dropped (e.g. Bruges/Berchem
///     stair-top-to-platform links tagged `virtual:highway=footway`). More ways
///     are imported → node/edge counts in osm.bin change → rebuild required.
///     GRAPH stays 17: no routing-core struct layout change.
pub const OSM_SCHEMA_VERSION: u32 = 12;
/// Bump when any `Graph`/`RaptorIndex` field changes layout (or, like v5, the baked
/// `elev_delta` edge values change meaning).
/// v7: `Graph` gained a serialized `contracted: Option<ContractedGraph>` (P3 node
///     contraction).
/// v8: `RaptorIndex` gained `transit_pattern_segment_timetables` (g-free transit-leg
///     reconstruction for the node-contraction drop).
/// v9: `RaptorIndex` gained `transit_stop_names` (g-free stop-name resolution for the
///     explain survey + plan nodes after the interior-node drop empties `g.nodes`).
/// v10: P3f cutover — node_contraction default ON, the interior-node arrays are DROPPED
///      at build/restore, so graph.bin carries empty `nodes`/`edges` + the contracted graph.
/// v11: overtaking-trips split — `build_raptor_index` now decomposes each pattern into
///      non-overtaking sub-routes so every per-stop departure column is monotonic, so the
///      built pattern set differs (old graph.bin holds unsplit, non-FIFO patterns).
/// v12: Stage B2a snap-relocation — Stage-A platform-matched stops are relocated onto their
///      matched OSM platform node and re-priced (boarding at the platform + a penalised
///      fallback connector to the original street snap node), so the stop anchors and foot
///      connector edges in graph.bin differ from v11. OSM_SCHEMA_VERSION stays 8: osm.bin is
///      serialized at the end of the OSM phase, before relocation runs in the GTFS phase, so
///      no relocated stop/edge/level ever enters it and its layout is unchanged.
/// v13: (was 13, now 14) Connector-cost baking — OSM stairs/elevator/ramp edge lengths are
///      rewritten at build time (before contraction) so `edge_secs` yields the correct
///      slower time instead of charging at flat walking speed. Super-edge segments in the
///      contracted graph now carry the baked lengths, so graph.bin content differs from v12.
/// v15: RaptorIndex carries transit_stop_platform_codes (parallel to names) so the plan/live
///      UI can show "Pl. N"; bump forces a rebuild to populate it (serde-default-compatible
///      load, but the existing graph.bin's array is empty until rebuilt).
/// v16: RaptorIndex carries transit_route_ids (raw GTFS route_id strings, parallel to
///      transit_routes) required for route-level realtime alert matching. Bump forces a rebuild
///      so the field is populated; old graphs silently skip route alerts until rebuilt.
/// v17: B2a platform relocation uses bounded foot Dijkstra to pick the cheapest reachable
///      platform node and suppresses the straight fallback connector when a real mapped path
///      exists. Relocation targets and edge counts change → rebuild required.
/// v18: RaptorIndex carries `transit_stations` (platforms grouped by GTFS `parent_station`
///      into deduped physical stations) plus `TransitStopData.parent_station`. Bump forces a
///      rebuild so the station index is populated; lookup maps are derived on load.
/// v19: `StationInfo` gains `modes` (per-station transport-type set), and the GTFS phase now
///      synthesizes `parent_station` for STIB/DeLijn orphan stops (radius-capped same-name
///      absorption), so the grouped station set + content differ. Rebuild required.
/// v20: `StationInfo` gains `lines` (distinct routes serving the station, each with mode +
///      hex colours), populated at build time for the per-mode line badges in autocomplete.
///      Rebuild required so the field is populated.
pub const GRAPH_SCHEMA_VERSION: u32 = 20;

/// Bump when the persisted (`#[serde]`-non-skipped) fields of [`AddressIndex`] change
/// layout. Sibling cache `address.bin`, independent of the routing graph.
/// v1: initial BeST-Add address index (interned streets/municipalities/postals + rows).
/// v2: records are building-level (keyed by `(street, house_number)`); per-row box
///     numbers collapsed into a `boxes: Vec<AddressBox>` metadata list. Rebuild
///     required so apartment rows aggregate into one building candidate.
pub const ADDRESS_SCHEMA_VERSION: u32 = 2;

const HEADER_LEN: usize = 8;

fn with_header(version: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Verify the magic + version header, returning the payload slice. Any mismatch
/// is an error so the caller can rebuild instead of deserializing stale bytes.
fn split_header<'a>(bytes: &'a [u8], expected: u32, path: &str) -> Result<&'a [u8], String> {
    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(format!(
            "'{path}' is not a maas-rs cache file (missing header)"
        ));
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != expected {
        return Err(format!(
            "'{path}' schema version mismatch (file={version}, expected={expected})"
        ));
    }
    Ok(&bytes[HEADER_LEN..])
}

pub fn save_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header(GRAPH_SCHEMA_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save graph: {e}"))?;
    tracing::info!("graph saved to {path}");
    Ok(())
}

pub fn load_graph(path: &str) -> Result<Graph, String> {
    tracing::info!("restoring graph from {path}…");
    let bytes = fs::read(path).map_err(|e| format!("Failed to read graph file: {e}"))?;
    let payload = split_header(&bytes, GRAPH_SCHEMA_VERSION, path)?;
    let mut graph: Graph =
        from_bytes(payload).map_err(|e| format!("Failed to deserialize graph: {e}"))?;
    graph.raptor.validate().map_err(|e| {
        tracing::error!("{e}");
        e
    })?;
    // Rebuild #[serde(skip)] runtime indices (e.g. trip_id → TripId).
    graph.raptor.build_runtime_indices();
    // Rebuild the #[serde(skip)] spatial edge index for edge-aware snapping.
    graph.build_edge_index();
    // Rebuild the contracted graph's #[serde(skip)] segment R-tree (P3 node contraction).
    if let Some(cg) = graph.contracted.as_mut() {
        cg.build_seg_index();
    }
    tracing::info!("graph restored from {path}");
    Ok(graph)
}

/// Save the OSM network only (no `RaptorIndex`) to `path`, headered with
/// `OSM_SCHEMA_VERSION` so it can be reused across transit-only struct changes.
pub fn save_osm_graph(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = graph.to_osm_postcard()?;
    let bytes = with_header(OSM_SCHEMA_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save OSM graph: {e}"))?;
    tracing::info!("OSM graph saved to {path}");
    Ok(())
}

/// Load an OSM-only cache into a `Graph` with an empty `RaptorIndex`.
pub fn load_osm_graph(path: &str) -> Result<Graph, String> {
    tracing::info!("restoring OSM graph from {path}…");
    let bytes = fs::read(path).map_err(|e| format!("Failed to read OSM graph file: {e}"))?;
    let payload = split_header(&bytes, OSM_SCHEMA_VERSION, path)?;
    let graph = Graph::from_osm_postcard(payload)?;
    tracing::info!("OSM graph restored from {path}");
    Ok(graph)
}

/// Save the sibling [`AddressIndex`] to `path`, headered with `ADDRESS_SCHEMA_VERSION`.
/// Only the interned tables and compact rows are serialized; the token/prefix lookup
/// structures are `#[serde(skip)]` and rebuilt on load.
pub fn save_address_index(index: &AddressIndex, path: &str) -> Result<(), String> {
    let payload =
        to_allocvec(index).map_err(|e| format!("Failed to serialize address index: {e}"))?;
    let bytes = with_header(ADDRESS_SCHEMA_VERSION, &payload);
    fs::write(path, &bytes).map_err(|e| format!("Failed to save address index: {e}"))?;
    tracing::info!("address index saved to {path}");
    Ok(())
}

/// Load an [`AddressIndex`] from `path`, then rebuild its `#[serde(skip)]` lookup
/// structures so search works immediately after deserialization.
pub fn load_address_index(path: &str) -> Result<AddressIndex, String> {
    let bytes = fs::read(path).map_err(|e| format!("Failed to read address index file: {e}"))?;
    let payload = split_header(&bytes, ADDRESS_SCHEMA_VERSION, path)?;
    let mut index: AddressIndex =
        from_bytes(payload).map_err(|e| format!("Failed to deserialize address index: {e}"))?;
    index.rebuild_indexes();
    Ok(index)
}

/// Save `graph` to `path` while preserving the previous good copy.
/// 1. serialize to `<path>.new`, 2. rotate existing `<path>` → `<path>.prev`,
/// 3. atomically rename `<path>.new` → `<path>`. A crash between steps always
///    leaves a valid `<path>` or `<path>.prev` for a later `--restore`.
pub fn save_graph_with_rollback(graph: &Graph, path: &str) -> Result<(), String> {
    let payload = to_allocvec(graph).map_err(|e| format!("Failed to serialize graph: {e}"))?;
    let bytes = with_header(GRAPH_SCHEMA_VERSION, &payload);
    let new_path = format!("{path}.new");
    fs::write(&new_path, &bytes).map_err(|e| format!("Failed to write '{new_path}': {e}"))?;

    if fs::metadata(path).is_ok() {
        let prev_path = format!("{path}.prev");
        fs::rename(path, &prev_path)
            .map_err(|e| format!("Failed to rotate '{path}' to '{prev_path}': {e}"))?;
    }
    fs::rename(&new_path, path).map_err(|e| format!("Failed to publish '{path}': {e}"))?;
    tracing::info!("graph saved to {path} (previous kept as {path}.prev)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::Graph;

    #[test]
    fn rollback_save_rotates_previous() {
        let dir = std::env::temp_dir().join("maas_persist_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();
        let prev_s = format!("{path_s}.prev");
        let _ = std::fs::remove_file(&prev_s);

        save_graph_with_rollback(&Graph::new(), path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());

        save_graph_with_rollback(&Graph::new(), path_s).unwrap();
        assert!(std::path::Path::new(path_s).exists());
        assert!(std::path::Path::new(&prev_s).exists());

        assert!(load_graph(path_s).is_ok());
    }

    #[test]
    fn load_graph_rebuilds_edge_index_for_snapping() {
        use crate::structures::{
            BikeAttrs, EdgeData, Endpoint, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            cost::VarGen,
        };
        let dir = std::env::temp_dir().join("maas_persist_edgeidx_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let mut g = Graph::new();
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.0085));
        let edge = |o, d| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: 607,
                foot: false,
                bike: true,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b));
        g.add_edge(b, edge(b, a));
        // Deliberately do NOT build the edge index before saving: it is #[serde(skip)]
        // and must be rebuilt on load.
        save_graph(&g, path_s).unwrap();

        let loaded = load_graph(path_s).unwrap();
        let (ep, _) = loaded
            .snap_to_edge(50.000, 4.00425, 300.0, |s| s.bike)
            .expect("loaded graph snaps onto the bike edge");
        assert!(
            matches!(ep, Endpoint::OnEdge { .. }),
            "edge index rebuilt on load"
        );
    }

    #[test]
    fn contracted_graph_survives_round_trip() {
        use crate::structures::{
            BikeAttrs, EdgeData, LatLng, NodeData, OsmNodeData, StreetEdgeData, cost::VarGen,
        };
        let dir = std::env::temp_dir().join("maas_persist_contracted_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng { latitude: lat, longitude: lon },
            })
        };
        let mut g = Graph::new();
        // A straight chain a-b-c-d-e: b,c,d are degree-2 interior pass-throughs that the
        // union contraction collapses into super-edges between junctions a and e.
        let coords = [
            ("a", 50.000, 4.0000),
            ("b", 50.000, 4.0010),
            ("c", 50.000, 4.0020),
            ("d", 50.000, 4.0030),
            ("e", 50.000, 4.0040),
        ];
        let ids: Vec<_> = coords.iter().map(|&(id, lat, lon)| g.add_node(mk(id, lat, lon))).collect();
        let edge = |o, d| {
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: 71,
                foot: true,
                bike: true,
                car: false,
                attrs: BikeAttrs::road_default(),
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        for w in ids.windows(2) {
            g.add_edge(w[0], edge(w[0], w[1]));
            g.add_edge(w[1], edge(w[1], w[0]));
        }
        // build_raptor_index() populates transit_node_to_stop, which the contraction reads.
        g.build_raptor_index();

        let mut cg = crate::structures::contraction::ContractedGraph::from_graph_union(&g);
        cg.build_seg_index();
        assert!(cg.junction_count() >= 2, "endpoints a,e are junctions");
        g.contracted = Some(cg);

        save_graph(&g, path_s).unwrap();
        let mut loaded = load_graph(path_s).unwrap();
        // Move the contracted graph out so it can borrow `loaded` immutably below; this
        // also proves load_graph populated it (None ⇒ unwrap panics).
        let cg = loaded.contracted.take().expect("contracted survives the round trip");
        // load_graph rebuilt the serde-skipped seg_index; a coord near a chain edge
        // midpoint resolves to its bounding junctions.
        let entries = cg.walk_entries_arena(&loaded, 50.000, 4.0015, 100.0);
        assert!(!entries.is_empty(), "snap near an edge yields junction entries");
    }

    #[test]
    fn load_rejects_version_mismatch() {
        let dir = std::env::temp_dir().join("maas_persist_version_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        let payload = to_allocvec(&Graph::new()).unwrap();
        let bytes = with_header(GRAPH_SCHEMA_VERSION + 1, &payload);
        std::fs::write(path_s, &bytes).unwrap();

        let err = load_graph(path_s).unwrap_err();
        assert!(err.contains("version mismatch"), "got: {err}");
    }

    #[test]
    fn load_rejects_missing_header() {
        let dir = std::env::temp_dir().join("maas_persist_nohdr_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        // Legacy file: raw postcard, no header.
        let payload = to_allocvec(&Graph::new()).unwrap();
        std::fs::write(path_s, &payload).unwrap();

        let err = load_graph(path_s).unwrap_err();
        assert!(err.contains("missing header"), "got: {err}");
    }

    #[test]
    fn osm_graph_round_trip_drops_raptor() {
        let dir = std::env::temp_dir().join("maas_persist_osm_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("osm.bin");
        let path_s = path.to_str().unwrap();

        save_osm_graph(&Graph::new(), path_s).unwrap();
        let restored = load_osm_graph(path_s).unwrap();
        assert_eq!(restored.node_count(), 0);
        assert_eq!(restored.raptor.transit_trips.len(), 0);
    }

    #[test]
    fn osm_graph_round_trip_preserves_platform_index() {
        use crate::ingestion::osm::{OsmPlatform, PlatformIndex};
        use crate::structures::LatLng;

        let dir = std::env::temp_dir().join("maas_persist_platform_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("osm.bin");
        let path_s = path.to_str().unwrap();

        let mut g = Graph::new();
        g.set_platform_index(PlatformIndex::from_platforms(vec![OsmPlatform {
            refs: vec!["9".into(), "10".into()],
            level: Some(1.0),
            centroid: LatLng {
                latitude: 51.199,
                longitude: 4.433,
            },
            node_ids: vec![crate::structures::NodeID(7)],
        }]));
        let mut nl = std::collections::HashMap::new();
        nl.insert(crate::structures::NodeID(7), 1i16);
        let mut ce = std::collections::HashMap::new();
        ce.insert(
            (crate::structures::NodeID(7), crate::structures::NodeID(8)),
            crate::structures::Connector::Steps,
        );
        g.set_osm_level_data(nl, ce);

        save_osm_graph(&g, path_s).unwrap();
        let restored = load_osm_graph(path_s).unwrap();
        let idx = restored.platform_index();
        assert_eq!(idx.len(), 1);
        let p = idx.platform(0).unwrap();
        assert_eq!(p.refs, vec!["9".to_string(), "10".to_string()]);
        assert_eq!(p.level, Some(1.0));
        assert!((p.centroid.latitude - 51.199).abs() < 1e-9);
        assert!((p.centroid.longitude - 4.433).abs() < 1e-9);
        assert_eq!(p.node_ids, vec![crate::structures::NodeID(7)]);
        // Stage B1 level/connector maps survive the osm.bin round-trip.
        assert_eq!(restored.node_level(crate::structures::NodeID(7)), Some(1));
        assert_eq!(
            restored.connector_kind(crate::structures::NodeID(7), crate::structures::NodeID(8)),
            Some(crate::structures::Connector::Steps)
        );
    }

    #[test]
    fn address_index_round_trip_rebuilds_search() {
        use crate::structures::{AddressIndexBuilder, Named};

        let dir = std::env::temp_dir().join("maas_persist_address_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("address.bin");
        let path_s = path.to_str().unwrap();

        let mut b = AddressIndexBuilder::new();
        let s = b.intern_street(
            "S1",
            Named {
                display: "Rue de la Loi".into(),
                aliases: vec!["Rue de la Loi".into(), "Wetstraat".into()],
            },
        );
        let m = b.intern_municipality(
            "M1",
            Named {
                display: "Bruxelles".into(),
                aliases: vec!["Bruxelles".into(), "Brussel".into()],
            },
        );
        let p = b.intern_postal("P1", "1000".into());
        b.push_record("A1".into(), s, m, p, "16".into(), String::new(), 50.846, 4.367);
        let idx = b.finish();

        save_address_index(&idx, path_s).unwrap();
        let loaded = load_address_index(path_s).unwrap();
        assert_eq!(loaded.search("rue de la loi 16", 5, None).len(), 1);
        assert_eq!(loaded.search("wetstraat 16", 5, None)[0].id, "A1");
    }

    #[test]
    fn load_osm_rejects_graph_file() {
        let dir = std::env::temp_dir().join("maas_persist_xfmt_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("graph.bin");
        let path_s = path.to_str().unwrap();

        // A full graph.bin must not be loadable as an OSM cache (version differs
        // only if the consts diverge, but the payloads are structurally distinct).
        save_graph(&Graph::new(), path_s).unwrap();
        // Force a version divergence to ensure the header guard triggers even
        // when both consts currently share a value.
        let bytes = std::fs::read(path_s).unwrap();
        let bumped = with_header(OSM_SCHEMA_VERSION + 99, &bytes[HEADER_LEN..]);
        std::fs::write(path_s, &bumped).unwrap();
        assert!(load_osm_graph(path_s).is_err());
    }
}
