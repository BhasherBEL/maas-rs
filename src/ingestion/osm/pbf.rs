use std::{
    collections::{HashMap, HashSet},
    result,
};

use osmpbf::{Element, ElementReader, RelMemberType, Way};

use crate::ingestion::osm::{
    Dem, bike_class, build_platform_index, effective_highway, elevation_smooth, is_platform_way,
    parse_connector, parse_way_level,
};
use crate::structures::cost::VarGen;
use crate::structures::{
    BikeAttrs, Connector, EdgeData, Graph, NodeData, NodeID, OsmNodeData, StreetEdgeData,
};

fn node_var_gen<'a>(tags: impl Iterator<Item = (&'a str, &'a str)>) -> VarGen {
    let mut vg = VarGen::NONE;
    for (k, v) in tags {
        match (k, v) {
            ("highway", "traffic_signals") => vg = vg.with(VarGen::SIGNALIZED),
            ("crossing", "traffic_signals") => vg = vg.with(VarGen::SIGNALIZED),
            ("highway", "elevator") => vg = vg.with(VarGen::ELEVATOR),
            ("crossing", "uncontrolled" | "unmarked") => vg = vg.with(VarGen::UNCONTROLLED),
            _ => {}
        }
    }
    vg
}

pub fn load_pbf_file(
    pbf_path: &str,
    dem: Option<&Dem>,
    smoothing_epsilon: f64,
    surface_speed_factors: &crate::structures::SurfaceSpeedFactors,
    g: &mut Graph,
) -> result::Result<(), osmpbf::Error> {
    let reader = ElementReader::from_path(pbf_path)?;
    // Street nodes (validate_way ways): indexed into the snap KD-tree as today.
    let mut street_node_ids: HashSet<i64> = HashSet::new();
    // Platform-way nodes (Stage B1): added as routable but kept OUT of the snap
    // KD-tree so GTFS stop snapping is unchanged. A node shared with a street stays
    // a street node (indexed).
    let mut platform_only_node_ids: HashSet<i64> = HashSet::new();
    // OSM nodes tagged public_transport=platform / railway=platform (not way refs):
    // added unindexed so build_platform_index can expose them for B2a relocation.
    let mut platform_node_ids: HashSet<i64> = HashSet::new();
    // Way IDs belonging to a bicycle route relation. In raw OSM, cycle-route
    // membership lives on the relation, not the member ways, so we collect it
    // here and feed it into `classify` when building edges.
    let mut cycle_route_ways: HashSet<i64> = HashSet::new();
    // Way IDs that are members of a railway/public_transport=platform RELATION.
    // These ways are typically untagged (all semantics on the relation), so they
    // are not caught by is_platform_way. Collected here; a separate pass resolves
    // their node refs into platform_only_node_ids so they register in the graph.
    let mut platform_relation_member_ways: HashSet<i64> = HashSet::new();

    reader.for_each(|element| match element {
        Element::Way(w) if validate_way(&w) => {
            street_node_ids.extend(w.refs());
        }
        Element::Way(w) if is_platform_way(&w.tags().collect::<Vec<_>>()) => {
            platform_only_node_ids.extend(w.refs());
        }
        Element::Relation(r) => {
            if bike_class::is_cycle_route_relation(&r) {
                for m in r.members() {
                    if m.member_type == RelMemberType::Way {
                        cycle_route_ways.insert(m.member_id);
                    }
                }
            }
            let rel_tags: Vec<(&str, &str)> = r.tags().collect();
            if is_platform_way(&rel_tags) {
                for m in r.members() {
                    if m.member_type == RelMemberType::Way {
                        platform_relation_member_ways.insert(m.member_id);
                    }
                }
            }
        }
        Element::DenseNode(n) => {
            if n.tags().any(|(k, v)| {
                (k == "railway" && v == "platform") || (k == "public_transport" && v == "platform")
            }) {
                platform_node_ids.insert(n.id());
            }
        }
        Element::Node(n) => {
            if n.tags().any(|(k, v)| {
                (k == "railway" && v == "platform") || (k == "public_transport" && v == "platform")
            }) {
                platform_node_ids.insert(n.id());
            }
        }
        _ => {}
    })?;

    // Pass 1.5: resolve relation member-way node refs into platform_only_node_ids.
    // PBF ordering (nodes→ways→relations) means member-way IDs are only known after
    // pass 1 ends; a separate way-scan collects their node refs.
    if !platform_relation_member_ways.is_empty() {
        let reader = ElementReader::from_path(pbf_path)?;
        reader.for_each(|element| {
            let Element::Way(w) = element else { return };
            if platform_relation_member_ways.contains(&w.id()) {
                platform_only_node_ids.extend(w.refs());
            }
        })?;
    }

    // A node on both a street and a platform is a street node (indexed).
    platform_only_node_ids.retain(|id| !street_node_ids.contains(id));

    let reader = ElementReader::from_path(pbf_path)?;
    let mut node_vargen: HashMap<i64, VarGen> = HashMap::new();
    reader.for_each(|element| {
        let (id, lat, lon, vg) = match element {
            Element::DenseNode(n) => (n.id(), n.lat(), n.lon(), node_var_gen(n.tags())),
            Element::Node(n) => (n.id(), n.lat(), n.lon(), node_var_gen(n.tags())),
            _ => return,
        };
        if street_node_ids.contains(&id) {
            add_osm_node(g, id, lat, lon, true);
            if vg != VarGen::NONE {
                node_vargen.insert(id, vg);
            }
        } else if platform_only_node_ids.contains(&id) {
            // Platform-only node: routable but unindexed (snap-tree excluded).
            add_osm_node(g, id, lat, lon, false);
        } else if platform_node_ids.contains(&id) {
            // Platform-tagged OSM node (public_transport=platform / railway=platform on a
            // node): unindexed so GTFS snapping is unchanged, but registered in id_mapper
            // so build_platform_index can expose it as a relocation target for B2a.
            add_osm_node(g, id, lat, lon, false);
        }
    })?;

    let reader = ElementReader::from_path(pbf_path)?;

    let mut n = 0;
    let mut failed = 0;
    let mut n_cycleroute = 0;
    let mut n_platform = 0;
    // Stage B1 auxiliary OSM data, collected by raw OSM id then resolved to graph
    // NodeIDs after the pass. `osm_levels`: semantic storey per node (leveled ways).
    // `osm_connectors`: directed pedestrian connector edges (stairs/elevator/ramp).
    let mut osm_levels: HashMap<i64, i16> = HashMap::new();
    let mut osm_connectors: HashMap<(i64, i64), Connector> = HashMap::new();

    reader.for_each(|element| {
        let Element::Way(w) = element else { return };
        let tags: Vec<(&str, &str)> = w.tags().collect();
        let is_street = validate_way(&w);
        let is_plat = is_platform_way(&tags);
        if !is_street && !is_plat {
            return;
        }

        let node_ids = w.refs().collect::<Vec<_>>();

        // Retain OSM level (semantic storey) on every node of a leveled way.
        if let Some(lvl) = parse_way_level(&tags) {
            for &id in &node_ids {
                osm_levels.insert(id, lvl);
            }
        }
        // Classify pedestrian vertical connectors from the highway tag.
        let connector = parse_connector(&tags);

        let (foot, bike, car, attrs_fwd, attrs_rev, surface_speed, seg_deltas) = if is_plat
            && !is_street
        {
            // Platform way → walkable foot-only, flat. Strictly additive: bikes/cars
            // never route across a platform; the GTFS stop snap is untouched.
            (
                true,
                false,
                false,
                BikeAttrs::road_default(),
                BikeAttrs::road_default(),
                100u8,
                vec![0i16; node_ids.len().saturating_sub(1)],
            )
        } else {
            let foot = tags
                .iter()
                .find(|t| t.0 == "foot")
                .is_none_or(|t| t.1 != "no");
            let bike = tags
                .iter()
                .find(|t| t.0 == "bicycle")
                .is_none_or(|t| t.1 != "no");
            let car = tags
                .iter()
                .find(|t| t.0 == "motorcar")
                .is_none_or(|t| t.1 != "no");
            let in_cycle_route = cycle_route_ways.contains(&w.id());
            let attrs_fwd = bike_class::classify(&w, true, in_cycle_route);
            let attrs_rev = bike_class::classify(&w, false, in_cycle_route);
            let surface_speed = bike_class::surface_speed(&w, surface_speed_factors);
            let is_structure = way_is_bridge_or_tunnel(&w);
            let seg_deltas =
                smoothed_segment_deltas(g, &node_ids, dem, smoothing_epsilon, is_structure);
            (foot, bike, car, attrs_fwd, attrs_rev, surface_speed, seg_deltas)
        };

        for i in 0..node_ids.len().saturating_sub(1) {
            n += 1;
            if is_plat && !is_street {
                n_platform += 1;
            }
            if attrs_fwd.cycleroute {
                n_cycleroute += 1;
            }
            if let Some(kind) = connector {
                osm_connectors.insert((node_ids[i], node_ids[i + 1]), kind);
                osm_connectors.insert((node_ids[i + 1], node_ids[i]), kind);
            }

            let seg_vg = node_vargen
                .get(&node_ids[i])
                .copied()
                .unwrap_or(VarGen::NONE)
                .with(
                    node_vargen
                        .get(&node_ids[i + 1])
                        .copied()
                        .unwrap_or(VarGen::NONE),
                );

            if !insert_from_osm_ids(
                g,
                node_ids[i],
                node_ids[i + 1],
                true,
                true,
                foot,
                bike,
                car,
                attrs_fwd,
                attrs_rev,
                seg_vg,
                seg_deltas[i],
                surface_speed,
            ) {
                failed += 1;
            }
        }
    })?;

    let cycleroute_rate = n_cycleroute as f32 / n as f32;

    tracing::info!(
        "imported {} / {} edges ({}%) - ({}% cycleroutes, {} platform segments)",
        n - failed,
        n,
        (n - failed) * 100 / n,
        cycleroute_rate * 100.0,
        n_platform
    );

    // Resolve raw OSM ids → graph NodeIDs for the level/connector maps.
    let to_nid = |id: i64| g.get_id(&format!("map#osm#{id}")).copied();
    let node_levels: HashMap<NodeID, i16> = osm_levels
        .into_iter()
        .filter_map(|(id, lvl)| to_nid(id).map(|n| (n, lvl)))
        .collect();
    let connector_edges: HashMap<(NodeID, NodeID), Connector> = osm_connectors
        .into_iter()
        .filter_map(|((a, b), k)| Some(((to_nid(a)?, to_nid(b)?), k)))
        .collect();
    tracing::info!(
        "osm level/connector data: {} leveled nodes, {} connector edges",
        node_levels.len(),
        connector_edges.len()
    );
    g.set_osm_level_data(node_levels, connector_edges);

    g.set_platform_index(build_platform_index(pbf_path, g)?);

    Ok(())
}

/// True when the way is tagged as a bridge or tunnel (any value except `no`).
/// Such ways get end-to-end linear elevation interpolation, because a DTM reads
/// the valley floor / canopy under them and fabricates huge false climbs.
fn way_is_bridge_or_tunnel(w: &Way) -> bool {
    w.tags().any(|(k, v)| {
        (k == "bridge" || k == "tunnel") && v != "no"
    })
}

/// Smoothed signed per-segment elevation delta (meters, `i16`) for each
/// consecutive node pair along `node_ids`. Returns one entry per segment.
///
/// When the DEM is absent — or a node has no DEM sample — the affected deltas
/// are `0`, preserving the no-elevation behavior. Otherwise the way's
/// `(cumulative_distance, elevation)` profile is denoised once (RDP, vertical
/// epsilon `smoothing_epsilon`; or straight linear interpolation for
/// bridges/tunnels) and each segment's delta is the difference of the smoothed
/// endpoint elevations, rounded to whole meters. Deltas telescope along the way,
/// so they sum to `smoothed(last) − smoothed(first)`.
fn smoothed_segment_deltas(
    g: &Graph,
    node_ids: &[i64],
    dem: Option<&Dem>,
    smoothing_epsilon: f64,
    is_structure: bool,
) -> Vec<i16> {
    let n_seg = node_ids.len().saturating_sub(1);
    let Some(dem) = dem else {
        return vec![0; n_seg];
    };

    let mut profile: Vec<(f64, f64)> = Vec::with_capacity(node_ids.len());
    let mut cum = 0.0;
    let mut prev_loc: Option<crate::structures::LatLng> = None;
    let mut all_sampled = true;
    for id in node_ids {
        let eid = format!("map#osm#{}", id);
        let loc = g.get_id(eid.as_str()).and_then(|nid| g.get_node(*nid)).map(|nd| nd.loc());
        let Some(loc) = loc else {
            all_sampled = false;
            break;
        };
        if let Some(prev) = prev_loc {
            cum += prev.dist(loc);
        }
        prev_loc = Some(loc);
        match dem.elevation(loc.latitude, loc.longitude) {
            Some(z) => profile.push((cum, z as f64)),
            None => {
                all_sampled = false;
                break;
            }
        }
    }

    if !all_sampled || profile.len() != node_ids.len() {
        return vec![0; n_seg];
    }

    let smoothed = if is_structure {
        elevation_smooth::linear_profile(&profile)
    } else {
        elevation_smooth::smooth_profile(&profile, smoothing_epsilon)
    };

    smoothed
        .windows(2)
        .map(|w| ((w[1] - w[0]).round() as i32).clamp(-30000, 30000) as i16)
        .collect()
}

fn add_osm_node(g: &mut Graph, id: i64, lat: f64, lon: f64, indexed: bool) {
    let eid = format!("map#osm#{}", id);
    let node = OsmNodeData {
        eid,
        lat_lng: crate::structures::LatLng {
            latitude: lat,
            longitude: lon,
        },
    };
    if indexed {
        g.add_node(NodeData::OsmNode(node));
    } else {
        g.add_osm_node_unindexed(node);
    }
}

fn validate_way(way: &Way) -> bool {
    let tags: Vec<(&str, &str)> = way.tags().collect();
    validate_way_tags(&tags)
}

/// Tag-slice core of [`validate_way`], extracted so it can be unit-tested
/// without constructing an `osmpbf::Way`.
fn validate_way_tags(tags: &[(&str, &str)]) -> bool {
    // Resolve highway type: `highway` wins; fall back to `virtual:highway` for
    // the foot-traversable pedestrian values already accepted below.
    let highway = effective_highway(tags);
    if !matches!(
        highway,
        Some(
            "motorway"
                | "trunk"
                | "primary"
                | "secondary"
                | "tertiary"
                | "unclassified"
                | "residential"
                | "service"
                | "living_street"
                | "motorway_link"
                | "trunk_link"
                | "primary_link"
                | "secondary_link"
                | "tertiary_link"
                | "footway"
                | "cycleway"
                | "bridleway"
                | "path"
                | "track"
                | "pedestrian"
                | "steps"
        )
    ) {
        return false;
    }

    let access = tags.iter().find(|t| t.0 == "access").map(|t| t.1);
    if matches!(access, Some("no" | "private" | "agricultural" | "forestry")) {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::{add_osm_node, insert_from_osm_ids, validate_way_tags};
    use crate::ingestion::osm::{ConnectorCost, is_platform_way, parse_connector, parse_way_level};
    use crate::structures::cost::VarGen;
    use crate::structures::{BikeAttrs, Connector, Graph};
    use std::collections::HashMap;

    #[test]
    fn b1_platform_way_imports_unindexed_foot_edge_carrying_level() {
        let plat_tags = [("railway", "platform"), ("level", "1")];
        assert!(is_platform_way(&plat_tags));
        assert!(parse_connector(&plat_tags).is_none());
        let level = parse_way_level(&plat_tags).expect("platform way carries a level");

        let mut g = Graph::new();
        add_osm_node(&mut g, 1001, 50.001, 4.001, false);
        add_osm_node(&mut g, 1002, 50.001, 4.002, false);

        let n1 = *g.get_id("map#osm#1001").expect("platform node registered in id_mapper");
        let n2 = *g.get_id("map#osm#1002").expect("platform node registered in id_mapper");

        assert!(
            insert_from_osm_ids(
                &mut g,
                1001,
                1002,
                true,
                true,
                true,
                false,
                false,
                BikeAttrs::road_default(),
                BikeAttrs::road_default(),
                VarGen::NONE,
                0,
                100,
            ),
            "platform foot edge should be inserted"
        );

        let mut levels = HashMap::new();
        levels.insert(n1, level);
        levels.insert(n2, level);
        g.set_osm_level_data(levels, HashMap::new());

        assert!(
            g.nearest_node(50.001, 4.0015).is_none(),
            "platform nodes must be excluded from the snap KD-tree"
        );

        g.build_raptor_index();
        let reach = g.walk_dijkstra(n1, 600);
        assert!(
            reach.contains_key(&n2),
            "platform way must produce a foot-traversable edge between its nodes"
        );

        assert_eq!(g.node_level(n1), Some(1), "platform level must be retained");
        assert_eq!(g.node_level(n2), Some(1), "platform level must be retained");
    }

    #[test]
    fn platform_relation_member_node_registered_not_in_snap_tree() {
        let mut g = Graph::new();
        add_osm_node(&mut g, 5001, 50.8, 4.7, false);

        assert!(
            g.get_id("map#osm#5001").is_some(),
            "relation member-way node must be registered in id_mapper"
        );
        assert!(
            g.nearest_node(50.8, 4.7).is_none(),
            "relation member-way node must not appear in the snap KD-tree"
        );
    }

    #[test]
    fn platform_node_registered_in_graph_but_not_in_snap_tree() {
        // A public_transport=platform OSM node must be added to the graph as an
        // unindexed node so build_platform_index can resolve it via g.get_id(),
        // but it must not pollute the snap KD-tree used for GTFS stop snapping.
        let mut g = Graph::new();
        add_osm_node(&mut g, 9001, 51.0, 4.0, false);

        assert!(
            g.get_id("map#osm#9001").is_some(),
            "platform node must be registered in id_mapper for build_platform_index"
        );
        assert!(
            g.nearest_node(51.0, 4.0).is_none(),
            "platform node must not appear in the snap KD-tree"
        );
    }

    #[test]
    fn b1_steps_connector_bridges_levels_with_costed_traversal() {
        let steps_tags = [("highway", "steps")];
        assert_eq!(parse_connector(&steps_tags), Some(Connector::Steps));

        let mut g = Graph::new();
        add_osm_node(&mut g, 2001, 50.0, 4.0, true);
        add_osm_node(&mut g, 2002, 50.0, 4.0001, true);

        let concourse = *g.get_id("map#osm#2001").expect("street node registered");
        let platform = *g.get_id("map#osm#2002").expect("street node registered");

        assert!(insert_from_osm_ids(
            &mut g,
            2001,
            2002,
            true,
            true,
            true,
            false,
            false,
            BikeAttrs::road_default(),
            BikeAttrs::road_default(),
            VarGen::NONE,
            0,
            100,
        ));

        let mut levels = HashMap::new();
        levels.insert(concourse, 0i16);
        levels.insert(platform, 1i16);
        let kind = parse_connector(&steps_tags).unwrap();
        let mut connectors = HashMap::new();
        connectors.insert((concourse, platform), kind);
        connectors.insert((platform, concourse), kind);
        g.set_osm_level_data(levels, connectors);

        g.set_connector_cost(ConnectorCost {
            stairs_speed_mps: 0.5,
            ramp_speed_mps: 0.9,
            elevator_secs: 45.0,
            relocation_fallback_secs: 60.0,
        });

        assert_eq!(g.node_level(concourse), Some(0));
        assert_eq!(g.node_level(platform), Some(1));
        assert_eq!(g.connector_kind(concourse, platform), Some(Connector::Steps));
        assert_eq!(g.connector_kind(platform, concourse), Some(Connector::Steps));

        let secs = g.connector_cost().seconds(Connector::Steps, 10.0);
        assert!((secs - 20.0).abs() < 1e-9, "config-costed stairs traversal");
        assert!(
            secs > 10.0 / g.walking_speed_mps(),
            "stairs traversal must be slower than level walking and non-zero"
        );

        g.build_raptor_index();
        let reach = g.walk_dijkstra(concourse, 600);
        assert!(
            reach.contains_key(&platform),
            "steps connector must be a foot-traversable edge bridging the two levels"
        );
    }

    // --- virtual:highway fallback tests for validate_way_tags ---

    #[test]
    fn virtual_highway_footway_accepted_when_highway_absent() {
        assert!(
            validate_way_tags(&[("virtual:highway", "footway")]),
            "virtual:highway=footway must be accepted as a walkable way when highway is absent"
        );
    }

    #[test]
    fn virtual_highway_steps_accepted_when_highway_absent() {
        assert!(
            validate_way_tags(&[("virtual:highway", "steps")]),
            "virtual:highway=steps must be accepted as a walkable way when highway is absent"
        );
    }

    #[test]
    fn virtual_highway_path_and_pedestrian_accepted() {
        assert!(validate_way_tags(&[("virtual:highway", "path")]));
        assert!(validate_way_tags(&[("virtual:highway", "pedestrian")]));
    }

    #[test]
    fn virtual_highway_motorway_rejected() {
        assert!(
            !validate_way_tags(&[("virtual:highway", "motorway")]),
            "virtual:highway=motorway must NOT be imported as a routable way"
        );
    }

    #[test]
    fn virtual_highway_non_pedestrian_values_rejected() {
        assert!(!validate_way_tags(&[("virtual:highway", "residential")]));
        assert!(!validate_way_tags(&[("virtual:highway", "cycleway")]));
        assert!(!validate_way_tags(&[("virtual:highway", "service")]));
    }

    #[test]
    fn real_highway_footway_still_accepted_regression() {
        assert!(
            validate_way_tags(&[("highway", "footway")]),
            "real highway=footway must still pass validate_way (regression)"
        );
    }

    #[test]
    fn highway_wins_over_virtual_highway() {
        assert!(
            validate_way_tags(&[("highway", "footway"), ("virtual:highway", "motorway")]),
            "explicit highway=footway wins over virtual:highway=motorway"
        );
        assert!(
            validate_way_tags(&[("highway", "motorway"), ("virtual:highway", "footway")]),
            "highway=motorway is a car road and must still pass validate_way"
        );
    }

    #[test]
    fn access_no_still_rejects_virtual_highway_footway() {
        assert!(
            !validate_way_tags(&[("virtual:highway", "footway"), ("access", "no")]),
            "access=no must suppress even a virtual:highway=footway way"
        );
        assert!(
            !validate_way_tags(&[("virtual:highway", "footway"), ("access", "private")]),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_from_osm_ids(
    g: &mut Graph,
    from: i64,
    to: i64,
    bidirectional: bool,
    partial: bool,
    foot: bool,
    bike: bool,
    car: bool,
    attrs_fwd: BikeAttrs,
    attrs_rev: BikeAttrs,
    var_gen: VarGen,
    delta: i16,
    surface_speed: u8,
) -> bool {
    let from_eid = format!("map#osm#{}", from);
    let to_eid = format!("map#osm#{}", to);
    let from_id = *match g.get_id(from_eid.as_str()) {
        Some(x) => x,
        None => {
            return false;
        }
    };
    let to_id = *match g.get_id(to_eid.as_str()) {
        Some(x) => x,
        None => {
            return false;
        }
    };

    let from_node = match g.get_node(from_id) {
        Some(x) => x,
        None => {
            return false;
        }
    };

    let to_node = match g.get_node(to_id) {
        Some(x) => x,
        None => {
            return false;
        }
    };

    let distance = from_node.loc().dist(to_node.loc()) as usize;

    g.add_edge(
        from_id,
        EdgeData::Street(StreetEdgeData {
            origin: from_id,
            destination: to_id,
            length: distance,
            partial,
            foot,
            bike,
            car,
            attrs: attrs_fwd,
            elev_delta: delta,
            surface_speed,
            var_gen,
        }),
    );
    if bidirectional {
        g.add_edge(
            to_id,
            EdgeData::Street(StreetEdgeData {
                origin: to_id,
                destination: from_id,
                length: distance,
                partial,
                foot,
                bike,
                car,
                attrs: attrs_rev,
                elev_delta: -delta,
                surface_speed,
                var_gen,
            }),
        );
    }

    true
}
