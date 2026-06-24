use std::{
    collections::{HashMap, HashSet},
    result,
};

use osmpbf::{Element, ElementReader, RelMemberType, Way};

use crate::ingestion::osm::{Dem, bike_class, elevation_smooth};
use crate::structures::cost::VarGen;
use crate::structures::{
    BikeAttrs, EdgeData, Graph, NodeData, OsmNodeData, StreetEdgeData,
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
    let mut valid_node_ids = HashSet::new();
    // Way IDs belonging to a bicycle route relation. In raw OSM, cycle-route
    // membership lives on the relation, not the member ways, so we collect it
    // here and feed it into `classify` when building edges.
    let mut cycle_route_ways: HashSet<i64> = HashSet::new();

    reader.for_each(|element| match element {
        Element::Way(w) if validate_way(&w) => {
            valid_node_ids.extend(w.refs());
        }
        Element::Relation(r) if bike_class::is_cycle_route_relation(&r) => {
            for m in r.members() {
                if m.member_type == RelMemberType::Way {
                    cycle_route_ways.insert(m.member_id);
                }
            }
        }
        _ => {}
    })?;

    let reader = ElementReader::from_path(pbf_path)?;
    let mut node_vargen: HashMap<i64, VarGen> = HashMap::new();
    reader.for_each(|element| match element {
        Element::DenseNode(n) if valid_node_ids.contains(&n.id()) => {
            add_osm_node(g, n.id(), n.lat(), n.lon());
            let vg = node_var_gen(n.tags());
            if vg != VarGen::NONE {
                node_vargen.insert(n.id(), vg);
            }
        }
        Element::Node(n) if valid_node_ids.contains(&n.id()) => {
            add_osm_node(g, n.id(), n.lat(), n.lon());
            let vg = node_var_gen(n.tags());
            if vg != VarGen::NONE {
                node_vargen.insert(n.id(), vg);
            }
        }
        _ => {}
    })?;

    let reader = ElementReader::from_path(pbf_path)?;

    let mut n = 0;
    let mut failed = 0;
    let mut n_cycleroute = 0;

    reader.for_each(|element| {
        if let Element::Way(w) = element
            && validate_way(&w)
        {
            let node_ids = w.refs().collect::<Vec<_>>();

            let foot = w
                .tags()
                .find(|tag| tag.0 == "foot")
                .is_none_or(|tag| tag.1 != "no");
            let bike = w
                .tags()
                .find(|tag| tag.0 == "bicycle")
                .is_none_or(|tag| tag.1 != "no");
            let car = w
                .tags()
                .find(|tag| tag.0 == "motorcar")
                .is_none_or(|tag| tag.1 != "no");

            let in_cycle_route = cycle_route_ways.contains(&w.id());
            let attrs_fwd = bike_class::classify(&w, true, in_cycle_route);
            let attrs_rev = bike_class::classify(&w, false, in_cycle_route);
            let surface_speed = bike_class::surface_speed(&w, surface_speed_factors);

            let is_structure = way_is_bridge_or_tunnel(&w);
            let seg_deltas = smoothed_segment_deltas(
                g,
                &node_ids,
                dem,
                smoothing_epsilon,
                is_structure,
            );

            for i in 0..node_ids.len().saturating_sub(1) {
                n += 1;
                if attrs_fwd.cycleroute {
                    n_cycleroute += 1;
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
        }
    })?;

    let cycleroute_rate = n_cycleroute as f32 / n as f32;

    tracing::info!(
        "imported {} / {} edges ({}%) - ({}% cycleroutes)",
        n - failed,
        n,
        (n - failed) * 100 / n,
        cycleroute_rate * 100.0
    );

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

fn add_osm_node(g: &mut Graph, id: i64, lat: f64, lon: f64) {
    let eid = format!("map#osm#{}", id);
    let node = OsmNodeData {
        eid,
        lat_lng: crate::structures::LatLng {
            latitude: lat,
            longitude: lon,
        },
    };
    g.add_node(NodeData::OsmNode(node));
}

fn validate_way(way: &Way) -> bool {
    let highway = way.tags().find(|tag| tag.0 == "highway").map(|tag| tag.1);
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

    let access = way.tags().find(|tag| tag.0 == "access").map(|tag| tag.1);
    if matches!(access, Some("no" | "private" | "agricultural" | "forestry")) {
        return false;
    }

    true
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
