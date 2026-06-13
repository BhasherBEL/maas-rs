use std::{collections::HashSet, result};

use osmpbf::{Element, ElementReader, RelMemberType, Way};

use crate::ingestion::osm::{Dem, bike_class};
use crate::structures::{
    BikeAttrs, EdgeData, Graph, NodeData, NodeID, OsmNodeData, StreetEdgeData,
};

pub fn load_pbf_file(
    pbf_path: &str,
    dem: Option<&Dem>,
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
    let mut add_osm_node = |id: i64, lat: f64, lon: f64| {
        let eid = format!("map#osm#{}", id);
        let node = OsmNodeData {
            eid,
            lat_lng: crate::structures::LatLng {
                latitude: lat,
                longitude: lon,
            },
        };
        g.add_node(NodeData::OsmNode(node));
    };
    reader.for_each(|element| match element {
        Element::DenseNode(n) if valid_node_ids.contains(&n.id()) => {
            add_osm_node(n.id(), n.lat(), n.lon());
        }
        Element::Node(n) if valid_node_ids.contains(&n.id()) => {
            add_osm_node(n.id(), n.lat(), n.lon());
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

            for i in 0..node_ids.len().saturating_sub(1) {
                n += 1;
                if attrs_fwd.cycleroute {
                    n_cycleroute += 1;
                }

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
                    dem,
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
    dem: Option<&Dem>,
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

    // Signed elevation change from→to (meters), from the DEM if available.
    let elev = |id: NodeID| -> Option<f32> {
        let loc = g.get_node(id)?.loc();
        dem.and_then(|d| d.elevation(loc.latitude, loc.longitude))
    };
    let delta = match (elev(from_id), elev(to_id)) {
        (Some(a), Some(b)) => ((b - a).round() as i32).clamp(-30000, 30000) as i16,
        _ => 0,
    };

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
            }),
        );
    }

    true
}
