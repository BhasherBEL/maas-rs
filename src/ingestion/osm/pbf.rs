use std::{collections::HashSet, result};

use osmpbf::{Element, ElementReader, Way};

use crate::structures::{EdgeData, Graph, NodeData, NodeID, StreetEdgeData};

pub fn load_pbf_file<'a>(pbf_path: &str, g: &mut Graph) -> result::Result<(), osmpbf::Error> {
    let reader = ElementReader::from_path(pbf_path)?;
    let mut valid_node_ids = HashSet::new();
    let mut valid_way_ids = HashSet::<i64>::new();

    reader.for_each(|element| {
        if let Element::Way(w) = element {
            if !validate_way(w.clone()) {
                return;
            }

            valid_way_ids.insert(w.id());
            valid_node_ids.extend(w.refs());
        }
    })?;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| match element {
        Element::DenseNode(n) if valid_node_ids.contains(&n.id()) => {
            let eid = format!("map#osm#{}", n.id());
            let node = NodeData {
                eid,
                lat_lng: crate::structures::LatLng {
                    latitude: n.lat(),
                    longitude: n.lon(),
                },
            };
            g.add_node(node);
        }
        Element::Node(n) if valid_node_ids.contains(&n.id()) => {
            let eid = format!("map#osm#{}", n.id());
            let node = NodeData {
                eid,
                lat_lng: crate::structures::LatLng {
                    latitude: n.lat(),
                    longitude: n.lon(),
                },
            };
            g.add_node(node);
        }

        _ => {}
    })?;

    let reader = ElementReader::from_path(pbf_path)?;

    let mut n = 0;
    let mut failed = 0;

    reader.for_each(|element| {
        if let Element::Way(w) = element {
            if valid_way_ids.contains(&w.id()) {
                let node_ids = w.refs().collect::<Vec<_>>();

                // let from = node_ids[0];
                // let to = node_ids[node_ids.len() - 1];
                //
                // n += 1;
                // if !insert_from_osm_ids(g, from, to, true, false) {
                //     failed += 1;
                // }

                let foot = w
                    .tags()
                    .find(|tag| tag.0 == "foot")
                    .map_or(true, |tag| tag.1 != "no");
                let bike = w
                    .tags()
                    .find(|tag| tag.0 == "bicycle")
                    .map_or(true, |tag| tag.1 != "no");
                let car = w
                    .tags()
                    .find(|tag| tag.0 == "motorcar")
                    .map_or(true, |tag| tag.1 != "no");

                for i in 0..node_ids.len().saturating_sub(1) {
                    n += 1;

                    if !insert_from_osm_ids(
                        g,
                        node_ids[i],
                        node_ids[i + 1],
                        true,
                        true,
                        foot,
                        bike,
                        car,
                    ) {
                        failed += 1;
                    }
                }
            }
        }
    })?;

    println!(
        "Sucessfully imported {} edges out of {} ({}%)",
        n - failed,
        n,
        (n - failed) * 100 / n
    );

    Ok(())
}

fn validate_way(way: Way) -> bool {
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

fn insert_from_osm_ids(
    g: &mut Graph,
    from: i64,
    to: i64,
    bidirectional: bool,
    partial: bool,
    foot: bool,
    bike: bool,
    car: bool,
) -> bool {
    let from_eid = format!("map#osm#{}", from);
    let to_eid = format!("map#osm#{}", to);
    let from_id = *match g.get_id(from_eid.clone()) {
        Some(x) => x,
        None => {
            return false;
        }
    };
    let to_id = *match g.get_id(to_eid.clone()) {
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

    let distance = from_node.lat_lng.dist(to_node.lat_lng) as usize;

    if from_id == NodeID(644251) || to_id == NodeID(644251) {
        println!("Inserting {} <-> {}", from_id, to_id);
    }

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
        }),
    );
    if bidirectional {
        g.add_edge(
            to_id,
            EdgeData::Street(StreetEdgeData {
                origin: to_id,
                destination: from_id,
                partial: partial,
                length: distance,
                foot: true,
                bike: true,
                car: true,
            }),
        );
    }

    true
}
