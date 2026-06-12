//! Classify an OSM `Way`'s tags into `BikeAttrs` once at ingest. Mirrors the
//! BRouter `way`-context logic for the tags we read (ferries excluded).

use osmpbf::Way;

use crate::structures::{BikeAttrs, HighwayClass, Surface};

/// Looks up a tag value on a way.
fn tag<'a>(w: &'a Way, key: &str) -> Option<&'a str> {
    w.tags().find(|(k, _)| *k == key).map(|(_, v)| v)
}

fn classify_highway(v: Option<&str>) -> HighwayClass {
    match v {
        Some("motorway") => HighwayClass::Motorway,
        Some("motorway_link") => HighwayClass::MotorwayLink,
        Some("trunk") => HighwayClass::Trunk,
        Some("trunk_link") => HighwayClass::TrunkLink,
        Some("primary") => HighwayClass::Primary,
        Some("primary_link") => HighwayClass::PrimaryLink,
        Some("secondary") => HighwayClass::Secondary,
        Some("secondary_link") => HighwayClass::SecondaryLink,
        Some("tertiary") => HighwayClass::Tertiary,
        Some("tertiary_link") => HighwayClass::TertiaryLink,
        Some("unclassified") => HighwayClass::Unclassified,
        Some("residential") => HighwayClass::Residential,
        Some("living_street") => HighwayClass::LivingStreet,
        Some("service") => HighwayClass::Service,
        Some("cycleway") => HighwayClass::Cycleway,
        Some("footway") => HighwayClass::Footway,
        Some("path") => HighwayClass::Path,
        Some("track") => HighwayClass::Track,
        Some("bridleway") => HighwayClass::Bridleway,
        Some("pedestrian") => HighwayClass::Pedestrian,
        Some("steps") => HighwayClass::Steps,
        Some("road") => HighwayClass::Road,
        _ => HighwayClass::Other,
    }
}

fn classify_surface(v: Option<&str>) -> Surface {
    match v {
        Some("paved" | "asphalt" | "concrete" | "paving_stones" | "sett") => Surface::Paved,
        Some(_) => Surface::Unpaved,
        None => Surface::Unknown,
    }
}

fn any_cycleroute(w: &Way) -> bool {
    ["icn", "ncn", "rcn", "lcn"]
        .iter()
        .any(|n| tag(w, &format!("route_bicycle_{n}")) == Some("yes"))
        || tag(w, "lcn") == Some("yes")
}

fn is_bike(w: &Way) -> bool {
    tag(w, "bicycle_road") == Some("yes")
        || matches!(tag(w, "bicycle"), Some("yes" | "permissive" | "designated"))
        || tag(w, "lcn") == Some("yes")
}

/// BRouter way-level `bikeaccess`.
fn bike_access(w: &Way) -> bool {
    let default_access = match tag(w, "access") {
        None => tag(w, "motorroad") != Some("yes"),
        Some("private" | "no") => false,
        Some(_) => true,
    };
    match tag(w, "bicycle") {
        None => match tag(w, "vehicle") {
            None => {
                if matches!(tag(w, "highway"), Some("footway")) {
                    false
                } else {
                    default_access
                }
            }
            Some(v) => !matches!(v, "private" | "no"),
        },
        Some(b) if tag(w, "bicycle_road") == Some("yes") => b != "no", // bicycle_road wins
        Some(b) => !matches!(b, "private" | "no" | "dismount" | "use_sidepath"),
    }
}

fn foot_access(w: &Way, bikeaccess: bool) -> bool {
    if bikeaccess || tag(w, "bicycle") == Some("dismount") {
        return true;
    }
    match tag(w, "foot") {
        None => match tag(w, "access") {
            None => tag(w, "motorroad") != Some("yes"),
            Some("private" | "no") => false,
            Some(_) => true,
        },
        Some(f) => !matches!(f, "private" | "no"),
    }
}

/// Whether traversing in the given direction is a "bad oneway" for bikes.
/// `forward` = the edge runs along the way's node order.
fn wrong_way(w: &Way, forward: bool) -> bool {
    let reversed = !forward;
    let cycleway_opp = ["cycleway", "cycleway:left", "cycleway:right"].iter().any(|k| {
        matches!(tag(w, k), Some("opposite" | "opposite_lane" | "opposite_track"))
    });
    let oneway_bicycle_no = tag(w, "oneway:bicycle") == Some("no");
    if cycleway_opp || oneway_bicycle_no {
        return false;
    }
    let roundabout = tag(w, "junction") == Some("roundabout");
    if reversed {
        match tag(w, "oneway:bicycle") {
            Some("yes") => true,
            _ => match tag(w, "oneway") {
                None => roundabout,
                Some(o) => matches!(o, "yes" | "true" | "1"),
            },
        }
    } else {
        tag(w, "oneway") == Some("-1")
    }
}

/// Classify a way for a directed edge. `forward` distinguishes the two emitted
/// directions so oneway handling is direction-correct.
pub fn classify(w: &Way, forward: bool) -> BikeAttrs {
    let bikeaccess = bike_access(w);
    BikeAttrs {
        highway: classify_highway(tag(w, "highway")),
        surface: classify_surface(tag(w, "surface")),
        tracktype: match tag(w, "tracktype") {
            Some("grade1") => 1,
            Some("grade2") => 2,
            Some("grade3") => 3,
            Some("grade4") => 4,
            Some("grade5") => 5,
            _ => 0,
        },
        isbike: is_bike(w),
        cycleroute: any_cycleroute(w),
        bikeaccess,
        footaccess: foot_access(w, bikeaccess),
        wrong_way: wrong_way(w, forward),
    }
}
