//! Classify an OSM `Way`'s tags into `BikeAttrs` once at ingest. Mirrors the
//! BRouter `way`-context logic for the tags we read (ferries excluded).

use osmpbf::{Relation, Way};

use crate::structures::{BikeAttrs, HighwayClass, Surface, SurfaceSpeedFactors};

/// Looks up a tag value on a way.
fn tag<'a>(w: &'a Way, key: &str) -> Option<&'a str> {
    w.tags().find(|(k, _)| *k == key).map(|(_, v)| v)
}

/// Per-edge bike speed factor (quantized to `u8`) for a way, from its raw OSM
/// `surface=*` tag and the configured factor table. Non-directional, so both
/// emitted directed edges share it.
pub fn surface_speed(w: &Way, factors: &SurfaceSpeedFactors) -> u8 {
    factors.quantize(tag(w, "surface"))
}

/// Pure predicate: do these relation tags describe a signposted cycle route?
/// In raw OSM, cycle-route membership lives on `type=route, route=bicycle`
/// (and `mtb` / `superroute`) relations — not on the member ways — so the ways'
/// `route_bicycle_*`/`lcn` tags are almost never present. This lets the ingester
/// propagate membership from the relation onto its member ways.
fn tags_are_cycle_route<'a>(tags: impl Iterator<Item = (&'a str, &'a str)>) -> bool {
    let mut is_route = false;
    let mut is_bike = false;
    for (k, v) in tags {
        match k {
            "type" => is_route = matches!(v, "route" | "superroute"),
            "route" => is_bike = matches!(v, "bicycle" | "mtb"),
            _ => {}
        }
    }
    is_route && is_bike
}

/// Whether a relation is a cycle route whose member ways should be treated as a
/// signposted cycle route (BRouter's lcn/rcn/ncn/icn membership).
pub fn is_cycle_route_relation(r: &Relation) -> bool {
    tags_are_cycle_route(r.tags())
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
    let cycleway_opp = ["cycleway", "cycleway:left", "cycleway:right"]
        .iter()
        .any(|k| {
            matches!(
                tag(w, k),
                Some("opposite" | "opposite_lane" | "opposite_track")
            )
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
/// directions so oneway handling is direction-correct. `in_cycle_route` is true
/// when the way belongs to a bicycle route relation (see
/// [`is_cycle_route_relation`]); it's OR-ed with any way-level route tags.
pub fn classify(w: &Way, forward: bool, in_cycle_route: bool) -> BikeAttrs {
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
        cycleroute: in_cycle_route || any_cycleroute(w),
        bikeaccess,
        footaccess: foot_access(w, bikeaccess),
        wrong_way: wrong_way(w, forward),
    }
}

#[cfg(test)]
mod tests {
    use super::tags_are_cycle_route;

    fn check(tags: &[(&str, &str)]) -> bool {
        tags_are_cycle_route(tags.iter().copied())
    }

    #[test]
    fn detects_bicycle_route_relation() {
        assert!(check(&[
            ("type", "route"),
            ("route", "bicycle"),
            ("network", "rcn"),
            ("name", "Knooppuntnetwerk"),
        ]));
        assert!(check(&[("type", "route"), ("route", "mtb")]));
        assert!(check(&[("type", "superroute"), ("route", "bicycle")]));
    }

    #[test]
    fn rejects_non_bicycle_routes() {
        assert!(!check(&[("type", "route"), ("route", "hiking")]));
        assert!(!check(&[("type", "route"), ("route", "bus")]));
        // A bicycle restriction relation, not a route.
        assert!(!check(&[("type", "restriction"), ("route", "bicycle")]));
        // Missing the route value entirely.
        assert!(!check(&[("type", "route")]));
        assert!(!check(&[]));
    }
}
