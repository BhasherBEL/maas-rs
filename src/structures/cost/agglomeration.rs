//! SNCB agglomeration flat-zones (spec Appendix A.2, §5).
//!
//! SNCB's fare is `base + per_km * km`, where `km` counts ONLY railway distance
//! OUTSIDE the flat agglomeration zones. Brussels and Antwerpen are EACH a single
//! flat zone: travelling within a zone, and the choice of destination stop within
//! a zone, add NO per-km. The cleanest model is to collapse each agglomeration's
//! SNCB stops to ONE fare node, so any inter-stop railway segment with BOTH
//! endpoints in the same agglomeration contributes 0 chargeable km. Applied
//! pairwise along a pattern's cumulative railway distance
//! (`Graph::rebuild_sncb_railway_km`), this yields:
//!   - Brussels -> Brussels = base only (every in-Brussels segment is 0 km);
//!   - Ostende -> (any Brussels station) = identical (the in-Brussels tail is 0);
//!   - Ostende -> Liege = full km (no segment has both endpoints in one zone).
//!
//! Zone geometry source: the polygons are config-driven bounding polygons per
//! agglomeration (`config.yaml` `default_routing.fares.agglomerations`), derived
//! from the OSM administrative boundaries (Brussels-Capital Region admin_level 4;
//! City of Antwerp municipality admin_level 8 as a documented approximation of the
//! SNCB Antwerp fare zone). Config-driven so nothing is hardcoded in Rust logic
//! and so the tags are a `#[serde(skip)]` lookup recomputed on load from the
//! stored polygons (no graph.bin schema bump). See the module note in
//! `raptor_index.rs::sncb_stop_zone`. [ASSUMPTION: a bounding polygon is a
//! documented approximation of the exact admin boundary; PREFER refining to the
//! real assembled OSM multipolygon rings later — see the TODO in `railway.rs`.]

use serde::{Deserialize, Serialize};

use crate::structures::LatLng;

/// Which SNCB flat agglomeration a stop belongs to (spec Appendix A.2). `None`
/// means the stop is charged normal per-km. Two stops in the SAME non-`None`
/// zone have zero chargeable railway distance between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Agglomeration {
    /// Not inside any flat agglomeration zone (normal per-km).
    #[default]
    None,
    /// Brussels-Capital Region flat zone.
    Brussels,
    /// Antwerpen agglomeration flat zone.
    Antwerpen,
}

/// One agglomeration flat zone: an identity plus a bounding polygon (a ring of
/// WGS84 vertices, implicitly closed). Config-driven; compiled into `FareModel`.
/// A stop is "in" the zone iff its coordinate is inside the polygon (ray-cast).
#[derive(Clone, Debug)]
pub struct AgglomerationZone {
    /// Which flat zone this polygon defines.
    pub zone: Agglomeration,
    /// The bounding polygon as `(lat, lng)` vertices in order, implicitly closed
    /// (the last vertex connects back to the first). Must have >= 3 vertices to
    /// enclose any area; a shorter ring contains no point.
    pub polygon: Vec<LatLng>,
    /// Canonical central-station name for this agglomeration's fare REFERENCE node
    /// (spec Appendix A.2 zone collapse): e.g. "Bruxelles-Central"/"Brussel-Centraal"
    /// for Brussels, "Antwerpen-Centraal" for Antwerpen. The zone-to-zone fare
    /// distance is measured between reference nodes, not between the actual boarding
    /// / alighting stops, so any Brussels station → any Antwerpen station is one
    /// fixed fare. Matched (case-insensitive substring) against SNCB stop names to
    /// pick the reference railway node; when `None` or unmatched, the polygon
    /// centroid's nearest railway node is used instead. Config-driven.
    pub reference: Option<String>,
}

impl AgglomerationZone {
    /// True iff `p` lies inside this zone's polygon, by the even-odd ray-casting
    /// rule (a horizontal ray to +lng; a point is inside iff it crosses an odd
    /// number of edges). Operates in the lat/lng plane, which is adequate for a
    /// coarse municipal bounding polygon (the same planar approximation the KD-tree
    /// snapping uses). A polygon with < 3 vertices contains nothing.
    pub fn contains(&self, p: LatLng) -> bool {
        point_in_polygon(p, &self.polygon)
    }
}

/// Even-odd ray-casting point-in-polygon test in the lat/lng plane. `ring` is an
/// implicitly-closed vertex list. Returns false for a degenerate ring (< 3
/// vertices). A point exactly on an edge is treated by the standard half-open
/// crossing rule (deterministic, no special-casing) — adequate for coarse zones.
pub fn point_in_polygon(p: LatLng, ring: &[LatLng]) -> bool {
    let n = ring.len();
    if n < 3 {
        return false;
    }
    let (x, y) = (p.longitude, p.latitude);
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = (ring[i].longitude, ring[i].latitude);
        let (xj, yj) = (ring[j].longitude, ring[j].latitude);
        // Does the edge j->i straddle the horizontal line y? If so, find the
        // longitude of the crossing and count it when it is to the right of x.
        let straddles = (yi > y) != (yj > y);
        if straddles {
            let x_cross = xi + (y - yi) / (yj - yi) * (xj - xi);
            if x < x_cross {
                inside = !inside;
            }
        }
        j = i;
    }
    inside
}

/// Resolve a stop coordinate to its agglomeration by testing the configured
/// zone polygons in order; the first containing polygon wins. `None` when no zone
/// contains it (the common case). Cheap linear scan over a handful of zones.
pub fn zone_of(p: LatLng, zones: &[AgglomerationZone]) -> Agglomeration {
    for z in zones {
        if z.contains(p) {
            return z.zone;
        }
    }
    Agglomeration::None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple square around (50.85, 4.35) ~ central Brussels, +/- 0.1 deg.
    fn brussels_box() -> AgglomerationZone {
        AgglomerationZone {
            zone: Agglomeration::Brussels,
            polygon: vec![
                LatLng { latitude: 50.75, longitude: 4.25 },
                LatLng { latitude: 50.75, longitude: 4.45 },
                LatLng { latitude: 50.95, longitude: 4.45 },
                LatLng { latitude: 50.95, longitude: 4.25 },
            ],
            reference: None,
        }
    }

    fn antwerpen_box() -> AgglomerationZone {
        AgglomerationZone {
            zone: Agglomeration::Antwerpen,
            polygon: vec![
                LatLng { latitude: 51.15, longitude: 4.35 },
                LatLng { latitude: 51.15, longitude: 4.50 },
                LatLng { latitude: 51.28, longitude: 4.50 },
                LatLng { latitude: 51.28, longitude: 4.35 },
            ],
            reference: None,
        }
    }

    #[test]
    fn point_inside_box_is_contained() {
        let z = brussels_box();
        // Bruxelles-Midi ~ (50.836, 4.336): inside.
        assert!(z.contains(LatLng { latitude: 50.836, longitude: 4.336 }));
    }

    #[test]
    fn point_outside_box_is_not_contained() {
        let z = brussels_box();
        // Ostende ~ (51.23, 2.93): well outside.
        assert!(!z.contains(LatLng { latitude: 51.23, longitude: 2.93 }));
        // Liege ~ (50.62, 5.57): outside.
        assert!(!z.contains(LatLng { latitude: 50.62, longitude: 5.57 }));
    }

    #[test]
    fn degenerate_ring_contains_nothing() {
        let z = AgglomerationZone {
            zone: Agglomeration::Brussels,
            polygon: vec![
                LatLng { latitude: 50.8, longitude: 4.3 },
                LatLng { latitude: 50.9, longitude: 4.4 },
            ],
            reference: None,
        };
        assert!(!z.contains(LatLng { latitude: 50.85, longitude: 4.35 }));
    }

    #[test]
    fn zone_of_picks_the_containing_zone() {
        let zones = vec![brussels_box(), antwerpen_box()];
        // A Brussels coord resolves to Brussels.
        assert_eq!(
            zone_of(LatLng { latitude: 50.85, longitude: 4.35 }, &zones),
            Agglomeration::Brussels
        );
        // An Antwerpen coord resolves to Antwerpen.
        assert_eq!(
            zone_of(LatLng { latitude: 51.22, longitude: 4.42 }, &zones),
            Agglomeration::Antwerpen
        );
        // A rural coord resolves to None.
        assert_eq!(
            zone_of(LatLng { latitude: 50.62, longitude: 5.57 }, &zones),
            Agglomeration::None
        );
    }

    #[test]
    fn empty_zone_list_is_none() {
        assert_eq!(
            zone_of(LatLng { latitude: 50.85, longitude: 4.35 }, &[]),
            Agglomeration::None
        );
    }

    #[test]
    fn concave_polygon_notch_is_excluded() {
        // An L-shaped polygon: a point in the notch (cut-out) must be OUTSIDE, which
        // a bounding-box test would wrongly include but ray-casting excludes.
        let ring = vec![
            LatLng { latitude: 0.0, longitude: 0.0 },
            LatLng { latitude: 0.0, longitude: 2.0 },
            LatLng { latitude: 1.0, longitude: 2.0 },
            LatLng { latitude: 1.0, longitude: 1.0 },
            LatLng { latitude: 2.0, longitude: 1.0 },
            LatLng { latitude: 2.0, longitude: 0.0 },
        ];
        // (1.5, 1.5) is in the removed upper-right notch -> outside.
        assert!(!point_in_polygon(LatLng { latitude: 1.5, longitude: 1.5 }, &ring));
        // (0.5, 0.5) is in the solid part -> inside.
        assert!(point_in_polygon(LatLng { latitude: 0.5, longitude: 0.5 }, &ring));
    }
}
