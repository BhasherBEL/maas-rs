//! A railway segment with BOTH endpoints in the same non-`None` agglomeration zone
//! contributes 0 chargeable km (spec Appendix A.2). Zones are config-driven polygons.

use serde::{Deserialize, Serialize};

use crate::structures::LatLng;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Agglomeration {
    #[default]
    None,
    Brussels,
    Antwerpen,
}

#[derive(Clone, Debug)]
pub struct AgglomerationZone {
    pub zone: Agglomeration,
    /// Implicitly-closed ring; needs >= 3 vertices to enclose any area.
    pub polygon: Vec<LatLng>,
    /// Reference node for zone-to-zone fare distance (not the actual stops).
    /// Case-insensitive substring; `None`/unmatched ⇒ centroid's nearest railway node.
    pub reference: Option<String>,
}

impl AgglomerationZone {
    pub fn contains(&self, p: LatLng) -> bool {
        point_in_polygon(p, &self.polygon)
    }
}

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
        assert!(z.contains(LatLng { latitude: 50.836, longitude: 4.336 }));
    }

    #[test]
    fn point_outside_box_is_not_contained() {
        let z = brussels_box();
        assert!(!z.contains(LatLng { latitude: 51.23, longitude: 2.93 }));
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
        assert_eq!(
            zone_of(LatLng { latitude: 50.85, longitude: 4.35 }, &zones),
            Agglomeration::Brussels
        );
        assert_eq!(
            zone_of(LatLng { latitude: 51.22, longitude: 4.42 }, &zones),
            Agglomeration::Antwerpen
        );
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
        let ring = vec![
            LatLng { latitude: 0.0, longitude: 0.0 },
            LatLng { latitude: 0.0, longitude: 2.0 },
            LatLng { latitude: 1.0, longitude: 2.0 },
            LatLng { latitude: 1.0, longitude: 1.0 },
            LatLng { latitude: 2.0, longitude: 1.0 },
            LatLng { latitude: 2.0, longitude: 0.0 },
        ];
        assert!(!point_in_polygon(LatLng { latitude: 1.5, longitude: 1.5 }, &ring));
        assert!(point_in_polygon(LatLng { latitude: 0.5, longitude: 0.5 }, &ring));
    }
}
