use rstar::{AABB, PointDistance, RTree, RTreeObject};

use crate::structures::StreetEdgeData;

/// A street edge projected into a local equirectangular plane (meters), carrying
/// the source `StreetEdgeData` so the caller can apply a mode predicate and
/// reconstruct an `Endpoint`. Distances are perpendicular point-to-segment.
#[derive(Clone, Debug)]
pub struct EdgeEnvelope {
    pub edge: StreetEdgeData,
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
}

impl RTreeObject for EdgeEnvelope {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners([self.ax, self.ay], [self.bx, self.by])
    }
}

impl PointDistance for EdgeEnvelope {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let (px, py) = (point[0], point[1]);
        let (dx, dy) = (self.bx - self.ax, self.by - self.ay);
        let len2 = dx * dx + dy * dy;
        let t = if len2 == 0.0 {
            0.0
        } else {
            (((px - self.ax) * dx + (py - self.ay) * dy) / len2).clamp(0.0, 1.0)
        };
        let (cx, cy) = (self.ax + t * dx, self.ay + t * dy);
        let (ex, ey) = (px - cx, py - cy);
        ex * ex + ey * ey
    }
}

/// Bulk-loaded R*-tree over street-edge bodies for nearest-edge snapping. Built
/// on load/build from the graph's nodes+edges, never serialized.
#[derive(Default, Debug)]
pub struct EdgeIndex {
    tree: RTree<EdgeEnvelope>,
    m_lat: f64,
    m_lon: f64,
}

impl EdgeIndex {
    /// Build from `(StreetEdgeData, a_latlon, b_latlon)` triples, projecting into a
    /// plane centred on `ref_lat`. The same projection must be used at query time.
    pub fn build(
        edges: impl Iterator<Item = (StreetEdgeData, (f64, f64), (f64, f64))>,
        ref_lat: f64,
    ) -> Self {
        let m_lat = 111_320.0_f64;
        let m_lon = 111_320.0_f64 * ref_lat.to_radians().cos();
        let items: Vec<EdgeEnvelope> = edges
            .map(|(edge, (alat, alon), (blat, blon))| EdgeEnvelope {
                edge,
                ax: alon * m_lon,
                ay: alat * m_lat,
                bx: blon * m_lon,
                by: blat * m_lat,
            })
            .collect();
        EdgeIndex {
            tree: RTree::bulk_load(items),
            m_lat,
            m_lon,
        }
    }

    /// Nearest `usable` edge to `(lat, lon)` by perpendicular body distance, within
    /// `radius_m`. Iterates candidates in increasing body distance and stops once
    /// the nearest remaining one is beyond `radius_m`. Returns the edge and its
    /// perpendicular distance in meters, or `None` if none qualifies.
    pub fn nearest_usable(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        usable: impl Fn(&StreetEdgeData) -> bool,
    ) -> Option<(StreetEdgeData, f64)> {
        let q = [lon * self.m_lon, lat * self.m_lat];
        let r2 = radius_m * radius_m;
        for (e, d2) in self.tree.nearest_neighbor_iter_with_distance_2(q) {
            if d2 > r2 {
                break;
            }
            if usable(&e.edge) {
                return Some((e.edge, d2.sqrt()));
            }
        }
        None
    }
}
