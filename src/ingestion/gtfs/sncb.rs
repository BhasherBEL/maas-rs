use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Reverse;

use kdtree::{KdTree, distance::squared_euclidean};
use osmpbf::{Element, ElementReader};

use crate::structures::{Graph, LatLng, NodeID};
use super::load_gtfs_with_hook;

// ── Railway way filter ────────────────────────────────────────────────────────

fn is_railway_way(tags: &[(&str, &str)]) -> bool {
    let railway = tags.iter().find(|t| t.0 == "railway").map(|t| t.1);
    if !matches!(railway, Some("rail" | "light_rail" | "narrow_gauge")) {
        return false;
    }
    let service = tags.iter().find(|t| t.0 == "service").map(|t| t.1);
    !matches!(service, Some("yard"))
}

/// Maximum distance (metres) a SNCB stop may be from any railway node.
/// Stops beyond this threshold are not snapped — affected segments fall back to
/// straight lines rather than routing through an unrelated railway nearby.
const MAX_SNAP_DIST_M: f64 = 2_000.0;

/// How many nearest railway nodes to try when routing a single segment.
/// Trying a small set of candidates handles the common case where the single
/// nearest node sits on a dead-end platform track or an isolated siding.
const SNAP_CANDIDATES: usize = 3;

// ── RailwayGraph ──────────────────────────────────────────────────────────────

struct RailwayNode {
    lat: f64,
    lon: f64,
}

struct RailwayGraph {
    nodes: Vec<RailwayNode>,
    adj: Vec<Vec<(usize, u32)>>,
    tree: KdTree<f64, usize, [f64; 2]>,
}

impl RailwayGraph {
    fn from_raw(node_coords: Vec<(f64, f64)>, adj: Vec<Vec<(usize, u32)>>) -> Self {
        let nodes: Vec<RailwayNode> = node_coords
            .into_iter()
            .map(|(lat, lon)| RailwayNode { lat, lon })
            .collect();
        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, n) in nodes.iter().enumerate() {
            let _ = tree.add([n.lat, n.lon], i);
        }
        RailwayGraph { nodes, adj, tree }
    }

    fn build(osm_path: &str) -> Result<Self, osmpbf::Error> {
        // Pass 1: collect all node IDs referenced by railway ways
        let mut valid_ids: HashSet<i64> = HashSet::new();
        ElementReader::from_path(osm_path)?.for_each(|el| {
            if let Element::Way(w) = el {
                let tags: Vec<(&str, &str)> = w.tags().collect();
                if is_railway_way(&tags) {
                    valid_ids.extend(w.refs());
                }
            }
        })?;

        // Pass 2: load their coordinates
        let mut nodes: Vec<RailwayNode> = Vec::new();
        let mut id_map: HashMap<i64, usize> = HashMap::new();
        ElementReader::from_path(osm_path)?.for_each(|el| {
            let (id, lat, lon) = match el {
                Element::DenseNode(n) if valid_ids.contains(&n.id()) => (n.id(), n.lat(), n.lon()),
                Element::Node(n) if valid_ids.contains(&n.id()) => (n.id(), n.lat(), n.lon()),
                _ => return,
            };
            id_map.insert(id, nodes.len());
            nodes.push(RailwayNode { lat, lon });
        })?;

        // Pass 3: build adjacency list
        let mut adj: Vec<Vec<(usize, u32)>> = vec![Vec::new(); nodes.len()];
        ElementReader::from_path(osm_path)?.for_each(|el| {
            if let Element::Way(w) = el {
                let tags: Vec<(&str, &str)> = w.tags().collect();
                if !is_railway_way(&tags) {
                    return;
                }
                let ids: Vec<i64> = w.refs().collect();
                for pair in ids.windows(2) {
                    let (a, b) = match (id_map.get(&pair[0]), id_map.get(&pair[1])) {
                        (Some(&a), Some(&b)) => (a, b),
                        _ => continue,
                    };
                    let d = haversine_m(nodes[a].lat, nodes[a].lon, nodes[b].lat, nodes[b].lon);
                    adj[a].push((b, d as u32));
                    adj[b].push((a, d as u32));
                }
            }
        })?;

        // KD-tree for nearest-node lookup
        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, n) in nodes.iter().enumerate() {
            let _ = tree.add([n.lat, n.lon], i);
        }

        tracing::info!(
            "railway graph built: {} nodes, {} edges",
            nodes.len(),
            adj.iter().map(|v| v.len()).sum::<usize>()
        );
        Ok(RailwayGraph { nodes, adj, tree })
    }

    /// Returns up to `SNAP_CANDIDATES` railway node indices closest to `(lat, lon)`,
    /// filtered to those within `MAX_SNAP_DIST_M`. Closest first.
    fn nearest_nodes(&self, lat: f64, lon: f64) -> Vec<usize> {
        let query = [lat, lon];
        let Ok(iter) = self.tree.iter_nearest(&query, &squared_euclidean) else {
            return Vec::new();
        };
        iter.take(SNAP_CANDIDATES)
            .filter_map(|(_, &idx)| {
                let n = &self.nodes[idx];
                if haversine_m(lat, lon, n.lat, n.lon) <= MAX_SNAP_DIST_M {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Dijkstra from `from` to `to`; returns ordered `Vec<LatLng>` including
    /// both endpoints, or `None` if unreachable.
    fn dijkstra(&self, from: usize, to: usize) -> Option<Vec<LatLng>> {
        if from == to {
            return Some(vec![LatLng {
                latitude: self.nodes[from].lat,
                longitude: self.nodes[from].lon,
            }]);
        }
        let n = self.nodes.len();
        let mut dist = vec![u32::MAX; n];
        let mut prev: Vec<Option<usize>> = vec![None; n];
        let mut heap: BinaryHeap<(Reverse<u32>, usize)> = BinaryHeap::new();
        dist[from] = 0;
        heap.push((Reverse(0), from));

        while let Some((Reverse(d), u)) = heap.pop() {
            if d > dist[u] {
                continue;
            }
            if u == to {
                break;
            }
            for &(v, w) in &self.adj[u] {
                let nd = d.saturating_add(w);
                if nd < dist[v] {
                    dist[v] = nd;
                    prev[v] = Some(u);
                    heap.push((Reverse(nd), v));
                }
            }
        }

        if dist[to] == u32::MAX {
            return None;
        }

        let mut path = Vec::new();
        let mut cur = to;
        loop {
            path.push(LatLng {
                latitude: self.nodes[cur].lat,
                longitude: self.nodes[cur].lon,
            });
            match prev[cur] {
                Some(p) => cur = p,
                None => break,
            }
        }
        path.reverse();
        Some(path)
    }
}

fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    6_371_000.0 * 2.0 * a.sqrt().asin()
}

// ── Shape synthesis ───────────────────────────────────────────────────────────

/// Try to route between the best candidate pair from `from_candidates` ×
/// `to_candidates`. Returns the first Dijkstra path that succeeds, or `None`
/// if every combination is unreachable.
///
/// Trying multiple candidates handles the common case where the single nearest
/// node sits on an isolated platform siding rather than the through track.
fn best_dijkstra(
    railway: &RailwayGraph,
    from_candidates: &[usize],
    to_candidates: &[usize],
) -> Option<Vec<LatLng>> {
    for &a in from_candidates {
        for &b in to_candidates {
            if let Some(path) = railway.dijkstra(a, b) {
                return Some(path);
            }
        }
    }
    None
}

/// Returns `Some((pts, stop_idx, had_fallback))` or `None` if no segment could
/// be routed at all. `had_fallback` is `true` when at least one segment used a
/// straight-line fallback instead of an on-track path.
fn compute_railway_shape(
    stops: &[NodeID],
    g: &Graph,
    railway: &RailwayGraph,
) -> Option<(Vec<LatLng>, Vec<u32>, bool)> {
    if stops.len() < 2 {
        return None;
    }

    // Resolve stop coordinates and snap each to its nearest railway candidates.
    let stop_coords: Vec<LatLng> = stops
        .iter()
        .map(|&s| g.get_node(s).map(|n| n.loc()))
        .collect::<Option<Vec<_>>>()?;

    let candidates: Vec<Vec<usize>> = stop_coords
        .iter()
        .map(|c| railway.nearest_nodes(c.latitude, c.longitude))
        .collect();

    let mut all_pts: Vec<LatLng> = Vec::new();
    let mut stop_idx: Vec<u32> = Vec::new();
    let mut routed_segments = 0usize;
    let mut fallback_segments = 0usize;

    // Seed with the first stop's geographic position. If a segment is
    // successfully routed, the seeded point will be corrected to the
    // track-snapped position (see below).
    all_pts.push(stop_coords[0]);
    stop_idx.push(0);

    for i in 1..stops.len() {
        let path = best_dijkstra(railway, &candidates[i - 1], &candidates[i]);

        match path {
            Some(p) if p.len() > 1 => {
                // p[0] is the snapped start of this segment. Overwrite the
                // previously-pushed point so it snaps to the track rather than
                // the raw stop coordinate (no-op when the previous segment was
                // also routed, since they share the same junction node).
                *all_pts.last_mut().unwrap() = p[0];
                all_pts.extend_from_slice(&p[1..]);
                routed_segments += 1;
            }
            _ => {
                // Segment unreachable — straight line to the stop coordinate.
                // The rest of the pattern continues unaffected.
                all_pts.push(stop_coords[i]);
                fallback_segments += 1;
            }
        }
        stop_idx.push((all_pts.len() - 1) as u32);
    }

    if routed_segments == 0 {
        // Every segment fell back — no improvement over the default empty shape.
        return None;
    }

    // Return whether any segment had to fall back, so the caller can aggregate.
    Some((all_pts, stop_idx, fallback_segments > 0))
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Called during the OSM snapshot phase to cache railway topology into the graph.
/// The stored data is serialized into osm.bin and reused by `load_gtfs_sncb`
/// during --update-gtfs, avoiding a re-parse of the OSM PBF.
pub fn prepare_sncb(osm_path: &str, g: &mut Graph) -> Result<(), osmpbf::Error> {
    tracing::info!("caching railway graph from {osm_path}...");
    let railway = RailwayGraph::build(osm_path)?;
    let nodes: Vec<(f64, f64)> = railway.nodes.iter().map(|n| (n.lat, n.lon)).collect();
    let node_count = nodes.len();
    g.store_railway_graph(nodes, railway.adj);
    tracing::info!("railway graph cached ({node_count} nodes)");
    Ok(())
}

pub fn load_gtfs_sncb(
    gtfs_path: &str,
    osm_path: &str,
    g: &mut Graph,
) -> Result<(), gtfs_structures::Error> {
    let railway = if let Some((nodes, adj)) = g.get_railway_graph_data() {
        tracing::info!("using cached railway graph ({} nodes)", nodes.len());
        RailwayGraph::from_raw(nodes, adj)
    } else {
        tracing::info!("building railway graph from {osm_path}...");
        match RailwayGraph::build(osm_path) {
            Ok(rg) => rg,
            Err(e) => {
                tracing::error!("failed to build railway graph: {e} — falling back to generic GTFS load");
                return load_gtfs_with_hook(gtfs_path, g, |_, _| None);
            }
        }
    };

    let patterns_before = g.transit_pattern_count();
    load_gtfs_with_hook(gtfs_path, g, |_, _| None)?;
    let patterns_after = g.transit_pattern_count();

    let mut n_computed = 0usize;
    let mut n_partial = 0usize;
    let mut n_failed = 0usize;
    for p in patterns_before..patterns_after {
        if g.get_pattern_shape(p).is_some() {
            n_computed += 1;
            continue; // pattern already had a GTFS shape
        }
        let stops = g.get_pattern_stop_nodes(p).to_vec();
        match compute_railway_shape(&stops, g, &railway) {
            Some((pts, idx, had_fallback)) => {
                g.set_pattern_shape(p, pts, idx);
                n_computed += 1;
                if had_fallback { n_partial += 1; }
            }
            None => n_failed += 1,
        }
    }
    tracing::info!("{n_computed} shapes synthesised, {n_failed} fully unmapped");
    if n_partial > 0 {
        tracing::warn!(
            "{n_partial} patterns partially mapped — some segments fell back to straight lines"
        );
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a RailwayGraph where consecutive nodes are connected in a chain.
    fn make_chain(coords: &[(f64, f64)]) -> RailwayGraph {
        let nodes: Vec<RailwayNode> = coords
            .iter()
            .map(|&(lat, lon)| RailwayNode { lat, lon })
            .collect();
        let n = nodes.len();
        let mut adj = vec![Vec::new(); n];
        for i in 0..n.saturating_sub(1) {
            let d = haversine_m(nodes[i].lat, nodes[i].lon, nodes[i + 1].lat, nodes[i + 1].lon);
            adj[i].push((i + 1, d as u32));
            adj[i + 1].push((i, d as u32));
        }
        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, nd) in nodes.iter().enumerate() {
            let _ = tree.add([nd.lat, nd.lon], i);
        }
        RailwayGraph { nodes, adj, tree }
    }

    // ── is_railway_way ────────────────────────────────────────────────────────

    #[test]
    fn test_is_railway_way_accepts_rail() {
        assert!(is_railway_way(&[("railway", "rail")]));
        assert!(is_railway_way(&[("railway", "light_rail")]));
        assert!(is_railway_way(&[("railway", "narrow_gauge")]));
    }

    #[test]
    fn test_is_railway_way_rejects_yard() {
        assert!(!is_railway_way(&[("railway", "rail"), ("service", "yard")]));
    }

    #[test]
    fn test_is_railway_way_rejects_non_rail() {
        assert!(!is_railway_way(&[("highway", "primary")]));
        assert!(!is_railway_way(&[("railway", "subway")]));
        assert!(!is_railway_way(&[]));
    }

    // ── nearest_nodes / distance cap ─────────────────────────────────────────

    #[test]
    fn test_nearest_nodes_returns_closest() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        let result = rg.nearest_nodes(50.001, 4.0);
        // Node 1 is exactly at (50.001, 4.0) — should come first
        assert_eq!(result[0], 1);
    }

    #[test]
    fn test_nearest_nodes_distance_cap() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0)]);
        // 200 km away — nothing within MAX_SNAP_DIST_M
        let result = rg.nearest_nodes(52.0, 6.0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_nearest_nodes_returns_multiple_candidates() {
        let rg = make_chain(&[(50.0, 4.0), (50.0001, 4.0), (50.0002, 4.0)]);
        // All three nodes are within 2 km; we should get up to SNAP_CANDIDATES
        let result = rg.nearest_nodes(50.0001, 4.0);
        assert!(result.len() <= SNAP_CANDIDATES);
        assert!(!result.is_empty());
    }

    // ── Dijkstra ─────────────────────────────────────────────────────────────

    #[test]
    fn test_dijkstra_linear() {
        let rg = make_chain(&[(50.0, 4.0), (50.0001, 4.0), (50.0002, 4.0)]);
        let path = rg.dijkstra(0, 2).expect("should be reachable");
        assert_eq!(path.len(), 3);
        assert!((path[0].latitude - 50.0).abs() < 1e-9);
        assert!((path[2].latitude - 50.0002).abs() < 1e-9);
    }

    #[test]
    fn test_dijkstra_same_node() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0)]);
        let path = rg.dijkstra(1, 1).expect("same node returns single point");
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn test_dijkstra_unreachable() {
        // Two isolated nodes (no edges between them)
        let nodes = vec![
            RailwayNode { lat: 50.0, lon: 4.0 },
            RailwayNode { lat: 51.0, lon: 5.0 },
        ];
        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        let _ = tree.add([50.0, 4.0], 0usize);
        let _ = tree.add([51.0, 5.0], 1usize);
        let rg = RailwayGraph {
            nodes,
            adj: vec![Vec::new(), Vec::new()],
            tree,
        };
        assert!(rg.dijkstra(0, 1).is_none());
    }

    // ── best_dijkstra ─────────────────────────────────────────────────────────

    #[test]
    fn test_best_dijkstra_falls_back_to_second_candidate() {
        // Graph: nodes 0-1-2 connected in a chain. Node 3 is isolated.
        // If first candidate is the isolated node 3, routing should still
        // succeed using the second candidate (node 0).
        let mut rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        // Add an isolated node 3
        rg.nodes.push(RailwayNode { lat: 49.0, lon: 3.0 });
        rg.adj.push(Vec::new());
        let _ = rg.tree.add([49.0, 3.0], 3usize);

        // from_candidates: [3 (isolated), 0 (connected)]
        // to_candidates:   [2]
        let path = best_dijkstra(&rg, &[3, 0], &[2]);
        assert!(path.is_some(), "should succeed via second candidate");
        assert_eq!(path.unwrap().last().unwrap().latitude, rg.nodes[2].lat);
    }

    // ── stop_idx correctness ─────────────────────────────────────────────────

    #[test]
    fn test_stop_indices_all_routed() {
        // 5-node chain: 0-1-2-3-4.
        // 3 stops snapping to nodes 0, 2, 4 → stop_idx = [0, 2, 4], 5 total pts.
        let rg = make_chain(&[
            (50.00, 4.0),
            (50.01, 4.0),
            (50.02, 4.0),
            (50.03, 4.0),
            (50.04, 4.0),
        ]);
        let candidates = [vec![0usize], vec![2usize], vec![4usize]];
        let mut all_pts: Vec<LatLng> = Vec::new();
        let mut stop_idx: Vec<u32> = Vec::new();
        all_pts.push(LatLng { latitude: rg.nodes[0].lat, longitude: rg.nodes[0].lon });
        stop_idx.push(0);
        for i in 1..candidates.len() {
            let path = best_dijkstra(&rg, &candidates[i - 1], &candidates[i]).unwrap();
            *all_pts.last_mut().unwrap() = path[0];
            all_pts.extend_from_slice(&path[1..]);
            stop_idx.push((all_pts.len() - 1) as u32);
        }
        assert_eq!(stop_idx, vec![0, 2, 4]);
        assert_eq!(all_pts.len(), 5);
    }

    #[test]
    fn test_stop_indices_partial_fallback() {
        // 3-node chain: 0-1-2.  Middle segment (stop 1 → stop 2) is unreachable
        // because stop 2 has no candidates.  Stop 0→1 should be routed, stop 1→2
        // falls back to a straight line (1 extra point pushed).
        // Expected: stop_idx = [0, 2, 3], total 4 pts.
        let rg = make_chain(&[(50.0, 4.0), (50.01, 4.0), (50.02, 4.0)]);

        // Stop 2 has no valid candidates — simulates "no railway nearby"
        let candidates: &[&[usize]] = &[&[0], &[2], &[]];

        let stop_coords = vec![
            LatLng { latitude: 50.0,  longitude: 4.0 },
            LatLng { latitude: 50.01, longitude: 4.0 },
            LatLng { latitude: 50.02, longitude: 4.0 },
        ];

        let mut all_pts: Vec<LatLng> = Vec::new();
        let mut stop_idx: Vec<u32> = Vec::new();
        let mut routed = 0usize;
        all_pts.push(stop_coords[0]);
        stop_idx.push(0);

        for i in 1..candidates.len() {
            let path = best_dijkstra(&rg, candidates[i - 1], candidates[i]);
            match path {
                Some(p) if p.len() > 1 => {
                    *all_pts.last_mut().unwrap() = p[0];
                    all_pts.extend_from_slice(&p[1..]);
                    routed += 1;
                }
                _ => {
                    all_pts.push(stop_coords[i]);
                }
            }
            stop_idx.push((all_pts.len() - 1) as u32);
        }

        assert_eq!(routed, 1, "first segment should be routed");
        assert_eq!(stop_idx[0], 0);
        assert_eq!(stop_idx[1], 2);   // routed 0→2 via nodes 0,1,2
        assert_eq!(stop_idx[2], 3);   // fallback: one extra straight-line point
        assert_eq!(all_pts.len(), 4);
    }
}
