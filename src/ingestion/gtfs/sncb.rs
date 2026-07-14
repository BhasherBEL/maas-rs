use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use kdtree::{KdTree, distance::squared_euclidean};
use osmpbf::{Element, ElementReader};

use super::load_gtfs_with_hook;
use crate::structures::{Graph, LatLng, NodeID};

fn is_railway_way(tags: &[(&str, &str)]) -> bool {
    let railway = tags.iter().find(|t| t.0 == "railway").map(|t| t.1);
    if !matches!(railway, Some("rail" | "light_rail" | "narrow_gauge")) {
        return false;
    }
    let service = tags.iter().find(|t| t.0 == "service").map(|t| t.1);
    !matches!(service, Some("yard"))
}

/// Stops beyond this from any railway node are not snapped (fall back to straight lines).
const MAX_SNAP_DIST_M: f64 = 2_000.0;

const SNAP_CANDIDATES: usize = 3;

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
        let mut valid_ids: HashSet<i64> = HashSet::new();
        ElementReader::from_path(osm_path)?.for_each(|el| {
            if let Element::Way(w) = el {
                let tags: Vec<(&str, &str)> = w.tags().collect();
                if is_railway_way(&tags) {
                    valid_ids.extend(w.refs());
                }
            }
        })?;

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

fn compute_railway_shape(
    stops: &[NodeID],
    g: &Graph,
    railway: &RailwayGraph,
) -> Option<(Vec<LatLng>, Vec<u32>, bool)> {
    if stops.len() < 2 {
        return None;
    }

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

    all_pts.push(stop_coords[0]);
    stop_idx.push(0);

    for i in 1..stops.len() {
        let path = best_dijkstra(railway, &candidates[i - 1], &candidates[i]);

        match path {
            Some(p) if p.len() > 1 => {
                *all_pts.last_mut().unwrap() = p[0];
                all_pts.extend_from_slice(&p[1..]);
                routed_segments += 1;
            }
            _ => {
                all_pts.push(stop_coords[i]);
                fallback_segments += 1;
            }
        }
        stop_idx.push((all_pts.len() - 1) as u32);
    }

    if routed_segments == 0 {
        return None;
    }

    Some((all_pts, stop_idx, fallback_segments > 0))
}

pub fn prepare_sncb(osm_path: &str, g: &mut Graph) -> Result<(), osmpbf::Error> {
    tracing::info!("caching railway graph from {osm_path}...");
    let railway = RailwayGraph::build(osm_path)?;
    let nodes: Vec<(f64, f64)> = railway.nodes.iter().map(|n| (n.lat, n.lon)).collect();
    let node_count = nodes.len();
    g.store_railway_graph(nodes, railway.adj);
    tracing::info!("railway graph cached ({node_count} nodes)");
    Ok(())
}

/// SNCB permits bikes on all trains, so default to allowed when GTFS gives no info.
fn sncb_bikes_decision(explicit: gtfs_structures::BikesAllowedType) -> Option<bool> {
    use gtfs_structures::BikesAllowedType;
    match explicit {
        BikesAllowedType::AtLeastOneBike => Some(true),
        BikesAllowedType::NoBikesAllowed => Some(false),
        BikesAllowedType::NoBikeInfo | BikesAllowedType::Unknown(_) => Some(true),
    }
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
                tracing::error!(
                    "failed to build railway graph: {e} — falling back to generic GTFS load"
                );
                return load_gtfs_with_hook(
                    gtfs_path,
                    g,
                    super::GtfsProvider::Sncb,
                    |_, _| None,
                );
            }
        }
    };

    let patterns_before = g.transit_pattern_count();
    load_gtfs_with_hook(gtfs_path, g, super::GtfsProvider::Sncb, |trip, _| {
        sncb_bikes_decision(trip.bikes_allowed)
    })?;
    let patterns_after = g.transit_pattern_count();

    let mut n_computed = 0usize;
    let mut n_partial = 0usize;
    let mut n_failed = 0usize;
    for p in patterns_before..patterns_after {
        if g.get_pattern_shape(p).is_some() {
            n_computed += 1;
            continue;
        }
        let stops = g.get_pattern_stop_nodes(p).to_vec();
        match compute_railway_shape(&stops, g, &railway) {
            Some((pts, idx, had_fallback)) => {
                g.set_pattern_shape(p, pts, idx);
                n_computed += 1;
                if had_fallback {
                    n_partial += 1;
                }
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

/// Builds the SNCB `OperatorModel` from its `distance_base_per_km` config, plus the
/// uppercased airport-station tokens (an OD touching one prices at the airport fare).
pub fn build_sncb_operator(
    op: &crate::structures::FareOperatorConfig,
    cents: impl Fn(Option<f64>) -> u32,
) -> (crate::structures::cost::OperatorModel, Vec<String>) {
    use crate::structures::cost::{DistanceTariff, OperatorModel, SncbTimeRules};

    let airport_station_names: Vec<String> = op
        .airport_station_names
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .collect();

    let mut windows = [(0u32, 0u32); 2];
    let n = op.peak_windows.len().min(2);
    windows[..n].copy_from_slice(&op.peak_windows[..n]);
    let rules = SncbTimeRules {
        peak_windows: windows,
        n_peak_windows: n as u8,
        weekend_discount_adult: op.weekend_discount_adult.unwrap_or(0.0),
        weekend_discount_reduced: op.weekend_discount_reduced.unwrap_or(0.0),
        train_plus_offpeak_discount: op.train_plus_offpeak_discount.unwrap_or(0.0),
        train_plus_peak_cap_adult: op
            .train_plus_peak_cap_adult_euros
            .map(|e| cents(Some(e)))
            .unwrap_or(u32::MAX),
        train_plus_peak_cap_reduced: op
            .train_plus_peak_cap_reduced_euros
            .map(|e| cents(Some(e)))
            .unwrap_or(u32::MAX),
    };
    let min_km = op.min_km.unwrap_or(3);
    let max_km = op.max_km.unwrap_or(118);
    let floor_cents = op.floor_euros.map(|e| cents(Some(e))).unwrap_or(260);
    let mut fc_thresholds = [36u32, 51u32];
    for (i, t) in op.first_class_thresholds.iter().take(2).enumerate() {
        fc_thresholds[i] = *t;
    }
    let mut fc_coeffs = [1.40f64, 1.50, 1.60];
    for (i, c) in op.first_class_coefficients.iter().take(3).enumerate() {
        fc_coeffs[i] = *c;
    }
    let mut fc_round_thresholds = [2500u32, 5000u32];
    for (i, t) in op.first_class_round_thresholds.iter().take(2).enumerate() {
        fc_round_thresholds[i] = (*t * 100.0).round() as u32;
    }
    let mut fc_round_grids = [10u32, 50u32, 100u32];
    for (i, gr) in op.first_class_round_grids.iter().take(3).enumerate() {
        fc_round_grids[i] = (*gr * 100.0).round() as u32;
    }
    let tariff = match op.distance_tariff.as_deref() {
        Some("linear") => DistanceTariff::Linear {
            intercept_cents: op.intercept_euros.unwrap_or(0.0) * 100.0,
            slope_cents_per_km: op.slope_euros_per_km.unwrap_or(0.0) * 100.0,
            min_km,
            max_km,
            floor_cents,
        },
        Some("band") => {
            let mut thresholds = [36u32, 51u32];
            for (i, t) in op.band_thresholds.iter().take(2).enumerate() {
                thresholds[i] = *t;
            }
            let mut coeffs = [1.40f64, 1.50, 1.60];
            for (i, c) in op.band_coefficients.iter().take(3).enumerate() {
                coeffs[i] = *c;
            }
            DistanceTariff::Band {
                per_km_rate_cents: op.per_km_rate.unwrap_or(0.0) * 100.0,
                thresholds,
                coeffs,
                min_km,
                max_km,
                floor_cents,
            }
        }
        _ => DistanceTariff::Bracketed {
            a_cents_per_km: op.a_euros_per_km.unwrap_or(0.0) * 100.0,
            b_cents: op.b_euros.unwrap_or(0.0) * 100.0,
            floor_cents,
            min_km,
            cap_from_km: op.cap_from_km.unwrap_or(116),
            cap_km: op.cap_km.unwrap_or(118),
            first_class_thresholds: fc_thresholds,
            first_class_coeffs: fc_coeffs,
            first_class_round_thresholds: fc_round_thresholds,
            first_class_round_grids: fc_round_grids,
        },
    };
    let model = OperatorModel::DistanceBasePerKm {
        tariff,
        rules,
        airport_od_cents: op.airport_od_euros.map(|e| cents(Some(e))).unwrap_or(0),
    };
    (model, airport_station_names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtfs_structures::BikesAllowedType;

    #[test]
    fn sncb_allows_bikes_by_default() {
        assert_eq!(
            sncb_bikes_decision(BikesAllowedType::NoBikeInfo),
            Some(true)
        );
        assert_eq!(
            sncb_bikes_decision(BikesAllowedType::Unknown(7)),
            Some(true)
        );
    }

    #[test]
    fn sncb_respects_explicit_gtfs_bike_info() {
        assert_eq!(
            sncb_bikes_decision(BikesAllowedType::AtLeastOneBike),
            Some(true)
        );
        assert_eq!(
            sncb_bikes_decision(BikesAllowedType::NoBikesAllowed),
            Some(false)
        );
    }

    fn make_chain(coords: &[(f64, f64)]) -> RailwayGraph {
        let nodes: Vec<RailwayNode> = coords
            .iter()
            .map(|&(lat, lon)| RailwayNode { lat, lon })
            .collect();
        let n = nodes.len();
        let mut adj = vec![Vec::new(); n];
        for i in 0..n.saturating_sub(1) {
            let d = haversine_m(
                nodes[i].lat,
                nodes[i].lon,
                nodes[i + 1].lat,
                nodes[i + 1].lon,
            );
            adj[i].push((i + 1, d as u32));
            adj[i + 1].push((i, d as u32));
        }
        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, nd) in nodes.iter().enumerate() {
            let _ = tree.add([nd.lat, nd.lon], i);
        }
        RailwayGraph { nodes, adj, tree }
    }

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

    #[test]
    fn test_nearest_nodes_returns_closest() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        let result = rg.nearest_nodes(50.001, 4.0);
        assert_eq!(result[0], 1);
    }

    #[test]
    fn test_nearest_nodes_distance_cap() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0)]);
        let result = rg.nearest_nodes(52.0, 6.0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_nearest_nodes_returns_multiple_candidates() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        let result = rg.nearest_nodes(50.001, 4.0);
        assert!(result.len() <= SNAP_CANDIDATES);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_dijkstra_linear() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        let path = rg.dijkstra(0, 2).expect("should be reachable");
        assert_eq!(path.len(), 3);
        assert!((path[0].latitude - 50.0).abs() < 1e-9);
        assert!((path[2].latitude - 50.002).abs() < 1e-9);
    }

    #[test]
    fn test_dijkstra_same_node() {
        let rg = make_chain(&[(50.0, 4.0), (50.001, 4.0)]);
        let path = rg.dijkstra(1, 1).expect("same node returns single point");
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn test_dijkstra_unreachable() {
        let nodes = vec![
            RailwayNode {
                lat: 50.0,
                lon: 4.0,
            },
            RailwayNode {
                lat: 51.0,
                lon: 5.0,
            },
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

    #[test]
    fn test_best_dijkstra_falls_back_to_second_candidate() {
        let mut rg = make_chain(&[(50.0, 4.0), (50.001, 4.0), (50.002, 4.0)]);
        rg.nodes.push(RailwayNode {
            lat: 49.0,
            lon: 3.0,
        });
        rg.adj.push(Vec::new());
        let _ = rg.tree.add([49.0, 3.0], 3usize);

        let path = best_dijkstra(&rg, &[3, 0], &[2]);
        assert!(path.is_some(), "should succeed via second candidate");
        assert_eq!(path.unwrap().last().unwrap().latitude, rg.nodes[2].lat);
    }

    #[test]
    fn test_stop_indices_all_routed() {
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
        all_pts.push(LatLng {
            latitude: rg.nodes[0].lat,
            longitude: rg.nodes[0].lon,
        });
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
        let rg = make_chain(&[(50.0, 4.0), (50.01, 4.0), (50.02, 4.0)]);

        let candidates: &[&[usize]] = &[&[0], &[2], &[]];

        let stop_coords = [
            LatLng {
                latitude: 50.0,
                longitude: 4.0,
            },
            LatLng {
                latitude: 50.01,
                longitude: 4.0,
            },
            LatLng {
                latitude: 50.02,
                longitude: 4.0,
            },
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
        assert_eq!(stop_idx[1], 2);
        assert_eq!(stop_idx[2], 3);
        assert_eq!(all_pts.len(), 4);
    }
}
