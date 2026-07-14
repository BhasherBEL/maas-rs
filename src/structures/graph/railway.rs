use std::cmp::Reverse;
use std::collections::BinaryHeap;

use kdtree::{KdTree, distance::squared_euclidean};

use super::Graph;
use crate::structures::LatLng;
use crate::structures::cost::{Agglomeration, OperatorFareId, OperatorModel, zone_of};

const MAX_SNAP_DIST_M: f64 = 2_000.0;
const SNAP_CANDIDATES: usize = 3;

impl Graph {
    pub fn store_railway_graph(&mut self, nodes: Vec<(f64, f64)>, adj: Vec<Vec<(usize, u32)>>) {
        self.raptor.railway_nodes = nodes;
        self.raptor.railway_adj = adj;
    }

    pub fn rebuild_sncb_stop_zones(&mut self) {
        self.raptor.sncb_stop_zone = Vec::new();
        if !self.raptor.fare_model.enabled || self.raptor.fare_model.agglomerations.is_empty() {
            return;
        }
        let zones = self.raptor.fare_model.agglomerations.clone();
        let n_stops = self.raptor.transit_stop_to_node.len();
        let mut tags = vec![Agglomeration::None; n_stops];
        let mut counts = [0usize; 2];
        for stop in 0..n_stops {
            if let Some(coord) = self.stop_lat_lng(stop) {
                let z = zone_of(coord, &zones);
                match z {
                    Agglomeration::Brussels => counts[0] += 1,
                    Agglomeration::Antwerpen => counts[1] += 1,
                    Agglomeration::None => {}
                }
                tags[stop] = z;
            }
        }
        tracing::info!(
            "SNCB flat-zones: tagged {} stops BRUSSELS, {} stops ANTWERPEN ({} zones)",
            counts[0],
            counts[1],
            zones.len(),
        );
        self.raptor.sncb_stop_zone = tags;
    }

    pub fn rebuild_sncb_airport_stops(&mut self) {
        self.raptor.sncb_airport_stop = Vec::new();
        if !self.raptor.fare_model.enabled {
            return;
        }
        let mut tokens: Vec<String> = Vec::new();
        for op in &self.raptor.fare_model.operators {
            if let OperatorModel::DistanceBasePerKm { airport_od_cents, .. } = op.model
                && airport_od_cents > 0
            {
                tokens.extend(op.airport_station_names.iter().cloned());
            }
        }
        if tokens.is_empty() {
            return;
        }
        let n_stops = self.raptor.transit_stop_to_node.len();
        let mut tags = vec![false; n_stops];
        let mut count = 0usize;
        for (stop, tag) in tags.iter_mut().enumerate() {
            if let Some(name) = self.raptor.transit_stop_names.get(stop) {
                let upper = name.to_ascii_uppercase();
                if tokens.iter().any(|t| upper.contains(t.as_str())) {
                    *tag = true;
                    count += 1;
                }
            }
        }
        tracing::info!(
            "SNCB airport special-OD: tagged {count} stops (tokens: {})",
            tokens.join(", ")
        );
        self.raptor.sncb_airport_stop = tags;
    }

    /// `sncb_pattern_cum_railway_m` holds the UN-COLLAPSED full railway distance;
    /// zone collapse is applied downstream, so this array is read only for the
    /// non-zone → non-zone case.
    pub fn rebuild_sncb_railway_km(&mut self) {
        self.raptor.sncb_pattern_cum_railway_m = Vec::new();
        if !self.raptor.fare_model.enabled {
            return;
        }
        let is_sncb_pattern: Vec<bool> = self
            .raptor
            .transit_patterns
            .iter()
            .map(|p| {
                matches!(
                    self.raptor.operator_fare_of_route.get(p.route.0 as usize),
                    Some(OperatorFareId::Modeled {
                        model: OperatorModel::DistanceBasePerKm { .. }
                    })
                )
            })
            .collect();
        if !is_sncb_pattern.iter().any(|&b| b) {
            return;
        }
        if self.raptor.railway_nodes.is_empty() {
            tracing::warn!(
                "SNCB fare enabled but railway topology is empty — all SNCB \
                 segments will fall back to haversine distances"
            );
        }

        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, &(lat, lon)) in self.raptor.railway_nodes.iter().enumerate() {
            let _ = tree.add([lat, lon], i);
        }

        let n_patterns = self.raptor.transit_patterns.len();
        let mut cum_per_pattern: Vec<Vec<f64>> = vec![Vec::new(); n_patterns];
        let mut fallback_segments = 0usize;

        for p in 0..n_patterns {
            if !is_sncb_pattern[p] {
                continue;
            }
            let stops = self.get_pattern_stop_nodes(p).to_vec();
            if stops.len() < 2 {
                cum_per_pattern[p] = vec![0.0; stops.len()];
                continue;
            }
            let coords: Vec<LatLng> = stops.iter().map(|&n| self.node_loc(n)).collect();
            let candidates: Vec<Vec<usize>> = coords
                .iter()
                .map(|c| nearest_railway_nodes(&tree, &self.raptor.railway_nodes, c))
                .collect();

            let mut cum = Vec::with_capacity(stops.len());
            cum.push(0.0);
            for i in 1..stops.len() {
                let seg = self
                    .rail_meters(&candidates[i - 1], &candidates[i])
                    .unwrap_or_else(|| {
                        fallback_segments += 1;
                        coords[i - 1].dist(coords[i])
                    });
                cum.push(cum[i - 1] + seg);
            }
            cum_per_pattern[p] = cum;
        }

        if fallback_segments > 0 {
            tracing::warn!(
                "SNCB railway-km precompute: {fallback_segments} inter-stop segments \
                 fell back to haversine (no rail path / unsnappable stop)"
            );
        }
        self.raptor.sncb_pattern_cum_railway_m = cum_per_pattern;
    }

    /// Run AFTER `rebuild_sncb_stop_zones`.
    pub fn rebuild_sncb_zone_refs(&mut self) {
        self.raptor.sncb_ref_to_stop = Vec::new();
        self.raptor.sncb_ref_to_ref = Vec::new();
        self.raptor.sncb_zone_ref_node = Vec::new();
        if !self.raptor.fare_model.enabled || self.raptor.fare_model.agglomerations.is_empty() {
            return;
        }
        let has_sncb = self.raptor.fare_model.operators.iter().any(|op| {
            matches!(op.model, crate::structures::cost::OperatorModel::DistanceBasePerKm { .. })
        });
        if !has_sncb {
            return;
        }
        if self.raptor.railway_nodes.is_empty() {
            tracing::warn!(
                "SNCB zone refs: fares enabled with zones but railway topology is \
                 empty — zone-to-zone distances unavailable"
            );
            return;
        }

        let mut tree: KdTree<f64, usize, [f64; 2]> = KdTree::new(2);
        for (i, &(lat, lon)) in self.raptor.railway_nodes.iter().enumerate() {
            let _ = tree.add([lat, lon], i);
        }

        // Use ALL candidates, not just the closest: a stop snapped onto a
        // disconnected platform STUB gives an INFINITE (→ 0) reference distance;
        // `ref_to_stop` takes the MIN over candidates to bypass such a stub.
        let n_stops = self.raptor.transit_stop_to_node.len();
        let stop_rail_nodes: Vec<Vec<usize>> = (0..n_stops)
            .map(|s| {
                self.stop_lat_lng(s)
                    .map(|c| nearest_railway_nodes(&tree, &self.raptor.railway_nodes, &c))
                    .unwrap_or_default()
            })
            .collect();

        let zones = self.raptor.fare_model.agglomerations.clone();
        // Per zone: Dijkstra from EACH reference candidate, min over sources, bypasses
        // a stub the reference itself snapped onto.
        let mut ref_candidates: Vec<Vec<usize>> = Vec::with_capacity(zones.len());
        let mut ref_nodes: Vec<Option<usize>> = Vec::with_capacity(zones.len());
        for z in &zones {
            // Named reference: among matching stops, prefer the SHORTEST normalized
            // name (bare parent station over a platform sub-stop), avoiding a stub.
            let mut cands: Vec<usize> = Vec::new();
            if let Some(token) = z.reference.as_ref().map(|s| normalize_station(s)) {
                let mut best_len = usize::MAX;
                for s in 0..n_stops {
                    if stop_rail_nodes[s].is_empty() {
                        continue;
                    }
                    if let Some(name) = self.raptor.transit_stop_names.get(s) {
                        let norm = normalize_station(name);
                        if norm.contains(token.as_str()) && norm.len() < best_len {
                            best_len = norm.len();
                            cands = stop_rail_nodes[s].clone();
                        }
                    }
                }
            }
            // Fallback: railway nodes nearest the polygon centroid.
            if cands.is_empty() {
                let centroid = polygon_centroid(&z.polygon);
                cands = nearest_railway_nodes(&tree, &self.raptor.railway_nodes, &centroid);
                // Centroid may sit off any track beyond MAX_SNAP_DIST_M; unconditional
                // nearest so a reference always resolves.
                if cands.is_empty()
                    && let Some(n) = unconditional_nearest(&tree, &centroid)
                {
                    cands.push(n);
                }
            }
            ref_nodes.push(cands.first().copied());
            ref_candidates.push(cands);
        }

        // Distances are MIN over source candidates AND over the target's own
        // candidates, so a stub on either side is bypassed by a through-track candidate.
        let ref_dist_rows: Vec<Vec<Vec<f64>>> = ref_candidates
            .iter()
            .map(|cands| {
                cands
                    .iter()
                    .map(|&src| rail_dijkstra_all(&self.raptor.railway_adj, src))
                    .collect()
            })
            .collect();
        let ref_to_node = |zi: usize, node: usize| -> f64 {
            ref_dist_rows[zi]
                .iter()
                .filter_map(|row| row.get(node).copied())
                .fold(f64::INFINITY, f64::min)
        };

        let mut ref_to_stop: Vec<Vec<f64>> = Vec::with_capacity(zones.len());
        for zi in 0..zones.len() {
            let row: Vec<f64> = (0..n_stops)
                .map(|s| {
                    stop_rail_nodes[s]
                        .iter()
                        .map(|&node| ref_to_node(zi, node))
                        .fold(f64::INFINITY, f64::min)
                })
                .collect();
            ref_to_stop.push(row);
        }

        let mut ref_to_ref: Vec<Vec<f64>> = Vec::with_capacity(zones.len());
        for zi in 0..zones.len() {
            let row: Vec<f64> = (0..zones.len())
                .map(|zj| {
                    if zi == zj {
                        return 0.0;
                    }
                    ref_candidates[zj]
                        .iter()
                        .map(|&node| ref_to_node(zi, node))
                        .fold(f64::INFINITY, f64::min)
                })
                .collect();
            ref_to_ref.push(row);
        }

        tracing::info!(
            "SNCB zone refs: resolved {}/{} agglomeration reference nodes",
            ref_nodes.iter().filter(|r| r.is_some()).count(),
            zones.len(),
        );
        // A zone reference reaching NO stop with finite distance collapses every
        // zone-to-station fare to base-only (the live regression). Warn, don't degrade
        // silently.
        for (zi, row) in ref_to_stop.iter().enumerate() {
            if !row.is_empty() && row.iter().all(|d| !d.is_finite()) {
                tracing::warn!(
                    "SNCB zone refs: zone {:?} reference reaches NO stop (all distances \
                     unreachable) — its zone-to-station fares collapse to base only; the \
                     reference likely resolved onto a disconnected rail component",
                    zones[zi].zone,
                );
            }
        }
        self.raptor.sncb_ref_to_stop = ref_to_stop;
        self.raptor.sncb_ref_to_ref = ref_to_ref;
        self.raptor.sncb_zone_ref_node = ref_nodes;
    }

    /// Index into `fare_model.agglomerations` (and thus into `sncb_ref_to_stop` /
    /// `sncb_ref_to_ref`) for agglomeration `z`, or `None` for `Agglomeration::None`.
    fn sncb_zone_idx(&self, z: Agglomeration) -> Option<usize> {
        if z == Agglomeration::None {
            return None;
        }
        self.raptor
            .fare_model
            .agglomerations
            .iter()
            .position(|a| a.zone == z)
    }

    /// FIXED zone-to-zone SNCB per-km fare distance (metres) for the run
    /// `board_stop`→`alight_stop`:
    ///   - both endpoints in zones     → `ref_to_ref[zb][za]`
    ///   - board in zone, alight free  → `ref_to_stop[zb][alight]`
    ///   - board free, alight in zone  → `ref_to_stop[za][board]`
    ///   - both free → full along-path `cum[alight_pos] - cum[board_pos]` (NO zeroing).
    ///
    /// `prior_free_m` is the railway distance covered by earlier rides of the run when
    /// BOTH endpoints are free (0 for a single ride); ignored for the zone cases.
    /// Returns 0 when a table entry is missing or INFINITE (unreachable ref).
    pub fn sncb_fare_distance_m(
        &self,
        board_stop: usize,
        alight_stop: usize,
        pattern: usize,
        board_pos: usize,
        alight_pos: usize,
        prior_free_m: f64,
    ) -> f64 {
        let zb = self
            .raptor
            .sncb_stop_zone
            .get(board_stop)
            .copied()
            .unwrap_or(Agglomeration::None);
        let za = self
            .raptor
            .sncb_stop_zone
            .get(alight_stop)
            .copied()
            .unwrap_or(Agglomeration::None);
        let bi = self.sncb_zone_idx(zb);
        let ai = self.sncb_zone_idx(za);
        let finite = |d: f64| if d.is_finite() { d } else { 0.0 };
        match (bi, ai) {
            (Some(zi), Some(zj)) => self
                .raptor
                .sncb_ref_to_ref
                .get(zi)
                .and_then(|r| r.get(zj))
                .copied()
                .map(finite)
                .unwrap_or(0.0),
            (Some(zi), None) => self
                .raptor
                .sncb_ref_to_stop
                .get(zi)
                .and_then(|r| r.get(alight_stop))
                .copied()
                .map(finite)
                .unwrap_or(0.0),
            (None, Some(zj)) => self
                .raptor
                .sncb_ref_to_stop
                .get(zj)
                .and_then(|r| r.get(board_stop))
                .copied()
                .map(finite)
                .unwrap_or(0.0),
            (None, None) => {
                let seg = self
                    .raptor
                    .sncb_pattern_cum_railway_m
                    .get(pattern)
                    .and_then(|cum| {
                        let a = cum.get(alight_pos)?;
                        let b = cum.get(board_pos)?;
                        Some((a - b).max(0.0))
                    })
                    .unwrap_or(0.0);
                prior_free_m.max(0.0) + seg
            }
        }
    }

    /// Railway metres between the best reachable pair from `from_candidates` ×
    /// `to_candidates`; `None` when every pair is unreachable.
    fn rail_meters(&self, from_candidates: &[usize], to_candidates: &[usize]) -> Option<f64> {
        for &a in from_candidates {
            for &b in to_candidates {
                if let Some(m) = rail_dijkstra(&self.raptor.railway_adj, a, b) {
                    return Some(m);
                }
            }
        }
        None
    }

    pub fn get_railway_graph_data(&self) -> Option<(Vec<(f64, f64)>, Vec<Vec<(usize, u32)>>)> {
        if self.raptor.railway_nodes.is_empty() {
            None
        } else {
            Some((
                self.raptor.railway_nodes.clone(),
                self.raptor.railway_adj.clone(),
            ))
        }
    }
}

/// Up to `SNAP_CANDIDATES` railway nodes nearest `coord` within `MAX_SNAP_DIST_M`,
/// closest first.
fn nearest_railway_nodes(
    tree: &KdTree<f64, usize, [f64; 2]>,
    nodes: &[(f64, f64)],
    coord: &LatLng,
) -> Vec<usize> {
    let query = [coord.latitude, coord.longitude];
    let Ok(iter) = tree.iter_nearest(&query, &squared_euclidean) else {
        return Vec::new();
    };
    iter.take(SNAP_CANDIDATES)
        .filter_map(|(_, &idx)| {
            let (lat, lon) = nodes[idx];
            let d = LatLng {
                latitude: lat,
                longitude: lon,
            }
            .dist(*coord);
            if d <= MAX_SNAP_DIST_M { Some(idx) } else { None }
        })
        .collect()
}

/// Shortest-path distance (metres) from `from` to `to` over `adj`, or `None` if
/// unreachable.
fn rail_dijkstra(adj: &[Vec<(usize, u32)>], from: usize, to: usize) -> Option<f64> {
    if from >= adj.len() || to >= adj.len() {
        return None;
    }
    if from == to {
        return Some(0.0);
    }
    let n = adj.len();
    let mut dist = vec![u32::MAX; n];
    let mut heap: BinaryHeap<(Reverse<u32>, usize)> = BinaryHeap::new();
    dist[from] = 0;
    heap.push((Reverse(0), from));
    while let Some((Reverse(d), u)) = heap.pop() {
        if d > dist[u] {
            continue;
        }
        if u == to {
            return Some(d as f64);
        }
        for &(v, w) in &adj[u] {
            let nd = d.saturating_add(w);
            if nd < dist[v] {
                dist[v] = nd;
                heap.push((Reverse(nd), v));
            }
        }
    }
    if dist[to] == u32::MAX {
        None
    } else {
        Some(dist[to] as f64)
    }
}

/// Single-source shortest-path distances (metres) from `src` to every railway node
/// over `adj`; unreachable nodes are `f64::INFINITY`.
fn rail_dijkstra_all(adj: &[Vec<(usize, u32)>], src: usize) -> Vec<f64> {
    let n = adj.len();
    if src >= n {
        return Vec::new();
    }
    let mut dist = vec![u32::MAX; n];
    let mut heap: BinaryHeap<(Reverse<u32>, usize)> = BinaryHeap::new();
    dist[src] = 0;
    heap.push((Reverse(0), src));
    while let Some((Reverse(d), u)) = heap.pop() {
        if d > dist[u] {
            continue;
        }
        for &(v, w) in &adj[u] {
            let nd = d.saturating_add(w);
            if nd < dist[v] {
                dist[v] = nd;
                heap.push((Reverse(nd), v));
            }
        }
    }
    dist.iter()
        .map(|&d| if d == u32::MAX { f64::INFINITY } else { d as f64 })
        .collect()
}

/// Area-weighted (shoelace) centroid of a polygon ring (lat/lng plane); falls back
/// to the vertex mean for a degenerate (near-zero-area) ring.
fn polygon_centroid(ring: &[LatLng]) -> LatLng {
    let n = ring.len();
    if n == 0 {
        return LatLng { latitude: 0.0, longitude: 0.0 };
    }
    let mut area2 = 0.0f64;
    let mut cx = 0.0f64;
    let mut cy = 0.0f64;
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = (ring[i].longitude, ring[i].latitude);
        let (xj, yj) = (ring[j].longitude, ring[j].latitude);
        let cross = xj * yi - xi * yj;
        area2 += cross;
        cx += (xj + xi) * cross;
        cy += (yj + yi) * cross;
        j = i;
    }
    if area2.abs() < 1e-12 {
        let lat = ring.iter().map(|p| p.latitude).sum::<f64>() / n as f64;
        let lng = ring.iter().map(|p| p.longitude).sum::<f64>() / n as f64;
        return LatLng { latitude: lat, longitude: lng };
    }
    let a = area2 / 2.0;
    LatLng {
        latitude: cy / (6.0 * a),
        longitude: cx / (6.0 * a),
    }
}

/// Normalize a station name for reference-token substring matching (uppercase,
/// hyphens/underscores/dots → spaces, whitespace collapsed), so a config token like
/// "Antwerpen-Centraal" matches "Antwerpen Centraal Station".
fn normalize_station(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = true;
    for ch in s.chars() {
        let c = if ch == '-' || ch == '_' || ch == '.' || ch.is_whitespace() {
            ' '
        } else {
            ch.to_ascii_uppercase()
        };
        if c == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    while out.ends_with(' ') {
        out.pop();
    }
    out
}

/// The single nearest railway node to `coord` with NO distance cutoff; `None` only
/// when the tree is empty.
fn unconditional_nearest(tree: &KdTree<f64, usize, [f64; 2]>, coord: &LatLng) -> Option<usize> {
    let query = [coord.latitude, coord.longitude];
    tree.iter_nearest(&query, &squared_euclidean)
        .ok()?
        .next()
        .map(|(_, &idx)| idx)
}

#[cfg(test)]
mod tests {
    use crate::structures::Graph;
    use crate::structures::LatLng;
    use crate::structures::cost::{
        Agglomeration, AgglomerationZone, FareModel, KnownEurosEpsilon, OperatorFare,
        OperatorFareId, OperatorModel, SncbTimeRules,
    };
    use crate::structures::raptor::{Lookup, PatternID, PatternInfo};
    use crate::structures::{NodeData, TransitStopData};
    use gtfs_structures::Availability;

    fn sncb_fare_model(zones: Vec<AgglomerationZone>) -> FareModel {
        FareModel {
            enabled: true,
            known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
            operators: vec![OperatorFare {
                name: "SNCB".into(),
                model: OperatorModel::DistanceBasePerKm {
                    tariff: crate::structures::cost::DistanceTariff::Band {
                        per_km_rate_cents: 12.40,
                        thresholds: [36, 51],
                        coeffs: [1.40, 1.50, 1.60],
                        min_km: 3,
                        max_km: 118,
                        floor_cents: 260,
                    },
                    rules: SncbTimeRules {
                        peak_windows: [(0, 0); 2],
                        n_peak_windows: 0,
                        weekend_discount_adult: 0.0,
                        weekend_discount_reduced: 0.0,
                        train_plus_offpeak_discount: 0.0,
                        train_plus_peak_cap_adult: u32::MAX,
                        train_plus_peak_cap_reduced: u32::MAX,
                    },
                    airport_od_cents: 0,
                },
                express_route_names: Vec::new(),
                express_route_prefixes: Vec::new(),
                express_single_cents: 0,
                express_card6_cents: 0,
                express_card6_reduced_cents: 0,
                airport_station_names: Vec::new(),
            }],
            agglomerations: zones,
            brupass_cents: 0,
            brupass_validity_secs: 0,
        }
    }

    fn box_zone(zone: Agglomeration, lat: f64, lng: f64) -> AgglomerationZone {
        AgglomerationZone {
            zone,
            polygon: vec![
                LatLng { latitude: lat - 0.05, longitude: lng - 0.05 },
                LatLng { latitude: lat - 0.05, longitude: lng + 0.05 },
                LatLng { latitude: lat + 0.05, longitude: lng + 0.05 },
                LatLng { latitude: lat + 0.05, longitude: lng - 0.05 },
            ],
            reference: None,
        }
    }

    fn sncb_pattern_graph(coords: &[(f64, f64)], zones: Vec<AgglomerationZone>) -> Graph {
        sncb_pattern_graph_full(coords, zones, None, None)
    }

    fn sncb_pattern_graph_full(
        coords: &[(f64, f64)],
        zones: Vec<AgglomerationZone>,
        names: Option<&[&str]>,
        rail: Option<(Vec<(f64, f64)>, Vec<Vec<(usize, u32)>>)>,
    ) -> Graph {
        let mut g = Graph::new();
        let mut stop_nodes = Vec::new();
        for (i, &(lat, lng)) in coords.iter().enumerate() {
            let nid = g.add_node(NodeData::TransitStop(TransitStopData {
                name: names.map(|ns| ns[i].to_string()).unwrap_or_else(|| format!("stop{i}")),
                lat_lng: LatLng { latitude: lat, longitude: lng },
                accessibility: Availability::InformationNotAvailable,
                id: format!("s{i}"),
                platform_code: None,
                parent_station: None,
            }));
            stop_nodes.push(nid);
        }
        // Compact stop index == position in `coords`.
        let n = coords.len();
        let max_node = stop_nodes.iter().map(|nd| nd.0).max().unwrap_or(0);
        g.raptor.transit_node_to_stop = vec![u32::MAX; max_node + 1];
        g.raptor.transit_stop_to_node = vec![crate::structures::NodeID(0); n];
        g.raptor.transit_stop_ids = (0..n).map(|i| format!("s{i}")).collect();
        g.raptor.transit_stop_names = names
            .map(|ns| ns.iter().map(|s| s.to_string()).collect())
            .unwrap_or_else(|| (0..n).map(|i| format!("stop{i}")).collect());
        for (compact, &nid) in stop_nodes.iter().enumerate() {
            g.raptor.transit_node_to_stop[nid.0] = compact as u32;
            g.raptor.transit_stop_to_node[compact] = nid;
        }
        g.raptor.transit_agencies = vec![crate::ingestion::gtfs::AgencyInfo {
            name: "SNCB".into(),
            url: String::new(),
            timezone: String::new(),
        }];
        g.raptor.transit_routes = vec![crate::ingestion::gtfs::RouteInfo {
            route_short_name: "IC".into(),
            route_long_name: "InterCity".into(),
            route_type: gtfs_structures::RouteType::Rail,
            agency_id: crate::ingestion::gtfs::AgencyId(0),
            route_color: None,
            route_text_color: None,
        }];
        g.raptor.transit_patterns = vec![PatternInfo { route: crate::ingestion::gtfs::RouteId(0), num_trips: 1 }];
        g.raptor.transit_pattern_stops = stop_nodes.clone();
        g.raptor.transit_idx_pattern_stops = vec![Lookup { start: 0, len: n }];
        g.raptor.transit_stop_patterns = (0..n).map(|_| (PatternID(0), 0u32)).collect();
        // Custom metre-weighted railway if supplied, else a chain through the stops.
        let (rail_nodes, adj) = rail.unwrap_or_else(|| {
            let rail_nodes: Vec<(f64, f64)> = coords.to_vec();
            let mut adj: Vec<Vec<(usize, u32)>> = vec![Vec::new(); n];
            for i in 1..n {
                let d = LatLng { latitude: coords[i - 1].0, longitude: coords[i - 1].1 }
                    .dist(LatLng { latitude: coords[i].0, longitude: coords[i].1 })
                    as u32;
                adj[i - 1].push((i, d));
                adj[i].push((i - 1, d));
            }
            (rail_nodes, adj)
        });
        g.store_railway_graph(rail_nodes, adj);
        g.set_fare_model(sncb_fare_model(zones));
        g
    }

    fn seg_m(a: (f64, f64), b: (f64, f64)) -> f64 {
        LatLng { latitude: a.0, longitude: a.1 }.dist(LatLng { latitude: b.0, longitude: b.1 })
    }

    fn add_pattern_times(g: &mut Graph, times: &[(u32, u32)]) {
        use crate::ingestion::gtfs::{StopTime, TripId};
        let n = times.len();
        g.raptor.transit_pattern_stop_times = times
            .iter()
            .map(|&(arr, dep)| StopTime { arrival: arr, departure: dep, ..StopTime::default() })
            .collect();
        g.raptor.transit_idx_pattern_stop_times = vec![Lookup { start: 0, len: n }];
        g.raptor.transit_pattern_trips = vec![TripId(0)];
        g.raptor.transit_idx_pattern_trips = vec![Lookup { start: 0, len: 1 }];
    }

    fn one_leg_arena(board_pos: u32, alight_pos: u32) -> (Vec<crate::structures::graph::raptor_route::Label>, u32) {
        use crate::structures::graph::raptor_route::Label;
        use crate::structures::raptor::Trace;
        let root = Label { parent: u32::MAX, trace: Trace::NONE, ..Label::NONE };
        let transit = Label {
            parent: 0,
            arena_id: 1,
            trace: Trace {
                pattern: 0,
                trip: 0,
                boarded_at: board_pos,
                alighted_at: alight_pos,
                from_stop: u32::MAX,
                from_bucket: 0,
            },
            ..Label::NONE
        };
        (vec![Label { arena_id: 0, ..root }, transit], 1)
    }

    #[test]
    fn posthoc_prices_brussels_to_antwerpen_run() {
        let ost = (51.230, 2.930);
        let bxl = (50.836, 4.336);
        let ant = (51.200, 4.400);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl, ant, liege];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let mut g = sncb_pattern_graph_full(&coords, zones, None, None);
        add_pattern_times(&mut g, &[(0, 0), (600, 660), (1200, 1260), (1800, 1860)]);

        let (arena, start) = one_leg_arena(1, 2);
        let profile = crate::structures::cost::FareProfile::default();
        let price = g
            .plan_price_posthoc(&arena, start, 2, profile)
            .expect("fares on ⇒ a post-hoc price");

        let run_m = g.sncb_fare_distance_m(1, 2, 0, 1, 2, 0.0);
        let tariff = crate::structures::cost::DistanceTariff::Band {
            per_km_rate_cents: 12.40,
            thresholds: [36, 51],
            coeffs: [1.40, 1.50, 1.60],
            min_km: 3,
            max_km: 118,
            floor_cents: 260,
        };
        let expected = tariff.fare_cents(run_m / 1000.0) as f64 / 100.0;
        assert!(run_m > 0.0, "Bxl→Ant has a real fixed distance");
        assert!(
            (price.known_euros - expected).abs() < 0.005,
            "post-hoc fare {} must equal the tariff of the zone-to-zone distance {}",
            price.known_euros,
            expected
        );
        let km = price.sncb_fare_km.expect("SNCB run has a fare km");
        assert!((km - run_m / 1000.0).abs() < 0.01, "sncb_fare_km == run km");
    }

    #[test]
    fn posthoc_breakdown_single_sncb_item_sums_to_capped() {
        let ost = (51.230, 2.930);
        let bxl = (50.836, 4.336);
        let ant = (51.200, 4.400);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl, ant, liege];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let mut g = sncb_pattern_graph_full(&coords, zones, None, None);
        add_pattern_times(&mut g, &[(0, 0), (600, 660), (1200, 1260), (1800, 1860)]);
        let (arena, start) = one_leg_arena(1, 2);
        let price = g
            .plan_price_posthoc(&arena, start, 2, crate::structures::cost::FareProfile::default())
            .expect("fares on ⇒ a post-hoc price");
        assert_eq!(price.breakdown.len(), 1, "a single SNCB run is ONE breakdown item");
        let item = &price.breakdown[0];
        assert_eq!(item.operator, "SNCB");
        assert!(item.coverage.is_none(), "a paid ticket has no coverage reason");
        assert!(
            item.description.contains("2nd class"),
            "default profile is 2nd class: {}",
            item.description
        );
        let sum: f64 = price.breakdown.iter().map(|i| i.euros).sum();
        assert!((sum - price.capped_euros).abs() < 0.005, "breakdown sum == capped_euros");
        assert!((item.euros - price.capped_euros).abs() < 0.005);
    }

    #[test]
    fn posthoc_breakdown_first_class_item() {
        let bxl = (50.836, 4.336);
        let ant = (51.200, 4.400);
        let coords = [bxl, ant];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        add_pattern_times(&mut g, &[(0, 0), (43200, 43260)]);
        // Swap to the Bracketed tariff so the 1st-class formula applies.
        if let crate::structures::cost::OperatorModel::DistanceBasePerKm { tariff, .. } =
            &mut g.raptor.fare_model.operators[0].model
        {
            *tariff = crate::structures::cost::DistanceTariff::Bracketed {
                a_cents_per_km: 16.8546,
                b_cents: 145.1226,
                floor_cents: 262,
                min_km: 3,
                cap_from_km: 116,
                cap_km: 118,
                first_class_thresholds: [36, 51],
                first_class_coeffs: [1.40, 1.50, 1.60],
                first_class_round_thresholds: [2500, 5000],
                first_class_round_grids: [10, 50, 100],
            };
        }
        g.raptor.rebuild_operator_fare_lookup();
        let (arena, start) = one_leg_arena(0, 1);
        let second = crate::structures::cost::FareProfile::default();
        let first = crate::structures::cost::FareProfile {
            travel_class: crate::structures::cost::TravelClass::First,
            ..Default::default()
        };
        let p2 = g.plan_price_posthoc(&arena, start, 2, second).unwrap();
        let p1 = g.plan_price_posthoc(&arena, start, 2, first).unwrap();
        assert!(p1.capped_euros > p2.capped_euros, "1st class dearer than 2nd");
        let item1 = &p1.breakdown[0];
        assert!(item1.description.contains("1st class"), "1st-class label: {}", item1.description);
        let sum1: f64 = p1.breakdown.iter().map(|i| i.euros).sum();
        assert!((sum1 - p1.capped_euros).abs() < 0.005, "1st-class breakdown sums to capped");
    }

    fn two_leg_arena(
        b1: u32,
        a1: u32,
        b2: u32,
        a2: u32,
    ) -> (Vec<crate::structures::graph::raptor_route::Label>, u32) {
        use crate::structures::graph::raptor_route::Label;
        use crate::structures::raptor::Trace;
        let root = Label { parent: u32::MAX, arena_id: 0, trace: Trace::NONE, ..Label::NONE };
        let leg1 = Label {
            parent: 0,
            arena_id: 1,
            trace: Trace { pattern: 0, trip: 0, boarded_at: b1, alighted_at: a1, from_stop: u32::MAX, from_bucket: 0 },
            ..Label::NONE
        };
        let leg2 = Label {
            parent: 1,
            arena_id: 2,
            trace: Trace { pattern: 0, trip: 0, boarded_at: b2, alighted_at: a2, from_stop: u32::MAX, from_bucket: 0 },
            ..Label::NONE
        };
        (vec![root, leg1, leg2], 2)
    }

    #[test]
    fn posthoc_breakdown_two_sncb_runs_no_double_count() {
        let ost = (51.230, 2.930);
        let bxl = (50.836, 4.336);
        let ant = (51.200, 4.400);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl, ant, liege];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        add_pattern_times(&mut g, &[(0, 0), (600, 660), (1200, 1260), (1800, 1860)]);
        // Airport OD fare + tag stop 1 (Bxl) as airport: a leg alighting there closes
        // the run at the flat fare; the next SNCB leg starts a fresh run.
        if let crate::structures::cost::OperatorModel::DistanceBasePerKm { airport_od_cents, .. } =
            &mut g.raptor.fare_model.operators[0].model
        {
            *airport_od_cents = 790;
        }
        g.raptor.rebuild_operator_fare_lookup();
        g.raptor.sncb_airport_stop = vec![false; coords.len()];
        g.raptor.sncb_airport_stop[1] = true;

        let (arena, start) = two_leg_arena(0, 1, 2, 3);
        let price = g
            .plan_price_posthoc(&arena, start, 2, crate::structures::cost::FareProfile::default())
            .unwrap();
        let sncb_items: Vec<_> = price
            .breakdown
            .iter()
            .filter(|i| i.operator == "SNCB")
            .collect();
        assert_eq!(sncb_items.len(), 2, "two separate SNCB runs → two items: {:?}", price.breakdown);
        assert!((sncb_items[0].euros - 7.90).abs() < 1e-9, "run 1 = airport flat 7.90");
        assert!(sncb_items[1].euros > 0.0, "run 2 carries its own (nonzero) fare");
        let sum: f64 = price.breakdown.iter().map(|i| i.euros).sum();
        assert!(
            (sum - price.capped_euros).abs() < 0.005,
            "two-run breakdown sums to capped_euros {} (got {sum})",
            price.capped_euros
        );
    }

    #[test]
    fn posthoc_breakdown_sncb_cap_binds_on_item() {
        let ost = (51.230, 2.930);
        let bxl = (50.836, 4.336);
        let ant = (51.200, 4.400);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl, ant, liege];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        add_pattern_times(&mut g, &[(0, 0), (600, 660), (1200, 1260), (1800, 1860)]);
        if let crate::structures::cost::OperatorModel::DistanceBasePerKm { rules, .. } =
            &mut g.raptor.fare_model.operators[0].model
        {
            rules.peak_windows[0] = (0, 24 * 3600);
            rules.n_peak_windows = 1;
            rules.train_plus_peak_cap_adult = 100;
        }
        g.raptor.rebuild_operator_fare_lookup();
        let (arena, start) = one_leg_arena(0, 3);
        let profile = crate::structures::cost::FareProfile {
            sncb_train_plus: true,
            ..Default::default()
        };
        let price = g.plan_price_posthoc(&arena, start, 2, profile).unwrap();
        assert_eq!(price.breakdown.len(), 1, "one SNCB run");
        assert!((price.capped_euros - 1.00).abs() < 1e-9, "cap binds at 1.00, got {}", price.capped_euros);
        let sum: f64 = price.breakdown.iter().map(|i| i.euros).sum();
        assert!((sum - price.capped_euros).abs() < 0.005, "breakdown sum == capped (cap applied)");
        assert!((price.breakdown[0].euros - 1.00).abs() < 1e-9, "SNCB item shows the capped 1.00");
        assert!(price.known_euros > price.capped_euros, "raw known exceeds the cap");
    }

    #[test]
    fn posthoc_breakdown_subscription_covered() {
        let coords = [(50.836, 4.336), (51.200, 4.400)];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        add_pattern_times(&mut g, &[(0, 0), (600, 660)]);
        let (arena, start) = one_leg_arena(0, 1);
        let profile = crate::structures::cost::FareProfile {
            sncb_subscription: true,
            ..Default::default()
        };
        let price = g.plan_price_posthoc(&arena, start, 2, profile).unwrap();
        assert_eq!(price.capped_euros, 0.0, "subscription rides free");
        assert_eq!(price.breakdown.len(), 1);
        let item = &price.breakdown[0];
        assert_eq!(item.euros, 0.0);
        assert_eq!(item.coverage.as_deref(), Some("SNCB subscription"));
        let sum: f64 = price.breakdown.iter().map(|i| i.euros).sum();
        assert!((sum - price.capped_euros).abs() < 0.005);
    }

    #[test]
    fn posthoc_none_when_fares_disabled() {
        let coords = [(50.836, 4.336), (51.200, 4.400)];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        add_pattern_times(&mut g, &[(0, 0), (600, 660)]);
        g.raptor.fare_model.enabled = false;
        let (arena, start) = one_leg_arena(0, 1);
        assert!(
            g.plan_price_posthoc(&arena, start, 2, crate::structures::cost::FareProfile::default())
                .is_none(),
            "disabled fares ⇒ no post-hoc price"
        );
    }

    #[test]
    fn cum_is_full_distance_and_zones_tagged() {
        let ostende = (51.230, 2.930);
        let midi = (50.836, 4.336);
        let nord = (50.860, 4.360);
        let liege = (50.620, 5.570);
        let coords = [ostende, midi, nord, liege];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let g = sncb_pattern_graph(&coords, zones);

        assert_eq!(g.raptor.sncb_stop_zone[0], Agglomeration::None, "Ostende");
        assert_eq!(g.raptor.sncb_stop_zone[1], Agglomeration::Brussels, "Midi");
        assert_eq!(g.raptor.sncb_stop_zone[2], Agglomeration::Brussels, "Nord");
        assert_eq!(g.raptor.sncb_stop_zone[3], Agglomeration::None, "Liege");

        let cum = &g.raptor.sncb_pattern_cum_railway_m[0];
        assert!((cum[1] - cum[0] - seg_m(ostende, midi)).abs() < 1.0, "Ost->Midi full");
        assert!((cum[2] - cum[1] - seg_m(midi, nord)).abs() < 1.0, "Midi->Nord NOT zeroed");
        assert!((cum[3] - cum[2] - seg_m(nord, liege)).abs() < 1.0, "Nord->Liege full");

        // Zone collapse is at the fare-distance lookup: Brussels->Brussels = 0.
        let d = g.sncb_fare_distance_m(1, 2, 0, 1, 2, 0.0);
        assert!(d.abs() < 1.0, "Bxl->Bxl fare distance = 0 (got {d})");
    }

    /// KEY regression: any Brussels boarding → any Antwerpen alighting charges per-km
    /// for the SAME fixed zone-to-zone distance, independent of which stations.
    #[test]
    fn brussels_to_antwerpen_fare_identical_across_stations() {
        let ost = (51.230, 2.930);
        let bxl_a = (50.836, 4.336);
        let bxl_b = (50.860, 4.360);
        let ant_a = (51.200, 4.400);
        let ant_b = (51.240, 4.440);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl_a, bxl_b, ant_a, ant_b, liege];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let g = sncb_pattern_graph(&coords, zones);
        let d_a_a = g.sncb_fare_distance_m(1, 3, 0, 1, 3, 0.0);
        let d_a_b = g.sncb_fare_distance_m(1, 4, 0, 1, 4, 0.0);
        let d_b_a = g.sncb_fare_distance_m(2, 3, 0, 2, 3, 0.0);
        let d_b_b = g.sncb_fare_distance_m(2, 4, 0, 2, 4, 0.0);
        assert!(d_a_a > 0.0, "Brussels->Antwerpen has a real fixed distance");
        for (label, d) in [("A->B", d_a_b), ("B->A", d_b_a), ("B->B", d_b_b)] {
            assert!(
                (d - d_a_a).abs() < 1.0,
                "Brussels->Antwerpen must be identical across stations: {label} {d} vs A->A {d_a_a}"
            );
        }
        // Pattern-independence: zone-to-zone is `ref_to_ref`, never reads the cum array.
        let d_other_pattern = g.sncb_fare_distance_m(1, 3, 999, 0, 0, 0.0);
        assert!(
            (d_other_pattern - d_a_a).abs() < 1.0,
            "zone-to-zone distance is pattern-independent: {d_other_pattern} vs {d_a_a}"
        );
    }

    #[test]
    fn brussels_to_brussels_is_zero() {
        let ost = (51.230, 2.930);
        let bxl_a = (50.836, 4.336);
        let bxl_b = (50.860, 4.360);
        let coords = [ost, bxl_a, bxl_b];
        let zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        let g = sncb_pattern_graph(&coords, zones);
        let d = g.sncb_fare_distance_m(1, 2, 0, 1, 2, 0.0);
        assert!(d.abs() < 1.0, "Bxl->Bxl = 0 chargeable metres (got {d})");
    }

    #[test]
    fn ostende_to_either_brussels_station_is_identical() {
        let ost = (51.230, 2.930);
        let bxl_a = (50.836, 4.336);
        let bxl_b = (50.860, 4.360);
        let coords = [ost, bxl_a, bxl_b];
        let zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        let g = sncb_pattern_graph(&coords, zones);
        let to_a = g.sncb_fare_distance_m(0, 1, 0, 0, 1, 0.0);
        let to_b = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
        assert!(to_a > 0.0, "Ostende->Brussels has a real distance");
        assert!(
            (to_a - to_b).abs() < 1.0,
            "Ostende->Bxl-A ({to_a}) and Ostende->Bxl-B ({to_b}) must be identical"
        );
    }

    #[test]
    fn brussels_to_liege_fixed_across_boarding_station() {
        let bxl_a = (50.836, 4.336);
        let bxl_b = (50.860, 4.360);
        let liege = (50.620, 5.570);
        let coords = [bxl_a, bxl_b, liege];
        let zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        let g = sncb_pattern_graph(&coords, zones);
        let from_a = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
        let from_b = g.sncb_fare_distance_m(1, 2, 0, 1, 2, 0.0);
        assert!(from_a > 0.0, "Brussels->Liege has a real distance");
        assert!(
            (from_a - from_b).abs() < 1.0,
            "Brussels->Liege must be fixed across boarding station: {from_a} vs {from_b}"
        );
    }

    /// Non-zone -> non-zone through a zone pays the FULL railway distance (the
    /// pass-through-zone segment is NOT zeroed).
    #[test]
    fn non_zone_through_zone_pays_full_distance() {
        let ost = (51.230, 2.930);
        let bxl = (50.848, 4.348);
        let liege = (50.620, 5.570);
        let coords = [ost, bxl, liege];
        let zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        let g = sncb_pattern_graph(&coords, zones);
        let d = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
        let full = seg_m(ost, bxl) + seg_m(bxl, liege);
        assert!(
            (d - full).abs() < 1.0,
            "Ostende->Liege through Brussels pays full distance: {d} vs {full}"
        );
    }

    #[test]
    fn contiguous_run_brussels_to_antwerpen_is_one_zone_ticket() {
        let bxl = (50.836, 4.336);
        let mid = (50.950, 4.380);
        let ant = (51.200, 4.400);
        let coords = [bxl, mid, ant];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let g = sncb_pattern_graph(&coords, zones);
        // `prior_free_m` is ignored for a zone->zone run (ref_to_ref).
        let d_run = g.sncb_fare_distance_m(0, 2, 0, 1, 2, 12_345.0);
        let d_direct = g.sncb_fare_distance_m(0, 2, 0, 0, 2, 0.0);
        assert!(d_direct > 0.0, "Brussels->Antwerpen has a real fixed distance");
        assert!(
            (d_run - d_direct).abs() < 1.0,
            "contiguous run = one zone-to-zone ticket (run {d_run} vs direct {d_direct})"
        );
    }

    /// REAL-GRAPH REGRESSION: the multi-candidate snap must pick the through-track
    /// candidate (min over candidates), not collapse to 0 on a disconnected stub.
    #[test]
    fn ref_to_stop_bypasses_disconnected_stub_and_is_nonzero() {
        let stop0 = (50.848, 4.348);
        let stop1 = (50.848, 4.900);
        let thru0 = (50.849, 4.348);
        let thru_mid = (50.849, 4.600);
        let thru1 = (50.849, 4.900);
        let stub0 = (50.8485, 4.348);
        let stub1 = (50.8485, 4.900);
        let rail_nodes = vec![thru0, thru_mid, thru1, stub0, stub1];
        let d01 = 10_000u32;
        let d12 = 20_000u32;
        let adj = vec![
            vec![(1usize, d01)],
            vec![(0usize, d01), (2usize, d12)],
            vec![(1usize, d12)],
            vec![],
            vec![],
        ];
        let zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        let g = sncb_pattern_graph_full(
            &[stop0, stop1],
            zones,
            None,
            Some((rail_nodes, adj)),
        );
        let d = g.sncb_fare_distance_m(0, 1, 0, 0, 1, 0.0);
        assert!(
            d > 25_000.0,
            "zone->station distance must bypass the stub and be real (~30 km), got {d} m"
        );
        assert!(d < 35_000.0, "distance should be ~30 km, got {d} m");
    }

    /// A hyphen config token ("Antwerpen-Centraal") must match a spaced feed name
    /// ("Antwerpen Centraal Station") via `normalize_station`. A DECOY node at the
    /// centroid (nearer than the station) distinguishes the name-match path from the
    /// centroid fallback.
    #[test]
    fn named_reference_matches_hyphen_vs_space() {
        let ost = (51.230, 2.930);
        let ant_central = (51.100, 4.700);
        let ant_other = (51.199, 4.432);
        let names = ["Oostende", "Antwerpen Centraal Station", "Antwerpen-Berchem"];
        let centroid = (51.220, 4.420);
        let rail_nodes = vec![ost, ant_central, ant_other, centroid];
        let w = 10_000u32;
        let adj = vec![
            vec![(1usize, w)],
            vec![(0usize, w), (2usize, w)],
            vec![(1usize, w), (3usize, w)],
            vec![(2usize, w)],
        ];
        let make = |reference: &str| {
            let mut zones = vec![box_zone(Agglomeration::Antwerpen, centroid.0, centroid.1)];
            zones[0].reference = Some(reference.to_string());
            sncb_pattern_graph_full(
                &[ost, ant_central, ant_other],
                zones,
                Some(&names),
                Some((rail_nodes.clone(), adj.clone())),
            )
        };
        let g = make("Antwerpen-Centraal");
        assert_eq!(
            g.raptor.sncb_zone_ref_node[0],
            Some(1),
            "hyphen token 'Antwerpen-Centraal' must match 'Antwerpen Centraal Station' (node 1)"
        );
        let g_ctrl = make("Zzz-Nonexistent");
        assert_eq!(
            g_ctrl.raptor.sncb_zone_ref_node[0],
            Some(3),
            "non-matching token must fall back to the centroid's nearest node (decoy 3)"
        );
    }

    #[test]
    fn named_reference_station_resolves() {
        let ost = (51.230, 2.930);
        let bxl_central = (50.845, 4.357);
        let bxl_nord = (50.860, 4.360);
        let coords = [ost, bxl_central, bxl_nord];
        let mut zones = vec![box_zone(Agglomeration::Brussels, 50.848, 4.348)];
        zones[0].reference = Some("stop1".into());
        let g = sncb_pattern_graph(&coords, zones);
        assert_eq!(
            g.raptor.sncb_zone_ref_node[0],
            Some(1),
            "named reference 'stop1' selects that stop's railway node"
        );
    }

    #[test]
    fn cross_country_journey_counts_full_km_when_no_zones() {
        let ostende = (51.230, 2.930);
        let mid = (50.900, 4.700);
        let liege = (50.620, 5.570);
        let coords = [ostende, mid, liege];
        let zones = vec![
            box_zone(Agglomeration::Brussels, 50.848, 4.348),
            box_zone(Agglomeration::Antwerpen, 51.220, 4.420),
        ];
        let g_zoned = sncb_pattern_graph(&coords, zones);
        let g_plain = sncb_pattern_graph(&coords, Vec::new());
        let cz = &g_zoned.raptor.sncb_pattern_cum_railway_m[0];
        let cp = &g_plain.raptor.sncb_pattern_cum_railway_m[0];
        assert_eq!(cz.len(), cp.len());
        for i in 0..cz.len() {
            assert!(
                (cz[i] - cp[i]).abs() < 1e-6,
                "far-from-zone journey must count full km (zoned {} vs plain {} at {i})",
                cz[i],
                cp[i]
            );
        }
        assert!(*cz.last().unwrap() > 100_000.0, "Ostende->Liege is >100 km");
    }

    #[test]
    fn disabled_fares_build_no_zone_tags_or_cum() {
        let coords = [(50.836, 4.336), (50.860, 4.360)];
        let mut g = sncb_pattern_graph(&coords, Vec::new());
        let mut off = sncb_fare_model(Vec::new());
        off.enabled = false;
        g.set_fare_model(off);
        assert!(g.raptor.sncb_stop_zone.is_empty(), "no zone tags when fares off");
        assert!(
            g.raptor.sncb_pattern_cum_railway_m.is_empty(),
            "no cum array when fares off"
        );
    }

    #[test]
    fn no_zones_configured_leaves_zone_tags_empty() {
        let coords = [(50.836, 4.336), (50.860, 4.360)];
        let g = sncb_pattern_graph(&coords, Vec::new());
        assert!(
            g.raptor.sncb_stop_zone.is_empty(),
            "no zones => no tags (zero point-in-polygon work)"
        );
        let cum = &g.raptor.sncb_pattern_cum_railway_m[0];
        let s0 = seg_m(coords[0], coords[1]);
        assert!((cum[1] - s0).abs() < 1.0, "no-zone segment charged in full");
    }

    #[test]
    fn pattern_route_resolves_to_sncb_model() {
        let coords = [(50.836, 4.336), (50.860, 4.360)];
        let g = sncb_pattern_graph(&coords, Vec::new());
        assert!(matches!(
            g.raptor.operator_fare_of_route[0],
            OperatorFareId::Modeled { model: OperatorModel::DistanceBasePerKm { .. } }
        ));
    }

    #[test]
    fn normalize_station_unifies_hyphen_space_and_case() {
        use super::normalize_station;
        assert_eq!(normalize_station("Antwerpen-Centraal"), "ANTWERPEN CENTRAAL");
        assert_eq!(
            normalize_station("  Antwerpen   Centraal  Station "),
            "ANTWERPEN CENTRAAL STATION"
        );
        let token = normalize_station("Antwerpen-Centraal");
        assert!(normalize_station("Antwerpen Centraal Station").contains(token.as_str()));
        assert!(
            normalize_station("Bruxelles-Central / Brussel-Centraal")
                .contains(normalize_station("Bruxelles-Central").as_str())
        );
    }

    #[test]
    fn store_and_get_railway_graph_data() {
        let mut g = Graph::new();
        let nodes = vec![(50.0, 4.0), (50.001, 4.0)];
        let adj = vec![vec![(1usize, 111u32)], vec![(0usize, 111u32)]];
        g.store_railway_graph(nodes.clone(), adj.clone());
        let result = g.get_railway_graph_data();
        assert!(result.is_some());
        let (got_nodes, got_adj) = result.unwrap();
        assert_eq!(got_nodes, nodes);
        assert_eq!(got_adj, adj);
    }

    #[test]
    fn get_railway_graph_data_empty() {
        let g = Graph::new();
        assert!(g.get_railway_graph_data().is_none());
    }
}
