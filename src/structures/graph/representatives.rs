//! Trim an ε-Pareto front to ≤k diverse options by greedy farthest-point
//! selection in min-max-normalized objective space.

use super::{Graph, multiobj::ParetoPath};
use crate::structures::cost::{Axis, LegRole, RoutingMode};
use crate::structures::{BikeCost, NodeID};

impl Graph {
    pub fn multiobj_representatives(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
    ) -> Vec<ParetoPath> {
        self.multiobj_representatives_budgeted(
            origin,
            destination,
            mode,
            role,
            bike,
            self.raptor.distance_budget,
            false,
        )
    }

    /// `distance_budget == f64::INFINITY` skips the O(edges) `length_lower_bounds`
    /// precompute (right for short access/egress legs).
    pub(crate) fn multiobj_representatives_budgeted(
        &self,
        origin: NodeID,
        destination: NodeID,
        mode: RoutingMode,
        role: LegRole,
        bike: &BikeCost,
        distance_budget: f64,
        astar: bool,
    ) -> Vec<ParetoPath> {
        let res = self.multiobj_search(
            origin,
            destination,
            mode,
            role,
            bike,
            &self.raptor.cost_weights,
            &self.raptor.epsilon,
            distance_budget,
            astar,
        );
        let idx = select_representatives(&res.front, self.raptor.representatives_k, mode.effective_front_axes(self.raptor.bike_select_dplus));
        idx.into_iter().map(|i| res.front[i].clone()).collect()
    }
}

/// Deterministic. Seeds the chosen set with each axis' minimizer (extrema), then
/// greedily adds the max-min-distant path, breaking ties by smallest index.
pub fn select_representatives(front: &[ParetoPath], k: usize, axes: &[Axis]) -> Vec<usize> {
    if front.is_empty() || k == 0 {
        return Vec::new();
    }
    if front.len() <= k {
        return (0..front.len()).collect();
    }

    let mut lo = vec![f64::INFINITY; axes.len()];
    let mut hi = vec![f64::NEG_INFINITY; axes.len()];
    for p in front {
        for (j, &a) in axes.iter().enumerate() {
            let v = p.cost.get(a);
            lo[j] = lo[j].min(v);
            hi[j] = hi[j].max(v);
        }
    }
    let norm = |i: usize, j: usize| -> f64 {
        let range = hi[j] - lo[j];
        if range <= 0.0 {
            0.0
        } else {
            (front[i].cost.get(axes[j]) - lo[j]) / range
        }
    };
    let dist = |i: usize, j: usize| -> f64 {
        (0..axes.len())
            .map(|a| {
                let d = norm(i, a) - norm(j, a);
                d * d
            })
            .sum::<f64>()
            .sqrt()
    };

    let mut chosen: Vec<usize> = Vec::with_capacity(k);
    for j in 0..axes.len() {
        let best = (0..front.len())
            .min_by(|&i1, &i2| {
                norm(i1, j)
                    .partial_cmp(&norm(i2, j))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(i1.cmp(&i2))
            })
            .unwrap();
        if !chosen.contains(&best) {
            chosen.push(best);
        }
        if chosen.len() == k {
            return chosen;
        }
    }

    while chosen.len() < k {
        let mut best_idx = usize::MAX;
        let mut best_d = f64::NEG_INFINITY;
        for cand in 0..front.len() {
            if chosen.contains(&cand) {
                continue;
            }
            let d = chosen
                .iter()
                .map(|&c| dist(cand, c))
                .fold(f64::INFINITY, f64::min);
            if d > best_d {
                best_d = d;
                best_idx = cand;
            }
        }
        if best_idx == usize::MAX {
            break;
        }
        chosen.push(best_idx);
    }
    chosen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::NodeID;
    use crate::structures::cost::{CostVector, LegRole, RoutingMode};
    use crate::structures::{BikeCost, Graph};

    fn tiny_two_route_graph() -> (Graph, NodeID, NodeID) {
        use crate::structures::cost::VarGen;
        use crate::structures::{
            BikeAttrs, EdgeData, HighwayClass, LatLng, NodeData, OsmNodeData, StreetEdgeData,
            Surface,
        };
        let mut g = Graph::new();
        let mk = |id: &str, lat: f64, lon: f64| {
            NodeData::OsmNode(OsmNodeData {
                eid: id.into(),
                lat_lng: LatLng {
                    latitude: lat,
                    longitude: lon,
                },
            })
        };
        let a = g.add_node(mk("a", 50.000, 4.000));
        let b = g.add_node(mk("b", 50.000, 4.010));
        let c = g.add_node(mk("c", 50.001, 4.005));
        g.build_raptor_index();
        let edge = |o: NodeID, d: NodeID, len: usize, surface: Surface| {
            let mut at = BikeAttrs::road_default();
            at.highway = HighwayClass::Residential;
            at.surface = surface;
            EdgeData::Street(StreetEdgeData {
                origin: o,
                destination: d,
                partial: false,
                length: len,
                foot: true,
                bike: true,
                car: false,
                attrs: at,
                elev_delta: 0,
                surface_speed: 100,
                var_gen: VarGen::NONE,
            })
        };
        g.add_edge(a, edge(a, b, 100, Surface::Unpaved));
        g.add_edge(a, edge(a, c, 90, Surface::Paved));
        g.add_edge(c, edge(c, b, 90, Surface::Paved));
        (g, a, b)
    }

    #[test]
    fn multiobj_representatives_returns_at_most_k_paths_from_origin_to_dest() {
        let (mut g, a, b) = tiny_two_route_graph();
        g.set_representatives_k(1);
        let bike = BikeCost::new(g.raptor.bike_profile);
        let reps = g.multiobj_representatives(a, b, RoutingMode::Walk, LegRole::Neutral, &bike);
        assert!(!reps.is_empty(), "a route exists");
        assert!(reps.len() <= 1, "trimmed to representatives_k");
        for p in &reps {
            assert_eq!(*p.nodes.first().unwrap(), a);
            assert_eq!(*p.nodes.last().unwrap(), b);
        }
    }

    #[test]
    #[ignore]
    fn representatives_smoke_real_brussels() {
        use std::time::Instant;
        let path = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let mut g = Graph::new();
        crate::ingestion::osm::load_pbf_file(path, None, 4.0, &Default::default(), &mut g).unwrap();
        g.build_raptor_index();
        let (_, &o) = g.nearest_node_dist(50.846, 4.352).expect("origin snaps");
        let (_, &d) = g.nearest_node_dist(50.851, 4.358).expect("dest snaps");
        let bike = BikeCost::new(g.raptor.bike_profile);

        let full = g.multiobj_search(
            o,
            d,
            RoutingMode::Bike,
            LegRole::Neutral,
            &bike,
            &g.raptor.cost_weights,
            &g.raptor.epsilon,
            g.raptor.distance_budget,
            false,
        );
        let ts = Instant::now();
        let reps = g.multiobj_representatives(o, d, RoutingMode::Bike, LegRole::Neutral, &bike);
        eprintln!(
            "REPS front={} -> reps={} k={} elapsed={:.1?}",
            full.front.len(),
            reps.len(),
            g.raptor.representatives_k,
            ts.elapsed()
        );
        assert!(!reps.is_empty(), "non-empty representative set");
        assert!(reps.len() <= g.raptor.representatives_k, "trimmed to k");
        assert!(reps.len() <= full.front.len(), "never more than the front");

        let extreme = |axis| {
            full.front
                .iter()
                .min_by(|a, b| a.cost.get(axis).partial_cmp(&b.cost.get(axis)).unwrap())
                .map(|p| p.cost.get(axis))
                .unwrap()
        };
        let best_time = extreme(Axis::Time);
        let best_surface = extreme(Axis::Surface);
        assert!(
            reps.iter().any(|p| p.cost.get(Axis::Time) == best_time),
            "Time extreme kept"
        );
        assert!(
            reps.iter()
                .any(|p| p.cost.get(Axis::Surface) == best_surface),
            "Surface extreme kept"
        );
    }

    fn path(time: f64, surface: f64) -> ParetoPath {
        ParetoPath {
            nodes: vec![NodeID(0)],
            edges: Vec::new(),
            cost: CostVector::from_active(&[Axis::Time, Axis::Surface], &[time, surface]),
            elev_buffer: (0.0, 0.0),
        }
    }

    fn fixture() -> Vec<ParetoPath> {
        vec![
            path(0.0, 10.0),
            path(10.0, 0.0),
            path(5.0, 5.0),
            path(1.0, 9.0),
            path(9.0, 1.0),
        ]
    }

    #[test]
    fn empty_or_zero_k_is_empty() {
        let axes = [Axis::Time, Axis::Surface];
        assert!(select_representatives(&[], 6, &axes).is_empty());
        assert!(select_representatives(&fixture(), 0, &axes).is_empty());
    }

    #[test]
    fn k_at_least_front_returns_all_in_order() {
        let axes = [Axis::Time, Axis::Surface];
        let front = fixture();
        assert_eq!(
            select_representatives(&front, 5, &axes),
            vec![0, 1, 2, 3, 4]
        );
        assert_eq!(
            select_representatives(&front, 99, &axes),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn extrema_are_always_seeded() {
        let axes = [Axis::Time, Axis::Surface];
        let chosen = select_representatives(&fixture(), 2, &axes);
        assert_eq!(
            chosen,
            vec![0, 1],
            "k=2 returns the Time-best and Surface-best extrema"
        );
    }

    #[test]
    fn greedy_picks_the_diversity_maximal_middle() {
        let axes = [Axis::Time, Axis::Surface];
        assert_eq!(select_representatives(&fixture(), 3, &axes), vec![0, 1, 2]);
    }

    #[test]
    fn selection_is_deterministic() {
        let axes = [Axis::Time, Axis::Surface];
        let front = fixture();
        let a = select_representatives(&front, 4, &axes);
        let b = select_representatives(&front, 4, &axes);
        assert_eq!(a, b);
    }

    #[test]
    fn degenerate_axis_with_zero_range_does_not_divide_by_zero() {
        let front = vec![
            path(5.0, 0.0),
            path(5.0, 4.0),
            path(5.0, 8.0),
            path(5.0, 2.0),
        ];
        let axes = [Axis::Time, Axis::Surface];
        let chosen = select_representatives(&front, 3, &axes);
        assert_eq!(chosen.len(), 3);
        let mut sorted = chosen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 3, "no duplicate indices");
    }
}
