use serde::{Deserialize, Serialize};

use crate::structures::MAX_SCENARIOS;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DelayCDF {
    /// Sorted bins `(delay_seconds, cumulative_probability)`; delay signed (negative = early).
    pub bins: Vec<(i32, f32)>,
}

impl DelayCDF {
    /// P(delay ≤ budget_secs).
    pub fn prob_on_time(&self, budget_secs: i32) -> f32 {
        let pos = self
            .bins
            .partition_point(|&(delay, _)| delay <= budget_secs);
        if pos == 0 { 0.0 } else { self.bins[pos - 1].1 }
    }

    /// Probability mass per bin (CDF differences); zero-mass entries skipped.
    pub fn pmf(&self) -> impl Iterator<Item = (i32, f32)> + '_ {
        let mut prev = 0.0f32;
        self.bins.iter().filter_map(move |&(delay, cum)| {
            let mass = cum - prev;
            prev = cum;
            (mass > 0.0).then_some((delay, mass))
        })
    }

    /// P(delay ≥ secs), the upper tail.
    pub fn prob_at_least(&self, secs: i32) -> f32 {
        self.pmf()
            .filter(|&(delay, _)| delay >= secs)
            .map(|(_, mass)| mass)
            .sum()
    }

    /// P(transfer) with `margin` slack, folding in the boarding vehicle's delay:
    /// board iff `D_feeder − D_board ≤ margin`, assuming independence. With no
    /// boarding model collapses to `prob_on_time(margin)`.
    pub fn prob_on_time_vs(&self, board: Option<&DelayCDF>, margin: i32) -> f32 {
        match board {
            Some(b) if !b.bins.is_empty() => b
                .pmf()
                .map(|(delay, mass)| mass * self.prob_on_time(margin + delay))
                .sum(),
            _ => self.prob_on_time(margin),
        }
    }
}

/// Reliability values `>=` this collapse into the single CERTAIN bucket.
pub const CERTAIN_THRESHOLD: f32 = 0.99;

#[derive(Clone, Debug)]
pub struct ReliabilityBuckets {
    /// Sorted, strictly increasing edges in (0,1); the CERTAIN bucket (>=0.99) is
    /// implicit and always the highest index.
    edges: Vec<f32>,
}

impl Default for ReliabilityBuckets {
    fn default() -> Self {
        Self::new(&[0.50, 0.80, 0.95])
    }
}

/// Non-empty, every value in (0,1), strictly increasing.
pub fn valid_reliability_edges(edges: &[f32]) -> bool {
    !edges.is_empty()
        && edges.iter().all(|&e| e > 0.0 && e < 1.0)
        && edges.windows(2).all(|w| w[0] < w[1])
}

impl ReliabilityBuckets {
    pub fn new(edges: &[f32]) -> Self {
        ReliabilityBuckets {
            edges: edges.to_vec(),
        }
    }

    /// Bucket index `0..=edges.len()+1`; `edges.len()+1` = CERTAIN (>= 0.99).
    #[inline]
    pub fn bucket(&self, reliability: f32) -> u8 {
        if reliability >= CERTAIN_THRESHOLD {
            return (self.edges.len() + 1) as u8;
        }
        let mut idx = 0u8;
        for &e in &self.edges {
            if reliability >= e {
                idx += 1;
            } else {
                break;
            }
        }
        idx
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Scenario {
    pub time: u32,
    pub prob: f32,
}

#[derive(Clone, Copy, Debug)]
pub struct ScenarioBag {
    data: [Scenario; MAX_SCENARIOS],
    len: u8,
}

impl ScenarioBag {
    pub const EMPTY: Self = Self {
        data: [Scenario {
            time: u32::MAX,
            prob: 0.0,
        }; MAX_SCENARIOS],
        len: 0,
    };

    pub fn single(time: u32) -> Self {
        let mut bag = Self::EMPTY;
        bag.data[0] = Scenario { time, prob: 1.0 };
        bag.len = 1;
        bag
    }

    /// 2-scenario bag sorted by time ascending; `single()` when `hit_prob >= 1.0`.
    pub fn with_scenarios(hit: u32, hit_prob: f32, miss: u32, miss_prob: f32) -> Self {
        if hit_prob >= 1.0 {
            return Self::single(hit);
        }
        let mut bag = Self::EMPTY;
        let (first, second) = if hit <= miss {
            (
                Scenario {
                    time: hit,
                    prob: hit_prob,
                },
                Scenario {
                    time: miss,
                    prob: miss_prob,
                },
            )
        } else {
            (
                Scenario {
                    time: miss,
                    prob: miss_prob,
                },
                Scenario {
                    time: hit,
                    prob: hit_prob,
                },
            )
        };
        bag.data[0] = first;
        bag.data[1] = second;
        bag.len = 2;
        bag
    }

    #[inline]
    pub fn earliest(&self) -> u32 {
        if self.len > 0 {
            self.data[0].time
        } else {
            u32::MAX
        }
    }

    /// Expected arrival `Σ prob_i * time_i`; `f32::MAX` when empty.
    #[inline]
    pub fn expected(&self) -> f32 {
        if self.len == 0 {
            return f32::MAX;
        }
        self.data[..self.len as usize]
            .iter()
            .map(|s| s.prob * s.time as f32)
            .sum()
    }

    #[inline]
    pub fn hit_prob(&self) -> f32 {
        if self.len > 0 { self.data[0].prob } else { 0.0 }
    }

    #[inline]
    pub fn scenarios(&self) -> &[Scenario] {
        &self.data[..self.len as usize]
    }

    pub fn shifted_by(&self, delta: u32) -> Self {
        let mut bag = *self;
        for i in 0..bag.len as usize {
            bag.data[i].time = bag.data[i].time.saturating_add(delta);
        }
        bag
    }

    #[inline]
    pub fn is_reached(&self) -> bool {
        self.len > 0
    }

    /// Dominance via expected arrival, not earliest.
    #[inline]
    pub fn improves_on(&self, existing: &Self) -> bool {
        self.is_reached() && self.expected() < existing.expected()
    }

    #[inline]
    pub fn try_improve(&mut self, candidate: &Self) -> bool {
        if candidate.improves_on(self) {
            *self = *candidate;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_collapses_certain() {
        let b = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
        assert_eq!(b.bucket(1.0), b.bucket(0.99));
        assert_eq!(b.bucket(0.995), b.bucket(0.99));
        assert!(b.bucket(0.989) < b.bucket(0.99));
    }

    #[test]
    fn bucket_bands_are_ordered_and_inclusive_low() {
        let b = ReliabilityBuckets::new(&[0.50, 0.80, 0.95]);
        assert!(b.bucket(0.10) < b.bucket(0.60));
        assert!(b.bucket(0.60) < b.bucket(0.90));
        assert!(b.bucket(0.90) < b.bucket(0.97));
        assert_eq!(b.bucket(0.50), b.bucket(0.79));
        assert_eq!(b.bucket(0.80), b.bucket(0.94));
    }

    #[test]
    fn bucket_handles_zero_and_default() {
        let b = ReliabilityBuckets::default();
        assert_eq!(b.bucket(0.0), 0);
        assert_eq!(b.bucket(1.0), 4);
    }

    fn make_cdf() -> DelayCDF {
        DelayCDF {
            bins: vec![(0, 0.1), (60, 0.5), (120, 0.9), (300, 1.0)],
        }
    }

    fn make_cdf_with_early() -> DelayCDF {
        DelayCDF {
            bins: vec![
                (-120, 0.01),
                (-60, 0.02),
                (0, 0.08),
                (60, 0.22),
                (120, 0.50),
                (180, 0.80),
                (300, 0.96),
                (900, 1.00),
            ],
        }
    }

    #[test]
    fn cdf_empty_returns_zero() {
        let cdf = DelayCDF { bins: vec![] };
        assert_eq!(cdf.prob_on_time(100), 0.0);
    }

    #[test]
    fn cdf_budget_zero_returns_first_bin() {
        let cdf = make_cdf();
        assert_eq!(cdf.prob_on_time(0), 0.1);
    }

    #[test]
    fn cdf_budget_between_bins() {
        let cdf = make_cdf();
        assert_eq!(cdf.prob_on_time(30), 0.1);
    }

    #[test]
    fn cdf_budget_exact_bin_boundary() {
        let cdf = make_cdf();
        assert_eq!(cdf.prob_on_time(60), 0.5);
        assert_eq!(cdf.prob_on_time(120), 0.9);
    }

    #[test]
    fn cdf_budget_exceeds_all_bins() {
        let cdf = make_cdf();
        assert_eq!(cdf.prob_on_time(1000), 1.0);
        assert_eq!(cdf.prob_on_time(300), 1.0);
    }

    #[test]
    fn cdf_single_bin() {
        let cdf = DelayCDF {
            bins: vec![(120, 0.85)],
        };
        assert_eq!(cdf.prob_on_time(119), 0.0);
        assert_eq!(cdf.prob_on_time(120), 0.85);
        assert_eq!(cdf.prob_on_time(121), 0.85);
    }

    #[test]
    fn cdf_negative_bins_below_leftmost_returns_zero() {
        let cdf = make_cdf_with_early();
        assert_eq!(cdf.prob_on_time(-180), 0.0);
    }

    #[test]
    fn cdf_negative_bins_exact_early_bin() {
        let cdf = make_cdf_with_early();
        assert_eq!(cdf.prob_on_time(-120), 0.01);
        assert_eq!(cdf.prob_on_time(-60), 0.02);
    }

    #[test]
    fn cdf_negative_bins_between_early_bins() {
        let cdf = make_cdf_with_early();
        assert_eq!(cdf.prob_on_time(-90), 0.01);
    }

    #[test]
    fn cdf_negative_budget_means_late_arrival() {
        let cdf = make_cdf_with_early();
        assert_eq!(cdf.prob_on_time(-1), 0.02);
        assert_eq!(cdf.prob_on_time(-59), 0.02);
        assert_eq!(cdf.prob_on_time(-61), 0.01);
    }

    #[test]
    fn cdf_positive_bins_unchanged_with_signed_type() {
        let cdf = make_cdf_with_early();
        assert_eq!(cdf.prob_on_time(0), 0.08);
        assert_eq!(cdf.prob_on_time(120), 0.50);
        assert_eq!(cdf.prob_on_time(180), 0.80);
        assert_eq!(cdf.prob_on_time(1000), 1.0);
    }

    #[test]
    fn prob_at_least_is_upper_tail() {
        let cdf = make_cdf();
        assert!((cdf.prob_at_least(0) - 1.0).abs() < 1e-6);
        assert!((cdf.prob_at_least(60) - 0.9).abs() < 1e-6);
        assert!((cdf.prob_at_least(120) - 0.5).abs() < 1e-6);
        assert!((cdf.prob_at_least(300) - 0.1).abs() < 1e-6);
        assert!((cdf.prob_at_least(301) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn prob_at_least_complements_prob_on_time() {
        let cdf = make_cdf_with_early();
        for s in [-200, -120, -60, -1, 0, 1, 60, 120, 300, 1000] {
            let lhs = cdf.prob_at_least(s);
            let rhs = 1.0 - cdf.prob_on_time(s - 1);
            assert!((lhs - rhs).abs() < 1e-6, "s={s}: {lhs} vs {rhs}");
        }
    }

    fn make_bus_cdf() -> DelayCDF {
        DelayCDF {
            bins: vec![
                (-300, 0.03),
                (-120, 0.09),
                (-60, 0.16),
                (0, 0.45),
                (60, 0.58),
                (120, 0.67),
                (180, 0.74),
                (300, 0.84),
                (600, 0.93),
                (900, 0.97),
                (1800, 1.00),
            ],
        }
    }

    #[test]
    fn pmf_reproduces_bin_jumps() {
        let pmf: Vec<(i32, f32)> = make_cdf().pmf().collect();
        let expected = [(0, 0.1f32), (60, 0.4), (120, 0.4), (300, 0.1)];
        assert_eq!(pmf.len(), expected.len());
        for (got, want) in pmf.iter().zip(expected.iter()) {
            assert_eq!(got.0, want.0);
            assert!((got.1 - want.1).abs() < 1e-6, "got {got:?} want {want:?}");
        }
        let total: f32 = pmf.iter().map(|&(_, m)| m).sum();
        assert!((total - 1.0).abs() < 1e-6);
    }

    #[test]
    fn prob_on_time_vs_none_collapses_to_feeder_only() {
        let feeder = make_cdf_with_early();
        for m in [-50, 0, 96, 200, 1000] {
            assert_eq!(feeder.prob_on_time_vs(None, m), feeder.prob_on_time(m));
        }
    }

    #[test]
    fn prob_on_time_vs_point_mass_at_zero_equals_feeder_only() {
        let feeder = make_cdf_with_early();
        let on_time = DelayCDF {
            bins: vec![(0, 1.0)],
        };
        assert!(
            (feeder.prob_on_time_vs(Some(&on_time), 96) - feeder.prob_on_time(96)).abs() < 1e-6
        );
    }

    #[test]
    fn prob_on_time_vs_late_boarding_vehicle_raises_reliability() {
        let feeder = make_cdf_with_early();
        let board = make_bus_cdf();
        let merged = feeder.prob_on_time_vs(Some(&board), 96);
        assert_eq!(feeder.prob_on_time(96), 0.22);
        assert!(
            merged > 0.22,
            "merged {merged} should exceed feeder-only 0.22"
        );
        assert!((merged - 0.516).abs() < 1e-3, "merged was {merged}");
    }

    #[test]
    fn prob_on_time_vs_early_boarding_vehicle_lowers_reliability() {
        let feeder = make_cdf_with_early();
        let early = DelayCDF {
            bins: vec![(-120, 0.9), (0, 1.0)],
        };
        let merged = feeder.prob_on_time_vs(Some(&early), 96);
        assert!((merged - 0.04).abs() < 1e-6, "merged was {merged}");
        assert!(merged < feeder.prob_on_time(96));
    }

    #[test]
    fn empty_bag_is_not_reached() {
        assert!(!ScenarioBag::EMPTY.is_reached());
        assert_eq!(ScenarioBag::EMPTY.earliest(), u32::MAX);
        assert_eq!(ScenarioBag::EMPTY.hit_prob(), 0.0);
        assert_eq!(ScenarioBag::EMPTY.expected(), f32::MAX);
        assert_eq!(ScenarioBag::EMPTY.scenarios().len(), 0);
    }

    #[test]
    fn single_bag_properties() {
        let bag = ScenarioBag::single(1000);
        assert!(bag.is_reached());
        assert_eq!(bag.earliest(), 1000);
        assert_eq!(bag.hit_prob(), 1.0);
        assert!((bag.expected() - 1000.0).abs() < 1e-3);
        assert_eq!(bag.scenarios().len(), 1);
        assert_eq!(bag.scenarios()[0].time, 1000);
        assert!((bag.scenarios()[0].prob - 1.0).abs() < 1e-6);
    }

    #[test]
    fn with_scenarios_hit_before_miss() {
        let bag = ScenarioBag::with_scenarios(500, 0.7, 700, 0.3);
        assert!(bag.is_reached());
        assert_eq!(bag.earliest(), 500);
        assert_eq!(bag.hit_prob(), 0.7);
        assert!(
            (bag.expected() - 560.0).abs() < 1e-3,
            "expected 560.0, got {}",
            bag.expected()
        );
        assert_eq!(bag.scenarios().len(), 2);
    }

    #[test]
    fn with_scenarios_miss_before_hit_sorts_by_time() {
        let bag = ScenarioBag::with_scenarios(700, 0.3, 500, 0.7);
        assert_eq!(bag.earliest(), 500);
        assert_eq!(bag.scenarios().len(), 2);
        assert_eq!(bag.scenarios()[0].time, 500);
        assert_eq!(bag.scenarios()[1].time, 700);
    }

    #[test]
    fn with_scenarios_hit_prob_one_returns_single() {
        let bag = ScenarioBag::with_scenarios(1000, 1.0, 1200, 0.0);
        assert_eq!(bag.scenarios().len(), 1);
        assert_eq!(bag.earliest(), 1000);
    }

    #[test]
    fn shifted_by_adjusts_all_times() {
        let bag = ScenarioBag::with_scenarios(500, 0.7, 700, 0.3);
        let shifted = bag.shifted_by(100);
        assert_eq!(shifted.earliest(), 600);
        let scenarios = shifted.scenarios();
        assert_eq!(scenarios[0].time, 600);
        assert_eq!(scenarios[1].time, 800);
        assert!((scenarios[0].prob - 0.7).abs() < 1e-6);
    }

    #[test]
    fn shifted_by_saturates_at_max() {
        let bag = ScenarioBag::single(u32::MAX - 10);
        let shifted = bag.shifted_by(100);
        assert_eq!(shifted.earliest(), u32::MAX);
    }

    #[test]
    fn improves_on_lower_expected_wins() {
        let better = ScenarioBag::single(400);
        let worse = ScenarioBag::single(600);
        assert!(better.improves_on(&worse));
        assert!(!worse.improves_on(&better));
    }

    #[test]
    fn improves_on_empty_never_beats_reached() {
        assert!(!ScenarioBag::EMPTY.improves_on(&ScenarioBag::single(9999)));
    }

    #[test]
    fn improves_on_any_beats_empty() {
        assert!(ScenarioBag::single(9999).improves_on(&ScenarioBag::EMPTY));
    }

    #[test]
    fn try_improve_updates_when_better() {
        let mut bag = ScenarioBag::single(600);
        let candidate = ScenarioBag::single(500);
        assert!(bag.try_improve(&candidate));
        assert_eq!(bag.earliest(), 500);
    }

    #[test]
    fn try_improve_no_change_when_worse() {
        let mut bag = ScenarioBag::single(400);
        let candidate = ScenarioBag::single(500);
        assert!(!bag.try_improve(&candidate));
        assert_eq!(bag.earliest(), 400);
    }

    #[test]
    fn try_improve_empty_accepts_any() {
        let mut bag = ScenarioBag::EMPTY;
        let candidate = ScenarioBag::single(100);
        assert!(bag.try_improve(&candidate));
        assert_eq!(bag.earliest(), 100);
    }
}
