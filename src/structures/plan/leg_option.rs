use async_graphql::SimpleObject;

use crate::structures::NodeID;
use crate::structures::cost::{Axis, BalanceWeights};
use crate::structures::plan::PlanCoordinate;

/// Inclusive index range into the option's `geometry`.
#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct DismountRun {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct LegOption {
    pub time: f64,
    pub dplus: f64,
    pub surface: f64,
    pub variance: f64,
    pub cycleway_deficit: f64,
    pub p50: u32,
    pub p95: u32,
    pub length: usize,
    pub unpaved_length: usize,
    pub dismount_length: usize,
    pub dismount_runs: Vec<DismountRun>,
    pub elevation_gain: Option<usize>,
    pub cycleroute_length: Option<usize>,
    pub geometry: Vec<PlanCoordinate>,
    #[graphql(skip)]
    pub nodes: Vec<NodeID>,
    /// Aligned to `nodes.windows(2)`; empty when reconstructed off the full graph.
    #[graphql(skip)]
    pub edges: Vec<crate::structures::StreetEdgeData>,
}

/// The ONLY place a weight is read; ties break to lowest index. `options` non-empty.
pub fn initial_cursor(options: &[LegOption], balance: &BalanceWeights) -> usize {
    let axes = [
        Axis::Time,
        Axis::Dplus,
        Axis::Surface,
        Axis::CyclewayDeficit,
        Axis::Variance,
    ];
    let val = |o: &LegOption, a: Axis| match a {
        Axis::Time => o.time,
        Axis::Dplus => o.dplus,
        Axis::Surface => o.surface,
        Axis::CyclewayDeficit => o.cycleway_deficit,
        Axis::Variance => o.variance,
    };
    let mut lo = [f64::INFINITY; 5];
    let mut hi = [f64::NEG_INFINITY; 5];
    for o in options {
        for (j, &a) in axes.iter().enumerate() {
            lo[j] = lo[j].min(val(o, a));
            hi[j] = hi[j].max(val(o, a));
        }
    }
    let mut best = 0usize;
    let mut best_score = f64::INFINITY;
    for (i, o) in options.iter().enumerate() {
        let score: f64 = axes
            .iter()
            .enumerate()
            .map(|(j, &a)| {
                let range = hi[j] - lo[j];
                let norm = if range <= 0.0 {
                    0.0
                } else {
                    (val(o, a) - lo[j]) / range
                };
                balance.weight(a) * norm
            })
            .sum();
        if score < best_score {
            best_score = score;
            best = i;
        }
    }
    best
}

/// Best option with `p50 <= window`; falls back to fastest when none fit.
/// Without a window, exactly `initial_cursor`.
pub fn highlight_index(
    options: &[LegOption],
    window: Option<u32>,
    balance: &BalanceWeights,
) -> usize {
    let Some(w) = window else {
        return initial_cursor(options, balance);
    };
    let feasible: Vec<usize> = (0..options.len())
        .filter(|&i| options[i].p50 <= w)
        .collect();
    if feasible.is_empty() {
        return (0..options.len())
            .min_by(|&i, &j| options[i].p50.cmp(&options[j].p50).then(i.cmp(&j)))
            .unwrap_or(0);
    }
    let subset: Vec<LegOption> = feasible.iter().map(|&i| options[i].clone()).collect();
    let local = initial_cursor(&subset, balance);
    feasible[local]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opt(time: f64, variance: f64) -> LegOption {
        LegOption {
            time,
            dplus: 0.0,
            surface: 0.0,
            variance,
            cycleway_deficit: 0.0,
            p50: time as u32,
            p95: time as u32,
            length: 0,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![],
            edges: vec![],
        }
    }

    #[test]
    fn cursor_prefers_low_weighted_axis() {
        let options = vec![opt(10.0, 9.0), opt(20.0, 1.0)];
        let time_only = BalanceWeights {
            time: 1.0,
            dplus: 0.0,
            surface: 0.0,
            cycleway_deficit: 0.0,
            variance: 0.0,
        };
        let variance_only = BalanceWeights {
            time: 0.0,
            dplus: 0.0,
            surface: 0.0,
            cycleway_deficit: 0.0,
            variance: 1.0,
        };
        assert_eq!(initial_cursor(&options, &time_only), 0);
        assert_eq!(initial_cursor(&options, &variance_only), 1);
    }

    #[test]
    fn cursor_is_weight_invariant_in_option_set() {
        let options = vec![opt(10.0, 9.0), opt(15.0, 5.0), opt(20.0, 1.0)];
        let snap = |os: &[LegOption]| -> Vec<(f64, f64, f64, f64, f64)> {
            os.iter()
                .map(|o| (o.time, o.dplus, o.surface, o.cycleway_deficit, o.variance))
                .collect()
        };
        let before = snap(&options);
        for w in [0.0, 0.3, 0.7, 1.0] {
            let b = BalanceWeights {
                time: 1.0 - w,
                dplus: 0.0,
                surface: 0.0,
                cycleway_deficit: 0.0,
                variance: w,
            };
            let _ = initial_cursor(&options, &b);
        }
        assert_eq!(
            before,
            snap(&options),
            "initial_cursor must not mutate any scored axis of the option set"
        );
    }

    #[test]
    fn cursor_breaks_ties_by_lowest_index() {
        let options = vec![opt(10.0, 10.0), opt(10.0, 10.0)];
        let b = BalanceWeights::default();
        assert_eq!(initial_cursor(&options, &b), 0);
    }

    #[test]
    fn highlight_picks_balanced_when_no_window() {
        let options = vec![opt(10.0, 9.0), opt(20.0, 1.0)];
        let b = BalanceWeights {
            time: 0.0,
            dplus: 0.0,
            surface: 0.0,
            cycleway_deficit: 0.0,
            variance: 1.0,
        };
        assert_eq!(
            highlight_index(&options, None, &b),
            1,
            "no window ⇒ pure balance cursor"
        );
    }

    #[test]
    fn highlight_excludes_options_past_the_window() {
        let options = vec![opt(10.0, 9.0), opt(20.0, 1.0)];
        let variance = BalanceWeights {
            time: 0.0,
            dplus: 0.0,
            surface: 0.0,
            cycleway_deficit: 0.0,
            variance: 1.0,
        };
        assert_eq!(
            highlight_index(&options, Some(15), &variance),
            0,
            "infeasible varianceable option excluded"
        );
    }

    #[test]
    fn highlight_falls_back_to_fastest_when_none_feasible() {
        let options = vec![opt(30.0, 9.0), opt(40.0, 1.0)];
        let b = BalanceWeights::default();
        assert_eq!(
            highlight_index(&options, Some(10), &b),
            0,
            "none feasible ⇒ fastest (lowest p50)"
        );
    }
}
