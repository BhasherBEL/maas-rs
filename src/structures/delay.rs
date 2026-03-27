use serde::{Deserialize, Serialize};

use crate::structures::MAX_SCENARIOS;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DelayCDF {
    /// Sorted bins: (delay_seconds, cumulative_probability)
    pub bins: Vec<(u32, f32)>,
}

impl DelayCDF {
    /// Returns P(delay ≤ budget_secs) using a step-CDF lookup.
    pub fn prob_on_time(&self, budget_secs: u32) -> f32 {
        let pos = self.bins.partition_point(|&(delay, _)| delay <= budget_secs);
        if pos == 0 {
            0.0
        } else {
            self.bins[pos - 1].1
        }
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

    /// Creates a 2-scenario bag sorted by time ascending.
    /// Falls back to `single()` when `hit_prob >= 1.0`.
    pub fn with_scenarios(hit: u32, hit_prob: f32, miss: u32, miss_prob: f32) -> Self {
        if hit_prob >= 1.0 {
            return Self::single(hit);
        }
        let mut bag = Self::EMPTY;
        let (first, second) = if hit <= miss {
            (
                Scenario { time: hit, prob: hit_prob },
                Scenario { time: miss, prob: miss_prob },
            )
        } else {
            (
                Scenario { time: miss, prob: miss_prob },
                Scenario { time: hit, prob: hit_prob },
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

    /// Expected arrival time (Σ prob_i * time_i). Returns f32::MAX when empty.
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

    /// Probability of the earliest (best-case) scenario.
    #[inline]
    pub fn hit_prob(&self) -> f32 {
        if self.len > 0 {
            self.data[0].prob
        } else {
            0.0
        }
    }

    /// Returns the scenarios as a slice.
    #[inline]
    pub fn scenarios(&self) -> &[Scenario] {
        &self.data[..self.len as usize]
    }

    /// Returns a new bag with all scenario times shifted by `delta`.
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

    /// Dominance via expected arrival instead of earliest.
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
