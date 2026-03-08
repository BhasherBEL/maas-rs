use serde::{Deserialize, Serialize};

use crate::structures::MAX_SCENARIOS;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DelayCDF {
    /// Sorted bins: (delay_seconds, cumulative_probability)
    pub bins: Vec<(u32, f32)>,
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

    #[inline]
    pub fn earliest(&self) -> u32 {
        if self.len > 0 {
            self.data[0].time
        } else {
            u32::MAX
        }
    }

    #[inline]
    pub fn is_reached(&self) -> bool {
        self.len > 0
    }

    #[inline]
    pub fn improves_on(&self, existing: &Self) -> bool {
        self.is_reached() && self.earliest() < existing.earliest()
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
