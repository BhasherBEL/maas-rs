use serde::{Deserialize, Serialize};

use crate::ingestion::gtfs::RouteId;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct Lookup {
    pub start: usize,
    pub len: usize,
}

impl Lookup {
    pub fn of<'a, T>(&self, data: &'a [T]) -> &'a [T] {
        &data[self.start as usize..(self.start + self.len) as usize]
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatternID(pub u32);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatternInfo {
    pub route: RouteId,
    pub num_trips: u32,
}

#[derive(Clone, Copy)]
pub struct Trace {
    pub pattern: u32,
    pub trip: u32,
    pub boarded_at: u32,
    pub alighted_at: u32,
    pub from_stop: u32,
}

impl Trace {
    pub const NONE: Self = Self {
        pattern: u32::MAX,
        trip: u32::MAX,
        boarded_at: u32::MAX,
        alighted_at: u32::MAX,
        from_stop: u32::MAX,
    };

    #[inline]
    pub fn is_transit(&self) -> bool {
        self.pattern != u32::MAX
    }

    #[inline]
    pub fn is_transfer(&self) -> bool {
        self.from_stop != u32::MAX && !self.is_transit()
    }
}
