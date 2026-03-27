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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Lookup ────────────────────────────────────────────────────────────────

    #[test]
    fn lookup_of_extracts_correct_slice() {
        let data = [10u32, 20, 30, 40, 50];
        let l = Lookup { start: 1, len: 3 };
        assert_eq!(l.of(&data), &[20, 30, 40]);
    }

    #[test]
    fn lookup_of_start_zero() {
        let data = [1u32, 2, 3];
        let l = Lookup { start: 0, len: 2 };
        assert_eq!(l.of(&data), &[1, 2]);
    }

    #[test]
    fn lookup_of_empty_slice() {
        let data = [1u32, 2, 3];
        let l = Lookup { start: 2, len: 0 };
        assert_eq!(l.of(&data), &[] as &[u32]);
    }

    #[test]
    fn lookup_of_full_slice() {
        let data = [5u32, 6, 7, 8];
        let l = Lookup { start: 0, len: 4 };
        assert_eq!(l.of(&data), &[5, 6, 7, 8]);
    }

    // ── Trace ─────────────────────────────────────────────────────────────────

    #[test]
    fn trace_none_is_neither_transit_nor_transfer() {
        let t = Trace::NONE;
        assert!(!t.is_transit());
        assert!(!t.is_transfer());
    }

    #[test]
    fn trace_with_pattern_is_transit() {
        let t = Trace {
            pattern: 5,
            trip: 2,
            boarded_at: 0,
            alighted_at: 3,
            from_stop: u32::MAX,
        };
        assert!(t.is_transit());
        assert!(!t.is_transfer());
    }

    #[test]
    fn trace_with_from_stop_no_pattern_is_transfer() {
        let t = Trace {
            pattern: u32::MAX,
            trip: u32::MAX,
            boarded_at: u32::MAX,
            alighted_at: u32::MAX,
            from_stop: 3,
        };
        assert!(!t.is_transit());
        assert!(t.is_transfer());
    }

    #[test]
    fn trace_with_both_pattern_and_from_stop_is_transit_not_transfer() {
        // pattern != MAX → is_transit() wins, is_transfer() requires !is_transit()
        let t = Trace {
            pattern: 1,
            trip: 0,
            boarded_at: 0,
            alighted_at: 1,
            from_stop: 2,
        };
        assert!(t.is_transit());
        assert!(!t.is_transfer());
    }
}
