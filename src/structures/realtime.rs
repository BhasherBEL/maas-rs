//! Live realtime delays applied to RAPTOR routing.
//!
//! A [`RealtimeIndex`] maps `(trip, stop_sequence)` to a signed delay in seconds
//! (positive = late). It is produced by the realtime poller from one or more
//! feeds and hot-swapped behind an `ArcSwap`, independently of the graph.
//!
//! The router consults [`RealtimeIndex::delay`] as an *additive* offset: an
//! empty index yields 0 everywhere, exactly reproducing schedule-only behavior.

use std::collections::HashMap;

use crate::ingestion::gtfs::TripId;

#[derive(Debug, Clone, Default)]
pub struct RealtimeIndex {
    /// Delay in seconds per `(trip, compact_stop_index)`; positive = late.
    /// The stop key is the RAPTOR compact stop index (what `scan_route` uses),
    /// not the GTFS `stop_sequence`.
    delays: HashMap<(TripId, u32), i32>,
    /// Unix seconds when this snapshot was produced (0 for the empty index).
    pub generated_at: i64,
}

impl RealtimeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_delays(
        generated_at: i64,
        delays: impl IntoIterator<Item = ((TripId, u32), i32)>,
    ) -> Self {
        Self {
            delays: delays.into_iter().collect(),
            generated_at,
        }
    }

    /// Delay (seconds, positive = late) for a trip at a compact stop index.
    /// Returns 0 when no realtime information is known — the inert default.
    #[inline]
    pub fn delay(&self, trip: TripId, stop: u32) -> i32 {
        self.delays.get(&(trip, stop)).copied().unwrap_or(0)
    }

    /// Like [`delay`], but `None` when no realtime info exists for `(trip, stop)`
    /// — lets callers distinguish "known on time (0)" from "no data".
    #[inline]
    pub fn delay_opt(&self, trip: TripId, stop: u32) -> Option<i32> {
        self.delays.get(&(trip, stop)).copied()
    }

    /// True if any realtime delay is known for `trip` at any stop. Used to flag a
    /// leg as realtime-backed even when the specific board/alight stops are 0.
    pub fn len(&self) -> usize {
        self.delays.len()
    }

    pub fn is_empty(&self) -> bool {
        self.delays.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index_has_zero_delay_everywhere() {
        let idx = RealtimeIndex::new();
        assert_eq!(idx.delay(TripId(0), 0), 0);
        assert_eq!(idx.delay(TripId(42), 7), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn populated_index_returns_known_delays() {
        let idx = RealtimeIndex::from_delays(
            1_700_000_000,
            [((TripId(3), 2), 120), ((TripId(3), 3), -30)],
        );
        assert_eq!(idx.delay(TripId(3), 2), 120);
        assert_eq!(idx.delay(TripId(3), 3), -30);
        assert_eq!(idx.delay(TripId(3), 9), 0); // unknown stop on a known trip
        assert_eq!(idx.delay(TripId(4), 2), 0); // unknown trip
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.generated_at, 1_700_000_000);
    }
}
