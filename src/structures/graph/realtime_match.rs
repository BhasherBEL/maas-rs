//! STIB realtime → schedule matching.
//!
//! STIB's waiting-times feed gives `(pointid, lineid, expectedArrivalTime)` with
//! no trip id or delay. We recover both by matching each predicted arrival to the
//! GTFS static schedule (the maas-rt approach), using maas-rs's own index:
//!
//! * **line:** STIB `lineid` equals GTFS `route_short_name` (not `route_id`).
//! * **stop:** STIB `pointid` equals a GTFS `stop_id`, or is a prefix of one or
//!   more platform-suffixed stop ids (`0470` → `0470701`, `0470101`, …).
//! * **delay:** `predicted − scheduled`, accepted within tolerance, picking the
//!   scheduled arrival that minimises |delay|.

use crate::ingestion::gtfs::TripId;

use super::Graph;

/// Tolerances for accepting a predicted↔scheduled match (seconds). Vehicles run
/// late far more often than early, so the early bound is tight (prevents matching
/// a late vehicle onto the *next* scheduled departure).
#[derive(Debug, Clone, Copy)]
pub struct MatchParams {
    pub early_tolerance_secs: i64,
    pub max_late_secs: i64,
}

impl Default for MatchParams {
    fn default() -> Self {
        Self {
            early_tolerance_secs: 90,
            max_late_secs: 60 * 60,
        }
    }
}

/// A scheduled arrival candidate for matching: which trip, and its scheduled
/// arrival (seconds since the service-day midnight; may exceed 86400).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledArrival {
    pub trip: TripId,
    pub scheduled_secs: u32,
}

/// Pick the candidate whose scheduled arrival best matches `predicted_secs`,
/// where `delay = predicted − scheduled` lies in `[-early_tolerance, max_late]`.
/// Ties broken by smallest |delay|. Pure — unit-testable without a graph.
pub fn best_match(
    candidates: &[ScheduledArrival],
    predicted_secs: i64,
    params: &MatchParams,
) -> Option<(TripId, i32)> {
    candidates
        .iter()
        .filter_map(|c| {
            let delay = predicted_secs - c.scheduled_secs as i64;
            if delay >= -params.early_tolerance_secs && delay <= params.max_late_secs {
                Some((c.trip, delay))
            } else {
                None
            }
        })
        .min_by_key(|(_, delay)| delay.abs())
        .map(|(trip, delay)| (trip, delay as i32))
}

impl Graph {
    /// Compact stop indices a STIB `pointid` resolves to: an exact `stop_id`
    /// match, else every stop whose id is prefixed by `pointid` (platform
    /// suffixes). Empty if neither matches.
    pub fn stib_stop_indices(&self, pointid: &str) -> Vec<usize> {
        if let Some(idx) = self.raptor.stop_index_of(pointid) {
            return vec![idx];
        }
        self.raptor
            .transit_stop_ids
            .iter()
            .enumerate()
            .filter(|(_, sid)| sid.len() > pointid.len() && sid.starts_with(pointid))
            .map(|(i, _)| i)
            .collect()
    }

    /// All scheduled arrivals at compact `stop` for the line whose
    /// `route_short_name` equals `line`, among trips active on `(date, weekday)`.
    pub fn stib_scheduled_arrivals(
        &self,
        stop: usize,
        line: &str,
        date: u32,
        weekday: u8,
    ) -> Vec<ScheduledArrival> {
        let mut out = Vec::new();
        let pats =
            self.raptor.transit_idx_stop_patterns[stop].of(&self.raptor.transit_stop_patterns);
        for &(pat_id, pos) in pats {
            let p = pat_id.0 as usize;
            let route = self.raptor.transit_patterns[p].route;
            if self.raptor.transit_routes[route.0 as usize].route_short_name != line {
                continue;
            }
            let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
            if n_trips == 0 {
                continue;
            }
            let times = self.raptor.transit_idx_pattern_stop_times[p]
                .of(&self.raptor.transit_pattern_stop_times);
            let trip_ids =
                self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);
            let col = &times[pos as usize * n_trips..(pos as usize + 1) * n_trips];
            for t in 0..n_trips {
                if self.is_trip_active(trip_ids[t], date, weekday) {
                    out.push(ScheduledArrival {
                        trip: trip_ids[t],
                        scheduled_secs: col[t].arrival,
                    });
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_match_picks_closest_within_tolerance() {
        let params = MatchParams::default();
        let cands = [
            ScheduledArrival {
                trip: TripId(1),
                scheduled_secs: 30_000,
            },
            ScheduledArrival {
                trip: TripId(2),
                scheduled_secs: 30_300,
            }, // +5 min
        ];
        // Predicted 30_120: delay vs trip1 = +120 (late, ok); vs trip2 = -180 (too early).
        let (trip, delay) = best_match(&cands, 30_120, &params).unwrap();
        assert_eq!(trip, TripId(1));
        assert_eq!(delay, 120);
    }

    #[test]
    fn best_match_rejects_too_early() {
        let params = MatchParams::default();
        let cands = [ScheduledArrival {
            trip: TripId(1),
            scheduled_secs: 30_000,
        }];
        // Predicted 200s before schedule — beyond the 90s early tolerance.
        assert!(best_match(&cands, 29_800, &params).is_none());
    }

    #[test]
    fn best_match_rejects_beyond_max_late() {
        let params = MatchParams {
            early_tolerance_secs: 90,
            max_late_secs: 600,
        };
        let cands = [ScheduledArrival {
            trip: TripId(1),
            scheduled_secs: 30_000,
        }];
        assert!(best_match(&cands, 31_000, &params).is_none()); // +1000s > 600s
    }

    #[test]
    fn best_match_empty_is_none() {
        assert!(best_match(&[], 30_000, &MatchParams::default()).is_none());
    }
}
