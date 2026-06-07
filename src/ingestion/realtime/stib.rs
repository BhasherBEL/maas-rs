//! Custom STIB / MIVB realtime feed.
//!
//! STIB has no GTFS-Realtime feed; its proprietary waiting-times API returns
//! predicted arrival times per stop (`pointid`) and line (`lineid`), with no
//! trip ids or delays. We derive both by matching each prediction against the
//! GTFS static schedule held in the graph (see [`crate::structures::graph`]'s
//! `stib_*` methods and `best_match`).
//!
//! `expectedArrivalTime` is RFC3339 carrying the Brussels offset, so its
//! `naive_local()` is the local wall-clock directly — no timezone DB needed.
//! Predictions are matched against both the current and previous service day so
//! after-midnight trips (owned by the previous day's service) are found.
//!
//! **Match quality is validated against live data**, not unit tests; the tests
//! here cover the parse + matching mechanics on synthetic schedules.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike};
use serde::Deserialize;

use crate::ingestion::gtfs::date_to_days;
use crate::ingestion::realtime::fetcher::Fetcher;
use crate::ingestion::realtime::{RealtimeFeed, TripDelay};
use crate::structures::{Graph, MatchParams, ScheduledArrival, best_match};

pub struct StibFeed {
    name: String,
    waiting_time_url: String,
    headers: HashMap<String, String>,
    graph: Arc<Graph>,
    params: MatchParams,
}

impl StibFeed {
    pub fn new(
        name: String,
        waiting_time_url: String,
        headers: HashMap<String, String>,
        graph: Arc<Graph>,
    ) -> Self {
        Self {
            name,
            waiting_time_url,
            headers,
            graph,
            params: MatchParams::default(),
        }
    }
}

// ---- waiting-times JSON shapes (STIB Open Data) ----

#[derive(Deserialize)]
struct WaitingTimes {
    results: Vec<PointLine>,
}

#[derive(Deserialize)]
struct PointLine {
    pointid: String,
    lineid: String,
    /// A JSON-encoded string (escaped) holding an array of `Passage`.
    passingtimes: String,
}

#[derive(Deserialize)]
struct Passage {
    #[serde(rename = "expectedArrivalTime")]
    expected_arrival_time: Option<String>,
}

impl RealtimeFeed for StibFeed {
    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&self, fetcher: &Fetcher) -> Result<Vec<TripDelay>, String> {
        let bytes = fetcher.get(&self.waiting_time_url, &self.headers)?;
        let wt: WaitingTimes =
            serde_json::from_slice(&bytes).map_err(|e| format!("parsing STIB JSON: {e}"))?;

        let mut out = Vec::new();
        let mut matched = 0usize;
        let mut unmatched = 0usize;

        for row in &wt.results {
            let passages: Vec<Passage> = match serde_json::from_str(&row.passingtimes) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let stops = self.graph.stib_stop_indices(&row.pointid);
            if stops.is_empty() {
                continue;
            }

            for p in &passages {
                let Some(exp) = p.expected_arrival_time.as_deref() else {
                    continue;
                };
                let Ok(dt) = DateTime::parse_from_rfc3339(exp) else {
                    continue;
                };
                // RFC3339 offset is Brussels-local, so naive_local() is wall-clock.
                let local = dt.naive_local();

                match self.match_passage(&stops, &row.lineid, local) {
                    Some((trip_id, stop_id, delay)) => {
                        matched += 1;
                        out.push(TripDelay {
                            trip_id,
                            stop_id: Some(stop_id),
                            stop_sequence: None,
                            delay,
                        });
                    }
                    None => unmatched += 1,
                }
            }
        }
        tracing::info!(feed = %self.name, matched, unmatched, "polled STIB waiting-times");
        Ok(out)
    }
}

impl StibFeed {
    /// Match one predicted passage (local wall-clock) against the schedule across
    /// the candidate `stops` for `line`, trying the current and previous service
    /// day. Returns `(gtfs_trip_id, gtfs_stop_id, delay_secs)` on the best match.
    fn match_passage(
        &self,
        stops: &[usize],
        line: &str,
        local: chrono::NaiveDateTime,
    ) -> Option<(String, String, i32)> {
        let secs_in_day = local.time().num_seconds_from_midnight() as i64;
        let day = local.date();

        // Match against the prediction's own service day, then the previous day
        // (predicted time treated as +86400 past that day's midnight) so
        // after-midnight trips owned by the previous day's service are found.
        let same_day = self.match_on_day(stops, line, day, secs_in_day);
        let prev_day =
            self.match_on_day(stops, line, day - Duration::days(1), secs_in_day + 86_400);

        // Prefer the match with the smaller absolute delay.
        let chosen = match (same_day, prev_day) {
            (Some(a), Some(b)) => {
                if a.2.abs() <= b.2.abs() { Some(a) } else { Some(b) }
            }
            (a, b) => a.or(b),
        }?;

        let (trip, stop, delay) = chosen;
        let trip_id = self.graph.trip_id_str(trip)?.to_string();
        let stop_id = self.graph.stop_id_str(stop)?.to_string();
        Some((trip_id, stop_id, delay))
    }

    /// Best `(trip, stop, delay)` match for `line` across `stops` on one service
    /// day, given the predicted arrival in seconds since that day's midnight.
    fn match_on_day(
        &self,
        stops: &[usize],
        line: &str,
        service_date: NaiveDate,
        predicted_secs: i64,
    ) -> Option<(crate::ingestion::gtfs::TripId, usize, i32)> {
        let date = date_to_days(service_date);
        let weekday = 1u8 << service_date.weekday().num_days_from_monday();

        let mut all: Vec<(usize, ScheduledArrival)> = Vec::new();
        for &stop in stops {
            for sa in self.graph.stib_scheduled_arrivals(stop, line, date, weekday) {
                all.push((stop, sa));
            }
        }
        let sched: Vec<ScheduledArrival> = all.iter().map(|(_, sa)| *sa).collect();
        let (trip, delay) = best_match(&sched, predicted_secs, &self.params)?;
        let stop = all
            .iter()
            .find(|(_, sa)| sa.trip == trip)
            .map(|(s, _)| *s)
            .unwrap_or(stops[0]);
        Some((trip, stop, delay))
    }
}
