//! Background realtime poller.
//!
//! Builds the configured realtime feeds, then on an interval polls them all,
//! folds their [`TripDelay`]s into a fresh [`RealtimeIndex`] (resolving GTFS
//! string ids to internal indices via the live graph), and atomically swaps it
//! into a shared `ArcSwap`. Per-feed failures are isolated; a cycle where *every*
//! feed fails keeps the last good index rather than clearing delays.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::ingestion::realtime::fetcher::{Fetcher, RateLimitConfig};
use crate::ingestion::realtime::gtfs_rt::GtfsRtFeed;
use crate::ingestion::realtime::stib::StibFeed;
use crate::ingestion::realtime::{RealtimeFeed, TripDelay};
use crate::services::scheduler::SharedGraph;
use crate::structures::{Config, Graph, RealtimeConfig, RealtimeFeedConfig, RealtimeIndex};

pub type SharedRealtime = Arc<ArcSwap<RealtimeIndex>>;

/// Build the feed objects from config. STIB feeds need the graph (for schedule
/// matching) and so are constructed with a clone of the current graph snapshot.
pub fn build_feeds(cfg: &RealtimeConfig, graph: Arc<Graph>) -> Vec<Box<dyn RealtimeFeed>> {
    cfg.feeds
        .iter()
        .map(|f| match f {
            RealtimeFeedConfig::GtfsRt { name, url, headers } => Box::new(GtfsRtFeed::new(
                name.clone(),
                url.clone(),
                headers.clone(),
            )) as Box<dyn RealtimeFeed>,
            RealtimeFeedConfig::Stib { name, waiting_time_url, headers } => {
                Box::new(StibFeed::new(
                    name.clone(),
                    waiting_time_url.clone(),
                    headers.clone(),
                    graph.clone(),
                )) as Box<dyn RealtimeFeed>
            }
        })
        .collect()
}

/// Fold a poll cycle's delays into a `RealtimeIndex`, resolving GTFS string ids
/// to internal `(TripId, compact_stop)` keys via the graph. Delays whose trip or
/// stop is unknown to the graph are dropped.
pub fn build_index(graph: &Graph, delays: &[TripDelay], generated_at: i64) -> RealtimeIndex {
    let entries: Vec<((crate::ingestion::gtfs::TripId, u32), i32)> = delays
        .iter()
        .filter_map(|d| {
            let trip = graph.trip_index_of(&d.trip_id)?;
            let stop = d.stop_id.as_deref().and_then(|s| graph.stop_index_of(s))?;
            Some(((trip, stop as u32), d.delay))
        })
        .collect();
    RealtimeIndex::from_delays(generated_at, entries)
}

/// Poll every feed once. Returns the folded index and whether at least one feed
/// succeeded (used to decide whether to publish or keep the last good index).
fn poll_cycle(graph: &Graph, feeds: &[Box<dyn RealtimeFeed>], fetcher: &Fetcher) -> (RealtimeIndex, bool) {
    let mut all: Vec<TripDelay> = Vec::new();
    let mut any_success = false;
    for feed in feeds {
        match feed.poll(fetcher) {
            Ok(mut delays) => {
                any_success = true;
                all.append(&mut delays);
            }
            Err(e) => tracing::error!(feed = feed.name(), "realtime poll failed: {e}"),
        }
    }
    let now = chrono::Utc::now().timestamp();
    (build_index(graph, &all, now), any_success)
}

/// Spawn the realtime poller if `realtime` is enabled with at least one feed.
pub fn spawn(graph: SharedGraph, realtime: SharedRealtime, config: Arc<Config>) {
    let cfg = match &config.realtime {
        Some(c) if c.enabled => c.clone(),
        _ => return,
    };
    if cfg.feeds.is_empty() {
        tracing::warn!("realtime enabled but no feeds configured");
        return;
    }

    let feeds = Arc::new(build_feeds(&cfg, graph.load_full()));
    let fetcher = Arc::new(Fetcher::new(
        RateLimitConfig {
            consecutive_429_threshold: cfg.rate_limit.consecutive_429_threshold,
            throttled_min_interval: Duration::from_secs(cfg.rate_limit.throttled_min_interval_secs),
        },
        Duration::from_secs(cfg.request_timeout_secs),
    ));
    let interval = Duration::from_secs(cfg.poll_interval_secs.max(1));
    tracing::info!(feeds = feeds.len(), interval_secs = cfg.poll_interval_secs, "realtime poller started");

    tokio::spawn(async move {
        loop {
            let graph_snapshot = graph.load_full();
            let feeds_c = feeds.clone();
            let fetcher_c = fetcher.clone();
            let result = tokio::task::spawn_blocking(move || {
                poll_cycle(&graph_snapshot, &feeds_c, &fetcher_c)
            })
            .await;

            match result {
                Ok((index, true)) => {
                    let n = index.len();
                    realtime.store(Arc::new(index));
                    tracing::info!(delays = n, "realtime index updated");
                }
                Ok((_, false)) => {
                    tracing::warn!("all realtime feeds failed; keeping last good index");
                }
                Err(e) => tracing::error!("realtime poll cycle panicked: {e}"),
            }

            tokio::time::sleep(interval).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::gtfs::TripId;

    #[test]
    fn build_index_drops_unknown_ids_and_keeps_known() {
        // A graph whose reverse maps know trip "t0" (index 0) and stop "s5".
        let mut g = Graph::new();
        g.raptor.transit_trip_ids = vec!["t0".into(), "t1".into()];
        g.raptor.transit_stop_ids = vec!["s0".into(), "s5".into(), "s9".into()];
        g.raptor.build_runtime_indices();

        let delays = vec![
            TripDelay { trip_id: "t1".into(), stop_id: Some("s9".into()), stop_sequence: None, delay: 120 },
            TripDelay { trip_id: "t0".into(), stop_id: Some("s5".into()), stop_sequence: None, delay: 60 },
            // unknown trip — dropped
            TripDelay { trip_id: "ghost".into(), stop_id: Some("s0".into()), stop_sequence: None, delay: 30 },
            // unknown stop — dropped
            TripDelay { trip_id: "t0".into(), stop_id: Some("nope".into()), stop_sequence: None, delay: 30 },
            // no stop_id — dropped (no stop reference)
            TripDelay { trip_id: "t0".into(), stop_id: None, stop_sequence: Some(2), delay: 30 },
        ];

        let idx = build_index(&g, &delays, 42);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.delay(TripId(1), 2), 120); // "s9" is compact index 2
        assert_eq!(idx.delay(TripId(0), 1), 60); // "s5" is compact index 1
        assert_eq!(idx.generated_at, 42);
    }
}
