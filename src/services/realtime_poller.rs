//! Background realtime poller.
//!
//! Builds the configured realtime feeds, then on an interval polls them all,
//! folds their [`TripDelay`]s into a fresh [`RealtimeIndex`] (resolving GTFS
//! string ids to internal indices via the live graph), and atomically swaps it
//! into a shared `ArcSwap`. Per-feed failures are isolated; a cycle where *every*
//! feed fails keeps the last good index rather than clearing delays.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::ingestion::realtime::fetcher::{Fetcher, RateLimitConfig};
use crate::ingestion::realtime::gtfs_rt::GtfsRtFeed;
use crate::ingestion::realtime::stib::StibFeed;
use crate::ingestion::realtime::{FeedUpdate, RealtimeFeed, VehicleObservation};
use crate::services::scheduler::SharedGraph;
use crate::structures::{Config, Graph, RealtimeConfig, RealtimeFeedConfig, RealtimeIndex, VehiclePos};

pub type SharedRealtime = Arc<ArcSwap<RealtimeIndex>>;

/// Build the feed objects from config. STIB feeds need the graph (for schedule
/// matching) and so are constructed with a clone of the current graph snapshot.
pub fn build_feeds(cfg: &RealtimeConfig, graph: Arc<Graph>) -> Vec<Box<dyn RealtimeFeed>> {
    cfg.feeds
        .iter()
        .map(|f| match f {
            RealtimeFeedConfig::GtfsRt { name, url, headers } => {
                Box::new(GtfsRtFeed::new(name.clone(), url.clone(), headers.clone()))
                    as Box<dyn RealtimeFeed>
            }
            RealtimeFeedConfig::Stib {
                name,
                waiting_time_url,
                vehicle_position_url,
                headers,
            } => Box::new(StibFeed::new(
                name.clone(),
                waiting_time_url.clone(),
                vehicle_position_url.clone(),
                headers.clone(),
                graph.clone(),
            )) as Box<dyn RealtimeFeed>,
        })
        .collect()
}

/// Fold a poll cycle's observations into a `RealtimeIndex`, resolving GTFS string
/// ids to internal `(TripId, compact_stop)` keys via the graph. Delays whose trip
/// or stop is unknown to the graph are dropped; canceled trips are resolved by
/// trip id alone (cancellation is stop-independent). Service alerts are kept
/// verbatim (no resolution needed — the liveRefresh resolver matches by GTFS
/// string ids directly). Platform swaps are recorded when the RT actual stop_id
/// is a platform-level stop (contains `_`) that resolves to a known compact stop.
pub fn build_index(graph: &Graph, update: &FeedUpdate, generated_at: i64) -> RealtimeIndex {
    let entries: Vec<((crate::ingestion::gtfs::TripId, u32), i32)> = update
        .delays
        .iter()
        .filter_map(|d| {
            let trip = graph.trip_index_of(&d.trip_id)?;
            let stop = d.stop_id.as_deref().and_then(|s| graph.stop_index_of(s))?;
            Some(((trip, stop as u32), d.delay))
        })
        .collect();
    let canceled = update
        .canceled
        .iter()
        .filter_map(|t| graph.trip_index_of(t));
    let positions = fold_positions(graph, &update.positions);

    let mut platform_swaps = std::collections::HashMap::new();
    for actual in &update.actual_stops {
        let Some((parent, _)) = actual.stop_id.rsplit_once('_') else {
            continue;
        };
        let Some(trip) = graph.trip_index_of(&actual.trip_id) else {
            continue;
        };
        let Some(actual_compact) = graph.stop_index_of(&actual.stop_id) else {
            continue;
        };
        platform_swaps.insert((trip, parent.to_string()), actual_compact as u32);
    }

    RealtimeIndex::with_all(
        generated_at,
        entries,
        canceled,
        positions,
        update.alerts.iter().cloned(),
        platform_swaps,
    )
}

fn fold_positions(
    graph: &Graph,
    observations: &[VehicleObservation],
) -> HashMap<crate::ingestion::gtfs::TripId, VehiclePos> {
    let mut map: HashMap<crate::ingestion::gtfs::TripId, VehiclePos> = HashMap::new();
    for obs in observations {
        let Some(trip) = graph.trip_index_of(&obs.trip_id) else {
            continue;
        };
        let vp = VehiclePos {
            lat: obs.lat,
            lng: obs.lng,
            bearing: obs.bearing,
            current_stop_sequence: obs.current_stop_sequence,
            timestamp: obs.timestamp,
        };
        match map.entry(trip) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(vp);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let keep_new = match (e.get().timestamp, obs.timestamp) {
                    (Some(existing), Some(new)) => new >= existing,
                    _ => true,
                };
                if keep_new {
                    e.insert(vp);
                }
            }
        }
    }
    map
}

/// Poll every feed once. Returns the folded index and whether at least one feed
/// succeeded (used to decide whether to publish or keep the last good index).
fn poll_cycle(
    graph: &Graph,
    feeds: &[Box<dyn RealtimeFeed>],
    fetcher: &Fetcher,
) -> (RealtimeIndex, bool) {
    let mut all = FeedUpdate::default();
    let mut any_success = false;
    for feed in feeds {
        match feed.poll(fetcher) {
            Ok(mut update) => {
                any_success = true;
                all.delays.append(&mut update.delays);
                all.canceled.append(&mut update.canceled);
                all.positions.append(&mut update.positions);
                all.alerts.append(&mut update.alerts);
                all.actual_stops.append(&mut update.actual_stops);
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
    tracing::info!(
        feeds = feeds.len(),
        interval_secs = cfg.poll_interval_secs,
        "realtime poller started"
    );

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
                    let delays = index.len();
                    let canceled = index.canceled_len();
                    let positions = index.positions_len();
                    let alerts = index.alerts_len();
                    realtime.store(Arc::new(index));
                    tracing::info!(delays, canceled, positions, alerts, "realtime index updated");
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
    use crate::ingestion::realtime::{ActualStopId, TripDelay, VehicleObservation};

    fn base_graph() -> Graph {
        let mut g = Graph::new();
        g.raptor.transit_trip_ids = vec!["t0".into(), "t1".into()];
        g.raptor.transit_stop_ids = vec!["s0".into(), "s5".into(), "s9".into()];
        g.raptor.build_runtime_indices();
        g
    }

    #[test]
    fn build_index_drops_unknown_ids_and_keeps_known() {
        let g = base_graph();

        let delays = vec![
            TripDelay {
                trip_id: "t1".into(),
                stop_id: Some("s9".into()),
                stop_sequence: None,
                delay: 120,
            },
            TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("s5".into()),
                stop_sequence: None,
                delay: 60,
            },
            // unknown trip — dropped
            TripDelay {
                trip_id: "ghost".into(),
                stop_id: Some("s0".into()),
                stop_sequence: None,
                delay: 30,
            },
            // unknown stop — dropped
            TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("nope".into()),
                stop_sequence: None,
                delay: 30,
            },
            // no stop_id — dropped (no stop reference)
            TripDelay {
                trip_id: "t0".into(),
                stop_id: None,
                stop_sequence: Some(2),
                delay: 30,
            },
        ];

        let update = FeedUpdate {
            delays,
            canceled: vec!["t1".into(), "ghost".into()],
            positions: Vec::new(),
            alerts: Vec::new(),
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 42);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.delay(TripId(1), 2), 120); // "s9" is compact index 2
        assert_eq!(idx.delay(TripId(0), 1), 60); // "s5" is compact index 1
        assert!(idx.is_canceled(TripId(1)));
        assert_eq!(idx.canceled_len(), 1);
        assert_eq!(idx.generated_at, 42);
        assert_eq!(idx.positions_len(), 0);
    }

    #[test]
    fn build_index_folds_positions_resolves_trip_ids() {
        let g = base_graph();

        let positions = vec![
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 50.845,
                lng: 4.352,
                bearing: Some(90.0),
                current_stop_sequence: Some(1),
                stop_id: None,
                timestamp: Some(1_000),
            },
            // unknown trip — dropped
            VehicleObservation {
                trip_id: "ghost".into(),
                lat: 50.0,
                lng: 4.0,
                bearing: None,
                current_stop_sequence: None,
                stop_id: None,
                timestamp: Some(999),
            },
        ];

        let update = FeedUpdate {
            delays: vec![TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("s5".into()),
                stop_sequence: None,
                delay: 30,
            }],
            canceled: Vec::new(),
            positions,
            alerts: Vec::new(),
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 100);
        assert_eq!(idx.positions_len(), 1, "ghost trip dropped; t0 kept");
        let vp = idx.vehicle(TripId(0)).expect("t0 position should be present");
        assert!((vp.lat - 50.845_f32).abs() < 0.001);
        assert!((vp.lng - 4.352_f32).abs() < 0.001);
        assert_eq!(vp.bearing, Some(90.0));
        assert_eq!(vp.timestamp, Some(1_000));
        assert!(idx.vehicle(TripId(1)).is_none(), "t1 had no observation");
        assert_eq!(idx.delay(TripId(0), 1), 30, "delays are preserved");
    }

    #[test]
    fn build_index_newest_timestamp_wins_on_duplicate_trip() {
        let g = base_graph();

        let positions = vec![
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 50.0,
                lng: 4.0,
                bearing: None,
                current_stop_sequence: None,
                stop_id: None,
                timestamp: Some(500),
            },
            // Same trip, newer timestamp — should win
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 51.0,
                lng: 4.5,
                bearing: Some(45.0),
                current_stop_sequence: Some(3),
                stop_id: None,
                timestamp: Some(1_500),
            },
            // Same trip, older timestamp — should lose
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 49.0,
                lng: 3.5,
                bearing: None,
                current_stop_sequence: None,
                stop_id: None,
                timestamp: Some(200),
            },
        ];

        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions,
            alerts: Vec::new(),
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 200);
        assert_eq!(idx.positions_len(), 1);
        let vp = idx.vehicle(TripId(0)).expect("t0 should have a position");
        assert!((vp.lat - 51.0_f32).abs() < 0.001, "newest timestamp (1500) should win");
        assert_eq!(vp.timestamp, Some(1_500));
    }

    #[test]
    fn build_index_no_timestamp_is_last_write_wins() {
        let g = base_graph();

        let positions = vec![
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 50.0,
                lng: 4.0,
                bearing: None,
                current_stop_sequence: None,
                stop_id: None,
                timestamp: None,
            },
            VehicleObservation {
                trip_id: "t0".into(),
                lat: 51.0,
                lng: 4.5,
                bearing: None,
                current_stop_sequence: None,
                stop_id: None,
                timestamp: None,
            },
        ];

        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions,
            alerts: Vec::new(),
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 0);
        let vp = idx.vehicle(TripId(0)).expect("t0 should have a position");
        assert!((vp.lat - 51.0_f32).abs() < 0.001, "last-write-wins when no timestamp");
    }

    #[test]
    fn build_index_empty_positions_leaves_index_position_free() {
        let g = base_graph();

        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions: Vec::new(),
            alerts: Vec::new(),
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.positions_len(), 0);
        assert!(idx.vehicle(TripId(0)).is_none());
    }

    #[test]
    fn build_index_folds_alerts_verbatim_into_realtime_index() {
        use crate::ingestion::realtime::{AlertEntitySelector, ServiceAlert};

        let g = base_graph();
        let alert = ServiceAlert {
            header: Some("Strike".to_string()),
            description: None,
            cause: Some(4),
            effect: Some(1),
            active_period: vec![(Some(1_000_000), Some(2_000_000))],
            informed_entity: vec![AlertEntitySelector {
                trip_id: Some("t0".to_string()),
                route_id: None,
                stop_id: None,
            }],
        };
        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions: Vec::new(),
            alerts: vec![alert.clone()],
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 500);
        assert_eq!(idx.alerts_len(), 1, "alert must survive the fold");
        let found: Vec<_> = idx.alerts_for_leg("t0", "s0", "s5", None, 1_500_000).collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].header.as_deref(), Some("Strike"));
    }

    #[test]
    fn build_index_expired_alert_still_stored_filtering_done_by_accessor() {
        use crate::ingestion::realtime::{AlertEntitySelector, ServiceAlert};

        let g = base_graph();
        let expired_alert = ServiceAlert {
            header: Some("Old alert".to_string()),
            description: None,
            cause: Some(1),
            effect: Some(8),
            active_period: vec![(Some(0), Some(100))],
            informed_entity: vec![AlertEntitySelector {
                trip_id: Some("t0".to_string()),
                route_id: None,
                stop_id: None,
            }],
        };
        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions: Vec::new(),
            alerts: vec![expired_alert],
            actual_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 1_000);
        assert_eq!(idx.alerts_len(), 1, "stored verbatim regardless of period");
        let now_after_expiry = 1_000_000u64;
        let found: Vec<_> = idx
            .alerts_for_leg("t0", "s0", "s5", None, now_after_expiry)
            .collect();
        assert!(found.is_empty(), "accessor filters out expired alerts");
    }

    #[test]
    fn build_index_captures_platform_swaps_from_actual_stops() {
        let mut g = base_graph();
        // Two platform-level stops for the same parent station "ns_Gare".
        g.raptor.transit_stop_ids.push("ns_Gare_11".into()); // compact 3 — scheduled platform
        g.raptor.transit_stop_ids.push("ns_Gare_8".into());  // compact 4 — actual RT platform
        g.raptor.build_runtime_indices();

        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions: Vec::new(),
            alerts: Vec::new(),
            actual_stops: vec![
                ActualStopId {
                    trip_id: "t0".into(),
                    stop_id: "ns_Gare_8".into(), // RT reports platform 8
                },
            ],
        };

        let idx = build_index(&g, &update, 0);
        let gare_8_compact = g.stop_index_of("ns_Gare_8").unwrap() as u32; // = 4
        assert_eq!(
            idx.platform_swap(TripId(0), "ns_Gare"),
            Some(gare_8_compact),
            "platform-level stop is stored under its parent station key"
        );
        assert!(
            idx.platform_swap(TripId(0), "ns_Gare_8").is_none(),
            "no entry for a parent that was never the prefix of an actual stop"
        );
        assert!(
            idx.platform_swap(TripId(1), "ns_Gare").is_none(),
            "unknown trip → no swap"
        );
    }
}
