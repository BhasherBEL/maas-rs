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

use crate::ingestion::realtime::fetcher::{FetchError, Fetcher, RateLimitConfig};
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

/// Parent-station identity of a GTFS `stop_id`: the prefix before the last `_`
/// (the platform-code separator used by SNCB et al.), or the whole id when there
/// is no `_`. Lets a platform-level RT stop and a parent-level RT stop both match
/// the trip's scheduled platform for the same physical station.
fn parent_prefix(stop_id: &str) -> &str {
    stop_id.rsplit_once('_').map(|(p, _)| p).unwrap_or(stop_id)
}

/// Ordered scheduled compact stops for every trip, keyed by internal `TripId`.
/// Built once per poll cycle from the graph's patterns (each trip belongs to
/// exactly one pattern in RAPTOR). Empty when the graph has no patterns (e.g.
/// synthetic test graphs), in which case callers fall back to direct id lookup.
fn trip_scheduled_stops(
    graph: &Graph,
) -> HashMap<crate::ingestion::gtfs::TripId, std::rc::Rc<Vec<u32>>> {
    let r = &graph.raptor;
    let mut map = HashMap::new();
    for p in 0..r.transit_patterns.len() {
        let stops: std::rc::Rc<Vec<u32>> = std::rc::Rc::new(
            r.transit_idx_pattern_stops[p]
                .of(&r.transit_pattern_stops)
                .iter()
                .map(|n| r.transit_node_to_stop[n.0])
                .collect(),
        );
        for &trip in r.transit_idx_pattern_trips[p].of(&r.transit_pattern_trips) {
            map.insert(trip, stops.clone());
        }
    }
    map
}

/// Resolve an RT `stop_id` to the compact stop the router actually queries: the
/// trip's SCHEDULED pattern stop. Prefers a direct id match that lies on the
/// trip's pattern; otherwise (a platform reassignment, or a parent-level RT id)
/// falls back to the scheduled platform of the same parent station. Returns
/// `None` when neither the direct stop nor a same-station scheduled stop exists.
fn resolve_scheduled_compact(graph: &Graph, sched_stops: &[u32], rt_stop_id: &str) -> Option<u32> {
    if let Some(c) = graph.stop_index_of(rt_stop_id) {
        let c = c as u32;
        if sched_stops.contains(&c) {
            return Some(c);
        }
    }
    let rt_parent = parent_prefix(rt_stop_id);
    sched_stops.iter().copied().find(|&c| {
        graph
            .raptor
            .transit_stop_ids
            .get(c as usize)
            .map(|s| parent_prefix(s))
            == Some(rt_parent)
    })
}

/// Fold a poll cycle's observations into a `RealtimeIndex`, resolving GTFS string
/// ids to internal `(TripId, compact_stop)` keys via the graph. Delays whose trip
/// or stop is unknown to the graph are dropped; canceled trips are resolved by
/// trip id alone (cancellation is stop-independent). Service alerts are kept
/// verbatim (no resolution needed — the liveRefresh resolver matches by GTFS
/// string ids directly). Platform swaps are recorded when the RT actual stop_id
/// is a platform-level stop (contains `_`) that resolves to a known compact stop.
///
/// Delays and skips are keyed at the trip's SCHEDULED pattern stop (bug 8): the
/// router and live-refresh look them up at the scheduled leg/pattern stop, so a
/// delay reported at a reassigned platform (or a parent-level RT id) must land on
/// the scheduled stop, not the actual one, or it is silently unused.
///
/// Each listed delay is also forward-filled to every subsequent scheduled stop
/// until the next listed update (bug 7), per the GTFS-RT spec, so an alighting
/// stop the feed did not explicitly list still reports the trip's known delay.
pub fn build_index(graph: &Graph, update: &FeedUpdate, generated_at: i64) -> RealtimeIndex {
    let trip_stops = trip_scheduled_stops(graph);

    // Resolve one RT (trip_id, stop_id) to (TripId, scheduled compact stop). When
    // the trip has a known pattern, resolve to its scheduled stop (handles platform
    // swaps / parent-level ids); otherwise (pattern-less test graphs) fall back to
    // a direct id lookup so behaviour is unchanged there.
    let resolve = |trip_id: &str, stop_id: &str| -> Option<(crate::ingestion::gtfs::TripId, u32)> {
        let trip = graph.trip_index_of(trip_id)?;
        let compact = match trip_stops.get(&trip) {
            Some(sched) => resolve_scheduled_compact(graph, sched, stop_id)?,
            None => graph.stop_index_of(stop_id)? as u32,
        };
        Some((trip, compact))
    };

    use crate::ingestion::gtfs::TripId;
    use std::collections::HashSet;

    // Skips resolved to scheduled pattern stops (bug 6/8): guard the router AND
    // break the forward-fill chain below (bug 7).
    let skipped: HashSet<(TripId, u32)> = update
        .skipped_stops
        .iter()
        .filter_map(|(trip_id, stop_id)| resolve(trip_id, stop_id))
        .collect();

    // Explicit per-stop delays the feed listed, grouped by trip, keyed at the
    // scheduled compact stop.
    let mut explicit: HashMap<TripId, HashMap<u32, i32>> = HashMap::new();
    let mut dropped = 0usize;
    for d in &update.delays {
        match d.stop_id.as_deref().and_then(|s| resolve(&d.trip_id, s)) {
            Some((trip, compact)) => {
                explicit.entry(trip).or_default().insert(compact, d.delay);
            }
            None => dropped += 1,
        }
    }
    if dropped > 0 {
        // Makes the per-cycle "delays we could not attribute to any scheduled
        // stop" gap visible instead of vanishing silently.
        tracing::debug!(
            dropped,
            total = update.delays.len(),
            "realtime delays dropped: no resolvable scheduled stop"
        );
    }

    // Forward-fill (bug 7): a StopTimeUpdate's delay applies to every subsequent
    // scheduled stop until the next update. Walk each trip's pattern stops in
    // order, carrying the nearest preceding explicit delay forward. A SKIPPED stop
    // breaks the chain — the trip does not serve it, so we neither emit a delay
    // there nor forward-fill THROUGH it as if served; downstream stays unknown
    // until the next explicit update. Stops before the first update stay unknown.
    let mut entries: Vec<((TripId, u32), i32)> = Vec::new();
    for (&trip, per_stop) in &explicit {
        match trip_stops.get(&trip) {
            Some(sched) => {
                let mut current: Option<i32> = None;
                for &compact in sched.iter() {
                    if skipped.contains(&(trip, compact)) {
                        current = None;
                        continue;
                    }
                    if let Some(&d) = per_stop.get(&compact) {
                        current = Some(d);
                    }
                    if let Some(d) = current {
                        entries.push(((trip, compact), d));
                    }
                }
            }
            None => {
                // Pattern-less (synthetic) graph: no order to fill along; emit the
                // explicit delays verbatim (unchanged behaviour).
                for (&compact, &d) in per_stop {
                    entries.push(((trip, compact), d));
                }
            }
        }
    }

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
    .with_skipped(skipped)
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

/// Outcome tallies of one poll cycle, so the caller can distinguish "publish a
/// fresh index" from "every feed failed" from "everything was skipped because we
/// are backing off a gateway throttle" (which must stay silent, not warn).
#[derive(Default)]
struct CycleStats {
    succeeded: u32,
    failed: u32,
    throttled: u32,
}

/// Poll every feed once. Returns the folded index and per-outcome counts. A
/// throttle skip (the fetcher is backing off a 403/429) is neither a success nor
/// a logged failure — the fetcher already logged the backoff once, so the poller
/// stays quiet to avoid a per-cycle error storm.
fn poll_cycle(
    graph: &Graph,
    feeds: &[Box<dyn RealtimeFeed>],
    fetcher: &Fetcher,
) -> (RealtimeIndex, CycleStats) {
    let mut all = FeedUpdate::default();
    let mut stats = CycleStats::default();
    for feed in feeds {
        match feed.poll(fetcher) {
            Ok(mut update) => {
                stats.succeeded += 1;
                all.delays.append(&mut update.delays);
                all.canceled.append(&mut update.canceled);
                all.positions.append(&mut update.positions);
                all.alerts.append(&mut update.alerts);
                all.actual_stops.append(&mut update.actual_stops);
                all.skipped_stops.append(&mut update.skipped_stops);
            }
            Err(FetchError::Throttled) => stats.throttled += 1,
            Err(FetchError::Failed(e)) => {
                stats.failed += 1;
                tracing::error!(feed = feed.name(), "realtime poll failed: {e}");
            }
        }
    }
    let now = chrono::Utc::now().timestamp();
    (build_index(graph, &all, now), stats)
}

/// Fold one cycle's live delays into the persistent sticky cache and evict stale
/// entries. Each observed `(trip, stop)` is upserted with `last_seen = now`
/// (a fresh live sighting refreshes retention, even for delay 0); then every
/// entry older than `ttl` is dropped. Pure so the retention/TTL behaviour is
/// unit-testable without the poll loop. The cache is the source of truth copied
/// into each published index's `sticky_delays` for the live-refresh overlay.
fn merge_sticky(
    cache: &mut HashMap<(crate::ingestion::gtfs::TripId, u32), (i32, i64)>,
    live: impl Iterator<Item = ((crate::ingestion::gtfs::TripId, u32), i32)>,
    now: i64,
    ttl: i64,
) {
    for (key, delay) in live {
        cache.insert(key, (delay, now));
    }
    cache.retain(|_, (_, seen)| now.saturating_sub(*seen) <= ttl);
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
            consecutive_failure_threshold: cfg.rate_limit.consecutive_failure_threshold,
            throttled_min_interval: Duration::from_secs(cfg.rate_limit.throttled_min_interval_secs),
        },
        Duration::from_secs(cfg.request_timeout_secs),
    ));
    let interval = Duration::from_secs(cfg.poll_interval_secs.max(1));
    let (per_min, per_day) = cfg.request_rate();
    // Staleness TTL stamped onto every published snapshot; the routing consumer
    // boundary drops a snapshot older than this so a feed outage cannot serve
    // stale delays/cancellations indefinitely (bug #2).
    let index_max_age = cfg.index_max_age_secs as i64;
    // Retention TTL for the sticky last-known-delay cache of TRACKED journeys.
    let sticky_ttl = cfg.tracked_delay_ttl_secs as i64;
    tracing::info!(
        feeds = feeds.len(),
        interval_secs = cfg.poll_interval_secs,
        req_per_min = per_min,
        req_per_day = per_day,
        index_max_age_secs = cfg.index_max_age_secs,
        "realtime poller started"
    );
    if !cfg.within_quota() {
        tracing::warn!(
            req_per_min = per_min,
            req_per_day = per_day,
            max_per_min = cfg.rate_limit.max_requests_per_min,
            max_per_day = cfg.rate_limit.max_requests_per_day,
            "realtime cadence exceeds the documented gateway quota; expect 403 \
             backoff. Increase poll_interval_secs."
        );
    }

    tokio::spawn(async move {
        // Persistent across poll cycles: last-known live delay per (trip, stop) for
        // TRACKED journeys, copied into every published index's sticky field.
        let mut sticky_cache: HashMap<(crate::ingestion::gtfs::TripId, u32), (i32, i64)> =
            HashMap::new();
        loop {
            let graph_snapshot = graph.load_full();
            let feeds_c = feeds.clone();
            let fetcher_c = fetcher.clone();
            let result = tokio::task::spawn_blocking(move || {
                poll_cycle(&graph_snapshot, &feeds_c, &fetcher_c)
            })
            .await;

            match result {
                Ok((index, stats)) if stats.succeeded > 0 => {
                    let now = index.generated_at;
                    merge_sticky(&mut sticky_cache, index.iter_delays(), now, sticky_ttl);
                    let index = index
                        .with_max_age_secs(index_max_age)
                        .with_sticky_delays(sticky_cache.clone());
                    let delays = index.len();
                    let canceled = index.canceled_len();
                    let positions = index.positions_len();
                    let alerts = index.alerts_len();
                    let sticky = index.sticky_len();
                    realtime.store(Arc::new(index));
                    tracing::info!(
                        delays,
                        canceled,
                        positions,
                        alerts,
                        sticky,
                        "realtime index updated"
                    );
                }
                Ok((_, stats)) if stats.failed > 0 => {
                    tracing::warn!("all realtime feeds failed; keeping last good index");
                }
                Ok((_, _)) => {}
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
    fn merge_sticky_retains_delay_after_feed_drops_it() {
        let ttl = 86_400i64;
        let mut cache: HashMap<(TripId, u32), (i32, i64)> = HashMap::new();

        // Cycle 1 (now=1000): the feed reports trip 1 @ stop 2 delayed 120.
        let idx1 = RealtimeIndex::from_delays(1_000, [((TripId(1), 2), 120)]);
        merge_sticky(&mut cache, idx1.iter_delays(), 1_000, ttl);
        assert_eq!(cache.get(&(TripId(1), 2)), Some(&(120, 1_000)));

        // Cycle 2 (now=1060): the user has boarded; the feed no longer reports this
        // (trip, stop). The sticky cache must still carry it, unchanged, within TTL.
        let idx2 = RealtimeIndex::new(); // empty live index
        merge_sticky(&mut cache, idx2.iter_delays(), 1_060, ttl);
        assert_eq!(
            cache.get(&(TripId(1), 2)),
            Some(&(120, 1_000)),
            "retained delay survives a cycle where the feed dropped it"
        );

        // Publishing that index exposes the sticky delay to live-refresh only.
        let published = idx2.with_sticky_delays(cache.clone());
        assert!(published.is_empty(), "sticky-only publish is empty for routing");
        assert_eq!(published.delay_with_sticky(TripId(1), 2), 120);
    }

    #[test]
    fn merge_sticky_evicts_entries_older_than_ttl() {
        let ttl = 100i64;
        let mut cache: HashMap<(TripId, u32), (i32, i64)> = HashMap::new();

        let idx1 = RealtimeIndex::from_delays(1_000, [((TripId(1), 0), 60)]);
        merge_sticky(&mut cache, idx1.iter_delays(), 1_000, ttl);
        assert!(cache.contains_key(&(TripId(1), 0)));

        // A later cycle within TTL keeps it; a fresh live sighting refreshes last_seen.
        merge_sticky(&mut cache, std::iter::empty(), 1_050, ttl);
        assert!(cache.contains_key(&(TripId(1), 0)), "1050-1000=50 <= 100 TTL");

        // A cycle past TTL evicts the stale entry.
        merge_sticky(&mut cache, std::iter::empty(), 1_101, ttl);
        assert!(
            !cache.contains_key(&(TripId(1), 0)),
            "1101-1000=101 > 100 TTL → evicted"
        );
    }

    #[test]
    fn merge_sticky_refreshes_last_seen_on_relive() {
        let ttl = 100i64;
        let mut cache: HashMap<(TripId, u32), (i32, i64)> = HashMap::new();
        merge_sticky(
            &mut cache,
            std::iter::once(((TripId(1), 0), 60)),
            1_000,
            ttl,
        );
        // Re-seen at 1080 with a new delay refreshes both value and last_seen.
        merge_sticky(
            &mut cache,
            std::iter::once(((TripId(1), 0), 90)),
            1_080,
            ttl,
        );
        assert_eq!(cache.get(&(TripId(1), 0)), Some(&(90, 1_080)));
        // Now survives until 1180 rather than 1100.
        merge_sticky(&mut cache, std::iter::empty(), 1_170, ttl);
        assert!(cache.contains_key(&(TripId(1), 0)), "refreshed last_seen extends TTL");
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
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
            skipped_stops: Vec::new(),
        };

        let idx = build_index(&g, &update, 1_000);
        assert_eq!(idx.alerts_len(), 1, "stored verbatim regardless of period");
        let now_after_expiry = 1_000_000u64;
        let found: Vec<_> = idx
            .alerts_for_leg("t0", "s0", "s5", None, now_after_expiry)
            .collect();
        assert!(found.is_empty(), "accessor filters out expired alerts");
    }

    /// A one-pattern graph whose scheduled stops are `OTHER_3` (compact 2) then
    /// `GARE_11` (compact 0). `GARE_8` (compact 1) is a *different* platform of the
    /// same parent station `GARE`, NOT on the pattern — it models the RT feed
    /// reporting a reassigned platform. `GARE_11` is the LAST scheduled stop so a
    /// delay resolved onto it has no downstream stop to forward-fill, keeping the
    /// keying tests focused on resolution alone.
    fn pattern_graph() -> Graph {
        use crate::ingestion::gtfs::RouteId;
        use crate::structures::NodeID;
        use crate::structures::raptor::{Lookup, PatternInfo};

        let mut g = Graph::new();
        g.raptor.transit_stop_ids =
            vec!["GARE_11".into(), "GARE_8".into(), "OTHER_3".into()];
        g.raptor.transit_trip_ids = vec!["t0".into()];
        // Identity node→compact so pattern NodeIDs index straight to compact stops.
        g.raptor.transit_node_to_stop = vec![0, 1, 2];
        // Order: OTHER_3 (compact 2) → GARE_11 (compact 0).
        g.raptor.transit_pattern_stops = vec![NodeID(2), NodeID(0)];
        g.raptor.transit_idx_pattern_stops = vec![Lookup { start: 0, len: 2 }];
        g.raptor.transit_pattern_trips = vec![TripId(0)];
        g.raptor.transit_idx_pattern_trips = vec![Lookup { start: 0, len: 1 }];
        g.raptor.transit_patterns = vec![PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        }];
        g.raptor.build_runtime_indices();
        g
    }

    /// A one-pattern, one-trip graph with six scheduled stops (compact 1..=6, in
    /// order) for exercising forward-fill and skip-breaks-chain (bug 7). Stop
    /// `stop_k` has compact index `k`.
    fn six_stop_graph() -> Graph {
        use crate::ingestion::gtfs::RouteId;
        use crate::structures::NodeID;
        use crate::structures::raptor::{Lookup, PatternInfo};

        let mut g = Graph::new();
        // compact 0 unused as a stop; stops 1..=6.
        g.raptor.transit_stop_ids = (0..=6).map(|k| format!("stop_{k}")).collect();
        g.raptor.transit_trip_ids = vec!["t0".into()];
        g.raptor.transit_node_to_stop = (0..=6).collect();
        g.raptor.transit_pattern_stops = (1..=6).map(NodeID).collect();
        g.raptor.transit_idx_pattern_stops = vec![Lookup { start: 0, len: 6 }];
        g.raptor.transit_pattern_trips = vec![TripId(0)];
        g.raptor.transit_idx_pattern_trips = vec![Lookup { start: 0, len: 1 }];
        g.raptor.transit_patterns = vec![PatternInfo {
            route: RouteId(0),
            num_trips: 1,
        }];
        g.raptor.build_runtime_indices();
        g
    }

    fn delay_at(trip: &str, stop: &str, delay: i32) -> TripDelay {
        TripDelay {
            trip_id: trip.into(),
            stop_id: Some(stop.into()),
            stop_sequence: None,
            delay,
        }
    }

    #[test]
    fn build_index_forward_fills_delay_downstream_of_last_update() {
        let g = six_stop_graph();
        // A single update at scheduled stop 3 (+900). Stops 4,5,6 must inherit it;
        // stops 1,2 (before the update) stay unknown (0).
        let update = FeedUpdate {
            delays: vec![delay_at("t0", "stop_3", 900)],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.delay(TripId(0), 1), 0, "stop before the update stays unknown");
        assert_eq!(idx.delay(TripId(0), 2), 0, "stop before the update stays unknown");
        assert_eq!(idx.delay(TripId(0), 3), 900, "the explicit update stop");
        assert_eq!(idx.delay(TripId(0), 4), 900, "forward-filled");
        assert_eq!(idx.delay(TripId(0), 5), 900, "forward-filled");
        assert_eq!(idx.delay(TripId(0), 6), 900, "forward-filled to the last stop");
        // stops 3..=6 carry the delay: four entries.
        assert_eq!(idx.len(), 4);
    }

    #[test]
    fn build_index_forward_fill_stops_at_next_update() {
        let g = six_stop_graph();
        // Two updates: +300 at stop 2, +60 at stop 5. Fill: 2,3,4 → 300; 5,6 → 60.
        let update = FeedUpdate {
            delays: vec![delay_at("t0", "stop_2", 300), delay_at("t0", "stop_5", 60)],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.delay(TripId(0), 1), 0);
        assert_eq!(idx.delay(TripId(0), 2), 300);
        assert_eq!(idx.delay(TripId(0), 3), 300, "filled from stop 2");
        assert_eq!(idx.delay(TripId(0), 4), 300, "filled from stop 2");
        assert_eq!(idx.delay(TripId(0), 5), 60, "next update overrides the fill");
        assert_eq!(idx.delay(TripId(0), 6), 60, "filled from stop 5");
    }

    #[test]
    fn build_index_skipped_stop_breaks_the_fill_chain() {
        let g = six_stop_graph();
        // +900 at stop 3; stop 4 is SKIPPED. The chain must break: stop 4 carries no
        // delay, and stops 5,6 downstream of the skip stay unknown (not filled with
        // the pre-skip +900 as if the trip still ran through them).
        let update = FeedUpdate {
            delays: vec![delay_at("t0", "stop_3", 900)],
            skipped_stops: vec![("t0".into(), "stop_4".into())],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.delay(TripId(0), 3), 900);
        assert_eq!(idx.delay(TripId(0), 4), 0, "skipped stop carries no delay");
        assert_eq!(idx.delay(TripId(0), 5), 0, "chain broken by the skip");
        assert_eq!(idx.delay(TripId(0), 6), 0, "chain broken by the skip");
        assert!(idx.is_skipped(TripId(0), 4), "the skip is still recorded for routing");
    }

    #[test]
    fn build_index_keys_platform_swapped_delay_on_scheduled_stop() {
        let g = pattern_graph();
        let sched = g.stop_index_of("GARE_11").unwrap() as u32; // = 0

        // RT reports the delay at GARE_8 (reassigned platform, not on the pattern).
        let update = FeedUpdate {
            delays: vec![TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("GARE_8".into()),
                stop_sequence: None,
                delay: 240,
            }],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(
            idx.delay(TripId(0), sched),
            240,
            "delay must land on the SCHEDULED compact stop the router queries"
        );
        assert_eq!(
            idx.delay(TripId(0), 1),
            0,
            "nothing keyed at the actual (reassigned) platform"
        );
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn build_index_keys_parent_level_rt_id_on_scheduled_platform() {
        let g = pattern_graph();
        let sched = g.stop_index_of("GARE_11").unwrap() as u32;

        // RT reports a PARENT-level id (`GARE`, no platform suffix) while the graph
        // stores platform-level stops. It must still resolve to the scheduled stop.
        let update = FeedUpdate {
            delays: vec![TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("GARE".into()),
                stop_sequence: None,
                delay: 300,
            }],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.delay(TripId(0), sched), 300);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn build_index_drops_delay_with_no_resolvable_stop() {
        let g = pattern_graph();
        // A stop of a wholly different station the trip never serves → unresolvable.
        let update = FeedUpdate {
            delays: vec![TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("ELSEWHERE_2".into()),
                stop_sequence: None,
                delay: 120,
            }],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.len(), 0, "unresolvable delay is dropped (and counted)");
    }

    #[test]
    fn build_index_direct_scheduled_stop_is_unchanged() {
        let g = pattern_graph();
        let sched = g.stop_index_of("OTHER_3").unwrap() as u32; // = 2, on the pattern
        // Common case: RT reports the scheduled stop directly → same compact key.
        let update = FeedUpdate {
            delays: vec![TripDelay {
                trip_id: "t0".into(),
                stop_id: Some("OTHER_3".into()),
                stop_sequence: None,
                delay: 90,
            }],
            ..Default::default()
        };
        let idx = build_index(&g, &update, 0);
        assert_eq!(idx.delay(TripId(0), sched), 90);
    }

    #[test]
    fn build_index_resolves_skipped_stops_and_stays_inert_when_empty() {
        let g = base_graph();

        // A skip for a known trip+stop resolves; unknown trip / unknown stop drop.
        let update = FeedUpdate {
            delays: Vec::new(),
            canceled: Vec::new(),
            positions: Vec::new(),
            alerts: Vec::new(),
            actual_stops: Vec::new(),
            skipped_stops: vec![
                ("t0".into(), "s5".into()),   // resolves → (TripId(0), compact 1)
                ("ghost".into(), "s5".into()), // unknown trip → dropped
                ("t0".into(), "nope".into()),  // unknown stop → dropped
            ],
        };
        let idx = build_index(&g, &update, 7);
        assert_eq!(idx.skipped_len(), 1, "only the resolvable skip is kept");
        assert!(idx.is_skipped(TripId(0), 1), "s5 is compact index 1");
        assert!(!idx.is_skipped(TripId(1), 1), "unknown trip not skipped");
        assert!(
            !idx.is_empty(),
            "a resolvable skip makes the index non-empty so routing consults it"
        );

        // No skips at all → inert default: is_skipped false everywhere, empty.
        let empty = build_index(&g, &FeedUpdate::default(), 0);
        assert_eq!(empty.skipped_len(), 0);
        assert!(!empty.is_skipped(TripId(0), 1));
        assert!(empty.is_empty());
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
            skipped_stops: Vec::new(),
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
