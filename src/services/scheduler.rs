//! Background auto-update: on a cron schedule, re-download remote GTFS feeds,
//! and when their decompressed content changed, rebuild the GTFS phase atop the
//! cached osm.bin and hot-swap the live graph. Failures leave the running graph
//! untouched; the loop is sequential so two rebuilds never overlap.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Local};
use cron::Schedule;

use crate::ingestion::cache::{
    SourceLocation, gtfs_content_hash, load_feed_hashes, load_last_checked, resolve_source,
    save_feed_hashes, save_input_labels, save_last_checked,
};
use crate::services::build::{
    apply_routing_defaults, build_gtfs_phase, finalize_contraction, gtfs_input_labels,
};
use crate::services::persistence::{load_osm_graph, save_graph_with_rollback};
use crate::structures::{Config, Graph, Ingestor};

pub type SharedGraph = Arc<ArcSwap<Graph>>;

/// Spawn the background updater if `auto_update` is enabled. No-op otherwise.
pub fn spawn(graph: SharedGraph, config: Arc<Config>) {
    let au = match &config.auto_update {
        Some(a) if a.enabled => a.clone(),
        _ => return,
    };
    let schedule = match parse_cron(&au.schedule) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("auto_update disabled: {e}");
            return;
        }
    };
    tracing::info!(
        "auto_update enabled (schedule '{}', cache '{}')",
        au.schedule,
        au.cache_dir
    );
    tokio::spawn(async move {
        run_loop(graph, config, schedule, au.cache_dir).await;
    });
}

async fn run_loop(graph: SharedGraph, config: Arc<Config>, schedule: Schedule, cache_dir: String) {
    // Startup catch-up: if a scheduled tick elapsed while the service was down
    // (or no check was ever recorded), refresh once before the normal wait loop.
    let due = match load_last_checked(&cache_dir) {
        Some(last) => feeds_stale(&schedule, last, Local::now()),
        None => true,
    };
    if due {
        tracing::info!("auto_update: feeds stale at startup, running catch-up");
        run_once(&graph, &config, &cache_dir).await;
    }

    loop {
        let Some(next) = schedule.upcoming(chrono::Local).next() else {
            tracing::warn!("auto_update: cron has no future occurrences; stopping");
            return;
        };
        let wait = (next - chrono::Local::now())
            .to_std()
            .unwrap_or(std::time::Duration::from_secs(1));
        tracing::info!("auto_update: next run at {next}");
        tokio::time::sleep(wait).await;

        run_once(&graph, &config, &cache_dir).await;
    }
}

/// Run one update cycle on a blocking thread and log the outcome.
async fn run_once(graph: &SharedGraph, config: &Arc<Config>, cache_dir: &str) {
    let graph_c = graph.clone();
    let config_c = config.clone();
    let cache_c = cache_dir.to_string();
    let result =
        tokio::task::spawn_blocking(move || run_update_cycle(&graph_c, &config_c, &cache_c)).await;
    match result {
        Ok(Ok(true)) => tracing::info!("auto_update: graph updated and swapped"),
        Ok(Ok(false)) => tracing::info!("auto_update: no feed changes"),
        Ok(Err(e)) => tracing::error!("auto_update: cycle failed (keeping current graph): {e}"),
        Err(e) => tracing::error!("auto_update: cycle panicked (keeping current graph): {e}"),
    }
}

/// True if a scheduled fire time falls in `(last_checked, now]` — i.e. at least
/// one refresh tick elapsed since the last successful feed check.
fn feeds_stale(schedule: &Schedule, last_checked: DateTime<Local>, now: DateTime<Local>) -> bool {
    matches!(schedule.after(&last_checked).next(), Some(fire) if fire <= now)
}

/// One update cycle. Returns Ok(true) if a new graph was swapped in.
fn run_update_cycle(graph: &SharedGraph, config: &Config, cache_dir: &str) -> Result<bool, String> {
    let old_hashes = load_feed_hashes(cache_dir);
    let mut new_hashes = old_hashes.clone();

    for input in &config.build.inputs {
        if !is_remote_gtfs(input) {
            continue;
        }
        let path = resolve_source(input, cache_dir, true)?;
        let hash = gtfs_content_hash(&path)?;
        new_hashes.insert(input.label().to_string(), hash);
    }

    // Record the check time regardless of outcome so a quiet period does not
    // make every restart re-pull.
    if let Err(e) = save_last_checked(cache_dir, Local::now()) {
        tracing::warn!("auto_update: failed to persist last_checked: {e}");
    }

    if !any_changed(&old_hashes, &new_hashes) {
        return Ok(false);
    }

    let osm = load_osm_graph(&config.build.osm_output)?;
    // Feeds were just downloaded above (force=true); reuse the cache here.
    let mut new_graph = build_gtfs_phase(
        osm,
        &config.build,
        cache_dir,
        false,
        config.default_routing.station_merge_radius_m,
    )
    .ok_or_else(|| "GTFS rebuild failed".to_string())?;
    apply_routing_defaults(&mut new_graph, &config.default_routing);
    // P3f: drop the interior arrays before persisting + swapping in, else the background
    // refresh silently reverts the memory win and saves a bloated graph.bin.
    finalize_contraction(&mut new_graph)?;

    save_graph_with_rollback(&new_graph, &config.build.output)?;
    save_feed_hashes(cache_dir, &new_hashes)?;
    let labels = gtfs_input_labels(&config.build);
    if let Err(e) = save_input_labels(cache_dir, &labels) {
        tracing::warn!("auto_update: failed to persist input_labels: {e}");
    }
    graph.store(Arc::new(new_graph));
    Ok(true)
}

fn is_remote_gtfs(input: &Ingestor) -> bool {
    matches!(
        input,
        Ingestor::GtfsGeneric(_) | Ingestor::GtfsStib(_) | Ingestor::GtfsSncb(_)
    ) && matches!(input.location(), Ok(SourceLocation::Remote(_)))
}

/// True if any feed hash in `new` differs from (or is absent in) `old`.
fn any_changed(old: &BTreeMap<String, String>, new: &BTreeMap<String, String>) -> bool {
    new.iter().any(|(k, v)| old.get(k) != Some(v))
}

/// Parse a cron expression. Accepts standard 5-field expressions by prepending
/// a "0" seconds field for the `cron` crate (which expects 6–7 fields).
fn parse_cron(expr: &str) -> Result<Schedule, String> {
    let normalized = if expr.split_whitespace().count() == 5 {
        format!("0 {expr}")
    } else {
        expr.to_string()
    };
    Schedule::from_str(&normalized).map_err(|e| format!("invalid cron '{expr}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn parse_cron_accepts_five_field() {
        let sched = parse_cron("0 5 * * *").unwrap();
        let next = sched.upcoming(chrono::Local).next().unwrap();
        use chrono::Timelike;
        assert_eq!(next.hour(), 5);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn parse_cron_accepts_six_field() {
        assert!(parse_cron("0 0 5 * * *").is_ok());
    }

    #[test]
    fn parse_cron_rejects_garbage() {
        assert!(parse_cron("not a cron").is_err());
    }

    #[test]
    fn feeds_stale_detects_elapsed_tick() {
        // Hourly schedule.
        let sched = parse_cron("0 * * * *").unwrap();
        let now = Local::now();

        // Checked 90 minutes ago: at least one top-of-hour tick has passed.
        assert!(feeds_stale(
            &sched,
            now - chrono::Duration::minutes(90),
            now
        ));

        // Checked just now: no tick since (next fire is in the future).
        assert!(!feeds_stale(&sched, now, now));
    }

    #[test]
    fn changed_detects_new_and_modified() {
        let mut old = BTreeMap::new();
        old.insert("a".to_string(), "1".to_string());
        let new = old.clone();
        assert!(!any_changed(&old, &new));
        let mut modified = old.clone();
        modified.insert("a".to_string(), "2".to_string());
        assert!(any_changed(&old, &modified));
        let mut added = old.clone();
        added.insert("b".to_string(), "9".to_string());
        assert!(any_changed(&old, &added));
    }
}
