//! Generic GTFS-Realtime trip-update feed (SNCB, TEC).
//!
//! These feeds carry an explicit delay per stop-time, keyed by GTFS string
//! `trip_id` and `stop_sequence`, so no schedule join is needed: decode the
//! protobuf and emit one [`TripDelay`] per stop-time update that carries a delay.
//! Stop-time updates that give only an absolute predicted time (no delay) are
//! skipped — turning those into delays needs a schedule join (see STIB).

use std::collections::HashMap;

use prost::Message;

use crate::ingestion::realtime::fetcher::Fetcher;
use crate::ingestion::realtime::proto::FeedMessage;
use crate::ingestion::realtime::{RealtimeFeed, TripDelay};

pub struct GtfsRtFeed {
    name: String,
    url: String,
    headers: HashMap<String, String>,
}

impl GtfsRtFeed {
    pub fn new(name: String, url: String, headers: HashMap<String, String>) -> Self {
        Self { name, url, headers }
    }
}

impl RealtimeFeed for GtfsRtFeed {
    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&self, fetcher: &Fetcher) -> Result<Vec<TripDelay>, String> {
        let bytes = fetcher.get(&self.url, &self.headers)?;
        let delays = parse_trip_updates(&bytes)?;
        tracing::info!(feed = %self.name, delays = delays.len(), "polled GTFS-RT feed");
        Ok(delays)
    }
}

/// Decode a GTFS-RT `FeedMessage` and extract per-stop delays. Pure (no I/O), so
/// it is unit-testable against a fixture protobuf.
pub fn parse_trip_updates(bytes: &[u8]) -> Result<Vec<TripDelay>, String> {
    let feed = FeedMessage::decode(bytes).map_err(|e| format!("decoding GTFS-RT protobuf: {e}"))?;

    let mut out = Vec::new();
    for entity in feed.entity {
        let Some(tu) = entity.trip_update else {
            continue;
        };
        let Some(trip_id) = tu.trip.trip_id.clone() else {
            continue; // cannot attach a delay without a trip id
        };
        for stu in &tu.stop_time_update {
            // Need a stop reference to place the delay; prefer stop_id.
            if stu.stop_id.is_none() && stu.stop_sequence.is_none() {
                continue;
            }
            // Prefer arrival delay (what matters for connections); fall back to
            // departure delay.
            let delay = stu
                .arrival
                .as_ref()
                .and_then(|e| e.delay)
                .or_else(|| stu.departure.as_ref().and_then(|e| e.delay));
            let Some(delay) = delay else {
                continue;
            };
            out.push(TripDelay {
                trip_id: trip_id.clone(),
                stop_id: stu.stop_id.clone(),
                stop_sequence: stu.stop_sequence,
                delay,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::realtime::proto::{
        FeedEntity, FeedHeader, TripUpdate,
        trip_update::{StopTimeEvent, StopTimeUpdate},
    };

    fn encode_feed(entities: Vec<FeedEntity>) -> Vec<u8> {
        let feed = FeedMessage {
            header: FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                incrementality: None,
                timestamp: None,
                feed_version: None,
            },
            entity: entities,
        };
        feed.encode_to_vec()
    }

    fn trip_update_entity(trip_id: &str, updates: Vec<StopTimeUpdate>) -> FeedEntity {
        let mut tu = TripUpdate::default();
        tu.trip.trip_id = Some(trip_id.to_string());
        tu.stop_time_update = updates;
        FeedEntity {
            id: trip_id.to_string(),
            trip_update: Some(tu),
            ..Default::default()
        }
    }

    fn stop_update(seq: u32, arrival_delay: Option<i32>) -> StopTimeUpdate {
        StopTimeUpdate {
            stop_sequence: Some(seq),
            stop_id: Some(format!("stop_{seq}")),
            arrival: arrival_delay.map(|d| StopTimeEvent {
                delay: Some(d),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn parses_explicit_delays() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_x",
            vec![stop_update(1, Some(60)), stop_update(2, Some(180))],
        )]);
        let delays = parse_trip_updates(&bytes).unwrap();
        assert_eq!(
            delays,
            vec![
                TripDelay {
                    trip_id: "trip_x".into(),
                    stop_id: Some("stop_1".into()),
                    stop_sequence: Some(1),
                    delay: 60
                },
                TripDelay {
                    trip_id: "trip_x".into(),
                    stop_id: Some("stop_2".into()),
                    stop_sequence: Some(2),
                    delay: 180
                },
            ]
        );
    }

    #[test]
    fn skips_updates_without_delay() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_y",
            vec![stop_update(1, None), stop_update(5, Some(-30))],
        )]);
        let delays = parse_trip_updates(&bytes).unwrap();
        // Only the stop with a delay survives.
        assert_eq!(
            delays,
            vec![TripDelay {
                trip_id: "trip_y".into(),
                stop_id: Some("stop_5".into()),
                stop_sequence: Some(5),
                delay: -30
            }]
        );
    }

    #[test]
    fn empty_feed_yields_no_delays() {
        let bytes = encode_feed(vec![]);
        assert!(parse_trip_updates(&bytes).unwrap().is_empty());
    }

    #[test]
    fn garbage_bytes_error() {
        assert!(parse_trip_updates(&[0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
