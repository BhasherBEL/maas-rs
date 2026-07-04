//! Generic GTFS-Realtime trip-update feed (SNCB, TEC).
//!
//! These feeds carry an explicit delay per stop-time, keyed by GTFS string
//! `trip_id` and `stop_sequence`, so no schedule join is needed: decode the
//! protobuf and emit one [`TripDelay`] per stop-time update that carries a delay.
//! Stop-time updates that give only an absolute predicted time (no delay) are
//! skipped — turning those into delays needs a schedule join (see STIB).

use std::collections::HashMap;

use prost::Message;

use crate::ingestion::realtime::fetcher::{FetchError, Fetcher};
use crate::ingestion::realtime::proto::FeedMessage;
use crate::ingestion::realtime::proto::trip_descriptor::ScheduleRelationship;
use crate::ingestion::realtime::{ActualStopId, AlertEntitySelector, FeedUpdate, RealtimeFeed, ServiceAlert, TripDelay};

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

    fn poll(&self, fetcher: &Fetcher) -> Result<FeedUpdate, FetchError> {
        let bytes = fetcher.get(&self.url, &self.headers)?;
        let update = parse_trip_updates(&bytes)?;
        tracing::info!(
            feed = %self.name,
            delays = update.delays.len(),
            canceled = update.canceled.len(),
            "polled GTFS-RT feed"
        );
        Ok(update)
    }
}

/// Extract the first available translation text from a `TranslatedString`,
/// preferring English if present; otherwise returns the first entry.
fn first_translation(ts: &crate::ingestion::realtime::proto::TranslatedString) -> Option<String> {
    if ts.translation.is_empty() {
        return None;
    }
    ts.translation
        .iter()
        .find(|t| t.language.as_deref() == Some("en"))
        .or_else(|| ts.translation.first())
        .map(|t| t.text.clone())
}

/// Decode a GTFS-RT `FeedMessage` into per-stop delays, CANCELED trip ids, and
/// service alerts. Pure (no I/O), so it is unit-testable against a fixture protobuf.
///
/// Before this change, `entity.alert` was silently skipped via the
/// `let Some(tu) = entity.trip_update else { continue }` guard. Now alerts are
/// parsed in a second pass over the same entities.
pub fn parse_trip_updates(bytes: &[u8]) -> Result<FeedUpdate, String> {
    let feed = FeedMessage::decode(bytes).map_err(|e| format!("decoding GTFS-RT protobuf: {e}"))?;

    use crate::ingestion::realtime::proto::trip_update::stop_time_update::ScheduleRelationship as StopRel;

    let mut out = Vec::new();
    let mut canceled = Vec::new();
    let mut alerts = Vec::new();
    let mut actual_stops = Vec::new();
    let mut skipped_stops = Vec::new();

    for entity in &feed.entity {
        if let Some(tu) = &entity.trip_update {
            let Some(trip_id) = tu.trip.trip_id.clone() else {
                continue;
            };
            // CANCELED and DELETED both mean the whole trip will not run; DELETED
            // additionally asks consumers to hide it, but for routing purposes we
            // treat it exactly like a cancellation (never boarded).
            if tu.trip.schedule_relationship == Some(ScheduleRelationship::Canceled as i32)
                || tu.trip.schedule_relationship == Some(ScheduleRelationship::Deleted as i32)
            {
                canceled.push(trip_id);
                continue;
            }
            for stu in &tu.stop_time_update {
                if stu.stop_id.is_none() && stu.stop_sequence.is_none() {
                    continue;
                }
                // Honour the stop-time schedule_relationship BEFORE recording an
                // actual stop or a delay:
                //   SKIPPED  → the trip no longer serves this stop. Record it as a
                //              skip (partial cancellation) and emit neither a delay
                //              nor an actual_stop (which would fabricate a platform
                //              assignment at an un-served stop).
                //   NO_DATA  → no prediction here; any event fields are meaningless,
                //              so skip the update entirely.
                match stu.schedule_relationship {
                    Some(sr) if sr == StopRel::Skipped as i32 => {
                        if let Some(stop_id) = &stu.stop_id {
                            skipped_stops.push((trip_id.clone(), stop_id.clone()));
                        }
                        continue;
                    }
                    Some(sr) if sr == StopRel::NoData as i32 => {
                        continue;
                    }
                    _ => {}
                }
                // Capture the actual RT stop_id unconditionally (not gated on delay)
                // so platform assignments are recorded even for on-time stops.
                if let Some(stop_id) = &stu.stop_id {
                    actual_stops.push(ActualStopId {
                        trip_id: trip_id.clone(),
                        stop_id: stop_id.clone(),
                    });
                }
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
        } else if let Some(alert) = &entity.alert {
            let active_period = alert
                .active_period
                .iter()
                .map(|tr| (tr.start, tr.end))
                .collect();
            let informed_entity = alert
                .informed_entity
                .iter()
                .map(|e| AlertEntitySelector {
                    trip_id: e.trip.as_ref().and_then(|t| t.trip_id.clone()),
                    route_id: e.route_id.clone(),
                    stop_id: e.stop_id.clone(),
                })
                .collect();
            alerts.push(ServiceAlert {
                header: alert.header_text.as_ref().and_then(first_translation),
                description: alert.description_text.as_ref().and_then(first_translation),
                cause: alert.cause,
                effect: alert.effect,
                active_period,
                informed_entity,
            });
        }
    }
    Ok(FeedUpdate {
        delays: out,
        canceled,
        positions: Vec::new(),
        alerts,
        actual_stops,
        skipped_stops,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::realtime::proto::{
        FeedEntity, FeedHeader, TripUpdate,
        trip_update::{StopTimeEvent, StopTimeUpdate},
    };

    fn canceled_trip_entity(trip_id: &str) -> FeedEntity {
        let mut tu = TripUpdate::default();
        tu.trip.trip_id = Some(trip_id.to_string());
        tu.trip.schedule_relationship = Some(ScheduleRelationship::Canceled as i32);
        FeedEntity {
            id: trip_id.to_string(),
            trip_update: Some(tu),
            ..Default::default()
        }
    }

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

    fn skipped_stop_update(seq: u32) -> StopTimeUpdate {
        use crate::ingestion::realtime::proto::trip_update::stop_time_update::ScheduleRelationship;
        StopTimeUpdate {
            stop_sequence: Some(seq),
            stop_id: Some(format!("stop_{seq}")),
            schedule_relationship: Some(ScheduleRelationship::Skipped as i32),
            // A malformed feed may even carry event fields on a SKIPPED stop; they
            // must be ignored, never turned into a delay.
            arrival: Some(StopTimeEvent {
                delay: Some(600),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn no_data_stop_update(seq: u32) -> StopTimeUpdate {
        use crate::ingestion::realtime::proto::trip_update::stop_time_update::ScheduleRelationship;
        StopTimeUpdate {
            stop_sequence: Some(seq),
            stop_id: Some(format!("stop_{seq}")),
            schedule_relationship: Some(ScheduleRelationship::NoData as i32),
            arrival: Some(StopTimeEvent {
                delay: Some(120),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn deleted_trip_entity(trip_id: &str) -> FeedEntity {
        let mut tu = TripUpdate::default();
        tu.trip.trip_id = Some(trip_id.to_string());
        tu.trip.schedule_relationship = Some(ScheduleRelationship::Deleted as i32);
        FeedEntity {
            id: trip_id.to_string(),
            trip_update: Some(tu),
            ..Default::default()
        }
    }

    #[test]
    fn skipped_stop_emits_no_delay_no_actual_stop_and_is_recorded() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_skip",
            vec![stop_update(1, Some(60)), skipped_stop_update(2), stop_update(3, Some(180))],
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        // The skipped stop (seq 2) produces neither a delay nor an actual_stop.
        assert_eq!(
            update.delays,
            vec![
                TripDelay {
                    trip_id: "trip_skip".into(),
                    stop_id: Some("stop_1".into()),
                    stop_sequence: Some(1),
                    delay: 60
                },
                TripDelay {
                    trip_id: "trip_skip".into(),
                    stop_id: Some("stop_3".into()),
                    stop_sequence: Some(3),
                    delay: 180
                },
            ]
        );
        assert_eq!(
            update.actual_stops,
            vec![
                ActualStopId { trip_id: "trip_skip".into(), stop_id: "stop_1".into() },
                ActualStopId { trip_id: "trip_skip".into(), stop_id: "stop_3".into() },
            ],
            "skipped stop must not fabricate a platform assignment"
        );
        assert_eq!(
            update.skipped_stops,
            vec![("trip_skip".to_string(), "stop_2".to_string())],
            "the skipped stop is recorded so routing can avoid boarding/alighting"
        );
    }

    #[test]
    fn no_data_stop_update_is_ignored_entirely() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_nd",
            vec![no_data_stop_update(4)],
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert!(update.delays.is_empty(), "NO_DATA event fields must not become a delay");
        assert!(update.actual_stops.is_empty(), "NO_DATA yields no actual stop");
        assert!(update.skipped_stops.is_empty(), "NO_DATA is not a skip");
    }

    #[test]
    fn deleted_trip_is_treated_as_canceled() {
        let bytes = encode_feed(vec![
            deleted_trip_entity("trip_gone"),
            trip_update_entity("trip_live", vec![stop_update(1, Some(30))]),
        ]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert_eq!(update.canceled, vec!["trip_gone".to_string()]);
        assert_eq!(update.delays.len(), 1, "the live trip's delay survives");
    }

    #[test]
    fn parses_explicit_delays() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_x",
            vec![stop_update(1, Some(60)), stop_update(2, Some(180))],
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert!(update.canceled.is_empty());
        assert_eq!(
            update.delays,
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
    fn parses_canceled_trip_and_leaves_delays_unaffected() {
        let bytes = encode_feed(vec![
            canceled_trip_entity("trip_dead"),
            trip_update_entity("trip_late", vec![stop_update(3, Some(90))]),
        ]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert_eq!(update.canceled, vec!["trip_dead".to_string()]);
        assert_eq!(
            update.delays,
            vec![TripDelay {
                trip_id: "trip_late".into(),
                stop_id: Some("stop_3".into()),
                stop_sequence: Some(3),
                delay: 90
            }]
        );
    }

    #[test]
    fn skips_updates_without_delay() {
        let bytes = encode_feed(vec![trip_update_entity(
            "trip_y",
            vec![stop_update(1, None), stop_update(5, Some(-30))],
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        // Only the stop with a delay survives.
        assert_eq!(
            update.delays,
            vec![TripDelay {
                trip_id: "trip_y".into(),
                stop_id: Some("stop_5".into()),
                stop_sequence: Some(5),
                delay: -30
            }]
        );
    }

    #[test]
    fn actual_stop_captured_without_delay() {
        use crate::ingestion::realtime::ActualStopId;

        let stu = StopTimeUpdate {
            stop_sequence: Some(3),
            stop_id: Some("platform_stop_4".to_string()),
            arrival: None,
            departure: None,
            ..Default::default()
        };
        let bytes = encode_feed(vec![trip_update_entity("trip_no_delay", vec![stu])]);
        let update = parse_trip_updates(&bytes).unwrap();

        assert!(update.delays.is_empty(), "no delay → no TripDelay");
        assert_eq!(
            update.actual_stops,
            vec![ActualStopId {
                trip_id: "trip_no_delay".into(),
                stop_id: "platform_stop_4".into(),
            }],
            "actual stop must be captured even when there is no delay"
        );
    }

    #[test]
    fn actual_stop_and_delay_both_captured() {
        use crate::ingestion::realtime::ActualStopId;

        let bytes = encode_feed(vec![trip_update_entity(
            "trip_late",
            vec![stop_update(5, Some(120))],
        )]);
        let update = parse_trip_updates(&bytes).unwrap();

        assert_eq!(update.delays.len(), 1, "delay present");
        assert_eq!(
            update.actual_stops,
            vec![ActualStopId {
                trip_id: "trip_late".into(),
                stop_id: "stop_5".into(),
            }],
            "actual stop captured alongside the delay"
        );
    }

    #[test]
    fn empty_feed_yields_no_delays() {
        let bytes = encode_feed(vec![]);
        assert_eq!(parse_trip_updates(&bytes).unwrap(), FeedUpdate::default());
    }

    #[test]
    fn garbage_bytes_error() {
        assert!(parse_trip_updates(&[0xff, 0xff, 0xff, 0xff]).is_err());
    }

    fn alert_entity(
        id: &str,
        header: &str,
        description: &str,
        cause: i32,
        effect: i32,
        active_start: Option<u64>,
        active_end: Option<u64>,
        trip_id: Option<&str>,
        route_id: Option<&str>,
        stop_id: Option<&str>,
    ) -> FeedEntity {
        use crate::ingestion::realtime::proto::{
            Alert, EntitySelector, TimeRange, TranslatedString, TripDescriptor,
            translated_string::Translation,
        };
        let header_ts = TranslatedString {
            translation: vec![Translation {
                text: header.to_string(),
                language: Some("en".to_string()),
                ..Default::default()
            }],
        };
        let desc_ts = TranslatedString {
            translation: vec![Translation {
                text: description.to_string(),
                language: None,
                ..Default::default()
            }],
        };
        let selector = EntitySelector {
            trip: trip_id.map(|t| TripDescriptor {
                trip_id: Some(t.to_string()),
                ..Default::default()
            }),
            route_id: route_id.map(|r| r.to_string()),
            stop_id: stop_id.map(|s| s.to_string()),
            ..Default::default()
        };
        FeedEntity {
            id: id.to_string(),
            alert: Some(Alert {
                active_period: vec![TimeRange {
                    start: active_start,
                    end: active_end,
                }],
                informed_entity: vec![selector],
                cause: Some(cause),
                effect: Some(effect),
                header_text: Some(header_ts),
                description_text: Some(desc_ts),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn parses_alert_entity_into_service_alert() {
        use crate::ingestion::realtime::{AlertEntitySelector, ServiceAlert};

        let bytes = encode_feed(vec![alert_entity(
            "alert_1",
            "Line disrupted",
            "Bus 42 does not run today due to maintenance.",
            9,
            1,
            Some(1_750_000_000),
            Some(1_750_086_400),
            Some("trip_42"),
            None,
            None,
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert!(update.delays.is_empty());
        assert!(update.canceled.is_empty());
        assert_eq!(update.alerts.len(), 1);
        let alert = &update.alerts[0];
        assert_eq!(alert.header.as_deref(), Some("Line disrupted"));
        assert_eq!(
            alert.description.as_deref(),
            Some("Bus 42 does not run today due to maintenance.")
        );
        assert_eq!(alert.cause, Some(9));
        assert_eq!(alert.effect, Some(1));
        assert_eq!(
            alert.active_period,
            vec![(Some(1_750_000_000u64), Some(1_750_086_400u64))]
        );
        assert_eq!(
            alert.informed_entity,
            vec![AlertEntitySelector {
                trip_id: Some("trip_42".into()),
                route_id: None,
                stop_id: None,
            }]
        );
    }

    #[test]
    fn alert_and_trip_update_in_same_feed_both_parsed() {
        let bytes = encode_feed(vec![
            trip_update_entity("trip_late", vec![stop_update(1, Some(60))]),
            alert_entity(
                "alert_2",
                "Storm warning",
                "Delays expected.",
                8,
                3,
                None,
                None,
                None,
                Some("R42"),
                None,
            ),
        ]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert_eq!(update.delays.len(), 1);
        assert_eq!(update.alerts.len(), 1);
        assert_eq!(update.alerts[0].header.as_deref(), Some("Storm warning"));
        assert_eq!(update.alerts[0].informed_entity[0].route_id.as_deref(), Some("R42"));
    }

    #[test]
    fn alert_with_stop_selector_parsed() {
        use crate::ingestion::realtime::AlertEntitySelector;

        let bytes = encode_feed(vec![alert_entity(
            "alert_stop",
            "Stop closed",
            "Stop ABC is closed.",
            3,
            9,
            Some(1_750_000_000),
            None,
            None,
            None,
            Some("stop_ABC"),
        )]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert_eq!(update.alerts.len(), 1);
        let alert = &update.alerts[0];
        assert_eq!(
            alert.informed_entity,
            vec![AlertEntitySelector {
                trip_id: None,
                route_id: None,
                stop_id: Some("stop_ABC".into()),
            }]
        );
        assert_eq!(alert.active_period, vec![(Some(1_750_000_000), None)]);
    }

    #[test]
    fn empty_feed_yields_no_alerts() {
        let bytes = encode_feed(vec![]);
        let update = parse_trip_updates(&bytes).unwrap();
        assert!(update.alerts.is_empty());
    }
}
