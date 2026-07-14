//! Custom STIB / MIVB realtime feed. STIB has no GTFS-RT; its waiting-times API
//! gives predicted arrivals per `pointid`/`lineid` with no trip ids or delays,
//! which we derive by matching against the GTFS schedule in the graph.
//!
//! `expectedArrivalTime` is RFC3339 carrying the Brussels offset, so its
//! `naive_local()` is the local wall-clock directly. Predictions are matched
//! against both the current and previous service day so after-midnight trips
//! (owned by the previous day's service) are found.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, NaiveDateTime, Timelike, Utc};
use serde::Deserialize;

use crate::ingestion::gtfs::date_to_days;
use crate::ingestion::realtime::fetcher::{FetchError, Fetcher};
use crate::ingestion::realtime::{FeedUpdate, RealtimeFeed, TripDelay, VehicleObservation};
use crate::structures::{Graph, MatchParams, ScheduledArrival, best_match};

pub struct StibFeed {
    name: String,
    waiting_time_url: String,
    vehicle_position_url: Option<String>,
    headers: HashMap<String, String>,
    graph: Arc<Graph>,
    params: MatchParams,
}

impl StibFeed {
    pub fn new(
        name: String,
        waiting_time_url: String,
        vehicle_position_url: Option<String>,
        headers: HashMap<String, String>,
        graph: Arc<Graph>,
    ) -> Self {
        Self {
            name,
            waiting_time_url,
            vehicle_position_url,
            headers,
            graph,
            params: MatchParams::default(),
        }
    }
}

#[derive(Deserialize)]
struct WaitingTimes {
    results: Vec<PointLine>,
}

#[derive(Deserialize)]
struct PointLine {
    pointid: String,
    lineid: String,
    /// Double-encoded: a JSON string holding a JSON array of `Passage`.
    passingtimes: String,
}

#[derive(Deserialize)]
struct Passage {
    #[serde(rename = "expectedArrivalTime")]
    expected_arrival_time: Option<String>,
}

#[derive(Deserialize)]
struct VehiclePositionsResp {
    results: Vec<LineVehicles>,
}

#[derive(Deserialize)]
struct LineVehicles {
    lineid: String,
    vehiclepositions: String,
}

#[derive(Deserialize)]
struct VpRecord {
    #[serde(rename = "pointId")]
    point_id: String,
    #[allow(dead_code)]
    #[serde(rename = "directionId")]
    direction_id: String,
    #[serde(rename = "distanceFromPoint")]
    distance_from_point: f32,
}

impl RealtimeFeed for StibFeed {
    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&self, fetcher: &Fetcher) -> Result<FeedUpdate, FetchError> {
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

        let mut positions = Vec::new();
        if let Some(vp_url) = &self.vehicle_position_url {
            match fetcher.get(vp_url, &self.headers) {
                Ok(vp_bytes) => {
                    let now_local = Local::now().naive_local();
                    let now_unix = Utc::now().timestamp() as u64;
                    let (obs, vp_matched, vp_unmatched) =
                        self.parse_vehicle_positions(&vp_bytes, now_local, now_unix);
                    tracing::info!(
                        feed = %self.name,
                        matched = vp_matched,
                        unmatched = vp_unmatched,
                        "polled STIB vehicle-positions"
                    );
                    positions = obs;
                }
                Err(FetchError::Throttled) => {}
                Err(FetchError::Failed(e)) => {
                    tracing::error!(feed = %self.name, "STIB VP fetch failed: {e}");
                }
            }
        }

        Ok(FeedUpdate {
            delays: out,
            canceled: Vec::new(),
            positions,
            alerts: Vec::new(),
            actual_stops: Vec::new(),
            skipped_stops: Vec::new(),
        })
    }
}

impl StibFeed {
    fn match_passage(
        &self,
        stops: &[usize],
        line: &str,
        local: chrono::NaiveDateTime,
    ) -> Option<(String, String, i32)> {
        let secs_in_day = local.time().num_seconds_from_midnight() as i64;
        let day = local.date();

        // Prev day treats the predicted time as +86400 so after-midnight trips
        // owned by the previous service day are found.
        let same_day = self.match_on_day(stops, line, day, secs_in_day);
        let prev_day =
            self.match_on_day(stops, line, day - Duration::days(1), secs_in_day + 86_400);

        let chosen = match (same_day, prev_day) {
            (Some(a), Some(b)) => {
                if a.2.abs() <= b.2.abs() {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (a, b) => a.or(b),
        }?;

        let (trip, stop, delay) = chosen;
        let trip_id = self.graph.trip_id_str(trip)?.to_string();
        let stop_id = self.graph.stop_id_str(stop)?.to_string();
        Some((trip_id, stop_id, delay))
    }

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
            for sa in self
                .graph
                .stib_scheduled_arrivals(stop, line, date, weekday)
            {
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

    fn match_vehicle_position(
        &self,
        stops: &[usize],
        line: &str,
        local: NaiveDateTime,
    ) -> Option<(String, usize)> {
        let secs_in_day = local.time().num_seconds_from_midnight() as i64;
        let day = local.date();

        let same_day = self.match_vp_on_day(stops, line, day, secs_in_day);
        let prev_day =
            self.match_vp_on_day(stops, line, day - Duration::days(1), secs_in_day + 86_400);

        let chosen = match (same_day, prev_day) {
            (Some(a), Some(b)) => {
                if a.1.unsigned_abs() <= b.1.unsigned_abs() {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (a, b) => a.or(b),
        }?;

        let (trip, _delay, stop) = chosen;
        let trip_id = self.graph.trip_id_str(trip)?.to_string();
        Some((trip_id, stop))
    }

    fn match_vp_on_day(
        &self,
        stops: &[usize],
        line: &str,
        service_date: NaiveDate,
        predicted_secs: i64,
    ) -> Option<(crate::ingestion::gtfs::TripId, i32, usize)> {
        let date = date_to_days(service_date);
        let weekday = 1u8 << service_date.weekday().num_days_from_monday();

        let mut all: Vec<(usize, ScheduledArrival)> = Vec::new();
        for &stop in stops {
            for sa in self
                .graph
                .stib_scheduled_arrivals(stop, line, date, weekday)
            {
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
        Some((trip, delay, stop))
    }

    fn parse_vehicle_positions(
        &self,
        bytes: &[u8],
        now_local: NaiveDateTime,
        now_unix: u64,
    ) -> (Vec<VehicleObservation>, usize, usize) {
        let vp_resp: VehiclePositionsResp = match serde_json::from_slice(bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("parsing STIB VP JSON: {e}");
                return (Vec::new(), 0, 0);
            }
        };

        let mut out = Vec::new();
        let mut matched = 0usize;
        let mut unmatched = 0usize;

        for line_vp in &vp_resp.results {
            let records: Vec<VpRecord> = match serde_json::from_str(&line_vp.vehiclepositions) {
                Ok(r) => r,
                Err(_) => continue,
            };

            for rec in &records {
                let stops = self.graph.stib_stop_indices(&rec.point_id);
                if stops.is_empty() {
                    unmatched += 1;
                    continue;
                }

                match self.match_vehicle_position(&stops, &line_vp.lineid, now_local) {
                    Some((trip_id_str, stop_idx)) => {
                        let distance_m = rec.distance_from_point as f64;
                        let loc = self
                            .graph
                            .trip_index_of(&trip_id_str)
                            .and_then(|tid| {
                                self.graph
                                    .interpolate_along_trip_shape(tid, stop_idx, distance_m)
                            })
                            .or_else(|| self.graph.stop_lat_lng(stop_idx))
                            .unwrap_or(crate::structures::LatLng {
                                latitude: 0.0,
                                longitude: 0.0,
                            });
                        matched += 1;
                        out.push(VehicleObservation {
                            trip_id: trip_id_str,
                            lat: loc.latitude as f32,
                            lng: loc.longitude as f32,
                            bearing: None,
                            current_stop_sequence: None,
                            stop_id: None,
                            timestamp: Some(now_unix),
                        });
                    }
                    None => unmatched += 1,
                }
            }
        }

        (out, matched, unmatched)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use gtfs_structures::{Availability, RouteType};

    use crate::ingestion::gtfs::{
        AgencyId, RouteId, RouteInfo, ServiceId, ServicePattern, StopTime, TripId, TripInfo,
    };
    use crate::structures::raptor::{Lookup, PatternInfo};
    use crate::structures::{Graph, LatLng, NodeData, TransitStopData};

    use super::*;

    const FIXTURE: &str = include_str!("fixtures/stib_vehicle_positions.json");

    fn stib_vp_graph() -> Graph {
        let mut g = Graph::new();

        g.add_node(NodeData::TransitStop(TransitStopData {
            name: "Test Stop 2073".into(),
            lat_lng: LatLng {
                latitude: 50.845,
                longitude: 4.352,
            },
            accessibility: Availability::Available,
            id: "2073".into(),
            platform_code: None,
            parent_station: None,
        }));

        g.add_transit_routes(vec![RouteInfo {
            route_short_name: "20".into(),
            route_long_name: "Line 20".into(),
            route_type: RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        }]);

        g.add_transit_services(vec![ServicePattern {
            days_of_week: 0x7F,
            start_date: 0,
            end_date: 9999,
            added_dates: vec![],
            removed_dates: vec![],
        }]);

        g.add_transit_trips(vec![TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }]);
        g.add_transit_trip_ids(vec!["trip-20-A".into()]);

        {
            let stop_node = crate::structures::NodeID(0);
            let ss = g.transit_pattern_stops_len();
            g.extend_transit_pattern_stops(&[stop_node]);
            g.push_transit_idx_pattern_stops(Lookup { start: ss, len: 1 });

            let ts = g.transit_pattern_trips_len();
            g.push_transit_pattern_trip(TripId(0));
            g.push_transit_idx_pattern_trips(Lookup { start: ts, len: 1 });

            let sts = g.transit_pattern_stop_times_len();
            g.push_transit_pattern_stop_time(StopTime {
                arrival: 9 * 3600,
                departure: 9 * 3600,
                ..Default::default()
            });
            g.push_transit_idx_pattern_stop_times(Lookup { start: sts, len: 1 });

            g.push_transit_pattern(PatternInfo {
                route: RouteId(0),
                num_trips: 1,
            });
        }

        g.build_raptor_index();
        g
    }

    fn make_feed(g: Graph) -> StibFeed {
        StibFeed::new(
            "test".into(),
            "".into(),
            None,
            HashMap::new(),
            Arc::new(g),
        )
    }

    fn at_nine_oh_one() -> NaiveDateTime {
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            NaiveTime::from_hms_opt(9, 1, 0).unwrap(),
        )
    }

    #[test]
    fn fixture_double_encoded_array_decodes() {
        let resp: VehiclePositionsResp = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(resp.results.len(), 2, "two line entries in fixture");
        let first = &resp.results[0];
        assert_eq!(first.lineid, "20");
        let records: Vec<VpRecord> = serde_json::from_str(&first.vehiclepositions).unwrap();
        assert_eq!(records.len(), 2, "two VP records for line 20");
        let r = &records[0];
        assert_eq!(r.point_id, "2073");
        assert_eq!(r.direction_id, "1083");
        assert_eq!(r.distance_from_point, 321.0);
    }

    #[test]
    fn unresolvable_point_ids_are_dropped_not_panicked() {
        let g = stib_vp_graph();
        let feed = make_feed(g);
        let now = at_nine_oh_one();
        let (obs, matched, unmatched) =
            feed.parse_vehicle_positions(FIXTURE.as_bytes(), now, 1_751_000_000);
        assert_eq!(matched, 1, "only the 2073 record resolves");
        assert_eq!(unmatched, 2, "1194 and 2246 have no matching stops");
        assert_eq!(obs.len(), matched);
    }

    #[test]
    fn matching_record_resolves_to_trip_and_stop_coord() {
        let g = stib_vp_graph();
        let feed = make_feed(g);
        let now = at_nine_oh_one();
        let (obs, matched, _unmatched) =
            feed.parse_vehicle_positions(FIXTURE.as_bytes(), now, 1_751_000_000);

        assert_eq!(matched, 1, "exactly one record should resolve");
        assert_eq!(obs.len(), 1);
        let vo = &obs[0];
        assert_eq!(vo.trip_id, "trip-20-A");
        assert!((vo.lat - 50.845_f32).abs() < 0.001, "lat near 50.845");
        assert!((vo.lng - 4.352_f32).abs() < 0.001, "lng near 4.352");
        assert_eq!(vo.timestamp, Some(1_751_000_000));
        assert!(vo.lat.is_finite());
        assert!(vo.lng.is_finite());
    }

    #[test]
    fn vehicle_position_url_none_leaves_positions_empty() {
        let g = stib_vp_graph();
        let feed = StibFeed::new(
            "test".into(),
            "".into(),
            None,
            HashMap::new(),
            Arc::new(g),
        );
        assert!(feed.vehicle_position_url.is_none());
        let (obs, matched, unmatched) =
            feed.parse_vehicle_positions(b"{\"results\":[]}", at_nine_oh_one(), 0);
        assert!(obs.is_empty());
        assert_eq!(matched, 0);
        assert_eq!(unmatched, 0);
    }

    fn stib_vp_graph_with_shape() -> Graph {
        let mut g = Graph::new();

        g.add_node(NodeData::TransitStop(TransitStopData {
            name: "Test Stop 2073".into(),
            lat_lng: LatLng {
                latitude: 50.845,
                longitude: 4.352,
            },
            accessibility: Availability::Available,
            id: "2073".into(),
            platform_code: None,
            parent_station: None,
        }));

        g.add_transit_routes(vec![RouteInfo {
            route_short_name: "20".into(),
            route_long_name: "Line 20".into(),
            route_type: gtfs_structures::RouteType::Tramway,
            agency_id: AgencyId(0),
            route_color: None,
            route_text_color: None,
        }]);

        g.add_transit_services(vec![ServicePattern {
            days_of_week: 0x7F,
            start_date: 0,
            end_date: 9999,
            added_dates: vec![],
            removed_dates: vec![],
        }]);

        g.add_transit_trips(vec![TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        }]);
        g.add_transit_trip_ids(vec!["trip-20-A".into()]);

        {
            let stop_node = crate::structures::NodeID(0);
            let ss = g.transit_pattern_stops_len();
            g.extend_transit_pattern_stops(&[stop_node]);
            g.push_transit_idx_pattern_stops(crate::structures::raptor::Lookup { start: ss, len: 1 });

            let ts = g.transit_pattern_trips_len();
            g.push_transit_pattern_trip(TripId(0));
            g.push_transit_idx_pattern_trips(crate::structures::raptor::Lookup { start: ts, len: 1 });

            let sts = g.transit_pattern_stop_times_len();
            g.push_transit_pattern_stop_time(StopTime {
                arrival: 9 * 3600,
                departure: 9 * 3600,
                ..Default::default()
            });
            g.push_transit_idx_pattern_stop_times(crate::structures::raptor::Lookup { start: sts, len: 1 });

            g.push_transit_pattern(crate::structures::raptor::PatternInfo {
                route: RouteId(0),
                num_trips: 1,
            });

            let shape_pts = vec![
                LatLng { latitude: 50.845, longitude: 4.352 },
                LatLng { latitude: 50.849, longitude: 4.352 },
            ];
            let stop_idx = vec![0u32];
            g.push_transit_pattern_shape(shape_pts, stop_idx);
        }

        g.build_raptor_index();
        g
    }

    #[test]
    fn interpolated_position_is_offset_from_stop_when_shape_available() {
        let g = stib_vp_graph_with_shape();
        let feed = make_feed(g);
        let now = at_nine_oh_one();
        let (obs, matched, _unmatched) =
            feed.parse_vehicle_positions(FIXTURE.as_bytes(), now, 1_751_000_000);

        assert_eq!(matched, 1);
        assert_eq!(obs.len(), 1);
        let vo = &obs[0];
        assert_eq!(vo.trip_id, "trip-20-A");

        let stop_lat = 50.845_f32;
        let stop_lng = 4.352_f32;
        let stop = LatLng { latitude: stop_lat as f64, longitude: stop_lng as f64 };
        let result = LatLng { latitude: vo.lat as f64, longitude: vo.lng as f64 };
        let dist = stop.dist(result);

        assert!(
            (dist - 321.0).abs() < 15.0,
            "expected interpolated point ~321 m from stop, got {dist:.1} m"
        );
        assert!(
            vo.lat > stop_lat,
            "interpolated lat should be north of stop: {} vs {stop_lat}",
            vo.lat
        );
        assert!(vo.lat.is_finite());
        assert!(vo.lng.is_finite());
    }

    #[test]
    fn no_shape_falls_back_to_stop_coord() {
        let g = stib_vp_graph();
        let feed = make_feed(g);
        let now = at_nine_oh_one();
        let (obs, matched, _) =
            feed.parse_vehicle_positions(FIXTURE.as_bytes(), now, 1_751_000_000);

        assert_eq!(matched, 1);
        let vo = &obs[0];
        let stop = LatLng { latitude: 50.845, longitude: 4.352 };
        let result = LatLng { latitude: vo.lat as f64, longitude: vo.lng as f64 };
        let dist = stop.dist(result);
        assert!(dist < 1.0, "no-shape fallback should equal stop coord (dist={dist:.2} m)");
    }
}
