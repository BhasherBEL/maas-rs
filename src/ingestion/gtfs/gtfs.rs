use std::collections::HashMap;

use gtfs_structures::RouteType;
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::IdMapper,
    structures::{
        EdgeData, Graph, LatLng, NodeData, NodeID, StreetEdgeData, TransitEdgeData,
        TransitStopData,
        raptor::{Lookup, PatternInfo},
    },
};

static MAX_NEIGHBOR_DISTANCE: f64 = 1000.0;

// Identifiers

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct AgencyId(pub u16);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TripId(pub u32);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteId(pub u32);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceId(pub u32);

// Structures

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StopTime {
    pub arrival: u32,
    pub departure: u32,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TripSegment {
    pub trip_id: TripId,
    pub origin_stop_sequence: u32,
    pub destination_stop_sequence: u32,
    pub departure: u32,
    pub arrival: u32,
    pub service_id: ServiceId,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteSegment {
    pub departure: NodeID,
    pub arrival: NodeID,
    pub route_id: RouteId,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimetableSegment {
    pub start: usize,
    pub len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteInfo {
    pub route_short_name: String,
    pub route_long_name: String,
    pub route_type: RouteType,
    pub agency_id: AgencyId,
    /// GTFS `route_color` as (R, G, B), `None` if absent or black (#000000).
    pub route_color: Option<(u8, u8, u8)>,
    /// GTFS `route_text_color` as (R, G, B), `None` if absent or white (#FFFFFF).
    pub route_text_color: Option<(u8, u8, u8)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TripInfo {
    pub trip_headsign: Option<String>,
    pub route_id: RouteId,
    pub service_id: ServiceId,
    pub bikes_allowed: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgencyInfo {
    pub name: String,
    pub url: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicePattern {
    pub days_of_week: u8,
    pub start_date: u32,
    pub end_date: u32,
    pub added_dates: Vec<u32>,
    pub removed_dates: Vec<u32>,
}

impl ServicePattern {
    pub fn is_active(&self, date: u32, weekday: u8) -> bool {
        if self.removed_dates.binary_search(&date).is_ok() {
            return false;
        }
        if self.added_dates.binary_search(&date).is_ok() {
            return true;
        }
        date >= self.start_date && date <= self.end_date && (self.days_of_week & weekday) != 0
    }
}

pub fn load_gtfs(gtfs_path: &str, g: &mut Graph) -> Result<(), gtfs_structures::Error> {
    load_gtfs_with_hook(gtfs_path, g, |_, _| None)
}

pub(crate) fn load_gtfs_with_hook<F>(
    gtfs_path: &str,
    g: &mut Graph,
    bikes_fn: F,
) -> Result<(), gtfs_structures::Error>
where
    F: Fn(&gtfs_structures::Trip, RouteType) -> Option<bool>,
{
    let gtfs = gtfs_structures::Gtfs::new(gtfs_path)?;

    let mut gtfs_nodes_mapper = HashMap::<String, NodeID>::new();

    let mut count_node_no_latlng = 0;
    let mut count_node_no_name = 0;
    let mut count_node_no_neighbor = 0;
    let mut count_node_too_far_neighbor = 0;

    let n_stops = gtfs.stops.len();

    for (stop_id, raw) in gtfs.stops {
        let loc = match (raw.latitude, raw.longitude) {
            (Some(lat), Some(lng)) => LatLng {
                latitude: lat,
                longitude: lng,
            },
            _ => {
                count_node_no_latlng += 1;
                continue;
            }
        };

        let name = match &raw.name {
            Some(name) => name,
            _ => {
                count_node_no_name += 1;
                continue;
            }
        };

        let gtfs_stop_data = TransitStopData {
            name: name.clone(),
            lat_lng: loc,
            accessibility: raw.wheelchair_boarding,
        };

        let transit_stop = NodeData::TransitStop(gtfs_stop_data);
        let id = g.add_node(transit_stop);
        gtfs_nodes_mapper.insert(stop_id, id);

        let nearest_node_dist = match g.nearest_node_dist(loc.latitude, loc.longitude) {
            Some(node_dist) => node_dist,
            _ => {
                count_node_no_neighbor += 1;
                continue;
            }
        };

        if nearest_node_dist.0 > MAX_NEIGHBOR_DISTANCE {
            count_node_too_far_neighbor += 1;
            continue;
        }

        let nearest_node = nearest_node_dist.1.clone();
        let distance = nearest_node_dist.0 as usize;

        g.add_edge(
            id,
            EdgeData::Street(StreetEdgeData {
                origin: id,
                destination: nearest_node,
                length: distance,
                partial: true,
                foot: true,
                bike: false,
                car: false,
            }),
        );
        g.add_edge(
            nearest_node,
            EdgeData::Street(StreetEdgeData {
                origin: nearest_node,
                destination: id,
                length: distance,
                partial: true,
                foot: true,
                bike: false,
                car: false,
            }),
        );
    }

    println!("{} nodes parsed", n_stops);
    println!(" - {} nodes without geo data", count_node_no_latlng);
    println!(" - {} nodes without name", count_node_no_name);
    println!(" - {} nodes without neighbor", count_node_no_neighbor);
    println!(
        " - {} nodes without close neighbor",
        count_node_too_far_neighbor
    );

    let mut agency_mapper: IdMapper<String, usize> = IdMapper::new();
    let mut agencies: Vec<AgencyInfo> = Vec::new();
    let agencies_offset = g.get_transit_agencies_size();

    for agency in gtfs.agencies {
        let agency_id = agency_mapper.get_or_insert(agency.id.unwrap_or("default".to_string()));

        while agencies.len() <= agency_id {
            agencies.push(AgencyInfo {
                name: String::new(),
                url: String::new(),
                timezone: String::new(),
            });
        }

        agencies[agency_id] = AgencyInfo {
            name: agency.name,
            url: agency.url,
            timezone: agency.timezone,
        };
    }

    let mut service_mapper: IdMapper<String, usize> = IdMapper::new();
    let mut services: Vec<ServicePattern> = Vec::new();
    let services_offset = g.get_transit_services_size();

    for (service_id_str, cal) in gtfs.calendar {
        let service_id = service_mapper.get_or_insert(service_id_str.clone());

        let udays: u8 = (cal.monday as u8)
            | ((cal.tuesday as u8) << 1)
            | ((cal.wednesday as u8) << 2)
            | ((cal.thursday as u8) << 3)
            | ((cal.friday as u8) << 4)
            | ((cal.saturday as u8) << 5)
            | ((cal.sunday as u8) << 6);

        let start_date = date_to_days(cal.start_date);
        let end_date = date_to_days(cal.end_date);

        while services.len() <= service_id {
            services.push(ServicePattern {
                days_of_week: 0,
                start_date: 0,
                end_date: 0,
                added_dates: Vec::new(),
                removed_dates: Vec::new(),
            });
        }

        services[service_id] = ServicePattern {
            days_of_week: udays,
            start_date,
            end_date,
            added_dates: Vec::new(),
            removed_dates: Vec::new(),
        };
    }

    for (service_id_str, cal_dates) in gtfs.calendar_dates {
        let service_id = service_mapper.get_or_insert(service_id_str.clone());

        while services.len() <= service_id {
            services.push(ServicePattern {
                days_of_week: 0,
                start_date: 0,
                end_date: u32::MAX,
                added_dates: Vec::new(),
                removed_dates: Vec::new(),
            });
        }

        services[service_id].added_dates = cal_dates
            .iter()
            .filter(|cal_date| cal_date.exception_type == gtfs_structures::Exception::Added)
            .map(|cal_date| date_to_days(cal_date.date))
            .collect();
        services[service_id].removed_dates = cal_dates
            .iter()
            .filter(|cal_date| cal_date.exception_type == gtfs_structures::Exception::Deleted)
            .map(|cal_date| date_to_days(cal_date.date))
            .collect();

        services[service_id].added_dates.sort();
        services[service_id].removed_dates.sort();
    }

    let mut route_mapper: IdMapper<String, usize> = IdMapper::new();
    let mut route_infos: Vec<RouteInfo> = Vec::new();
    let routes_offset = g.get_transit_routes_size();

    for (_, route) in gtfs.routes {
        let route_id = route_mapper.get_or_insert(route.id);

        let agency_id = match agency_mapper.get(route.agency_id.unwrap_or("default".to_string())) {
            Some(v) => AgencyId((v + agencies_offset) as u16),
            None => continue,
        };

        while route_infos.len() <= route_id as usize {
            route_infos.push(RouteInfo {
                agency_id: AgencyId(0),
                route_type: RouteType::Other(-1),
                route_short_name: String::new(),
                route_long_name: String::new(),
                route_color: None,
                route_text_color: None,
            });
        }

        // Treat black (#000000) as "no color" since it is the GTFS default for
        // routes that do not define a colour.
        let route_color = route.color.and_then(|c| {
            if c.r == 0 && c.g == 0 && c.b == 0 {
                None
            } else {
                Some((c.r, c.g, c.b))
            }
        });
        // Treat white (#FFFFFF) as "no text color" (GTFS default).
        let route_text_color = route.text_color.and_then(|c| {
            if c.r == 255 && c.g == 255 && c.b == 255 {
                None
            } else {
                Some((c.r, c.g, c.b))
            }
        });

        route_infos[route_id] = RouteInfo {
            route_short_name: route.short_name.unwrap_or("??".to_string()),
            route_long_name: route.long_name.unwrap_or("Unknown".to_string()),
            route_type: route.route_type,
            agency_id,
            route_color,
            route_text_color,
        };
    }

    let mut trip_mapper: IdMapper<String, usize> = IdMapper::new();
    let mut trip_infos: Vec<TripInfo> = Vec::new();
    let trips_offset = g.get_transit_trips_size();

    let mut route_hops = HashMap::<RouteSegment, Vec<TripSegment>>::new();

    let mut pattern_mapper: IdMapper<Vec<NodeID>, usize> = IdMapper::new();
    let mut pattern_sequences: Vec<Vec<NodeID>> = Vec::new();
    let mut pattern_route_ids: Vec<RouteId> = Vec::new();
    let mut pattern_trip_data: Vec<Vec<(TripId, Vec<StopTime>)>> = Vec::new();
    // For each pattern: the shape_id and per-stop shape_dist_traveled values (from the first trip).
    let mut pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> = Vec::new();

    for (_, trip) in gtfs.trips {
        let trip_id = trip_mapper.get_or_insert(trip.id.clone());
        let service_id = match service_mapper.get(trip.service_id.clone()) {
            Some(id) => id,
            None => continue,
        };
        let route_id = match route_mapper.get(trip.route_id.clone()) {
            Some(id) => id,
            None => continue,
        };

        while trip_infos.len() <= trip_id {
            trip_infos.push(TripInfo {
                trip_headsign: Some(String::new()),
                route_id: RouteId(0),
                service_id: ServiceId(0),
                bikes_allowed: None,
            });
        }

        let route_type = route_infos[route_id].route_type;
        trip_infos[trip_id] = TripInfo {
            trip_headsign: trip.trip_headsign.clone(),
            route_id: RouteId((route_id + routes_offset) as u32),
            service_id: ServiceId((service_id + services_offset) as u32),
            bikes_allowed: bikes_fn(&trip, route_type),
        };

        let mut indices: Vec<usize> = (0..trip.stop_times.len()).collect();
        indices.sort_unstable_by_key(|&i| trip.stop_times[i].stop_sequence);

        let mut trip_nodes: Vec<NodeID> = Vec::new();
        let mut trip_stop_times: Vec<StopTime> = Vec::new();
        let mut trip_shape_dists: Vec<Option<f32>> = Vec::new();

        for &i in &indices {
            let st = &trip.stop_times[i];
            let node_id = match gtfs_nodes_mapper.get(&st.stop.id) {
                Some(id) => *id,
                None => continue,
            };
            let (dep, arr) = match (st.departure_time, st.arrival_time) {
                (Some(d), Some(a)) => (d, a),
                (Some(d), None) => (d, d),
                (None, Some(a)) => (a, a),
                _ => continue,
            };
            trip_nodes.push(node_id);
            trip_stop_times.push(StopTime {
                departure: dep,
                arrival: arr,
            });
            trip_shape_dists.push(st.shape_dist_traveled);
        }

        if trip_nodes.len() < 2 {
            continue;
        }

        let global_trip_id = TripId((trip_id + trips_offset) as u32);
        let global_route_id = RouteId((route_id + routes_offset) as u32);
        let global_service_id = ServiceId((service_id + services_offset) as u32);

        for i in 0..trip_nodes.len() - 1 {
            route_hops
                .entry(RouteSegment {
                    departure: trip_nodes[i],
                    arrival: trip_nodes[i + 1],
                    route_id: global_route_id,
                })
                .or_default()
                .push(TripSegment {
                    trip_id: global_trip_id,
                    origin_stop_sequence: i as u32,
                    destination_stop_sequence: (i + 1) as u32,
                    departure: trip_stop_times[i].departure,
                    arrival: trip_stop_times[i + 1].arrival,
                    service_id: global_service_id,
                });
        }

        let pattern_id = pattern_mapper.get_or_insert(trip_nodes.clone());
        while pattern_trip_data.len() <= pattern_id {
            pattern_trip_data.push(Vec::new());
            pattern_sequences.push(Vec::new());
            pattern_route_ids.push(RouteId(0));
        }
        if pattern_sequences[pattern_id].is_empty() {
            pattern_sequences[pattern_id] = trip_nodes;
            pattern_route_ids[pattern_id] = global_route_id;
        }
        pattern_trip_data[pattern_id].push((global_trip_id, trip_stop_times));

        while pattern_shape_data.len() <= pattern_id {
            pattern_shape_data.push(None);
        }
        if pattern_shape_data[pattern_id].is_none() {
            if let Some(ref shape_id) = trip.shape_id {
                pattern_shape_data[pattern_id] = Some((shape_id.clone(), trip_shape_dists));
            }
        }
    }

    for pattern_id in 0..pattern_sequences.len() {
        let sequence = &pattern_sequences[pattern_id];
        let trips = &mut pattern_trip_data[pattern_id];
        if sequence.len() < 2 || trips.is_empty() {
            continue;
        }

        trips.sort_unstable_by_key(|(_, times)| times[0].departure);

        let n_stops = sequence.len();
        let n_trips = trips.len();

        g.push_transit_pattern(PatternInfo {
            route: pattern_route_ids[pattern_id],
            num_trips: n_trips as u32,
        });

        let ps_start = g.transit_pattern_stops_len();
        g.extend_transit_pattern_stops(sequence);
        g.push_transit_idx_pattern_stops(Lookup {
            start: ps_start,
            len: n_stops,
        });

        let pt_start = g.transit_pattern_trips_len();
        for (trip_id, _) in trips.iter() {
            g.push_transit_pattern_trip(*trip_id);
        }
        g.push_transit_idx_pattern_trips(Lookup {
            start: pt_start,
            len: n_trips,
        });

        let st_start = g.transit_pattern_stop_times_len();
        for stop_idx in 0..n_stops {
            for (_, times) in trips.iter() {
                g.push_transit_pattern_stop_time(times[stop_idx]);
            }
        }
        g.push_transit_idx_pattern_stop_times(Lookup {
            start: st_start,
            len: n_stops * n_trips,
        });

        // ── Shape geometry ────────────────────────────────────────────────────
        let stop_coords: Vec<LatLng> = sequence
            .iter()
            .map(|&node_id| {
                g.get_node(node_id)
                    .map(|n| n.loc())
                    .unwrap_or(LatLng { latitude: 0.0, longitude: 0.0 })
            })
            .collect();
        let (shape_pts, stop_idx) =
            compute_pattern_shape(pattern_id, &stop_coords, &pattern_shape_data, &gtfs.shapes);
        g.push_transit_pattern_shape(shape_pts, stop_idx);
    }

    for (route_segment, mut trip_segments) in route_hops {
        trip_segments.sort_unstable_by_key(|ts| ts.departure);

        let timetable = TimetableSegment {
            start: g.get_transit_departures_size(),
            len: trip_segments.len(),
        };

        g.add_transit_departures(trip_segments);

        g.add_edge(
            route_segment.departure,
            EdgeData::Transit(TransitEdgeData {
                origin: route_segment.departure,
                destination: route_segment.arrival,
                route_id: route_segment.route_id,
                timetable_segment: timetable,
                length: g.nodes_distance(route_segment.departure, route_segment.arrival),
            }),
        );
    }

    g.add_transit_trips(trip_infos);
    g.add_transit_routes(route_infos);
    g.add_transit_services(services);
    g.add_transit_agencies(agencies);

    Ok(())
}

fn haversine_sq(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    dlat * dlat + dlon * dlon
}

fn compute_pattern_shape(
    pattern_id: usize,
    stop_coords: &[LatLng],
    pattern_shape_data: &[Option<(String, Vec<Option<f32>>)>],
    gtfs_shapes: &HashMap<String, Vec<gtfs_structures::Shape>>,
) -> (Vec<LatLng>, Vec<u32>) {
    let Some((shape_id, stop_dists)) =
        pattern_shape_data.get(pattern_id).and_then(|x| x.as_ref())
    else {
        return (vec![], vec![]);
    };

    let Some(raw_shapes) = gtfs_shapes.get(shape_id) else {
        return (vec![], vec![]);
    };

    let mut sorted: Vec<&gtfs_structures::Shape> = raw_shapes.iter().collect();
    sorted.sort_unstable_by_key(|s| s.sequence);
    if sorted.is_empty() {
        return (vec![], vec![]);
    }

    let n_stops = stop_coords.len();

    let has_dist = stop_dists.iter().any(|d| d.is_some())
        && sorted.iter().all(|s| s.dist_traveled.is_some());

    let stop_shape_indices: Vec<usize> = if has_dist {
        // Case A: use shape_dist_traveled
        stop_dists
            .iter()
            .map(|d| {
                let d = d.unwrap_or(0.0) as f64;
                let pos = sorted.partition_point(|s| s.dist_traveled.unwrap() as f64 <= d);
                pos.saturating_sub(1).min(sorted.len() - 1)
            })
            .collect()
    } else {
        // Case B: nearest-neighbor with monotonic forward scan
        let mut cursor = 0usize;
        stop_coords
            .iter()
            .map(|coord| {
                let best = (cursor..sorted.len())
                    .min_by(|&a, &b| {
                        let da = haversine_sq(
                            coord.latitude,
                            coord.longitude,
                            sorted[a].latitude,
                            sorted[a].longitude,
                        );
                        let db = haversine_sq(
                            coord.latitude,
                            coord.longitude,
                            sorted[b].latitude,
                            sorted[b].longitude,
                        );
                        da.partial_cmp(&db).unwrap()
                    })
                    .unwrap_or(cursor);
                cursor = best;
                best
            })
            .collect()
    };

    let from_idx = stop_shape_indices[0];
    let to_idx = stop_shape_indices[n_stops - 1];
    if to_idx < from_idx {
        return (vec![], vec![]);
    }

    let all_pts: Vec<LatLng> = sorted[from_idx..=to_idx]
        .iter()
        .map(|s| LatLng { latitude: s.latitude, longitude: s.longitude })
        .collect();

    let stop_idx: Vec<u32> = stop_shape_indices
        .iter()
        .map(|&i| (i - from_idx) as u32)
        .collect();

    (all_pts, stop_idx)
}

pub fn date_to_days(date: chrono::NaiveDate) -> u32 {
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    (date - epoch).num_days().max(0) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    // ── compute_pattern_shape ─────────────────────────────────────────────────

    fn shape_pt(seq: usize, lat: f64, lon: f64, dist: Option<f32>) -> gtfs_structures::Shape {
        gtfs_structures::Shape {
            id: "s1".into(),
            latitude: lat,
            longitude: lon,
            sequence: seq,
            dist_traveled: dist,
        }
    }

    fn dummy_coord(lat: f64, lon: f64) -> LatLng {
        LatLng { latitude: lat, longitude: lon }
    }

    #[test]
    fn test_compute_shape_with_dist_traveled() {
        // 3 stops, shape has 7 points at dists 0..=6
        // stop 0 → dist 0.0, stop 1 → dist 3.0, stop 2 → dist 6.0
        let shape_pts: Vec<gtfs_structures::Shape> = (0usize..7)
            .map(|i| shape_pt(i, i as f64, 0.0, Some(i as f32)))
            .collect();
        let mut gtfs_shapes = HashMap::new();
        gtfs_shapes.insert("s1".to_string(), shape_pts);

        let stop_coords = vec![
            dummy_coord(0.0, 0.0),
            dummy_coord(3.0, 0.0),
            dummy_coord(6.0, 0.0),
        ];
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> = vec![Some((
            "s1".to_string(),
            vec![Some(0.0), Some(3.0), Some(6.0)],
        ))];

        let (pts, idx) = compute_pattern_shape(0, &stop_coords, &pattern_shape_data, &gtfs_shapes);
        assert_eq!(pts.len(), 7);
        assert_eq!(idx, vec![0u32, 3u32, 6u32]);
    }

    #[test]
    fn test_compute_shape_without_dist_traveled_proximity() {
        // 2 stops, 5 shape points, no dist_traveled
        // stop 0 is near shape_pt[0], stop 1 is near shape_pt[4]
        let shape_pts: Vec<gtfs_structures::Shape> = (0usize..5)
            .map(|i| shape_pt(i, i as f64 * 0.01, 0.0, None))
            .collect();
        let mut gtfs_shapes = HashMap::new();
        gtfs_shapes.insert("s1".to_string(), shape_pts);

        let stop_coords = vec![
            dummy_coord(0.0, 0.0),       // near shape_pt[0] at (0.0, 0.0)
            dummy_coord(0.04, 0.0),      // near shape_pt[4] at (0.04, 0.0)
        ];
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> = vec![Some((
            "s1".to_string(),
            vec![None, None],
        ))];

        let (pts, idx) = compute_pattern_shape(0, &stop_coords, &pattern_shape_data, &gtfs_shapes);
        assert_eq!(pts.len(), 5);
        assert_eq!(idx, vec![0u32, 4u32]);
    }

    #[test]
    fn test_compute_shape_missing_shape_id() {
        let gtfs_shapes = HashMap::new();
        let stop_coords = vec![dummy_coord(0.0, 0.0), dummy_coord(1.0, 0.0)];
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> = vec![None];

        let (pts, idx) = compute_pattern_shape(0, &stop_coords, &pattern_shape_data, &gtfs_shapes);
        assert!(pts.is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn test_compute_shape_shape_id_not_in_gtfs_shapes() {
        let gtfs_shapes: HashMap<String, Vec<gtfs_structures::Shape>> = HashMap::new();
        let stop_coords = vec![dummy_coord(0.0, 0.0), dummy_coord(1.0, 0.0)];
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> =
            vec![Some(("missing_id".to_string(), vec![Some(0.0), Some(1.0)]))];

        let (pts, idx) = compute_pattern_shape(0, &stop_coords, &pattern_shape_data, &gtfs_shapes);
        assert!(pts.is_empty());
        assert!(idx.is_empty());
    }

    #[test]
    fn test_compute_shape_monotonicity_guard() {
        // stop 0 maps to shape index 5, stop 1 maps to index 1 → to_idx < from_idx
        let shape_pts: Vec<gtfs_structures::Shape> = (0usize..6)
            .map(|i| shape_pt(i, 0.0, 0.0, Some(i as f32)))
            .collect();
        let mut gtfs_shapes = HashMap::new();
        gtfs_shapes.insert("s1".to_string(), shape_pts);

        let stop_coords = vec![dummy_coord(0.0, 0.0), dummy_coord(0.0, 0.0)];
        // stop 0 has dist 5.0 → index 5; stop 1 has dist 1.0 → index 1
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> = vec![Some((
            "s1".to_string(),
            vec![Some(5.0), Some(1.0)],
        ))];

        let (pts, idx) = compute_pattern_shape(0, &stop_coords, &pattern_shape_data, &gtfs_shapes);
        assert!(pts.is_empty());
        assert!(idx.is_empty());
    }

    // Weekday bit encoding (matches ingestion code):
    //   Mon=0x01, Tue=0x02, Wed=0x04, Thu=0x08, Fri=0x10, Sat=0x20, Sun=0x40
    const MON: u8 = 0x01;
    const TUE: u8 = 0x02;
    const WED: u8 = 0x04;
    const FRI: u8 = 0x10;
    const SAT: u8 = 0x20;
    const SUN: u8 = 0x40;
    const WEEKDAYS: u8 = MON | TUE | WED | 0x08 | FRI; // Mon–Fri = 0x1F

    // ── date_to_days ──────────────────────────────────────────────────────────

    #[test]
    fn date_to_days_epoch_is_zero() {
        let epoch = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        assert_eq!(date_to_days(epoch), 0);
    }

    #[test]
    fn date_to_days_one_day_later() {
        let d = NaiveDate::from_ymd_opt(2000, 1, 2).unwrap();
        assert_eq!(date_to_days(d), 1);
    }

    #[test]
    fn date_to_days_leap_year_2000() {
        // 2000 is a leap year (366 days); 2001-01-01 is day 366 from epoch
        let d = NaiveDate::from_ymd_opt(2001, 1, 1).unwrap();
        assert_eq!(date_to_days(d), 366);
    }

    #[test]
    fn date_to_days_before_epoch_clamps_to_zero() {
        let d = NaiveDate::from_ymd_opt(1999, 12, 31).unwrap();
        assert_eq!(date_to_days(d), 0);
    }

    #[test]
    fn date_to_days_known_value() {
        // 2026-03-27 = 9582 days after 2000-01-01
        let d = NaiveDate::from_ymd_opt(2026, 3, 27).unwrap();
        let days = date_to_days(d);
        // Rough sanity check: 26 years × 365 ≈ 9490, accounting for leap years
        assert!(days > 9400 && days < 9700, "Unexpected value: {days}");
    }

    // ── ServicePattern::is_active ─────────────────────────────────────────────

    fn weekday_service() -> ServicePattern {
        ServicePattern {
            days_of_week: WEEKDAYS,
            start_date: 100,
            end_date: 200,
            added_dates: vec![],
            removed_dates: vec![],
        }
    }

    #[test]
    fn service_active_on_matching_weekday_and_date() {
        let sp = weekday_service();
        assert!(sp.is_active(150, MON));
        assert!(sp.is_active(150, FRI));
    }

    #[test]
    fn service_inactive_on_non_matching_weekday() {
        let sp = weekday_service();
        assert!(!sp.is_active(150, SAT));
        assert!(!sp.is_active(150, SUN));
    }

    #[test]
    fn service_inactive_before_start_date() {
        let sp = weekday_service();
        assert!(!sp.is_active(99, MON));
    }

    #[test]
    fn service_inactive_after_end_date() {
        let sp = weekday_service();
        assert!(!sp.is_active(201, MON));
    }

    #[test]
    fn service_active_on_boundary_dates() {
        let sp = weekday_service();
        assert!(sp.is_active(100, MON));
        assert!(sp.is_active(200, MON));
    }

    #[test]
    fn service_overridden_by_added_date() {
        let sp = ServicePattern {
            days_of_week: 0x00, // no regular service days
            start_date: 100,
            end_date: 200,
            added_dates: vec![50, 150],
            removed_dates: vec![],
        };
        assert!(sp.is_active(50, MON));
        assert!(sp.is_active(150, SUN)); // weekday doesn't matter for added dates
        assert!(!sp.is_active(100, MON)); // not in added_dates, weekday mask is 0
    }

    #[test]
    fn service_overridden_by_removed_date() {
        let sp = ServicePattern {
            days_of_week: WEEKDAYS,
            start_date: 100,
            end_date: 200,
            added_dates: vec![],
            removed_dates: vec![150],
        };
        assert!(!sp.is_active(150, MON)); // explicitly removed
        assert!(sp.is_active(151, MON));  // adjacent date still active
    }

    #[test]
    fn service_removed_takes_priority_over_added() {
        // A date in both added and removed: removed wins (checked first in is_active)
        let sp = ServicePattern {
            days_of_week: 0,
            start_date: 0,
            end_date: 1000,
            added_dates: vec![200],
            removed_dates: vec![200],
        };
        assert!(!sp.is_active(200, MON));
    }

    #[test]
    fn service_added_date_outside_regular_range() {
        // Exceptional service on a date outside the normal window
        let sp = ServicePattern {
            days_of_week: WEEKDAYS,
            start_date: 100,
            end_date: 200,
            added_dates: vec![300],
            removed_dates: vec![],
        };
        assert!(sp.is_active(300, SUN)); // added beats out-of-range check
        assert!(!sp.is_active(250, MON)); // in-range but between end_date and added date
    }
}
