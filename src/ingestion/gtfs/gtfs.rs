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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TripInfo {
    pub trip_headsign: Option<String>,
    pub route_id: RouteId,
    pub service_id: ServiceId,
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
            });
        }

        route_infos[route_id] = RouteInfo {
            route_short_name: route.short_name.unwrap_or("??".to_string()),
            route_long_name: route.long_name.unwrap_or("Unknown".to_string()),
            route_type: route.route_type,
            agency_id,
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

    for (_, trip) in gtfs.trips {
        let trip_id = trip_mapper.get_or_insert(trip.id);
        let service_id = match service_mapper.get(trip.service_id) {
            Some(id) => id,
            None => continue,
        };
        let route_id = match route_mapper.get(trip.route_id) {
            Some(id) => id,
            None => continue,
        };

        while trip_infos.len() <= trip_id {
            trip_infos.push(TripInfo {
                trip_headsign: Some(String::new()),
                route_id: RouteId(0),
                service_id: ServiceId(0),
            });
        }

        trip_infos[trip_id] = TripInfo {
            trip_headsign: trip.trip_headsign.clone(),
            route_id: RouteId((route_id + routes_offset) as u32),
            service_id: ServiceId((service_id + services_offset) as u32),
        };

        let mut indices: Vec<usize> = (0..trip.stop_times.len()).collect();
        indices.sort_unstable_by_key(|&i| trip.stop_times[i].stop_sequence);

        let mut trip_nodes: Vec<NodeID> = Vec::new();
        let mut trip_stop_times: Vec<StopTime> = Vec::new();

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

pub fn date_to_days(date: chrono::NaiveDate) -> u32 {
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    (date - epoch).num_days().max(0) as u32
}
