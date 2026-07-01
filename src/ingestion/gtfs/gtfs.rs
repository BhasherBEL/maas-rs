use std::collections::{HashMap, HashSet};

use gtfs_structures::{PickupDropOffType, RouteType};
use serde::{Deserialize, Serialize};

use crate::{
    ingestion::gtfs::IdMapper,
    ingestion::osm::{PLATFORM_MATCH_RADIUS_M, PlatformMatch, StopPlatformQuery, offset_stats},
    structures::{
        BikeAttrs, EdgeData, Graph, LatLng, NodeData, NodeID, StreetEdgeData, TransitEdgeData,
        TransitStopData,
        cost::VarGen,
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

fn bool_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StopTime {
    pub arrival: u32,
    pub departure: u32,
    /// `false` when GTFS `pickup_type == 1` (passengers may not board here).
    #[serde(default = "bool_true")]
    pub board_allowed: bool,
    /// `false` when GTFS `drop_off_type == 1` (passengers may not alight here).
    #[serde(default = "bool_true")]
    pub alight_allowed: bool,
}

impl Default for StopTime {
    fn default() -> Self {
        Self {
            arrival: 0,
            departure: 0,
            board_allowed: true,
            alight_allowed: true,
        }
    }
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
struct RouteSegment {
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

/// Transit feed provider, used to dispatch the per-provider `parent_station`
/// preprocessing seam in [`preprocess_parent_stations`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GtfsProvider {
    Sncb,
    Stib,
    Generic,
}

/// Per-provider preprocessing seam: a discoverable extension point where a
/// provider MAY synthesize `parent_station` for stops that lack it, using
/// provider-specific logic, before stations are grouped. Dispatched on the
/// provider variant. `radius_m` bounds how far an orphan stop may be merged into
/// a same-named station. Operates within the single feed only — never merges
/// across operators (the seam is invoked once per feed).
pub fn preprocess_parent_stations(
    provider: GtfsProvider,
    stops: &mut HashMap<String, std::sync::Arc<gtfs_structures::Stop>>,
    radius_m: f64,
) {
    match provider {
        GtfsProvider::Sncb => preprocess_parent_stations_sncb(stops),
        GtfsProvider::Stib | GtfsProvider::Generic => absorb_orphan_stops(stops, radius_m),
    }
}

/// SNCB feeds carry native `parent_station` values; pass them through unchanged.
fn preprocess_parent_stations_sncb(
    _stops: &mut HashMap<String, std::sync::Arc<gtfs_structures::Stop>>,
) {
}

/// Lowercase, trim, and collapse internal whitespace runs to a single space.
/// Deliberately stupid matching (no accent/punctuation folding) — fuzzy name
/// matching is a later concern.
fn normalize_station_name(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Synthesize `parent_station` for parent-less ("orphan") stops within a single
/// feed (STIB surface tram/bus stops, all DeLijn stops), so the downstream
/// station index collapses same-named neighbours that the feed left ungrouped.
///
/// Two paths, both radius-capped at `radius_m` so genuinely distinct same-named
/// stops far apart stay separate:
/// 1. SEED-FROM-NATIVE: clusters are seeded from stops that ALREADY carry a
///    native `parent_station`. An orphan whose normalized name matches a native
///    member's and that lies within `radius_m` of that member is attached to the
///    native group (its parent id is preserved; native groups are never re-keyed).
/// 2. ORPHAN CLUSTER: orphans whose name has no native seed group are clustered
///    by single-linkage within `radius_m`; each cluster of ≥2 gets a synthesized
///    parent id (`maas:synth:…`, which cannot collide with native ids). Lone
///    orphans, and same-named orphans farther than `radius_m` apart, stay separate.
fn absorb_orphan_stops(
    stops: &mut HashMap<String, std::sync::Arc<gtfs_structures::Stop>>,
    radius_m: f64,
) {
    use crate::structures::LatLng;

    let loc_of = |s: &gtfs_structures::Stop| match (s.latitude, s.longitude) {
        (Some(latitude), Some(longitude)) => Some(LatLng {
            latitude,
            longitude,
        }),
        _ => None,
    };

    let mut native_members: HashMap<String, Vec<(LatLng, String)>> = HashMap::new();
    let mut orphans: Vec<(String, String, LatLng)> = Vec::new();
    let mut native_names: HashSet<String> = HashSet::new();

    for (id, stop) in stops.iter() {
        let Some(loc) = loc_of(stop) else { continue };
        let Some(name) = stop.name.as_deref() else {
            continue;
        };
        let norm = normalize_station_name(name);
        match stop.parent_station.as_deref().filter(|p| !p.is_empty()) {
            Some(parent) => {
                native_members
                    .entry(parent.to_string())
                    .or_default()
                    .push((loc, norm.clone()));
                native_names.insert(norm);
            }
            None => orphans.push((id.clone(), norm, loc)),
        }
    }

    let mut attach: Vec<(String, String)> = Vec::new();
    let mut unseeded: Vec<(String, String, LatLng)> = Vec::new();

    for (id, norm, loc) in orphans {
        let mut best: Option<(String, f64)> = None;
        for (parent, members) in &native_members {
            for (mloc, mname) in members {
                if *mname != norm {
                    continue;
                }
                let d = loc.dist(*mloc);
                if d <= radius_m && best.as_ref().is_none_or(|(_, bd)| d < *bd) {
                    best = Some((parent.clone(), d));
                }
            }
        }
        match best {
            Some((parent, _)) => attach.push((id, parent)),
            None if native_names.contains(&norm) => {}
            None => unseeded.push((id, norm, loc)),
        }
    }

    let mut by_name: HashMap<String, Vec<(String, LatLng)>> = HashMap::new();
    for (id, norm, loc) in unseeded {
        by_name.entry(norm).or_default().push((id, loc));
    }

    for (_, members) in by_name {
        let n = members.len();
        let mut comp = vec![usize::MAX; n];
        let mut next_comp = 0usize;
        for i in 0..n {
            if comp[i] != usize::MAX {
                continue;
            }
            comp[i] = next_comp;
            let mut stack = vec![i];
            while let Some(u) = stack.pop() {
                for (v, item) in members.iter().enumerate() {
                    if comp[v] == usize::MAX && members[u].1.dist(item.1) <= radius_m {
                        comp[v] = comp[u];
                        stack.push(v);
                    }
                }
            }
            next_comp += 1;
        }

        for c in 0..next_comp {
            let group: Vec<&String> = members
                .iter()
                .enumerate()
                .filter(|(i, _)| comp[*i] == c)
                .map(|(_, (id, _))| id)
                .collect();
            if group.len() < 2 {
                continue;
            }
            let repr = group.iter().min().unwrap();
            let synth = format!("maas:synth:{repr}");
            for id in group {
                attach.push((id.clone(), synth.clone()));
            }
        }
    }

    for (id, parent) in attach {
        if let Some(arc) = stops.get_mut(&id) {
            std::sync::Arc::make_mut(arc).parent_station = Some(parent);
        }
    }
}

pub fn load_gtfs(gtfs_path: &str, g: &mut Graph) -> Result<(), gtfs_structures::Error> {
    load_gtfs_with_hook(gtfs_path, g, GtfsProvider::Generic, |_, _| None)
}

pub(crate) fn load_gtfs_with_hook<F>(
    gtfs_path: &str,
    g: &mut Graph,
    provider: GtfsProvider,
    bikes_fn: F,
) -> Result<(), gtfs_structures::Error>
where
    F: Fn(&gtfs_structures::Trip, RouteType) -> Option<bool>,
{
    let mut gtfs = gtfs_structures::Gtfs::new(gtfs_path)?;
    preprocess_parent_stations(provider, &mut gtfs.stops, g.station_merge_radius_m());

    let mut gtfs_nodes_mapper = HashMap::<String, NodeID>::new();

    let mut count_node_no_latlng = 0;
    let mut count_node_no_name = 0;
    let mut count_node_no_neighbor = 0;
    let mut count_node_too_far_neighbor = 0;

    let n_stops = gtfs.stops.len();

    let mut plat_queries: Vec<PlatQuery> = Vec::new();

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
            id: stop_id.clone(),
            platform_code: raw.platform_code.clone(),
            parent_station: raw
                .parent_station
                .clone()
                .filter(|s| !s.is_empty()),
        };

        if raw.parent_station.is_some() {
            plat_queries.push(PlatQuery {
                platform_code: raw.platform_code.clone(),
                level_id: raw.level_id.clone(),
                loc,
            });
        }

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

        let nearest_node = *nearest_node_dist.1;
        let distance = nearest_node_dist.0 as usize;

        // Stage B2a: a platform-matched stop is relocated onto its OSM platform node
        // and re-priced; it then SKIPS the default free street snap below. Unmatched
        // stops (and stops with no usable platform geometry) keep today's snap exactly.
        if raw.parent_station.is_some()
            && relocate_matched_stop(
                g,
                id,
                loc,
                nearest_node,
                raw.platform_code.as_deref(),
                raw.level_id.as_deref(),
            )
        {
            continue;
        }

        g.add_edge(id, foot_connector_edge(id, nearest_node, distance));
        g.add_edge(nearest_node, foot_connector_edge(nearest_node, id, distance));
    }

    tracing::info!("{n_stops} stops loaded");
    tracing::debug!(" - {count_node_no_latlng} without coordinates");
    tracing::debug!(" - {count_node_no_name} without name");
    tracing::debug!(" - {count_node_no_neighbor} without street neighbour");
    tracing::debug!(" - {count_node_too_far_neighbor} too far from any street node");

    report_platform_match(g, &plat_queries, gtfs_path);

    let mut agency_mapper: IdMapper<String, usize> = IdMapper::new();
    let mut agencies: Vec<AgencyInfo> = Vec::new();
    let agencies_offset = g.get_transit_agencies_size();

    for agency in gtfs.agencies {
        let agency_id = agency_mapper.get_or_insert(agency.id.unwrap_or("default".to_string()));

        agencies.resize_with(agency_id + 1, || AgencyInfo {
            name: String::new(),
            url: String::new(),
            timezone: String::new(),
        });

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

        services.resize_with(service_id + 1, || ServicePattern {
            days_of_week: 0,
            start_date: 0,
            end_date: 0,
            added_dates: Vec::new(),
            removed_dates: Vec::new(),
        });

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

        if service_id >= services.len() {
            services.resize_with(service_id + 1, || ServicePattern {
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

        let agency_id_str = route.agency_id.unwrap_or("default".to_string());
        let agency_id = match agency_mapper.get(&agency_id_str) {
            Some(v) => AgencyId((v + agencies_offset) as u16),
            None => continue,
        };

        route_infos.resize_with(route_id + 1, || RouteInfo {
            agency_id: AgencyId(0),
            route_type: RouteType::Other(-1),
            route_short_name: String::new(),
            route_long_name: String::new(),
            route_color: None,
            route_text_color: None,
        });

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
        let service_id = match service_mapper.get(&trip.service_id) {
            Some(id) => id,
            None => continue,
        };
        let route_id = match route_mapper.get(&trip.route_id) {
            Some(id) => id,
            None => continue,
        };

        trip_infos.resize_with(trip_id + 1, || TripInfo {
            trip_headsign: Some(String::new()),
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed: None,
        });

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
                board_allowed: st.pickup_type != PickupDropOffType::NotAvailable,
                alight_allowed: st.drop_off_type != PickupDropOffType::NotAvailable,
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
        let needed = pattern_id + 1;
        if needed > pattern_trip_data.len() {
            pattern_trip_data.resize_with(needed, Vec::new);
            pattern_sequences.resize_with(needed, Vec::new);
            pattern_route_ids.resize(needed, RouteId(0));
            pattern_shape_data.resize_with(needed, || None);
        }
        if pattern_sequences[pattern_id].is_empty() {
            pattern_sequences[pattern_id] = trip_nodes;
            pattern_route_ids[pattern_id] = global_route_id;
        }
        pattern_trip_data[pattern_id].push((global_trip_id, trip_stop_times));
        if pattern_shape_data[pattern_id].is_none()
            && let Some(ref shape_id) = trip.shape_id
        {
            pattern_shape_data[pattern_id] = Some((shape_id.clone(), trip_shape_dists));
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
                g.get_node(node_id).map(|n| n.loc()).unwrap_or(LatLng {
                    latitude: 0.0,
                    longitude: 0.0,
                })
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

    let (bikes_set, bikes_total) = bikes_allowed_coverage(&trip_infos);
    if bikes_total > 0 && bikes_set == 0 {
        tracing::warn!(
            "GTFS feed '{gtfs_path}' defines bikes_allowed on 0/{bikes_total} trips; \
             bike-on-transit modes will not use this feed (unknown = not allowed)"
        );
    }

    g.add_transit_trip_ids(trip_mapper.strings().to_vec());
    g.add_transit_trips(trip_infos);
    g.add_transit_route_ids(route_mapper.strings().to_vec());
    g.add_transit_routes(route_infos);
    g.add_transit_services(services);
    g.add_transit_agencies(agencies);

    Ok(())
}

/// A foot connector edge mirroring the GTFS stop-snap edge flags exactly
/// (`partial`, foot-only, neutral surface/elevation), so Stage B2a's boarding and
/// fallback edges traverse identically to today's snap and foot time is exactly
/// `length / walking_speed`.
fn foot_connector_edge(origin: NodeID, destination: NodeID, length: usize) -> EdgeData {
    EdgeData::Street(StreetEdgeData {
        origin,
        destination,
        length,
        partial: true,
        foot: true,
        bike: false,
        car: false,
        attrs: BikeAttrs::road_default(),
        elev_delta: 0,
        surface_speed: 100,
        var_gen: VarGen::NONE,
    })
}

/// Foot-Dijkstra budget (raw metres) for B2a reachability probe. Chosen to cover
/// realistic station concourse depths while keeping per-stop search cost tight
/// (~1 500 stops at build time). Raw metres because `bake_connector_lengths` runs
/// after the GTFS phase, so connector edge lengths are still physical metres here.
const B2A_FOOT_BUDGET_M: usize = 500;

/// Stage B2a: relocate a Stage-A platform-matched transit `stop` onto its matched
/// OSM platform node so boarding happens at the platform.
///
/// **Reachability logic**: runs a bounded foot Dijkstra from `orig_street_node`
/// over the real foot graph (street + platform-way + B1 connector edges, raw metres).
/// - If ≥1 platform node is reachable within budget: relocates the stop to the
///   LOWEST-COST reachable node. The real mapped path (stairs/elevator/ground)
///   is the only access; NO synthetic straight fallback connector is added.
/// - If NO platform node is reachable (truly islanded): relocates to the
///   nearest-by-distance node (previous behaviour) and adds the re-priced
///   straight fallback connector to `orig_street_node` so the stop is never
///   marooned.
///
/// Returns `true` when relocated (caller SKIPS the default free street snap),
/// `false` for an unmatched stop or one whose matched platform carries no graph
/// geometry (caller keeps today's snap byte-for-byte). On the `false` path this
/// performs NO mutation. B1's real stair/elevator edges are left untouched.
pub fn relocate_matched_stop(
    g: &mut Graph,
    stop: NodeID,
    stop_loc: LatLng,
    orig_street_node: NodeID,
    platform_code: Option<&str>,
    level_id: Option<&str>,
) -> bool {
    let platform = {
        let q = StopPlatformQuery {
            platform_code,
            level_id,
            station_centroid: stop_loc,
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        match g.platform_index().match_platform(&q) {
            PlatformMatch::ByNumber { platform, .. }
            | PlatformMatch::GeoNearest { platform, .. } => platform,
            PlatformMatch::None => return false,
        }
    };

    // Collect platform node IDs + the nearest-by-distance node (immutable borrow ends
    // before any mutation so the Dijkstra can safely borrow `&g` next).
    let (node_ids, nearest_by_dist, plat_level) = {
        let idx = g.platform_index();
        let Some(p) = idx.platform(platform) else {
            return false;
        };
        let level = p.level.map(|l| l.round() as i16);
        let node_ids: Vec<NodeID> = p.node_ids.clone();
        let mut best: Option<(NodeID, LatLng, f64)> = None;
        for &n in &node_ids {
            let Some(node) = g.get_node(n) else { continue };
            let nloc = node.loc();
            let d = stop_loc.dist(nloc);
            if best.is_none_or(|(_, _, bd)| d < bd) {
                best = Some((n, nloc, d));
            }
        }
        (node_ids, best, level)
    };

    let Some((fallback_plat_node, fallback_plat_loc, _)) = nearest_by_dist else {
        return false;
    };

    // Bounded foot Dijkstra from orig_street_node → lowest-cost reachable platform node.
    let target_set: HashSet<NodeID> = node_ids.iter().copied().collect();
    let reachable = g.foot_reach_to_targets(orig_street_node, &target_set, B2A_FOOT_BUDGET_M);

    let (plat_node, plat_loc, add_fallback) = if let Some((n, _cost)) = reachable {
        let loc = g.get_node(n).map(|nd| nd.loc()).unwrap_or(fallback_plat_loc);
        (n, loc, false)
    } else {
        (fallback_plat_node, fallback_plat_loc, true)
    };

    // Relocate the stop anchor onto the chosen platform node and pin its storey.
    g.relocate_transit_stop(stop, plat_loc);
    if let Some(lvl) = plat_level {
        g.set_node_level(stop, lvl);
    }

    // Boarding edges: stop ↔ platform node (colocated ⇒ free).
    g.add_edge(stop, foot_connector_edge(stop, plat_node, 0));
    g.add_edge(plat_node, foot_connector_edge(plat_node, stop, 0));

    // Straight fallback connector only when no real mapped path was found.
    // When a real path exists the Dijkstra already guarantees reachability via
    // the mapped edges — adding a second (cheaper straight) connector here would
    // let the teleport compete with and beat the real stairs.
    if add_fallback && plat_node != orig_street_node {
        let orig_loc = g
            .get_node(orig_street_node)
            .map(|n| n.loc())
            .unwrap_or(plat_loc);
        let run_m = plat_loc.dist(orig_loc);
        let penalty_secs = g.connector_cost().fallback_connector_secs(run_m);
        let length = ((penalty_secs * g.walking_speed_mps()).round() as usize).max(1);
        g.add_edge(
            plat_node,
            foot_connector_edge(plat_node, orig_street_node, length),
        );
        g.add_edge(
            orig_street_node,
            foot_connector_edge(orig_street_node, plat_node, length),
        );
    }
    true
}

/// A platform-stop candidate captured during stop ingestion (only stops with a
/// `parent_station`). Used by [`report_platform_match`] after the mutating stop
/// loop completes so the (immutable) platform index can be queried.
struct PlatQuery {
    platform_code: Option<String>,
    level_id: Option<String>,
    loc: LatLng,
}

/// Stage A measure-only deliverable: match every GTFS platform stop to its OSM
/// platform and log a grep-able (`platform-match:`) coverage + offset summary.
/// Changes no routing state.
fn report_platform_match(g: &Graph, queries: &[PlatQuery], feed: &str) {
    let idx = g.platform_index();
    let child_stops = queries.len();
    let with_code = queries
        .iter()
        .filter(|q| {
            q.platform_code
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false)
        })
        .count();

    let mut by_number = 0usize;
    let mut geo_nearest = 0usize;
    let mut unmatched = 0usize;
    let mut offsets: Vec<f64> = Vec::new();
    let mut geo_offsets: Vec<f64> = Vec::new();
    // Platform indices matched to ≥1 GTFS platform stop (unique), for the B1
    // connector-coverage measurement.
    let mut matched_platforms: std::collections::HashSet<usize> = std::collections::HashSet::new();

    for q in queries {
        let query = StopPlatformQuery {
            platform_code: q.platform_code.as_deref(),
            level_id: q.level_id.as_deref(),
            station_centroid: q.loc,
            search_radius_m: PLATFORM_MATCH_RADIUS_M,
        };
        match idx.match_platform(&query) {
            PlatformMatch::ByNumber { platform, dist_m } => {
                by_number += 1;
                offsets.push(dist_m);
                matched_platforms.insert(platform);
            }
            PlatformMatch::GeoNearest { platform, dist_m } => {
                geo_nearest += 1;
                geo_offsets.push(dist_m);
                matched_platforms.insert(platform);
            }
            PlatformMatch::None => unmatched += 1,
        }
    }

    let match_rate = if with_code > 0 {
        by_number as f64 / with_code as f64 * 100.0
    } else {
        0.0
    };

    tracing::info!(
        "platform-match: feed='{feed}' osm_platforms={} child_stops={child_stops} \
         with_platform_code={with_code} matched_by_number={by_number} \
         geo_nearest={geo_nearest} unmatched={unmatched} match_rate={match_rate:.1}%",
        idx.len()
    );

    let (n, mean, median, p90, max) = offset_stats(&mut offsets);
    tracing::info!(
        "platform-match: offsets(ByNumber) count={n} mean={mean:.1}m median={median:.1}m \
         p90={p90:.1}m max={max:.1}m"
    );

    if !geo_offsets.is_empty() {
        let (gn, gmean, gmed, gp90, gmax) = offset_stats(&mut geo_offsets);
        tracing::info!(
            "platform-match: offsets(GeoNearest) count={gn} mean={gmean:.1}m median={gmed:.1}m \
             p90={gp90:.1}m max={gmax:.1}m"
        );
    }

    report_connector_coverage(g, &matched_platforms, feed);
}

/// Connector budget (m) for the B1 coverage BFS: how far a pedestrian path from a
/// platform may walk to reach a ground street node. Generous — concourse↔platform
/// access at a large station is rarely more than a couple hundred metres.
const CONNECTOR_BUDGET_M: usize = 500;

/// Stage B1 deliverable + B2 go/no-go: for each matched platform, decide whether
/// it is reachable from the ground street graph via a level-continuous pedestrian
/// path crossing ≥1 vertical connector (stairs/elevator/ramp), and report the
/// extra walk distance/time that path adds. Also logs the reachability
/// non-regression check (transit stops reachable before vs after B1).
fn report_connector_coverage(g: &Graph, matched: &std::collections::HashSet<usize>, feed: &str) {
    let idx = g.platform_index();
    let platform_nodes = g.all_platform_nodes();
    let cost = g.connector_cost();

    let mut reachable = 0usize;
    let mut no_vertical_path = 0usize;
    let mut no_geometry = 0usize;
    let mut added_dist: Vec<f64> = Vec::new();
    let mut added_time: Vec<f64> = Vec::new();

    for &pi in matched {
        let Some(p) = idx.platform(pi) else { continue };
        if p.node_ids.is_empty() {
            no_geometry += 1;
            continue;
        }
        let reach =
            g.platform_connector_reach(&p.node_ids, p.centroid, &platform_nodes, CONNECTOR_BUDGET_M);
        if reach.reachable_via_connector {
            reachable += 1;
            if let (Some(path), Some(straight)) = (reach.path_dist_m, reach.straight_m) {
                let extra = (path - straight).max(0.0);
                added_dist.push(extra);
                // Time the whole connector path at walking speed as a floor, then add
                // the realistic stairs penalty for the run (documented in ConnectorCost).
                let walk_s = path / g.walking_speed_mps();
                let stairs_extra = cost.seconds(crate::structures::Connector::Steps, extra)
                    - extra / g.walking_speed_mps();
                added_time.push(walk_s + stairs_extra.max(0.0));
            }
        } else {
            no_vertical_path += 1;
        }
    }

    // Coverage counts only platforms reached via a path crossing ≥1 connector edge,
    // so a level-0 surface platform flush with the street (reachable on the flat, no
    // stairs needed) lands in `no_vertical_path`. The reported % is therefore a
    // CONSERVATIVE LOWER BOUND on real pedestrian accessibility, not the full figure.
    let matched_n = reachable + no_vertical_path + no_geometry;
    let coverage = if matched_n > 0 {
        reachable as f64 / matched_n as f64 * 100.0
    } else {
        0.0
    };
    tracing::info!(
        "connector-coverage: feed='{feed}' matched_platforms={matched_n} \
         reachable_via_connectors={reachable} coverage={coverage:.1}% \
         (no_vertical_path={no_vertical_path}, no_geometry={no_geometry})"
    );

    let (dn, dmean, dmed, dp90, dmax) = offset_stats(&mut added_dist);
    tracing::info!(
        "connector-coverage: walk_distance_added count={dn} mean={dmean:.1}m median={dmed:.1}m \
         p90={dp90:.1}m max={dmax:.1}m"
    );
    let (tn, tmean, tmed, tp90, tmax) = offset_stats(&mut added_time);
    tracing::info!(
        "connector-coverage: walk_time_added count={tn} mean={tmean:.0}s median={tmed:.0}s \
         p90={tp90:.0}s max={tmax:.0}s (stairs={:.2}m/s elevator={:.0}s)",
        cost.stairs_speed_mps,
        cost.elevator_secs
    );

    let (stops, after, before) = g.transit_stops_reachable(&platform_nodes);
    tracing::info!(
        "reachability-sanity: feed='{feed}' transit_stops={stops} \
         reachable_after={after} reachable_before={before} \
         (coarse accessibility indicator, NOT a non-regression proof; the structural \
         guarantee is the unchanged snap loop + platform nodes excluded from the snap KD-tree)"
    );
}

/// (trips with an explicit bikes_allowed value, total trips).
fn bikes_allowed_coverage(trips: &[TripInfo]) -> (usize, usize) {
    let set = trips.iter().filter(|t| t.bikes_allowed.is_some()).count();
    (set, trips.len())
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
    let Some((shape_id, stop_dists)) = pattern_shape_data.get(pattern_id).and_then(|x| x.as_ref())
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

    let has_dist =
        stop_dists.iter().any(|d| d.is_some()) && sorted.iter().all(|s| s.dist_traveled.is_some());

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
        .map(|s| LatLng {
            latitude: s.latitude,
            longitude: s.longitude,
        })
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

    // ── bikes_allowed_coverage ────────────────────────────────────────────────

    fn trip_with_bikes(bikes_allowed: Option<bool>) -> TripInfo {
        TripInfo {
            trip_headsign: None,
            route_id: RouteId(0),
            service_id: ServiceId(0),
            bikes_allowed,
        }
    }


    fn stop_with_parent(id: &str, parent: Option<&str>) -> std::sync::Arc<gtfs_structures::Stop> {
        std::sync::Arc::new(gtfs_structures::Stop {
            id: id.to_string(),
            parent_station: parent.map(|s| s.to_string()),
            ..Default::default()
        })
    }

    fn stop_at(
        id: &str,
        name: &str,
        lat: f64,
        lon: f64,
        parent: Option<&str>,
    ) -> std::sync::Arc<gtfs_structures::Stop> {
        std::sync::Arc::new(gtfs_structures::Stop {
            id: id.to_string(),
            name: Some(name.to_string()),
            latitude: Some(lat),
            longitude: Some(lon),
            parent_station: parent.map(|s| s.to_string()),
            ..Default::default()
        })
    }

    fn stops_fixture() -> HashMap<String, std::sync::Arc<gtfs_structures::Stop>> {
        let mut m = HashMap::new();
        m.insert("p1".to_string(), stop_with_parent("p1", Some("station_X")));
        m.insert("p2".to_string(), stop_with_parent("p2", None));
        m
    }

    #[test]
    fn preprocess_sncb_passes_native_parent_through_unchanged() {
        let mut stops = stops_fixture();
        preprocess_parent_stations(GtfsProvider::Sncb, &mut stops, 100.0);
        assert_eq!(
            stops["p1"].parent_station.as_deref(),
            Some("station_X"),
            "SNCB pass-through must keep native parent_station"
        );
        assert_eq!(stops["p2"].parent_station, None);
    }

    #[test]
    fn preprocess_sncb_never_absorbs_orphans() {
        let mut stops = HashMap::new();
        stops.insert(
            "rail".to_string(),
            stop_at("rail", "Gare", 50.0000, 4.0000, Some("STATION")),
        );
        stops.insert(
            "orphan".to_string(),
            stop_at("orphan", "Gare", 50.0001, 4.0000, None),
        );
        preprocess_parent_stations(GtfsProvider::Sncb, &mut stops, 100.0);
        assert_eq!(
            stops["orphan"].parent_station, None,
            "SNCB is native pass-through: orphans are never absorbed"
        );
    }

    #[test]
    fn orphan_absorbed_into_native_group_within_radius() {
        let mut stops = HashMap::new();
        stops.insert(
            "plat".to_string(),
            stop_at("plat", "Merode", 50.8330, 4.3920, Some("12")),
        );
        stops.insert(
            "surface".to_string(),
            stop_at("surface", "MERODE", 50.8331, 4.3920, None),
        );
        preprocess_parent_stations(GtfsProvider::Stib, &mut stops, 100.0);
        assert_eq!(
            stops["surface"].parent_station.as_deref(),
            Some("12"),
            "orphan attaches to the native group (name match within radius)"
        );
        assert_eq!(
            stops["plat"].parent_station.as_deref(),
            Some("12"),
            "the native group's id is untouched"
        );
    }

    #[test]
    fn orphan_far_from_native_group_stays_separate() {
        let mut stops = HashMap::new();
        stops.insert(
            "plat".to_string(),
            stop_at("plat", "Merode", 50.8330, 4.3920, Some("12")),
        );
        stops.insert(
            "far".to_string(),
            stop_at("far", "Merode", 50.8600, 4.3920, None),
        );
        preprocess_parent_stations(GtfsProvider::Stib, &mut stops, 100.0);
        assert_eq!(
            stops["far"].parent_station, None,
            "a same-name orphan beyond the radius is not absorbed"
        );
    }

    #[test]
    fn orphan_cluster_without_native_seed_synthesizes_parent() {
        let mut stops = HashMap::new();
        stops.insert(
            "a".to_string(),
            stop_at("a", "Morkhoven Station", 51.1500, 4.8500, None),
        );
        stops.insert(
            "b".to_string(),
            stop_at("b", "morkhoven  station", 51.1501, 4.8500, None),
        );
        stops.insert(
            "c".to_string(),
            stop_at("c", "Morkhoven Station", 51.2000, 4.8500, None),
        );
        preprocess_parent_stations(GtfsProvider::Generic, &mut stops, 100.0);

        let pa = stops["a"].parent_station.clone();
        let pb = stops["b"].parent_station.clone();
        assert!(pa.is_some(), "near orphans get a synthesized parent");
        assert_eq!(pa, pb, "the two near orphans share one synthesized parent");
        assert!(
            pa.as_deref().unwrap().starts_with("maas:synth:"),
            "synthesized id is namespaced so it cannot collide with native ids"
        );
        assert_eq!(
            stops["c"].parent_station, None,
            "the far same-name orphan stays separate (negative guard)"
        );
    }

    #[test]
    fn synth_parent_id_is_min_member_stop_id() {
        let mut stops = HashMap::new();
        stops.insert(
            "z_plat".to_string(),
            stop_at("z_plat", "Markt", 51.1500, 4.8500, None),
        );
        stops.insert(
            "a_plat".to_string(),
            stop_at("a_plat", "markt", 51.1501, 4.8500, None),
        );
        preprocess_parent_stations(GtfsProvider::Generic, &mut stops, 100.0);

        assert_eq!(
            stops["z_plat"].parent_station.as_deref(),
            Some("maas:synth:a_plat"),
            "synth id derives from the lexicographically smallest member stop_id"
        );
        assert_eq!(
            stops["z_plat"].parent_station,
            stops["a_plat"].parent_station,
            "both members share the same deterministic synth id"
        );
    }

    #[test]
    fn synth_parent_id_distinct_across_feeds_with_disjoint_ids() {
        let mut stib = HashMap::new();
        stib.insert(
            "1234".to_string(),
            stop_at("1234", "Markt", 50.8500, 4.3500, None),
        );
        stib.insert(
            "1235".to_string(),
            stop_at("1235", "markt", 50.8501, 4.3500, None),
        );
        preprocess_parent_stations(GtfsProvider::Stib, &mut stib, 100.0);

        let mut delijn = HashMap::new();
        delijn.insert(
            "gs:delijn:markt:1".to_string(),
            stop_at("gs:delijn:markt:1", "Markt", 51.2000, 4.4000, None),
        );
        delijn.insert(
            "gs:delijn:markt:2".to_string(),
            stop_at("gs:delijn:markt:2", "markt", 51.2001, 4.4000, None),
        );
        preprocess_parent_stations(GtfsProvider::Generic, &mut delijn, 100.0);

        let stib_id = stib["1234"].parent_station.clone();
        let delijn_id = delijn["gs:delijn:markt:1"].parent_station.clone();
        assert_eq!(stib_id.as_deref(), Some("maas:synth:1234"));
        assert_eq!(delijn_id.as_deref(), Some("maas:synth:gs:delijn:markt:1"));
        assert_ne!(
            stib_id, delijn_id,
            "same-name orphan clusters in different feeds must NOT share a synth id"
        );
    }

    #[test]
    fn lone_orphan_is_left_standalone() {
        let mut stops = HashMap::new();
        stops.insert(
            "solo".to_string(),
            stop_at("solo", "Unique Stop", 50.0, 4.0, None),
        );
        preprocess_parent_stations(GtfsProvider::Generic, &mut stops, 100.0);
        assert_eq!(
            stops["solo"].parent_station, None,
            "a lone orphan with no same-name neighbour stays standalone"
        );
    }

    #[test]
    fn bikes_allowed_coverage_counts_set_and_total() {
        let trips = vec![
            trip_with_bikes(Some(true)),
            trip_with_bikes(Some(false)),
            trip_with_bikes(None),
        ];
        assert_eq!(bikes_allowed_coverage(&trips), (2, 3));
        assert_eq!(bikes_allowed_coverage(&[]), (0, 0));
        let unset = vec![trip_with_bikes(None), trip_with_bikes(None)];
        assert_eq!(bikes_allowed_coverage(&unset), (0, 2));
    }

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
        LatLng {
            latitude: lat,
            longitude: lon,
        }
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
            dummy_coord(0.0, 0.0),  // near shape_pt[0] at (0.0, 0.0)
            dummy_coord(0.04, 0.0), // near shape_pt[4] at (0.04, 0.0)
        ];
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> =
            vec![Some(("s1".to_string(), vec![None, None]))];

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
        let pattern_shape_data: Vec<Option<(String, Vec<Option<f32>>)>> =
            vec![Some(("s1".to_string(), vec![Some(5.0), Some(1.0)]))];

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
        assert!(sp.is_active(151, MON)); // adjacent date still active
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
