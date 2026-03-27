use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap, HashMap},
};

use gtfs_structures::RouteType;
use kdtree::distance::squared_euclidean;

use crate::{
    ingestion::gtfs::{StopTime, TimetableSegment, TripId},
    structures::{
        EdgeData, NodeID, ScenarioBag, degrees_to_meters,
        plan::{
            ArrivalScenario, Plan, PlanCoordinate, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg,
            PlanTransitLegStep, PlanWalkLeg, PlanWalkLegStep,
        },
        raptor::Trace,
    },
};

use super::{Graph, MAX_ROUNDS, WALKING_SPEED_MS};

impl Graph {
    pub fn raptor(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<Plan> {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / WALKING_SPEED_MS) as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self.nearest_stop_secs(origin, straight_line_secs).max(1);

        loop {
            let sources: Vec<(usize, u32)> = self
                .nearby_stops(origin, access_secs)
                .into_iter()
                .map(|(s, w)| (s, start_time + w))
                .collect();

            let targets = self.nearby_stops(destination, access_secs);

            if !sources.is_empty() && !targets.is_empty() {
                let results =
                    self.raptor_inner(&sources, &targets, start_time, access_secs, date, weekday, origin, destination);
                if !results.is_empty() {
                    return results;
                }
            }

            access_secs = access_secs.saturating_mul(2);

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(
                    self.walk_dijkstra(origin, u32::MAX)
                        .get(&destination)
                        .copied()
                        .unwrap_or(u32::MAX),
                );
            }

            if let Some(actual) = walk_only_secs {
                if access_secs >= actual {
                    return if actual < u32::MAX {
                        vec![self.build_walk_plan(origin, destination, start_time, actual)]
                    } else {
                        vec![]
                    };
                }
            }
        }
    }

    pub fn raptor_inner(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        access_secs: u32,
        date: u32,
        weekday: u8,
        origin: NodeID,
        destination: NodeID,
    ) -> Vec<Plan> {
        let n_stops = self.transit_stop_to_node.len();
        let n_patterns = self.transit_patterns.len();

        let mut best = vec![ScenarioBag::EMPTY; n_stops];
        let mut labels = vec![vec![ScenarioBag::EMPTY; n_stops]; MAX_ROUNDS + 1];
        let mut labels_rt: Vec<Vec<Option<RouteType>>> =
            vec![vec![None; n_stops]; MAX_ROUNDS + 1];
        let mut traces = vec![vec![Trace::NONE; n_stops]; MAX_ROUNDS + 1];

        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_stops];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];

        for &(stop, time) in sources {
            let bag = ScenarioBag::single(time);
            labels[0][stop] = bag;
            best[stop] = bag;
            // labels_rt[0][stop] remains None = walking access
            Self::mark(stop, &mut marked, &mut is_marked);
        }

        self.apply_transfers(
            &mut labels[0],
            &mut labels_rt[0],
            &mut best,
            &mut traces[0],
            &mut marked,
            &mut is_marked,
            start_time + access_secs,
        );

        for k in 1..=MAX_ROUNDS {
            {
                let (prev, rest) = labels.split_at_mut(k);
                rest[0].copy_from_slice(&prev[k - 1]);
            }
            {
                let (prev_rt, rest_rt) = labels_rt.split_at_mut(k);
                rest_rt[0].copy_from_slice(&prev_rt[k - 1]);
            }

            self.collect_routes(&marked, &mut queue, &mut queue_pos);
            marked.clear();
            is_marked.fill(false);

            if queue.is_empty() {
                break;
            }

            let cutoff = Self::target_cutoff(&best, targets);

            {
                let (prev, rest) = labels.split_at_mut(k);
                let (prev_rt, rest_rt) = labels_rt.split_at_mut(k);
                let prev_slice = &prev[k - 1];
                let curr_slice = &mut rest[0];
                let prev_rt_slice = &prev_rt[k - 1];
                let curr_rt_slice = &mut rest_rt[0];

                for &pat in &queue {
                    self.scan_route(
                        pat,
                        queue_pos[pat],
                        date,
                        weekday,
                        cutoff,
                        prev_slice,
                        prev_rt_slice,
                        curr_slice,
                        curr_rt_slice,
                        &mut best,
                        &mut traces[k],
                        &mut marked,
                        &mut is_marked,
                    );
                }
            }

            for &p in &queue {
                queue_pos[p] = u32::MAX;
            }
            queue.clear();

            self.apply_transfers(
                &mut labels[k],
                &mut labels_rt[k],
                &mut best,
                &mut traces[k],
                &mut marked,
                &mut is_marked,
                cutoff,
            );

            if marked.is_empty() {
                break;
            }
        }

        self.extract(
            sources, targets, start_time, date, weekday, &labels, &labels_rt, &traces, origin,
            destination,
        )
    }

    fn build_walk_plan(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
    ) -> Plan {
        let end = start_time + walk_secs;
        let length = (walk_secs as f64 * WALKING_SPEED_MS) as usize;

        let to_place = PlanPlace {
            node_id: destination,
            stop_position: None,
            arrival: Some(end),
            departure: None,
        };

        Plan {
            legs: vec![PlanLeg::Walk(PlanWalkLeg {
                from: PlanPlace {
                    node_id: origin,
                    stop_position: None,
                    arrival: None,
                    departure: Some(start_time),
                },
                to: to_place,
                start: start_time,
                end,
                duration: walk_secs,
                length,
                steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                    length,
                    time: walk_secs,
                    place: to_place,
                })],
                geometry: self.walk_path(origin, destination),
            })],
            start: start_time,
            end,
            arrival_distribution: vec![ArrivalScenario { time: end, probability: 1.0 }],
        }
    }

    fn collect_routes(&self, marked: &[usize], queue: &mut Vec<usize>, queue_pos: &mut [u32]) {
        for &stop in marked {
            let pats = self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
            for &(pat_id, pos) in pats {
                let p = pat_id.0 as usize;
                if queue_pos[p] == u32::MAX {
                    queue.push(p);
                }
                queue_pos[p] = queue_pos[p].min(pos);
            }
        }
    }

    fn scan_route(
        &self,
        pattern: usize,
        first_pos: u32,
        date: u32,
        weekday: u8,
        cutoff: u32,
        prev: &[ScenarioBag],
        prev_rt: &[Option<RouteType>],
        curr: &mut [ScenarioBag],
        curr_rt: &mut [Option<RouteType>],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
    ) {
        let pat_stops = self.transit_idx_pattern_stops[pattern].of(&self.transit_pattern_stops);
        let n_trips = self.transit_patterns[pattern].num_trips as usize;
        if n_trips == 0 {
            return;
        }

        let route_id = self.transit_patterns[pattern].route;
        let pat_rt = self.transit_routes[route_id.0 as usize].route_type;

        let all_times =
            self.transit_idx_pattern_stop_times[pattern].of(&self.transit_pattern_stop_times);
        let trip_ids = self.transit_idx_pattern_trips[pattern].of(&self.transit_pattern_trips);

        // (trip_index, boarding_pos, hit_prob)
        let mut boarded: Option<(usize, u32, f32)> = None;

        for pos in first_pos as usize..pat_stops.len() {
            let stop = self.transit_node_to_stop[pat_stops[pos].0] as usize;
            let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

            if let Some((t, bp, hit_prob)) = boarded {
                let arr = col[t].arrival;
                if arr < cutoff {
                    let bag = if hit_prob < 1.0 {
                        let miss_arr =
                            self.next_trip_arrival(trip_ids, t + 1, col, date, weekday);
                        match miss_arr {
                            Some(ma) => {
                                ScenarioBag::with_scenarios(arr, hit_prob, ma, 1.0 - hit_prob)
                            }
                            None => ScenarioBag::with_scenarios(
                                arr,
                                hit_prob,
                                u32::MAX,
                                1.0 - hit_prob,
                            ),
                        }
                    } else {
                        ScenarioBag::single(arr)
                    };

                    if bag.improves_on(&best[stop]) {
                        curr[stop].try_improve(&bag);
                        best[stop].try_improve(&bag);
                        curr_rt[stop] = Some(pat_rt);
                        traces[stop] = Trace {
                            pattern: pattern as u32,
                            trip: t as u32,
                            boarded_at: bp,
                            alighted_at: pos as u32,
                            from_stop: u32::MAX,
                        };
                        Self::mark(stop, marked, is_marked);
                    }
                }
            }

            if prev[stop].is_reached() {
                let min_dep = prev[stop].expected() as u32;
                let t_start = col.partition_point(|st| st.departure < min_dep);
                for t in t_start..n_trips {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        if boarded.map_or(true, |(ct, _, _)| t < ct) {
                            let trip_dep = col[t].departure;

                            let total_hit_prob: f32 = prev[stop]
                                .scenarios()
                                .iter()
                                .map(|s| {
                                    let buf = trip_dep.saturating_sub(s.time);
                                    let p_make = match prev_rt[stop] {
                                        Some(rt) => self
                                            .transit_delay_models
                                            .get(&rt)
                                            .map(|cdf| cdf.prob_on_time(buf))
                                            .unwrap_or(1.0),
                                        None => 1.0, // walking access = deterministic
                                    };
                                    s.prob * p_make
                                })
                                .sum();

                            boarded = Some((t, pos as u32, total_hit_prob));
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Find the arrival time of the next active trip after `start` at `col` (alighting column).
    fn next_trip_arrival(
        &self,
        trip_ids: &[TripId],
        start: usize,
        col: &[StopTime],
        date: u32,
        weekday: u8,
    ) -> Option<u32> {
        (start..trip_ids.len())
            .find(|&t| self.is_trip_active(trip_ids[t], date, weekday))
            .map(|t| col[t].arrival)
    }

    fn apply_transfers(
        &self,
        labels: &mut [ScenarioBag],
        labels_rt: &mut [Option<RouteType>],
        best: &mut [ScenarioBag],
        traces: &mut [Trace],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
        cutoff: u32,
    ) {
        let n = marked.len();
        for i in 0..n {
            let stop = marked[i];
            let time = labels[stop].earliest();
            if time >= cutoff {
                continue;
            }

            let transfers = self.transit_idx_stop_transfers[stop].of(&self.transit_stop_transfers);
            for &(target_node, walk) in transfers {
                let target = self.transit_node_to_stop[target_node.0] as usize;

                let bag = labels[stop].shifted_by(walk);
                if bag.earliest() >= cutoff {
                    continue;
                }
                if bag.improves_on(&best[target]) {
                    labels[target].try_improve(&bag);
                    best[target].try_improve(&bag);
                    labels_rt[target] = labels_rt[stop]; // preserve incoming route type through walk
                    traces[target] = Trace {
                        pattern: u32::MAX,
                        trip: u32::MAX,
                        boarded_at: u32::MAX,
                        alighted_at: u32::MAX,
                        from_stop: stop as u32,
                    };
                    Self::mark(target, marked, is_marked);
                }
            }
        }
    }

    fn extract(
        &self,
        sources: &[(usize, u32)],
        targets: &[(usize, u32)],
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        labels_rt: &[Vec<Option<RouteType>>],
        traces: &[Vec<Trace>],
        origin: NodeID,
        destination: NodeID,
    ) -> Vec<Plan> {
        let mut results = Vec::new();
        let mut pareto_best = u32::MAX;

        for k in 0..=MAX_ROUNDS {
            let mut best_arr = u32::MAX;
            let mut best_stop = 0usize;
            let mut best_walk = 0u32;

            for &(s, w) in targets {
                let total = labels[k][s].earliest().saturating_add(w);
                if total < best_arr {
                    best_arr = total;
                    best_stop = s;
                    best_walk = w;
                }
            }

            if best_arr >= pareto_best {
                continue;
            }

            pareto_best = best_arr;

            let (mut legs, origin_stop) =
                self.reconstruct(k, best_stop, date, weekday, labels, traces);

            if legs.is_empty() {
                continue;
            }

            if let Some(&(_, stop_arrival)) = sources.iter().find(|&&(s, _)| s == origin_stop) {
                let first_walk = stop_arrival.saturating_sub(start_time);
                if first_walk > 0 {
                    let stop_node = self.transit_stop_to_node[origin_stop];
                    let length = (first_walk as f64 * WALKING_SPEED_MS) as usize;
                    // Depart at the latest possible moment so the user arrives at the
                    // stop exactly when the first transit vehicle boards, not at query
                    // time (which would leave them waiting invisibly on the platform).
                    let walk_start = legs
                        .first()
                        .map(|l| match l {
                            PlanLeg::Transit(t) => t.start.saturating_sub(first_walk),
                            PlanLeg::Walk(w) => w.start.saturating_sub(first_walk),
                        })
                        .unwrap_or(start_time)
                        .max(start_time); // can never depart before query time
                    let walk_end = walk_start + first_walk;
                    let to_place = PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: Some(walk_end),
                        departure: None,
                    };
                    legs.insert(
                        0,
                        PlanLeg::Walk(PlanWalkLeg {
                            from: PlanPlace {
                                node_id: origin,
                                stop_position: None,
                                arrival: None,
                                departure: Some(walk_start),
                            },
                            to: to_place,
                            start: walk_start,
                            end: walk_end,
                            duration: first_walk,
                            length,
                            steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                                length,
                                time: first_walk,
                                place: to_place,
                            })],
                            geometry: self.walk_path(origin, stop_node),
                        }),
                    );
                }
            }

            if best_walk > 0 {
                let walk_start = labels[k][best_stop].earliest();
                let stop_node = self.transit_stop_to_node[best_stop];
                let length = (best_walk as f64 * WALKING_SPEED_MS) as usize;
                let to_place = PlanPlace {
                    node_id: destination,
                    stop_position: None,
                    arrival: Some(best_arr),
                    departure: None,
                };
                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        node_id: stop_node,
                        stop_position: None,
                        arrival: None,
                        departure: Some(walk_start),
                    },
                    to: to_place,
                    start: walk_start,
                    end: best_arr,
                    duration: best_walk,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: best_walk,
                        place: to_place,
                    })],
                    geometry: self.walk_path(stop_node, destination),
                }));
            }

            let departure = legs
                .first()
                .map(|l| match l {
                    PlanLeg::Walk(w) => w.start,
                    PlanLeg::Transit(t) => t.start,
                })
                .unwrap_or(start_time);

            let arrival_bag = labels[k][best_stop].shifted_by(best_walk);
            // Expand each scenario with the last transit vehicle's delay CDF.
            // Each CDF bin becomes a separate output scenario: one entry per
            // (internal scenario × CDF bin), with probability = s.prob × bin_mass.
            let arrival_distribution: Vec<ArrivalScenario> =
                match labels_rt[k][best_stop].and_then(|rt| self.transit_delay_models.get(&rt)) {
                    Some(cdf) if !cdf.bins.is_empty() => {
                        let mut dist =
                            Vec::with_capacity(arrival_bag.scenarios().len() * cdf.bins.len());
                        let mut prev_cum = 0.0f32;
                        for &(delay, cum_prob) in &cdf.bins {
                            let bin_mass = cum_prob - prev_cum;
                            if bin_mass > 0.0 {
                                for s in arrival_bag.scenarios() {
                                    dist.push(ArrivalScenario {
                                        time: s.time.saturating_add(delay),
                                        probability: s.prob * bin_mass,
                                    });
                                }
                            }
                            prev_cum = cum_prob;
                        }
                        dist.sort_by_key(|s| s.time);
                        dist
                    }
                    _ => arrival_bag
                        .scenarios()
                        .iter()
                        .map(|s| ArrivalScenario { time: s.time, probability: s.prob })
                        .collect(),
                };

            results.push(Plan {
                legs,
                start: departure,
                end: best_arr,
                arrival_distribution,
            });
        }

        results
    }

    fn reconstruct(
        &self,
        round: usize,
        target_stop: usize,
        date: u32,
        weekday: u8,
        labels: &[Vec<ScenarioBag>],
        traces: &[Vec<Trace>],
    ) -> (Vec<PlanLeg>, usize) {
        let mut legs = Vec::new();
        let mut stop = target_stop;
        let mut k = round;

        loop {
            let trace = traces[k][stop];
            if !trace.is_transit() && !trace.is_transfer() {
                break;
            }

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let start = labels[k][from].earliest();
                let end = labels[k][stop].earliest();
                let duration = end - start;
                let from_node = self.transit_stop_to_node[from];
                let to_node = self.transit_stop_to_node[stop];
                let length = (duration as f64 * WALKING_SPEED_MS) as usize;

                let to_place = PlanPlace {
                    stop_position: None,
                    arrival: Some(end),
                    departure: None,
                    node_id: to_node,
                };

                legs.push(PlanLeg::Walk(PlanWalkLeg {
                    from: PlanPlace {
                        stop_position: None,
                        arrival: None,
                        departure: Some(start),
                        node_id: from_node,
                    },
                    to: to_place,
                    start,
                    end,
                    duration,
                    length,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep {
                        length,
                        time: duration,
                        place: to_place,
                    })],
                    geometry: self.walk_path(from_node, to_node),
                }));
                stop = from;
                continue;
            }

            let p = trace.pattern as usize;
            let t = trace.trip as usize;
            let bp = trace.boarded_at as usize;
            let ap = trace.alighted_at as usize;

            let pat_stops = self.transit_idx_pattern_stops[p].of(&self.transit_pattern_stops);
            let n_trips = self.transit_patterns[p].num_trips as usize;
            let times = self.transit_idx_pattern_stop_times[p].of(&self.transit_pattern_stop_times);
            let trip_ids = self.transit_idx_pattern_trips[p].of(&self.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let mut steps = Vec::with_capacity(ap - bp);
            let mut total_length = 0usize;
            for s in (bp + 1)..=ap {
                let seg_len = self.nodes_distance(pat_stops[s - 1], pat_stops[s]);
                total_length += seg_len;

                let arr = times[s * n_trips + t].arrival;
                let prev_dep = times[(s - 1) * n_trips + t].departure;

                steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    length: seg_len,
                    time: arr - prev_dep,
                    place: PlanPlace {
                        node_id: pat_stops[s],
                        stop_position: Some(s as u32),
                        arrival: Some(arr),
                        departure: if s < ap {
                            Some(times[s * n_trips + t].departure)
                        } else {
                            None
                        },
                    },
                    date,
                    weekday,
                    timetable_segment: TimetableSegment { start: 0, len: 0 },
                    departure_index: 0,
                }));
            }

            let transit_geometry: Vec<PlanCoordinate> = (bp..=ap)
                .map(|s| self.node_coord(pat_stops[s]))
                .collect();

            legs.push(PlanLeg::Transit(PlanTransitLeg {
                from: PlanPlace {
                    stop_position: Some(bp as u32),
                    arrival: None,
                    departure: Some(board_dep),
                    node_id: pat_stops[bp],
                },
                to: PlanPlace {
                    stop_position: Some(ap as u32),
                    arrival: Some(alight_arr),
                    departure: None,
                    node_id: pat_stops[ap],
                },
                start: board_dep,
                end: alight_arr,
                trip_id: trip_ids[t],
                length: total_length,
                duration: alight_arr - board_dep,
                steps,
                geometry: transit_geometry,
            }));

            stop = self.transit_node_to_stop[pat_stops[bp].0] as usize;
            k -= 1;
        }

        legs.reverse();

        (legs, stop)
    }

    #[inline]
    fn is_trip_active(&self, trip_id: TripId, date: u32, weekday: u8) -> bool {
        let svc = self.transit_trips[trip_id.0 as usize].service_id;
        self.transit_services[svc.0 as usize].is_active(date, weekday)
    }

    #[inline]
    fn target_cutoff(best: &[ScenarioBag], targets: &[(usize, u32)]) -> u32 {
        targets
            .iter()
            .map(|&(s, w)| {
                if best[s].is_reached() {
                    (best[s].expected() + w as f32) as u32
                } else {
                    u32::MAX
                }
            })
            .min()
            .unwrap_or(u32::MAX)
    }

    #[inline]
    fn mark(stop: usize, marked: &mut Vec<usize>, is_marked: &mut [bool]) {
        if !is_marked[stop] {
            is_marked[stop] = true;
            marked.push(stop);
        }
    }

    fn node_coord(&self, id: NodeID) -> PlanCoordinate {
        let loc = self.nodes[id.0].loc();
        PlanCoordinate { lat: loc.latitude, lon: loc.longitude }
    }

    /// Returns the sequence of OSM nodes forming the shortest walking path
    /// from `origin` to `destination`, converted to lat/lon coordinates.
    ///
    /// Falls back to a two-point straight line if no path is found.
    fn walk_path(&self, origin: NodeID, destination: NodeID) -> Vec<PlanCoordinate> {
        if origin == destination {
            let c = self.node_coord(origin);
            return vec![c];
        }

        const WALK_MMS: u32 = (WALKING_SPEED_MS * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut parent: HashMap<NodeID, NodeID> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        'outer: while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }
            if node == destination {
                break 'outer;
            }
            // Do not expand through transit stop nodes (except the origin which
            // may itself be a transit-stop-snapped OSM node).
            if node != origin && self.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }
            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / WALK_MMS as u64) as u32;
                            let nd = d.saturating_add(t);
                            let entry = dist.entry(street.destination).or_insert(u32::MAX);
                            if nd < *entry {
                                *entry = nd;
                                parent.insert(street.destination, node);
                                pq.push(Reverse((nd, street.destination)));
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                                parent.entry(transit.destination).or_insert(node);
                            }
                        }
                    }
                }
            }
        }

        if !dist.contains_key(&destination) {
            return vec![self.node_coord(origin), self.node_coord(destination)];
        }

        // Backtrack from destination to origin.
        let mut path_nodes = vec![destination];
        let mut current = destination;
        while current != origin {
            match parent.get(&current) {
                Some(&p) => {
                    path_nodes.push(p);
                    current = p;
                }
                None => break,
            }
        }
        path_nodes.reverse();
        path_nodes.iter().map(|&n| self.node_coord(n)).collect()
    }

    pub fn walk_dijkstra(&self, origin: NodeID, max_seconds: u32) -> HashMap<NodeID, u32> {
        const WALK_MMS: u32 = (WALKING_SPEED_MS * 1000.0) as u32;

        let mut dist: HashMap<NodeID, u32> = HashMap::new();
        let mut pq: BinaryHeap<Reverse<(u32, NodeID)>> = BinaryHeap::new();

        dist.insert(origin, 0);
        pq.push(Reverse((0, origin)));

        while let Some(Reverse((d, node))) = pq.pop() {
            if d > *dist.get(&node).unwrap_or(&u32::MAX) {
                continue;
            }

            if self.transit_node_to_stop[node.0] != u32::MAX {
                continue;
            }

            if let Some(neighbors) = self.edges.get(node.0) {
                for edge in neighbors {
                    match edge {
                        EdgeData::Street(street) => {
                            let t = (street.length as u64 * 1000 / WALK_MMS as u64) as u32;
                            let nd = d.saturating_add(t);
                            if nd <= max_seconds {
                                let entry = dist.entry(street.destination).or_insert(u32::MAX);
                                if nd < *entry {
                                    *entry = nd;
                                    pq.push(Reverse((nd, street.destination)));
                                }
                            }
                        }
                        EdgeData::Transit(transit) => {
                            let entry = dist.entry(transit.destination).or_insert(u32::MAX);
                            if d < *entry {
                                *entry = d;
                            }
                        }
                    }
                }
            }
        }

        dist
    }

    fn nearest_stop_secs(&self, node: NodeID, straight_line_secs: u32) -> u32 {
        let loc = self.nodes[node.0].loc();
        self.transit_stops_tree
            .nearest(&[loc.latitude, loc.longitude], 1, &squared_euclidean)
            .ok()
            .and_then(|v| v.into_iter().next())
            .map(|(dist_sq, _)| {
                let dist_m = degrees_to_meters(dist_sq.sqrt(), loc.latitude);
                (dist_m / WALKING_SPEED_MS) as u32
            })
            .unwrap_or(straight_line_secs)
    }

    pub fn nearby_stops(&self, origin: NodeID, max_walk_secs: u32) -> Vec<(usize, u32)> {
        let walk_times = self.walk_dijkstra(origin, max_walk_secs);

        let mut stops = Vec::new();
        for (&node, &walk_secs) in &walk_times {
            let compact = self.transit_node_to_stop[node.0];
            if compact != u32::MAX {
                stops.push((compact as usize, walk_secs));
            }
        }
        stops
    }

    /// Collect every origin-departure time `T` in `[earliest, latest]` such that
    /// a transit vehicle departs from some access stop at `T + walk_secs`.
    ///
    /// Returns times sorted **descending** (latest first) so a Range-RAPTOR loop
    /// can progressively tighten the `target_cutoff` on each iteration.
    fn collect_interesting_times(
        &self,
        raw_stops: &[(usize, u32)], // (compact_stop_idx, walk_secs_from_origin)
        earliest_origin_departure: u32,
        latest_origin_departure: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<u32> {
        let mut departure_times: BTreeSet<u32> = BTreeSet::new();

        for &(stop, walk_secs) in raw_stops {
            let earliest_at_stop = earliest_origin_departure.saturating_add(walk_secs);
            let latest_at_stop = latest_origin_departure.saturating_add(walk_secs);

            let pats = self.transit_idx_stop_patterns[stop].of(&self.transit_stop_patterns);
            for &(pat_id, stop_pos) in pats {
                let p = pat_id.0 as usize;
                let n_trips = self.transit_patterns[p].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }

                let stop_times =
                    self.transit_idx_pattern_stop_times[p].of(&self.transit_pattern_stop_times);
                let trip_ids =
                    self.transit_idx_pattern_trips[p].of(&self.transit_pattern_trips);

                // Column-major layout: times for stop_pos are at [stop_pos*n_trips .. (stop_pos+1)*n_trips]
                let col = &stop_times[stop_pos as usize * n_trips..(stop_pos as usize + 1) * n_trips];

                // Binary search for the window [earliest_at_stop, latest_at_stop]
                let lo = col.partition_point(|st| st.departure < earliest_at_stop);
                let hi = col.partition_point(|st| st.departure <= latest_at_stop);

                for t in lo..hi {
                    if self.is_trip_active(trip_ids[t], date, weekday) {
                        departure_times.insert(col[t].departure - walk_secs);
                    }
                }
            }
        }

        // Keep at most the next 5 departure opportunities (earliest first).
        // Capping here avoids O(N * RAPTOR_cost) blowup on frequent corridors.
        let result: Vec<u32> = departure_times.into_iter().take(5).collect();
        result
    }

    /// Range-RAPTOR: run RAPTOR for every interesting departure within
    /// `[start_time, start_time + window_secs]` and return the Pareto-optimal set
    /// of plans (transfer count × arrival time × departure time).
    pub fn raptor_range(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        window_secs: u32,
        date: u32,
        weekday: u8,
    ) -> Vec<Plan> {
        let straight_line_secs =
            (self.nodes_distance(origin, destination) as f64 / WALKING_SPEED_MS) as u32;

        let mut walk_only_secs: Option<u32> = None;
        let mut access_secs = self.nearest_stop_secs(origin, straight_line_secs).max(1);

        loop {
            let raw_stops = self.nearby_stops(origin, access_secs);
            let targets = self.nearby_stops(destination, access_secs);

            if !raw_stops.is_empty() && !targets.is_empty() {
                let sources: Vec<(usize, u32)> = raw_stops
                    .iter()
                    .map(|&(s, w)| (s, start_time + w))
                    .collect();
                // Probe at start_time to confirm this access_secs is valid.
                let probe = self.raptor_inner(
                    &sources,
                    &targets,
                    start_time,
                    access_secs,
                    date,
                    weekday,
                    origin,
                    destination,
                );
                if !probe.is_empty() {
                    let departure_times = self.collect_interesting_times(
                        &raw_stops,
                        start_time,
                        start_time.saturating_add(window_secs),
                        date,
                        weekday,
                    );

                    if departure_times.is_empty() {
                        return probe;
                    }

                    let mut all_plans = Vec::new();
                    for t in departure_times {
                        let sources_t: Vec<(usize, u32)> = raw_stops
                            .iter()
                            .map(|&(s, w)| (s, t + w))
                            .collect();
                        let plans = self.raptor_inner(
                            &sources_t,
                            &targets,
                            t,
                            access_secs,
                            date,
                            weekday,
                            origin,
                            destination,
                        );
                        all_plans.extend(plans);
                    }
                    return Self::pareto_filter(all_plans);
                }
            }

            access_secs = access_secs.saturating_mul(2);

            if access_secs >= straight_line_secs && walk_only_secs.is_none() {
                walk_only_secs = Some(
                    self.walk_dijkstra(origin, u32::MAX)
                        .get(&destination)
                        .copied()
                        .unwrap_or(u32::MAX),
                );
            }

            if let Some(actual) = walk_only_secs {
                if access_secs >= actual {
                    return if actual < u32::MAX {
                        vec![self.build_walk_plan(origin, destination, start_time, actual)]
                    } else {
                        vec![]
                    };
                }
            }
        }
    }

    /// Remove dominated plans from `plans`.
    ///
    /// Plan A dominates plan B when A is at least as good in all three dimensions
    /// (departure time, arrival time, transfer count) and strictly better in at
    /// least one.
    fn pareto_filter(plans: Vec<Plan>) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        let mut result: Vec<Plan> = Vec::new();

        'outer: for plan in plans {
            let tc_p = transfer_count(&plan);
            // Skip plan if any existing plan is at least as good in every dimension
            // (handles both dominated plans and exact duplicates).
            for existing in &result {
                let tc_e = transfer_count(existing);
                if tc_e <= tc_p && existing.end <= plan.end && existing.start >= plan.start {
                    continue 'outer;
                }
            }
            // Remove any plans in `result` that `plan` dominates.
            result.retain(|existing| {
                let tc_e = transfer_count(existing);
                !(tc_p <= tc_e
                    && plan.end <= existing.end
                    && plan.start >= existing.start
                    && (tc_p < tc_e || plan.end < existing.end || plan.start > existing.start))
            });
            result.push(plan);
        }

        result
    }
}
