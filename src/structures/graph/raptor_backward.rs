use crate::ingestion::gtfs::{StopTime, TripId};
use crate::structures::RealtimeIndex;

use super::Graph;

impl Graph {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn latest_trip_arriving_at_stop_before(
        &self,
        col: &[StopTime],
        trip_ids: &[TripId],
        max_arrival: u32,
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Option<usize> {
        // Arrival ordering is not guaranteed (trips sorted by departure), so scan backward.
        for t in (0..col.len()).rev() {
            if col[t].arrival <= max_arrival && col[t].alight_allowed {
                let svc = self.raptor.transit_trips[trip_ids[t].0 as usize].service_id;
                if self.raptor.transit_services[svc.0 as usize].is_active(date, weekday)
                    && !rt.is_canceled(trip_ids[t])
                {
                    return Some(t);
                }
            }
        }
        None
    }

    /// `lambda[remaining][stop]` = latest ready-time at `stop` with `remaining` legs
    /// left that still reaches the target by `target_latest_arr`. 0 = unreachable (sentinel).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn raptor_backward(
        &self,
        target_compact_stop: usize,
        target_latest_arr: u32,
        num_transit_legs: usize,
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Vec<Vec<u32>> {
        let n_stops = self.raptor.transit_stop_to_node.len();
        let n_patterns = self.raptor.transit_patterns.len();

        let mut lambda: Vec<Vec<u32>> = vec![vec![0u32; n_stops]; num_transit_legs + 1];

        let mut marked: Vec<usize> = Vec::new();
        let mut is_marked: Vec<bool> = vec![false; n_stops];

        if target_latest_arr > 0 {
            lambda[0][target_compact_stop] = target_latest_arr;
            Self::mark(target_compact_stop, &mut marked, &mut is_marked);
        }

        self.apply_reverse_footpaths(&mut lambda[0], &mut marked, &mut is_marked);

        for round in 1..=num_transit_legs {
            if marked.is_empty() {
                break;
            }

            let mut queue: Vec<usize> = Vec::new();
            let mut in_queue: Vec<bool> = vec![false; n_patterns];

            for &stop in &marked {
                let pats = self.raptor.transit_idx_stop_patterns[stop]
                    .of(&self.raptor.transit_stop_patterns);
                for &(pat_id, _pos) in pats {
                    let p = pat_id.0 as usize;
                    if !in_queue[p] {
                        in_queue[p] = true;
                        queue.push(p);
                    }
                }
            }

            marked.clear();
            is_marked.fill(false);

            for &pat in &queue {
                let pat_stops = self.raptor.transit_idx_pattern_stops[pat]
                    .of(&self.raptor.transit_pattern_stops);
                let n_trips = self.raptor.transit_patterns[pat].num_trips as usize;
                if n_trips == 0 {
                    continue;
                }
                let all_times = self.raptor.transit_idx_pattern_stop_times[pat]
                    .of(&self.raptor.transit_pattern_stop_times);
                let trip_ids = self.raptor.transit_idx_pattern_trips[pat]
                    .of(&self.raptor.transit_pattern_trips);

                let mut t_star: Option<usize> = None;

                for pos in (0..pat_stops.len()).rev() {
                    let compact = self.raptor.transit_node_to_stop[pat_stops[pos].0];
                    if compact == u32::MAX {
                        continue;
                    }
                    let stop = compact as usize;
                    let col = &all_times[pos * n_trips..(pos + 1) * n_trips];

                    if let Some(t) = t_star
                        && col[t].board_allowed
                    {
                        let dep = col[t].departure;
                        if dep > 0 && dep > lambda[round][stop] {
                            lambda[round][stop] = dep;
                            Self::mark(stop, &mut marked, &mut is_marked);
                        }
                    }

                    if lambda[round - 1][stop] > 0
                        && let Some(t) = self.latest_trip_arriving_at_stop_before(
                            col,
                            trip_ids,
                            lambda[round - 1][stop],
                            date,
                            weekday,
                            rt,
                        )
                    {
                        let update = match t_star {
                            None => true,
                            Some(ct) => col[t].departure > col[ct].departure,
                        };
                        if update {
                            t_star = Some(t);
                        }
                    }
                }
            }

            self.apply_reverse_footpaths(&mut lambda[round], &mut marked, &mut is_marked);
        }

        lambda
    }

    /// Uses the capped ≤`MAX_TRANSFER_DISTANCE_M` table even under forward MCR: plans
    /// with a longer forward transfer lack a backward label, but `tighten_with_backward_labels`
    /// then no-ops (never a drop) — SOUND, just looser margins/expectedEnd.
    pub(super) fn apply_reverse_footpaths(
        &self,
        lambda_k: &mut [u32],
        marked: &mut Vec<usize>,
        is_marked: &mut [bool],
    ) {
        let n = marked.len(); // snapshot: appended marks must not be re-relaxed this pass
        for i in 0..n {
            let stop = marked[i];
            if stop >= self.raptor.transit_idx_stop_reverse_transfers.len() {
                continue;
            }
            let rev = self.raptor.transit_idx_stop_reverse_transfers[stop]
                .of(&self.raptor.transit_stop_reverse_transfers);
            for &(source, walk_time) in rev {
                let t = lambda_k[stop].saturating_sub(walk_time);
                if t > 0 && t > lambda_k[source] {
                    lambda_k[source] = t;
                    Self::mark(source, marked, is_marked);
                }
            }
        }
    }
}
