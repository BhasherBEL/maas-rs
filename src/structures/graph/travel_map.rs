//! Travel-time map (isochrone / one-to-many reachability).
//!
//! From a single CENTER coordinate at a given day + departure time, compute the
//! travel time to reach MANY sampled points, so a client can paint a continuous
//! green(0)->red(maxSeconds) heatmap. This REUSES the RAPTOR forward pass and the
//! exact one-to-many foot machinery — it adds no new graph traversal:
//!
//!  1. **Reach every stop** — [`Graph::stop_arrivals`] seeds foot/bike/car access
//!     from the centre (via the production [`Graph::build_mode_context`], with an
//!     empty egress so the forward search is not clamped to a destination) and runs
//!     ONE [`Graph::run_departure_into`] sweep, the same forward pass `raptor`
//!     drives. The per-stop earliest arrival is then read straight off the carried
//!     label grid — the same survey `raptorExplain`'s `stops_reached` uses.
//!  2. **Fill the area** — the reachable bounding box is sampled on a lat/lng grid
//!     (cell edge `travel_map_grid_step_m`). For each grid point `P` the travel time
//!     is `min(direct walk centre->P, min over reached stops s of
//!     arrival[s] + walk s->P)`, capped at `maxSeconds`. Both the stop->P walk set
//!     and the direct walk reuse the exact contracted-graph one-to-many
//!     (`nearby_stops_arena` / CCH egress) and point-to-point (`walk_secs_coord_to_coord`)
//!     foot cost — no bespoke Dijkstra.
//!  3. **Departure window** — [`Graph::travel_time_map_window`] runs steps 1-2 at
//!     several departures spaced `travel_map_window_sample_secs` apart across the
//!     window and aggregates each cell (BEST = min, AVERAGE = mean).

use crate::structures::{
    ActiveModes, BikeCost, LatLng, NodeID, RealtimeIndex, ReliabilityBuckets,
};

use super::raptor_route::{BestGrid, FullRow, Label, LabelRow, QueryEndpoints, SlimRow};
use super::Graph;

/// One sampled reachability cell: a coordinate and the travel time (seconds) to
/// reach it from the centre at the query departure.
#[derive(Clone, Copy, Debug)]
pub struct TravelCell {
    pub loc: LatLng,
    pub seconds: u32,
}

/// Per-cell aggregation across a departure window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TravelAggregation {
    /// Best (minimum) travel time across the sampled departures — "if you time it
    /// perfectly". This is the default and matches a single-departure isochrone.
    Best,
    /// Mean travel time across the sampled departures — "on an average departure".
    Average,
}

impl Graph {
    /// Earliest arrival time (seconds since midnight) at every compact transit stop
    /// reachable from `center` departing at `start_time`, or `u32::MAX` for stops
    /// not reached within one forward pass. Reuses the production forward pass:
    /// access is seeded from `center` through [`Graph::build_mode_context`] (empty
    /// egress, so [`Graph::target_cutoff`] stays unbounded and the sweep relaxes the
    /// whole reachable network in one pass), then the per-stop minimum arrival is
    /// read off the carried label grid exactly as `raptorExplain`'s survey does.
    ///
    /// `max_secs` is the isochrone budget: it is used as the FOOT-ACCESS radius so
    /// every stop boardable within the budget's walk is seeded (a fixed
    /// `min_access_secs` disc would silently omit stops reachable by a longer access
    /// walk on a large isochrone). Vehicle (bike/car) access still uses the
    /// length-scaled budget from `build_mode_context`.
    #[allow(clippy::too_many_arguments)]
    pub fn stop_arrivals(
        &self,
        center: LatLng,
        start_time: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
    ) -> Vec<u32> {
        if super::raptor_route::slim_grid_enabled() {
            self.stop_arrivals_grid::<SlimRow>(
                center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted,
                use_cch, rt, bike, true,
            )
        } else {
            self.stop_arrivals_grid::<FullRow>(
                center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted,
                use_cch, rt, bike, true,
            )
        }
    }

    /// Test-only oracle for OPT-B (horizon) + OPT-C1 (skip-egress): the SAME forward
    /// pass with both optimizations DISABLED (full egress sweep, unbounded flood).
    /// [`Graph::stop_arrivals`] must return a bit-identical arrival vector; pinned in
    /// `tests/travel_map_tests.rs`.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn stop_arrivals_reference(
        &self,
        center: LatLng,
        start_time: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
    ) -> Vec<u32> {
        if super::raptor_route::slim_grid_enabled() {
            self.stop_arrivals_grid::<SlimRow>(
                center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted,
                use_cch, rt, bike, false,
            )
        } else {
            self.stop_arrivals_grid::<FullRow>(
                center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted,
                use_cch, rt, bike, false,
            )
        }
    }

    /// `optimize` (default `true` on the production path): when `false`, DISABLE the
    /// travel-map forward-pass optimizations — compute the full center egress and run
    /// an UNBOUNDED flood (no horizon). Used only by [`Graph::stop_arrivals_reference`]
    /// as the bit-identity oracle.
    #[allow(clippy::too_many_arguments)]
    fn stop_arrivals_grid<R: LabelRow>(
        &self,
        center: LatLng,
        start_time: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
        optimize: bool,
    ) -> Vec<u32> {
        use super::MAX_ROUNDS;

        let n_stops = self.raptor.transit_stop_to_node.len();
        if n_stops == 0 || !am.wants_transit() {
            // No transit (walk/bike/car-only isochrone) or an empty network: the
            // area fill below handles the direct-walk term, so return "no stop
            // reached" without running a pass.
            return vec![u32::MAX; n_stops];
        }

        // The centre snaps to itself for both endpoints; only the ACCESS side is
        // used (egress is forced empty), so the destination endpoint is irrelevant.
        let center_node = match self.arena_snap_center(center) {
            Some(n) => n,
            None => return vec![u32::MAX; n_stops],
        };
        let ep = QueryEndpoints {
            origin: center,
            destination: center,
            origin_station: None,
            destination_station: None,
        };
        // Foot-access radius = the isochrone budget (floored at the configured
        // minimum), so every stop boardable within `max_secs` on foot is seeded.
        // On the CCH path `build_mode_context` returns ALL foot-reachable stops
        // regardless of this value (even more complete); this bounds only the
        // radius-limited two-pass foot Dijkstra fallback.
        let access_secs = max_secs.max(self.raptor.min_access_secs);
        // OPT-C1: skip the wasted CCH/two-pass center-egress sweep whenever there is
        // no vehicle (bike/car) access. The center egress is unobserved by an
        // isochrone (we clear the per-state egress grid below), and its only OTHER
        // consumer — the park&ride vehicle-access retain-filter in
        // `build_mode_context` — is a provable no-op when no bike/car access states
        // exist. For walk / walk-transit isochrones (the UI default) this holds, so
        // `skip_egress = true` is bit-identical. If a vehicle mode is active we pass
        // `false` and compute egress exactly as before, keeping vehicle isochrones
        // correct.
        let skip_egress = optimize && !am.uses_vehicle();
        let mut mc = self.build_mode_context_opts(
            am,
            center_node,
            center_node,
            access_secs,
            bike,
            unrestricted,
            use_cch,
            Some(&ep),
            crate::structures::cost::FareProfile::default(),
            skip_egress,
        );
        // Force EGRESS empty: an isochrone has no destination, so the forward search
        // must not be clamped by a destination-based `target_cutoff`. With `skip_egress`
        // the egress lists are already empty; otherwise (vehicle modes) clear them now.
        for e in mc.egress.iter_mut() {
            e.clear();
        }
        if !mc.any_access() {
            return vec![u32::MAX; n_stops];
        }
        // OPT-B: bound the forward pass to the isochrone horizon. With egress forced
        // empty the destination-based `target_cutoff` is `u32::MAX` for every state,
        // so `run_departure_into` would otherwise flood the WHOLE network across all
        // MAX_ROUNDS rounds even for a small budget. Setting the horizon mins every
        // cutoff with `start_time + max_secs`, pruning labels that arrive after the
        // budget and letting rounds terminate once nothing under the horizon improves.
        // No arrival `> horizon` can survive `fill_area`'s `offset > max_secs` filter,
        // so the surviving cells are bit-identical to the unbounded pass.
        //
        // The `+1` makes the horizon EXCLUSIVE at `start_time + max_secs`: the cutoff
        // comparisons prune `arr >= cutoff`, and a stop arriving at EXACTLY
        // `start_time + max_secs` (offset == max_secs) is still kept by `fill_area`
        // (its `offset > max_secs` filter is `<=`-inclusive) — e.g. a cell sitting on
        // that stop with a zero-length egress stub. Pruning it would drop a cell the
        // reference keeps, so we widen the horizon by one second to preserve it while
        // still bounding the flood.
        if optimize {
            mc.horizon = Some(start_time.saturating_add(max_secs).saturating_add(1));
        }

        let n_states = mc.n_states();
        let n_cells = n_stops * n_states;
        let n_patterns = self.raptor.transit_patterns.len();

        let mut best = BestGrid::new(n_cells, buckets);
        let mut labels: Vec<R> = (0..=MAX_ROUNDS).map(|_| R::empty(n_cells)).collect();
        let mut marked = Vec::with_capacity(2048);
        let mut is_marked = vec![false; n_cells];
        let mut queue = Vec::with_capacity(512);
        let mut queue_pos = vec![u32::MAX; n_patterns];
        let mut arena: Vec<Label> = Vec::new();

        self.run_departure_into(
            &mc,
            start_time,
            access_secs,
            date,
            weekday,
            buckets,
            slack,
            rt,
            0,
            false,
            &mut best,
            &mut labels,
            &mut marked,
            &mut is_marked,
            &mut queue,
            &mut queue_pos,
            &mut arena,
            None,
        );

        // Survey: per stop, the minimum earliest arrival across all rounds/states.
        let mut arrivals = vec![u32::MAX; n_stops];
        for stop_idx in 0..n_stops {
            let mut best_arr = u32::MAX;
            for k in 0..=MAX_ROUNDS {
                for s in 0..n_states {
                    let cell = stop_idx * n_states + s;
                    if labels[k].is_reached(cell) {
                        best_arr = best_arr.min(labels[k].earliest(cell));
                    }
                }
            }
            arrivals[stop_idx] = best_arr;
        }
        arrivals
    }

    /// Single-departure travel-time map: sample the reachable area and return one
    /// [`TravelCell`] per grid point reachable within `max_secs`. Points beyond
    /// `max_secs` (or unreachable) are omitted.
    #[allow(clippy::too_many_arguments)]
    pub fn travel_time_map(
        &self,
        center: LatLng,
        start_time: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        grid_step_m: f64,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
    ) -> Vec<TravelCell> {
        let arrivals = self.stop_arrivals(
            center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted, use_cch,
            rt, bike,
        );
        self.fill_area(center, start_time, max_secs, grid_step_m, &arrivals)
    }

    /// Test-only reference travel-time map using the pre-OPT-A per-cell [`Graph::fill_area_reference`]
    /// (two full graph searches per grid cell). The inverted [`Graph::travel_time_map`] must
    /// return bit-identical cells; the equivalence is pinned in `tests/travel_map_tests.rs`.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn travel_time_map_reference(
        &self,
        center: LatLng,
        start_time: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        grid_step_m: f64,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
    ) -> Vec<TravelCell> {
        let arrivals = self.stop_arrivals(
            center, start_time, date, weekday, max_secs, am, buckets, slack, unrestricted, use_cch,
            rt, bike,
        );
        self.fill_area_reference(center, start_time, max_secs, grid_step_m, &arrivals)
    }

    /// Departure-window travel-time map: evaluate [`Graph::travel_time_map`] at
    /// departures spaced `travel_map_window_sample_secs` apart across
    /// `[start_time, window_end]` (inclusive of both ends), then aggregate each
    /// cell across the samples (`Best` = min, `Average` = mean). A cell is emitted
    /// iff it was reachable within `max_secs` on at least one sampled departure.
    #[allow(clippy::too_many_arguments)]
    pub fn travel_time_map_window(
        &self,
        center: LatLng,
        start_time: u32,
        window_end: u32,
        date: u32,
        weekday: u8,
        max_secs: u32,
        grid_step_m: f64,
        agg: TravelAggregation,
        am: &ActiveModes,
        buckets: &ReliabilityBuckets,
        slack: u32,
        unrestricted: bool,
        use_cch: bool,
        rt: &RealtimeIndex,
        bike: &BikeCost,
    ) -> Vec<TravelCell> {
        let departures = self.window_departures(start_time, window_end);

        // Aggregate per grid point, keyed by quantized lat/lng so the same sampled
        // point across departures lands in the same bucket (grid points are generated
        // identically per departure, so the quantization is exact for our own grid).
        // `sum`/`count` support Average; `min` supports Best.
        use std::collections::HashMap;
        let mut acc: HashMap<(i64, i64), (u64, u32, u32)> = HashMap::new(); // (sum, count, min)

        for dep in &departures {
            for cell in self.travel_time_map(
                center, *dep, date, weekday, max_secs, grid_step_m, am, buckets, slack,
                unrestricted, use_cch, rt, bike,
            ) {
                let key = quantize(cell.loc);
                let e = acc.entry(key).or_insert((0, 0, u32::MAX));
                e.0 += cell.seconds as u64;
                e.1 += 1;
                e.2 = e.2.min(cell.seconds);
            }
        }

        let n = departures.len().max(1) as u64;
        acc.into_iter()
            .map(|((qlat, qlng), (sum, count, min))| {
                let seconds = match agg {
                    TravelAggregation::Best => min,
                    // Mean over ALL sampled departures: a departure on which the cell
                    // was NOT reachable within `max_secs` counts as `max_secs` (the
                    // cap), so a cell reachable only sometimes reads as slower-on-
                    // average rather than spuriously fast. Cells never reachable on
                    // any departure are absent from `acc` and so are omitted.
                    TravelAggregation::Average => {
                        let missed = n.saturating_sub(count as u64);
                        ((sum + missed * max_secs as u64) / n) as u32
                    }
                };
                TravelCell {
                    loc: dequantize(qlat, qlng),
                    seconds,
                }
            })
            .collect()
    }

    /// Resolve the effective grid step (metres) for a fill, applying the safety cap.
    /// `req_step_m` is the requested per-query step (already clamped by the resolver);
    /// it is floored at 1 m. If a grid at that step over the reachable bounding box
    /// `[min_lat,max_lat]×[min_lng,max_lng]` would produce more than
    /// `travel_map_max_cells` cells, the step is COARSENED upward by
    /// `sqrt(cells / cap)` so the emitted cell count stays bounded regardless of how
    /// fine a step the client asked for. Both `fill_area` and its reference use this,
    /// so their grids stay identical.
    fn effective_grid_step_m(
        &self,
        req_step_m: f64,
        center_lat: f64,
        min_lat: f64,
        max_lat: f64,
        min_lng: f64,
        max_lng: f64,
    ) -> f64 {
        let step_m = req_step_m.max(1.0);
        let cap = self.raptor.travel_map_max_cells.max(1) as f64;
        let cos = center_lat.to_radians().cos().max(0.2);
        let dlat_step = step_m / 111_320.0;
        let dlng_step = step_m / (111_320.0 * cos);
        // Candidate cell count over the (un-snapped) box. Snapping the low corner down
        // only extends the box by <1 step, so this over-estimates by at most one row
        // and column — a safe bound for the cap.
        let n_lat = ((max_lat - min_lat) / dlat_step).ceil().max(0.0) + 1.0;
        let n_lng = ((max_lng - min_lng) / dlng_step).ceil().max(0.0) + 1.0;
        let product = n_lat * n_lng;
        if product > cap {
            // Scale the step so the (product) shrinks by the overshoot factor; cell
            // count scales ~1/step², so multiply the step by sqrt(product/cap).
            step_m * (product / cap).sqrt()
        } else {
            step_m
        }
    }

    /// Fill the reachable area on a lat/lng grid. For each grid point `P` the travel
    /// time is `min(direct walk centre->P, min over reached stops of
    /// arrival[s] - start_time + walk s->P)`, capped at `max_secs`; points beyond
    /// `max_secs` are omitted.
    ///
    /// **Inverted (OPT-A):** rather than running two full graph searches per cell
    /// (re-flooding the centre walk and constructing a fresh CCH one-to-many over all
    /// ~83k stops), this builds ONE bounded multi-source foot field over the contracted
    /// graph, seeded at every reached stop (pinned at its arrival offset) AND at the
    /// centre's snap (offset 0). Foot cost is direction-symmetric, so reading that forward
    /// field at a cell's ≤2 snap junctions reproduces exactly the per-cell "min over
    /// reached stops of arrival + walk(stop -> P)" merged with the centre's via-junction
    /// walk. The only piece the junction field cannot carry — the centre's same-super-edge
    /// direct walk (a cell on the centre's own chain) — is added per-cell as a cheap
    /// special case. Result is bit-identical to the un-inverted per-cell searches.
    fn fill_area(
        &self,
        center: LatLng,
        start_time: u32,
        max_secs: u32,
        grid_step_m: f64,
        arrivals: &[u32],
    ) -> Vec<TravelCell> {
        // Bounding box: the centre can walk `max_secs` in any direction, and can also
        // arrive at a far stop and walk out from THERE, so expand the box to cover
        // the centre's own walk circle plus every reached stop's residual walk circle.
        let mut min_lat = center.latitude;
        let mut max_lat = center.latitude;
        let mut min_lng = center.longitude;
        let mut max_lng = center.longitude;
        let mut extend = |loc: LatLng, radius_secs: u32| {
            let radius_m = radius_secs as f64 * self.raptor.walking_speed_mps;
            let dlat = radius_m / 111_320.0;
            let dlng = radius_m / (111_320.0 * loc.latitude.to_radians().cos().max(0.2));
            min_lat = min_lat.min(loc.latitude - dlat);
            max_lat = max_lat.max(loc.latitude + dlat);
            min_lng = min_lng.min(loc.longitude - dlng);
            max_lng = max_lng.max(loc.longitude + dlng);
        };
        extend(center, max_secs);
        for (stop_idx, &arr) in arrivals.iter().enumerate() {
            if arr == u32::MAX {
                continue;
            }
            let residual = max_secs.saturating_sub(arr.saturating_sub(start_time));
            if residual == 0 {
                continue;
            }
            let node = self.raptor.transit_stop_to_node[stop_idx];
            extend(self.node_loc(node), residual);
        }

        let radius = self.raptor.edge_snap_radius_m;
        let step_m = self.effective_grid_step_m(
            grid_step_m, center.latitude, min_lat, max_lat, min_lng, max_lng,
        );
        let dlat_step = step_m / 111_320.0;
        let dlng_step = step_m / (111_320.0 * center.latitude.to_radians().cos().max(0.2));

        // Anchor the lattice at the CENTRE, snapping the box's low corner down to an
        // integer number of steps from it. The grid points are then a pure function
        // of (centre, step), IDENTICAL across departures regardless of which stops a
        // given departure reached — so the window aggregation's per-cell buckets line
        // up exactly (no split cells, no spurious AVERAGE inflation).
        let snap_down = |v: f64, anchor: f64, step: f64| {
            anchor + ((v - anchor) / step).floor() * step
        };
        min_lat = snap_down(min_lat, center.latitude, dlat_step);
        min_lng = snap_down(min_lng, center.longitude, dlng_step);

        let Some(cg) = self.contracted.as_ref() else {
            // No contracted graph: no foot cost is available at all (the un-inverted path
            // returned u32::MAX for every cell's centre walk and an empty egress), so no
            // cell is ever <= max_secs. Nothing to emit.
            return Vec::new();
        };

        // Reached stops, filtered to offset <= max_secs (a stop arriving later than the
        // budget always caps out: offset + walk >= offset > max_secs), keyed by contracted
        // junction index for the multi-source flood.
        let mut stop_seeds: Vec<(usize, u32)> = Vec::new();
        for (stop_idx, &arr) in arrivals.iter().enumerate() {
            if arr == u32::MAX {
                continue;
            }
            let offset = arr.saturating_sub(start_time);
            if offset > max_secs {
                continue;
            }
            let node = self.raptor.transit_stop_to_node[stop_idx];
            let ji = cg.junction_of[node.0];
            if ji != u32::MAX {
                stop_seeds.push((ji as usize, offset));
            }
        }

        // Snap the centre ONCE. Its bounding-junction entries become offset-0 coord seeds
        // in the same flood (reproducing `walk_secs_coord_to_coord`'s via-junction term),
        // and its chain identity + prefix drive the per-cell same-chain direct term.
        let center_snap = cg.foot_snap_travel_map(self, center.latitude, center.longitude, radius);
        let coord_seeds: Vec<(usize, u32)> = center_snap
            .as_ref()
            .map(|s| s.entries.clone())
            .unwrap_or_default();

        // ONE bounded multi-source foot field for this departure.
        let field =
            self.walk_dijkstra_travel_map_field(&stop_seeds, &coord_seeds, max_secs, cg);

        let mut cells = Vec::new();
        let n_lat = ((max_lat - min_lat) / dlat_step).ceil() as i64 + 1;
        let n_lng = ((max_lng - min_lng) / dlng_step).ceil() as i64 + 1;
        for i in 0..n_lat {
            for j in 0..n_lng {
                let p = LatLng {
                    latitude: min_lat + i as f64 * dlat_step,
                    longitude: min_lng + j as f64 * dlng_step,
                };

                let Some(p_snap) = cg.foot_snap_travel_map(self, p.latitude, p.longitude, radius)
                else {
                    continue;
                };

                // O(1) read: best over P's ≤2 snap junctions of field[junction] + stub.
                // This merges the centre via-junction walk and every reached stop's
                // arrival + walk(stop -> P), all respecting the stop-sink rule.
                let mut best = u32::MAX;
                for &(dj, stub) in &p_snap.entries {
                    if let Some(&d) = field.get(&cg.junctions[dj]) {
                        let t = d.saturating_add(stub);
                        if t < best {
                            best = t;
                        }
                    }
                }

                // Centre same-super-edge direct walk: a cell on the centre's own chain can
                // walk straight along it, never via a junction — not representable in the
                // junction field. Mirrors `walk_secs_coord_to_coord`'s same-chain shortcut.
                if let Some(cs) = center_snap.as_ref() {
                    if cs.seg_start == p_snap.seg_start && cs.seg_len == p_snap.seg_len {
                        if let (Some(pc), Some(pp)) = (cs.from_ji_prefix, p_snap.from_ji_prefix) {
                            let direct = pc.abs_diff(pp);
                            if direct <= max_secs && direct < best {
                                best = direct;
                            }
                        }
                    }
                }

                if best <= max_secs {
                    cells.push(TravelCell { loc: p, seconds: best });
                }
            }
        }
        cells
    }

    /// Test-only public wrapper over [`Graph::fill_area_reference`], so a test can
    /// build reference cells from an EXTERNALLY-supplied arrival vector (e.g. the
    /// unbounded/full-egress [`Graph::stop_arrivals_reference`] output).
    #[doc(hidden)]
    pub fn fill_area_reference_from(
        &self,
        center: LatLng,
        start_time: u32,
        max_secs: u32,
        grid_step_m: f64,
        arrivals: &[u32],
    ) -> Vec<TravelCell> {
        self.fill_area_reference(center, start_time, max_secs, grid_step_m, arrivals)
    }

    /// Pre-OPT-A reference fill: the original per-cell implementation that ran TWO full
    /// graph searches per grid cell (a centre-bounded [`ContractedGraph::walk_secs_coord_to_coord`]
    /// and a fresh CCH/arena egress one-to-many over all stops). Kept only as a correctness
    /// oracle for the inverted [`Graph::fill_area`]; the two must agree cell-for-cell.
    #[doc(hidden)]
    fn fill_area_reference(
        &self,
        center: LatLng,
        start_time: u32,
        max_secs: u32,
        grid_step_m: f64,
        arrivals: &[u32],
    ) -> Vec<TravelCell> {
        let mut min_lat = center.latitude;
        let mut max_lat = center.latitude;
        let mut min_lng = center.longitude;
        let mut max_lng = center.longitude;
        let mut extend = |loc: LatLng, radius_secs: u32| {
            let radius_m = radius_secs as f64 * self.raptor.walking_speed_mps;
            let dlat = radius_m / 111_320.0;
            let dlng = radius_m / (111_320.0 * loc.latitude.to_radians().cos().max(0.2));
            min_lat = min_lat.min(loc.latitude - dlat);
            max_lat = max_lat.max(loc.latitude + dlat);
            min_lng = min_lng.min(loc.longitude - dlng);
            max_lng = max_lng.max(loc.longitude + dlng);
        };
        extend(center, max_secs);
        for (stop_idx, &arr) in arrivals.iter().enumerate() {
            if arr == u32::MAX {
                continue;
            }
            let residual = max_secs.saturating_sub(arr.saturating_sub(start_time));
            if residual == 0 {
                continue;
            }
            let node = self.raptor.transit_stop_to_node[stop_idx];
            extend(self.node_loc(node), residual);
        }

        let radius = self.raptor.edge_snap_radius_m;
        let step_m = self.effective_grid_step_m(
            grid_step_m, center.latitude, min_lat, max_lat, min_lng, max_lng,
        );
        let dlat_step = step_m / 111_320.0;
        let dlng_step = step_m / (111_320.0 * center.latitude.to_radians().cos().max(0.2));

        let snap_down = |v: f64, anchor: f64, step: f64| {
            anchor + ((v - anchor) / step).floor() * step
        };
        min_lat = snap_down(min_lat, center.latitude, dlat_step);
        min_lng = snap_down(min_lng, center.longitude, dlng_step);

        let reached: Vec<(usize, u32)> = arrivals
            .iter()
            .enumerate()
            .filter_map(|(s, &arr)| {
                (arr != u32::MAX).then(|| (s, arr.saturating_sub(start_time)))
            })
            .collect();

        let mut cells = Vec::new();
        let n_lat = ((max_lat - min_lat) / dlat_step).ceil() as i64 + 1;
        let n_lng = ((max_lng - min_lng) / dlng_step).ceil() as i64 + 1;
        for i in 0..n_lat {
            for j in 0..n_lng {
                let p = LatLng {
                    latitude: min_lat + i as f64 * dlat_step,
                    longitude: min_lng + j as f64 * dlng_step,
                };

                let mut best = self
                    .contracted
                    .as_ref()
                    .and_then(|cg| {
                        cg.walk_secs_coord_to_coord(self, center, p, radius, max_secs)
                    })
                    .unwrap_or(u32::MAX);

                if !reached.is_empty() {
                    let egress = if self.cch.is_some() {
                        self.cch_egress(self.cch.as_ref().unwrap(), p)
                    } else if let Some(cg) = self.contracted.as_ref() {
                        cg.nearby_stops_arena(self, p.latitude, p.longitude, radius, max_secs)
                    } else {
                        Vec::new()
                    };
                    if !egress.is_empty() {
                        let mut a = 0usize;
                        for &(stop, walk) in &egress {
                            while a < reached.len() && reached[a].0 < stop {
                                a += 1;
                            }
                            if a < reached.len() && reached[a].0 == stop {
                                let t = reached[a].1.saturating_add(walk);
                                if t < best {
                                    best = t;
                                }
                            }
                        }
                    }
                }

                if best <= max_secs {
                    cells.push(TravelCell { loc: p, seconds: best });
                }
            }
        }
        cells
    }

    /// Snap the centre coordinate to a bounding-junction NodeID over the contracted
    /// graph (the same arena snap `route` uses), or `None` if unsnappable / no
    /// contracted graph.
    fn arena_snap_center(&self, center: LatLng) -> Option<NodeID> {
        let cg = self.contracted.as_ref()?;
        let radius = self.raptor.edge_snap_radius_m;
        cg.foot_bounding_junction(self, center.latitude, center.longitude, radius)
    }

    /// Sampled departure times across `[start, end]` (inclusive), spaced
    /// `travel_map_window_sample_secs` apart. At least the two endpoints are always
    /// sampled; `end <= start` yields a single sample at `start`.
    fn window_departures(&self, start: u32, end: u32) -> Vec<u32> {
        let step = self.raptor.travel_map_window_sample_secs.max(1);
        if end <= start {
            return vec![start];
        }
        let mut out = Vec::new();
        let mut t = start;
        while t < end {
            out.push(t);
            t = t.saturating_add(step);
        }
        out.push(end); // always include the window end
        out
    }
}

/// Quantize a coordinate to a ~0.1 m grid so the same sampled point across window
/// departures maps to one aggregation bucket (grid points are generated identically
/// per departure, so this is exact for our own grid).
fn quantize(loc: LatLng) -> (i64, i64) {
    (
        (loc.latitude * 1e6).round() as i64,
        (loc.longitude * 1e6).round() as i64,
    )
}

fn dequantize(qlat: i64, qlng: i64) -> LatLng {
    LatLng {
        latitude: qlat as f64 / 1e6,
        longitude: qlng as f64 / 1e6,
    }
}
