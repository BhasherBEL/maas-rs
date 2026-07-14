use super::raptor_route::{Label, LabelCell, LabelRow, ModeContext, apply_delay};

use crate::{
    ingestion::gtfs::TimetableSegment,
    structures::{
        Mode, NodeID, RealtimeIndex, ReliabilityBuckets, Scenario, ScenarioBag, VehicleState,
        delay::DelayCDF,
        plan::{
            AccessAlternative, ArrivalScenario, CandidateStatus, Plan, PlanCandidate,
            PlanCoordinate, PlanLeg, PlanLegStep, PlanPlace, PlanTransitLeg, PlanTransitLegStep,
            PlanWalkLeg, PlanWalkLegStep, TransferRisk,
        },
    },
};

use super::{Graph, raptor_access::StreetProfile};

#[derive(Clone, Copy)]
struct PostHocBoarding {
    pattern: usize,
    board_pos: usize,
    alight_pos: usize,
    board_stop: usize,
    alight_stop: usize,
    route_id: usize,
    board_time: u32,
}

pub const TIGHTEN_MODE_CHAIN: u8 = 0;
pub const TIGHTEN_MODE_LAMBDA: u8 = 1;
pub const TIGHTEN_MODE_DIFF: u8 = 2;

static TIGHTEN_MODE: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(TIGHTEN_MODE_CHAIN);
static DIFF_INIT: std::sync::Once = std::sync::Once::new();

static DIFF_CHECKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DIFF_IDENTICAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DIFF_CLASS1: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DIFF_CLASS2: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DIFF_CLASS3: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DIFF_SEED_MISMATCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn tighten_mode() -> u8 {
    use std::sync::atomic::Ordering::Relaxed;
    DIFF_INIT.call_once(|| {
        if std::env::var("MAAS_TIGHTEN_DIFF").is_ok() {
            TIGHTEN_MODE.store(TIGHTEN_MODE_DIFF, Relaxed);
        } else if let Ok(m) = std::env::var("MAAS_TIGHTEN_MODE") {
            match m.as_str() {
                "chain" => TIGHTEN_MODE.store(TIGHTEN_MODE_CHAIN, Relaxed),
                "lambda" => TIGHTEN_MODE.store(TIGHTEN_MODE_LAMBDA, Relaxed),
                "diff" => TIGHTEN_MODE.store(TIGHTEN_MODE_DIFF, Relaxed),
                _ => {}
            }
        }
    });
    TIGHTEN_MODE.load(Relaxed)
}

fn long_transfer_tightening(g: &Graph) -> bool {
    static ENV: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    g.raptor.tighten_long_transfers
        || *ENV.get_or_init(|| std::env::var("MAAS_TIGHTEN_LONG_TRANSFERS").is_ok())
}

/// Transfer margins between consecutive transit legs. Negative ⇒ time-inconsistent.
fn plan_transfer_margins(legs: &[PlanLeg]) -> Vec<i32> {
    let mut margins = Vec::new();
    let mut prev_transit_end: Option<u32> = None;
    let mut walk_acc: u32 = 0;
    for l in legs {
        match l {
            PlanLeg::Transit(t) => {
                if let Some(end) = prev_transit_end {
                    margins.push(t.start as i32 - (end + walk_acc) as i32);
                }
                prev_transit_end = Some(t.end);
                walk_acc = 0;
            }
            PlanLeg::Walk(w) => {
                if prev_transit_end.is_some() {
                    walk_acc += w.duration;
                }
            }
        }
    }
    margins
}

fn legs_timing_eq(a: &[PlanLeg], b: &[PlanLeg]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).all(|(x, y)| match (x, y) {
        (PlanLeg::Transit(p), PlanLeg::Transit(q)) => {
            p.trip_id == q.trip_id && p.start == q.start && p.end == q.end
        }
        (PlanLeg::Walk(p), PlanLeg::Walk(q)) => {
            p.start == q.start && p.end == q.end && p.duration == q.duration
        }
        _ => false,
    })
}

fn plan_has_long_transfer(legs: &[PlanLeg], max_m: f64) -> bool {
    let ti: Vec<usize> = legs
        .iter()
        .enumerate()
        .filter_map(|(i, l)| matches!(l, PlanLeg::Transit(_)).then_some(i))
        .collect();
    if ti.len() < 2 {
        return false;
    }
    let (first, last) = (ti[0], *ti.last().unwrap());
    legs[first..last]
        .iter()
        .any(|l| matches!(l, PlanLeg::Walk(w) if (w.length as f64) > max_m))
}

impl Graph {
    pub(super) fn build_walk_plan_ep(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        walk_secs: u32,
        ep: Option<&super::QueryEndpoints>,
    ) -> Plan {
        self.build_street_plan_ep(
            origin,
            destination,
            start_time,
            walk_secs,
            StreetProfile::Foot,
            ep,
        )
    }

    pub(super) fn build_street_plan_ep(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        secs: u32,
        profile: StreetProfile,
        ep: Option<&super::QueryEndpoints>,
    ) -> Plan {
        let geometry = match ep {
            Some(ep) => self.street_path_geom_coords(
                ep.origin,
                ep.destination,
                profile,
            ),
            _ => self.street_path_geom(origin, destination, profile),
        };
        self.build_street_plan_geom(origin, destination, start_time, secs, profile, geometry)
    }

    fn build_street_plan_geom(
        &self,
        origin: NodeID,
        destination: NodeID,
        start_time: u32,
        secs: u32,
        profile: StreetProfile,
        geometry: Vec<PlanCoordinate>,
    ) -> Plan {
        let end = start_time + secs;
        let (speed, mode) = match profile {
            StreetProfile::Foot => (self.raptor.walking_speed_mps, Mode::Walk),
            StreetProfile::Bike => (self.raptor.cycling_speed_mps, Mode::Bike),
            StreetProfile::Car => (self.raptor.driving_speed_mps, Mode::Car),
        };
        let length = (secs as f64 * speed) as usize;

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
                duration: secs,
                length,
                cycleroute_length: None,
                elevation_gain: None,
                street_mode: mode,
                steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                    length, secs, to_place,
                ))],
                geometry,
                alternatives: vec![],
                leave_by: None,
            })],
            start: start_time,
            end,
            mode,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
            price: None,
        }
    }

    pub(super) fn build_bike_plan_ep(
        &self,
        _origin: NodeID,
        _destination: NodeID,
        start_time: u32,
        max_secs: u32,
        bike: &crate::structures::BikeCost,
        ep: Option<&super::QueryEndpoints>,
    ) -> Option<Plan> {
        let ep = ep?;
        self.build_bike_plan_arena(ep.origin, ep.destination, start_time, max_secs, bike)
    }

    fn build_bike_plan_arena(
        &self,
        origin: crate::structures::LatLng,
        destination: crate::structures::LatLng,
        start_time: u32,
        max_secs: u32,
        bike: &crate::structures::BikeCost,
    ) -> Option<Plan> {
        let cg = self.contracted.as_ref().unwrap();
        let radius = self.raptor.edge_snap_radius_m;
        let o = cg.foot_bounding_junction(self, origin.latitude, origin.longitude, radius)?;
        let d = cg.foot_bounding_junction(self, destination.latitude, destination.longitude, radius)?;
        let plan = self.multiobj_direct_plan(
            o,
            d,
            crate::structures::cost::RoutingMode::Bike,
            crate::structures::cost::LegRole::Neutral,
            bike,
            start_time,
        )?;
        if plan.end.saturating_sub(start_time) > max_secs {
            return None;
        }
        Some(plan)
    }

    pub(super) fn plan_price_of(
        &self,
        price: &crate::structures::cost::PriceValue,
        breakdown: Vec<crate::structures::plan::FareBreakdownItem>,
        brupass_savings_cents: u32,
    ) -> Option<crate::structures::plan::PlanPrice> {
        if !self.raptor.fare_model.enabled {
            return None;
        }
        let known_euros =
            price.known_cents.saturating_sub(brupass_savings_cents) as f64 / 100.0;
        // capped = known − sncb_spend + min(sncb_spend, cap); no-op when sncb_cap_cents == u32::MAX.
        let capped_sncb = price.sncb_spend_cents.min(price.sncb_cap_cents);
        let capped_cents = price
            .known_cents
            .saturating_sub(price.sncb_spend_cents)
            .saturating_add(capped_sncb)
            .saturating_sub(brupass_savings_cents);
        let capped_euros = capped_cents as f64 / 100.0;
        let mut unknown_operators = Vec::new();
        for (slot, &count) in price.unknown.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let name = self
                .raptor
                .unknown_operator_names
                .get(slot)
                .cloned()
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| format!("unknown_{slot}"));
            unknown_operators.push(if count > 1 {
                format!("{name} x{count}")
            } else {
                name
            });
        }
        Some(crate::structures::plan::PlanPrice {
            known_euros,
            capped_euros,
            unknown_operators,
            sncb_fare_km: (price.sncb_run_m > 0).then(|| price.sncb_run_m as f64 / 1000.0),
            breakdown,
        })
    }

    fn collect_posthoc_boardings(&self, arena: &[Label], start_id: u32) -> Vec<PostHocBoarding> {
        let mut boardings = Vec::new();
        let mut cur = start_id;
        while cur != u32::MAX {
            let node = &arena[cur as usize];
            let trace = node.trace;
            if !trace.is_transit() && !trace.is_transfer() {
                break; // reached the source / root
            }
            if trace.is_transit() {
                let p = trace.pattern as usize;
                let t = trace.trip as usize;
                let bp = trace.boarded_at as usize;
                let ap = trace.alighted_at as usize;
                let pat_stops = self.raptor.transit_idx_pattern_stops[p]
                    .of(&self.raptor.transit_pattern_stops);
                let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
                let times = self.raptor.transit_idx_pattern_stop_times[p]
                    .of(&self.raptor.transit_pattern_stop_times);
                let board_stop = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
                let alight_stop = self.raptor.transit_node_to_stop[pat_stops[ap].0] as usize;
                let route_id = self.raptor.transit_patterns[p].route.0 as usize;
                let board_time = times[bp * n_trips + t].departure;
                boardings.push(PostHocBoarding {
                    pattern: p,
                    board_pos: bp,
                    alight_pos: ap,
                    board_stop,
                    alight_stop,
                    route_id,
                    board_time,
                });
            }
            cur = node.parent;
        }
        boardings.reverse();
        boardings
    }

    /// Price a FIXED boarding sequence EXACTLY (identity ε-bucketing, no dominance);
    /// every operator pays its own ticket. Brupass CAP is a separate post-pass.
    fn price_boardings(
        &self,
        boardings: &[PostHocBoarding],
        weekday: u8,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> crate::structures::cost::PriceValue {
        use crate::structures::cost::{KnownEurosEpsilon, OperatorModel, PriceValue};
        // Identity ε-bucketing so cents are not quantized to the in-search 10-cent tiers.
        let mut fm = self.raptor.fare_model.clone();
        fm.known_euros_epsilon = KnownEurosEpsilon { a: 0.0, b: 0.0 };
        let ctx = crate::structures::cost::FareContext { profile: fare_profile, weekday };
        let mut price = PriceValue::ZERO;
        for b in boardings.iter() {
            let Some(&op) = self.raptor.operator_fare_of_route.get(b.route_id) else {
                continue;
            };
            let board_time = b.board_time;
            fm.charge_board(&mut price, op, board_time, &ctx);
            if let crate::structures::cost::OperatorFareId::Modeled {
                model: OperatorModel::DistanceBasePerKm { tariff, rules, airport_od_cents },
            } = op
            {
                let prior_free_m = price.sncb_run_m as f64;
                let run_board_stop = if price.sncb_run_board_stop == u32::MAX {
                    price.sncb_run_board_stop = b.board_stop as u32;
                    b.board_stop
                } else {
                    price.sncb_run_board_stop as usize
                };
                let run_m = self.sncb_fare_distance_m(
                    run_board_stop,
                    b.alight_stop,
                    b.pattern,
                    b.board_pos,
                    b.alight_pos,
                    prior_free_m,
                );
                fm.accrue_sncb_km(&mut price, tariff, run_m, &rules, &ctx, board_time);
                if airport_od_cents > 0 {
                    let is_airport = self
                        .raptor
                        .sncb_airport_stop
                        .get(b.alight_stop)
                        .copied()
                        .unwrap_or(false)
                        || self
                            .raptor
                            .sncb_airport_stop
                            .get(b.board_stop)
                            .copied()
                            .unwrap_or(false);
                    if is_airport {
                        fm.apply_sncb_airport_od(&mut price, airport_od_cents);
                    }
                }
            }
        }
        price
    }

    fn operator_display_name(&self, route_id: usize) -> String {
        self.raptor
            .transit_routes
            .get(route_id)
            .and_then(|r| self.raptor.transit_agencies.get(r.agency_id.0 as usize))
            .map(|a| a.name.trim().to_string())
            .unwrap_or_default()
    }

    fn sncb_stop_label(&self, stop: usize) -> String {
        use crate::structures::cost::Agglomeration;
        match self.raptor.sncb_stop_zone.get(stop).copied().unwrap_or(Agglomeration::None) {
            Agglomeration::Brussels => "Brussels".to_string(),
            Agglomeration::Antwerpen => "Antwerpen".to_string(),
            Agglomeration::None => self
                .raptor
                .transit_stop_names
                .get(stop)
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Itemized fare breakdown for a FIXED boarding sequence, replaying the same charge
    /// decisions as `price_boardings`. Returns the items plus the Brupass savings in cents
    /// (0 when no cap fired). Item euros (SNCB + Brupass caps applied) sum to `capped_euros`.
    fn build_breakdown(
        &self,
        boardings: &[PostHocBoarding],
        weekday: u8,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> (Vec<crate::structures::plan::FareBreakdownItem>, u32) {
        use crate::structures::cost::{
            KnownEurosEpsilon, OperatorFareId, OperatorModel, PriceValue, TravelClass,
            TimeWindowOperator,
        };
        use crate::structures::plan::FareBreakdownItem;

        let mut fm = self.raptor.fare_model.clone();
        fm.known_euros_epsilon = KnownEurosEpsilon { a: 0.0, b: 0.0 };
        let ctx = crate::structures::cost::FareContext { profile: fare_profile, weekday };
        let mut price = PriceValue::ZERO;
        let mut items: Vec<FareBreakdownItem> = Vec::new();
        // PAID-ticket Brupass tags only: `(item_index, operator_key, board_stop)`.
        // Covered / subscription / within-window items stay untagged (never replaced).
        let mut paid_tags: Vec<(usize, String, usize)> = Vec::new();

        let mut sncb_item: Option<usize> = None;
        let mut sncb_board_stop = 0usize;
        // Raw SNCB spend when the open run's item was created, so each item gets ITS
        // run's raw delta (not the journey-cumulative total).
        let mut sncb_spend_at_open: u32 = 0;
        let mut sncb_item_indices: Vec<usize> = Vec::new();

        let finalize_sncb = |items: &mut Vec<FareBreakdownItem>,
                             sncb_item: &mut Option<usize>,
                             price: &PriceValue,
                             spend_at_open: u32,
                             board_stop: usize,
                             alight_stop: usize| {
            if let Some(idx) = sncb_item.take() {
                let run_raw = price.sncb_spend_cents.saturating_sub(spend_at_open);
                let class = match fare_profile.travel_class {
                    TravelClass::First => "1st class",
                    TravelClass::Second => "2nd class",
                };
                items[idx].euros = run_raw as f64 / 100.0;
                items[idx].description = format!(
                    "SNCB {} {}->{}",
                    class,
                    self.sncb_stop_label(board_stop),
                    self.sncb_stop_label(alight_stop)
                );
            }
        };

        for b in boardings.iter() {
            let Some(&op) = self.raptor.operator_fare_of_route.get(b.route_id) else {
                continue;
            };
            let board_time = b.board_time;
            let display = self.operator_display_name(b.route_id);

            let before_known = price.known_cents;
            let before_sncb_spend = price.sncb_spend_cents;
            let was_sncb_active = price.sncb_active;
            fm.charge_board(&mut price, op, board_time, &ctx);

            match op {
                OperatorFareId::Modeled {
                    model: OperatorModel::DistanceBasePerKm { tariff, rules, airport_od_cents },
                } => {
                    // A non-contiguous re-entry (run was reset) starts a new SNCB item.
                    if !was_sncb_active {
                        finalize_sncb(
                            &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                            sncb_board_stop, b.alight_stop,
                        );
                        sncb_spend_at_open = before_sncb_spend;
                        sncb_board_stop = b.board_stop;
                        sncb_item = Some(items.len());
                        sncb_item_indices.push(items.len());
                        // Only a PAID SNCB run feeds the Brupass cap.
                        if !fare_profile.sncb_subscription {
                            paid_tags.push((items.len(), "SNCB".to_string(), b.board_stop));
                        }
                        items.push(FareBreakdownItem {
                            operator: display.clone(),
                            description: "SNCB".to_string(),
                            euros: 0.0,
                            coverage: fare_profile.sncb_subscription.then(|| "SNCB subscription".to_string()),
                        });
                    }
                    let prior_free_m = price.sncb_run_m as f64;
                    let run_board_stop = if price.sncb_run_board_stop == u32::MAX {
                        price.sncb_run_board_stop = b.board_stop as u32;
                        b.board_stop
                    } else {
                        price.sncb_run_board_stop as usize
                    };
                    let run_m = self.sncb_fare_distance_m(
                        run_board_stop,
                        b.alight_stop,
                        b.pattern,
                        b.board_pos,
                        b.alight_pos,
                        prior_free_m,
                    );
                    fm.accrue_sncb_km(&mut price, tariff, run_m, &rules, &ctx, board_time);
                    if airport_od_cents > 0 {
                        let is_airport = self
                            .raptor
                            .sncb_airport_stop
                            .get(b.alight_stop)
                            .copied()
                            .unwrap_or(false)
                            || self
                                .raptor
                                .sncb_airport_stop
                                .get(b.board_stop)
                                .copied()
                                .unwrap_or(false);
                        if is_airport {
                            fm.apply_sncb_airport_od(&mut price, airport_od_cents);
                        }
                    }
                    if !price.sncb_active {
                        let desc_alight = b.alight_stop;
                        if airport_od_cents > 0
                            && (self.raptor.sncb_airport_stop.get(b.alight_stop).copied().unwrap_or(false)
                                || self.raptor.sncb_airport_stop.get(b.board_stop).copied().unwrap_or(false))
                        {
                            if let Some(idx) = sncb_item.take() {
                                // Airport OD is a flat fare with no per-journey cap.
                                let run_raw =
                                    price.sncb_spend_cents.saturating_sub(sncb_spend_at_open);
                                items[idx].euros = run_raw as f64 / 100.0;
                                items[idx].description = "SNCB airport".to_string();
                            }
                        } else {
                            finalize_sncb(
                                &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                                sncb_board_stop, desc_alight,
                            );
                        }
                    }
                }
                OperatorFareId::Modeled {
                    model: OperatorModel::TimeWindowFlat { operator, .. },
                } => {
                    finalize_sncb(
                        &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                        sncb_board_stop, b.alight_stop,
                    );
                    let (subscribed, op_name, single_desc, card_desc, card_held) = match operator {
                        TimeWindowOperator::Stib => (
                            fare_profile.stib_subscription,
                            "STIB",
                            "STIB single (90 min)",
                            "STIB single (90 min)",
                            false,
                        ),
                        TimeWindowOperator::Delijn => (
                            fare_profile.delijn_subscription,
                            "De Lijn",
                            "De Lijn single (60 min)",
                            "De Lijn 10-journey card",
                            fare_profile.delijn_10_journey,
                        ),
                    };
                    let charged = price.known_cents.saturating_sub(before_known);
                    if subscribed {
                        items.push(FareBreakdownItem {
                            operator: op_name.to_string(),
                            description: format!("{op_name} (subscription)"),
                            euros: 0.0,
                            coverage: Some(format!("{op_name} subscription")),
                        });
                    } else if charged > 0 {
                        paid_tags.push((items.len(), op_name.to_string(), b.board_stop));
                        items.push(FareBreakdownItem {
                            operator: op_name.to_string(),
                            description: if card_held { card_desc } else { single_desc }.to_string(),
                            euros: charged as f64 / 100.0,
                            coverage: None,
                        });
                    } else {
                        items.push(FareBreakdownItem {
                            operator: op_name.to_string(),
                            description: format!("{op_name} (same ticket, within window)"),
                            euros: 0.0,
                            coverage: Some(format!("{op_name} ticket")),
                        });
                    }
                }
                OperatorFareId::Modeled {
                    model: OperatorModel::TimeWindowFlatTiered { is_express, .. },
                } => {
                    finalize_sncb(
                        &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                        sncb_board_stop, b.alight_stop,
                    );
                    let charged = price.known_cents.saturating_sub(before_known);
                    let tier = if is_express { "express" } else { "classic" };
                    let product = if fare_profile.tec_6_journey {
                        format!("TEC {tier} 6-journey")
                    } else {
                        format!("TEC {tier} single")
                    };
                    if fare_profile.tec_subscription {
                        items.push(FareBreakdownItem {
                            operator: "TEC".to_string(),
                            description: "TEC (subscription)".to_string(),
                            euros: 0.0,
                            coverage: Some("TEC subscription".to_string()),
                        });
                    } else {
                        paid_tags.push((items.len(), "TEC".to_string(), b.board_stop));
                        items.push(FareBreakdownItem {
                            operator: "TEC".to_string(),
                            description: product,
                            euros: charged as f64 / 100.0,
                            coverage: None,
                        });
                    }
                }
                OperatorFareId::Unknown { .. } => {
                    finalize_sncb(
                        &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                        sncb_board_stop, b.alight_stop,
                    );
                    items.push(FareBreakdownItem {
                        operator: if display.is_empty() { "Unknown".to_string() } else { display },
                        description: "fare not modeled".to_string(),
                        euros: 0.0,
                        coverage: Some("price unknown".to_string()),
                    });
                }
            }
        }
        if let Some(&last) = boardings.last() {
            finalize_sncb(
                &mut items, &mut sncb_item, &price, sncb_spend_at_open,
                sncb_board_stop, last.alight_stop,
            );
        }

        // Per-JOURNEY SNCB cap (Train+ peak): the SNCB items must sum to
        // min(total_sncb_raw, cap), matching `capped = known - sncb_spend +
        // min(sncb_spend, cap)`. Reduction is spilled onto trailing items.
        if price.sncb_cap_cents != u32::MAX && !sncb_item_indices.is_empty() {
            let total_raw: u32 = price.sncb_spend_cents;
            let capped_total = total_raw.min(price.sncb_cap_cents);
            if capped_total < total_raw {
                let mut remaining = capped_total;
                for &idx in &sncb_item_indices {
                    let raw = (items[idx].euros * 100.0).round() as u32;
                    let give = raw.min(remaining);
                    items[idx].euros = give as f64 / 100.0;
                    remaining = remaining.saturating_sub(give);
                }
            }
        }

        let brupass_savings = self.apply_brupass_cap(&mut items, &paid_tags);
        (items, brupass_savings)
    }

    /// Automatic Brupass cap over a built breakdown. `paid_tags` are `(item_index,
    /// operator_key, board_stop)` for PAID tickets. Fires only when the in-Brussels-zone
    /// paid items span 2+ DISTINCT operators AND Brupass is strictly cheaper than their
    /// sum. Returns the savings in cents (0 when it does not fire).
    fn apply_brupass_cap(
        &self,
        items: &mut Vec<crate::structures::plan::FareBreakdownItem>,
        paid_tags: &[(usize, String, usize)],
    ) -> u32 {
        use crate::structures::cost::Agglomeration;
        use crate::structures::plan::FareBreakdownItem;
        let brupass_cents = self.raptor.fare_model.brupass_cents;
        if brupass_cents == 0 {
            return 0;
        }
        let in_zone: Vec<&(usize, String, usize)> = paid_tags
            .iter()
            .filter(|(_, _, stop)| {
                self.raptor.sncb_stop_zone.get(*stop).copied().unwrap_or(Agglomeration::None)
                    == Agglomeration::Brussels
            })
            .collect();
        // Need 2+ DISTINCT operators for a genuine Brussels multi-operator journey.
        let mut distinct_ops: Vec<&str> = in_zone.iter().map(|(_, op, _)| op.as_str()).collect();
        distinct_ops.sort_unstable();
        distinct_ops.dedup();
        if distinct_ops.len() < 2 {
            return 0;
        }
        let sum_cents: u32 = in_zone
            .iter()
            .map(|(idx, _, _)| (items[*idx].euros * 100.0).round() as u32)
            .sum();
        // Only cap when Brupass is strictly cheaper.
        if brupass_cents >= sum_cents {
            return 0;
        }
        let mut replaced: Vec<usize> = in_zone.iter().map(|(idx, _, _)| *idx).collect();
        replaced.sort_unstable();
        let insert_at = replaced[0];
        for &idx in &replaced {
            items[idx].euros = 0.0;
            items[idx].coverage = Some("Brupass".to_string());
        }
        items.insert(
            insert_at,
            FareBreakdownItem {
                operator: "Brupass".to_string(),
                description: "Brupass (Brussels)".to_string(),
                euros: brupass_cents as f64 / 100.0,
                coverage: None,
            },
        );
        sum_cents.saturating_sub(brupass_cents)
    }

    /// Price a FINISHED plan post-hoc from its arena chain, EXACT cents, no dominance.
    /// Returns `None` when fares are disabled. The search is price-blind; price is only
    /// an annotation computed here.
    pub(super) fn plan_price_posthoc(
        &self,
        arena: &[Label],
        start_id: u32,
        weekday: u8,
        fare_profile: crate::structures::cost::FareProfile,
    ) -> Option<crate::structures::plan::PlanPrice> {
        if !self.raptor.fare_model.enabled {
            return None;
        }
        let boardings = self.collect_posthoc_boardings(arena, start_id);
        let price = self.price_boardings(&boardings, weekday, fare_profile);
        let (breakdown, brupass_savings) =
            self.build_breakdown(&boardings, weekday, fare_profile);
        self.plan_price_of(&price, breakdown, brupass_savings)
    }

    const EXTREME_RISK_RELIABILITY: f32 = 0.10;
    const EXTREME_RISK_WAIT_SECS: u32 = 7200;
    const TIGHTEN_MIN_RELIABILITY: f32 = 0.80;

    pub(super) fn is_extreme_risk(plan: &Plan) -> bool {
        plan.legs.iter().any(|leg| {
            if let PlanLeg::Transit(t) = leg
                && let Some(ref risk) = t.transfer_risk
                && risk.reliability < Self::EXTREME_RISK_RELIABILITY
            {
                let wait = risk
                    .next_departure
                    .map(|nd| nd.saturating_sub(risk.scheduled_departure))
                    .unwrap_or(u32::MAX);
                return wait > Self::EXTREME_RISK_WAIT_SECS;
            }
            false
        })
    }

    /// Arrival distribution and expected arrival from a scenario bag. `time == u32::MAX`
    /// scenarios (missed connection, no later trip) MUST be excluded before the delay-CDF
    /// convolution, else it shifts the sentinel to a bogus finite time. `expected_end` is
    /// conditioned on arriving; falls back to `fallback_end` when nothing is reachable.
    pub(crate) fn arrival_stats(
        bag: &ScenarioBag,
        cdf: Option<&DelayCDF>,
        fallback_end: u32,
    ) -> (Vec<ArrivalScenario>, u32) {
        let reachable: Vec<Scenario> = bag
            .scenarios()
            .iter()
            .copied()
            .filter(|s| s.time != u32::MAX)
            .collect();

        let dist: Vec<ArrivalScenario> = match cdf {
            Some(cdf) if !cdf.bins.is_empty() => {
                let mut dist = Vec::with_capacity(reachable.len() * cdf.bins.len());
                let mut prev_cum = 0.0f32;
                for &(delay, cum_prob) in &cdf.bins {
                    let bin_mass = cum_prob - prev_cum;
                    if bin_mass > 0.0 {
                        for s in &reachable {
                            dist.push(ArrivalScenario {
                                time: s.time.saturating_add_signed(delay),
                                probability: s.prob * bin_mass,
                            });
                        }
                    }
                    prev_cum = cum_prob;
                }
                dist.sort_by_key(|s| s.time);
                dist
            }
            _ => reachable
                .iter()
                .map(|s| ArrivalScenario {
                    time: s.time,
                    probability: s.prob,
                })
                .collect(),
        };

        let mass: f64 = dist.iter().map(|s| s.probability as f64).sum();
        let expected_end = if mass > 0.0 {
            (dist
                .iter()
                .map(|s| s.time as f64 * s.probability as f64)
                .sum::<f64>()
                / mass) as u32
        } else {
            fallback_end
        };
        (dist, expected_end)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn extract_with_debug<R: LabelRow>(
        &self,
        mc: &ModeContext,
        start_time: u32,
        date: u32,
        weekday: u8,
        labels: &[R],
        buckets: &ReliabilityBuckets,
        origin: NodeID,
        destination: NodeID,
        rt: &RealtimeIndex,
        mut debug_sink: Option<&mut Vec<PlanCandidate>>,
        departure_stamp: u32,
        arena: &[Label],
        onboard: bool,
        bw_cache: &mut std::collections::HashMap<(usize, u32, usize, u32, u8), Vec<Vec<u32>>>,
    ) -> Vec<Plan> {
        use super::MAX_ROUNDS;

        let n_states = mc.n_states();
        // Mode class: walk-rooted plans must not be suppressed by faster bike-state
        // arrivals (burden comparison happens at plan level).
        let class_of = |vs: VehicleState| -> usize {
            match vs {
                VehicleState::Walked => 0,
                VehicleState::BikeInHand | VehicleState::BikeDropped | VehicleState::BikeEgress => 1,
                VehicleState::CarParked | VehicleState::CarEgress => 2,
            }
        };
        let n_classes = 3;

        let mut candidates: Vec<Plan> = Vec::new();
        // Parallel to `candidates`: index in `debug_sink` (dummy when None so the zip works).
        let mut sink_indices: Vec<usize> = Vec::new();
        // Best arrival per (mode class, reliability bucket): the cross-round pruning bound.
        let n_buckets = buckets.bucket(1.0) as usize + 1;
        let n_keys = n_classes * n_buckets;
        let mut bucket_best = vec![u32::MAX; n_keys];

        for k in 0..=MAX_ROUNDS {
            let mut per_key: Vec<Option<(u32, bool, usize, u32, usize)>> = vec![None; n_keys];
            for (sidx, vs) in mc.am.states() {
                let class = class_of(vs);
                for &(s, w) in &mc.egress[sidx] {
                    let set = labels[k].cell(s * n_states + sidx);
                    for i in 0..set.count() {
                        let sm = set.summary_at(i);
                        if sm.created_by != departure_stamp {
                            continue;
                        }
                        let b = buckets.bucket(sm.reliability) as usize;
                        let key = class * n_buckets + b;
                        let arr = sm.earliest.saturating_add(w);
                        let full = set.full_at(i, arena);
                        let intra_member_transfer = full.trace.is_transfer()
                            && mc
                                .dest_station
                                .as_ref()
                                .is_some_and(|m| m.contains(&(full.trace.from_stop as usize)));
                        match per_key[key] {
                            Some((cur_arr, cur_intra, ..))
                                if (cur_arr, cur_intra) <= (arr, intra_member_transfer) => {}
                            _ => per_key[key] = Some((arr, intra_member_transfer, s, w, sidx)),
                        }
                    }
                }
            }

            for key in 0..n_keys {
                let b = key % n_buckets;
                let (best_arr, _best_intra, best_stop, best_walk, dest_sidx) = match per_key[key] {
                    Some(t) => t,
                    None => continue,
                };

                if best_arr >= bucket_best[key] {
                    if let Some(ref mut sink) = debug_sink {
                        sink.push(PlanCandidate {
                            round: k,
                            origin_departure: start_time,
                            plan: None,
                            status: CandidateStatus::NotImproving,
                        });
                    }
                    continue;
                }
                let cell = best_stop * n_states + dest_sidx;
                let chosen = Self::pick_label(
                    &labels[k].cell(cell),
                    buckets,
                    b as u8,
                    departure_stamp,
                    arena,
                );

                // Drop zero-transit chains BEFORE committing `bucket_best`: committing
                // first would let a degenerate walk-chain poison the (class, bucket)
                // cross-round bound and suppress a genuine transit candidate later.
                if !chosen.is_some_and(|l| Self::chain_has_transit(arena, l.arena_id)) {
                    if let Some(ref mut sink) = debug_sink {
                        sink.push(PlanCandidate {
                            round: k,
                            origin_departure: start_time,
                            plan: None,
                            status: CandidateStatus::ReconstructionEmpty,
                        });
                    }
                    continue;
                }
                bucket_best[key] = best_arr;

                let chosen_bag = chosen.map(|l| l.bag).unwrap_or(ScenarioBag::EMPTY);
                let chosen_rt = chosen.and_then(|l| l.route_type);

                let (mut legs, origin_stop, root_state) = match chosen {
                    Some(l) => self.reconstruct(arena, l.arena_id, date, weekday, rt),
                    None => (Vec::new(), best_stop, 0),
                };

                let root_vs = mc.am.state_at(root_state as usize);
                let dest_vs = mc.am.state_at(dest_sidx);
                let mode = match root_vs {
                    VehicleState::Walked => Mode::WalkTransit,
                    VehicleState::CarParked => Mode::CarDropOff,
                    VehicleState::CarEgress => Mode::CarPickup,
                    VehicleState::BikeEgress => Mode::BikePickup,
                    VehicleState::BikeInHand | VehicleState::BikeDropped => match dest_vs {
                        VehicleState::BikeInHand if mc.am.selected(Mode::BikeOnTransit) => {
                            Mode::BikeOnTransit
                        }
                        VehicleState::BikeInHand => Mode::BikeTransit,
                        _ if mc.am.selected(Mode::BikeToTransit) => Mode::BikeToTransit,
                        _ => Mode::BikeTransit,
                    },
                };

                let transit_count = legs
                    .iter()
                    .filter(|l| matches!(l, PlanLeg::Transit(_)))
                    .count();
                // The backward pass is bike-unaware, so only walk-rooted plans are tightened.
                if transit_count > 0 && mode == Mode::WalkTransit {
                    let target = best_arr.saturating_sub(best_walk);
                    let tmode = tighten_mode();
                    if tmode == TIGHTEN_MODE_CHAIN {
                        let bounds = super::latency_profile::time_backward(|| {
                            self.chain_bounds(&legs, best_stop, target, date, weekday, rt)
                        });
                        self.tighten_with_bounds(
                            &mut legs, &bounds, date, weekday, rt, onboard, true,
                        );
                    } else {
                        let bw_key = (best_stop, target, transit_count, date, weekday);
                        let lambda = bw_cache.entry(bw_key).or_insert_with(|| {
                            super::latency_profile::time_backward(|| {
                                self.raptor_backward(
                                    bw_key.0, bw_key.1, bw_key.2, bw_key.3, bw_key.4, rt,
                                )
                            })
                        });
                        let bounds_lambda = self.bounds_from_lambda(&legs, lambda);
                        if tmode == TIGHTEN_MODE_DIFF {
                            let bounds_chain =
                                self.chain_bounds(&legs, best_stop, target, date, weekday, rt);
                            self.tighten_diff_check(
                                &legs,
                                &bounds_lambda,
                                &bounds_chain,
                                date,
                                weekday,
                                rt,
                                onboard,
                            );
                            self.tighten_with_bounds(
                                &mut legs,
                                &bounds_chain,
                                date,
                                weekday,
                                rt,
                                onboard,
                                true,
                            );
                        } else {
                            self.tighten_with_bounds(
                                &mut legs,
                                &bounds_lambda,
                                date,
                                weekday,
                                rt,
                                onboard,
                                false,
                            );
                        }
                    }
                }

                self.apply_realtime(&mut legs, rt, onboard);

                // After tighten + realtime settle the times, so the outbound margin matches.
                Self::link_following_connections(&mut legs);

                let (access_profile, access_mode) = match root_vs {
                    VehicleState::Walked | VehicleState::CarEgress | VehicleState::BikeEgress => {
                        (StreetProfile::Foot, Mode::Walk)
                    }
                    VehicleState::BikeInHand | VehicleState::BikeDropped => {
                        (StreetProfile::Bike, Mode::Bike)
                    }
                    VehicleState::CarParked => (StreetProfile::Car, Mode::Car),
                };
                if let Some(&(_, first_walk)) = mc.access[root_state as usize]
                    .iter()
                    .find(|&&(s, _)| s == origin_stop)
                {
                    if first_walk > 0 {
                        let stop_node = self.raptor.transit_stop_to_node[origin_stop];
                        let board = legs
                            .first()
                            .map(|l| match l {
                                PlanLeg::Transit(t) => t.start,
                                PlanLeg::Walk(w) => w.start,
                            })
                            .unwrap_or(start_time + first_walk);
                        let speed = match access_profile {
                            StreetProfile::Foot => self.raptor.walking_speed_mps,
                            StreetProfile::Bike => self.raptor.cycling_speed_mps,
                            StreetProfile::Car => self.raptor.driving_speed_mps,
                        };
                        let length = (first_walk as f64 * speed) as usize;
                        let walk_start = board.saturating_sub(first_walk).max(start_time);
                        let to_place = PlanPlace {
                            node_id: stop_node,
                            stop_position: None,
                            arrival: Some(walk_start + first_walk),
                            departure: None,
                        };
                        let access_leg = PlanWalkLeg {
                            from: PlanPlace {
                                node_id: origin,
                                stop_position: None,
                                arrival: None,
                                departure: Some(walk_start),
                            },
                            to: to_place,
                            start: walk_start,
                            end: walk_start + first_walk,
                            duration: first_walk,
                            length,
                            cycleroute_length: None,
                            elevation_gain: None,
                            street_mode: access_mode,
                            steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                                length, first_walk, to_place,
                            ))],
                            geometry: self.street_path_geom(origin, stop_node, access_profile),
                            alternatives: vec![],
                            leave_by: None,
                        };
                        legs.insert(0, PlanLeg::Walk(access_leg));
                    }
                }

                if best_walk > 0 {
                    let (egress_profile, egress_mode) = match dest_vs {
                        VehicleState::BikeInHand | VehicleState::BikeEgress => {
                            (StreetProfile::Bike, Mode::Bike)
                        }
                        VehicleState::CarEgress => (StreetProfile::Car, Mode::Car),
                        _ => (StreetProfile::Foot, Mode::Walk),
                    };
                    let alight = chosen_bag.earliest();
                    let stop_node = self.raptor.transit_stop_to_node[best_stop];
                    let speed = match egress_profile {
                        StreetProfile::Foot => self.raptor.walking_speed_mps,
                        StreetProfile::Bike => self.raptor.cycling_speed_mps,
                        StreetProfile::Car => self.raptor.driving_speed_mps,
                    };
                    let length = (best_walk as f64 * speed) as usize;
                    let to_place = PlanPlace {
                        node_id: destination,
                        stop_position: None,
                        arrival: Some(alight + best_walk),
                        departure: None,
                    };
                    let egress_leg = PlanWalkLeg {
                        from: PlanPlace {
                            node_id: stop_node,
                            stop_position: None,
                            arrival: None,
                            departure: Some(alight),
                        },
                        to: to_place,
                        start: alight,
                        end: alight + best_walk,
                        duration: best_walk,
                        length,
                        street_mode: egress_mode,
                        cycleroute_length: None,
                        elevation_gain: None,
                        steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                            length, best_walk, to_place,
                        ))],
                        geometry: self.street_path_geom(stop_node, destination, egress_profile),
                        alternatives: vec![],
                        leave_by: None,
                    };
                    legs.push(PlanLeg::Walk(egress_leg));
                }

                let (departure, arrival) = Self::plan_timeline(&mut legs);

                let arrival_bag = chosen_bag.shifted_by(best_walk);
                let (arrival_distribution, expected_end) = Self::arrival_stats(
                    &arrival_bag,
                    chosen_rt.and_then(|rt| self.raptor.transit_delay_models.get(&rt)),
                    arrival,
                );
                // Expected arrival must never precede the deterministic realtime arrival.
                let expected_end = expected_end.max(arrival);
                let price = chosen
                    .and_then(|l| self.plan_price_posthoc(arena, l.arena_id, weekday, mc.fare_profile));
                let plan = Plan {
                    legs: Self::merge_consecutive_walks(legs),
                    start: departure,
                    end: arrival,
                    mode,
                    access_alternatives: vec![],
                    arrival_distribution,
                    expected_end,
                    price,
                };

                if let Some(ref mut sink) = debug_sink {
                    sink_indices.push(sink.len());
                    sink.push(PlanCandidate {
                        round: k,
                        origin_departure: start_time,
                        plan: Some(plan.clone()),
                        status: CandidateStatus::Kept,
                    });
                } else {
                    sink_indices.push(candidates.len()); // dummy
                }
                candidates.push(plan);
            }
        }

        if candidates.iter().any(|p| !Self::is_extreme_risk(p)) {
            let mut new_candidates = Vec::new();
            for (plan, si) in candidates.into_iter().zip(sink_indices) {
                if Self::is_extreme_risk(&plan) {
                    if let Some(ref mut sink) = debug_sink {
                        sink[si].status = CandidateStatus::ExtremeRisk;
                    }
                } else {
                    new_candidates.push(plan);
                }
            }
            candidates = new_candidates;
        }

        candidates
    }

    /// Re-chains any walk leg that follows another leg onto that leg's end (preserving
    /// duration), then returns `(plan.start, plan.end)`. The first leg is the anchor and
    /// is never shifted. Keeps the timeline monotonic after a realtime delay shifts an
    /// egress walk's scheduled base; a no-op for schedule-only plans.
    pub(super) fn plan_timeline(legs: &mut [PlanLeg]) -> (u32, u32) {
        let mut cursor: Option<u32> = None;
        for leg in legs.iter_mut() {
            match leg {
                PlanLeg::Walk(w) => {
                    if let Some(prev_end) = cursor {
                        let dur = w.duration;
                        w.start = prev_end;
                        w.end = prev_end + dur;
                        w.from.departure = Some(w.start);
                        w.to.arrival = Some(w.end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(w.end);
                            }
                        }
                    }
                    cursor = Some(w.end);
                }
                PlanLeg::Transit(t) => cursor = Some(t.end),
            }
        }
        let leg_start = |l: &PlanLeg| match l {
            PlanLeg::Walk(w) => w.start,
            PlanLeg::Transit(t) => t.start,
        };
        let leg_end = |l: &PlanLeg| match l {
            PlanLeg::Walk(w) => w.end,
            PlanLeg::Transit(t) => t.end,
        };
        let start = legs.first().map(leg_start).unwrap_or(0);
        let end = legs.last().map(leg_end).unwrap_or(start);
        (start, end)
    }

    pub(super) fn merge_consecutive_walks(legs: Vec<PlanLeg>) -> Vec<PlanLeg> {
        let mut out: Vec<PlanLeg> = Vec::with_capacity(legs.len());
        for leg in legs {
            match (out.last_mut(), &leg) {
                (Some(PlanLeg::Walk(prev)), PlanLeg::Walk(next))
                    if prev.street_mode == next.street_mode
                        && prev.alternatives.is_empty()
                        && next.alternatives.is_empty() =>
                {
                    let mut merged_geo = prev.geometry.clone();
                    if merged_geo.last().map(|c| (c.lat, c.lon))
                        == next.geometry.first().map(|c| (c.lat, c.lon))
                    {
                        merged_geo.extend_from_slice(&next.geometry[1..]);
                    } else {
                        merged_geo.extend_from_slice(&next.geometry);
                    }
                    let new_duration = prev.duration + next.duration;
                    let new_length = prev.length + next.length;
                    let new_end = next.end;
                    let to = next.to;
                    let step =
                        PlanLegStep::Walk(PlanWalkLegStep::plain(new_length, new_duration, to));
                    let prev_alternatives = prev.alternatives.clone();
                    let prev_leave_by = prev.leave_by;
                    *prev = PlanWalkLeg {
                        from: prev.from,
                        to,
                        start: prev.start,
                        end: new_end,
                        duration: new_duration,
                        length: new_length,
                        cycleroute_length: None,
                        elevation_gain: None,
                        street_mode: prev.street_mode,
                        steps: vec![step],
                        geometry: merged_geo,
                        alternatives: prev_alternatives,
                        leave_by: prev_leave_by,
                    };
                }
                _ => out.push(leg),
            }
        }
        out
    }

    /// Whether the arena parent chain rooted at `start_id` contains any transit trace.
    /// Equivalent to "reconstruct would emit ≥1 Transit leg", without building any legs.
    fn chain_has_transit(arena: &[Label], start_id: u32) -> bool {
        let mut cur = start_id;
        while cur != u32::MAX {
            let node = &arena[cur as usize];
            if node.trace.is_transit() {
                return true;
            }
            cur = node.parent;
        }
        false
    }

    fn pick_label<C: LabelCell>(
        set: &C,
        buckets: &ReliabilityBuckets,
        b: u8,
        stamp: u32,
        arena: &[Label],
    ) -> Option<Label> {
        // Primary: first current-stamp member in bucket `b`.
        for i in 0..set.count() {
            let sm = set.summary_at(i);
            if sm.created_by == stamp && buckets.bucket(sm.reliability) == b {
                return Some(set.full_at(i, arena));
            }
        }
        // Fallback: min-earliest among current-stamp members (first-wins on ties).
        let mut best: Option<(usize, u32)> = None;
        for i in 0..set.count() {
            let sm = set.summary_at(i);
            if sm.created_by != stamp {
                continue;
            }
            match best {
                Some((_, be)) if be <= sm.earliest => {}
                _ => best = Some((i, sm.earliest)),
            }
        }
        best.map(|(i, _)| set.full_at(i, arena))
    }

    /// Rebuilds the ordered legs of a journey by following exact parent pointers through
    /// the per-pass `arena` from the destination label `start_id`.
    pub(super) fn reconstruct(
        &self,
        arena: &[Label],
        start_id: u32,
        date: u32,
        weekday: u8,
        // Named `realtime` (not `rt`) because the transfer-risk block below binds a
        // local `rt: RouteType` that would otherwise shadow it.
        realtime: &RealtimeIndex,
    ) -> (Vec<PlanLeg>, usize, u8) {
        let mut legs = Vec::new();
        let mut origin_stop = 0usize;
        let mut cur = start_id;

        while cur != u32::MAX {
            let node = &arena[cur as usize];
            let trace = node.trace;
            if !trace.is_transit() && !trace.is_transfer() {
                break; // reached the source / root
            }
            let parent = node.parent;
            let parent_node = if parent != u32::MAX {
                Some(&arena[parent as usize])
            } else {
                None
            };

            if trace.is_transfer() {
                let from = trace.from_stop as usize;
                let to = node.at_stop as usize;
                let start = parent_node.map(|l| l.bag.earliest()).unwrap_or(0);
                let end = node.bag.earliest();
                let duration = end.saturating_sub(start);
                let from_node = self.raptor.transit_stop_to_node[from];
                let to_node = self.raptor.transit_stop_to_node[to];
                let length = (duration as f64 * self.raptor.walking_speed_mps) as usize;

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
                    cycleroute_length: None,
                    elevation_gain: None,
                    street_mode: Mode::Walk,
                    steps: vec![PlanLegStep::Walk(PlanWalkLegStep::plain(
                        length, duration, to_place,
                    ))],
                    geometry: self.street_path_geom(from_node, to_node, StreetProfile::Foot),
                    alternatives: vec![],
                    leave_by: None,
                }));
                origin_stop = from;
                cur = parent;
                continue;
            }

            let p = trace.pattern as usize;
            let t = trace.trip as usize;
            let bp = trace.boarded_at as usize;
            let ap = trace.alighted_at as usize;

            let pat_stops =
                self.raptor.transit_idx_pattern_stops[p].of(&self.raptor.transit_pattern_stops);
            let n_trips = self.raptor.transit_patterns[p].num_trips as usize;
            let times = self.raptor.transit_idx_pattern_stop_times[p]
                .of(&self.raptor.transit_pattern_stop_times);
            let trip_ids =
                self.raptor.transit_idx_pattern_trips[p].of(&self.raptor.transit_pattern_trips);

            let board_dep = times[bp * n_trips + t].departure;
            let alight_arr = times[ap * n_trips + t].arrival;

            let bs = self.raptor.transit_node_to_stop[pat_stops[bp].0] as usize;
            let boarding_col = &times[bp * n_trips..(bp + 1) * n_trips];

            let preceding_rt = parent_node.and_then(|l| l.route_type);
            let preceding_arr = parent_node.map(|l| l.bag.earliest());

            let transfer_risk = if let (Some(rt), Some(arrival_at_bs)) =
                (preceding_rt, preceding_arr)
            {
                let margin = board_dep as i32 - arrival_at_bs as i32;
                let next_departure = self.next_active_trip_departure(
                    trip_ids,
                    t + 1,
                    boarding_col,
                    date,
                    weekday,
                    realtime,
                );
                let board = self
                    .route_type_of_trip(trip_ids[t])
                    .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                let (reliability, next_reliability) =
                    match self.raptor.transit_delay_models.get(&rt) {
                        Some(cdf) => (
                            cdf.prob_on_time_vs(board, margin),
                            next_departure.map(|nd| {
                                cdf.prob_on_time_vs(board, nd as i32 - arrival_at_bs as i32)
                            }),
                        ),
                        None => (1.0, None),
                    };
                Some(TransferRisk {
                    reliability,
                    scheduled_departure: board_dep,
                    next_departure,
                    next_reliability,
                    margin_secs: Some(margin),
                })
            } else {
                None
            };

            let mut steps = Vec::with_capacity(ap - bp);
            let mut total_length = 0usize;
            for s in (bp + 1)..=ap {
                let seg_len = self.transit_seg_length(pat_stops[s - 1], pat_stops[s]);
                total_length += seg_len;

                let arr = times[s * n_trips + t].arrival;
                let prev_dep = times[(s - 1) * n_trips + t].departure;

                let timetable_segment = {
                    let t = self
                        .raptor
                        .transit_pattern_segment_timetables
                        .get(p)
                        .and_then(|segs| segs.get(s - 1).copied());
                    debug_assert!(t.is_some(), "contracted segment-timetable side-table miss (pattern {p})");
                    t
                }
                .unwrap_or(TimetableSegment { start: 0, len: 0 });

                let departure_index = if s == bp + 1 {
                    self.raptor.transit_departures
                        [timetable_segment.start..timetable_segment.start + timetable_segment.len]
                        .iter()
                        .position(|ts| ts.trip_id == trip_ids[t])
                        .map(|i| timetable_segment.start + i)
                        .unwrap_or(timetable_segment.start)
                } else {
                    0
                };

                steps.push(PlanLegStep::Transit(PlanTransitLegStep {
                    length: seg_len,
                    time: arr - prev_dep,
                    place: crate::structures::plan::PlanPlace {
                        node_id: pat_stops[s],
                        stop_position: Some(s as u32),
                        arrival: Some(arr),
                        departure: if s < ap {
                            Some(times[s * n_trips + t].departure)
                        } else {
                            None
                        },
                    },
                    scheduled_arrival: Some(arr),
                    scheduled_departure: if s < ap {
                        Some(times[s * n_trips + t].departure)
                    } else {
                        None
                    },
                    date,
                    weekday,
                    timetable_segment,
                    departure_index,
                }));
            }

            let transit_geometry: Vec<crate::structures::plan::PlanCoordinate> =
                match self.get_pattern_shape(p) {
                    Some((shape_pts, stop_idx)) => {
                        let from = stop_idx[bp] as usize;
                        let to = stop_idx[ap] as usize;
                        shape_pts[from..=to]
                            .iter()
                            .map(|coord| crate::structures::plan::PlanCoordinate {
                                lat: coord.latitude,
                                lon: coord.longitude,
                            })
                            .collect()
                    }
                    None => (bp..=ap)
                        .map(|s| {
                            let loc = self.node_loc(pat_stops[s]);
                            crate::structures::plan::PlanCoordinate {
                                lat: loc.latitude,
                                lon: loc.longitude,
                            }
                        })
                        .collect(),
                };

            legs.push(PlanLeg::Transit(PlanTransitLeg {
                from: PlanPlace {
                    stop_position: Some(bp as u32),
                    arrival: Some(times[bp * n_trips + t].arrival),
                    departure: Some(board_dep),
                    node_id: pat_stops[bp],
                },
                to: PlanPlace {
                    stop_position: Some(ap as u32),
                    arrival: Some(alight_arr),
                    departure: Some(times[ap * n_trips + t].departure),
                    node_id: pat_stops[ap],
                },
                start: board_dep,
                end: alight_arr,
                scheduled_start: board_dep,
                scheduled_end: alight_arr,
                realtime: false,
                trip_id: trip_ids[t],
                length: total_length,
                duration: alight_arr - board_dep,
                steps,
                geometry: transit_geometry,
                transfer_risk,
                preceding_arrival: if preceding_rt.is_none() {
                    None
                } else {
                    preceding_arr
                },
                preceding_route_type: preceding_rt,
                route_type: self.route_type_of_trip(trip_ids[t]),
                // Populated later by `link_following_connections`.
                following_route_type: None,
                following_margin_secs: None,
                bikes_allowed: self.get_trip(trip_ids[t]).and_then(|t| t.bikes_allowed),
                time_shift: 0,
            }));

            origin_stop = bs;
            cur = parent;
        }

        legs.reverse();

        // `cur` is the source/root label; its state identifies the access profile.
        let root_state = arena.get(cur as usize).map(|l| l.state).unwrap_or(0);

        (legs, origin_stop, root_state)
    }

    /// Fills `following_route_type` / `following_margin_secs` on each transit leg from
    /// the next transit leg. The margin is next boarding − this leg's scheduled arrival −
    /// intervening transfer walk. Requires forward-ordered legs (transit/transfer only).
    fn link_following_connections(legs: &mut [PlanLeg]) {
        let transit: Vec<(usize, u32, Option<gtfs_structures::RouteType>)> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| match l {
                PlanLeg::Transit(t) => Some((i, t.scheduled_start, t.route_type)),
                _ => None,
            })
            .collect();

        for w in transit.windows(2) {
            let (i, _, _) = w[0];
            let (j, next_start, next_rt) = w[1];
            let walk: u32 = legs[i + 1..j]
                .iter()
                .map(|l| match l {
                    PlanLeg::Walk(wk) => wk.duration,
                    _ => 0,
                })
                .sum();
            if let PlanLeg::Transit(t) = &mut legs[i] {
                t.following_route_type = next_rt;
                t.following_margin_secs =
                    Some(next_start as i32 - t.scheduled_end as i32 - walk as i32);
            }
        }
    }

    /// Realtime post-pass: rewrite each transit leg's times to effective (scheduled +
    /// live delay), re-chain the timeline, and recompute transfer reliability on the new
    /// margins. No-op with an empty index. Runs before access/egress walks are attached,
    /// so `legs` is the transit/transfer chain only.
    pub(super) fn apply_realtime(&self, legs: &mut [PlanLeg], rt: &RealtimeIndex, onboard: bool) {
        if rt.is_empty() {
            return;
        }
        let compact = |node: NodeID| -> Option<u32> {
            let c = self.raptor.transit_node_to_stop[node.0];
            if c == u32::MAX { None } else { Some(c) }
        };

        let mut cursor: Option<u32> = None;
        let mut first_transit = true;
        for leg in legs.iter_mut() {
            match leg {
                PlanLeg::Transit(t) => {
                    let is_first_transit = first_transit;
                    first_transit = false;

                    // INVARIANT: a reconstructed plan may carry a CANCELED transit leg
                    // only as the ONBOARD first leg; any other means the search boarded
                    // a dead trip (regression tripwire below).
                    let canceled = rt.is_canceled(t.trip_id);
                    debug_assert!(
                        !canceled || (onboard && is_first_transit),
                        "apply_realtime: a non-onboard transit leg is CANCELED (trip {:?}) — \
                         the boarding guards should have prevented this",
                        t.trip_id,
                    );
                    if canceled && onboard && is_first_transit {
                        // Cancellation outranks stale per-stop delays: keep scheduled
                        // times, don't flag realtime. The boarded trip is the user's reality.
                        t.scheduled_start = t.start;
                        t.scheduled_end = t.end;
                        t.realtime = false;
                        cursor = Some(t.end);
                        continue;
                    }

                    let board = compact(t.from.node_id);
                    let alight = compact(t.to.node_id);
                    let d_board = board.map_or(0, |s| rt.delay(t.trip_id, s));
                    let d_alight = alight.map_or(0, |s| rt.delay(t.trip_id, s));
                    let has_rt = board.is_some_and(|s| rt.delay_opt(t.trip_id, s).is_some())
                        || alight.is_some_and(|s| rt.delay_opt(t.trip_id, s).is_some());

                    t.scheduled_start = t.start;
                    t.scheduled_end = t.end;
                    t.start = apply_delay(t.start, d_board);
                    t.end = apply_delay(t.end, d_alight);
                    t.realtime = has_rt;
                    t.duration = t.end.saturating_sub(t.start);
                    t.from.departure = Some(t.start);
                    t.to.arrival = Some(t.end);
                    t.from.arrival = t.from.arrival.map(|a| apply_delay(a, d_board));
                    t.to.departure = t.to.departure.map(|x| apply_delay(x, d_alight));

                    for step in t.steps.iter_mut() {
                        if let PlanLegStep::Transit(s) = step
                            && let Some(sc) = compact(s.place.node_id)
                        {
                            let d = rt.delay(t.trip_id, sc);
                            s.place.arrival = s.place.arrival.map(|a| apply_delay(a, d));
                            s.place.departure = s.place.departure.map(|x| apply_delay(x, d));
                        }
                    }

                    if let (Some(prev_arr), Some(prt)) = (cursor, t.preceding_route_type) {
                        let margin = t.start as i32 - prev_arr as i32;
                        let next_dep = t.transfer_risk.as_ref().and_then(|r| r.next_departure);
                        let board = t
                            .route_type
                            .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                        let (rel, next_rel) = match self.raptor.transit_delay_models.get(&prt) {
                            Some(cdf) => (
                                cdf.prob_on_time_vs(board, margin),
                                next_dep.map(|nd| {
                                    cdf.prob_on_time_vs(board, nd as i32 - prev_arr as i32)
                                }),
                            ),
                            None => (1.0, None),
                        };
                        t.preceding_arrival = Some(prev_arr);
                        t.transfer_risk = Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: t.scheduled_start,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                            margin_secs: Some(margin),
                        });
                    }
                    cursor = Some(t.end);
                }
                PlanLeg::Walk(w) => {
                    if let Some(prev) = cursor {
                        let dur = w.duration;
                        w.start = prev;
                        w.end = prev + dur;
                        w.from.departure = Some(w.start);
                        w.to.arrival = Some(w.end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(w.end);
                            }
                        }
                        cursor = Some(w.end);
                    }
                }
            }
        }
    }

    /// Latest alighting (≤ `max_alighting`) whose onward transfer stays at or above
    /// `TIGHTEN_MIN_RELIABILITY`; binary-searched. Uncapped when the feeder has no CDF.
    fn reliability_capped_alighting(
        &self,
        feeder_rt: Option<gtfs_structures::RouteType>,
        board_rt: Option<gtfs_structures::RouteType>,
        walk_to_next: u32,
        next_start: u32,
        max_alighting: u32,
    ) -> u32 {
        if feeder_rt
            .and_then(|rt| self.raptor.transit_delay_models.get(&rt))
            .is_none()
        {
            return max_alighting;
        }
        let reliable = |alight: u32| {
            self.transfer_on_time_prob(
                feeder_rt,
                board_rt,
                alight.saturating_add(walk_to_next),
                next_start,
            ) >= Self::TIGHTEN_MIN_RELIABILITY
        };
        if reliable(max_alighting) {
            return max_alighting;
        }
        if !reliable(0) {
            return 0;
        }
        let mut lo = 0u32;
        let mut hi = max_alighting;
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            if reliable(mid) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Per-transit-leg alighting bounds `B[i] = lambda[k-i-1][alighting_stop_i]` (raw,
    /// pre-reliability-cap; 0 = no bound). Kept for the differential gate validating
    /// `chain_bounds` against the full backward pass.
    pub(super) fn bounds_from_lambda(&self, legs: &[PlanLeg], lambda: &[Vec<u32>]) -> Vec<u32> {
        let transit_indices: Vec<usize> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| matches!(l, PlanLeg::Transit(_)).then_some(i))
            .collect();
        let k = transit_indices.len();
        let mut bounds = vec![0u32; k];
        for (i, &ti) in transit_indices.iter().enumerate() {
            let remaining = k - i - 1;
            let alighting_node = match &legs[ti] {
                PlanLeg::Transit(t) => t.to.node_id,
                _ => unreachable!(),
            };
            let ac = self.raptor.transit_node_to_stop[alighting_node.0];
            bounds[i] = if ac != u32::MAX && remaining < lambda.len() {
                lambda[remaining][ac as usize]
            } else {
                0
            };
        }
        bounds
    }

    /// O(k) chain-bounds sweep (S1): plan-consistent replacement for the full backward
    /// pass. Returns `B[i]` = latest permitted arrival at transit leg `i`'s alighting
    /// stop, from the plan's own legs/walks. Cap-unaware (mirrors lambda); the
    /// reliability cap is applied downstream in `tighten_with_bounds`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn chain_bounds(
        &self,
        legs: &[PlanLeg],
        target_stop: usize, // compact destination stop (== lambda's target_compact_stop)
        target: u32,        // latest arrival at target_stop (== best_arr - best_walk)
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Vec<u32> {
        let transit_indices: Vec<usize> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| matches!(l, PlanLeg::Transit(_)).then_some(i))
            .collect();
        let k = transit_indices.len();
        let mut bounds = vec![0u32; k];
        if k == 0 {
            return bounds;
        }

        // B[k-1]: reproduce lambda[0][alighting_of_last_leg] exactly.
        let last_ti = transit_indices[k - 1];
        let last_alight = match &legs[last_ti] {
            PlanLeg::Transit(t) => t.to.node_id,
            _ => unreachable!(),
        };
        let last_compact = self.raptor.transit_node_to_stop[last_alight.0];
        bounds[k - 1] = if last_compact != u32::MAX && last_compact as usize == target_stop {
            target
        } else if target > 0 && last_compact != u32::MAX {
            // lambda relaxes one reverse footpath hop; off-table (long) egress ⇒ 0.
            match self.reverse_transfer_walk(target_stop, last_compact as usize) {
                Some(walk) => target.saturating_sub(walk),
                None => 0,
            }
        } else {
            0
        };

        // Chain backward through the fixed legs: for leg i, the latest trip on its
        // (board, alight) pair arriving <= B[i] (original leg start always feasible);
        // the earlier leg's bound is that departure minus the inter-leg walk.
        for i in (1..k).rev() {
            let ti = transit_indices[i];
            let (board, alight, leg_start) = match &legs[ti] {
                PlanLeg::Transit(t) => (t.from.node_id, t.to.node_id, t.start),
                _ => unreachable!(),
            };
            let dep_i = self
                .latest_departure_before_arrival(
                    board, alight, leg_start, bounds[i], date, weekday, rt,
                )
                .map(|(_, dep, _)| dep);

            let prev_ti = transit_indices[i - 1];
            let prev_alight = match &legs[prev_ti] {
                PlanLeg::Transit(t) => t.to.node_id,
                _ => unreachable!(),
            };
            let board_compact = self.raptor.transit_node_to_stop[board.0];
            let prev_alight_compact = self.raptor.transit_node_to_stop[prev_alight.0];
            // Inter-leg transfer: reconstructed walk (metres, seconds).
            let (plan_walk_len, plan_walk_dur) = legs[prev_ti + 1..ti].iter().fold(
                (0usize, 0u32),
                |(len, dur), l| match l {
                    PlanLeg::Walk(w) => (len + w.length, dur + w.duration),
                    _ => (len, dur),
                },
            );
            // A transfer > MAX_TRANSFER_DISTANCE_M is exactly what lambda's capped
            // reverse footpath cannot represent (label 0, untightened).
            let off_table = (plan_walk_len as f64) > super::MAX_TRANSFER_DISTANCE_M;
            bounds[i - 1] = if off_table {
                // Match lambda's no-op by default; opt-in tightens with the plan walk.
                if long_transfer_tightening(self) {
                    dep_i.map(|d| d.saturating_sub(plan_walk_dur)).unwrap_or(0)
                } else {
                    0
                }
            } else {
                // Short / same-stop: capped table walk (== lambda's reverse footpath)
                // when present, else the plan's own reconstructed walk.
                let w = if board_compact != u32::MAX && prev_alight_compact != u32::MAX {
                    self.reverse_transfer_walk(board_compact as usize, prev_alight_compact as usize)
                        .unwrap_or(plan_walk_dur)
                } else {
                    plan_walk_dur
                };
                dep_i.map(|d| d.saturating_sub(w)).unwrap_or(0)
            };
        }
        bounds
    }

    /// Capped reverse-transfer table walk (seconds) from `from_stop` to `to_stop`, or
    /// `None` when off-table. Keeps the smallest walk on duplicate sources (== lambda).
    pub(super) fn reverse_transfer_walk(&self, to_stop: usize, from_stop: usize) -> Option<u32> {
        if to_stop >= self.raptor.transit_idx_stop_reverse_transfers.len() {
            return None;
        }
        self.raptor.transit_idx_stop_reverse_transfers[to_stop]
            .of(&self.raptor.transit_stop_reverse_transfers)
            .iter()
            .filter(|&&(source, _)| source == from_stop)
            .map(|&(_, walk)| walk)
            .min()
    }

    pub fn set_tighten_mode(mode: u8) {
        TIGHTEN_MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn set_tighten_mode_chain() {
        Self::set_tighten_mode(TIGHTEN_MODE_CHAIN);
    }
    pub fn set_tighten_mode_lambda() {
        Self::set_tighten_mode(TIGHTEN_MODE_LAMBDA);
    }
    pub fn set_tighten_mode_diff() {
        Self::set_tighten_mode(TIGHTEN_MODE_DIFF);
    }
    /// Opt-in: tighten long (off-table) transfers with the plan's own walk.
    pub fn set_tighten_long_transfers(&mut self, on: bool) {
        self.raptor.tighten_long_transfers = on;
    }

    pub fn reset_tighten_diff_stats() {
        use std::sync::atomic::Ordering::Relaxed;
        for c in [
            &DIFF_CHECKS,
            &DIFF_IDENTICAL,
            &DIFF_CLASS1,
            &DIFF_CLASS2,
            &DIFF_CLASS3,
            &DIFF_SEED_MISMATCH,
        ] {
            c.store(0, Relaxed);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn chain_bounds_pub(
        &self,
        legs: &[PlanLeg],
        target_stop: usize,
        target: u32,
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Vec<u32> {
        self.chain_bounds(legs, target_stop, target, date, weekday, rt)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bounds_from_lambda_pub(
        &self,
        legs: &[PlanLeg],
        target_stop: usize,
        target: u32,
        transit_count: usize,
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
    ) -> Vec<u32> {
        let lambda = self.raptor_backward(target_stop, target, transit_count, date, weekday, rt);
        self.bounds_from_lambda(legs, &lambda)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tighten_with_bounds_pub(
        &self,
        legs: &mut Vec<PlanLeg>,
        bounds: &[u32],
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
        onboard: bool,
        debug_check: bool,
    ) {
        self.tighten_with_bounds(legs, bounds, date, weekday, rt, onboard, debug_check);
    }

    /// Differential-gate counters:
    /// `(checks, identical, class1, class2, class3, seed_mismatch)`.
    pub fn tighten_diff_stats() -> (u64, u64, u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            DIFF_CHECKS.load(Relaxed),
            DIFF_IDENTICAL.load(Relaxed),
            DIFF_CLASS1.load(Relaxed),
            DIFF_CLASS2.load(Relaxed),
            DIFF_CLASS3.load(Relaxed),
            DIFF_SEED_MISMATCH.load(Relaxed),
        )
    }

    /// Differential gate: tighten under both `bounds_lambda` and `bounds_chain`, assert
    /// the chain plan is time-consistent, and classify any divergence.
    #[allow(clippy::too_many_arguments)]
    fn tighten_diff_check(
        &self,
        legs: &[PlanLeg],
        bounds_lambda: &[u32],
        bounds_chain: &[u32],
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
        onboard: bool,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        DIFF_CHECKS.fetch_add(1, Relaxed);
        let k = bounds_lambda.len();

        // Seed fidelity (recorded, not fatal): chain's last-leg bound should
        // reproduce lambda[0][last-alight].
        if k > 0 && bounds_chain[k - 1] != bounds_lambda[k - 1] {
            DIFF_SEED_MISMATCH.fetch_add(1, Relaxed);
            eprintln!(
                "TIGHTEN_DIFF seed-mismatch B[k-1] chain={} lambda={}",
                bounds_chain[k - 1],
                bounds_lambda[k - 1]
            );
        }

        let mut l = legs.to_vec();
        let mut c = legs.to_vec();
        self.tighten_with_bounds(&mut l, bounds_lambda, date, weekday, rt, onboard, false);
        self.tighten_with_bounds(&mut c, bounds_chain, date, weekday, rt, onboard, true);

        // Hard gate: the chain plan must be time-consistent everywhere; a negative
        // margin is a genuine chain_bounds bug.
        for (idx, m) in plan_transfer_margins(&c).into_iter().enumerate() {
            assert!(
                m >= 0,
                "S1 chain plan has NEGATIVE transfer margin {m}s at transfer {idx} — chain_bounds bug. \
                 bounds_chain={bounds_chain:?} bounds_lambda={bounds_lambda:?}"
            );
        }

        if legs_timing_eq(&l, &c) {
            DIFF_IDENTICAL.fetch_add(1, Relaxed);
            return;
        }

        // Divergence classification (reporting only):
        //   1: lambda plan time-inconsistent (the bug S1 fixes)
        //   2: long transfer lambda left untightened
        //   3: walk-value drift, both consistent
        let l_consistent = plan_transfer_margins(&l).into_iter().all(|m| m >= 0);
        let lambda_noop = (0..k).any(|i| bounds_lambda[i] == 0 && bounds_chain[i] > 0);
        let long_transfer = plan_has_long_transfer(&c, super::MAX_TRANSFER_DISTANCE_M);
        if !l_consistent {
            DIFF_CLASS1.fetch_add(1, Relaxed);
            eprintln!(
                "TIGHTEN_DIFF class=1 (lambda negative margin) lambda={bounds_lambda:?} chain={bounds_chain:?}"
            );
        } else if long_transfer || lambda_noop {
            DIFF_CLASS2.fetch_add(1, Relaxed);
            eprintln!(
                "TIGHTEN_DIFF class=2 (long transfer untightened by lambda) lambda={bounds_lambda:?} chain={bounds_chain:?}"
            );
        } else {
            DIFF_CLASS3.fetch_add(1, Relaxed);
            eprintln!(
                "TIGHTEN_DIFF class=3 (walk-value drift, both consistent) lambda={bounds_lambda:?} chain={bounds_chain:?}"
            );
        }
    }

    /// Forward tightening driven by per-leg alighting bounds `bounds[i]` (latest arrival
    /// permitted at transit leg `i`'s alighting stop), agnostic to their origin
    /// (`bounds_from_lambda` or `chain_bounds`). The reliability cap and all timing lives here.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn tighten_with_bounds(
        &self,
        legs: &mut [PlanLeg],
        bounds: &[u32],
        date: u32,
        weekday: u8,
        rt: &RealtimeIndex,
        onboard: bool,
        // When true (S1 chain-sweep), assert every recomputed transfer margin is
        // non-negative. False for the lambda path, which can legitimately emit one.
        debug_check: bool,
    ) {
        let transit_indices: Vec<usize> = legs
            .iter()
            .enumerate()
            .filter_map(|(i, l)| {
                if matches!(l, PlanLeg::Transit(_)) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        let k = transit_indices.len();
        if k == 0 {
            return;
        }

        let mut current_time: u32 = 0;

        for i in 0..k {
            let ti = transit_indices[i];

            let (boarding_node, alighting_node, leg_start) = match &legs[ti] {
                PlanLeg::Transit(t) => (t.from.node_id, t.to.node_id, t.start),
                _ => unreachable!(),
            };
            let _ = alighting_node;

            let max_alighting = bounds.get(i).copied().unwrap_or(0);

            let walk_to_next: u32 = if i < k - 1 {
                let next_ti = transit_indices[i + 1];
                legs[ti + 1..next_ti]
                    .iter()
                    .map(|l| match l {
                        PlanLeg::Walk(w) => w.duration,
                        _ => 0,
                    })
                    .sum()
            } else {
                0
            };

            let max_alighting = if i < k - 1 && max_alighting > 0 {
                let next_ti = transit_indices[i + 1];
                let (next_start, next_rt) = match &legs[next_ti] {
                    PlanLeg::Transit(t) => (t.start, t.route_type),
                    _ => unreachable!(),
                };
                let feeder_rt = match &legs[ti] {
                    PlanLeg::Transit(t) => t.route_type,
                    _ => unreachable!(),
                };
                self.reliability_capped_alighting(
                    feeder_rt,
                    next_rt,
                    walk_to_next,
                    next_start,
                    max_alighting,
                )
            } else {
                max_alighting
            };

            // Onboard plans must never re-time leg[0]: the user is already aboard
            // that specific vehicle.
            if max_alighting > 0 && !(onboard && i == 0) {
                let min_dep = if i == 0 { leg_start } else { current_time };

                if let Some((dep_idx, new_dep, _)) = self.latest_departure_before_arrival(
                    boarding_node,
                    alighting_node,
                    min_dep,
                    max_alighting,
                    date,
                    weekday,
                    rt,
                ) && new_dep > leg_start
                {
                    let cloned = match &legs[ti] {
                        PlanLeg::Transit(t) => t.clone(),
                        _ => unreachable!(),
                    };
                    if let Ok(mut alts) = cloned.find_alternatives(
                        self,
                        std::iter::once((dep_idx, &self.raptor.transit_departures[dep_idx])),
                        1,
                    ) && let Some(new_leg) = alts.pop()
                    {
                        legs[ti] = PlanLeg::Transit(new_leg);
                    }
                }
            }

            let new_leg_end = match &legs[ti] {
                PlanLeg::Transit(t) => t.end,
                _ => unreachable!(),
            };

            if i < k - 1 {
                let next_ti = transit_indices[i + 1];

                let mut cursor = new_leg_end;
                for l in legs[ti + 1..next_ti].iter_mut() {
                    if let PlanLeg::Walk(w) = l {
                        let new_start = cursor;
                        let new_end = new_start + w.duration;
                        w.start = new_start;
                        w.end = new_end;
                        w.from.departure = Some(new_start);
                        w.to.arrival = Some(new_end);
                        for step in w.steps.iter_mut() {
                            if let PlanLegStep::Walk(ws) = step {
                                ws.place.arrival = Some(new_end);
                            }
                        }
                        cursor = new_end;
                    }
                }
                current_time = cursor;

                if let PlanLeg::Transit(next_t) = &mut legs[next_ti] {
                    next_t.preceding_arrival = Some(cursor);
                    // S1 invariant: a bound must never re-time this leg past its
                    // downstream connection.
                    debug_assert!(
                        !debug_check || next_t.start >= cursor,
                        "tighten produced a negative transfer margin: start={} cursor={} (margin={})",
                        next_t.start,
                        cursor,
                        next_t.start as i32 - cursor as i32,
                    );
                    if let Some(prt) = next_t.preceding_route_type {
                        let margin = next_t.start as i32 - cursor as i32;
                        let next_dep = next_t.transfer_risk.as_ref().and_then(|r| r.next_departure);
                        let board = next_t
                            .route_type
                            .and_then(|brt| self.raptor.transit_delay_models.get(&brt));
                        let (rel, next_rel) = match self.raptor.transit_delay_models.get(&prt) {
                            Some(cdf) => (
                                cdf.prob_on_time_vs(board, margin),
                                next_dep.map(|nd| {
                                    cdf.prob_on_time_vs(board, nd as i32 - cursor as i32)
                                }),
                            ),
                            None => (1.0, None),
                        };
                        next_t.transfer_risk = Some(TransferRisk {
                            reliability: rel,
                            scheduled_departure: next_t.start,
                            next_departure: next_dep,
                            next_reliability: next_rel,
                            margin_secs: Some(margin),
                        });
                    } else {
                        next_t.transfer_risk = None;
                    }
                }
            } else {
                let _ = walk_to_next;
            }
        }
    }

    /// Plan reliability = product of each transit leg's `transfer_risk.reliability`
    /// (legs without a risk, and walk-only plans, count as 1.0).
    pub fn plan_reliability(plan: &Plan) -> f32 {
        plan.legs
            .iter()
            .filter_map(|l| {
                if let PlanLeg::Transit(t) = l {
                    t.transfer_risk.as_ref().map(|r| r.reliability)
                } else {
                    None
                }
            })
            .product::<f32>()
    }

    fn plan_street_secs(plan: &Plan) -> u32 {
        plan.legs
            .iter()
            .filter_map(|l| {
                if let PlanLeg::Walk(w) = l {
                    Some(w.duration)
                } else {
                    None
                }
            })
            .sum()
    }

    /// The plan's transit core (boarded trip segments); plans sharing it are the same
    /// journey with different street legs.
    fn transit_core(plan: &Plan) -> Vec<(u32, usize, usize)> {
        plan.legs
            .iter()
            .filter_map(|l| match l {
                PlanLeg::Transit(t) => Some((t.trip_id.0, t.from.node_id.0, t.to.node_id.0)),
                _ => None,
            })
            .collect()
    }

    /// Collapses same-transit-core plans differing only in street access/egress into one
    /// plan with `access_alternatives`. Primary = lightest-burden member; a member
    /// arriving strictly earlier stays standalone (a genuine Pareto endpoint). Direct
    /// plans (empty core) pass through untouched.
    pub(super) fn group_access_alternatives(plans: Vec<Plan>) -> Vec<Plan> {
        use std::collections::HashMap;

        let keys: Vec<Vec<(u32, usize, usize)>> = plans.iter().map(Self::transit_core).collect();
        let mut groups: HashMap<&[(u32, usize, usize)], Vec<usize>> = HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            if !key.is_empty() {
                groups.entry(key.as_slice()).or_default().push(i);
            }
        }

        let mut slots: Vec<Option<Plan>> = plans.into_iter().map(Some).collect();

        for members in groups.values() {
            if members.len() < 2 {
                continue;
            }
            let &primary_idx = members
                .iter()
                .min_by_key(|&&i| {
                    let p = slots[i].as_ref().unwrap();
                    (
                        p.mode.burden(),
                        p.end,
                        std::cmp::Reverse(p.start),
                        Self::plan_street_secs(p),
                    )
                })
                .unwrap();
            let primary_end = slots[primary_idx].as_ref().unwrap().end;

            let mut alternatives: Vec<AccessAlternative> = Vec::new();
            for &i in members {
                if i == primary_idx {
                    continue;
                }
                if slots[i].as_ref().unwrap().end < primary_end {
                    continue; // earlier arrival: stays standalone
                }
                let member = slots[i].take().unwrap();
                let alt = AccessAlternative {
                    mode: member.mode,
                    start: member.start,
                    end: member.end,
                    expected_end: member.expected_end,
                    street_secs: Self::plan_street_secs(&member),
                };
                alternatives.extend(member.access_alternatives);
                if member.mode != slots[primary_idx].as_ref().unwrap().mode {
                    alternatives.push(alt);
                }
            }

            let primary = slots[primary_idx].as_mut().unwrap();
            for alt in alternatives {
                let dup = primary
                    .access_alternatives
                    .iter_mut()
                    .find(|a| a.mode == alt.mode);
                match dup {
                    // Keep the latest-departing variant per mode.
                    Some(existing) if existing.start >= alt.start => {}
                    Some(existing) => *existing = alt,
                    None => primary.access_alternatives.push(alt),
                }
            }
            primary
                .access_alternatives
                .sort_by_key(|a| (a.mode.burden(), a.start));
        }

        slots.into_iter().flatten().collect()
    }

    /// Final pipeline: collapse access twins, drop transit plans no faster than an
    /// equal-or-lighter-burden direct ride, then burden-aware Pareto.
    pub(super) fn finalize_plans(plans: Vec<Plan>, buckets: &ReliabilityBuckets) -> Vec<Plan> {
        let grouped = Self::group_access_alternatives(plans);
        Self::pareto_filter(Self::prune_slower_than_direct(grouped), buckets)
    }

    /// Drops any transit plan strictly longer than a direct street plan of
    /// equal-or-lighter burden. A lighter-burden direct ride suppresses a heavier
    /// transit plan, never the reverse.
    pub(super) fn prune_slower_than_direct(plans: Vec<Plan>) -> Vec<Plan> {
        let is_direct = |p: &Plan| !p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_)));
        let dur = |p: &Plan| p.end.saturating_sub(p.start);

        // best_direct[b] = shortest direct duration available at burden <= b.
        let mut best_direct = [u32::MAX; 3];
        for p in &plans {
            if is_direct(p) {
                let d = dur(p);
                for slot in best_direct.iter_mut().skip(p.mode.burden() as usize) {
                    *slot = (*slot).min(d);
                }
            }
        }

        plans
            .into_iter()
            .filter(|p| is_direct(p) || dur(p) <= best_direct[p.mode.burden() as usize])
            .collect()
    }

    pub(super) fn pareto_filter(plans: Vec<Plan>, buckets: &ReliabilityBuckets) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        fn walk_secs(plan: &Plan) -> u32 {
            plan.legs
                .iter()
                .filter_map(|l| {
                    if let PlanLeg::Walk(w) = l {
                        Some(w.duration)
                    } else {
                        None
                    }
                })
                .sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        // 4-D Pareto (transfers ↓, end ↓, start ↑, reliability_bucket ↑), guarded by
        // burden: a plan may only dominate equal-or-heavier-burden plans. Burden and
        // walk seconds are NOT axes; they only break exact 4-axis ties.
        let dominates = |a: &Plan, b: &Plan| {
            let (tc_a, tc_b) = (transfer_count(a), transfer_count(b));
            let (rb_a, rb_b) = (rel_bucket(a), rel_bucket(b));
            a.mode.burden() <= b.mode.burden()
                && tc_a <= tc_b
                && a.end <= b.end
                && a.start >= b.start
                && rb_a >= rb_b
                && (tc_a < tc_b
                    || a.end < b.end
                    || a.start > b.start
                    || rb_a > rb_b
                    || a.mode.burden() < b.mode.burden()
                    || walk_secs(a) < walk_secs(b))
        };
        let equal_4 = |a: &Plan, b: &Plan| {
            transfer_count(a) == transfer_count(b)
                && a.end == b.end
                && a.start == b.start
                && rel_bucket(a) == rel_bucket(b)
        };
        let tie_break_wins = |a: &Plan, b: &Plan| {
            a.mode.burden() < b.mode.burden()
                || (a.mode.burden() == b.mode.burden() && walk_secs(a) <= walk_secs(b))
        };

        let mut result: Vec<Plan> = Vec::new();

        'outer: for plan in plans {
            for existing in &result {
                if dominates(existing, &plan)
                    || (equal_4(existing, &plan) && tie_break_wins(existing, &plan))
                {
                    continue 'outer;
                }
            }
            result.retain(|existing| !dominates(&plan, existing));
            result.push(plan);
        }

        result.sort_by(|a, b| {
            a.end
                .cmp(&b.end)
                .then(b.start.cmp(&a.start))
                .then(rel_bucket(b).cmp(&rel_bucket(a)))
                .then(a.mode.burden().cmp(&b.mode.burden()))
                .then(walk_secs(a).cmp(&walk_secs(b)))
        });
        result
    }

    /// Debug-aware pareto filter. `plan_to_sink_idx[i]` is the index of `plans[i]` in
    /// `sink`; dominated plans get their `sink` entry updated with the dominator's index.
    pub(super) fn pareto_filter_with_debug(
        plans: Vec<Plan>,
        plan_to_sink_idx: &[usize],
        sink: &mut [PlanCandidate],
        buckets: &ReliabilityBuckets,
    ) -> Vec<Plan> {
        fn transfer_count(plan: &Plan) -> usize {
            plan.legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count()
                .saturating_sub(1)
        }

        fn walk_secs(plan: &Plan) -> u32 {
            plan.legs
                .iter()
                .filter_map(|l| {
                    if let PlanLeg::Walk(w) = l {
                        Some(w.duration)
                    } else {
                        None
                    }
                })
                .sum()
        }

        let rel_bucket = |p: &Plan| buckets.bucket(Self::plan_reliability(p));

        // Burden-guarded 4-D Pareto with burden/walk tie-breaks (see `pareto_filter`).
        let dominates = |a: &Plan, b: &Plan| {
            let (tc_a, tc_b) = (transfer_count(a), transfer_count(b));
            let (rb_a, rb_b) = (rel_bucket(a), rel_bucket(b));
            a.mode.burden() <= b.mode.burden()
                && tc_a <= tc_b
                && a.end <= b.end
                && a.start >= b.start
                && rb_a >= rb_b
                && (tc_a < tc_b
                    || a.end < b.end
                    || a.start > b.start
                    || rb_a > rb_b
                    || a.mode.burden() < b.mode.burden()
                    || walk_secs(a) < walk_secs(b))
        };
        let equal_4 = |a: &Plan, b: &Plan| {
            transfer_count(a) == transfer_count(b)
                && a.end == b.end
                && a.start == b.start
                && rel_bucket(a) == rel_bucket(b)
        };
        let tie_break_wins = |a: &Plan, b: &Plan| {
            a.mode.burden() < b.mode.burden()
                || (a.mode.burden() == b.mode.burden() && walk_secs(a) <= walk_secs(b))
        };

        let mut result: Vec<Plan> = Vec::new();
        let mut result_sink_idx: Vec<usize> = Vec::new();

        'outer: for (plan, &sink_idx) in plans.into_iter().zip(plan_to_sink_idx.iter()) {
            let tc_p = transfer_count(&plan);
            let rb_p = rel_bucket(&plan);

            for (i, existing) in result.iter().enumerate() {
                if dominates(existing, &plan)
                    || (equal_4(existing, &plan) && tie_break_wins(existing, &plan))
                {
                    let tc_e = transfer_count(existing);
                    let rb_e = rel_bucket(existing);
                    sink[sink_idx].status = CandidateStatus::ParetoDominated {
                        dominator_index: result_sink_idx[i],
                        departure_worse: existing.start > plan.start,
                        arrival_worse: existing.end < plan.end,
                        transfers_worse: tc_e < tc_p,
                        reliability_worse: rb_e > rb_p,
                    };
                    continue 'outer;
                }
            }

            let mut dominated = vec![false; result.len()];
            for (i, existing) in result.iter().enumerate() {
                if dominates(&plan, existing) {
                    let tc_e = transfer_count(existing);
                    let rb_e = rel_bucket(existing);
                    dominated[i] = true;
                    sink[result_sink_idx[i]].status = CandidateStatus::ParetoDominated {
                        dominator_index: sink_idx,
                        departure_worse: plan.start > existing.start,
                        arrival_worse: plan.end < existing.end,
                        transfers_worse: tc_p < tc_e,
                        reliability_worse: rb_p > rb_e,
                    };
                }
            }

            let (new_result, new_result_sink_idx): (Vec<Plan>, Vec<usize>) = result
                .into_iter()
                .zip(result_sink_idx)
                .zip(dominated.iter())
                .filter_map(|((p, si), &dom)| if dom { None } else { Some((p, si)) })
                .unzip();
            result = new_result;
            result_sink_idx = new_result_sink_idx;

            result.push(plan);
            result_sink_idx.push(sink_idx);
        }

        result.sort_by(|a, b| {
            a.end
                .cmp(&b.end)
                .then(b.start.cmp(&a.start))
                .then(rel_bucket(b).cmp(&rel_bucket(a)))
                .then(a.mode.burden().cmp(&b.mode.burden()))
                .then(walk_secs(a).cmp(&walk_secs(b)))
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::delay::{DelayCDF, ScenarioBag};

    #[test]
    fn plan_price_of_none_when_fares_disabled() {
        use crate::structures::cost::PriceValue;
        let g = Graph::new();
        let p = PriceValue { known_cents: 210, ..PriceValue::ZERO };
        assert!(
            g.plan_price_of(&p, Vec::new(), 0).is_none(),
            "disabled fares surface no Plan.price (byte-identical output)"
        );
    }

    fn brupass_graph() -> Graph {
        use crate::structures::cost::{Agglomeration, FareModel};
        let mut g = Graph::new();
        g.raptor.fare_model = FareModel {
            enabled: true,
            brupass_cents: 270,
            ..FareModel::default()
        };
        // Stop 5 is inside the Brussels flat zone; stop 6 is outside.
        g.raptor.sncb_stop_zone = vec![Agglomeration::None; 8];
        g.raptor.sncb_stop_zone[5] = Agglomeration::Brussels;
        g
    }

    fn paid_item(operator: &str, euros: f64) -> crate::structures::plan::FareBreakdownItem {
        crate::structures::plan::FareBreakdownItem {
            operator: operator.to_string(),
            description: format!("{operator} single"),
            euros,
            coverage: None,
        }
    }

    #[test]
    fn brupass_does_not_cap_single_operator_in_zone_journey() {
        // Brupass is a MULTI-OPERATOR Brussels product: a lone in-zone operator stays on
        // its own ticket even when Brupass would be numerically cheaper.
        let g = brupass_graph();
        let mut items = vec![paid_item("TEC", 2.80)];
        let paid_tags = vec![(0usize, "TEC".to_string(), 5usize)];
        let savings = g.apply_brupass_cap(&mut items, &paid_tags);
        assert_eq!(savings, 0, "single-operator in-zone journey is not a Brupass trip");
        assert_eq!(items.len(), 1, "no Brupass item inserted");
        assert_eq!(items[0].euros, 2.80, "TEC ticket left untouched");
    }

    #[test]
    fn brupass_does_not_cap_single_operator_out_of_zone() {
        let g = brupass_graph();
        let mut items = vec![paid_item("TEC", 2.80)];
        let paid_tags = vec![(0usize, "TEC".to_string(), 6usize)];
        let savings = g.apply_brupass_cap(&mut items, &paid_tags);
        assert_eq!(savings, 0, "out-of-zone boarding does not qualify for Brupass");
        assert_eq!(items.len(), 1, "no Brupass item inserted");
        assert_eq!(items[0].euros, 2.80, "ticket left untouched");
    }

    #[test]
    fn brupass_multi_operator_in_zone_still_caps() {
        let g = brupass_graph();
        let mut items = vec![paid_item("TEC", 2.80), paid_item("De Lijn", 2.50)];
        let paid_tags = vec![
            (0usize, "TEC".to_string(), 5usize),
            (1usize, "De Lijn".to_string(), 5usize),
        ];
        let savings = g.apply_brupass_cap(&mut items, &paid_tags);
        assert_eq!(savings, 530 - 270, "5.30 - 2.70 = 2.60 saved");
        assert_eq!(items[0].operator, "Brupass");
        assert_eq!(items[0].euros, 2.70);
    }

    #[test]
    fn plan_price_of_populates_known_and_unknown_when_enabled() {
        use crate::structures::cost::{FareModel, KnownEurosEpsilon, PriceValue};
        let mut g = Graph::new();
        g.raptor.fare_model = FareModel {
            enabled: true,
            known_euros_epsilon: KnownEurosEpsilon::default(),
            operators: Vec::new(),
            agglomerations: Vec::new(),
            ..FareModel::default()
        };
        // Slot labels are the display-cased agency names (as stored by
        // `rebuild_operator_fare_lookup`), surfaced verbatim in the badge tooltip.
        g.raptor.unknown_operator_names = vec!["De Lijn".into(), "TEC".into()];
        let mut unknown = [0u8; 4];
        unknown[0] = 2; // two De Lijn boardings
        unknown[1] = 1; // one TEC boarding
        let p = PriceValue { known_cents: 420, unknown, ..PriceValue::ZERO };
        let price = g.plan_price_of(&p, Vec::new(), 0).expect("enabled fares populate Plan.price");
        assert_eq!(price.known_euros, 4.20);
        assert_eq!(price.capped_euros, 4.20, "cap == known this increment");
        assert_eq!(price.unknown_operators, vec!["De Lijn x2".to_string(), "TEC".to_string()]);
    }

    #[test]
    fn chain_has_transit_matches_transit_trace_in_arena_chain() {
        use crate::structures::raptor::Trace;
        let transit_trace = Trace {
            pattern: 3,
            ..Trace::NONE
        };
        let transfer_trace = Trace {
            from_stop: 7,
            ..Trace::NONE
        };
        let root = Label {
            trace: Trace::NONE,
            parent: u32::MAX,
            ..Label::NONE
        };
        let transfer = Label {
            trace: transfer_trace,
            parent: 0,
            ..Label::NONE
        };
        let transit = Label {
            trace: transit_trace,
            parent: 1,
            ..Label::NONE
        };
        let arena = vec![root, transfer, transit];

        assert!(Graph::chain_has_transit(&arena, 2));
        assert!(!Graph::chain_has_transit(&arena, 1));
        assert!(!Graph::chain_has_transit(&arena, 0));
        assert!(!Graph::chain_has_transit(&arena, u32::MAX));
    }

    #[test]
    fn apply_realtime_keeps_scheduled_step_times_and_delays_effective() {
        use crate::ingestion::gtfs::TripId;

        let trip = TripId(0);
        let mut g = Graph::new();
        g.raptor.transit_node_to_stop = vec![0, 1, 2];

        let place = |node: usize, arr: Option<u32>, dep: Option<u32>| PlanPlace {
            stop_position: Some(node as u32),
            arrival: arr,
            departure: dep,
            node_id: NodeID(node),
        };
        let step = |node: usize, arr: u32, dep: Option<u32>| {
            PlanLegStep::Transit(PlanTransitLegStep {
                length: 0,
                time: 0,
                place: place(node, Some(arr), dep),
                scheduled_arrival: Some(arr),
                scheduled_departure: dep,
                timetable_segment: TimetableSegment { start: 0, len: 0 },
                departure_index: 0,
                date: 0,
                weekday: 0,
            })
        };

        let leg = PlanTransitLeg {
            length: 0,
            start: 1000,
            end: 1300,
            duration: 300,
            scheduled_start: 1000,
            scheduled_end: 1300,
            realtime: false,
            from: place(0, Some(1000), Some(1000)),
            to: place(2, Some(1300), Some(1300)),
            steps: vec![step(1, 1100, Some(1130)), step(2, 1300, None)],
            geometry: vec![],
            transfer_risk: None,
            trip_id: trip,
            preceding_arrival: None,
            preceding_route_type: None,
            route_type: None,
            following_route_type: None,
            following_margin_secs: None,
            bikes_allowed: None,
            time_shift: 0,
        };
        let mut legs = vec![PlanLeg::Transit(leg)];

        let rt = RealtimeIndex::from_delays(
            0,
            [((trip, 0), 60), ((trip, 1), 120), ((trip, 2), 180)],
        );
        g.apply_realtime(&mut legs, &rt, false);

        let PlanLeg::Transit(t) = &legs[0] else {
            panic!("expected a transit leg");
        };

        assert!(t.realtime);
        assert_eq!((t.scheduled_start, t.scheduled_end), (1000, 1300));
        assert_eq!((t.start, t.end), (1060, 1480));
        assert_eq!(t.from.arrival, Some(1060));
        assert_eq!(t.to.departure, Some(1480));

        let mid = match &t.steps[0] {
            PlanLegStep::Transit(s) => s,
            _ => panic!("mid step"),
        };
        // scheduled_* untouched; place.* carries effective (delayed) times.
        assert_eq!(mid.scheduled_arrival, Some(1100));
        assert_eq!(mid.scheduled_departure, Some(1130));
        assert_ne!(mid.scheduled_arrival, mid.scheduled_departure);
        assert_eq!(mid.place.arrival, Some(1220));
        assert_eq!(mid.place.departure, Some(1250));

        let alight = match &t.steps[1] {
            PlanLegStep::Transit(s) => s,
            _ => panic!("alight step"),
        };
        assert_eq!(alight.scheduled_arrival, Some(1300));
        assert_eq!(alight.scheduled_departure, None);
        assert_eq!(alight.place.arrival, Some(1480));
        assert_eq!(alight.place.departure, None);
    }

    #[test]
    fn access_leg_leave_by_is_board_minus_p95() {
        use crate::structures::plan::LegOption;
        let opt = |t: u32, p95: u32| LegOption {
            time: t as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50: t,
            p95,
            length: t as usize,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![NodeID(0), NodeID(1)],
            edges: vec![],
        };
        let options = vec![opt(100, 130), opt(150, 165)];
        let board = 30_000u32;
        let earliest = 29_800u32;
        let (leg_start, leave_by, cur) = super::super::street_enrich::access_timing(
            &options,
            board,
            earliest,
            &crate::structures::cost::BalanceWeights::default(),
        );
        assert!(cur < options.len());
        assert_eq!(leg_start, board - options[cur].p50);
        assert_eq!(leave_by, board - options[cur].p95);
    }

    #[test]
    fn egress_leg_end_equals_alight_plus_highlighted_p50() {
        use crate::structures::cost::BalanceWeights;
        use crate::structures::plan::{LegOption, highlight_index};
        let opt = |p50: u32, p95: u32| LegOption {
            time: p50 as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50,
            p95,
            length: p50 as usize,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![NodeID(0), NodeID(1)],
            edges: vec![],
        };
        let options = vec![opt(120, 160), opt(180, 220)];
        let alight = 32_400u32;
        let balance = BalanceWeights::default();
        let cur = highlight_index(&options, None, &balance);
        assert!(cur < options.len());
        let expected_end = alight + options[cur].p50;
        assert_eq!(expected_end, alight + options[cur].p50);
    }

    #[test]
    fn arrival_stats_excludes_unreachable_miss_scenarios() {
        let bag = ScenarioBag::with_scenarios(36000, 0.6, u32::MAX, 0.4);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 37000);
        assert_eq!(dist.len(), 1);
        assert_eq!(dist[0].time, 36000);
        assert!((dist[0].probability - 0.6).abs() < 1e-6);
        assert_eq!(expected_end, 36000);
    }

    #[test]
    fn arrival_stats_unreachable_survives_negative_delay_convolution() {
        let bag = ScenarioBag::with_scenarios(36000, 0.5, u32::MAX, 0.5);
        let cdf = DelayCDF {
            bins: vec![(-180, 0.2), (0, 0.8), (120, 1.0)],
        };
        let (dist, expected_end) = Graph::arrival_stats(&bag, Some(&cdf), 37000);
        assert!(
            dist.iter().all(|s| s.time < 200_000),
            "sentinel leaked: {dist:?}"
        );
        assert!(
            (35820..=36120).contains(&expected_end),
            "got {expected_end}"
        );
    }

    #[test]
    fn arrival_stats_all_unreachable_falls_back_to_best_arrival() {
        let bag = ScenarioBag::with_scenarios(u32::MAX, 0.6, u32::MAX, 0.4);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 37000);
        assert!(dist.is_empty());
        assert_eq!(expected_end, 37000);
    }

    #[test]
    fn arrival_stats_pure_bag_keeps_full_expectation() {
        let bag = ScenarioBag::with_scenarios(36000, 0.75, 36600, 0.25);
        let (dist, expected_end) = Graph::arrival_stats(&bag, None, 36000);
        assert_eq!(dist.len(), 2);
        assert_eq!(expected_end, 36150);
    }

    use crate::ingestion::gtfs::TripId;
    use crate::structures::Mode;

    fn place(node: usize) -> PlanPlace {
        PlanPlace {
            node_id: NodeID(node),
            stop_position: None,
            arrival: None,
            departure: None,
        }
    }

    fn walk_leg(street_mode: Mode, start: u32, end: u32) -> PlanLeg {
        PlanLeg::Walk(PlanWalkLeg {
            length: 0,
            cycleroute_length: None,
            elevation_gain: None,
            start,
            end,
            duration: end - start,
            street_mode,
            from: place(0),
            to: place(1),
            steps: vec![],
            geometry: vec![],
            alternatives: vec![],
            leave_by: None,
        })
    }

    fn transit_leg(trip: u32, from: usize, to: usize, start: u32, end: u32) -> PlanLeg {
        PlanLeg::Transit(PlanTransitLeg {
            length: 0,
            start,
            end,
            duration: end - start,
            scheduled_start: start,
            scheduled_end: end,
            realtime: false,
            from: place(from),
            to: place(to),
            steps: vec![],
            geometry: vec![],
            transfer_risk: None,
            trip_id: TripId(trip),
            preceding_arrival: None,
            preceding_route_type: None,
            route_type: None,
            following_route_type: None,
            following_margin_secs: None,
            bikes_allowed: None,
            time_shift: 0,
        })
    }

    fn plan(mode: Mode, start: u32, end: u32, legs: Vec<PlanLeg>) -> Plan {
        Plan {
            legs,
            start,
            end,
            mode,
            access_alternatives: vec![],
            arrival_distribution: vec![ArrivalScenario {
                time: end,
                probability: 1.0,
            }],
            expected_end: end,
            price: None,
        }
    }

    fn buckets() -> ReliabilityBuckets {
        ReliabilityBuckets::new(&[0.50, 0.80, 0.95])
    }

    #[test]
    fn plan_timeline_rechains_trailing_walk_after_realtime_shift() {
        // Transit delayed to 720..800; egress still carries stale times (200..260).
        let mut legs = vec![
            walk_leg(Mode::Walk, 100, 120),   // access anchor
            transit_leg(7, 10, 11, 720, 800), // realtime-delayed
            walk_leg(Mode::Walk, 200, 260),   // egress, stale
        ];
        let (start, end) = Graph::plan_timeline(&mut legs);
        assert_eq!(start, 100);
        assert_eq!(
            end, 860,
            "egress must chain off the realtime arrival (800 + 60)"
        );
        assert!(end >= start);
        match &legs[2] {
            PlanLeg::Walk(w) => {
                assert_eq!(w.start, 800);
                assert_eq!(w.end, 860);
            }
            _ => panic!("expected egress walk"),
        }
    }

    #[test]
    fn plan_timeline_is_noop_when_already_chained() {
        let mut legs = vec![
            walk_leg(Mode::Walk, 100, 120),
            transit_leg(7, 10, 11, 120, 200),
            walk_leg(Mode::Walk, 200, 260),
        ];
        let (start, end) = Graph::plan_timeline(&mut legs);
        assert_eq!((start, end), (100, 260));
    }

    #[test]
    fn burden_tie_goes_to_lighter_mode() {
        let core = || vec![transit_leg(7, 10, 11, 100, 200)];
        let walk = plan(Mode::WalkTransit, 90, 210, core());
        let bike = plan(Mode::BikeTransit, 90, 210, core());
        let out = Graph::pareto_filter(vec![bike, walk], &buckets());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mode, Mode::WalkTransit);
    }

    #[test]
    fn heavier_mode_survives_on_strict_improvement() {
        let walk = plan(
            Mode::WalkTransit,
            90,
            250,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeTransit,
            90,
            210,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::pareto_filter(vec![walk, bike], &buckets());
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn lighter_mode_never_dominated_by_heavier() {
        let walk = plan(
            Mode::WalkTransit,
            80,
            300,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeTransit,
            90,
            210,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::pareto_filter(vec![walk, bike], &buckets());
        assert_eq!(
            out.len(),
            2,
            "{:?}",
            out.iter().map(|p| p.mode).collect::<Vec<_>>()
        );
    }

    #[test]
    fn same_core_groups_into_alternatives() {
        let walk = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![
                walk_leg(Mode::Walk, 80, 100),
                transit_leg(7, 10, 11, 100, 200),
                walk_leg(Mode::Walk, 200, 260),
            ],
        );
        let bike = plan(
            Mode::BikeTransit,
            94,
            260,
            vec![
                walk_leg(Mode::Bike, 94, 100),
                transit_leg(7, 10, 11, 100, 200),
                walk_leg(Mode::Walk, 200, 260),
            ],
        );
        let out = Graph::group_access_alternatives(vec![walk, bike]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].mode, Mode::WalkTransit);
        assert_eq!(out[0].access_alternatives.len(), 1);
        let alt = &out[0].access_alternatives[0];
        assert_eq!(alt.mode, Mode::BikeTransit);
        assert_eq!(alt.start, 94);
    }

    #[test]
    fn same_core_earlier_arrival_stays_standalone() {
        let walk = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let bike = plan(
            Mode::BikeOnTransit,
            94,
            220,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let out = Graph::group_access_alternatives(vec![walk, bike]);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn different_core_not_grouped() {
        let a = plan(
            Mode::WalkTransit,
            80,
            260,
            vec![transit_leg(7, 10, 11, 100, 200)],
        );
        let b = plan(
            Mode::BikeTransit,
            94,
            260,
            vec![transit_leg(8, 10, 11, 100, 200)],
        );
        let out = Graph::group_access_alternatives(vec![a, b]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|p| p.access_alternatives.is_empty()));
    }

    #[test]
    fn direct_shorter_duration_suppresses_longer_same_burden_transit() {
        let bike_direct = plan(Mode::Bike, 0, 1260, vec![walk_leg(Mode::Bike, 0, 1260)]);
        let bike_transit = plan(
            Mode::BikeOnTransit,
            300,
            1740,
            vec![transit_leg(7, 10, 11, 400, 1700)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, bike_transit], &buckets());
        assert!(
            out.iter().all(|p| p.mode != Mode::BikeOnTransit),
            "a bike+transit slower than cycling direct must be dropped: {:?}",
            out.iter()
                .map(|p| (p.mode, p.end - p.start))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn direct_does_not_suppress_lighter_burden_transit() {
        let bike_direct = plan(Mode::Bike, 0, 1260, vec![walk_leg(Mode::Bike, 0, 1260)]);
        let walk_transit = plan(
            Mode::WalkTransit,
            300,
            1740,
            vec![transit_leg(7, 10, 11, 400, 1700)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, walk_transit], &buckets());
        assert!(
            out.iter().any(|p| p.mode == Mode::WalkTransit),
            "lighter-burden walk+transit must survive a heavier bike-direct"
        );
    }

    #[test]
    fn direct_does_not_suppress_faster_transit() {
        let bike_direct = plan(Mode::Bike, 0, 1500, vec![walk_leg(Mode::Bike, 0, 1500)]);
        let bike_transit = plan(
            Mode::BikeOnTransit,
            0,
            1320,
            vec![transit_leg(7, 10, 11, 100, 1300)],
        );
        let out = Graph::finalize_plans(vec![bike_direct, bike_transit], &buckets());
        assert!(
            out.iter().any(|p| p.mode == Mode::BikeOnTransit),
            "a bike+transit faster than cycling direct must survive"
        );
    }

    #[test]
    fn direct_plans_never_grouped() {
        let a = plan(Mode::Walk, 80, 260, vec![walk_leg(Mode::Walk, 80, 260)]);
        let b = plan(Mode::Bike, 80, 140, vec![walk_leg(Mode::Bike, 80, 140)]);
        let out = Graph::group_access_alternatives(vec![a, b]);
        assert_eq!(out.len(), 2);
    }

    /// Real-graph smoke: verify access leg carries multiobj `alternatives` + `leave_by`
    /// and the egress leg carries `alternatives`.
    ///   cargo test --release --lib access_egress_smoke -- --ignored --nocapture
    #[test]
    #[ignore]
    fn access_egress_smoke() {
        use crate::ingestion::gtfs::load_gtfs_stib;
        use crate::ingestion::osm::load_pbf_file;
        use crate::routing::routing_raptor::{RouteQuery, route};
        use crate::structures::{Mode, RealtimeIndex};
        use chrono::{NaiveDate, NaiveTime};
        use std::time::Instant;

        let pbf = "data/brussels_capital_region-2026_01_24.osm.pbf";
        let gtfs = "data/stib.zip";

        let t0 = Instant::now();
        let mut g = Graph::new();
        load_pbf_file(pbf, None, 4.0, &Default::default(), &mut g).expect("OSM load failed");
        eprintln!(
            "SMOKE osm_load={:.1?} nodes={}",
            t0.elapsed(),
            g.nodes.len()
        );
        load_gtfs_stib(gtfs, &mut g).expect("GTFS load failed");
        eprintln!("SMOKE gtfs_load={:.1?}", t0.elapsed());
        g.build_raptor_index();
        eprintln!("SMOKE raptor_index={:.1?}", t0.elapsed());

        let q = RouteQuery {
            from_lat: 50.810,
            from_lng: 4.330,
            to_lat: 50.880,
            to_lng: 4.430,
            date: NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(),
            time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            window_minutes: None,
            min_access_secs: Some(600),
            arrival_slack_secs: None,
            unrestricted_transfers: None,
            use_cch_access: None,
            reliability_bucket_edges: None,
            modes: Some(vec![Mode::Walk, Mode::WalkTransit]),
            bike_profile: None,
            terminal_deadline: false,
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: None,
        };

        eprintln!("SMOKE stop_count={}", g.raptor.transit_stop_to_node.len());
        let (dist_o, &orig_node) = g
            .nearest_node_dist(q.from_lat, q.from_lng)
            .expect("origin snaps");
        let (dist_d, &dest_node) = g.nearest_node_dist(q.to_lat, q.to_lng).expect("dest snaps");
        eprintln!(
            "SMOKE origin_node={:?} dist={:.0}m dest_node={:?} dist={:.0}m",
            orig_node, dist_o, dest_node, dist_d
        );
        let access_stops = g.nearby_stops(orig_node, 600);
        let egress_stops = g.nearby_stops(dest_node, 600);
        eprintln!(
            "SMOKE access_stops={} egress_stops={}",
            access_stops.len(),
            egress_stops.len()
        );

        use crate::routing::routing_raptor::route_explain;
        let explain = route_explain(&g, &q, &RealtimeIndex::new()).expect("explain failed");
        eprintln!(
            "SMOKE explain stops_reached={} access_fallback={}",
            explain.stops_reached.len(),
            explain.access.fell_back_to_walk_only
        );
        eprintln!("SMOKE explain plans_before_filter={}", explain.plans.len());

        let plans = route(&g, &q, &RealtimeIndex::new()).expect("route failed");
        eprintln!("SMOKE plans={} elapsed={:.1?}", plans.len(), t0.elapsed());
        for (i, p) in plans.iter().enumerate() {
            let tlegs = p
                .legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Transit(_)))
                .count();
            let wlegs = p
                .legs
                .iter()
                .filter(|l| matches!(l, PlanLeg::Walk(_)))
                .count();
            eprintln!(
                "  plan[{i}] mode={:?} transit_legs={tlegs} walk_legs={wlegs}",
                p.mode
            );
        }

        let transit_plan = plans
            .iter()
            .find(|p| {
                p.mode == Mode::WalkTransit
                    && p.legs.iter().any(|l| matches!(l, PlanLeg::Transit(_)))
            })
            .expect("expected at least one WalkTransit plan with a transit leg");

        for (i, leg) in transit_plan.legs.iter().enumerate() {
            match leg {
                PlanLeg::Walk(w) => eprintln!(
                    "  leg[{i}] Walk alts={} leave_by={:?} from={:?} to={:?}",
                    w.alternatives.len(),
                    w.leave_by,
                    w.from.node_id,
                    w.to.node_id
                ),
                PlanLeg::Transit(_) => eprintln!("  leg[{i}] Transit"),
            }
        }

        let first_walk_from = transit_plan
            .legs
            .iter()
            .find_map(|l| match l {
                PlanLeg::Walk(w) => Some((w.from.node_id, w.to.node_id)),
                _ => None,
            })
            .unwrap();
        let (fw_from, fw_to) = first_walk_from;
        eprintln!("SMOKE first_walk from={:?} to={:?}", fw_from, fw_to);
        let bike_cost = crate::structures::BikeCost::new(crate::structures::BikeProfile::default());
        let reps = g.multiobj_representatives(
            fw_from,
            fw_to,
            crate::structures::cost::RoutingMode::Walk,
            crate::structures::cost::LegRole::Deadline,
            &bike_cost,
        );
        eprintln!(
            "SMOKE first_walk_multiobj_reps={} distance_budget={}",
            reps.len(),
            g.raptor.distance_budget
        );

        let multiobj_result = g.multiobj_search(
            fw_from,
            fw_to,
            crate::structures::cost::RoutingMode::Walk,
            crate::structures::cost::LegRole::Deadline,
            &bike_cost,
            &g.raptor.cost_weights,
            &g.raptor.epsilon,
            g.raptor.distance_budget,
            false,
        );
        eprintln!("SMOKE first_walk_front={}", multiobj_result.front.len());

        let multiobj_unlimited = g.multiobj_search(
            fw_from,
            fw_to,
            crate::structures::cost::RoutingMode::Walk,
            crate::structures::cost::LegRole::Deadline,
            &bike_cost,
            &g.raptor.cost_weights,
            &g.raptor.epsilon,
            f64::INFINITY,
            false,
        );
        eprintln!(
            "SMOKE first_walk_front_unlimited={}",
            multiobj_unlimited.front.len()
        );

        let access_leg = transit_plan
            .legs
            .iter()
            .find_map(|l| match l {
                PlanLeg::Walk(w) if w.leave_by.is_some() => Some(w),
                _ => None,
            })
            .expect("expected an access walk leg with leave_by");

        eprintln!(
            "SMOKE access_opts={} leave_by={:?} egress checking…",
            access_leg.alternatives.len(),
            access_leg.leave_by,
        );

        assert!(
            !access_leg.alternatives.is_empty(),
            "access leg must have non-empty multiobj alternatives"
        );
        assert!(
            access_leg.leave_by.is_some(),
            "access leg must carry leave_by"
        );

        let egress_leg = transit_plan.legs.iter().rev().find_map(|l| match l {
            PlanLeg::Walk(w) if w.leave_by.is_none() && !w.alternatives.is_empty() => Some(w),
            _ => None,
        });

        let egress_opts = egress_leg.map(|l| l.alternatives.len()).unwrap_or(0);
        eprintln!(
            "SMOKE access_opts={} leave_by={:?} egress_opts={}",
            access_leg.alternatives.len(),
            access_leg.leave_by,
            egress_opts,
        );

        assert!(
            egress_leg.is_some(),
            "transit plan must have an egress walk leg with non-empty multiobj alternatives"
        );
    }

    fn leg_option(p50: u32, p95: u32) -> crate::structures::plan::LegOption {
        crate::structures::plan::LegOption {
            time: p50 as f64,
            dplus: 0.0,
            surface: 0.0,
            variance: 0.0,
            cycleway_deficit: 0.0,
            p50,
            p95,
            length: p50 as usize,
            unpaved_length: 0,
            dismount_length: 0,
            dismount_runs: vec![],
            elevation_gain: None,
            cycleroute_length: None,
            geometry: vec![],
            nodes: vec![],
            edges: vec![],
        }
    }

    fn coord(lat: f64, lon: f64) -> PlanCoordinate {
        PlanCoordinate { lat, lon }
    }

    fn walk_leg_with_alternatives(
        start: u32,
        end: u32,
        alternatives: Vec<crate::structures::plan::LegOption>,
        leave_by: Option<u32>,
        geometry: Vec<PlanCoordinate>,
    ) -> PlanLeg {
        PlanLeg::Walk(PlanWalkLeg {
            length: 0,
            cycleroute_length: None,
            elevation_gain: None,
            start,
            end,
            duration: end - start,
            street_mode: Mode::Walk,
            from: place(0),
            to: place(1),
            steps: vec![],
            geometry,
            alternatives,
            leave_by,
        })
    }

    #[test]
    fn merge_consecutive_walks_does_not_merge_when_egress_has_alternatives() {
        let geo1 = vec![coord(1.0, 2.0), coord(1.1, 2.1)];
        let geo2 = vec![coord(1.1, 2.1), coord(1.2, 2.2)];
        let transfer_walk = walk_leg_with_alternatives(200, 250, vec![], None, geo1);
        let egress_walk =
            walk_leg_with_alternatives(250, 320, vec![leg_option(70, 90)], None, geo2);
        let legs = vec![transfer_walk, egress_walk];
        let merged = Graph::merge_consecutive_walks(legs);
        assert_eq!(merged.len(), 2, "legs with alternatives must NOT be merged");
        match &merged[1] {
            PlanLeg::Walk(w) => {
                assert_eq!(w.alternatives.len(), 1, "egress alternatives must survive")
            }
            _ => panic!("expected walk leg"),
        }
    }

    #[test]
    fn merge_consecutive_walks_merges_plain_walks_without_alternatives() {
        let geo1 = vec![coord(1.0, 2.0), coord(1.1, 2.1)];
        let geo2 = vec![coord(1.2, 2.2), coord(1.3, 2.3)];
        let walk1 = walk_leg_with_alternatives(100, 150, vec![], None, geo1);
        let walk2 = walk_leg_with_alternatives(150, 220, vec![], None, geo2);
        let legs = vec![walk1, walk2];
        let merged = Graph::merge_consecutive_walks(legs);
        assert_eq!(merged.len(), 1, "two plain walks must merge into one");
        match &merged[0] {
            PlanLeg::Walk(w) => {
                assert_eq!(w.start, 100);
                assert_eq!(w.end, 220);
                assert!(w.alternatives.is_empty());
            }
            _ => panic!("expected walk leg"),
        }
    }

    #[test]
    fn access_timing_clamps_leg_start_to_earliest() {
        let options = vec![leg_option(5000, 6000)];
        let board = 30_000u32;
        let earliest = 29_000u32;
        let (leg_start, _leave_by, _cur) = super::super::street_enrich::access_timing(
            &options,
            board,
            earliest,
            &crate::structures::cost::BalanceWeights::default(),
        );
        assert_eq!(
            leg_start, earliest,
            "leg_start must be clamped to earliest when p50 exceeds the window"
        );
    }
}
