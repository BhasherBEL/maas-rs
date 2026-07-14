//! Transit pricing as a post-hoc plan annotation (multi-operator fares).

// Boardings beyond the last slot fold into it; raise if the operator set grows.
pub const N_OP: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriceValue {
    pub known_cents: u32,
    pub unknown: [u8; N_OP],
    // Ticket-window activation; seconds since midnight, `u32::MAX` = none.
    pub stib_activation: u32,
    pub sncb_active: bool,
    // Reusable paid-base credit; a pre-paid label must not be pruned by a
    // credit-less cheaper one (see `dominates` buy-back).
    pub sncb_base_credit: u32,
    pub stib_ticket_credit: u32,
    pub delijn_activation: u32,
    pub delijn_ticket_credit: u32,
    // Raw SNCB portion of `known_cents`, for the display cap. Dominance carries the
    // raw total; a lower raw always implies a lower-or-equal capped price.
    pub sncb_spend_cents: u32,
    pub sncb_cap_cents: u32,
    pub brupass_credit: u32,
    pub brupass_activation: u32,
    pub sncb_run_board_stop: u32,
    // Integer metres (not f64) so `PriceValue` keeps `Eq`/`Hash`.
    pub sncb_run_m: u32,
    pub sncb_run_perkm_cents: u32,
}

impl PriceValue {
    pub const ZERO: Self = PriceValue {
        known_cents: 0,
        unknown: [0; N_OP],
        stib_activation: u32::MAX,
        sncb_active: false,
        sncb_base_credit: 0,
        stib_ticket_credit: 0,
        delijn_activation: u32::MAX,
        delijn_ticket_credit: 0,
        sncb_spend_cents: 0,
        sncb_cap_cents: u32::MAX,
        brupass_credit: 0,
        brupass_activation: u32::MAX,
        sncb_run_board_stop: u32::MAX,
        sncb_run_m: 0,
        sncb_run_perkm_cents: 0,
    };

    #[inline]
    pub fn end_sncb_run(&mut self) {
        self.sncb_active = false;
        self.sncb_base_credit = 0;
        self.sncb_run_board_stop = u32::MAX;
        self.sncb_run_m = 0;
        self.sncb_run_perkm_cents = 0;
    }

    #[inline]
    // Sound only with the credit buy-back: `self` prunes `other` iff
    // `known_self + Σ value(credit other holds and self lacks) <= known_other`
    // (plus unknowns componentwise). The display cap must NOT enter here (raw
    // `known_cents` is what stays sound).
    pub fn dominates(&self, other: &PriceValue) -> bool {
        let penalty = (if self.sncb_base_credit == 0 { other.sncb_base_credit } else { 0 })
            + (if self.stib_ticket_credit == 0 { other.stib_ticket_credit } else { 0 })
            + (if self.delijn_ticket_credit == 0 { other.delijn_ticket_credit } else { 0 })
            + (if self.brupass_credit == 0 { other.brupass_credit } else { 0 });
        if self.known_cents.saturating_add(penalty) > other.known_cents {
            return false;
        }
        for op in 0..N_OP {
            if self.unknown[op] > other.unknown[op] {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KnownEurosEpsilon {
    pub a: f64,
    pub b: f64,
}

impl Default for KnownEurosEpsilon {
    fn default() -> Self {
        KnownEurosEpsilon { a: 10.0, b: 0.0 }
    }
}

impl KnownEurosEpsilon {
    #[inline]
    pub fn bucket(&self, cents: u32) -> u32 {
        let width = self.a + self.b * cents as f64;
        if width <= 0.0 {
            return cents;
        }
        let b = (cents as f64 / width).floor() * width;
        b as u32
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PassengerCategory {
    #[default]
    Adult,
    Young,
    Senior,
    Bim,
}

impl PassengerCategory {
    #[inline]
    pub fn is_reduced(self) -> bool {
        !matches!(self, PassengerCategory::Adult)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TravelClass {
    #[default]
    Second,
    First,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FareProfile {
    pub category: PassengerCategory,
    pub stib_subscription: bool,
    pub delijn_subscription: bool,
    pub tec_subscription: bool,
    pub sncb_subscription: bool,
    pub sncb_train_plus: bool,
    pub delijn_10_journey: bool,
    pub tec_6_journey: bool,
    pub travel_class: TravelClass,
}

#[derive(Clone, Copy, Debug)]
pub struct FareContext {
    pub profile: FareProfile,
    // 0 = Mon .. 6 = Sun (chrono `num_days_from_monday`).
    pub weekday: u8,
}

impl FareContext {
    pub const DEFAULT: Self = FareContext {
        profile: FareProfile {
            category: PassengerCategory::Adult,
            stib_subscription: false,
            delijn_subscription: false,
            tec_subscription: false,
            sncb_subscription: false,
            sncb_train_plus: false,
            delijn_10_journey: false,
            tec_6_journey: false,
            travel_class: TravelClass::Second,
        },
        weekday: 0,
    };

    #[inline]
    pub fn is_weekend(&self) -> bool {
        self.weekday >= 5
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeBucket {
    Peak,
    Weekend,
    OffPeak,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SncbTimeRules {
    pub peak_windows: [(u32, u32); 2],
    pub n_peak_windows: u8,
    pub weekend_discount_adult: f64,
    pub weekend_discount_reduced: f64,
    pub train_plus_offpeak_discount: f64,
    pub train_plus_peak_cap_adult: u32,
    pub train_plus_peak_cap_reduced: u32,
}

impl SncbTimeRules {
    #[inline]
    pub fn bucket(&self, weekday: u8, board_time: u32) -> TimeBucket {
        if weekday >= 5 {
            return TimeBucket::Weekend;
        }
        for w in &self.peak_windows[..self.n_peak_windows as usize] {
            if board_time >= w.0 && board_time < w.1 {
                return TimeBucket::Peak;
            }
        }
        TimeBucket::OffPeak
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DistanceTariff {
    Bracketed {
        a_cents_per_km: f64,
        b_cents: f64,
        floor_cents: u32,
        min_km: u32,
        cap_from_km: u32,
        cap_km: u32,
        first_class_thresholds: [u32; 2],
        first_class_coeffs: [f64; 3],
        first_class_round_thresholds: [u32; 2],
        first_class_round_grids: [u32; 3],
    },
    Linear {
        intercept_cents: f64,
        slope_cents_per_km: f64,
        min_km: u32,
        max_km: u32,
        floor_cents: u32,
    },
    Band {
        per_km_rate_cents: f64,
        thresholds: [u32; 2],
        coeffs: [f64; 3],
        min_km: u32,
        max_km: u32,
        floor_cents: u32,
    },
}

impl DistanceTariff {
    #[inline]
    pub fn floor_cents(&self) -> u32 {
        match self {
            DistanceTariff::Bracketed { floor_cents, .. }
            | DistanceTariff::Linear { floor_cents, .. }
            | DistanceTariff::Band { floor_cents, .. } => *floor_cents,
        }
    }

    #[inline]
    fn d_eff(d_km: f64, min_km: u32, cap_from_km: u32, cap_km: u32) -> f64 {
        // Round to a whole km FIRST, before clamp/bracket, else a value near a
        // bracket edge (39.7 -> 40, not 39) drops a bracket.
        let d = (d_km.max(0.0).round() as u32).max(min_km);
        if d >= cap_from_km {
            return cap_km as f64;
        }
        if d <= 30 {
            return d as f64;
        }
        if (31..=60).contains(&d) {
            (3 * ((d - 31) / 3) + 32) as f64
        } else if (61..=115).contains(&d) {
            (5 * ((d - 61) / 5) + 63) as f64
        } else {
            d as f64
        }
    }

    #[inline]
    pub fn fare_cents(&self, d_km: f64) -> u32 {
        self.fare_cents_class(d_km, TravelClass::Second)
    }

    #[inline]
    pub fn fare_cents_class(&self, d_km: f64, class: TravelClass) -> u32 {
        match self {
            DistanceTariff::Bracketed {
                a_cents_per_km,
                b_cents,
                floor_cents,
                min_km,
                cap_from_km,
                cap_km,
                first_class_thresholds,
                first_class_coeffs,
                first_class_round_thresholds,
                first_class_round_grids,
            } => {
                let d_eff = Self::d_eff(d_km, *min_km, *cap_from_km, *cap_km);
                let raw = (a_cents_per_km * d_eff + b_cents).max(*floor_cents as f64);
                match class {
                    TravelClass::Second => {
                        let tenths = ((raw + 5.0) / 10.0).floor();
                        (tenths.max(0.0) * 10.0) as u32
                    }
                    TravelClass::First => {
                        let coeff = if d_eff <= first_class_thresholds[0] as f64 {
                            first_class_coeffs[0]
                        } else if d_eff <= first_class_thresholds[1] as f64 {
                            first_class_coeffs[1]
                        } else {
                            first_class_coeffs[2]
                        };
                        Self::round_first_class(
                            raw * coeff,
                            first_class_round_thresholds,
                            first_class_round_grids,
                        )
                    }
                }
            }
            DistanceTariff::Linear {
                intercept_cents,
                slope_cents_per_km,
                min_km,
                max_km,
                floor_cents,
            } => {
                let km = Self::tariff_km(d_km, *min_km, *max_km);
                let raw = intercept_cents + slope_cents_per_km * km as f64;
                (raw.round().max(0.0) as u32).max(*floor_cents)
            }
            DistanceTariff::Band {
                per_km_rate_cents,
                thresholds,
                coeffs,
                min_km,
                max_km,
                floor_cents,
            } => {
                let km = Self::tariff_km(d_km, *min_km, *max_km);
                let coeff = if km <= thresholds[0] {
                    coeffs[0]
                } else if km <= thresholds[1] {
                    coeffs[1]
                } else {
                    coeffs[2]
                };
                let raw = per_km_rate_cents * coeff * km as f64;
                (raw.round().max(0.0) as u32).max(*floor_cents)
            }
        }
    }

    #[inline]
    fn tariff_km(d_km: f64, min_km: u32, max_km: u32) -> u32 {
        let r = d_km.max(0.0).round() as u32;
        if r + 2 >= max_km {
            max_km
        } else {
            r.max(min_km)
        }
    }

    #[inline]
    fn round_first_class(cents: f64, thresholds: &[u32; 2], grids: &[u32; 3]) -> u32 {
        let c = cents.max(0.0);
        let grid = if c < thresholds[0] as f64 {
            grids[0]
        } else if c <= thresholds[1] as f64 {
            grids[1]
        } else {
            grids[2]
        };
        if grid == 0 {
            return c as u32;
        }
        let g = grid as f64;
        ((c + g / 2.0) / g).floor() as u32 * grid
    }
}

#[derive(Clone, Copy, Debug)]
pub enum OperatorModel {
    TimeWindowFlat {
        ticket_cents: u32,
        card_cents: Option<u32>,
        validity_secs: u32,
        operator: TimeWindowOperator,
    },
    TimeWindowFlatTiered {
        is_express: bool,
        single_cents: u32,
        card6_cents: u32,
        card6_reduced_cents: u32,
    },
    DistanceBasePerKm {
        tariff: DistanceTariff,
        rules: SncbTimeRules,
        airport_od_cents: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeWindowOperator {
    Stib,
    Delijn,
}

#[derive(Clone, Debug)]
pub struct OperatorFare {
    pub name: String,
    pub model: OperatorModel,
    pub express_route_names: Vec<String>,
    pub express_route_prefixes: Vec<String>,
    pub express_single_cents: u32,
    pub express_card6_cents: u32,
    pub express_card6_reduced_cents: u32,
    pub airport_station_names: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct FareModel {
    pub enabled: bool,
    pub known_euros_epsilon: KnownEurosEpsilon,
    pub operators: Vec<OperatorFare>,
    pub agglomerations: Vec<crate::structures::cost::AgglomerationZone>,
    pub brupass_cents: u32,
    pub brupass_validity_secs: u32,
}

impl Default for FareModel {
    fn default() -> Self {
        FareModel {
            enabled: false,
            known_euros_epsilon: KnownEurosEpsilon::default(),
            operators: Vec::new(),
            agglomerations: Vec::new(),
            brupass_cents: 0,
            brupass_validity_secs: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum OperatorFareId {
    Modeled { model: OperatorModel },
    Unknown { slot: usize },
}

impl FareModel {
    #[inline]
    fn sncb_scale_and_cap(
        &self,
        rules: &SncbTimeRules,
        ctx: &FareContext,
        board_time: u32,
    ) -> (f64, u32) {
        let reduced = ctx.profile.category.is_reduced();
        let bucket = rules.bucket(ctx.weekday, board_time);
        if ctx.profile.sncb_train_plus {
            match bucket {
                TimeBucket::Peak => {
                    let cap = if reduced {
                        rules.train_plus_peak_cap_reduced
                    } else {
                        rules.train_plus_peak_cap_adult
                    };
                    (1.0, cap)
                }
                TimeBucket::OffPeak | TimeBucket::Weekend => {
                    (1.0 - rules.train_plus_offpeak_discount, u32::MAX)
                }
            }
        } else {
            match bucket {
                TimeBucket::Weekend => {
                    let d = if reduced {
                        rules.weekend_discount_reduced
                    } else {
                        rules.weekend_discount_adult
                    };
                    (1.0 - d, u32::MAX)
                }
                TimeBucket::Peak | TimeBucket::OffPeak => (1.0, u32::MAX),
            }
        }
    }

    #[inline]
    pub fn brupass_active(&self, price: &PriceValue, board_time: u32) -> bool {
        price.brupass_activation != u32::MAX
            && board_time >= price.brupass_activation
            && board_time.saturating_sub(price.brupass_activation) < self.brupass_validity_secs
    }

    #[inline]
    pub fn activate_brupass(&self, price: &mut PriceValue, board_time: u32) {
        let bucketed = self.known_euros_epsilon.bucket(self.brupass_cents);
        price.known_cents = price.known_cents.saturating_add(bucketed);
        price.brupass_credit = self.brupass_cents;
        price.brupass_activation = board_time;
        price.end_sncb_run();
    }

    #[inline]
    pub fn charge_board(
        &self,
        price: &mut PriceValue,
        op: OperatorFareId,
        board_time: u32,
        ctx: &FareContext,
    ) {
        match op {
            OperatorFareId::Modeled {
                model:
                    OperatorModel::TimeWindowFlat { ticket_cents, card_cents, validity_secs, operator },
            } => {
                let free = match operator {
                    TimeWindowOperator::Stib => ctx.profile.stib_subscription,
                    TimeWindowOperator::Delijn => ctx.profile.delijn_subscription,
                };
                let ticket_cents = match operator {
                    TimeWindowOperator::Delijn if ctx.profile.delijn_10_journey => {
                        card_cents.unwrap_or(ticket_cents)
                    }
                    _ => ticket_cents,
                };
                let (activation, credit) = match operator {
                    TimeWindowOperator::Stib => {
                        (&mut price.stib_activation, &mut price.stib_ticket_credit)
                    }
                    TimeWindowOperator::Delijn => {
                        (&mut price.delijn_activation, &mut price.delijn_ticket_credit)
                    }
                };
                if !free {
                    let active = *activation != u32::MAX
                        && board_time >= *activation
                        && board_time.saturating_sub(*activation) < validity_secs;
                    if !active {
                        let bucketed = self.known_euros_epsilon.bucket(ticket_cents);
                        price.known_cents = price.known_cents.saturating_add(bucketed);
                        *activation = board_time;
                    }
                    *credit = ticket_cents;
                }
                price.end_sncb_run();
            }
            OperatorFareId::Modeled {
                model:
                    OperatorModel::TimeWindowFlatTiered {
                        is_express: _,
                        single_cents,
                        card6_cents,
                        card6_reduced_cents,
                    },
            } => {
                if !ctx.profile.tec_subscription {
                    let cents = if ctx.profile.tec_6_journey {
                        if ctx.profile.category.is_reduced() {
                            card6_reduced_cents
                        } else {
                            card6_cents
                        }
                    } else {
                        single_cents
                    };
                    let bucketed = self.known_euros_epsilon.bucket(cents);
                    price.known_cents = price.known_cents.saturating_add(bucketed);
                }
                price.end_sncb_run();
            }
            OperatorFareId::Modeled {
                model: OperatorModel::DistanceBasePerKm { tariff, rules, .. },
            } => {
                if ctx.profile.sncb_subscription {
                    price.sncb_active = true;
                    price.sncb_base_credit = 0;
                    return;
                }
                let base_cents = tariff.floor_cents();
                let (scale, cap) = self.sncb_scale_and_cap(&rules, ctx, board_time);
                if !price.sncb_active {
                    let scaled = (base_cents as f64 * scale).round() as u32;
                    price.known_cents = self
                        .known_euros_epsilon
                        .bucket(price.known_cents.saturating_add(scaled));
                    price.sncb_spend_cents = price.sncb_spend_cents.saturating_add(scaled);
                    price.sncb_active = true;
                    price.sncb_cap_cents = cap;
                    price.sncb_base_credit = scaled;
                } else {
                    price.sncb_base_credit =
                        price.sncb_base_credit.max((base_cents as f64 * scale).round() as u32);
                }
            }
            OperatorFareId::Unknown { slot } => {
                let s = slot.min(N_OP - 1);
                price.unknown[s] = price.unknown[s].saturating_add(1);
                price.end_sncb_run();
            }
        }
    }

    #[inline]
    pub fn accrue_sncb_km(
        &self,
        price: &mut PriceValue,
        tariff: DistanceTariff,
        run_m: f64,
        rules: &SncbTimeRules,
        ctx: &FareContext,
        board_time: u32,
    ) {
        if ctx.profile.sncb_subscription {
            return;
        }
        let (scale, _cap) = self.sncb_scale_and_cap(rules, ctx, board_time);
        let run_m = run_m.max(0.0);
        let km = run_m / 1000.0;
        let total = tariff.fare_cents_class(km, ctx.profile.travel_class) as f64 * scale;
        let floor = tariff.floor_cents() as f64 * scale;
        let new_perkm = (total - floor).max(0.0).round() as u32;
        let old_perkm = price.sncb_run_perkm_cents;
        price.sncb_spend_cents = price
            .sncb_spend_cents
            .saturating_add(new_perkm)
            .saturating_sub(old_perkm);
        let raw = price
            .known_cents
            .saturating_add(new_perkm)
            .saturating_sub(old_perkm);
        price.known_cents = self.known_euros_epsilon.bucket(raw);
        price.sncb_run_perkm_cents = new_perkm;
        price.sncb_run_m = run_m.round() as u32;
    }

    #[inline]
    pub fn apply_sncb_airport_od(&self, price: &mut PriceValue, airport_od_cents: u32) {
        if airport_od_cents == 0 {
            return;
        }
        let base = price.known_cents.saturating_sub(price.sncb_spend_cents);
        price.known_cents = self.known_euros_epsilon.bucket(base.saturating_add(airport_od_cents));
        price.sncb_spend_cents = airport_od_cents;
        price.sncb_cap_cents = u32::MAX;
        price.end_sncb_run();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stib() -> OperatorFareId {
        OperatorFareId::Modeled {
            model: OperatorModel::TimeWindowFlat {
                ticket_cents: 210,
                card_cents: None,
                validity_secs: 5400,
                operator: TimeWindowOperator::Stib,
            },
        }
    }

    const CTX: FareContext = FareContext::DEFAULT;

    fn sncb_rules() -> SncbTimeRules {
        SncbTimeRules {
            peak_windows: [(6 * 3600, 9 * 3600), (16 * 3600, 18 * 3600)],
            n_peak_windows: 2,
            weekend_discount_adult: 0.30,
            weekend_discount_reduced: 0.40,
            train_plus_offpeak_discount: 0.40,
            train_plus_peak_cap_adult: 1400,
            train_plus_peak_cap_reduced: 550,
        }
    }

    fn model() -> FareModel {
        FareModel {
            enabled: true,
            known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
            operators: Vec::new(),
            agglomerations: Vec::new(),
            brupass_cents: 260,
            brupass_validity_secs: 3600,
        }
    }

    #[test]
    fn stib_first_board_charges_and_activates() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        assert_eq!(p.known_cents, 210);
        assert_eq!(p.stib_activation, 8 * 3600);
    }

    #[test]
    fn stib_board_within_window_is_free() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        m.charge_board(&mut p, stib(), 8 * 3600 + 60 * 60, &CTX);
        assert_eq!(p.known_cents, 210, "within-window re-board must be free");
        assert_eq!(p.stib_activation, 8 * 3600, "activation not reset within window");
    }

    #[test]
    fn stib_board_after_window_charges_and_resets() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        let t2 = 8 * 3600 + 91 * 60;
        m.charge_board(&mut p, stib(), t2, &CTX);
        assert_eq!(p.known_cents, 420, "after-window re-board charges a new ticket");
        assert_eq!(p.stib_activation, t2, "activation reset to the new boarding time");
    }

    #[test]
    fn stib_boundary_at_exactly_validity_is_charged() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        m.charge_board(&mut p, stib(), 8 * 3600 + 5400, &CTX);
        assert_eq!(p.known_cents, 420, "board at exactly validity_secs is a new ticket");
    }

    #[test]
    fn stib_subscription_is_free() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.stib_subscription = true;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 0, "STIB subscription rides free");
        assert_eq!(p.stib_activation, u32::MAX, "no ticket window on a subscription");
        assert_eq!(p.stib_ticket_credit, 0, "no reusable credit on a subscription");
    }

    #[test]
    fn unknown_operator_increments_its_slot() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 1 }, 8 * 3600, &CTX);
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 1 }, 9 * 3600, &CTX);
        assert_eq!(p.unknown[1], 2);
        assert_eq!(p.known_cents, 0, "unmodeled boardings add no known spend");
    }


    fn delijn(ticket_cents: u32) -> OperatorFareId {
        OperatorFareId::Modeled {
            model: OperatorModel::TimeWindowFlat {
                ticket_cents,
                card_cents: Some(220),
                validity_secs: 3600,
                operator: TimeWindowOperator::Delijn,
            },
        }
    }

    #[test]
    fn delijn_single_charges_real_euros() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, delijn(300), 8 * 3600, &CTX);
        assert_eq!(p.known_cents, 300, "De Lijn single is 3.00 EUR");
        assert_eq!(p.delijn_activation, 8 * 3600);
    }

    #[test]
    fn delijn_10_journey_price() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.delijn_10_journey = true;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, delijn(300), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 220, "De Lijn 10-journey is 2.20 EUR/journey");
    }

    #[test]
    fn delijn_subscription_is_free() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.delijn_subscription = true;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, delijn(300), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 0, "De Lijn subscription rides free");
    }

    #[test]
    fn stib_and_delijn_windows_are_independent() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        m.charge_board(&mut p, delijn(300), 8 * 3600 + 60, &CTX);
        assert_eq!(p.known_cents, 510, "STIB + De Lijn are charged separately");
        m.charge_board(&mut p, stib(), 8 * 3600 + 600, &CTX);
        assert_eq!(p.known_cents, 510, "STIB within its own window rides free");
        m.charge_board(&mut p, delijn(300), 8 * 3600 + 600, &CTX);
        assert_eq!(p.known_cents, 510, "De Lijn within its own window rides free");
    }


    fn tec(is_express: bool) -> OperatorFareId {
        OperatorFareId::Modeled {
            model: OperatorModel::TimeWindowFlatTiered {
                is_express,
                single_cents: if is_express { 550 } else { 280 },
                card6_cents: if is_express { 440 } else { 223 },
                card6_reduced_cents: if is_express { 352 } else { 180 },
            },
        }
    }

    #[test]
    fn tec_classic_single_adult() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(false), 8 * 3600, &CTX);
        assert_eq!(p.known_cents, 280, "TEC classic single = 2.80");
    }

    #[test]
    fn tec_express_single_adult() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(true), 8 * 3600, &CTX);
        assert_eq!(p.known_cents, 550, "TEC express single = 5.50");
    }

    #[test]
    fn tec_6journey_classic_and_express() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.tec_6_journey = true;
        let mut c = PriceValue::ZERO;
        m.charge_board(&mut c, tec(false), 8 * 3600, &ctx);
        assert_eq!(c.known_cents, 223, "TEC 6-journey classic = 2.23/j");
        let mut e = PriceValue::ZERO;
        m.charge_board(&mut e, tec(true), 8 * 3600, &ctx);
        assert_eq!(e.known_cents, 440, "TEC 6-journey express = 4.40/j");
    }

    #[test]
    fn tec_6journey_reduced_category() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.tec_6_journey = true;
        ctx.profile.category = PassengerCategory::Young;
        let mut c = PriceValue::ZERO;
        m.charge_board(&mut c, tec(false), 8 * 3600, &ctx);
        assert_eq!(c.known_cents, 180, "reduced TEC 6-journey classic = 1.80/j");
        let mut e = PriceValue::ZERO;
        m.charge_board(&mut e, tec(true), 8 * 3600, &ctx);
        assert_eq!(e.known_cents, 352, "reduced TEC 6-journey express = 3.52/j");
    }

    #[test]
    fn tec_reduced_single_has_no_variant() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.category = PassengerCategory::Bim;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(false), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 280, "reduced single falls back to full single");
    }

    #[test]
    fn tec_subscription_is_free() {
        let m = model();
        let mut ctx = CTX;
        ctx.profile.tec_subscription = true;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(true), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 0, "TEC subscription rides free");
    }

    #[test]
    fn tec_charged_every_boarding_no_window() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(false), 8 * 3600, &CTX);
        m.charge_board(&mut p, tec(false), 8 * 3600 + 300, &CTX);
        assert_eq!(p.known_cents, 560, "each TEC boarding is charged (no window)");
    }


    fn sncb_tariff() -> DistanceTariff {
        DistanceTariff::Bracketed {
            a_cents_per_km: 16.8546,
            b_cents: 145.1226,
            floor_cents: 262,
            min_km: 3,
            cap_from_km: 116,
            cap_km: 118,
            first_class_thresholds: [36, 51],
            first_class_coeffs: [1.40, 1.50, 1.60],
            first_class_round_thresholds: [2500, 5000],
            first_class_round_grids: [10, 50, 100],
        }
    }

    fn sncb() -> OperatorFareId {
        OperatorFareId::Modeled {
            model: OperatorModel::DistanceBasePerKm {
                tariff: sncb_tariff(),
                rules: sncb_rules(),
                airport_od_cents: 790,
            },
        }
    }

    fn ctx_weekday_offpeak() -> FareContext {
        FareContext { profile: FareProfile::default(), weekday: 2 }
    }


    #[test]
    fn bracketed_d_eff_bracket_midpoints() {
        let t = sncb_tariff();
        assert_eq!(t.fare_cents(1.0), t.fare_cents(3.0), "d=1 → d_eff=3");
        assert_eq!(t.fare_cents(2.0), t.fare_cents(3.0), "d=2 → d_eff=3");
        assert_eq!(t.fare_cents(31.0), t.fare_cents(32.0), "d=31 → d_eff=32");
        assert_eq!(t.fare_cents(33.0), t.fare_cents(32.0), "d=33 → d_eff=32");
        assert_eq!(t.fare_cents(34.0), t.fare_cents(35.0), "d=34 → d_eff=35");
        assert_eq!(t.fare_cents(60.0), t.fare_cents(59.0), "d=60 → d_eff=59");
        assert_eq!(t.fare_cents(61.0), t.fare_cents(63.0), "d=61 → d_eff=63");
        assert_eq!(t.fare_cents(65.0), t.fare_cents(63.0), "d=65 → d_eff=63");
        assert_eq!(t.fare_cents(66.0), t.fare_cents(68.0), "d=66 → d_eff=68");
        assert_eq!(t.fare_cents(115.0), t.fare_cents(113.0), "d=115 → d_eff=113");
        assert_eq!(t.fare_cents(116.0), t.fare_cents(118.0), "d=116 → d_eff=118");
        assert_eq!(t.fare_cents(117.0), t.fare_cents(118.0), "d=117 → d_eff=118");
        assert_eq!(t.fare_cents(200.0), t.fare_cents(118.0), "d=200 → d_eff=118 (cap)");
        assert_eq!(t.fare_cents(23.2), t.fare_cents(23.0), "23.2 → 23 (rounds down)");
        assert_eq!(t.fare_cents(23.5), t.fare_cents(24.0), "23.5 → 24 (half up)");
        assert_eq!(t.fare_cents(39.7), t.fare_cents(41.0), "39.7 → 40 → midpoint 41");
        assert!(
            t.fare_cents(39.7) > t.fare_cents(39.0),
            "39.7 must not truncate down into the 37-39 bracket"
        );
    }

    #[test]
    fn bracketed_tariff_exact_sample_fares() {
        let t = sncb_tariff();
        assert_eq!(t.fare_cents(3.0), 260, "d_eff=3 floored to 2.60");
        assert_eq!(t.fare_cents(1.0), 260, "d=1 → d_eff=3 → 2.60 (floor)");
        assert_eq!(t.fare_cents(32.0), 680, "d_eff=32 → 6.80");
        assert_eq!(t.fare_cents(47.0), 940, "d_eff=47 → 9.40");
        assert_eq!(t.fare_cents(118.0), 2130, "d_eff=118 → 21.30 (ceiling)");
        assert_eq!(t.fare_cents(200.0), 2130, "d=200 caps at the 21.30 ceiling");
    }


    #[test]
    fn first_class_sample_fares_band_coeffs() {
        let t = sncb_tariff();
        assert_eq!(t.fare_cents_class(3.0, TravelClass::First), 370, "d_eff=3 ×1.40 → 3.70");
        assert_eq!(t.fare_cents_class(32.0, TravelClass::First), 960, "d_eff=32 ×1.40 → 9.60");
        assert_eq!(t.fare_cents_class(47.0, TravelClass::First), 1410, "d_eff=47 ×1.50 → 14.10");
        assert_eq!(t.fare_cents_class(118.0, TravelClass::First), 3400, "d_eff=118 ×1.60 → 34.00 (0.50 grid)");
        for km in [3.0, 20.0, 50.0, 100.0, 118.0] {
            assert!(
                t.fare_cents_class(km, TravelClass::First) > t.fare_cents(km),
                "1st class must exceed 2nd at {km} km"
            );
        }
    }

    #[test]
    fn first_class_rounding_tiers() {
        let th = [2500u32, 5000u32];
        let gr = [10u32, 50u32, 100u32];
        assert_eq!(DistanceTariff::round_first_class(366.8, &th, &gr), 370);
        assert_eq!(DistanceTariff::round_first_class(364.9, &th, &gr), 360);
        assert_eq!(DistanceTariff::round_first_class(365.0, &th, &gr), 370, "x.x5 half up");
        assert_eq!(DistanceTariff::round_first_class(2760.0, &th, &gr), 2750);
        assert_eq!(DistanceTariff::round_first_class(2775.0, &th, &gr), 2800, "0.25 midpoint half up");
        assert_eq!(DistanceTariff::round_first_class(5149.0, &th, &gr), 5100);
        assert_eq!(DistanceTariff::round_first_class(5150.0, &th, &gr), 5200, "0.50 midpoint half up");
        assert_eq!(DistanceTariff::round_first_class(2500.0, &th, &gr), 2500);
    }

    #[test]
    fn first_class_above_50_euros_rounds_to_1_euro() {
        let t = DistanceTariff::Bracketed {
            a_cents_per_km: 500.0,
            b_cents: 0.0,
            floor_cents: 262,
            min_km: 3,
            cap_from_km: 116,
            cap_km: 118,
            first_class_thresholds: [36, 51],
            first_class_coeffs: [1.40, 1.50, 1.60],
            first_class_round_thresholds: [2500, 5000],
            first_class_round_grids: [10, 50, 100],
        };
        assert_eq!(t.fare_cents_class(118.0, TravelClass::First), 94400, ">50 EUR → 1 EUR grid");
        let t2 = DistanceTariff::Bracketed {
            a_cents_per_km: 500.0,
            b_cents: 23.0,
            floor_cents: 262,
            min_km: 3,
            cap_from_km: 116,
            cap_km: 118,
            first_class_thresholds: [36, 51],
            first_class_coeffs: [1.40, 1.50, 1.60],
            first_class_round_thresholds: [2500, 5000],
            first_class_round_grids: [10, 50, 100],
        };
        assert_eq!(t2.fare_cents_class(118.0, TravelClass::First), 94400, "94436.8 → 944 EUR");
    }

    #[test]
    fn first_class_composes_with_weekend_discount() {
        let m = model();
        let ctx = FareContext {
            profile: FareProfile {
                travel_class: TravelClass::First,
                ..FareProfile::default()
            },
            weekday: 5,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.known_cents, 183, "weekend board floor −30% (down-payment, class-agnostic)");
        m.accrue_sncb_km(&mut p, sncb_tariff(), 40_000.0, &sncb_rules(), &ctx, 12 * 3600);
        let first_full = sncb_tariff().fare_cents_class(41.0, TravelClass::First);
        assert_eq!(first_full, 1250, "1st-class 40 km base = 12.50");
        assert_eq!(p.known_cents, (1250f64 * 0.70).round() as u32, "weekend −30% on 1st-class total");
    }

    #[test]
    fn second_class_default_unchanged_by_travel_class_field() {
        let t = sncb_tariff();
        for km in [3.0, 20.0, 41.0, 100.0, 118.0] {
            assert_eq!(
                t.fare_cents_class(km, TravelClass::Second),
                t.fare_cents(km),
                "Second class == legacy fare_cents at {km} km"
            );
        }
    }

    #[test]
    fn bracketed_tariff_floor_applies() {
        let t = sncb_tariff();
        assert_eq!(t.fare_cents(3.0), 260, "short-trip raw < floor → floored to 2.60");
    }

    #[test]
    fn bracketed_tariff_monotonic_nondecreasing_to_cap() {
        let t = sncb_tariff();
        let mut prev = 0u32;
        for km in 1..=130 {
            let f = t.fare_cents(km as f64);
            assert!(f >= prev, "fare must be non-decreasing at {km} km ({f} < {prev})");
            prev = f;
        }
        assert_eq!(t.fare_cents(116.0), t.fare_cents(500.0));
    }

    #[test]
    fn band_and_linear_alternatives_still_available() {
        let band = DistanceTariff::Band {
            per_km_rate_cents: 12.40,
            thresholds: [36, 51],
            coeffs: [1.40, 1.50, 1.60],
            min_km: 3,
            max_km: 118,
            floor_cents: 260,
        };
        assert_eq!(band.fare_cents(20.0), 347, "band: 12.40*1.40*20 = 347.2 → 347");
        let lin = DistanceTariff::Linear {
            intercept_cents: 157.0,
            slope_cents_per_km: 16.4,
            min_km: 3,
            max_km: 118,
            floor_cents: 260,
        };
        assert_eq!(lin.fare_cents(3.0), 260, "linear: floored to 2.60");
        assert_eq!(lin.fare_cents(100.0), 1797, "linear: 157 + 16.4*100 = 1797");
    }

    #[test]
    fn sncb_first_board_charges_base_once_and_activates() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx_weekday_offpeak());
        assert_eq!(p.known_cents, 262, "first SNCB board charges base floor (2.6151 → 262)");
        assert!(p.sncb_active, "SNCB run active after first board");
    }

    #[test]
    fn sncb_per_km_accrues_over_a_ride_weekday() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 40_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 840, "40 km fare: d_eff=41 → 8.40 EUR");
    }

    #[test]
    fn sncb_subscription_is_free() {
        let m = model();
        let mut ctx = ctx_weekday_offpeak();
        ctx.profile.sncb_subscription = true;
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 40_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 0, "SNCB subscription rides free (base + per-km)");
    }

    #[test]
    fn sncb_weekend_adult_minus_30_percent() {
        let m = model();
        let ctx = FareContext { profile: FareProfile::default(), weekday: 5 };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.known_cents, 183, "weekend adult floor −30% = 183");
        m.accrue_sncb_km(&mut p, sncb_tariff(), 40_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 588, "weekend adult 40 km fare 840 ×0.70 = 588");
    }

    #[test]
    fn sncb_weekend_reduced_minus_40_percent() {
        let m = model();
        let ctx = FareContext {
            profile: FareProfile { category: PassengerCategory::Senior, ..FareProfile::default() },
            weekday: 6,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.known_cents, 157, "weekend reduced base −40% = 157");
    }

    #[test]
    fn sncb_train_plus_offpeak_minus_40_percent() {
        let m = model();
        let ctx = FareContext {
            profile: FareProfile { sncb_train_plus: true, ..FareProfile::default() },
            weekday: 2,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.known_cents, 157, "Train+ off-peak base −40% = 157");
        assert_eq!(p.sncb_cap_cents, u32::MAX, "no cap off-peak");
    }

    #[test]
    fn sncb_train_plus_peak_full_price_with_cap() {
        let m = model();
        let ctx = FareContext {
            profile: FareProfile { sncb_train_plus: true, ..FareProfile::default() },
            weekday: 2,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 8 * 3600, &ctx);
        assert_eq!(p.known_cents, 262, "Train+ peak base is full price");
        assert_eq!(p.sncb_cap_cents, 1400, "Train+ peak sets the adult per-journey cap");
        m.accrue_sncb_km(&mut p, sncb_tariff(), 100_000.0, &sncb_rules(), &ctx, 8 * 3600);
        assert_eq!(p.known_cents, 1800, "raw spend carried uncapped in known_cents");
        assert_eq!(p.sncb_spend_cents, 1800, "raw SNCB spend tracked for the cap");
        assert_eq!(capped_cents(&p), 1400, "display cap binds at 14.00");
    }

    #[test]
    fn sncb_train_plus_peak_cap_reduced() {
        let m = model();
        let ctx = FareContext {
            profile: FareProfile {
                sncb_train_plus: true,
                category: PassengerCategory::Bim,
                ..FareProfile::default()
            },
            weekday: 1,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 17 * 3600, &ctx);
        assert_eq!(p.sncb_cap_cents, 550, "reduced Train+ peak cap = 5.50");
    }

    #[test]
    fn sncb_airport_od_overrides_base_formula() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 12_000.0, &sncb_rules(), &ctx, 12 * 3600);
        m.apply_sncb_airport_od(&mut p, 790);
        assert_eq!(p.known_cents, 790, "airport OD replaces the base+per-km fare");
        assert_eq!(p.sncb_spend_cents, 790);
    }

    #[test]
    fn sncb_airport_od_terminates_the_run() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.apply_sncb_airport_od(&mut p, 790);
        assert_eq!(p.known_cents, 790);
        assert!(!p.sncb_active, "airport OD ends the SNCB run");
        assert_eq!(p.sncb_base_credit, 0, "airport OD clears the base credit");
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx);
        assert_eq!(p.known_cents, 790 + 262, "post-airport rail is a new ticket");
        assert!(p.sncb_active);
    }

    #[test]
    fn sncb_second_train_same_run_does_not_recharge_base() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600);
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1800, &ctx);
        assert_eq!(p.known_cents, 480, "contiguous SNCB change re-charges no base");
        assert!(p.sncb_active);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 30_000.0, &sncb_rules(), &ctx, 12 * 3600 + 1800);
        assert_eq!(p.known_cents, 650, "30 km fare: d_eff=30 → 6.50 EUR");
    }

    #[test]
    fn accrue_sncb_km_is_run_total_and_backs_out_prior() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 480, "20 km fare: d_eff=20 → 4.80 EUR");
        assert_eq!(p.sncb_run_perkm_cents, 480 - 262, "accrued = fare − floor");
        m.accrue_sncb_km(&mut p, sncb_tariff(), 30_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 650, "delta-charged to the 30 km fare = 650");
        assert_eq!(p.sncb_run_perkm_cents, 650 - 262);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 10_000.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 310, "refunds to the 10 km fare (3.10 EUR)");
        assert_eq!(p.sncb_spend_cents, 310, "raw spend tracks the run total too");
    }

    #[test]
    fn end_sncb_run_clears_run_tracking() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        p.sncb_run_board_stop = 7;
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600);
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 0 }, 12 * 3600 + 600, &ctx);
        assert_eq!(p.sncb_run_board_stop, u32::MAX, "run board stop cleared");
        assert_eq!(p.sncb_run_m, 0, "run metres cleared");
        assert_eq!(p.sncb_run_perkm_cents, 0, "run per-km cleared");
        assert!(!p.sncb_active);
    }

    #[test]
    fn sncb_bus_sncb_reentry_charges_base_twice() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 0 }, 12 * 3600 + 600, &ctx);
        assert!(!p.sncb_active, "unmodeled board ends the SNCB run");
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx);
        assert_eq!(p.known_cents, 524, "SNCB re-entry after a bus charges base twice");
        assert!(p.sncb_active);
    }

    #[test]
    fn stib_board_ends_sncb_run() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.charge_board(&mut p, stib(), 12 * 3600 + 600, &ctx);
        assert!(!p.sncb_active, "STIB board ends the SNCB run");
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx);
        assert_eq!(p.known_cents, 262 + 210 + 262);
    }

    #[test]
    fn accrue_sncb_km_zero_distance_is_noop() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        m.accrue_sncb_km(&mut p, sncb_tariff(), 0.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 262, "zero-km segment stays at the floor 262");
    }


    fn stib_only() -> PriceValue {
        PriceValue { known_cents: 210, ..PriceValue::ZERO }
    }
    fn stib_plus_letec() -> PriceValue {
        let mut u = [0; N_OP];
        u[0] = 1;
        PriceValue { known_cents: 210, unknown: u, ..PriceValue::ZERO }
    }
    fn letec_only() -> PriceValue {
        let mut u = [0; N_OP];
        u[0] = 1;
        PriceValue { known_cents: 0, unknown: u, ..PriceValue::ZERO }
    }

    fn capped_cents(p: &PriceValue) -> u32 {
        let sncb = p.sncb_spend_cents.min(p.sncb_cap_cents);
        p.known_cents - p.sncb_spend_cents + sncb
    }

    #[test]
    fn stib_only_dominates_stib_plus_letec() {
        assert!(stib_only().dominates(&stib_plus_letec()));
        assert!(!stib_plus_letec().dominates(&stib_only()));
    }

    #[test]
    fn letec_only_incomparable_with_stib_only() {
        assert!(!letec_only().dominates(&stib_only()));
        assert!(!stib_only().dominates(&letec_only()));
    }

    #[test]
    fn letec_only_dominates_stib_plus_letec() {
        assert!(letec_only().dominates(&stib_plus_letec()));
        assert!(!stib_plus_letec().dominates(&letec_only()));
    }

    #[test]
    fn cheaper_sncb_not_dominated_by_pricier() {
        let cheap = PriceValue { known_cents: 470, sncb_active: true, sncb_base_credit: 250, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 690, sncb_active: true, sncb_base_credit: 250, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "cheaper SNCB dominates pricier on price");
        assert!(!pricey.dominates(&cheap), "pricier SNCB does not dominate cheaper");
    }


    #[test]
    fn sncb_prepaid_not_dominated_by_cheaper_stib_to_same_hub() {
        let sncb_prepaid = PriceValue {
            known_cents: 360,
            sncb_active: true,
            sncb_base_credit: 250,
            ..PriceValue::ZERO
        };
        let stib_to_hub = PriceValue {
            known_cents: 210,
            stib_activation: 8 * 3600,
            stib_ticket_credit: 210,
            ..PriceValue::ZERO
        };
        assert!(
            !stib_to_hub.dominates(&sncb_prepaid),
            "cheaper STIB-to-hub label must not prune the SNCB-prepaid label"
        );
        assert!(
            !sncb_prepaid.dominates(&stib_to_hub),
            "SNCB-prepaid does not dominate the STIB-to-hub label either"
        );
    }

    #[test]
    fn stib_held_ticket_not_dominated_by_cheaper_creditless() {
        let stib_held = PriceValue {
            known_cents: 210,
            stib_activation: 8 * 3600,
            stib_ticket_credit: 210,
            ..PriceValue::ZERO
        };
        let creditless_cheaper = PriceValue {
            known_cents: 50,
            ..PriceValue::ZERO
        };
        assert!(
            !creditless_cheaper.dominates(&stib_held),
            "cheaper creditless label must not prune a held-STIB-ticket label"
        );
    }

    #[test]
    fn same_credits_dominance_reduces_to_known_cents() {
        let cheap = PriceValue {
            known_cents: 470,
            sncb_active: true,
            sncb_base_credit: 250,
            ..PriceValue::ZERO
        };
        let pricey = PriceValue { known_cents: 690, ..cheap };
        assert!(cheap.dominates(&pricey), "same SNCB credit: cheaper dominates");
        assert!(!pricey.dominates(&cheap));
    }

    #[test]
    fn disabled_path_price_dominance_is_pure_known_and_unknown() {
        let a = PriceValue::ZERO;
        let b = PriceValue::ZERO;
        assert!(a.dominates(&b) && b.dominates(&a), "ZERO dominates ZERO both ways");
    }

    #[test]
    fn charge_board_sets_and_clears_credits() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.sncb_base_credit, 262, "SNCB board records base credit");
        assert_eq!(p.stib_ticket_credit, 0);
        m.charge_board(&mut p, stib(), 12 * 3600 + 600, &ctx);
        assert_eq!(p.stib_ticket_credit, 210, "STIB board records ticket credit");
        assert_eq!(p.sncb_base_credit, 0, "STIB board clears the SNCB base credit");
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 0 }, 13 * 3600, &ctx);
        assert_eq!(p.stib_ticket_credit, 210, "held STIB ticket survives an unmodeled board");
        assert_eq!(p.sncb_base_credit, 0);
    }

    #[test]
    fn cheaper_profile_priced_plan_not_dominated_by_pricier() {
        let cheap = PriceValue { known_cents: 300, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 560, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "cheaper profile-priced plan dominates pricier");
        assert!(!pricey.dominates(&cheap), "pricier does not prune the cheaper plan");
    }

    #[test]
    fn train_plus_peak_cap_not_used_in_dominance() {
        let low_raw = PriceValue {
            known_cents: 1500,
            sncb_spend_cents: 1500,
            sncb_cap_cents: 1400,
            sncb_active: true,
            ..PriceValue::ZERO
        };
        let high_raw = PriceValue { known_cents: 2760, sncb_spend_cents: 2760, ..low_raw };
        assert!(low_raw.dominates(&high_raw), "lower raw spend dominates (cap not in compare)");
        assert!(!high_raw.dominates(&low_raw));
        assert_eq!(capped_cents(&low_raw), 1400);
        assert_eq!(capped_cents(&high_raw), 1400);
    }

    #[test]
    fn delijn_credit_buy_back_dominance() {
        let delijn_held = PriceValue {
            known_cents: 300,
            delijn_activation: 8 * 3600,
            delijn_ticket_credit: 300,
            ..PriceValue::ZERO
        };
        let creditless_cheaper = PriceValue { known_cents: 50, ..PriceValue::ZERO };
        assert!(
            !creditless_cheaper.dominates(&delijn_held),
            "cheaper creditless label must not prune a held-De-Lijn-ticket label"
        );
    }


    #[test]
    fn brupass_activation_charges_once_and_sets_window() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.activate_brupass(&mut p, 8 * 3600);
        assert_eq!(p.known_cents, 260, "Brupass price charged once on activation");
        assert_eq!(p.brupass_credit, 260, "reusable Brupass credit recorded");
        assert_eq!(p.brupass_activation, 8 * 3600, "coverage window anchored at board");
    }

    #[test]
    fn brupass_active_within_window_only() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.activate_brupass(&mut p, 8 * 3600);
        assert!(m.brupass_active(&p, 8 * 3600), "active at activation time");
        assert!(m.brupass_active(&p, 8 * 3600 + 59 * 60), "active within 60 min");
        assert!(!m.brupass_active(&p, 8 * 3600 + 3600), "expired at exactly validity");
        assert!(!m.brupass_active(&p, 8 * 3600 + 3601), "expired past window");
        assert!(!m.brupass_active(&PriceValue::ZERO, 8 * 3600));
    }

    #[test]
    fn brupass_activation_ends_sncb_run() {
        let m = model();
        let mut p = PriceValue { sncb_active: true, sncb_base_credit: 260, ..PriceValue::ZERO };
        m.activate_brupass(&mut p, 8 * 3600);
        assert!(!p.sncb_active, "Brupass activation ends the SNCB run");
        assert_eq!(p.sncb_base_credit, 0, "SNCB base credit cleared");
    }

    #[test]
    fn brupass_held_not_dominated_by_cheaper_creditless() {
        let stib_ticket = PriceValue {
            known_cents: 240,
            stib_activation: 8 * 3600,
            stib_ticket_credit: 240,
            ..PriceValue::ZERO
        };
        let brupass_held = PriceValue {
            known_cents: 260,
            brupass_activation: 8 * 3600,
            brupass_credit: 260,
            ..PriceValue::ZERO
        };
        assert!(
            !stib_ticket.dominates(&brupass_held),
            "cheaper single-ticket label must not prune the Brupass-held label"
        );
        assert!(
            !brupass_held.dominates(&stib_ticket),
            "Brupass-held label must not prune the cheaper single-ticket label"
        );
    }

    #[test]
    fn brupass_credit_absent_is_pure_known_dominance() {
        let cheap = PriceValue { known_cents: 240, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 500, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "no Brupass credit ⇒ plain known dominance");
        assert!(!pricey.dominates(&cheap));
    }

    #[test]
    fn known_cents_epsilon_bucketing() {
        let e = KnownEurosEpsilon { a: 10.0, b: 0.0 };
        assert_eq!(e.bucket(0), 0);
        assert_eq!(e.bucket(9), 0);
        assert_eq!(e.bucket(10), 10);
        assert_eq!(e.bucket(19), 10);
        assert_eq!(e.bucket(210), 210);
        assert_eq!(e.bucket(214), 210);
        let id = KnownEurosEpsilon { a: 0.0, b: 0.0 };
        assert_eq!(id.bucket(214), 214);
    }

    #[test]
    fn disabled_model_is_constructible_and_default_off() {
        let m = FareModel::default();
        assert!(!m.enabled, "fares default off");
        assert!(m.operators.is_empty());
    }
}
