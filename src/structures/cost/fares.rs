//! Transit pricing as a POST-HOC plan annotation (multi-operator fares).
//!
//! Price was originally an in-search Pareto dominance axis but has been DEMOTED to
//! a post-hoc annotation: the fare of a chosen plan is computed after routing, not
//! carried in the RAPTOR labels. The `PriceValue`/`dominates` machinery below is
//! retained (it now only runs in the fare unit tests) and still documents the
//! additive per-boarding fare model that the post-hoc walker reuses.
//!
//! Price is not a plain scalar. Only some operators are fare-modeled, so a
//! label's price is a pair:
//!   - `known_cents`: accumulated modeled spend, ε-bucketed for dominance;
//!   - `unknown[op]`: a per-unmodeled-operator boarding count (e.g. generic
//!     feeds). An unpriceable route is never hidden: it is incomparable
//!     on price, so it is always retained.
//!
//! The whole feature is gated by a single master switch (`FareModel.enabled`).
//! When off, no price field influences dominance and the hot loop is untouched.
//!
//! Scope of this increment: STIB `time_window_flat` and SNCB
//! `distance_base_per_km`. Fare products and day caps deserialize leniently but
//! are inert here (see the design spec phase 3).

/// Fixed cap on the number of distinct UNMODELED operators tracked per label
/// (`unknown[op]` count vector). Belgium's unmodeled transit operators are
/// De Lijn, TEC, and at most a couple of generic feeds; 4 covers the current
/// operator set with headroom. A boarding on an unmodeled operator beyond this
/// cap folds into the last slot (documented approximation; raise `N_OP` if the
/// operator set grows). Kept small so the `[u8; N_OP]` compare stays O(1).
pub const N_OP: usize = 4;

/// Per-label price value carried through RAPTOR. `Copy`, O(1) to compare/update.
/// All fields are inert (zero / sentinel) when `FareModel.enabled` is false.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriceValue {
    /// Accumulated modeled spend in ε-bucketed euro cents.
    pub known_cents: u32,
    /// Per-unmodeled-operator boarding count.
    pub unknown: [u8; N_OP],
    /// STIB time-window ticket activation timestamp (seconds since midnight);
    /// `u32::MAX` = no active ticket.
    pub stib_activation: u32,
    /// SNCB "contiguous run" flag: `true` while a single SNCB ticket is in force,
    /// i.e. the rider has boarded SNCB and not yet boarded a non-SNCB operator.
    /// The `base_euros` charge is levied once per contiguous run (on the first
    /// SNCB board while this is `false`); a train-to-train change on the same
    /// journey keeps it `true` so `base` is not re-charged, while the per-km
    /// component keeps accruing on each ride. Boarding any non-SNCB operator
    /// resets it to `false`, so a later re-entry into SNCB is a genuinely new
    /// ticket and charges `base` again. Carried state, NOT a dominance axis.
    pub sncb_active: bool,
    /// Cash value (cents) of the reusable SNCB-base credit this label carries:
    /// `base_cents` while `sncb_active`, else `0`. A contiguous SNCB run has
    /// already paid the base, so a future SNCB board reuses it for free — that
    /// makes this label cheaper in SNCB-continuing futures than a label that has
    /// NOT paid it. `dominates` charges this difference (see the buy-back rule),
    /// so a pre-paid label is never pruned by a cheaper one lacking the credit.
    /// Carried state, NOT an independent dominance axis.
    pub sncb_base_credit: u32,
    /// Cash value (cents) of the reusable STIB-ticket credit this label carries:
    /// `ticket_cents` while a STIB ticket is held (`stib_activation != u32::MAX`),
    /// else `0`. Mirrors `sncb_base_credit` for the STIB time-window ticket: a
    /// held ticket lets a future STIB board ride free within its window, so it
    /// lowers future cost. The credit is a flat cash value with no time-decay in
    /// `dominates`. KNOWN LIMITATION (applies equally to `delijn_ticket_credit`
    /// and `brupass_credit`): because the credit ignores the REMAINING validity
    /// window, two labels with equal price and equal credit but different ticket
    /// activation timestamps are mutually dominating, so the one with the longer
    /// remaining window (which could still ride a later in-window board free) can
    /// be pruned by the one with a shorter window. This is a narrow corner, not a
    /// blanket "only ever retains labels" guarantee. Carried state, NOT an axis.
    pub stib_ticket_credit: u32,
    /// De Lijn time-window ticket activation timestamp (seconds since midnight);
    /// `u32::MAX` = no active ticket. Mirrors `stib_activation` for the De Lijn
    /// `time_window_flat` model (a separate operator ⇒ a separate ticket window).
    pub delijn_activation: u32,
    /// Cash value (cents) of the reusable De Lijn-ticket credit (mirrors
    /// `stib_ticket_credit`). Carried state, NOT an independent dominance axis.
    pub delijn_ticket_credit: u32,
    /// RAW SNCB portion of `known_cents` (cents), tracked so a per-JOURNEY SNCB cap
    /// (Train+ peak) can be applied at DISPLAY as `min(sncb_spend, sncb_cap)` while
    /// dominance still carries the raw total (spec §9 soundness). Carried state,
    /// NOT a dominance axis: a lower raw spend always implies a lower-or-equal
    /// capped price, so dominance on `known_cents` (which includes this) is sound.
    pub sncb_spend_cents: u32,
    /// Per-journey SNCB cap (cents) in force for THIS label's profile/time regime;
    /// `u32::MAX` = no cap. Set when an SNCB fare is charged under a capping regime
    /// (Train+ at peak). Used only at display: `capped = known - sncb_spend +
    /// min(sncb_spend, sncb_cap)`. Never in `dominates`.
    pub sncb_cap_cents: u32,
    /// Cash value (cents) of the reusable Brupass credit this label carries:
    /// `brupass_cents` while a Brupass is held/active in-zone, else `0` (spec
    /// Appendix A.3). A Brupass is an OPTIONAL, ADDITIVE product: one Brupass covers
    /// STIB+SNCB+De Lijn+TEC WITHIN the Brussels zone for `brupass_activation`'s
    /// ~60-min window. Once bought (charged once into `known_cents`), every further
    /// in-zone boarding on ANY operator within the window rides free, so this credit
    /// lowers future in-zone cost exactly like `stib_ticket_credit`. Mirrors the
    /// buy-back rule in `dominates`: a label lacking the Brupass credit the other
    /// holds must pay `brupass_cents` to be as capable in a future that reuses it,
    /// so the "with-Brupass" and "without-Brupass" branches stay mutually
    /// incomparable and the cheaper per continuation survives. Carried state, NOT an
    /// independent dominance axis.
    pub brupass_credit: u32,
    /// Brupass activation timestamp (seconds since midnight); `u32::MAX` = no active
    /// Brupass. Set when a Brupass is bought at the first in-zone boarding; drives
    /// the ~60-min coverage window (`FareModel.brupass_validity_secs`). Boarding
    /// in-zone within `[activation, activation + validity)` on any operator is free.
    pub brupass_activation: u32,
    /// Compact stop index of the current contiguous SNCB run's FIRST boarding
    /// (`u32::MAX` = no active run). The SNCB per-km fare is a fixed ZONE-TO-ZONE
    /// tariff (spec Appendix A.2, corrected): the chargeable distance is measured
    /// between the FARE ENDPOINTS of the run's first-board stop and the current
    /// alighting stop (each collapsed to its agglomeration's reference node when it
    /// is in a zone), NOT along the ridden pattern from the boarding station. So any
    /// Brussels station → any Antwerpen station yields the same distance regardless
    /// of which stations or lines are used. Set lazily at the first per-km accrual
    /// of a run; carried across a contiguous SNCB→SNCB change; reset with the run.
    /// Carried state, NOT a dominance axis.
    pub sncb_run_board_stop: u32,
    /// Chargeable railway metres charged so far for the current contiguous SNCB run
    /// (from `sncb_run_board_stop`'s fare endpoint to the last-charged alight's fare
    /// endpoint), rounded to whole metres. Recomputed per alight as a whole (the
    /// zone-to-zone distance is an O(1) lookup), so `accrue_sncb_km` can back out the
    /// previously charged per-km and re-charge the exact run total. `0` with no
    /// active run. Integer metres (not `f64`) so `PriceValue` keeps `Eq`/`Hash`.
    /// Carried state.
    pub sncb_run_m: u32,
    /// Per-km cents charged so far for the current contiguous SNCB run (the euro
    /// value of `sncb_run_m` under this run's scale). Backed out and re-charged as
    /// the run distance is recomputed per alight. `0` with no active run. Carried
    /// state, NOT a dominance axis (it is already reflected in `known_cents`).
    pub sncb_run_perkm_cents: u32,
}

impl PriceValue {
    /// The zero price a source/root label starts with (no spend, no boardings,
    /// no active ticket). Identical whether or not fares are enabled.
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

    /// End the current contiguous SNCB run: clear the run-active flag, the reusable
    /// base credit, and the zone-to-zone run distance tracking (`sncb_run_board_stop`
    /// / `sncb_run_m` / `sncb_run_perkm_cents`). Called wherever a run ends (a
    /// non-SNCB board, a Brupass activation, an airport-OD substitution), so a later
    /// SNCB re-entry starts a genuinely new zone-to-zone ticket. Leaves the already
    /// accrued `known_cents` / `sncb_spend_cents` untouched.
    #[inline]
    pub fn end_sncb_run(&mut self) {
        self.sncb_active = false;
        self.sncb_base_credit = 0;
        self.sncb_run_board_stop = u32::MAX;
        self.sncb_run_m = 0;
        self.sncb_run_perkm_cents = 0;
    }

    /// Price dominance with the "credit buy-back" rule: `self ⪯ other` (self
    /// prunes other) iff `self` is no worse on every price component AFTER paying
    /// for any reusable credit `other` holds that `self` lacks.
    ///
    /// The subtlety (defect A): `known_cents` alone is not a sound dominance key,
    /// because two carried credits — a paid SNCB base (`sncb_base_credit`) and a
    /// held STIB ticket (`stib_ticket_credit`) — lower FUTURE cost by letting a
    /// later boarding ride free. A label that has pre-paid a credit can look
    /// pricier now yet finish cheaper in a future that reuses it, so pruning it by
    /// a cheaper credit-less label discards the eventually-cheaper plan.
    ///
    /// The tight sound condition is
    ///   `known_self + Σ_{c ∈ other\self} value(c)  ≤  known_other`  (and unknowns
    /// componentwise). `penalty` is exactly that sum: for every reusable credit
    /// `other` holds but `self` lacks, `self` must "buy it back" to be as capable
    /// in futures that reuse it. When `self` already holds a credit, `other`'s
    /// same credit gives it no advantage over `self`, so it adds nothing.
    ///
    /// Fares-off invariant: every label is `PriceValue::ZERO`, so both credit
    /// fields are 0, `penalty` is always 0, and this reduces to the plain
    /// `known_cents <=` + `unknown <=` test — byte-identical to pre-feature.
    #[inline]
    pub fn dominates(&self, other: &PriceValue) -> bool {
        // Buy-back penalty over every reusable credit `other` holds and `self`
        // lacks (SNCB base, STIB ticket, De Lijn ticket, Brupass). The SNCB per-journey cap
        // (`sncb_cap_cents`/`sncb_spend_cents`) is DISPLAY-only and never enters
        // this compare: dominance carries the RAW `known_cents`, and a lower raw
        // always implies a lower-or-equal capped price (spec §9 soundness).
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

/// ε-bucket parameters for the euro (known_cents) axis, shaped like the existing
/// per-axis `epsilon` tuning (`a + b * value`). Bucketing bounds the number of
/// price tiers surviving per Pareto cell, controlling frontier blow-up.
#[derive(Clone, Copy, Debug)]
pub struct KnownEurosEpsilon {
    /// Absolute bucket width in cents.
    pub a: f64,
    /// Relative bucket width (fraction of the accumulated cents).
    pub b: f64,
}

impl Default for KnownEurosEpsilon {
    fn default() -> Self {
        // 10 cents absolute, no relative component: one bucket per ~10-cent tier.
        KnownEurosEpsilon { a: 10.0, b: 0.0 }
    }
}

impl KnownEurosEpsilon {
    /// Snap an exact cent amount onto its ε-bucket lower edge, so two prices
    /// within one bucket width quantize identically and thus compare equal on the
    /// dominance axis. `a <= 0` disables bucketing (identity, strict no-op).
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

/// Passenger category. `Young`/`Senior`/`Bim` are the "reduced" categories
/// (BIM = Belgian increased-reimbursement status). Drives TEC "reduced" pricing
/// and the SNCB reduced discounts/caps. Default `Adult` (full fare).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PassengerCategory {
    #[default]
    Adult,
    Young,
    Senior,
    Bim,
}

impl PassengerCategory {
    /// True for the "reduced" categories (Young/Senior/BIM).
    #[inline]
    pub fn is_reduced(self) -> bool {
        !matches!(self, PassengerCategory::Adult)
    }
}

/// SNCB travel class. Only SNCB fares differ by class; every other operator
/// ignores it. `Second` is the default (the fares computed everywhere else).
/// `First` re-prices the SNCB base off the UNROUNDED 2nd-class raw times a
/// distance-band coefficient, on a coarser rounding grid (see
/// `DistanceTariff::fare_cents_class`). The whole discount pipeline
/// (Train+ cap, weekend/off-peak percentages, subscription → 0) then applies on
/// top of the 1st-class base exactly as for 2nd class — only the base changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TravelClass {
    #[default]
    Second,
    First,
}

/// Pre-committed, FIXED-per-query fare profile: the passenger category plus the
/// held products (subscriptions, N-journey cards, Train+, Brupass). Every
/// boarding's marginal fare is a deterministic function of `(time, tier, OD,
/// profile)`, which keeps the price additive and the in-search dominance sound.
///
/// This is the runtime (cost-layer) profile the fare functions consume. The
/// GraphQL `FareProfileInput` and `routing_raptor::FareProfile` map into it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FareProfile {
    pub category: PassengerCategory,
    /// Per-operator subscription ⇒ that operator's legs are free (0).
    pub stib_subscription: bool,
    pub delijn_subscription: bool,
    pub tec_subscription: bool,
    pub sncb_subscription: bool,
    /// SNCB Train+ advantage card (peak per-journey cap; off-peak/weekend −40%).
    pub sncb_train_plus: bool,
    /// De Lijn 10-journey card ⇒ a fixed lower per-journey price.
    pub delijn_10_journey: bool,
    /// TEC 6-journey card ⇒ a fixed lower per-journey price (classic/express).
    pub tec_6_journey: bool,
    /// SNCB travel class (default `Second`). Affects ONLY SNCB fares; every other
    /// operator ignores it.
    pub travel_class: TravelClass,
}

/// The per-query fare context threaded into the charge functions: the resolved
/// profile plus the query's calendar signals needed for SNCB time buckets. Built
/// once per query in the routing layer, borrowed read-only in the hot loop.
///
/// `weekday` is `0 = Monday .. 6 = Sunday` (matching `chrono::Weekday`'s
/// `num_days_from_monday`). Peak is derived from `weekday` + boarding time; the
/// weekend flag is `weekday >= 5` (Sat/Sun).
#[derive(Clone, Copy, Debug)]
pub struct FareContext {
    pub profile: FareProfile,
    /// Day of week, `0 = Mon .. 6 = Sun`.
    pub weekday: u8,
}

impl FareContext {
    /// A default (no products, adult) context on a weekday. Used when a query
    /// carries no profile (spec §13.7: "full single-ticket price, no products").
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

    /// True on Saturday/Sunday.
    #[inline]
    pub fn is_weekend(&self) -> bool {
        self.weekday >= 5
    }
}

/// SNCB time bucket derived from `(weekday, boarding_time)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeBucket {
    /// Weekday within a configured peak window (e.g. 06:00-09:00 / 16:00-18:00).
    Peak,
    /// Weekend (Sat/Sun) — always treated as off-peak.
    Weekend,
    /// Weekday outside every peak window.
    OffPeak,
}

/// Config-derived SNCB peak windows and profile-dependent discount/cap knobs.
/// Held inside the SNCB `OperatorModel` variant so the whole model is data-driven
/// (project policy: nothing hardcoded in Rust fare logic).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SncbTimeRules {
    /// Peak windows as `[(start_secs, end_secs)]` (seconds since midnight),
    /// half-open `[start, end)`, weekdays only. Empty ⇒ never peak.
    pub peak_windows: [(u32, u32); 2],
    /// Number of valid entries in `peak_windows` (0..=2).
    pub n_peak_windows: u8,
    /// Weekend discount without Train+, ADULT (fraction removed, e.g. 0.30).
    pub weekend_discount_adult: f64,
    /// Weekend discount without Train+, reduced categories (e.g. 0.40).
    pub weekend_discount_reduced: f64,
    /// Off-peak (incl. weekend) discount WITH Train+, all categories (e.g. 0.40).
    pub train_plus_offpeak_discount: f64,
    /// Train+ peak per-journey cap (cents), ADULT (e.g. 1400).
    pub train_plus_peak_cap_adult: u32,
    /// Train+ peak per-journey cap (cents), reduced (e.g. 550).
    pub train_plus_peak_cap_reduced: u32,
}

impl SncbTimeRules {
    /// The time bucket for a boarding at `board_time` on `weekday`.
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

/// SNCB base single-ticket fare as a function of TARIFF DISTANCE (whole km).
///
/// The chargeable distance `D` (metres, from `sncb_fare_distance_m`, zone-collapsed)
/// is turned into a tariff distance `tariff_km = clamp(round(D_km), min_km, max_km)`
/// and then priced. Two data-driven shapes are supported so the exact SNCB table can
/// be swapped in later without code changes:
///
/// - `Linear` — the validated linear fit of the real 2026 second-class single base
///   fare: `fare = intercept + slope * tariff_km`, floored at `floor_cents`. Accurate
///   to ~EUR 0.20 across 10-110 km; drifts because the true function is piecewise.
///   This is the DEFAULT.
/// - `Band` — the true piecewise form: `fare = per_km_rate * band_coeff(tariff_km) *
///   tariff_km`, floored at `floor_cents`, with `band_coeff` selected by two km
///   thresholds. Left available for a cent-exact table once SNCB's per-km constant
///   and rounding are known; NOT the default.
///
/// Both floor at `floor_cents` (EUR 2.60) and clamp to `[min_km, max_km]` (short
/// trips billed as `min_km`; long trips capped at `max_km`, the advertised "120 km"
/// ceiling billed as 118 km). All fields are compiled from `config.yaml`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DistanceTariff {
    /// The EXACT published 2026 SNCB second-class base tariff (verified zero-error
    /// vs the published table). A single linear price of an EFFECTIVE distance
    /// `d_eff`, where `d_eff` snaps the real distance onto the SNCB tariff table's
    /// bracket midpoints, then floored and rounded to the 0.10-EUR grid.
    ///
    /// `d_eff(d_km)` (the fixed SNCB tariff STRUCTURE, implemented in code):
    ///   - clamp `d` up to `min_km` (minimum taxable distance);
    ///   - `d >= cap_from_km` (116) ⇒ `cap_km` (118, the maximum taxable distance);
    ///   - `d <= 30` ⇒ `d` (each km its own value);
    ///   - `31..=60` ⇒ arithmetic midpoint of its 3-km bracket (32,35,…,59);
    ///   - `61..=115` ⇒ arithmetic midpoint of its 5-km bracket (63,68,…,113).
    ///
    /// Price: `raw = max(a * d_eff + b, floor)`, then round to the NEAREST 0.10 EUR
    /// with halves rounding UP. `a`/`b`/`floor` are EUR (config), carried as cents.
    ///
    /// FIRST-CLASS (not computed here, kept for a future option): 1st class = the
    /// UNROUNDED 2nd-class raw × a distance-dependent multiplier (×1.40 for 3..=36,
    /// ×1.50 for 37..=51, ×1.60 for 52+), then rounded on the 0.10 grid. The
    /// `first_class_coeffs`/`first_class_thresholds` fields carry this inert.
    Bracketed {
        /// Slope `a` in cents per effective km (EUR/km × 100).
        a_cents_per_km: f64,
        /// Intercept `b` in cents (EUR × 100).
        b_cents: f64,
        /// Absolute fare floor in cents (always applied before rounding).
        floor_cents: u32,
        /// Minimum taxable distance (km): shorter trips bill as `min_km`.
        min_km: u32,
        /// At/above this distance (km) the fare caps: `d_eff = cap_km`.
        cap_from_km: u32,
        /// The capped effective distance (km) used at/above `cap_from_km`.
        cap_km: u32,
        /// The two ascending km thresholds over `d_eff` for the 1st-class multiplier
        /// bands (e.g. [36, 51]).
        first_class_thresholds: [u32; 2],
        /// The three 1st-class multipliers applied to the UNROUNDED 2nd-class raw
        /// (e.g. [1.40, 1.50, 1.60]).
        first_class_coeffs: [f64; 3],
        /// 1st-class rounding tier boundaries in cents, ascending (e.g. [2500, 5000]).
        /// A base below `[0]` rounds to `first_class_round_grids[0]`, in `[[0],[1]]`
        /// to grid[1], above `[1]` to grid[2].
        first_class_round_thresholds: [u32; 2],
        /// 1st-class rounding grid (cents) per tier (e.g. [10, 50, 100]). Half-up.
        first_class_round_grids: [u32; 3],
    },
    /// Validated linear fit: `max(floor, intercept + slope * tariff_km)`.
    Linear {
        /// Intercept in cents (e.g. 157 for EUR 1.57).
        intercept_cents: f64,
        /// Slope in cents per tariff km (e.g. 16.4 for EUR 0.164/km).
        slope_cents_per_km: f64,
        min_km: u32,
        max_km: u32,
        floor_cents: u32,
    },
    /// Piecewise band model: `max(floor, per_km_rate * band_coeff * tariff_km)`.
    /// `band_coeff` is `coeffs[0]` for `tariff_km <= thresholds[0]`, `coeffs[1]` for
    /// `thresholds[0] < tariff_km <= thresholds[1]`, else `coeffs[2]`.
    Band {
        /// SNCB base per-km rate in cents/km (the constant from the tariff PDF).
        per_km_rate_cents: f64,
        /// Two ascending km thresholds delimiting the three coefficient bands.
        thresholds: [u32; 2],
        /// Band coefficients (e.g. [1.40, 1.50, 1.60]).
        coeffs: [f64; 3],
        min_km: u32,
        max_km: u32,
        floor_cents: u32,
    },
}

impl DistanceTariff {
    /// The absolute fare floor (cents) for this tariff — also the amount charged at
    /// the SNCB board step (`known` gets the floor up front; the distance-dependent
    /// remainder accrues at alight in `accrue_sncb_km`).
    #[inline]
    pub fn floor_cents(&self) -> u32 {
        match self {
            DistanceTariff::Bracketed { floor_cents, .. }
            | DistanceTariff::Linear { floor_cents, .. }
            | DistanceTariff::Band { floor_cents, .. } => *floor_cents,
        }
    }

    /// The SNCB effective tariff distance `d_eff` (see `DistanceTariff::Bracketed`):
    /// clamp up to `min_km`; cap to `cap_km` at/above `cap_from_km`; keep `d` as-is
    /// up to 30 km; otherwise snap to the arithmetic midpoint of its 3-km (31..=60)
    /// or 5-km (61..=115) bracket. `d_km` may be fractional; the bracket region is
    /// decided by `floor(d_km)`.
    #[inline]
    fn d_eff(d_km: f64, min_km: u32, cap_from_km: u32, cap_km: u32) -> f64 {
        // The tariff is defined on INTEGER tariff-kilometres, so the fractional
        // rail-approximation distance must be rounded to the nearest whole km
        // (half up) FIRST, before the min/max clamps and bracket assignment.
        // Without this, sub-km error emits off-grid <=30 km fares and can drop a
        // whole bracket near a boundary (e.g. 39.7 km must price as 40, not 39).
        let d = (d_km.max(0.0).round() as u32).max(min_km);
        if d >= cap_from_km {
            return cap_km as f64;
        }
        // 1-30 km: every whole km is its own value.
        if d <= 30 {
            return d as f64;
        }
        if (31..=60).contains(&d) {
            // 3-km brackets, midpoints 32,35,…,59.
            (3 * ((d - 31) / 3) + 32) as f64
        } else if (61..=115).contains(&d) {
            // 5-km brackets, midpoints 63,68,…,113.
            (5 * ((d - 61) / 5) + 63) as f64
        } else {
            d as f64
        }
    }

    /// Whole-ticket base fare (cents, pre-discount) for a chargeable distance of
    /// `d_km` kilometres, 2nd class. Convenience wrapper over `fare_cents_class`.
    #[inline]
    pub fn fare_cents(&self, d_km: f64) -> u32 {
        self.fare_cents_class(d_km, TravelClass::Second)
    }

    /// Whole-ticket base fare (cents, pre-discount) for a chargeable distance of
    /// `d_km` kilometres in travel class `class`. `d_km` is rounded to the nearest
    /// whole km and clamped to `[min_km, max_km]`, then priced by the selected shape
    /// and floored.
    ///
    /// FIRST CLASS (Bracketed only): 1st-class base =
    /// `round_first_class(raw_2nd_unrounded * band_coeff(d_eff))`, where `raw_2nd`
    /// is the UNROUNDED, floored 2nd-class raw (`max(a*d_eff + b, floor)`),
    /// `band_coeff` is selected by `first_class_thresholds` over `d_eff`
    /// (`coeffs[0]` for `d_eff <= t0`, `coeffs[1]` for `t0 < d_eff <= t1`, else
    /// `coeffs[2]`), and `round_first_class` rounds (half up) to 0.10 EUR below
    /// 25 EUR, 0.50 EUR in [25,50], 1 EUR above 50. The `Linear`/`Band` shapes have
    /// no 1st-class formula, so they return their 2nd-class fare for either class.
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
                // Raw 2nd-class price in cents, floored at the absolute minimum
                // (UNROUNDED — the 1st-class formula multiplies this raw directly).
                let raw = (a_cents_per_km * d_eff + b_cents).max(*floor_cents as f64);
                match class {
                    TravelClass::Second => {
                        // Round to the nearest 0.10 EUR (10-cent grid), halves rounding
                        // UP: floor((raw + 5) / 10) * 10. A value exactly at the x.x5
                        // midpoint (raw ending in .5 cents on the 10-cent grid) rounds up.
                        let tenths = ((raw + 5.0) / 10.0).floor();
                        (tenths.max(0.0) * 10.0) as u32
                    }
                    TravelClass::First => {
                        // 1st class: scale the UNROUNDED 2nd-class raw by the distance
                        // band coefficient, then round on the coarser 1st-class grid.
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

    /// Whole tariff kilometres. Rounds to the nearest km, bills trips shorter than
    /// `min_km` as `min_km`, and applies SNCB's "120 km cap": any distance within
    /// 2 km of the billed maximum (i.e. >= 116 km for a 118 km `max_km`) bills as
    /// `max_km`. See the SNCB fare block in config.yaml.
    #[inline]
    fn tariff_km(d_km: f64, min_km: u32, max_km: u32) -> u32 {
        let r = d_km.max(0.0).round() as u32;
        if r + 2 >= max_km {
            max_km
        } else {
            r.max(min_km)
        }
    }

    /// Round a 1st-class fare (`cents`, may be fractional) onto its per-tier grid,
    /// half up. The grid is picked by which tier `cents` falls in: below
    /// `thresholds[0]` ⇒ `grids[0]`; in `[thresholds[0], thresholds[1]]` ⇒
    /// `grids[1]`; above `thresholds[1]` ⇒ `grids[2]`. Tier selection uses the
    /// UNROUNDED value (so a fare of exactly 25.00 EUR uses the 25-50 grid). A grid
    /// of 0 means "no rounding" (return the value truncated to whole cents).
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
        // Half-up onto the grid: floor((c + g/2) / g) * g.
        ((c + g / 2.0) / g).floor() as u32 * grid
    }
}

/// Per-operator marginal-fare model. Each variant is a data-driven function of
/// `(boarding time, route tier, OD, profile) → cents`. All fields are compiled
/// from `config.yaml`; nothing is hardcoded here (project policy).
#[derive(Clone, Copy, Debug)]
pub enum OperatorModel {
    /// STIB / De Lijn: a flat ticket valid for `validity_secs` from activation,
    /// gated on boarding. Boarding within the window is marginal €0; else charge
    /// the ticket and reset the window anchor to the boarding time. `card_cents`
    /// (`Some`) is the per-journey price of a held N-journey card (De Lijn
    /// 10-journey); it replaces `ticket_cents` when the profile holds that card.
    /// `operator` selects which ticket-window state fields to use so two distinct
    /// time-window operators (STIB and De Lijn) keep independent windows.
    TimeWindowFlat {
        ticket_cents: u32,
        card_cents: Option<u32>,
        validity_secs: u32,
        operator: TimeWindowOperator,
    },
    /// TEC: a per-boarding flat fare split by route tier (classic vs express).
    /// No transfer window — each boarding is charged. The tier is resolved at
    /// build time into `is_express`, so the hot path only reads a bool. Prices are
    /// profile-dependent (single vs 6-journey card, reduced category).
    TimeWindowFlatTiered {
        /// Resolved at build time from the config express classification rule.
        is_express: bool,
        /// Single-ticket price for this route's tier (cents).
        single_cents: u32,
        /// 6-journey per-journey price, non-reduced (cents).
        card6_cents: u32,
        /// 6-journey per-journey price, reduced category (cents).
        card6_reduced_cents: u32,
    },
    /// SNCB: a single distance tariff `tariff.fare_cents(D_km)` (real 2026 base
    /// single fare as a function of the zone-collapsed chargeable distance), then
    /// scaled/capped by the time bucket + profile (weekend/off-peak discounts,
    /// Train+ peak cap). The distance-independent FLOOR (`tariff.floor_cents()`) is
    /// charged at the board step (`charge_board`); the remaining distance-dependent
    /// amount accrues at the route-scan alight site (`accrue_sncb_km`), which
    /// recomputes the whole `fare_cents(D)` per candidate alight. `airport_od` is a
    /// fixed special-OD override (Brussels ↔ Airport), keyed by stop name.
    DistanceBasePerKm {
        /// Base single-fare-of-distance function (linear fit or piecewise band).
        tariff: DistanceTariff,
        rules: SncbTimeRules,
        /// Fixed airport special-OD fare (cents); `0` ⇒ no airport override.
        airport_od_cents: u32,
    },
}

/// Which time-window operator a `TimeWindowFlat` model belongs to, selecting the
/// (activation, credit) field pair used on `PriceValue` so STIB and De Lijn keep
/// independent ticket windows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeWindowOperator {
    Stib,
    Delijn,
}

/// A single fare-modeled operator: its normalized name and marginal model.
#[derive(Clone, Debug)]
pub struct OperatorFare {
    /// Normalized `agency.name` key (e.g. "STIB").
    pub name: String,
    pub model: OperatorModel,
    /// For a tiered operator (TEC): the route-name tokens (uppercased) whose
    /// `route_short_name` (or long name) marks an EXPRESS route by SUBSTRING match.
    /// Resolved per route at lookup-build time into `TimeWindowFlatTiered.is_express`.
    /// Empty for non-tiered operators. Config-driven (project policy).
    pub express_route_names: Vec<String>,
    /// For a tiered operator (TEC): the route-name PREFIXES (uppercased) whose
    /// `route_short_name` ONLY marks an EXPRESS route by starts-with match (TEC's
    /// rule: route number begins with "E"). Long-name matching is intentionally
    /// excluded here: many classic routes carry an E-initial destination in
    /// `route_long_name` (Eupen, Eghezée, Esneux) that must not be misclassified.
    /// (The `express_route_names` substring field still matches both short and long
    /// names.) ORed with `express_route_names`. Empty for non-tiered operators.
    /// Config-driven.
    pub express_route_prefixes: Vec<String>,
    /// For a tiered operator (TEC): the EXPRESS-tier price set (single, 6-journey,
    /// 6-journey reduced) in cents. The `model` template carries the CLASSIC tier;
    /// at per-route lookup build a route classified express (see
    /// `express_route_names`/`express_route_prefixes`) swaps these in. All zero for
    /// non-tiered operators. Config-driven.
    pub express_single_cents: u32,
    pub express_card6_cents: u32,
    pub express_card6_reduced_cents: u32,
    /// For SNCB: substrings (uppercased) that identify an airport station name,
    /// so an OD touching such a station applies the fixed airport special-OD fare.
    /// Empty for non-SNCB operators. Config-driven.
    pub airport_station_names: Vec<String>,
}

/// Compiled, always-on fare model applied into `RaptorIndex` at startup from
/// config. Runtime tuning (`#[serde(skip)]` on `RaptorIndex`), never serialized.
#[derive(Clone, Debug)]
pub struct FareModel {
    /// THE master switch. When false, the whole pricing feature is a no-op.
    pub enabled: bool,
    /// ε-bucket parameters for the known-cents dominance axis.
    pub known_euros_epsilon: KnownEurosEpsilon,
    /// Fare-modeled operators (this increment: STIB only is active).
    pub operators: Vec<OperatorFare>,
    /// SNCB flat agglomeration zones (Brussels / Antwerpen). Config-driven bounding
    /// polygons; a stop inside a zone is collapsed to that zone's single fare node,
    /// so railway distance within a zone is not charged (spec Appendix A.2). Empty
    /// when no zones are configured, which restores plain full-km SNCB pricing.
    pub agglomerations: Vec<crate::structures::cost::AgglomerationZone>,
    /// Brupass single-journey price (cents); `0` = Brupass unavailable. Brupass is
    /// NOT a user option: it is a post-hoc CAP on the Brussels multi-operator fare,
    /// applied automatically in the central post-hoc breakdown walker. When a plan's
    /// PAID (non-subscription) boardings whose stops are in the Brussels flat zone
    /// span two or more DISTINCT operators, the traveller could buy ONE Brupass
    /// covering them, so the in-zone multi-operator cost is capped at
    /// `min(Σ individual in-zone tickets, brupass_cents)`. Config-driven.
    pub brupass_cents: u32,
    /// Brupass coverage window (seconds). Retained for config/back-compat; the
    /// post-hoc cap is window-agnostic (it caps the whole in-zone multi-operator
    /// sum for the journey). Config-driven.
    pub brupass_validity_secs: u32,
}

impl Default for FareModel {
    fn default() -> Self {
        // Off by default: a graph built without a `fares` config block behaves
        // byte-identically to pre-feature routing.
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

/// Runtime identity assigned to a boarding at fare-application time.
///
/// Resolved from the pattern's route → agency name via `RaptorIndex`'s
/// operator lookup. `Modeled` carries the operator's compiled model; `Unknown`
/// carries the `unknown[]` slot to increment.
#[derive(Clone, Copy, Debug)]
pub enum OperatorFareId {
    /// A fare-modeled operator (index into `FareModel.operators`), with its model.
    Modeled { model: OperatorModel },
    /// An unmodeled operator, tracked by its `unknown[]` slot (`0..N_OP`).
    Unknown { slot: usize },
}

impl FareModel {
    /// SNCB per-boarding discount multiplier for the time bucket + profile, and the
    /// per-journey cap (cents; `u32::MAX` = none) that display applies. Both the
    /// base (charged at board) and the per-km (accrued at alight) are scaled by the
    /// same multiplier so the whole SNCB fare is consistently discounted; the cap
    /// is a per-journey ceiling carried raw and applied only at display (spec §9).
    ///
    /// - With Train+: peak ⇒ full price (×1.0) but a per-journey cap; off-peak /
    ///   weekend ⇒ −`train_plus_offpeak_discount` for all categories, no cap.
    /// - Without Train+: weekend ⇒ ADULT −`weekend_discount_adult`, reduced
    ///   −`weekend_discount_reduced`; weekday (peak or off-peak) ⇒ full price.
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
                // Off-peak and weekend: −40% for all categories, no cap.
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
                // Weekday (peak or off-peak) without a card: full price, no cap.
                TimeBucket::Peak | TimeBucket::OffPeak => (1.0, u32::MAX),
            }
        }
    }

    /// True when `price` carries a Brupass that is active at `board_time`, i.e. one
    /// was bought and the ~60-min coverage window has not elapsed (spec Appendix
    /// A.3). While active, an in-zone boarding on ANY operator is free. Conservative
    /// on the window edge (`< validity`, matching the STIB ticket-window model).
    #[inline]
    pub fn brupass_active(&self, price: &PriceValue, board_time: u32) -> bool {
        price.brupass_activation != u32::MAX
            && board_time >= price.brupass_activation
            && board_time.saturating_sub(price.brupass_activation) < self.brupass_validity_secs
    }

    /// Activate a Brupass at the FIRST in-zone boarding (spec Appendix A.3): charge
    /// `brupass_cents` once into `known_cents`, record the reusable credit (for the
    /// dominance buy-back), and anchor the coverage window at `board_time`. The
    /// boarding that activates the Brupass is itself covered (free) — no operator
    /// fare is charged for it. Any contiguous SNCB run ends here: the Brupass, not an
    /// SNCB base ticket, now covers in-zone rail. Caller has checked `enabled`,
    /// `profile.brupass`, in-zone, no already-active Brupass, and `brupass_cents > 0`.
    #[inline]
    pub fn activate_brupass(&self, price: &mut PriceValue, board_time: u32) {
        let bucketed = self.known_euros_epsilon.bucket(self.brupass_cents);
        price.known_cents = price.known_cents.saturating_add(bucketed);
        price.brupass_credit = self.brupass_cents;
        price.brupass_activation = board_time;
        // The Brupass now covers in-zone rail, so end any contiguous SNCB run: a
        // later out-of-zone SNCB re-entry is a genuinely new (non-covered) ticket.
        price.end_sncb_run();
    }

    /// Charge the marginal fare for boarding operator `op` at `board_time`
    /// (seconds since midnight) under the fixed per-query `ctx` (profile + weekday),
    /// mutating `price` in place. Unmodeled boardings bump the `unknown[]` count.
    /// All euro amounts are ε-bucketed into `known_cents`.
    ///
    /// Caller must have checked `enabled` first; this is only ever reached on the
    /// gated (fares-on) path.
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
                // A subscription makes this operator's legs free: no charge, no
                // window/credit (the rider never buys a ticket).
                let free = match operator {
                    TimeWindowOperator::Stib => ctx.profile.stib_subscription,
                    TimeWindowOperator::Delijn => ctx.profile.delijn_subscription,
                };
                // De Lijn 10-journey card replaces the single-ticket price.
                let ticket_cents = match operator {
                    TimeWindowOperator::Delijn if ctx.profile.delijn_10_journey => {
                        card_cents.unwrap_or(ticket_cents)
                    }
                    _ => ticket_cents,
                };
                // Select the per-operator (activation, credit) fields so STIB and
                // De Lijn keep independent ticket windows.
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
                        // Charge a fresh ticket and reset the window anchor.
                        let bucketed = self.known_euros_epsilon.bucket(ticket_cents);
                        price.known_cents = price.known_cents.saturating_add(bucketed);
                        *activation = board_time;
                    }
                    // Within the window: marginal €0, activation unchanged. Either way
                    // a valid ticket is now held, so record its reusable buy-back
                    // value (defect A): a future board within the window rides free.
                    *credit = ticket_cents;
                }
                // Boarding any non-SNCB operator ends a contiguous SNCB run: a later
                // SNCB re-entry is a new ticket and charges `base` again.
                price.end_sncb_run();
            }
            // TEC: a per-boarding flat fare split by route tier. No transfer window,
            // so every boarding is charged (unless a subscription is held). The
            // per-journey price depends on the profile (single vs 6-journey card,
            // reduced category). Tier already resolved into `is_express` at build.
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
                        // Single ticket has no reduced variant in the source data
                        // (spec A.2 [ASSUMPTION]): reduced applies to the 6-journey.
                        single_cents
                    };
                    let bucketed = self.known_euros_epsilon.bucket(cents);
                    price.known_cents = price.known_cents.saturating_add(bucketed);
                }
                // TEC is not SNCB: boarding it ends any contiguous SNCB run.
                price.end_sncb_run();
            }
            // SNCB: charge `base` once per contiguous run (first SNCB board while
            // inactive), scaled by the time/profile multiplier, and mark the run
            // active. The per-km component accrues per stop at the scan site via
            // `accrue_sncb_km` (same multiplier). A subscription makes SNCB free.
            OperatorFareId::Modeled {
                model: OperatorModel::DistanceBasePerKm { tariff, rules, .. },
            } => {
                if ctx.profile.sncb_subscription {
                    // Free: no base, no per-km (accrue is skipped by the caller when
                    // subscribed via the same profile check). Still mark the run so
                    // a contiguous SNCB→SNCB change is not treated as a new journey.
                    price.sncb_active = true;
                    price.sncb_base_credit = 0;
                    return;
                }
                // The distance-independent floor is charged up front; the remaining
                // distance-dependent amount accrues at alight (`accrue_sncb_km`).
                let base_cents = tariff.floor_cents();
                let (scale, cap) = self.sncb_scale_and_cap(&rules, ctx, board_time);
                if !price.sncb_active {
                    let scaled = (base_cents as f64 * scale).round() as u32;
                    price.known_cents = self
                        .known_euros_epsilon
                        .bucket(price.known_cents.saturating_add(scaled));
                    price.sncb_spend_cents = price.sncb_spend_cents.saturating_add(scaled);
                    price.sncb_active = true;
                    // The per-journey cap in force for this journey's regime (the
                    // first SNCB board sets it; carried for display-time min).
                    price.sncb_cap_cents = cap;
                    // Reusable base credit (defect A): scaled value, so a label that
                    // pre-paid the base is not pruned by one that would re-pay it.
                    price.sncb_base_credit = scaled;
                } else {
                    // Contiguous SNCB→SNCB change: base already paid, run stays
                    // active; keep the base credit (do not re-charge).
                    price.sncb_base_credit =
                        price.sncb_base_credit.max((base_cents as f64 * scale).round() as u32);
                }
            }
            OperatorFareId::Unknown { slot } => {
                let s = slot.min(N_OP - 1);
                price.unknown[s] = price.unknown[s].saturating_add(1);
                // An unmodeled boarding also ends a contiguous SNCB run: the base
                // credit is spent, so clear it. (A held time-window ticket, STIB or
                // De Lijn, survives — an unmodeled board does not invalidate it.)
                price.end_sncb_run();
            }
        }
    }

    /// Accrue the SNCB per-km component for a ride's segment under the fixed
    /// per-query `ctx`. `run_m` is the WHOLE contiguous-run chargeable railway
    /// distance (metres) — the fixed ZONE-TO-ZONE tariff distance between the fare
    /// endpoints of the run's first-board stop and the current alighting stop (spec
    /// Appendix A.2, corrected). This is recomputed and passed FRESH per alight (an
    /// O(1) zone-to-zone lookup at the caller), so the per-km charge is a function of
    /// the run's endpoints only, NOT of which stations/lines were used along the way:
    /// any Brussels station → any Antwerpen station charges the same per-km.
    ///
    /// Because it is the run TOTAL, this method backs out the per-km already charged
    /// for the run (`price.sncb_run_perkm_cents`) and re-charges the exact new total,
    /// keeping `known_cents` and `sncb_spend_cents` consistent. Called at the
    /// route-scan alight site, once per candidate alight label, for a modeled SNCB
    /// operator only. `board_time` selects the time bucket (peak/off-peak/weekend).
    /// No-op for a subscription (free) or a zero per-km. A shrinking `run_m` (e.g. an
    /// intra-zone alight after an out-of-zone one) refunds the difference.
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
        // The whole SNCB base ticket for this run's chargeable distance, minus the
        // distance-independent floor already charged at the board step. That
        // remainder (scaled by the time/profile multiplier) is the "per-km-equivalent"
        // accrual; it is delta-charged against what was already accrued for this run.
        let total = tariff.fare_cents_class(km, ctx.profile.travel_class) as f64 * scale;
        let floor = tariff.floor_cents() as f64 * scale;
        let new_perkm = (total - floor).max(0.0).round() as u32;
        let old_perkm = price.sncb_run_perkm_cents;
        // Update the raw SNCB spend by the signed delta (saturating on both sides).
        price.sncb_spend_cents = price
            .sncb_spend_cents
            .saturating_add(new_perkm)
            .saturating_sub(old_perkm);
        // `known_cents` carries the same signed delta, re-bucketed to stay quantized.
        let raw = price
            .known_cents
            .saturating_add(new_perkm)
            .saturating_sub(old_perkm);
        price.known_cents = self.known_euros_epsilon.bucket(raw);
        price.sncb_run_perkm_cents = new_perkm;
        price.sncb_run_m = run_m.round() as u32;
    }

    /// Apply the fixed airport special-OD fare: it OVERRIDES the base+per-km SNCB
    /// fare for this journey. Called at the alight site (in place of the per-km
    /// accrual) when the boarding/alighting pair is an airport OD. Removes the SNCB
    /// spend charged so far this run and substitutes the flat `airport_od_cents`.
    /// A subscription still makes it free. No time/category discount is applied
    /// (spec A.2 [ASSUMPTION]: fixed regardless of card/category).
    ///
    /// The airport OD is a self-contained flat ticket, so it TERMINATES the SNCB
    /// run: `sncb_active`/`sncb_base_credit` are cleared, so a further contiguous
    /// rail ride starts a genuinely new ticket (charges `base` again) rather than
    /// accruing per-km on top of the flat 7.90 or reusing a stale base credit in
    /// dominance. Without this, a Brussels↔Airport leg followed by more rail would
    /// double-charge / mis-dominate.
    #[inline]
    pub fn apply_sncb_airport_od(&self, price: &mut PriceValue, airport_od_cents: u32) {
        if airport_od_cents == 0 {
            return;
        }
        // Back out the SNCB spend accrued so far this journey, substitute the flat
        // fare. `known_cents` re-bucketed to stay quantized.
        let base = price.known_cents.saturating_sub(price.sncb_spend_cents);
        price.known_cents = self.known_euros_epsilon.bucket(base.saturating_add(airport_od_cents));
        price.sncb_spend_cents = airport_od_cents;
        // Airport OD is a flat fare: no per-journey cap applies to it.
        price.sncb_cap_cents = u32::MAX;
        // The flat airport ticket closes the SNCB run: a subsequent contiguous rail
        // ride is a new ticket, not a continuation of this one.
        price.end_sncb_run();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// STIB ticket: 210 cents, 90-minute (5400 s) window.
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

    /// A weekday, adult, no-products context (the default single-ticket query).
    const CTX: FareContext = FareContext::DEFAULT;

    /// SNCB time rules used in the fare tests: peak 06:00-09:00 / 16:00-18:00,
    /// weekend −30% adult / −40% reduced, Train+ off-peak −40%, Train+ peak caps
    /// 1400 adult / 550 reduced.
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
            // Disable ε-bucketing in the window-logic tests so exact cents show.
            known_euros_epsilon: KnownEurosEpsilon { a: 0.0, b: 0.0 },
            operators: Vec::new(),
            agglomerations: Vec::new(),
            // Brupass placeholder: 2.60 EUR, 60-min window (exercised by the Brupass
            // buy-back / activation unit tests below).
            brupass_cents: 260,
            brupass_validity_secs: 3600,
        }
    }

    #[test]
    fn stib_first_board_charges_and_activates() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX); // board 08:00
        assert_eq!(p.known_cents, 210);
        assert_eq!(p.stib_activation, 8 * 3600);
    }

    #[test]
    fn stib_board_within_window_is_free() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        // Re-board 60 min later (< 90 min): free, activation unchanged.
        m.charge_board(&mut p, stib(), 8 * 3600 + 60 * 60, &CTX);
        assert_eq!(p.known_cents, 210, "within-window re-board must be free");
        assert_eq!(p.stib_activation, 8 * 3600, "activation not reset within window");
    }

    #[test]
    fn stib_board_after_window_charges_and_resets() {
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX);
        // Re-board 91 min later (> 90 min): charged again, window reset.
        let t2 = 8 * 3600 + 91 * 60;
        m.charge_board(&mut p, stib(), t2, &CTX);
        assert_eq!(p.known_cents, 420, "after-window re-board charges a new ticket");
        assert_eq!(p.stib_activation, t2, "activation reset to the new boarding time");
    }

    #[test]
    fn stib_boundary_at_exactly_validity_is_charged() {
        // Boundary: at exactly `validity_secs` the ticket has expired (strict `<`).
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

    // --- De Lijn: time_window_flat, 300 c, 60-min window, 10-journey 220 c ---

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
        // With the 10-journey card in the profile, the per-journey `card_cents`
        // (220) replaces the single-ticket price (300).
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
        // A held STIB ticket does not make a De Lijn board free, and vice-versa:
        // they are distinct operators with distinct windows.
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, stib(), 8 * 3600, &CTX); // +210
        m.charge_board(&mut p, delijn(300), 8 * 3600 + 60, &CTX); // +300 (own window)
        assert_eq!(p.known_cents, 510, "STIB + De Lijn are charged separately");
        // Re-board STIB within its window: free.
        m.charge_board(&mut p, stib(), 8 * 3600 + 600, &CTX);
        assert_eq!(p.known_cents, 510, "STIB within its own window rides free");
        // Re-board De Lijn within its window: free.
        m.charge_board(&mut p, delijn(300), 8 * 3600 + 600, &CTX);
        assert_eq!(p.known_cents, 510, "De Lijn within its own window rides free");
    }

    // --- TEC: time_window_flat_tiered, classic vs express ---

    fn tec(is_express: bool) -> OperatorFareId {
        OperatorFareId::Modeled {
            model: OperatorModel::TimeWindowFlatTiered {
                is_express,
                // classic single 280 / express 550; 6-journey classic 223 / express 440;
                // reduced 6-journey classic 180 / express 352.
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
        // [ASSUMPTION spec A.2]: the single ticket has no reduced variant, so a
        // reduced passenger without the 6-journey card pays the full single fare.
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
        // TEC has no transfer window: two boardings are charged twice.
        let m = model();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, tec(false), 8 * 3600, &CTX);
        m.charge_board(&mut p, tec(false), 8 * 3600 + 300, &CTX);
        assert_eq!(p.known_cents, 560, "each TEC boarding is charged (no window)");
    }

    // --- SNCB: base + per-km with time/category/Train+ logic ---

    /// SNCB test tariff: the EXACT published 2026 2nd-class BRACKETED model —
    /// a = 0.168546 EUR/km (16.8546 c/km), b = 1.451226 EUR (145.1226 c),
    /// floor 2.6151 EUR (262 c), min 3 km, cap ≥116 km → 118 km. `sncb_tariff()`
    /// exposes it so the pure-function tests can call `fare_cents` directly.
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

    /// Weekday off-peak context (12:00): no discount, no cap.
    fn ctx_weekday_offpeak() -> FareContext {
        FareContext { profile: FareProfile::default(), weekday: 2 }
    }

    // --- DistanceTariff (bracketed model, the EXACT 2026 SNCB 2nd-class fare) ---

    /// The effective distance `d_eff` as computed by the tariff, exposed for the
    /// dedicated d_eff test via a fare-round-trip is awkward, so we re-derive it here
    /// by exercising `fare_cents` at distances that share a bracket midpoint.
    #[test]
    fn bracketed_d_eff_bracket_midpoints() {
        let t = sncb_tariff();
        // d < min_km bills as min_km (3). d=1,2 → d_eff=3 → same fare as d=3.
        assert_eq!(t.fare_cents(1.0), t.fare_cents(3.0), "d=1 → d_eff=3");
        assert_eq!(t.fare_cents(2.0), t.fare_cents(3.0), "d=2 → d_eff=3");
        // d=30 keeps its own value (below the bracketing region).
        // 31,32,33 all snap to the 3-km bracket midpoint 32 ⇒ equal fares.
        assert_eq!(t.fare_cents(31.0), t.fare_cents(32.0), "d=31 → d_eff=32");
        assert_eq!(t.fare_cents(33.0), t.fare_cents(32.0), "d=33 → d_eff=32");
        // 34 snaps to the next 3-km midpoint, 35.
        assert_eq!(t.fare_cents(34.0), t.fare_cents(35.0), "d=34 → d_eff=35");
        // 60 snaps to 59 (top of the 3-km region).
        assert_eq!(t.fare_cents(60.0), t.fare_cents(59.0), "d=60 → d_eff=59");
        // 61,65 snap to the first 5-km midpoint 63; 66 to 68.
        assert_eq!(t.fare_cents(61.0), t.fare_cents(63.0), "d=61 → d_eff=63");
        assert_eq!(t.fare_cents(65.0), t.fare_cents(63.0), "d=65 → d_eff=63");
        assert_eq!(t.fare_cents(66.0), t.fare_cents(68.0), "d=66 → d_eff=68");
        // 115 snaps to 113 (top of the 5-km region).
        assert_eq!(t.fare_cents(115.0), t.fare_cents(113.0), "d=115 → d_eff=113");
        // >= 116 caps at d_eff=118.
        assert_eq!(t.fare_cents(116.0), t.fare_cents(118.0), "d=116 → d_eff=118");
        assert_eq!(t.fare_cents(117.0), t.fare_cents(118.0), "d=117 → d_eff=118");
        assert_eq!(t.fare_cents(200.0), t.fare_cents(118.0), "d=200 → d_eff=118 (cap)");
        // Fractional rail-approximation distances round to the nearest whole km
        // (half up) BEFORE clamp/bracket. 23.2 → 23 (not off-grid 23.2); 23.5 → 24.
        assert_eq!(t.fare_cents(23.2), t.fare_cents(23.0), "23.2 → 23 (rounds down)");
        assert_eq!(t.fare_cents(23.5), t.fare_cents(24.0), "23.5 → 24 (half up)");
        // Near a bracket edge the rounding must not drop a bracket: 39.7 → 40
        // (bracket 40-42, midpoint 41), NOT floor 39 (bracket 37-39, midpoint 38).
        assert_eq!(t.fare_cents(39.7), t.fare_cents(41.0), "39.7 → 40 → midpoint 41");
        assert!(
            t.fare_cents(39.7) > t.fare_cents(39.0),
            "39.7 must not truncate down into the 37-39 bracket"
        );
    }

    #[test]
    fn bracketed_tariff_exact_sample_fares() {
        // The user-provided verified samples (raw = a*d_eff + b, floored, rounded to
        // 0.10 EUR half up). d_eff shown for reference.
        let t = sncb_tariff();
        // d_eff=3   → raw 1.9569 → floor 2.6151 → 2.6
        assert_eq!(t.fare_cents(3.0), 260, "d_eff=3 floored to 2.60");
        assert_eq!(t.fare_cents(1.0), 260, "d=1 → d_eff=3 → 2.60 (floor)");
        // d_eff=32  → raw 6.8447 → 6.8
        assert_eq!(t.fare_cents(32.0), 680, "d_eff=32 → 6.80");
        // d_eff=47  → raw 9.3729 → 9.4
        assert_eq!(t.fare_cents(47.0), 940, "d_eff=47 → 9.40");
        // d_eff=118 → raw 21.3397 → 21.3 (the ceiling)
        assert_eq!(t.fare_cents(118.0), 2130, "d_eff=118 → 21.30 (ceiling)");
        assert_eq!(t.fare_cents(200.0), 2130, "d=200 caps at the 21.30 ceiling");
    }

    // --- FIRST CLASS: 1st-class base off the UNROUNDED 2nd-class raw ---

    #[test]
    fn first_class_sample_fares_band_coeffs() {
        let t = sncb_tariff();
        // d_eff=3 (<=36) coeff 1.40. Raw (floored) = 262 c. 262*1.40 = 366.8 → grid
        // 0.10 half up → floor((366.8+5)/10)*10 = 370 c = 3.70 EUR.
        assert_eq!(t.fare_cents_class(3.0, TravelClass::First), 370, "d_eff=3 ×1.40 → 3.70");
        // d_eff=32 (<=36) coeff 1.40. Raw = 16.8546*32+145.1226 = 684.47 c. ×1.40 =
        // 958.26 → 0.10 grid → 960 c = 9.60 EUR.
        assert_eq!(t.fare_cents_class(32.0, TravelClass::First), 960, "d_eff=32 ×1.40 → 9.60");
        // d_eff=47 (37..51) coeff 1.50. Raw = 16.8546*47+145.1226 = 937.29 c. ×1.50 =
        // 1405.94 → 0.10 grid → 1410 c = 14.10 EUR.
        assert_eq!(t.fare_cents_class(47.0, TravelClass::First), 1410, "d_eff=47 ×1.50 → 14.10");
        // d_eff=118 (>51) coeff 1.60. Raw = 2133.97 c. ×1.60 = 3414.35 → tier [25,50]
        // grid 0.50 → floor((3414.35+25)/50)*50 = 3400 c = 34.00 EUR.
        assert_eq!(t.fare_cents_class(118.0, TravelClass::First), 3400, "d_eff=118 ×1.60 → 34.00 (0.50 grid)");
        // 1st class is always dearer than 2nd for the same distance.
        for km in [3.0, 20.0, 50.0, 100.0, 118.0] {
            assert!(
                t.fare_cents_class(km, TravelClass::First) > t.fare_cents(km),
                "1st class must exceed 2nd at {km} km"
            );
        }
    }

    #[test]
    fn first_class_rounding_tiers() {
        // round_first_class thresholds [2500, 5000] c, grids [10, 50, 100] c.
        let th = [2500u32, 5000u32];
        let gr = [10u32, 50u32, 100u32];
        // Below 25 EUR → 0.10 grid, half up. 366.8 → 370; 364.9 → 360.
        assert_eq!(DistanceTariff::round_first_class(366.8, &th, &gr), 370);
        assert_eq!(DistanceTariff::round_first_class(364.9, &th, &gr), 360);
        assert_eq!(DistanceTariff::round_first_class(365.0, &th, &gr), 370, "x.x5 half up");
        // In [25, 50] EUR → 0.50 grid. 2760 → floor((2760+25)/50)*50 = 2750; 2775 → 2800.
        assert_eq!(DistanceTariff::round_first_class(2760.0, &th, &gr), 2750);
        assert_eq!(DistanceTariff::round_first_class(2775.0, &th, &gr), 2800, "0.25 midpoint half up");
        // Above 50 EUR → 1 EUR grid. 5149 → 5100; 5150 → 5200 (half up).
        assert_eq!(DistanceTariff::round_first_class(5149.0, &th, &gr), 5100);
        assert_eq!(DistanceTariff::round_first_class(5150.0, &th, &gr), 5200, "0.50 midpoint half up");
        // Tier boundary: exactly 25.00 EUR uses the [25,50] (0.50) grid.
        assert_eq!(DistanceTariff::round_first_class(2500.0, &th, &gr), 2500);
    }

    #[test]
    fn first_class_above_50_euros_rounds_to_1_euro() {
        // A synthetic large-fare tariff so the >50 EUR (1 EUR grid) tier is exercised
        // with a real fare_cents_class call. slope 500 c/km, so d_eff=118 raw is huge.
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
        // d_eff=118 (>51) coeff 1.60. Raw = 500*118 = 59000 c. ×1.60 = 94400 → tier
        // >50 EUR → 1 EUR grid → 94400 c = 944.00 EUR (already on the grid).
        assert_eq!(t.fare_cents_class(118.0, TravelClass::First), 94400, ">50 EUR → 1 EUR grid");
        // A non-grid value rounds to the whole euro, half up.
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
        // raw = 500*118 + 23 = 59023 c. ×1.60 = 94436.8 → 1 EUR grid half up → 94400.
        assert_eq!(t2.fare_cents_class(118.0, TravelClass::First), 94400, "94436.8 → 944 EUR");
    }

    #[test]
    fn first_class_composes_with_weekend_discount() {
        // 1st-class base then the SAME 2nd-class discount pipeline (weekend −30% adult).
        let m = model();
        let ctx = FareContext {
            profile: FareProfile {
                travel_class: TravelClass::First,
                ..FareProfile::default()
            },
            weekday: 5, // Saturday
        };
        let mut p = PriceValue::ZERO;
        // Board floor 262 ×0.70 = 183 (same as 2nd class — floor is the down-payment).
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.known_cents, 183, "weekend board floor −30% (down-payment, class-agnostic)");
        // 40 km ride: 1st-class total at d_eff=41 (37..51 → ×1.50). Raw =
        // 16.8546*41+145.1226 = 836.16 c. ×1.50 = 1254.24 → 0.10 grid → 1250 c.
        // Weekend ×0.70 → 875 c.
        m.accrue_sncb_km(&mut p, sncb_tariff(), 40_000.0, &sncb_rules(), &ctx, 12 * 3600);
        let first_full = sncb_tariff().fare_cents_class(41.0, TravelClass::First);
        assert_eq!(first_full, 1250, "1st-class 40 km base = 12.50");
        assert_eq!(p.known_cents, (1250f64 * 0.70).round() as u32, "weekend −30% on 1st-class total");
    }

    #[test]
    fn second_class_default_unchanged_by_travel_class_field() {
        // TravelClass::Second (the default) reproduces the exact 2nd-class fares.
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
        // Short trips floor at 2.60. d_eff=3 raw 1.9569 < floor 2.6151 → 2.60.
        let t = sncb_tariff();
        assert_eq!(t.fare_cents(3.0), 260, "short-trip raw < floor → floored to 2.60");
    }

    #[test]
    fn bracketed_tariff_monotonic_nondecreasing_to_cap() {
        // The base fare never decreases as distance grows, up to the 118 km cap.
        let t = sncb_tariff();
        let mut prev = 0u32;
        for km in 1..=130 {
            let f = t.fare_cents(km as f64);
            assert!(f >= prev, "fare must be non-decreasing at {km} km ({f} < {prev})");
            prev = f;
        }
        // At/beyond the cap it is flat (all clamp to d_eff=118).
        assert_eq!(t.fare_cents(116.0), t.fare_cents(500.0));
    }

    #[test]
    fn band_and_linear_alternatives_still_available() {
        // The legacy band + inert linear shapes remain constructible/inert.
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
        // Weekday off-peak, adult, no card: full base.
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx_weekday_offpeak());
        assert_eq!(p.known_cents, 262, "first SNCB board charges base floor (2.6151 → 262)");
        assert!(p.sncb_active, "SNCB run active after first board");
    }

    #[test]
    fn sncb_per_km_accrues_over_a_ride_weekday() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // floor 262 at board
        // 40 km ride: d_eff=41 → raw 8.3624 → 8.4 EUR = 840 c total.
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
        // Sat, adult, no card: −30% on the whole fare. floor 262 → 183 at board;
        // the 40 km fare 840 scaled ×0.70 = 588.
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
        // Train+, weekday off-peak: −40% all categories, no cap.
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
        // Train+, weekday 08:00 (peak): full raw fare, but per-journey cap 1400 adult.
        let m = model();
        let ctx = FareContext {
            profile: FareProfile { sncb_train_plus: true, ..FareProfile::default() },
            weekday: 2,
        };
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 8 * 3600, &ctx); // floor 262 (full)
        assert_eq!(p.known_cents, 262, "Train+ peak base is full price");
        assert_eq!(p.sncb_cap_cents, 1400, "Train+ peak sets the adult per-journey cap");
        // Long ride: 100 km → d_eff=98 → 18.00 EUR = 1800 c, ABOVE the 14.00 cap.
        m.accrue_sncb_km(&mut p, sncb_tariff(), 100_000.0, &sncb_rules(), &ctx, 8 * 3600);
        assert_eq!(p.known_cents, 1800, "raw spend carried uncapped in known_cents");
        assert_eq!(p.sncb_spend_cents, 1800, "raw SNCB spend tracked for the cap");
        // Display cap binds: min(2760, 1400) applied to the SNCB portion.
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
        m.charge_board(&mut p, sncb(), 17 * 3600, &ctx); // 17:00 peak
        assert_eq!(p.sncb_cap_cents, 550, "reduced Train+ peak cap = 5.50");
    }

    #[test]
    fn sncb_airport_od_overrides_base_formula() {
        // Airport OD fare (790) replaces base+per-km, regardless of the km accrued.
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // floor 262
        m.accrue_sncb_km(&mut p, sncb_tariff(), 12_000.0, &sncb_rules(), &ctx, 12 * 3600); // 12 km → 350
        m.apply_sncb_airport_od(&mut p, 790);
        assert_eq!(p.known_cents, 790, "airport OD replaces the base+per-km fare");
        assert_eq!(p.sncb_spend_cents, 790);
    }

    #[test]
    fn sncb_airport_od_terminates_the_run() {
        // The flat airport ticket closes the SNCB run: a subsequent contiguous rail
        // ride charges a fresh base (not per-km on top of 790, nor a reused credit).
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // base 262, active
        m.apply_sncb_airport_od(&mut p, 790); // flat 790, run closed
        assert_eq!(p.known_cents, 790);
        assert!(!p.sncb_active, "airport OD ends the SNCB run");
        assert_eq!(p.sncb_base_credit, 0, "airport OD clears the base credit");
        // A new contiguous SNCB board charges base again (a fresh ticket).
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx);
        assert_eq!(p.known_cents, 790 + 262, "post-airport rail is a new ticket");
        assert!(p.sncb_active);
    }

    #[test]
    fn sncb_second_train_same_run_does_not_recharge_base() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // floor 262, active
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600); // run 20 km -> 480
        // Change to a second SNCB train on the same journey: still active, no base.
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1800, &ctx);
        assert_eq!(p.known_cents, 480, "contiguous SNCB change re-charges no base");
        assert!(p.sncb_active);
        // `accrue_sncb_km` takes the WHOLE-run distance, so the second train passes the
        // cumulative 30 km run total (20 + 10). The fare is recomputed for the full run.
        m.accrue_sncb_km(&mut p, sncb_tariff(), 30_000.0, &sncb_rules(), &ctx, 12 * 3600 + 1800); // run 30 km -> 650
        assert_eq!(p.known_cents, 650, "30 km fare: d_eff=30 → 6.50 EUR");
    }

    #[test]
    fn accrue_sncb_km_is_run_total_and_backs_out_prior() {
        // `accrue_sncb_km` charges the WHOLE-run distance and backs out the per-km
        // already charged for the run, so recomputing with a LARGER run total adds
        // only the delta, and a SMALLER total refunds.
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // floor 262
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600); // run 20 km -> 480
        assert_eq!(p.known_cents, 480, "20 km fare: d_eff=20 → 4.80 EUR");
        assert_eq!(p.sncb_run_perkm_cents, 480 - 262, "accrued = fare − floor");
        // Recompute the SAME run at 30 km: adds only the delta up to the 30 km fare.
        m.accrue_sncb_km(&mut p, sncb_tariff(), 30_000.0, &sncb_rules(), &ctx, 12 * 3600); // run 30 km -> 650
        assert_eq!(p.known_cents, 650, "delta-charged to the 30 km fare = 650");
        assert_eq!(p.sncb_run_perkm_cents, 650 - 262);
        // Recompute the run SHORTER (e.g. an intra-zone alight after out-of-zone):
        // refunds down to the smaller total. 10 km fare = d_eff=10 → 3.10 EUR.
        m.accrue_sncb_km(&mut p, sncb_tariff(), 10_000.0, &sncb_rules(), &ctx, 12 * 3600); // run 10 km -> 310
        assert_eq!(p.known_cents, 310, "refunds to the 10 km fare (3.10 EUR)");
        assert_eq!(p.sncb_spend_cents, 310, "raw spend tracks the run total too");
    }

    #[test]
    fn end_sncb_run_clears_run_tracking() {
        // A non-SNCB board ends the run and clears the zone-to-zone run tracking, so a
        // later SNCB re-entry starts a fresh run (its own base + its own run distance).
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // floor 260
        p.sncb_run_board_stop = 7; // pretend a run board stop was set at the accrual site
        m.accrue_sncb_km(&mut p, sncb_tariff(), 20_000.0, &sncb_rules(), &ctx, 12 * 3600); // run 20 km
        // A bus board ends the run: run tracking cleared.
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
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // base 262, active
        // Board an unmodeled bus: ends the SNCB run.
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 0 }, 12 * 3600 + 600, &ctx);
        assert!(!p.sncb_active, "unmodeled board ends the SNCB run");
        // Re-enter SNCB: a new ticket, base charged again.
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx);
        assert_eq!(p.known_cents, 524, "SNCB re-entry after a bus charges base twice");
        assert!(p.sncb_active);
    }

    #[test]
    fn stib_board_ends_sncb_run() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // base 262, active
        m.charge_board(&mut p, stib(), 12 * 3600 + 600, &ctx); // +210, ends SNCB run
        assert!(!p.sncb_active, "STIB board ends the SNCB run");
        m.charge_board(&mut p, sncb(), 12 * 3600 + 1200, &ctx); // re-entry: +262
        assert_eq!(p.known_cents, 262 + 210 + 262);
    }

    #[test]
    fn accrue_sncb_km_zero_distance_is_noop() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx); // 262
        m.accrue_sncb_km(&mut p, sncb_tariff(), 0.0, &sncb_rules(), &ctx, 12 * 3600);
        assert_eq!(p.known_cents, 262, "zero-km segment stays at the floor 262");
    }

    // --- Dominance: the three worked examples from spec §2.3 (letec = unknown slot 0) ---

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

    /// Display-time capped cents: `known − sncb_spend + min(sncb_spend, cap)`.
    /// Mirrors `plan_price_of` so the cap tests exercise the same formula.
    fn capped_cents(p: &PriceValue) -> u32 {
        let sncb = p.sncb_spend_cents.min(p.sncb_cap_cents);
        p.known_cents - p.sncb_spend_cents + sncb
    }

    #[test]
    fn stib_only_dominates_stib_plus_letec() {
        // Equal euros, 0 <= 1 on letec: the pricier-because-unpriceable plan loses.
        assert!(stib_only().dominates(&stib_plus_letec()));
        assert!(!stib_plus_letec().dominates(&stib_only()));
    }

    #[test]
    fn letec_only_incomparable_with_stib_only() {
        // 0.00 <= 2.10 but 1 > 0 on letec one way; 2.10 > 0.00 the other. Both kept.
        assert!(!letec_only().dominates(&stib_only()));
        assert!(!stib_only().dominates(&letec_only()));
    }

    #[test]
    fn letec_only_dominates_stib_plus_letec() {
        // 0.00 <= 2.10 and 1 <= 1.
        assert!(letec_only().dominates(&stib_plus_letec()));
        assert!(!stib_plus_letec().dominates(&letec_only()));
    }

    #[test]
    fn cheaper_sncb_not_dominated_by_pricier() {
        // Two SNCB-only plans: a short cheap ride vs a long pricey one. The cheaper
        // must not be dominated by the pricier (price is a real dominance axis), and
        // the pricier plan does not dominate the cheaper on price.
        let cheap = PriceValue { known_cents: 470, sncb_active: true, sncb_base_credit: 250, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 690, sncb_active: true, sncb_base_credit: 250, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "cheaper SNCB dominates pricier on price");
        assert!(!pricey.dominates(&cheap), "pricier SNCB does not dominate cheaper");
    }

    // --- Defect A: carried-credit buy-back dominance (spec regression) ---

    #[test]
    fn sncb_prepaid_not_dominated_by_cheaper_stib_to_same_hub() {
        // Worked failing example from the task. Both labels sit at a shared hub H
        // and both then continue SNCB rail H->D (10 km, per_km 11 c/km = 110 c, no
        // second base). SNCB base 250, STIB ticket 210, per_km over 10 km access:
        //   A: SNCB access 10 km  -> known 360, sncb_active, base credit 250.
        //   B: STIB to hub        -> known 210, no SNCB credit, stib credit 210.
        // Continuations: A finishes 360+110 = 470; B finishes 210+250+110 = 570.
        // The engine must NOT let B prune A. Before the fix B dominated A (210<=360).
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
        // B holds an SNCB base credit? No -> B lacks it -> B pays 250 to buy it back:
        // 210 + 250 = 460 > 360 -> B does NOT dominate A. (Fixed.)
        assert!(
            !stib_to_hub.dominates(&sncb_prepaid),
            "cheaper STIB-to-hub label must not prune the SNCB-prepaid label"
        );
        // And A does not dominate B either (360 > 210, and A lacks B's STIB credit).
        assert!(
            !sncb_prepaid.dominates(&stib_to_hub),
            "SNCB-prepaid does not dominate the STIB-to-hub label either"
        );
    }

    #[test]
    fn stib_held_ticket_not_dominated_by_cheaper_creditless() {
        // Analogous STIB within-window case: a label holding a valid STIB ticket
        // (credit 210) is cheaper in a STIB-continuing future than a creditless
        // label, even if the creditless label has a lower `known_cents` now.
        let stib_held = PriceValue {
            known_cents: 210,
            stib_activation: 8 * 3600,
            stib_ticket_credit: 210,
            ..PriceValue::ZERO
        };
        let creditless_cheaper = PriceValue {
            known_cents: 50, // e.g. a cheaper partial path with no STIB ticket held
            ..PriceValue::ZERO
        };
        // Creditless must buy back the 210 STIB credit it lacks: 50 + 210 = 260 > 210
        // -> it does NOT dominate the ticket-holder. (Fixed.)
        assert!(
            !creditless_cheaper.dominates(&stib_held),
            "cheaper creditless label must not prune a held-STIB-ticket label"
        );
    }

    #[test]
    fn same_credits_dominance_reduces_to_known_cents() {
        // When both labels carry the same credit, penalty is 0 both ways, so
        // dominance is decided purely on known_cents + unknowns, as before.
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
        // Fares-off invariant: with both credit fields 0 (every label ZERO on the
        // disabled path), penalty is always 0 and dominance is the pre-feature
        // componentwise test. This guards the disabled-path byte-identity.
        let a = PriceValue::ZERO;
        let b = PriceValue::ZERO;
        assert!(a.dominates(&b) && b.dominates(&a), "ZERO dominates ZERO both ways");
    }

    #[test]
    fn charge_board_sets_and_clears_credits() {
        let m = model();
        let ctx = ctx_weekday_offpeak();
        let mut p = PriceValue::ZERO;
        // SNCB board: base credit set, run active.
        m.charge_board(&mut p, sncb(), 12 * 3600, &ctx);
        assert_eq!(p.sncb_base_credit, 262, "SNCB board records base credit");
        assert_eq!(p.stib_ticket_credit, 0);
        // STIB board: STIB credit set; SNCB run and its credit cleared.
        m.charge_board(&mut p, stib(), 12 * 3600 + 600, &ctx);
        assert_eq!(p.stib_ticket_credit, 210, "STIB board records ticket credit");
        assert_eq!(p.sncb_base_credit, 0, "STIB board clears the SNCB base credit");
        // Unmodeled board: SNCB credit stays 0, STIB credit survives (ticket held).
        m.charge_board(&mut p, OperatorFareId::Unknown { slot: 0 }, 13 * 3600, &ctx);
        assert_eq!(p.stib_ticket_credit, 210, "held STIB ticket survives an unmodeled board");
        assert_eq!(p.sncb_base_credit, 0);
    }

    #[test]
    fn cheaper_profile_priced_plan_not_dominated_by_pricier() {
        // Dominance-soundness across profile pricing: a plan priced cheap under the
        // fixed query profile (e.g. a subscription/discount) must not be pruned by a
        // pricier plan. Since the profile is FIXED per query, each boarding's fare is
        // deterministic, so price stays additive and this reduces to `known_cents`.
        let cheap = PriceValue { known_cents: 300, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 560, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "cheaper profile-priced plan dominates pricier");
        assert!(!pricey.dominates(&cheap), "pricier does not prune the cheaper plan");
    }

    #[test]
    fn train_plus_peak_cap_not_used_in_dominance() {
        // Soundness of the display cap: two Train+ peak labels with different raw
        // SNCB spend but the SAME cap. Dominance compares RAW `known_cents`, never
        // the capped value, so the lower-raw label dominates even though both would
        // display the same capped price. (A cheaper raw plan is never discarded.)
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
        // Both cap to 1400 at display, but dominance kept the lower-raw label.
        assert_eq!(capped_cents(&low_raw), 1400);
        assert_eq!(capped_cents(&high_raw), 1400);
    }

    #[test]
    fn delijn_credit_buy_back_dominance() {
        // A held De Lijn ticket (credit 300) is cheaper in a De Lijn-continuing
        // future than a creditless label, so a cheaper-now creditless label must not
        // prune it (mirrors the STIB/SNCB buy-back rule).
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

    // --- Brupass (Appendix A.3): activation, coverage window, buy-back dominance ---

    #[test]
    fn brupass_activation_charges_once_and_sets_window() {
        // Activating a Brupass charges its price once and anchors the window.
        let m = model(); // brupass_cents 260, validity 3600
        let mut p = PriceValue::ZERO;
        m.activate_brupass(&mut p, 8 * 3600);
        assert_eq!(p.known_cents, 260, "Brupass price charged once on activation");
        assert_eq!(p.brupass_credit, 260, "reusable Brupass credit recorded");
        assert_eq!(p.brupass_activation, 8 * 3600, "coverage window anchored at board");
    }

    #[test]
    fn brupass_active_within_window_only() {
        let m = model(); // validity 3600 (60 min)
        let mut p = PriceValue::ZERO;
        m.activate_brupass(&mut p, 8 * 3600);
        assert!(m.brupass_active(&p, 8 * 3600), "active at activation time");
        assert!(m.brupass_active(&p, 8 * 3600 + 59 * 60), "active within 60 min");
        assert!(!m.brupass_active(&p, 8 * 3600 + 3600), "expired at exactly validity");
        assert!(!m.brupass_active(&p, 8 * 3600 + 3601), "expired past window");
        // No Brupass ever bought: never active.
        assert!(!m.brupass_active(&PriceValue::ZERO, 8 * 3600));
    }

    #[test]
    fn brupass_activation_ends_sncb_run() {
        // A Brupass covers in-zone rail, so activating it ends any contiguous SNCB
        // run (its base credit is no longer the paying instrument in-zone).
        let m = model();
        let mut p = PriceValue { sncb_active: true, sncb_base_credit: 260, ..PriceValue::ZERO };
        m.activate_brupass(&mut p, 8 * 3600);
        assert!(!p.sncb_active, "Brupass activation ends the SNCB run");
        assert_eq!(p.sncb_base_credit, 0, "SNCB base credit cleared");
    }

    #[test]
    fn brupass_held_not_dominated_by_cheaper_creditless() {
        // Soundness (the hub analog): a label that has bought a Brupass (credit 260)
        // is cheaper in an in-zone-continuing future than a creditless label, so a
        // cheaper-now creditless label must NOT prune it. Buy-back: the creditless
        // label must pay 260 to buy back the Brupass credit it lacks.
        //   A: one STIB ticket in-zone, no Brupass  -> known 240, no brupass credit.
        //   B: bought a Brupass covering in-zone     -> known 260, brupass credit 260.
        // A future with a SECOND in-zone operator: A pays another ticket, B rides
        // free. Neither may prune the other; both survive so the cheaper continuation
        // wins per Pareto.
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
        // STIB-ticket label lacks the Brupass credit: 240 + 260 = 500 > 260 ⇒ it does
        // NOT dominate the Brupass-held label.
        assert!(
            !stib_ticket.dominates(&brupass_held),
            "cheaper single-ticket label must not prune the Brupass-held label"
        );
        // And the Brupass label lacks the STIB credit: 260 + 240 = 500 > 240 ⇒ it does
        // NOT dominate the single-ticket label either. Both kept ⇒ pick-cheaper holds.
        assert!(
            !brupass_held.dominates(&stib_ticket),
            "Brupass-held label must not prune the cheaper single-ticket label"
        );
    }

    #[test]
    fn brupass_credit_absent_is_pure_known_dominance() {
        // With neither label holding a Brupass credit, the new penalty term is 0 and
        // dominance is the plain known_cents test (guards the disabled/no-brupass
        // path: brupass_credit is always 0 ⇒ zero penalty contribution).
        let cheap = PriceValue { known_cents: 240, ..PriceValue::ZERO };
        let pricey = PriceValue { known_cents: 500, ..PriceValue::ZERO };
        assert!(cheap.dominates(&pricey), "no Brupass credit ⇒ plain known dominance");
        assert!(!pricey.dominates(&cheap));
    }

    #[test]
    fn known_cents_epsilon_bucketing() {
        // a = 10 cents, no relative term: 0..9 -> 0, 10..19 -> 10, 205..214 -> 200.
        let e = KnownEurosEpsilon { a: 10.0, b: 0.0 };
        assert_eq!(e.bucket(0), 0);
        assert_eq!(e.bucket(9), 0);
        assert_eq!(e.bucket(10), 10);
        assert_eq!(e.bucket(19), 10);
        assert_eq!(e.bucket(210), 210);
        assert_eq!(e.bucket(214), 210);
        // a = 0 disables bucketing (identity).
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
