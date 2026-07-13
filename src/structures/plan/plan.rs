use async_graphql::SimpleObject;

use crate::structures::Mode;
use crate::structures::plan::{PlanCoordinate, PlanLeg};

#[derive(Debug, Clone, SimpleObject)]
pub struct ArrivalScenario {
    /// Arrival time at destination (seconds since midnight)
    pub time: u32,
    /// Probability of this scenario (individual, not cumulative; sum = 1.0)
    pub probability: f32,
}

/// A same-transit-core variant of a plan that differs only in how the street
/// (access/egress) portions are traversed — e.g. bike to the station instead
/// of walking, departing later for the same trips.
#[derive(Debug, Clone, SimpleObject)]
pub struct AccessAlternative {
    pub mode: Mode,
    pub start: u32,
    pub end: u32,
    pub expected_end: u32,
    /// Total street (non-transit) seconds of the variant.
    pub street_secs: u32,
}

/// Transit price carried by a plan (present only when the fares feature is on).
///
/// `known_euros` is the accumulated modeled spend (STIB / De Lijn / TEC / SNCB)
/// under the query's fare profile. `capped_euros` applies the display-time SNCB
/// per-journey cap (Train+ peak) as `known − sncb_spend + min(sncb_spend, cap)`;
/// it equals `known_euros` when no cap is in force (spec §9 — the cap is carried
/// as raw spend in dominance, applied only at display). `unknown_operators` names
/// the unmodeled operators the plan boards, each contributing an incomparable
/// price token so the plan is never dominated on price by a modeled-only plan.
#[derive(Debug, Clone, SimpleObject)]
pub struct PlanPrice {
    pub known_euros: f64,
    pub capped_euros: f64,
    pub unknown_operators: Vec<String>,
    /// SNCB tariff distance (km) of the last contiguous rail run in the plan, the
    /// zone-collapsed distance that feeds the SNCB fare formula. Exposed for fare
    /// calibration against SNCB's official tariff-distance table. `None` when the
    /// plan has no SNCB run.
    pub sncb_fare_km: Option<f64>,
    /// Itemized "shopping list" of the tickets/products the journey buys: one item
    /// per chargeable ticket (consecutive same-ticket boardings grouped: a STIB
    /// 90-min window is ONE item across transfers; a contiguous SNCB run is ONE
    /// item; a Brupass covering several in-zone legs is ONE item). Subscription- or
    /// pass-covered legs appear as a €0.00 item with `coverage` set. The sum of item
    /// `euros` (with caps applied) equals `capped_euros`.
    pub breakdown: Vec<FareBreakdownItem>,
}

/// One line of the fare breakdown (a single ticket or product the traveller buys,
/// or a covered leg). `euros` is what it costs (0.00 when covered); `coverage` is
/// `None` when paid, else the reason it is free (e.g. a subscription or Brupass).
#[derive(Debug, Clone, SimpleObject)]
pub struct FareBreakdownItem {
    /// Operator/agency the ticket is for (e.g. "STIB", "SNCB", "De Lijn", "TEC",
    /// or "Brupass" for the multi-operator pass).
    pub operator: String,
    /// The ticket/product bought, e.g. "STIB single (90 min)",
    /// "SNCB 2nd class Brussels->Antwerpen", "De Lijn 10-journey card",
    /// "TEC classic 6-journey", "Brupass (Brussels)".
    pub description: String,
    /// What this item costs in euros (0.00 when covered by a pass/subscription).
    pub euros: f64,
    /// `None` when paid; else the reason it is free/covered, e.g.
    /// "De Lijn subscription", "Brupass", "SNCB subscription".
    pub coverage: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Plan {
    pub legs: Vec<PlanLeg>,
    pub start: u32,
    pub end: u32,
    /// The travel mode that produced this plan.
    pub mode: Mode,
    /// Same-journey variants differing only in street-mode access/egress.
    pub access_alternatives: Vec<AccessAlternative>,
    /// Possible arrival times and their probabilities, sorted earliest first.
    /// Single entry with probability 1.0 for deterministic routes.
    pub arrival_distribution: Vec<ArrivalScenario>,
    /// Probability-weighted expected arrival time (seconds since midnight).
    /// Equals `end` for deterministic routes; higher than `end` when transfers
    /// carry risk (infeasible transfer → high-delay scenario shifts expectation up).
    pub expected_end: u32,
    /// Transit price of this plan. `None` when the fares feature is disabled;
    /// `Some` (computed post-hoc from the plan's boardings) when enabled.
    pub price: Option<PlanPrice>,
}

// ---------------------------------------------------------------------------
// Debug types — used by raptorExplain GraphQL query
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum CandidateStatus {
    /// Plan survived all filters and appears in the final result.
    Kept,
    /// This RAPTOR round produced no arrival improvement (pareto guard skipped it).
    NotImproving,
    /// `reconstruct()` returned zero legs for this round.
    ReconstructionEmpty,
    /// Dropped by the extreme-risk filter (only when at least one safe plan exists).
    ExtremeRisk,
    /// Dominated in (departure↑, arrival↓, transfers↓, reliability↓) by another plan.
    /// `dominator_index` is the position of the dominator in `ExplainResult::candidates`.
    /// The flags record *which* dimensions the dominator wins on.
    ParetoDominated {
        dominator_index: usize,
        departure_worse: bool,
        arrival_worse: bool,
        transfers_worse: bool,
        reliability_worse: bool,
    },
}

#[derive(Debug, Clone)]
pub struct PlanCandidate {
    /// RAPTOR round (0 = walk-only reach, 1 = one transit leg, …).
    pub round: usize,
    /// Origin departure time of the RAPTOR pass that produced this candidate.
    pub origin_departure: u32,
    /// `None` for `NotImproving` and `ReconstructionEmpty` (no Plan is assembled).
    pub plan: Option<Plan>,
    pub status: CandidateStatus,
}

#[derive(Debug, Clone)]
pub struct AccessInfo {
    pub walk_radius_secs: u32,
    pub walk_radius_meters: u32,
    pub origin_stops_found: u32,
    pub destination_stops_found: u32,
    /// How many extra access passes ran beyond the initial near-stop pass
    /// (0 = the near-radius Pass A alone; 1 = the admissible-radius Pass B also ran).
    pub access_attempts: u32,
    /// `true` when transit routing failed and a walk-only plan was returned.
    pub fell_back_to_walk_only: bool,
}

/// One leg in the path that led RAPTOR to a particular stop.
/// Segments are ordered origin → destination.
#[derive(Debug, Clone)]
pub struct StopPathLeg {
    /// `true` = transit leg on a scheduled route, `false` = walk.
    pub is_transit: bool,
    /// Route short name for transit legs; empty string for walk legs.
    pub route_label: String,
    /// Waypoints along the leg (boarding stop → intermediate stops → alighting stop,
    /// or just origin/destination for walk legs).
    pub geometry: Vec<PlanCoordinate>,
}

/// One transit stop reached during RAPTOR exploration.
/// `round = 0` means the stop was seeded as an access/egress stop (walk reach).
/// Only the lowest-round entry for each stop is included.
#[derive(Debug, Clone)]
pub struct StopReach {
    pub stop_idx: u32,
    pub round: u8,
    pub arrival_secs: u32,
    pub lat: f64,
    pub lon: f64,
    pub name: String,
    /// Ordered sequence of legs (origin → this stop) that RAPTOR followed to reach it.
    pub path: Vec<StopPathLeg>,
}

#[derive(Debug, Clone)]
pub struct ExplainResult {
    pub plans: Vec<Plan>,
    pub candidates: Vec<PlanCandidate>,
    pub access: AccessInfo,
    pub stops_reached: Vec<StopReach>,
    pub origin: PlanCoordinate,
    pub destination: PlanCoordinate,
}
