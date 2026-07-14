use async_graphql::SimpleObject;

use crate::structures::Mode;
use crate::structures::plan::{PlanCoordinate, PlanLeg};

#[derive(Debug, Clone, SimpleObject)]
pub struct ArrivalScenario {
    pub time: u32,
    /// Individual, not cumulative; sum = 1.0.
    pub probability: f32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct AccessAlternative {
    pub mode: Mode,
    pub start: u32,
    pub end: u32,
    pub expected_end: u32,
    pub street_secs: u32,
}

/// Transit price of a plan (present only when fares are on).
///
/// `capped_euros = known − sncb_spend + min(sncb_spend, cap)`, applying the SNCB
/// per-journey cap at display only (the cap is carried as raw spend in dominance);
/// equals `known_euros` when no cap is in force. `unknown_operators` each contribute
/// an incomparable price token so the plan is never price-dominated by a modeled-only
/// plan.
#[derive(Debug, Clone, SimpleObject)]
pub struct PlanPrice {
    pub known_euros: f64,
    pub capped_euros: f64,
    pub unknown_operators: Vec<String>,
    /// SNCB zone-collapsed tariff distance (km) of the last contiguous rail run.
    /// `None` when the plan has no SNCB run.
    pub sncb_fare_km: Option<f64>,
    /// One item per chargeable ticket (consecutive same-ticket boardings grouped;
    /// covered legs are €0.00 with `coverage` set). Sum of item `euros` (caps
    /// applied) equals `capped_euros`.
    pub breakdown: Vec<FareBreakdownItem>,
}

/// One fare-breakdown line. `euros` is 0.00 when covered; `coverage` is `None` when
/// paid, else the reason it is free.
#[derive(Debug, Clone, SimpleObject)]
pub struct FareBreakdownItem {
    pub operator: String,
    pub description: String,
    pub euros: f64,
    pub coverage: Option<String>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Plan {
    pub legs: Vec<PlanLeg>,
    pub start: u32,
    pub end: u32,
    pub mode: Mode,
    /// Same-journey variants differing only in street-mode access/egress.
    pub access_alternatives: Vec<AccessAlternative>,
    /// Arrival times and probabilities, earliest first; single 1.0 entry when deterministic.
    pub arrival_distribution: Vec<ArrivalScenario>,
    /// Probability-weighted expected arrival (secs since midnight); `end` when
    /// deterministic, higher when transfers carry risk.
    pub expected_end: u32,
    /// `None` when fares disabled; `Some` (post-hoc from boardings) when enabled.
    pub price: Option<PlanPrice>,
}

// Debug types used by the raptorExplain GraphQL query.
#[derive(Debug, Clone)]
pub enum CandidateStatus {
    Kept,
    NotImproving,
    /// `reconstruct()` returned zero legs for this round.
    ReconstructionEmpty,
    /// Dropped by the extreme-risk filter (only when at least one safe plan exists).
    ExtremeRisk,
    /// Dominated in (departure↑, arrival↓, transfers↓, reliability↓). `dominator_index`
    /// indexes `ExplainResult::candidates`; flags record which dimensions it wins on.
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
    pub origin_departure: u32,
    /// `None` for `NotImproving` and `ReconstructionEmpty` (no Plan assembled).
    pub plan: Option<Plan>,
    pub status: CandidateStatus,
}

#[derive(Debug, Clone)]
pub struct AccessInfo {
    pub walk_radius_secs: u32,
    pub walk_radius_meters: u32,
    pub origin_stops_found: u32,
    pub destination_stops_found: u32,
    /// Extra access passes beyond Pass A (0 = Pass A alone; 1 = Pass B also ran).
    pub access_attempts: u32,
    /// `true` when transit routing failed and a walk-only plan was returned.
    pub fell_back_to_walk_only: bool,
}

#[derive(Debug, Clone)]
pub struct StopPathLeg {
    /// `true` = transit leg, `false` = walk.
    pub is_transit: bool,
    /// Route short name for transit legs; empty for walk.
    pub route_label: String,
    pub geometry: Vec<PlanCoordinate>,
}

/// One transit stop reached during RAPTOR exploration. `round = 0` = seeded as an
/// access/egress stop. Only the lowest-round entry per stop is included.
#[derive(Debug, Clone)]
pub struct StopReach {
    pub stop_idx: u32,
    pub round: u8,
    pub arrival_secs: u32,
    pub lat: f64,
    pub lon: f64,
    pub name: String,
    /// Legs (origin → this stop) RAPTOR followed to reach it.
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
