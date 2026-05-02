use async_graphql::SimpleObject;

use crate::structures::plan::PlanLeg;

#[derive(Debug, Clone, SimpleObject)]
pub struct ArrivalScenario {
    /// Arrival time at destination (seconds since midnight)
    pub time: u32,
    /// Probability of this scenario (individual, not cumulative; sum = 1.0)
    pub probability: f32,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct Plan {
    pub legs: Vec<PlanLeg>,
    pub start: u32,
    pub end: u32,
    /// Possible arrival times and their probabilities, sorted earliest first.
    /// Single entry with probability 1.0 for deterministic routes.
    pub arrival_distribution: Vec<ArrivalScenario>,
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
    /// Dropped by the backward-detour filter (leg moves away from destination).
    BackwardDetour,
    /// Dominated in (departure↑, arrival↓, transfers↓) by another plan.
    /// `dominator_index` is the position of the dominator in `ExplainResult::candidates`.
    ParetoDominated { dominator_index: usize },
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
    /// How many times the walk radius doubled before a result was found.
    pub access_attempts: u32,
    /// `true` when transit routing failed and a walk-only plan was returned.
    pub fell_back_to_walk_only: bool,
}

#[derive(Debug, Clone)]
pub struct ExplainResult {
    pub plans: Vec<Plan>,
    pub candidates: Vec<PlanCandidate>,
    pub access: AccessInfo,
}
