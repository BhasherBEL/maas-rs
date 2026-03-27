use async_graphql::SimpleObject;

use crate::structures::plan::PlanLeg;

#[derive(Debug, SimpleObject)]
pub struct ArrivalScenario {
    /// Arrival time at destination (seconds since midnight)
    pub time: u32,
    /// Probability of this scenario (individual, not cumulative; sum = 1.0)
    pub probability: f32,
}

#[derive(Debug, SimpleObject)]
pub struct Plan {
    pub legs: Vec<PlanLeg>,
    pub start: u32,
    pub end: u32,
    /// Possible arrival times and their probabilities, sorted earliest first.
    /// Single entry with probability 1.0 for deterministic routes.
    pub arrival_distribution: Vec<ArrivalScenario>,
}
