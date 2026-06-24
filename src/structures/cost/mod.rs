//! Deterministic multi-objective cost foundation (Phase 0). The search consumes
//! `CostVector`; probability is a separate post-hoc moment pair (`variance`).
pub mod axis;
pub mod mode_axes;
pub mod variance;

pub use axis::{AXIS_COUNT, Axis, CostVector, Epsilon};
pub use mode_axes::{BalanceWeights, CostWeights, RoutingMode, edge_cost_vector};
pub use variance::{
    LegRole, TimeMoments, VarGen, VarianceModel, edge_moments, edge_time_penalty, edge_variance,
};
