//! Deterministic multi-objective cost foundation (Phase 0). The search consumes
//! `CostVector`; probability is a separate post-hoc moment pair (`variance`).
pub mod agglomeration;
pub mod axis;
pub mod fares;
pub mod mode_axes;
pub mod variance;

pub use agglomeration::{Agglomeration, AgglomerationZone, point_in_polygon, zone_of};
pub use axis::{AXIS_COUNT, Axis, CostVector, Epsilon};
pub use fares::{
    DistanceTariff, FareContext, FareModel, FareProfile, KnownEurosEpsilon, N_OP, OperatorFare,
    OperatorFareId, OperatorModel, PassengerCategory, PriceValue, SncbTimeRules, TimeBucket,
    TimeWindowOperator, TravelClass,
};
pub use mode_axes::{BalanceWeights, CostWeights, RoutingMode, edge_cost_vector};
pub use variance::{
    LegRole, TimeMoments, VarGen, VarianceModel, edge_moments, edge_time_penalty, edge_variance,
};
