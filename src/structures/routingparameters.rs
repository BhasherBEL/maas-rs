use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RoutingParameters {
    pub walking_speed: usize,   // mm/s (1000x m/s, 278x km/h)
    pub estimator_speed: usize, // mm/s
}
