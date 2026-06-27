//! Tunable bike cost profile (BRouter "trekking"-inspired). The default matches
//! the values shipped in `config.yaml`; a sparse override merges field-by-field.

use serde::{Deserialize, Serialize};

use crate::structures::HighwayClass;

/// Per-highway base cost factors (>= 1 ideal). `_bike` variants apply when the
/// way carries a bike hint (BRouter `isbike`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HighwayFactors {
    pub trunk: f64,
    pub trunk_bike: f64,
    pub primary: f64,
    pub primary_bike: f64,
    pub secondary: f64,
    pub secondary_bike: f64,
    pub tertiary: f64,
    pub tertiary_bike: f64,
    pub unclassified: f64,
    pub unclassified_bike: f64,
    pub residential_paved: f64,
    pub residential_unpaved: f64,
    pub service_paved: f64,
    pub service_unpaved: f64,
    pub cycleway: f64,
    pub pedestrian: f64,
    pub bridleway: f64,
    pub other: f64,
}

impl Default for HighwayFactors {
    fn default() -> Self {
        HighwayFactors {
            trunk: 10.0,
            trunk_bike: 1.5,
            primary: 3.0,
            primary_bike: 1.2,
            secondary: 1.6,
            secondary_bike: 1.1,
            tertiary: 1.4,
            tertiary_bike: 1.0,
            unclassified: 1.3,
            unclassified_bike: 1.0,
            residential_paved: 1.1,
            residential_unpaved: 1.5,
            service_paved: 1.3,
            service_unpaved: 1.6,
            cycleway: 1.0,
            pedestrian: 3.0,
            bridleway: 5.0,
            other: 2.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BikeProfile {
    pub allow_steps: bool,
    /// When false, push (dismount) ways — foot-accessible but not bike-accessible —
    /// are impassable, so routes detour entirely around them instead of assuming
    /// the rider walks the bike.
    pub allow_dismount: bool,
    pub ignore_cycleroutes: bool,
    pub stick_to_cycleroutes: bool,
    pub avoid_unsafe: bool,
    /// When true, riding against a one-way (a `wrong_way` edge that ingest did not
    /// clear via a cyclist contraflow exemption) is impassable, so the search routes
    /// the legal direction instead of merely paying the `oneway_*` penalty.
    pub respect_oneway: bool,

    pub highway: HighwayFactors,

    /// Cost factor for a `highway=steps` (when allowed).
    pub steps_cost: f64,
    /// Extra cost factor added on unsafe roads without a bike hint (avoid_unsafe).
    pub unsafe_penalty: f64,
    /// Oneway penalties by class (added to costfactor on a wrong-way traversal).
    pub oneway_roundabout: f64,
    pub oneway_primary: f64,
    pub oneway_secondary: f64,
    pub oneway_tertiary: f64,
    pub oneway_other: f64,
    /// Access penalties (foot-only / cycleroute fallback / forbidden).
    pub access_foot_only: f64,
    pub access_cycleroute: f64,
    pub access_forbidden: f64,
    /// Turn cost (meters) for a 90° turn; scaled by (1 - cos angle).
    pub turncost: f64,

    pub consider_elevation: bool,
    pub uphillcost: f64,
    pub uphillcutoff: f64,
    pub downhillcost: f64,
    pub downhillcutoff: f64,
    /// BRouter elevation hysteresis buffers (meters). Descent/ascent must exceed
    /// `elevation_penalty_buffer` before any cost; above `elevation_max_buffer`
    /// the excess is force-drained and charged; `elevation_buffer_reduce` is a
    /// slope-proportional bleed (0 = drain only at the max ceiling).
    pub elevation_penalty_buffer: f64,
    pub elevation_max_buffer: f64,
    pub elevation_buffer_reduce: f64,

    pub total_mass: f64,
    pub max_speed: f64,
    pub s_c_x: f64,
    pub c_r: f64,
    pub biker_power: f64,

    /// Braking deceleration (m/s²) used to time a slow-down for a corner or a stop.
    pub brake_decel: f64,
    /// Comfortable forward acceleration (m/s²) when getting back up to speed after a
    /// stop or a corner. A rider surges well above cruise power to accelerate, so this
    /// is a constant rate, not energy/cruise-power.
    pub accel_rate: f64,
    /// Comfortable lateral (centripetal) acceleration (m/s²) a cyclist accepts in a
    /// bend; the cornering speed is `sqrt(lateral_accel · radius)`.
    pub lateral_accel: f64,
    /// Higher lateral tolerance (m/s²) applied when entering bike infrastructure
    /// (cycleway / cycle route / signed bike way): following a dedicated cycleway is
    /// efficient by design, so its curves and through-junctions are taken near cruise
    /// — only a genuine hairpin still slows. Roads keep the conservative `lateral_accel`.
    pub lateral_accel_infra: f64,
    /// Floor (m) on the segment length used to derive a corner's radius
    /// (`r = max(min_seg_len, corner_min_len) / θ`). A cyclist cannot pivot tighter
    /// than roughly a bike-length, so finely digitized geometry or a short junction
    /// connector must not fabricate a hairpin radius and a phantom slow-down.
    pub corner_min_len_m: f64,
    /// Speed (m/s) of pushing a dismounted bike on a non-stairs way (slower than free
    /// walking — a loaded bike is awkward to maneuver).
    pub push_speed_mps: f64,
    /// Speed (m/s) of hauling a dismounted bike up/down stairs (slower still).
    pub steps_push_speed_mps: f64,
}

impl Default for BikeProfile {
    fn default() -> Self {
        BikeProfile {
            allow_steps: true,
            allow_dismount: true,
            ignore_cycleroutes: false,
            stick_to_cycleroutes: true,
            avoid_unsafe: true,
            respect_oneway: true,
            highway: HighwayFactors::default(),
            steps_cost: 40.0,
            unsafe_penalty: 2.0,
            oneway_roundabout: 60.0,
            oneway_primary: 50.0,
            oneway_secondary: 30.0,
            oneway_tertiary: 20.0,
            oneway_other: 4.0,
            access_foot_only: 4.0,
            access_cycleroute: 15.0,
            access_forbidden: 10000.0,
            turncost: 90.0,
            consider_elevation: true,
            uphillcost: 0.0,
            uphillcutoff: 1.5,
            downhillcost: 100.0,
            downhillcutoff: 0.5,
            elevation_penalty_buffer: 5.0,
            elevation_max_buffer: 10.0,
            elevation_buffer_reduce: 0.0,
            total_mass: 90.0,
            max_speed: 36.0,
            s_c_x: 0.55,
            c_r: 0.006,
            biker_power: 100.0,
            brake_decel: 2.5,
            accel_rate: 1.0,
            lateral_accel: 3.5,
            lateral_accel_infra: 8.0,
            corner_min_len_m: 10.0,
            push_speed_mps: 0.9,
            steps_push_speed_mps: 0.25,
        }
    }
}

impl BikeProfile {
    /// Base cost factor for a highway class given a bike hint and surface,
    /// before cycleroute/oneway/access/elevation/turn adjustments.
    pub fn highway_factor(&self, h: HighwayClass, isbike: bool, unpaved: bool) -> f64 {
        use HighwayClass::*;
        let f = &self.highway;
        match h {
            Trunk | TrunkLink => {
                if isbike {
                    f.trunk_bike
                } else {
                    f.trunk
                }
            }
            Primary | PrimaryLink => {
                if isbike {
                    f.primary_bike
                } else {
                    f.primary
                }
            }
            Secondary | SecondaryLink => {
                if isbike {
                    f.secondary_bike
                } else {
                    f.secondary
                }
            }
            Tertiary | TertiaryLink => {
                if isbike {
                    f.tertiary_bike
                } else {
                    f.tertiary
                }
            }
            Unclassified => {
                if isbike {
                    f.unclassified_bike
                } else {
                    f.unclassified
                }
            }
            Residential | LivingStreet => {
                if unpaved {
                    f.residential_unpaved
                } else {
                    f.residential_paved
                }
            }
            Service => {
                if unpaved {
                    f.service_unpaved
                } else {
                    f.service_paved
                }
            }
            Cycleway => f.cycleway,
            Pedestrian => f.pedestrian,
            Bridleway => f.bridleway,
            _ => f.other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_commuter_values() {
        let p = BikeProfile::default();
        assert_eq!(p.highway.cycleway, 1.0);
        assert_eq!(p.highway.trunk, 10.0);
        assert_eq!(p.downhillcost, 100.0);
        assert_eq!(p.biker_power, 100.0);
        assert!(p.avoid_unsafe && p.stick_to_cycleroutes);
    }

    #[test]
    fn highway_factor_respects_bike_hint() {
        let p = BikeProfile::default();
        assert_eq!(p.highway_factor(HighwayClass::Primary, false, false), 3.0);
        assert_eq!(p.highway_factor(HighwayClass::Primary, true, false), 1.2);
        assert_eq!(p.highway_factor(HighwayClass::Service, false, true), 1.6);
    }

    #[test]
    fn partial_yaml_merges_onto_defaults() {
        // `#[serde(default)]` means a sparse map keeps the trekking defaults for
        // unspecified fields.
        let p: BikeProfile =
            serde_yaml_ng::from_str("allow_steps: false\nbiker_power: 150").unwrap();
        assert!(!p.allow_steps);
        assert_eq!(p.biker_power, 150.0);
        assert_eq!(p.downhillcost, 100.0); // untouched default
    }
}
