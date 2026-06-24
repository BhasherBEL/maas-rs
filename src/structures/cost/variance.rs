//! Time uncertainty. `edge_variance` is the single source of truth: it feeds both
//! the always-on `Variance` front axis (via `edge_cost_vector`) and the post-hoc
//! `[p50, p95]` bracket summed along a fixed path (via `annotate_path`). Edges are
//! treated as independent, so variances add (the post-hoc bracket adds a
//! correlated systematic term for long direct paths).
//!
//! OSM lacks signal cycle/red times and traffic volume, so the σ magnitudes are
//! defaulted parameters (proxied from road class) held in `VarianceModel` and
//! overridable from config.

use serde::{Deserialize, Serialize};

use crate::structures::cost::RoutingMode;
use crate::structures::{HighwayClass, StreetEdgeData};

const Z_P95: f64 = 1.645;

/// Variance-generating features classified once at ingest, packed as a bit set.
/// A plain `u8` newtype (not `bitflags`) so it serializes trivially with the
/// graph cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VarGen(u8);

impl VarGen {
    pub const NONE: VarGen = VarGen(0);
    pub const SIGNALIZED: VarGen = VarGen(0b0001);
    pub const UNCONTROLLED: VarGen = VarGen(0b0010);
    pub const ELEVATOR: VarGen = VarGen(0b0100);

    pub fn contains(self, other: VarGen) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn with(self, other: VarGen) -> VarGen {
        VarGen(self.0 | other.0)
    }
}

/// Tunable σ (seconds) for each variance generator, proxied from road class
/// because OSM does not carry signal timing or traffic volume. Serde-defaulted so
/// a sparse config block keeps these values.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct VarianceModel {
    pub signal_sigma_major: f64,
    pub signal_sigma_secondary: f64,
    pub signal_sigma_minor: f64,
    pub elevator_sigma: f64,
    pub uncontrolled_sigma: f64,
    pub turn_sigma: f64,
    pub stress_var_per_m: f64,
    pub signal_delay_major: f64,
    pub signal_delay_secondary: f64,
    pub signal_delay_minor: f64,
    /// σ (seconds) of the uncertainty added once at each dismount (a push run): how
    /// long the stop+restart takes is far less predictable than riding.
    pub push_sigma: f64,
}

impl Default for VarianceModel {
    fn default() -> Self {
        VarianceModel {
            signal_sigma_major: 25.0,
            signal_sigma_secondary: 18.0,
            signal_sigma_minor: 12.0,
            elevator_sigma: 20.0,
            uncontrolled_sigma: 3.0,
            turn_sigma: 10.0,
            stress_var_per_m: 0.0,
            signal_delay_major: 15.0,
            signal_delay_secondary: 10.0,
            signal_delay_minor: 7.0,
            push_sigma: 8.0,
        }
    }
}

impl VarianceModel {
    /// Total time variance (seconds²) contributed by an edge's generators.
    pub fn variance(&self, vg: VarGen, highway: HighwayClass) -> f64 {
        let mut var = 0.0;
        if vg.contains(VarGen::SIGNALIZED) {
            let s = self.signal_sigma(highway);
            var += s * s;
        }
        if vg.contains(VarGen::ELEVATOR) {
            var += self.elevator_sigma * self.elevator_sigma;
        }
        if vg.contains(VarGen::UNCONTROLLED) {
            var += self.uncontrolled_sigma * self.uncontrolled_sigma;
        }
        var
    }

    fn signal_sigma(&self, h: HighwayClass) -> f64 {
        use HighwayClass::*;
        match h {
            Primary | PrimaryLink | Trunk | TrunkLink => self.signal_sigma_major,
            Secondary | SecondaryLink => self.signal_sigma_secondary,
            _ => self.signal_sigma_minor,
        }
    }

    fn signal_delay(&self, h: HighwayClass) -> f64 {
        use HighwayClass::*;
        match h {
            Primary | PrimaryLink | Trunk | TrunkLink => self.signal_delay_major,
            Secondary | SecondaryLink => self.signal_delay_secondary,
            _ => self.signal_delay_minor,
        }
    }
}

fn is_stressful_class(h: HighwayClass) -> bool {
    use HighwayClass::*;
    matches!(
        h,
        Primary | PrimaryLink | Secondary | SecondaryLink | Trunk | TrunkLink
    )
}

/// Total time variance (seconds²) for one directed edge: crossings/signals/
/// elevators (all modes), road-class exposure (all modes), and turns (bike only).
/// The single source of truth shared by the front `Variance` axis and the
/// post-hoc `[p50,p95]` bracket.
pub fn edge_variance(
    mode: RoutingMode,
    e: &StreetEdgeData,
    model: &VarianceModel,
    incoming: Option<(f64, f64)>,
    this_dir: (f64, f64),
) -> f64 {
    let mut var = model.variance(e.var_gen, e.attrs.highway);
    if is_stressful_class(e.attrs.highway) {
        var += model.stress_var_per_m * e.length as f64;
    }
    if mode == RoutingMode::Bike {
        if let Some(inc) = incoming {
            let dot = (inc.0 * this_dir.0 + inc.1 * this_dir.1).clamp(-1.0, 1.0);
            let s = model.turn_sigma * (1.0 - dot) / 2.0;
            var += s * s;
        }
    }
    var
}

/// Expected (mean) time in seconds added to one directed edge by traffic signals
/// (all modes). The bike turn-mean delay it formerly carried is now the
/// physically-grounded corner slow-down in `BikeCost::speed_change_secs`, charged
/// per-vertex by the fold (it needs the previous edge's length/cruise speed, which
/// this per-edge signature lacks). The single source of truth for the signal part,
/// shared by the front `Time` axis and the post-hoc `[p50,p95]` bracket's mean.
pub fn edge_time_penalty(e: &StreetEdgeData, model: &VarianceModel) -> f64 {
    let mut t = 0.0;
    if e.var_gen.contains(VarGen::SIGNALIZED) {
        t += model.signal_delay(e.attrs.highway);
    }
    t
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeMoments {
    pub mean: f64,
    pub var: f64,
}

impl TimeMoments {
    pub const ZERO: TimeMoments = TimeMoments {
        mean: 0.0,
        var: 0.0,
    };

    pub fn added(&self, other: &TimeMoments) -> TimeMoments {
        TimeMoments {
            mean: self.mean + other.mean,
            var: self.var + other.var,
        }
    }

    /// `[p50, p95] = [mean, mean + Z_P95*sqrt(var)]`.
    pub fn bracket(&self) -> (f64, f64) {
        (self.mean, self.mean + Z_P95 * self.var.sqrt())
    }
}

/// Leg structural role — drives access/egress percentile treatment at plan
/// reconstruction. No longer affects the street search (the `Variance` axis is
/// always on), so the two roles produce the same Pareto front.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LegRole {
    Neutral,
    Deadline,
}

impl Default for LegRole {
    fn default() -> Self {
        LegRole::Neutral
    }
}

/// Per-edge `(mean, variance)` time in seconds. Mean = kinematic time at
/// `speed_mps` plus the expected signal delay; variance accrues only from this
/// edge's generator flags. The turn-delay component of the mean (bike only)
/// requires turn geometry this signature lacks, so it is applied by the caller
/// (`annotate_path`), which threads `incoming`/`this_dir`.
pub fn edge_moments(e: &StreetEdgeData, speed_mps: f64, model: &VarianceModel) -> TimeMoments {
    let kinematic = (e.length as f64 / speed_mps.max(0.1)).round();
    let mean = kinematic + edge_time_penalty(e, model);
    let var = model.variance(var_gen(e), e.attrs.highway);
    TimeMoments { mean, var }
}

/// Read this edge's generator flags, classified at ingest from endpoint nodes.
fn var_gen(e: &StreetEdgeData) -> VarGen {
    e.var_gen
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::{BikeAttrs, HighwayClass, NodeID, StreetEdgeData};

    fn edge(h: HighwayClass, len: usize) -> StreetEdgeData {
        let mut a = BikeAttrs::road_default();
        a.highway = h;
        StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: len,
            foot: true,
            bike: true,
            car: true,
            attrs: a,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        }
    }

    #[test]
    fn moments_sum_along_a_path() {
        let m1 = TimeMoments {
            mean: 30.0,
            var: 4.0,
        };
        let m2 = TimeMoments {
            mean: 20.0,
            var: 9.0,
        };
        let total = m1.added(&m2);
        assert_eq!(total.mean, 50.0);
        assert_eq!(total.var, 13.0);
    }

    #[test]
    fn p95_bracket_uses_1645_sigma() {
        let m = TimeMoments {
            mean: 600.0,
            var: 100.0,
        };
        let (p50, p95) = m.bracket();
        assert_eq!(p50, 600.0);
        assert!((p95 - (600.0 + 1.645 * 10.0)).abs() < 1e-6);
    }

    #[test]
    fn signalized_crossing_adds_variance_plain_does_not() {
        let model = VarianceModel::default();
        let signal = model.variance(VarGen::SIGNALIZED, HighwayClass::Residential);
        let plain = model.variance(VarGen::NONE, HighwayClass::Residential);
        assert!(
            signal > plain,
            "signalized crossing is a variance generator"
        );
        assert_eq!(
            plain, 0.0,
            "a plain residential segment carries no variance"
        );
    }

    #[test]
    fn signal_variance_scales_with_road_class() {
        let model = VarianceModel::default();
        let major = model.variance(VarGen::SIGNALIZED, HighwayClass::Primary);
        let minor = model.variance(VarGen::SIGNALIZED, HighwayClass::Residential);
        assert!(major > minor, "a signal on a major road has wider spread");
    }

    #[test]
    fn edge_moments_mean_is_length_over_speed_and_no_generators_means_zero_var() {
        let m = edge_moments(
            &edge(HighwayClass::Residential, 60),
            1.2,
            &VarianceModel::default(),
        );
        assert_eq!(m.mean, (60.0_f64 / 1.2).round());
        assert_eq!(
            m.var, 0.0,
            "an edge with no generator flags carries no variance"
        );
    }

    #[test]
    fn var_gen_field_drives_edge_variance() {
        let mut e = edge(HighwayClass::Residential, 60);
        e.var_gen = VarGen::SIGNALIZED;
        let m = edge_moments(&e, 1.2, &VarianceModel::default());
        assert!(
            m.var > 0.0,
            "a stored SIGNALIZED flag must drive edge variance"
        );
    }

    #[test]
    fn legrole_defaults_to_neutral() {
        assert_eq!(LegRole::default(), LegRole::Neutral);
    }

    #[test]
    fn edge_variance_includes_crossing_variance_all_modes() {
        let model = VarianceModel::default();
        let mut e = edge(HighwayClass::Residential, 60);
        e.var_gen = VarGen::SIGNALIZED;
        let walk = edge_variance(RoutingMode::Walk, &e, &model, None, (1.0, 0.0));
        assert!(walk > 0.0, "crossing variance applies on foot");
        assert_eq!(
            walk,
            model.variance(VarGen::SIGNALIZED, HighwayClass::Residential),
            "no turn/road-class term on a plain signalized residential"
        );
    }

    #[test]
    fn edge_variance_adds_road_class_exposure() {
        let model = VarianceModel {
            stress_var_per_m: 1.0,
            ..VarianceModel::default()
        };
        let primary = edge(HighwayClass::Primary, 100);
        let resid = edge(HighwayClass::Residential, 100);
        let vp = edge_variance(RoutingMode::Walk, &primary, &model, None, (1.0, 0.0));
        let vr = edge_variance(RoutingMode::Walk, &resid, &model, None, (1.0, 0.0));
        assert!(vp > vr, "a stressful road class adds variance");
        assert!((vp - vr - model.stress_var_per_m * 100.0).abs() < 1e-6);
    }

    #[test]
    fn edge_time_penalty_signal_by_class_and_none_is_zero() {
        let model = VarianceModel::default();
        let mut major = edge(HighwayClass::Primary, 60);
        major.var_gen = VarGen::SIGNALIZED;
        let mut minor = edge(HighwayClass::Residential, 60);
        minor.var_gen = VarGen::SIGNALIZED;
        let plain = edge(HighwayClass::Residential, 60);
        let p_major = edge_time_penalty(&major, &model);
        let p_minor = edge_time_penalty(&minor, &model);
        let p_plain = edge_time_penalty(&plain, &model);
        assert_eq!(p_major, model.signal_delay_major);
        assert_eq!(p_minor, model.signal_delay_minor);
        assert!(p_major > p_minor, "a major-road signal costs more mean time");
        assert_eq!(p_plain, 0.0, "a non-signalized edge adds no mean delay");
    }

    #[test]
    fn edge_time_penalty_is_signal_only_no_turn_term() {
        // The turn-mean delay moved to the physically-grounded corner slow-down in
        // `BikeCost::speed_change_secs`; `edge_time_penalty` now carries only signals,
        // independent of any turn geometry.
        let model = VarianceModel::default();
        let mut signal = edge(HighwayClass::Residential, 60);
        signal.var_gen = VarGen::SIGNALIZED;
        let plain = edge(HighwayClass::Residential, 60);
        assert_eq!(edge_time_penalty(&signal, &model), model.signal_delay_minor);
        assert_eq!(
            edge_time_penalty(&plain, &model),
            0.0,
            "no signal, no turn term ⇒ no per-edge mean delay"
        );
    }

    #[test]
    fn edge_moments_mean_includes_signal_delay() {
        let model = VarianceModel::default();
        let plain = edge(HighwayClass::Residential, 60);
        let mut signal = edge(HighwayClass::Residential, 60);
        signal.var_gen = VarGen::SIGNALIZED;
        let m_plain = edge_moments(&plain, 1.2, &model);
        let m_signal = edge_moments(&signal, 1.2, &model);
        assert_eq!(
            m_signal.mean - m_plain.mean,
            model.signal_delay_minor,
            "p50 mean shifts by the expected signal delay"
        );
    }

    #[test]
    fn edge_variance_turn_term_is_bike_only() {
        let model = VarianceModel::default();
        let e = edge(HighwayClass::Residential, 60);
        let inc = Some((1.0, 0.0));
        let reversed = (-1.0, 0.0);
        let bike = edge_variance(RoutingMode::Bike, &e, &model, inc, reversed);
        let walk = edge_variance(RoutingMode::Walk, &e, &model, inc, reversed);
        assert!(bike > walk, "turns add variance for bike, not walk");
        assert_eq!(
            walk, 0.0,
            "plain residential, no crossing, walk has no turn variance"
        );
        assert!(
            (bike - model.turn_sigma * model.turn_sigma).abs() < 1e-6,
            "full reversal turn variance = turn_sigma^2"
        );
    }
}
