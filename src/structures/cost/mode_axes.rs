//! Mode-parametrization of the cost engine: which axes are active per mode and
//! how a single street edge maps to a `CostVector` for that mode. This folds the
//! existing scalar `BikeCost::cost_factor` logic into the bike axes (Task 0.4).

use crate::structures::cost::{Axis, CostVector, VarianceModel, edge_time_penalty, edge_variance};
use crate::structures::{BikeCost, BikeProfile, HighwayClass, StreetEdgeData};

/// Tunable per-axis weights for the deterministic cost vector. Serde-defaulted so
/// a sparse config block keeps these values. Lifted from formerly-hardcoded
/// constants so the cost model is fully config-driven.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CostWeights {
    pub surface_paved: f64,
    pub surface_unknown: f64,
    pub surface_unpaved: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        CostWeights {
            surface_paved: 1.0,
            surface_unknown: 1.3,
            surface_unpaved: 2.5,
        }
    }
}

/// Per-axis weights for the *balanced* presentation default. Read in exactly one
/// place (`initial_cursor`) to pick the highlighted representative — never by the
/// search or any dominance/pruning. Serde-defaulted so a sparse config keeps these.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct BalanceWeights {
    pub time: f64,
    pub dplus: f64,
    pub surface: f64,
    pub cycleway_deficit: f64,
    pub variance: f64,
}

impl Default for BalanceWeights {
    fn default() -> Self {
        BalanceWeights {
            time: 1.0,
            dplus: 0.5,
            surface: 0.7,
            cycleway_deficit: 0.5,
            variance: 0.8,
        }
    }
}

impl BalanceWeights {
    pub fn weight(&self, a: Axis) -> f64 {
        match a {
            Axis::Time => self.time,
            Axis::Dplus => self.dplus,
            Axis::Surface => self.surface,
            Axis::CyclewayDeficit => self.cycleway_deficit,
            Axis::Variance => self.variance,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoutingMode {
    Walk,
    Bike,
    Drive,
}

impl RoutingMode {
    pub fn axes(self) -> &'static [Axis] {
        match self {
            RoutingMode::Bike => &[
                Axis::Time,
                Axis::Dplus,
                Axis::Surface,
                Axis::CyclewayDeficit,
                Axis::Variance,
            ],
            RoutingMode::Walk => &[Axis::Time, Axis::Dplus, Axis::Surface, Axis::Variance],
            RoutingMode::Drive => &[Axis::Time, Axis::Variance],
        }
    }

    /// Axes the Pareto front DOMINATES on. A subset of `axes()`: the rest are
    /// carried in the cost vector and displayed, but never select routes. Bike uses
    /// the three the user ranked (Time › CyclewayDeficit › D+) so the front stays
    /// small enough to compute in full. Walk/Drive keep their whole axis set.
    pub fn front_axes(self) -> &'static [Axis] {
        match self {
            RoutingMode::Bike => &[Axis::Time, Axis::CyclewayDeficit, Axis::Dplus],
            RoutingMode::Walk => self.axes(),
            RoutingMode::Drive => self.axes(),
        }
    }

    /// Front axes honoring the `bike_select_dplus` config flag: when false (default),
    /// bike selection/dominance drops D+ (it stays in the cost vector, displayed only).
    /// Rationale: once climbing is realistically slow (gradient power model), Time
    /// already prices hills, so a separate "minimize D+ at any cost" axis only
    /// manufactures absurd extremes (e.g. a 1.5 km walk to shave a few m of ascent).
    pub fn effective_front_axes(self, select_dplus: bool) -> &'static [Axis] {
        const BIKE_NO_DPLUS: [Axis; 2] = [Axis::Time, Axis::CyclewayDeficit];
        if self == RoutingMode::Bike && !select_dplus {
            &BIKE_NO_DPLUS
        } else {
            self.front_axes()
        }
    }
}

/// Map a single directed street edge to a deterministic `CostVector` for `mode`.
/// `incoming`/`this_dir` carry turn geometry for the Comfort axis (bike only).
/// `speed_mps` is used by Walk and Drive to fill the Time axis via the same
/// integer arithmetic as `Graph::edge_secs` (`length*1000 / speed_mms`, truncating),
/// so the multi-objective engine is bit-identical to the scalar search.
/// Bike Time stays kinematic (`edge_time`); push/dismount edges walk at `speed_mps`.
/// Returns `None` for an impassable edge in this mode.
///
/// NOTE: D+ here is the per-edge positive ascent only, read from the DEM-denoised
/// `elev_delta` baked at ingestion (per-way RDP smoothing). No in-search elevation
/// hysteresis is added — the formerly path-coupled `BikeCost::elevation_step` term
/// was unsound for label-setting and has been dropped from the D+ axis.
pub fn edge_cost_vector(
    mode: RoutingMode,
    e: &StreetEdgeData,
    profile: &BikeProfile,
    weights: &CostWeights,
    model: &VarianceModel,
    speed_mps: f64,
    incoming: Option<(f64, f64)>,
    this_dir: (f64, f64),
) -> Option<CostVector> {
    match mode {
        RoutingMode::Bike => bike_vector(e, profile, weights, model, incoming, this_dir),
        RoutingMode::Walk => walk_vector(e, weights, model, speed_mps),
        RoutingMode::Drive => drive_vector(e, model, speed_mps),
    }
}

fn dplus(e: &StreetEdgeData) -> f64 {
    (e.elev_delta as f64).max(0.0)
}

fn street_secs(length: usize, speed_mps: f64) -> f64 {
    let speed_mms = (speed_mps * 1000.0) as u32;
    if speed_mms == 0 {
        return 0.0;
    }
    (length as u64 * 1000 / speed_mms as u64) as f64
}

fn bike_vector(
    e: &StreetEdgeData,
    profile: &BikeProfile,
    weights: &CostWeights,
    model: &VarianceModel,
    incoming: Option<(f64, f64)>,
    this_dir: (f64, f64),
) -> Option<CostVector> {
    let bc = BikeCost::new(*profile);
    bc.edge_cost(e, incoming, this_dir)?;
    let len = e.length as f64;
    let mut cv = CostVector::ZERO;
    cv.set(
        Axis::Time,
        bc.edge_time(e) as f64 + edge_time_penalty(e, model),
    );
    cv.set(Axis::Dplus, dplus(e));
    cv.set(Axis::Surface, len * surface_factor(e, weights));
    let on_infra =
        e.attrs.cycleroute || e.attrs.isbike || matches!(e.attrs.highway, HighwayClass::Cycleway);
    let deficit = if !BikeCost::is_push(&e.attrs) && on_infra {
        0.0
    } else {
        bc.edge_time(e) as f64
    };
    cv.set(Axis::CyclewayDeficit, deficit);
    cv.set(
        Axis::Variance,
        edge_variance(RoutingMode::Bike, e, model, incoming, this_dir),
    );
    Some(cv)
}

fn surface_factor(e: &StreetEdgeData, w: &CostWeights) -> f64 {
    use crate::structures::Surface;
    match e.attrs.surface {
        Surface::Paved => w.surface_paved,
        Surface::Unknown => w.surface_unknown,
        Surface::Unpaved => w.surface_unpaved,
    }
}

/// Walk and Drive Time is now filled from `speed_mps` via the same integer
/// arithmetic as `Graph::edge_secs` (`length*1000 / (speed_mps*1000) as u32`,
/// truncating division), so the multi-objective engine is bit-identical to the
/// scalar search. Bike Time stays kinematic (`edge_time`), not speed×length.
fn walk_vector(
    e: &StreetEdgeData,
    weights: &CostWeights,
    model: &VarianceModel,
    speed_mps: f64,
) -> Option<CostVector> {
    if !e.foot {
        return None;
    }
    let len = e.length as f64;
    let mut cv = CostVector::ZERO;
    cv.set(
        Axis::Time,
        street_secs(e.length, speed_mps) + edge_time_penalty(e, model),
    );
    cv.set(Axis::Dplus, dplus(e));
    cv.set(Axis::Surface, len * surface_factor(e, weights));
    cv.set(
        Axis::Variance,
        edge_variance(RoutingMode::Walk, e, model, None, (0.0, 0.0)),
    );
    Some(cv)
}

fn drive_vector(e: &StreetEdgeData, model: &VarianceModel, speed_mps: f64) -> Option<CostVector> {
    if !e.car {
        return None;
    }
    let mut cv = CostVector::ZERO;
    cv.set(Axis::Time, street_secs(e.length, speed_mps));
    cv.set(
        Axis::Variance,
        edge_variance(RoutingMode::Drive, e, model, None, (0.0, 0.0)),
    );
    Some(cv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::cost::{Axis, VarianceModel};

    #[test]
    fn bike_vector_separates_surface_and_cycleway_from_time() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, edge_cost_vector},
        };
        let profile = BikeProfile::default();
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Cycleway;
        attrs.isbike = true;
        attrs.surface = Surface::Paved;
        let edge = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        let cv = edge_cost_vector(
            RoutingMode::Bike,
            &edge,
            &profile,
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert_eq!(cv.get(Axis::CyclewayDeficit), 0.0);
        assert!(cv.get(Axis::Time) > 0.0);

        let mut road = attrs;
        road.highway = HighwayClass::Primary;
        road.isbike = false;
        let road_edge = StreetEdgeData {
            attrs: road,
            var_gen: crate::structures::cost::VarGen::SIGNALIZED,
            ..edge
        };
        let cvr = edge_cost_vector(
            RoutingMode::Bike,
            &road_edge,
            &profile,
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        let expected_penalty =
            crate::structures::cost::edge_time_penalty(&road_edge, &VarianceModel::default());
        assert_eq!(
            cvr.get(Axis::CyclewayDeficit),
            cvr.get(Axis::Time) - expected_penalty,
            "off-infra deficit is the kinematic ride time; Time also carries the signal delay"
        );
        assert!(cvr.get(Axis::Variance) > cv.get(Axis::Variance));
    }

    #[test]
    fn push_edge_is_not_effective_cycle_infra() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Cycleway;
        attrs.isbike = true;
        attrs.surface = Surface::Paved;
        attrs.bikeaccess = false;
        attrs.footaccess = true;
        let edge = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        let cv = edge_cost_vector(
            RoutingMode::Bike,
            &edge,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert_eq!(
            cv.get(Axis::CyclewayDeficit),
            cv.get(Axis::Time),
            "a push edge's deficit is its slow walk time, not zero — even tagged cycle infra"
        );
        assert!(cv.get(Axis::CyclewayDeficit) > 0.0);
    }

    #[test]
    fn impassable_edge_maps_to_none() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData,
            cost::{CostWeights, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Motorway;
        let edge = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: false,
            bike: true,
            car: true,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        assert!(
            edge_cost_vector(
                RoutingMode::Bike,
                &edge,
                &BikeProfile::default(),
                &CostWeights::default(),
                &VarianceModel::default(),
                1.2,
                None,
                (1.0, 0.0)
            )
            .is_none()
        );
    }

    #[test]
    fn dplus_axis_is_positive_ascent_only() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData,
            cost::{CostWeights, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Tertiary;
        let up = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 10,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        let down = StreetEdgeData {
            elev_delta: -10,
            ..up
        };
        let cu = edge_cost_vector(
            RoutingMode::Bike,
            &up,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        let cd = edge_cost_vector(
            RoutingMode::Bike,
            &down,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert_eq!(cu.get(Axis::Dplus), 10.0);
        assert_eq!(cd.get(Axis::Dplus), 0.0);
    }

    #[test]
    fn bike_turn_adds_variance_regardless_of_cycleroute() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Tertiary;
        attrs.surface = Surface::Paved;
        let mut on_route = attrs;
        on_route.cycleroute = true;
        let off = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        let on = StreetEdgeData {
            attrs: on_route,
            ..off
        };
        let c_off = edge_cost_vector(
            RoutingMode::Bike,
            &off,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            Some((0.0, 1.0)),
            (1.0, 0.0),
        )
        .unwrap();
        let c_on = edge_cost_vector(
            RoutingMode::Bike,
            &on,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            Some((0.0, 1.0)),
            (1.0, 0.0),
        )
        .unwrap();
        assert!(c_off.get(Axis::Variance) > 0.0, "a turn adds variance");
        assert_eq!(
            c_on.get(Axis::Variance),
            c_off.get(Axis::Variance),
            "variance no longer suppresses turns on cycleroutes"
        );
    }

    #[test]
    fn surface_axis_respects_config_weights() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Tertiary;
        attrs.surface = Surface::Unpaved;
        let e = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
        };
        let default_w = CostWeights::default();
        let mut soft = default_w;
        soft.surface_unpaved = 1.0;
        let cv_rough = edge_cost_vector(
            RoutingMode::Bike,
            &e,
            &BikeProfile::default(),
            &default_w,
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        let cv_soft = edge_cost_vector(
            RoutingMode::Bike,
            &e,
            &BikeProfile::default(),
            &soft,
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert!(
            cv_rough.get(Axis::Surface) > cv_soft.get(Axis::Surface),
            "higher unpaved weight raises the Surface axis"
        );
        assert_eq!(cv_soft.get(Axis::Surface), 100.0);
    }

    #[test]
    fn front_axes_bike_is_time_cycleway_dplus_walk_drive_unchanged() {
        assert_eq!(
            RoutingMode::Bike.front_axes(),
            &[Axis::Time, Axis::CyclewayDeficit, Axis::Dplus]
        );
        assert_eq!(RoutingMode::Walk.front_axes(), RoutingMode::Walk.axes());
        assert_eq!(RoutingMode::Drive.front_axes(), RoutingMode::Drive.axes());
    }

    #[test]
    fn effective_front_axes_demotes_dplus_by_default() {
        // Default (select_dplus = false): bike selects on Time + CyclewayDeficit only.
        assert_eq!(
            RoutingMode::Bike.effective_front_axes(false),
            &[Axis::Time, Axis::CyclewayDeficit]
        );
        // With the flag on, D+ is restored as the third selection axis.
        assert_eq!(
            RoutingMode::Bike.effective_front_axes(true),
            &[Axis::Time, Axis::CyclewayDeficit, Axis::Dplus]
        );
        // Walk/Drive are unaffected by the flag.
        assert_eq!(
            RoutingMode::Walk.effective_front_axes(false),
            RoutingMode::Walk.front_axes()
        );
    }

    #[test]
    fn mode_axis_sets_match_spec() {
        assert_eq!(
            RoutingMode::Bike.axes(),
            &[
                Axis::Time,
                Axis::Dplus,
                Axis::Surface,
                Axis::CyclewayDeficit,
                Axis::Variance
            ]
        );
        assert_eq!(
            RoutingMode::Walk.axes(),
            &[Axis::Time, Axis::Dplus, Axis::Surface, Axis::Variance]
        );
        assert_eq!(RoutingMode::Drive.axes(), &[Axis::Time, Axis::Variance]);
    }

    #[test]
    fn variance_is_a_base_axis_for_all_modes() {
        for m in [RoutingMode::Walk, RoutingMode::Bike, RoutingMode::Drive] {
            assert!(m.axes().contains(&Axis::Variance), "variance is always on");
        }
    }

    #[test]
    fn walk_vector_sets_variance_from_crossings() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData,
            cost::{CostWeights, VarGen, edge_cost_vector},
        };
        let mut a = BikeAttrs::road_default();
        a.highway = HighwayClass::Residential;
        let e = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 60,
            foot: true,
            bike: true,
            car: true,
            attrs: a,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::SIGNALIZED,
        };
        let cv = edge_cost_vector(
            RoutingMode::Walk,
            &e,
            &BikeProfile::default(),
            &CostWeights::default(),
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert!(
            cv.get(Axis::Variance) > 0.0,
            "a signalized walk edge carries variance"
        );
    }

    #[test]
    fn bike_time_axis_grows_by_signal_only_corner_is_a_transition_cost() {
        // The per-edge bike Time now carries only the signal delay; the turn/corner
        // slow-down is a transition cost charged by the fold (`speed_change_secs`),
        // not by `edge_cost_vector`, so a turn here does NOT change the per-edge Time.
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, VarGen, edge_cost_vector, edge_time_penalty},
        };
        let model = VarianceModel::default();
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Tertiary;
        attrs.surface = Surface::Paved;
        let plain = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        };
        let signal = StreetEdgeData {
            var_gen: VarGen::SIGNALIZED,
            ..plain
        };
        let this = (1.0, 0.0);
        let t = |e: &StreetEdgeData, inc: Option<(f64, f64)>| {
            edge_cost_vector(
                RoutingMode::Bike,
                e,
                &BikeProfile::default(),
                &CostWeights::default(),
                &model,
                1.2,
                inc,
                this,
            )
            .unwrap()
            .get(Axis::Time)
        };
        let base = t(&plain, None);
        let turned = t(&plain, Some((0.0, 1.0)));
        let signal_time = t(&signal, None);
        assert_eq!(turned, base, "a turn no longer changes the per-edge bike Time");
        assert!(
            (signal_time - base - edge_time_penalty(&signal, &model)).abs() < 1e-6,
            "a signal raises bike Time by exactly the signal delay"
        );
    }

    #[test]
    fn walk_time_axis_grows_by_signal_penalty() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData,
            cost::{CostWeights, VarGen, edge_cost_vector},
        };
        let model = VarianceModel::default();
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Residential;
        let plain = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 120,
            foot: true,
            bike: true,
            car: true,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        };
        let signal = StreetEdgeData {
            var_gen: VarGen::SIGNALIZED,
            ..plain
        };
        let t = |e: &StreetEdgeData| {
            edge_cost_vector(
                RoutingMode::Walk,
                e,
                &BikeProfile::default(),
                &CostWeights::default(),
                &model,
                1.2,
                None,
                (1.0, 0.0),
            )
            .unwrap()
            .get(Axis::Time)
        };
        assert_eq!(
            t(&signal) - t(&plain),
            model.signal_delay_minor,
            "a signalized walk edge's Time grows by the minor-road signal delay"
        );
    }

    #[test]
    fn balance_weights_default_is_uniform_positive() {
        let b = BalanceWeights::default();
        for &ax in &[
            Axis::Time,
            Axis::Dplus,
            Axis::Surface,
            Axis::CyclewayDeficit,
            Axis::Variance,
        ] {
            assert!(
                b.weight(ax) > 0.0,
                "every active axis has a positive default weight"
            );
        }
    }

    #[test]
    fn bike_push_edge_time_uses_profile_push_speed() {
        // A push edge is now timed at the profile's `push_speed_mps` (a property of
        // pushing a loaded bike), independent of the passed cost speed.
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData, Surface,
            cost::{CostWeights, VarGen, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Footway;
        attrs.surface = Surface::Paved;
        attrs.bikeaccess = false;
        attrs.footaccess = true;
        let e = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 100,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        };
        let w = CostWeights::default();
        let mk = |speed: f64| {
            edge_cost_vector(
                RoutingMode::Bike,
                &e,
                &BikeProfile::default(),
                &w,
                &VarianceModel::default(),
                speed,
                None,
                (1.0, 0.0),
            )
            .unwrap()
            .get(Axis::Time)
        };
        let expected = (100.0_f64 / BikeProfile::default().push_speed_mps).round();
        assert_eq!(mk(1.0), expected);
        assert_eq!(mk(2.0), expected, "push time is independent of the passed speed");
    }

    #[test]
    fn walk_and_drive_time_filled_from_speed() {
        use crate::structures::{
            BikeAttrs, BikeProfile, HighwayClass, NodeID, StreetEdgeData,
            cost::{CostWeights, VarGen, edge_cost_vector},
        };
        let mut attrs = BikeAttrs::road_default();
        attrs.highway = HighwayClass::Residential;
        let e = StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length: 120,
            foot: true,
            bike: true,
            car: true,
            attrs,
            elev_delta: 0,
            surface_speed: 100,
            var_gen: VarGen::NONE,
        };
        let w = CostWeights::default();
        let walk = edge_cost_vector(
            RoutingMode::Walk,
            &e,
            &BikeProfile::default(),
            &w,
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        let drive = edge_cost_vector(
            RoutingMode::Drive,
            &e,
            &BikeProfile::default(),
            &w,
            &VarianceModel::default(),
            11.0,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert_eq!(walk.get(Axis::Time), 100.0);
        assert_eq!(drive.get(Axis::Time), 10.0);
        let bike = edge_cost_vector(
            RoutingMode::Bike,
            &e,
            &BikeProfile::default(),
            &w,
            &VarianceModel::default(),
            1.2,
            None,
            (1.0, 0.0),
        )
        .unwrap();
        assert!(
            bike.get(Axis::Time) > 0.0,
            "bike Time stays kinematic and > 0"
        );
    }
}
