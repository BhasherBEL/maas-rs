//! Deterministic multi-objective cost vector. Fixed-width over `Axis` so a label
//! is `Copy` and dominance is a tight loop. Probability NEVER lives here — see
//! `cost::variance` for the post-hoc moment pair.

/// The full universe of routing axes. A given mode activates a subset (see
/// `cost::mode_axes`); inactive axes are held at 0.0 and ignored by callers, but
/// dominance over the fixed array is still correct because 0 <= 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Axis {
    /// Deterministic travel time (seconds), the spine axis present in every mode.
    Time,
    /// Cumulative ascent (meters), free closed-form heuristic.
    Dplus,
    /// Surface-roughness accumulation (unitless cost·meters).
    Surface,
    /// Inverse cycleway exposure: meters NOT on bike infrastructure.
    CyclewayDeficit,
    /// Time variance (seconds²), always active. Accumulates crossings/signals/
    /// elevators (all modes), road-class exposure (all modes), and turns (bike).
    /// Additive along a path; the user-facing expression is the `[p50,p95]`
    /// bracket. NOT a probability — see `cost::variance::edge_variance`.
    Variance,
}

pub const AXIS_COUNT: usize = 5;

impl Axis {
    #[inline]
    pub fn index(self) -> usize {
        match self {
            Axis::Time => 0,
            Axis::Dplus => 1,
            Axis::Surface => 2,
            Axis::CyclewayDeficit => 3,
            Axis::Variance => 4,
        }
    }
    pub const ALL: [Axis; AXIS_COUNT] = [
        Axis::Time,
        Axis::Dplus,
        Axis::Surface,
        Axis::CyclewayDeficit,
        Axis::Variance,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostVector {
    v: [f64; AXIS_COUNT],
}

impl CostVector {
    pub const ZERO: CostVector = CostVector {
        v: [0.0; AXIS_COUNT],
    };

    pub fn from_active(axes: &[Axis], values: &[f64]) -> Self {
        assert_eq!(axes.len(), values.len());
        let mut v = [0.0; AXIS_COUNT];
        for (a, &val) in axes.iter().zip(values) {
            v[a.index()] = val;
        }
        CostVector { v }
    }

    #[inline]
    pub fn get(&self, a: Axis) -> f64 {
        self.v[a.index()]
    }

    #[inline]
    pub fn set(&mut self, a: Axis, val: f64) {
        self.v[a.index()] = val;
    }

    /// Component-wise dominance: self <= other on all axes, < on at least one.
    #[inline]
    pub fn dominates(&self, other: &CostVector) -> bool {
        let mut strict = false;
        for i in 0..AXIS_COUNT {
            if self.v[i] > other.v[i] {
                return false;
            }
            if self.v[i] < other.v[i] {
                strict = true;
            }
        }
        strict
    }

    /// Weak (non-strict) dominance: `self <= other` on every axis, equality
    /// allowed. Unlike [`dominates`], a tie on all axes counts as weak dominance.
    /// Used for target pruning, where a completed destination label that merely
    /// ties a partial label's lower bound already makes that partial useless.
    #[inline]
    pub fn weakly_dominates(&self, other: &CostVector) -> bool {
        for i in 0..AXIS_COUNT {
            if self.v[i] > other.v[i] {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn added(&self, other: &CostVector) -> CostVector {
        let mut v = self.v;
        for i in 0..AXIS_COUNT {
            v[i] += other.v[i];
        }
        CostVector { v }
    }

    /// Copy with only `axes` retained (others zeroed). Used to compute Pareto
    /// dominance over a *subset* of axes — the core front — while the full vector
    /// is still carried for display. Zeroed axes never constrain dominance (0 ≤ x).
    #[inline]
    pub fn project(&self, axes: &[Axis]) -> CostVector {
        let mut v = [0.0; AXIS_COUNT];
        for &a in axes {
            v[a.index()] = self.v[a.index()];
        }
        CostVector { v }
    }
}

/// Per-axis ε = a_i + b_i·value (absolute floor + relative term, additive-leaning).
/// Sourced from config; `uniform` is for tests only.
#[derive(Debug, Clone, Copy)]
pub struct Epsilon {
    a: [f64; AXIS_COUNT],
    b: [f64; AXIS_COUNT],
}

impl Epsilon {
    pub fn new(a: [f64; AXIS_COUNT], b: [f64; AXIS_COUNT]) -> Self {
        Epsilon { a, b }
    }
    pub fn uniform(a: f64, b: f64) -> Self {
        Epsilon {
            a: [a; AXIS_COUNT],
            b: [b; AXIS_COUNT],
        }
    }
}

impl CostVector {
    /// ε-dominance: self dominates other after inflating self's allowance by ε on
    /// EACH axis independently. `other.v[i] <= self.v[i] + (a_i + b_i*self.v[i])`
    /// for all i, with at least one strict improvement (strict is measured pre-ε).
    #[inline]
    pub fn eps_dominates(&self, other: &CostVector, eps: &Epsilon) -> bool {
        let mut strict = false;
        for i in 0..AXIS_COUNT {
            let slack = eps.a[i] + eps.b[i] * self.v[i];
            if other.v[i] > self.v[i] + slack {
                return false;
            }
            if self.v[i] < other.v[i] {
                strict = true;
            }
        }
        strict
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dominance_is_componentwise_with_one_strict() {
        let a = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[100.0, 5.0]);
        let b = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[100.0, 8.0]);
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
        assert!(!a.dominates(&a));
        let c = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[90.0, 12.0]);
        assert!(!a.dominates(&c) && !c.dominates(&a));
    }

    #[test]
    fn addition_is_per_axis() {
        let a = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[10.0, 1.0]);
        let b = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[5.0, 2.0]);
        let s = a.added(&b);
        assert_eq!(s.get(Axis::Time), 15.0);
        assert_eq!(s.get(Axis::Dplus), 3.0);
    }

    #[test]
    fn project_keeps_only_listed_axes() {
        let mut c = CostVector::ZERO;
        c.set(Axis::Time, 10.0);
        c.set(Axis::Dplus, 5.0);
        c.set(Axis::Surface, 7.0);
        c.set(Axis::CyclewayDeficit, 3.0);
        c.set(Axis::Variance, 99.0);
        let p = c.project(&[Axis::Time, Axis::CyclewayDeficit, Axis::Dplus]);
        assert_eq!(p.get(Axis::Time), 10.0);
        assert_eq!(p.get(Axis::Dplus), 5.0);
        assert_eq!(p.get(Axis::CyclewayDeficit), 3.0);
        assert_eq!(p.get(Axis::Surface), 0.0, "demoted axis zeroed");
        assert_eq!(p.get(Axis::Variance), 0.0, "demoted axis zeroed");
    }

    #[test]
    fn epsilon_dominance_is_per_axis_additive() {
        let eps = Epsilon::uniform(1.0, 0.01);
        let a = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[100.0, 5.0]);
        let b = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[101.0, 5.0]);
        assert!(a.eps_dominates(&b, &eps));
        let c = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[103.0, 5.0]);
        assert!(!a.eps_dominates(&c, &eps));
    }

    #[test]
    fn epsilon_never_borrows_across_axes() {
        let eps = Epsilon::uniform(1.0, 0.01);
        let a = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[10.0, 6.0]);
        let b = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[100.0, 5.0]);
        assert!(
            !a.eps_dominates(&b, &eps),
            "huge Time lead must not excuse Dplus loss"
        );
        // A near-neighbour on Time (within ε) that is worse on Dplus *beyond* its
        // own ε must survive: each axis is judged only against its own ε.
        let near = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[100.0, 5.0]);
        let off_dplus = CostVector::from_active(&[Axis::Time, Axis::Dplus], &[101.0, 9.0]);
        assert!(
            !near.eps_dominates(&off_dplus, &eps),
            "Dplus worse by 4 (ε≈1.05) blocks pruning despite Time within ε"
        );
    }
}
