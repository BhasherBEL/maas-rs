//! Turns a `BikeProfile` + edge `BikeAttrs` into (a) a routing cost and (b) a
//! kinematic travel time. Cost drives route choice; time drives ETA + budget.

use crate::structures::{BikeAttrs, BikeProfile, HighwayClass, StreetEdgeData, Surface};

const G: f64 = 9.81; // gravity (m/s^2)
const RHO: f64 = 1.225; // air density (kg/m^3)
const IMPASSABLE: f64 = 1.0e7; // cost sentinel ≥ this ⇒ edge unusable

pub struct BikeCost {
    profile: BikeProfile,
}

impl BikeCost {
    pub fn new(profile: BikeProfile) -> Self {
        BikeCost { profile }
    }

    /// BRouter-style cost factor for an edge (before multiplying by length).
    fn cost_factor(&self, a: &BikeAttrs) -> f64 {
        let p = &self.profile;
        // Hard exclusions.
        if matches!(
            a.highway,
            HighwayClass::Motorway | HighwayClass::MotorwayLink | HighwayClass::Other
        ) {
            return IMPASSABLE;
        }
        if matches!(a.highway, HighwayClass::Steps) {
            return if p.allow_steps {
                p.steps_cost
            } else {
                IMPASSABLE
            };
        }
        let unpaved = matches!(a.surface, Surface::Unpaved);
        let is_ldcr = !p.ignore_cycleroutes && a.cycleroute;
        // Base: long-distance cycleroutes are perfect (1.0); else a small add.
        let mut cf = if is_ldcr {
            1.0
        } else {
            let base = if p.stick_to_cycleroutes { 0.5 } else { 0.05 };
            base + self.highway_part(a, unpaved)
        };
        // Avoid-unsafe surcharge on hintless ways.
        if p.avoid_unsafe && !a.isbike && self.is_road_class(a.highway) {
            cf += p.unsafe_penalty;
        }
        // Oneway + access penalties (max of the two, BRouter-style).
        let oneway = if a.wrong_way {
            self.oneway_penalty(a.highway)
        } else {
            0.0
        };
        let access = self.access_penalty(a);
        cf + oneway.max(access)
    }

    fn highway_part(&self, a: &BikeAttrs, unpaved: bool) -> f64 {
        use HighwayClass::*;
        match a.highway {
            Track | Road | Path | Footway => self.track_factor(a),
            _ => self.profile.highway_factor(a.highway, a.isbike, unpaved),
        }
    }

    /// Track-like ways graded by tracktype × surface quality (`probablyGood`).
    fn track_factor(&self, a: &BikeAttrs) -> f64 {
        let good = a.probably_good();
        match a.tracktype {
            1 => {
                if good {
                    1.0
                } else {
                    1.3
                }
            }
            2 => {
                if good {
                    1.1
                } else {
                    2.0
                }
            }
            3 => {
                if good {
                    1.5
                } else {
                    3.0
                }
            }
            4 => {
                if good {
                    2.0
                } else {
                    5.0
                }
            }
            5 => {
                if good {
                    3.0
                } else {
                    5.0
                }
            }
            _ => {
                if good {
                    1.0
                } else {
                    5.0
                }
            }
        }
    }

    fn is_road_class(&self, h: HighwayClass) -> bool {
        use HighwayClass::*;
        matches!(
            h,
            Trunk
                | TrunkLink
                | Primary
                | PrimaryLink
                | Secondary
                | SecondaryLink
                | Tertiary
                | TertiaryLink
                | Unclassified
        )
    }

    fn oneway_penalty(&self, h: HighwayClass) -> f64 {
        use HighwayClass::*;
        let p = &self.profile;
        match h {
            Primary | PrimaryLink => p.oneway_primary,
            Secondary | SecondaryLink => p.oneway_secondary,
            Tertiary | TertiaryLink => p.oneway_tertiary,
            _ => p.oneway_other,
        }
    }

    fn access_penalty(&self, a: &BikeAttrs) -> f64 {
        let p = &self.profile;
        if a.bikeaccess {
            0.0
        } else if a.footaccess {
            p.access_foot_only
        } else if a.cycleroute {
            p.access_cycleroute
        } else {
            p.access_forbidden
        }
    }

    /// Stateful, BRouter-style elevation cost. `buf` is the signed elevation
    /// accumulator carried along the path (meters); `delta`/`length` are this
    /// edge's elevation change and length. Returns `(added_cost, new_buf)`.
    ///
    /// Gentle grades (|gradient| ≤ cutoff) are treated as flat and don't
    /// accumulate. Otherwise the signed delta is added to `buf`; only the part
    /// of `|buf|` beyond `elevation_buffer_m` is charged (uphill/downhill cost
    /// per meter), then `buf` is clamped to ±buffer. This way an up-then-down
    /// wiggle inside the band nets to zero — only *sustained* net climbs and
    /// descents cost, unlike the old per-edge penalty.
    pub fn elevation_step(&self, buf: f64, delta: f64, length: f64) -> (f64, f64) {
        let p = &self.profile;
        if !p.consider_elevation || length <= 0.0 {
            return (0.0, buf);
        }
        let grade_pct = (delta / length) * 100.0;
        let cutoff = if delta >= 0.0 {
            p.uphillcutoff
        } else {
            p.downhillcutoff
        };
        if grade_pct.abs() <= cutoff {
            return (0.0, buf); // gentle slope ⇒ effectively flat
        }
        let mut b = buf + delta;
        let buffer = p.elevation_buffer_m;
        let cost = if b > buffer {
            let charged = (b - buffer) * p.uphillcost / 100.0;
            b = buffer;
            charged
        } else if b < -buffer {
            let charged = (-b - buffer) * p.downhillcost / 100.0;
            b = -buffer;
            charged
        } else {
            0.0
        };
        (cost, b)
    }

    /// Routing cost of an edge given the incoming direction (unit vector) for
    /// turn cost, or `None` for the first edge. Returns `None` if impassable.
    pub fn edge_cost(
        &self,
        e: &StreetEdgeData,
        incoming: Option<(f64, f64)>,
        this_dir: (f64, f64),
    ) -> Option<f64> {
        let cf = self.cost_factor(&e.attrs);
        if cf >= IMPASSABLE {
            return None;
        }
        let length = e.length as f64;
        // Elevation is NOT charged here — it is path-dependent (see `elevation_step`,
        // which the bike search threads through a hysteresis buffer). Charging it
        // per-edge would over-count every dip on rolling terrain.
        let mut cost = length * cf;
        if let Some(inc) = incoming {
            let dot = (inc.0 * this_dir.0 + inc.1 * this_dir.1).clamp(-1.0, 1.0);
            // turncost × (1 - cos θ) / 2  →  0 straight, turncost for 90°+.
            cost += self.profile.turncost * (1.0 - dot) / 2.0;
        }
        Some(cost)
    }

    /// Kinematic travel time (seconds) for an edge, solving the steady-state
    /// cyclist power equation for forward speed and capping at `max_speed`.
    pub fn edge_time(&self, e: &StreetEdgeData) -> u32 {
        let p = &self.profile;
        let length = e.length as f64;
        if length <= 0.0 {
            return 0;
        }
        let theta = (e.elev_delta as f64 / length).atan();
        let m = p.total_mass;
        let v_max = p.max_speed / 3.6; // km/h → m/s
        // Solve P = (C_r*m*g*cosθ + m*g*sinθ)*v + 0.5*ρ*S_C_x*v^3 for v in (0, v_max].
        let f_lin = p.c_r * m * G * theta.cos() + m * G * theta.sin();
        let c_cube = 0.5 * RHO * p.s_c_x;
        let power = |v: f64| f_lin * v + c_cube * v * v * v;
        let v = if power(v_max) <= p.biker_power {
            v_max
        } else {
            // Bisection on [eps, v_max]; power() is increasing for v>0 when f_lin>=0,
            // and still crosses biker_power once on descents within this range.
            let (mut lo, mut hi) = (0.01_f64, v_max);
            for _ in 0..40 {
                let mid = 0.5 * (lo + hi);
                if power(mid) < p.biker_power {
                    lo = mid
                } else {
                    hi = mid
                }
            }
            0.5 * (lo + hi)
        };
        (length / v.max(0.5)).round() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structures::NodeID;

    fn edge(attrs: BikeAttrs, length: usize, elev: i16) -> StreetEdgeData {
        StreetEdgeData {
            origin: NodeID(0),
            destination: NodeID(1),
            partial: false,
            length,
            foot: true,
            bike: true,
            car: false,
            attrs,
            elev_delta: elev,
        }
    }

    fn attrs(h: HighwayClass, isbike: bool, surface: Surface) -> BikeAttrs {
        let mut a = BikeAttrs::road_default();
        a.highway = h;
        a.isbike = isbike;
        a.surface = surface;
        a
    }

    #[test]
    fn cycleway_cheaper_than_unsafe_primary() {
        let bc = BikeCost::new(BikeProfile::default());
        let cyc = bc
            .edge_cost(
                &edge(attrs(HighwayClass::Cycleway, true, Surface::Paved), 100, 0),
                None,
                (1.0, 0.0),
            )
            .unwrap();
        let prim = bc
            .edge_cost(
                &edge(attrs(HighwayClass::Primary, false, Surface::Paved), 100, 0),
                None,
                (1.0, 0.0),
            )
            .unwrap();
        assert!(cyc < prim, "cycleway {cyc} should beat primary {prim}");
    }

    #[test]
    fn motorway_is_impassable() {
        let bc = BikeCost::new(BikeProfile::default());
        assert!(
            bc.edge_cost(
                &edge(attrs(HighwayClass::Motorway, false, Surface::Paved), 100, 0),
                None,
                (1.0, 0.0)
            )
            .is_none()
        );
    }

    #[test]
    fn steps_blocked_when_disallowed() {
        let mut prof = BikeProfile::default();
        prof.allow_steps = false;
        let bc = BikeCost::new(prof);
        assert!(
            bc.edge_cost(
                &edge(attrs(HighwayClass::Steps, false, Surface::Paved), 20, 0),
                None,
                (1.0, 0.0)
            )
            .is_none()
        );
    }

    #[test]
    fn edge_cost_excludes_elevation() {
        // Elevation is no longer charged per-edge; flat and steep edges of the
        // same class/length now cost the same from `edge_cost` alone.
        let bc = BikeCost::new(BikeProfile::default());
        let flat = bc
            .edge_cost(&edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 0), None, (1.0, 0.0))
            .unwrap();
        let steep = bc
            .edge_cost(&edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 10), None, (1.0, 0.0))
            .unwrap();
        assert_eq!(flat, steep);
    }

    #[test]
    fn elevation_buffer_charges_sustained_not_oscillation() {
        // defaults: downhillcost 100, downhillcutoff 0.5, elevation_buffer_m 5.
        let bc = BikeCost::new(BikeProfile::default());

        // Gentle slope (|grade| ≤ cutoff) is treated as flat: no cost, buffer kept.
        assert_eq!(bc.elevation_step(0.0, -0.4, 100.0), (0.0, 0.0));

        // Up-then-down wiggles within the 5 m band net to zero charge.
        let (mut buf, mut total) = (0.0_f64, 0.0_f64);
        for _ in 0..10 {
            let (c1, nb) = bc.elevation_step(buf, 2.0, 100.0);
            buf = nb;
            total += c1;
            let (c2, nb) = bc.elevation_step(buf, -2.0, 100.0);
            buf = nb;
            total += c2;
        }
        assert_eq!(total, 0.0, "oscillation within the buffer must not be charged");

        // A sustained 60 m descent (30 × −2 m) charges (60 − 5) × 100/100 = 55.
        let (mut buf, mut total) = (0.0_f64, 0.0_f64);
        for _ in 0..30 {
            let (c, nb) = bc.elevation_step(buf, -2.0, 100.0);
            buf = nb;
            total += c;
        }
        assert!((total - 55.0).abs() < 1e-6, "sustained descent cost {total}");

        // consider_elevation off ⇒ never charges.
        let mut prof = BikeProfile::default();
        prof.consider_elevation = false;
        let off = BikeCost::new(prof);
        assert_eq!(off.elevation_step(0.0, -50.0, 100.0), (0.0, 0.0));
    }

    #[test]
    fn kinematic_time_flat_is_reasonable() {
        let bc = BikeCost::new(BikeProfile::default());
        // 100 m flat: with 100 W and the default drag/rolling, ~5-6 m/s → ~17-20 s.
        let t = bc.edge_time(&edge(
            attrs(HighwayClass::Cycleway, true, Surface::Paved),
            100,
            0,
        ));
        assert!((10..=40).contains(&t), "flat 100m time {t}s out of range");
    }

    #[test]
    fn kinematic_time_uphill_slower_than_downhill() {
        let bc = BikeCost::new(BikeProfile::default());
        let up = bc.edge_time(&edge(
            attrs(HighwayClass::Tertiary, true, Surface::Paved),
            200,
            20,
        ));
        let down = bc.edge_time(&edge(
            attrs(HighwayClass::Tertiary, true, Surface::Paved),
            200,
            -20,
        ));
        assert!(up > down, "uphill {up}s should exceed downhill {down}s");
    }

    #[test]
    fn kinematic_time_capped_at_max_speed() {
        let bc = BikeCost::new(BikeProfile::default());
        // Steep descent: speed capped at max_speed (45 km/h = 12.5 m/s) → 1000m ≥ 80s.
        let t = bc.edge_time(&edge(
            attrs(HighwayClass::Secondary, true, Surface::Paved),
            1000,
            -200,
        ));
        assert!(t >= 80, "capped descent time {t}s too low");
    }
}
