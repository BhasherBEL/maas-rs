//! Turns a `BikeProfile` + edge `BikeAttrs` into (a) a routing cost and (b) a
//! kinematic travel time. Cost drives route choice; time drives ETA + budget.

use crate::structures::{BikeAttrs, BikeProfile, HighwayClass, StreetEdgeData, Surface};

const G: f64 = 9.81; // gravity (m/s^2)
const RHO: f64 = 1.225; // air density (kg/m^3)
const IMPASSABLE: f64 = 1.0e7; // cost sentinel ≥ this ⇒ edge unusable

pub struct BikeCost {
    profile: BikeProfile,
    /// Walking speed (m/s) used to time push (dismount) stretches, single-sourced
    /// from `RaptorIndex::walking_speed_mps` at construction.
    walk_speed_mps: f64,
}

impl BikeCost {
    pub fn new(profile: BikeProfile, walk_speed_mps: f64) -> Self {
        BikeCost {
            profile,
            walk_speed_mps,
        }
    }

    /// A push (dismount) edge: foot-accessible but not bike-accessible. Such ways
    /// are ridden on foot — pushed — so they are timed at walking speed.
    fn is_push(a: &BikeAttrs) -> bool {
        !a.bikeaccess && a.footaccess
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
        // Push (dismount) ways are impassable when the rider forbids dismounting.
        // Checked before the cycle-route shortcut so a push edge on a cycle route
        // is still blocked.
        if !p.allow_dismount && Self::is_push(a) {
            return IMPASSABLE;
        }
        let unpaved = matches!(a.surface, Surface::Unpaved);
        // On a cycle route BRouter sets costfactor = 1 flat ("magnetic"),
        // bypassing the baseline, highway value, unsafe surcharge and access
        // penalty. The wrong-way one-way penalty is the exception: cycle-route
        // membership does not license riding against a one-way, so it is still
        // charged on top. (Genuine cyclist exemptions — oneway:bicycle=no,
        // cycleway=opposite* — already clear `wrong_way` upstream at ingest.)
        if !p.ignore_cycleroutes && a.cycleroute {
            let oneway = if a.wrong_way {
                self.oneway_penalty(a.highway)
            } else {
                0.0
            };
            return 1.0 + oneway;
        }
        let base = if p.stick_to_cycleroutes { 0.5 } else { 0.05 };
        let mut cf = base + self.highway_part(a, unpaved);
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

    /// Faithful port of BRouter's `StdPath` elevation cost. Two one-sided
    /// hysteresis buffers carry along the path in meters: `ehbd` (descent) and
    /// `ehbu` (ascent). Per section of length `dist` and signed elevation change
    /// `delta`, each buffer fills net of a `cutoff·dist` allowance; the part above
    /// `elevation_max_buffer` (or bled by `elevation_buffer_reduce`) is charged at
    /// `downhillcost`/`uphillcost` **per meter** of elevation. Returns
    /// `(added_cost, ehbd, ehbu)`. Climbs are free when `uphillcost == 0`.
    pub fn elevation_step(&self, ehbd: f64, ehbu: f64, delta: f64, dist: f64) -> (f64, f64, f64) {
        let p = &self.profile;
        if !p.consider_elevation || dist <= 0.0 {
            return (0.0, ehbd, ehbu);
        }
        // cutoff is a percent grade; `dist * cutoff/100` is the per-section
        // elevation allowance that never enters the buffer.
        let mut ehbd = ehbd + (-delta) - dist * p.downhillcutoff / 100.0;
        let mut ehbu = ehbu + delta - dist * p.uphillcutoff / 100.0;
        let mut cost = 0.0;

        // Descent buffer.
        if ehbd > p.elevation_penalty_buffer {
            let excess = ehbd - p.elevation_penalty_buffer;
            let mut reduce = dist * p.elevation_buffer_reduce;
            if reduce > excess {
                reduce = excess;
            }
            let excess2 = ehbd - p.elevation_max_buffer;
            if reduce < excess2 {
                reduce = excess2; // force-drain everything above the ceiling
            }
            ehbd -= reduce;
            cost += reduce * p.downhillcost;
        } else if ehbd < 0.0 {
            ehbd = 0.0;
        }

        // Ascent buffer (symmetric).
        if ehbu > p.elevation_penalty_buffer {
            let excess = ehbu - p.elevation_penalty_buffer;
            let mut reduce = dist * p.elevation_buffer_reduce;
            if reduce > excess {
                reduce = excess;
            }
            let excess2 = ehbu - p.elevation_max_buffer;
            if reduce < excess2 {
                reduce = excess2;
            }
            ehbu -= reduce;
            cost += reduce * p.uphillcost;
        } else if ehbu < 0.0 {
            ehbu = 0.0;
        }

        (cost, ehbd, ehbu)
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
        // BRouter zeroes turncost on a cycle route ("magnetic" — no turn penalty
        // for staying on the route). Mirror that.
        let on_cycleroute = !self.profile.ignore_cycleroutes && e.attrs.cycleroute;
        if let Some(inc) = incoming
            && !on_cycleroute
        {
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
        // Push (dismount) stretches are walked, not ridden.
        if Self::is_push(&e.attrs) {
            return (length / self.walk_speed_mps.max(0.1)).round() as u32;
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
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
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
    fn wrong_way_penalized_even_on_cycleroute() {
        // A cycle-route edge gets the "magnetic" base, but riding it against a
        // one-way must still cost more than the legal direction.
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let mut right = attrs(HighwayClass::Tertiary, true, Surface::Paved);
        right.cycleroute = true;
        let mut wrong = right;
        wrong.wrong_way = true;
        let right_cost = bc
            .edge_cost(&edge(right, 100, 0), None, (1.0, 0.0))
            .unwrap();
        let wrong_cost = bc
            .edge_cost(&edge(wrong, 100, 0), None, (1.0, 0.0))
            .unwrap();
        assert!(
            wrong_cost > right_cost,
            "wrong-way cycleroute {wrong_cost} should exceed right-way {right_cost}"
        );
    }

    fn push_attrs() -> BikeAttrs {
        let mut a = attrs(HighwayClass::Footway, false, Surface::Paved);
        a.bikeaccess = false;
        a.footaccess = true;
        a
    }

    #[test]
    fn dismount_blocked_when_disallowed() {
        // A push edge is usable (at a penalty) by default, but impassable when the
        // rider forbids dismounting.
        let allow = BikeCost::new(BikeProfile::default(), 1.2);
        assert!(
            allow
                .edge_cost(&edge(push_attrs(), 50, 0), None, (1.0, 0.0))
                .is_some(),
            "push edge usable by default"
        );
        let mut prof = BikeProfile::default();
        prof.allow_dismount = false;
        let deny = BikeCost::new(prof, 1.2);
        assert!(
            deny.edge_cost(&edge(push_attrs(), 50, 0), None, (1.0, 0.0))
                .is_none(),
            "push edge impassable when dismount disallowed"
        );
    }

    #[test]
    fn dismount_disallowed_blocks_cycleroute_push() {
        // The block must apply even to a push edge that is also a cycle route
        // (i.e. before the magnetic cycle-route shortcut).
        let mut a = push_attrs();
        a.cycleroute = true;
        let mut prof = BikeProfile::default();
        prof.allow_dismount = false;
        let deny = BikeCost::new(prof, 1.2);
        assert!(
            deny.edge_cost(&edge(a, 50, 0), None, (1.0, 0.0)).is_none(),
            "cycleroute push edge still blocked when dismount disallowed"
        );
    }

    #[test]
    fn push_edge_timed_at_walk_speed() {
        // A push edge (foot-accessible, not bike-accessible) is timed at walking
        // speed, not the kinematic cycling model.
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let mut push = attrs(HighwayClass::Footway, false, Surface::Paved);
        push.bikeaccess = false;
        push.footaccess = true;
        let ride = attrs(HighwayClass::Cycleway, true, Surface::Paved);
        let t_push = bc.edge_time(&edge(push, 120, 0));
        let t_ride = bc.edge_time(&edge(ride, 120, 0));
        assert_eq!(t_push, (120.0_f64 / 1.2).round() as u32, "push at walk speed");
        assert!(t_push > t_ride, "push {t_push} slower than ride {t_ride}");
    }

    #[test]
    fn motorway_is_impassable() {
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
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
        let bc = BikeCost::new(prof, 1.2);
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
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        let flat = bc
            .edge_cost(&edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 0), None, (1.0, 0.0))
            .unwrap();
        let steep = bc
            .edge_cost(&edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 10), None, (1.0, 0.0))
            .unwrap();
        assert_eq!(flat, steep);
    }

    #[test]
    fn elevation_two_buffer_model() {
        // defaults: downhillcost 100, downhillcutoff 0.5, uphillcost 0,
        // uphillcutoff 1.5, penalty_buffer 5, max_buffer 10, buffer_reduce 0.
        let bc = BikeCost::new(BikeProfile::default(), 1.2);

        // consider_elevation off ⇒ identity (buffers untouched, no cost).
        let mut prof = BikeProfile::default();
        prof.consider_elevation = false;
        assert_eq!(
            BikeCost::new(prof, 1.2).elevation_step(3.0, 3.0, -50.0, 100.0),
            (0.0, 3.0, 3.0)
        );

        // A 20 m descent over 100 m: ehbd = 20 − 100·0.5/100 = 19.5; charged part is
        // (19.5 − max_buffer 10) = 9.5 m at downhillcost 100/m ⇒ 950 (NOT /100).
        let (c, ehbd, _) = bc.elevation_step(0.0, 0.0, -20.0, 100.0);
        assert!((c - 950.0).abs() < 1e-6, "descent cost {c}");
        assert!((ehbd - 10.0).abs() < 1e-6, "descent buffer drained to ceiling");

        // A 20 m climb is free in cost (uphillcost = 0) though the ascent buffer fills.
        let (c2, _, ehbu) = bc.elevation_step(0.0, 0.0, 20.0, 100.0);
        assert_eq!(c2, 0.0, "climbs are free when uphillcost = 0");
        assert!(ehbu > 5.0);

        // A gentle descent within the cutoff allowance is absorbed: no cost, buffer ~0.
        let (c3, ehbd3, _) = bc.elevation_step(0.0, 0.0, -0.4, 100.0);
        assert_eq!(c3, 0.0);
        assert_eq!(ehbd3, 0.0);
    }

    #[test]
    fn kinematic_time_flat_is_reasonable() {
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
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
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
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
        let bc = BikeCost::new(BikeProfile::default(), 1.2);
        // Steep descent: speed capped at max_speed (45 km/h = 12.5 m/s) → 1000m ≥ 80s.
        let t = bc.edge_time(&edge(
            attrs(HighwayClass::Secondary, true, Surface::Paved),
            1000,
            -200,
        ));
        assert!(t >= 80, "capped descent time {t}s too low");
    }
}
