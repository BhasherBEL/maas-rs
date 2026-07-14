use crate::structures::{BikeAttrs, BikeProfile, HighwayClass, StreetEdgeData, Surface};

const G: f64 = 9.81;
const RHO: f64 = 1.225;
const IMPASSABLE: f64 = 1.0e7;

#[derive(Debug, Clone, Copy)]
pub struct PrevCtx {
    pub dir: (f64, f64),
    pub len: f64,
    pub cruise: f64,
    pub push: bool,
    pub speed: f64,
}

pub struct BikeCost {
    profile: BikeProfile,
}

impl BikeCost {
    pub fn new(profile: BikeProfile) -> Self {
        BikeCost { profile }
    }

    pub fn profile(&self) -> crate::structures::BikeProfile {
        self.profile
    }

    pub(crate) fn is_push(a: &BikeAttrs) -> bool {
        !a.bikeaccess && a.footaccess
    }

    fn cost_factor(&self, a: &BikeAttrs) -> f64 {
        let p = &self.profile;
        if matches!(
            a.highway,
            HighwayClass::Motorway | HighwayClass::MotorwayLink | HighwayClass::Other
        ) {
            return IMPASSABLE;
        }
        if !a.bikeaccess && !a.footaccess {
            return IMPASSABLE;
        }
        if matches!(a.highway, HighwayClass::Steps) {
            return if p.allow_steps {
                p.steps_cost
            } else {
                IMPASSABLE
            };
        }
        // Checked before the cycle-route shortcut so a push edge on a cycle route
        // is still blocked when dismount is forbidden.
        if !p.allow_dismount && Self::is_push(a) {
            return IMPASSABLE;
        }
        if p.respect_oneway && a.wrong_way {
            return IMPASSABLE;
        }
        let unpaved = matches!(a.surface, Surface::Unpaved);
        // Cycle route: flat cf=1, bypassing baseline/highway/unsafe/access; the
        // wrong-way penalty is still charged on top.
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
        if p.avoid_unsafe && !a.isbike && self.is_road_class(a.highway) {
            cf += p.unsafe_penalty;
        }
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

    /// Port of BRouter's `StdPath` elevation cost. `ehbd`/`ehbu` are path-carried
    /// descent/ascent hysteresis buffers (meters).
    pub fn elevation_step(&self, ehbd: f64, ehbu: f64, delta: f64, dist: f64) -> (f64, f64, f64) {
        let p = &self.profile;
        if !p.consider_elevation || dist <= 0.0 {
            return (0.0, ehbd, ehbu);
        }
        let mut ehbd = ehbd + (-delta) - dist * p.downhillcutoff / 100.0;
        let mut ehbu = ehbu + delta - dist * p.uphillcutoff / 100.0;
        let mut cost = 0.0;

        if ehbd > p.elevation_penalty_buffer {
            let excess = ehbd - p.elevation_penalty_buffer;
            let mut reduce = dist * p.elevation_buffer_reduce;
            if reduce > excess {
                reduce = excess;
            }
            let excess2 = ehbd - p.elevation_max_buffer;
            if reduce < excess2 {
                reduce = excess2;
            }
            ehbd -= reduce;
            cost += reduce * p.downhillcost;
        } else if ehbd < 0.0 {
            ehbd = 0.0;
        }

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

    /// Denoised per-edge ascent (metres) for the walk D+ axis, via the
    /// `elevation_penalty_buffer` hysteresis. No grade cutoff (would erase gentle climbs).
    pub fn walk_ascent_step(&self, ehbu: f64, delta: f64, dist: f64) -> (f64, f64) {
        if dist <= 0.0 {
            return (0.0, ehbu);
        }
        let mut ehbu = ehbu + delta;
        let mut charged = 0.0;
        if ehbu > self.profile.elevation_penalty_buffer {
            charged = ehbu - self.profile.elevation_penalty_buffer;
            ehbu = self.profile.elevation_penalty_buffer;
        } else if ehbu < 0.0 {
            ehbu = 0.0;
        }
        (charged, ehbu)
    }

    /// Routing cost of an edge; `None` if impassable.
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
        // Elevation is NOT charged here: it is path-dependent (`elevation_step` threads
        // a hysteresis buffer); per-edge charging would over-count dips.
        let mut cost = length * cf;
        let on_cycleroute = !self.profile.ignore_cycleroutes && e.attrs.cycleroute;
        if let Some(inc) = incoming
            && !on_cycleroute
        {
            let dot = (inc.0 * this_dir.0 + inc.1 * this_dir.1).clamp(-1.0, 1.0);
            cost += self.profile.turncost * (1.0 - dot) / 2.0;
        }
        Some(cost)
    }

    /// `0` means unset (old cache / non-bike edge) and resolves to the unknown default.
    fn surface_factor(e: &StreetEdgeData) -> f64 {
        if e.surface_speed == 0 {
            crate::structures::UNKNOWN_SURFACE_FACTOR
        } else {
            e.surface_speed as f64 / 100.0
        }
    }

    /// Cruise speed (m/s): grade power-solve, then surface factor (AFTER the solve).
    pub fn cruise_speed(&self, e: &StreetEdgeData) -> f64 {
        (self.cruise_speed_geom(e) * Self::surface_factor(e)).max(0.5)
    }

    /// Cruise speed from the grade power-solve ONLY (before surface factor): the
    /// reference for the corner/stop model, so surface changes don't fake a brake/accel.
    pub fn cruise_speed_geom(&self, e: &StreetEdgeData) -> f64 {
        let p = &self.profile;
        let length = e.length as f64;
        let theta = if length > 0.0 {
            (e.elev_delta as f64 / length).atan()
        } else {
            0.0
        };
        let m = p.total_mass;
        let v_max = p.max_speed / 3.6;
        let f_lin = p.c_r * m * G * theta.cos() + m * G * theta.sin();
        let c_cube = 0.5 * RHO * p.s_c_x;
        let power = |v: f64| f_lin * v + c_cube * v * v * v;
        let v = if power(v_max) <= p.biker_power {
            v_max
        } else {
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
        v.max(0.5)
    }

    fn push_speed(&self, a: &BikeAttrs) -> f64 {
        if matches!(a.highway, HighwayClass::Steps) {
            self.profile.steps_push_speed_mps
        } else {
            self.profile.push_speed_mps
        }
        .max(0.1)
    }

    pub fn accel_secs(&self, v0: f64, v1: f64) -> f64 {
        if v1 <= v0 {
            return 0.0;
        }
        (v1 - v0) / self.profile.accel_rate.max(0.1)
    }

    pub fn decel_secs(&self, v0: f64, v1: f64) -> f64 {
        if v0 <= v1 {
            return 0.0;
        }
        (v0 - v1) / self.profile.brake_decel.max(0.1)
    }

    /// Carried (exit) speed (m/s) for `this` edge: push ⇒ 0, a ride turn ⇒
    /// `v_turn = sqrt(lateral_accel · r)` capped at cruise (`r = min(L_prev,L_this)/θ`),
    /// else cruise. A curve is held at its safe speed, not braked per segment.
    pub fn required_speed(
        &self,
        prev: Option<PrevCtx>,
        this: &StreetEdgeData,
        this_dir: (f64, f64),
    ) -> f64 {
        if Self::is_push(&this.attrs) {
            return 0.0;
        }
        let v_c = self.cruise_speed_geom(this);
        let Some(prev) = prev else {
            return v_c;
        };
        if prev.push {
            return v_c;
        }
        let dot = (prev.dir.0 * this_dir.0 + prev.dir.1 * this_dir.1).clamp(-1.0, 1.0);
        let theta = dot.acos();
        let min_len = prev.len.min(this.length as f64);
        if theta <= 1e-6 || min_len <= 0.0 {
            return v_c;
        }
        let on_infra = this.attrs.cycleroute
            || this.attrs.isbike
            || matches!(this.attrs.highway, HighwayClass::Cycleway);
        let lat = if on_infra {
            self.profile.lateral_accel_infra
        } else {
            self.profile.lateral_accel
        };
        let r = min_len.max(self.profile.corner_min_len_m) / theta;
        (lat.max(0.0) * r).sqrt().min(v_c)
    }

    /// Lost time (s) into `this` edge: the accel/decel between the carried speed and
    /// `required_speed`. Every term is >= 0 and it is 0 with no previous edge, so the
    /// A* Time heuristic stays admissible.
    pub fn speed_change_secs(
        &self,
        prev: Option<PrevCtx>,
        this: &StreetEdgeData,
        this_dir: (f64, f64),
    ) -> f64 {
        let Some(prev) = prev else {
            return 0.0;
        };
        let carried = prev.speed;
        let required = self.required_speed(Some(prev), this, this_dir);
        if required < carried {
            self.decel_secs(carried, required)
        } else {
            self.accel_secs(carried, required)
        }
    }

    /// Kinematic travel time (seconds) at cruise (ride) or push speed. The per-vertex
    /// speed-change cost is added separately via `speed_change_secs`.
    pub fn edge_time(&self, e: &StreetEdgeData) -> u32 {
        let length = e.length as f64;
        if length <= 0.0 {
            return 0;
        }
        if Self::is_push(&e.attrs) {
            return (length / self.push_speed(&e.attrs)).round() as u32;
        }
        (length / self.cruise_speed(e)).round() as u32
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
            surface_speed: 100,
            var_gen: crate::structures::cost::VarGen::NONE,
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
    fn walk_ascent_step_absorbs_noise_bumps() {
        let bc = BikeCost::new(BikeProfile::default());
        let (a1, b1) = bc.walk_ascent_step(0.0, 2.0, 50.0);
        let (a2, _b2) = bc.walk_ascent_step(b1, -2.0, 50.0);
        assert_eq!(a1, 0.0, "a 2 m bump is below the 5 m buffer → no ascent");
        assert_eq!(a2, 0.0, "the matching dip charges nothing");
    }

    #[test]
    fn walk_ascent_step_counts_sustained_climb() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut ehbu = 0.0;
        let mut total = 0.0;
        for _ in 0..6 {
            let (a, b) = bc.walk_ascent_step(ehbu, 5.0, 100.0);
            total += a;
            ehbu = b;
        }
        assert!(
            total > 14.0 && total < 30.0,
            "sustained 30 m climb still registers real ascent (minus cutoff+buffer), got {total}"
        );
    }

    #[test]
    fn walk_ascent_step_zero_params_is_raw_max0() {
        let mut prof = BikeProfile::default();
        prof.uphillcutoff = 0.0;
        prof.elevation_penalty_buffer = 0.0;
        let bc = BikeCost::new(prof);
        assert_eq!(bc.walk_ascent_step(0.0, 7.0, 100.0).0, 7.0);
        assert_eq!(bc.walk_ascent_step(0.0, -7.0, 100.0).0, 0.0);
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
    fn wrong_way_impassable_by_default() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut a = attrs(HighwayClass::Tertiary, true, Surface::Paved);
        a.cycleroute = true;
        a.wrong_way = true;
        assert!(
            bc.edge_cost(&edge(a, 100, 0), None, (1.0, 0.0)).is_none(),
            "non-exempt wrong-way must be impassable by default"
        );
    }

    #[test]
    fn wrong_way_penalized_even_on_cycleroute() {
        let mut prof = BikeProfile::default();
        prof.respect_oneway = false;
        let bc = BikeCost::new(prof);
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
        let allow = BikeCost::new(BikeProfile::default());
        assert!(
            allow
                .edge_cost(&edge(push_attrs(), 50, 0), None, (1.0, 0.0))
                .is_some(),
            "push edge usable by default"
        );
        let mut prof = BikeProfile::default();
        prof.allow_dismount = false;
        let deny = BikeCost::new(prof);
        assert!(
            deny.edge_cost(&edge(push_attrs(), 50, 0), None, (1.0, 0.0))
                .is_none(),
            "push edge impassable when dismount disallowed"
        );
    }

    #[test]
    fn dismount_disallowed_blocks_cycleroute_push() {
        let mut a = push_attrs();
        a.cycleroute = true;
        let mut prof = BikeProfile::default();
        prof.allow_dismount = false;
        let deny = BikeCost::new(prof);
        assert!(
            deny.edge_cost(&edge(a, 50, 0), None, (1.0, 0.0)).is_none(),
            "cycleroute push edge still blocked when dismount disallowed"
        );
    }

    #[test]
    fn push_edge_timed_at_push_speed() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut push = attrs(HighwayClass::Footway, false, Surface::Paved);
        push.bikeaccess = false;
        push.footaccess = true;
        let ride = attrs(HighwayClass::Cycleway, true, Surface::Paved);
        let t_push = bc.edge_time(&edge(push, 120, 0));
        let t_ride = bc.edge_time(&edge(ride, 120, 0));
        assert_eq!(
            t_push,
            (120.0_f64 / BikeProfile::default().push_speed_mps).round() as u32,
            "push at push speed (0.9 m/s default)"
        );
        assert!(t_push > t_ride, "push {t_push} slower than ride {t_ride}");
    }

    #[test]
    fn steps_push_is_slower_than_flat_push() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut flat = attrs(HighwayClass::Footway, false, Surface::Paved);
        flat.bikeaccess = false;
        flat.footaccess = true;
        let mut steps = attrs(HighwayClass::Steps, false, Surface::Paved);
        steps.bikeaccess = false;
        steps.footaccess = true;
        let t_flat = bc.edge_time(&edge(flat, 20, 0));
        let t_steps = bc.edge_time(&edge(steps, 20, 0));
        assert_eq!(t_steps, (20.0_f64 / 0.25).round() as u32, "steps at 0.25 m/s");
        assert!(t_steps > t_flat, "steps push {t_steps} slower than flat {t_flat}");
    }

    #[test]
    fn accel_decel_helpers_monotone_and_closed_form() {
        let bc = BikeCost::new(BikeProfile::default());
        assert_eq!(bc.accel_secs(5.0, 5.0), 0.0);
        assert_eq!(bc.accel_secs(8.0, 5.0), 0.0);
        assert_eq!(bc.decel_secs(5.0, 5.0), 0.0);
        assert_eq!(bc.decel_secs(3.0, 5.0), 0.0);
        assert!(bc.accel_secs(0.0, 6.0) > bc.accel_secs(0.0, 4.0));
        assert!(bc.decel_secs(6.0, 0.0) > bc.decel_secs(4.0, 0.0));
        let p = BikeProfile::default();
        let a = 6.0 / p.accel_rate;
        assert!((bc.accel_secs(0.0, 6.0) - a).abs() < 1e-9);
        assert!((bc.decel_secs(6.0, 0.0) - 6.0 / p.brake_decel).abs() < 1e-9);
    }

    fn ride_edge(len: usize) -> StreetEdgeData {
        edge(attrs(HighwayClass::Tertiary, false, Surface::Paved), len, 0)
    }

    #[test]
    fn corner_tight_short_costs_long_sweep_free() {
        let bc = BikeCost::new(BikeProfile::default());
        let this = ride_edge(8);
        let v_c = bc.cruise_speed(&this);
        let short_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 8.0,
            cruise: v_c,
            push: false,
            speed: v_c,
        };
        let right = (0.0, 1.0);
        let c_short = bc.speed_change_secs(Some(short_prev), &this, right);
        assert!(c_short > 0.0, "a tight 90° corner over short edges costs time");
        let long_this = ride_edge(400);
        let long_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 400.0,
            cruise: bc.cruise_speed(&long_this),
            push: false,
            speed: bc.cruise_speed(&long_this),
        };
        let c_long = bc.speed_change_secs(Some(long_prev), &long_this, right);
        assert_eq!(c_long, 0.0, "a 90° spread over long edges needs no slow-down");
    }

    #[test]
    fn infra_corner_is_near_free_unlike_road() {
        let bc = BikeCost::new(BikeProfile::default());
        let road = ride_edge(8);
        let infra = edge(attrs(HighwayClass::Cycleway, true, Surface::Paved), 8, 0);
        let right = (0.0, 1.0);
        let prev = |e: &StreetEdgeData| PrevCtx {
            dir: (1.0, 0.0),
            len: 8.0,
            cruise: bc.cruise_speed(e),
            push: false,
            speed: bc.cruise_speed(e),
        };
        let c_road = bc.speed_change_secs(Some(prev(&road)), &road, right);
        let c_infra = bc.speed_change_secs(Some(prev(&infra)), &infra, right);
        assert!(c_road > 0.0, "a tight road corner costs time");
        assert_eq!(c_infra, 0.0, "the same corner on a cycleway is near-free");
    }

    #[test]
    fn corner_gentle_bend_is_free_and_grows_as_radius_shrinks() {
        let bc = BikeCost::new(BikeProfile::default());
        let this = ride_edge(20);
        let v_c = bc.cruise_speed(&this);
        let prev = |len: f64| PrevCtx {
            dir: (1.0, 0.0),
            len,
            cruise: v_c,
            push: false,
            speed: v_c,
        };
        let gentle = (10f64.to_radians().cos(), 10f64.to_radians().sin());
        assert_eq!(bc.speed_change_secs(Some(prev(20.0)), &this, gentle), 0.0);
        let right = (0.0, 1.0);
        let c40 = {
            let e = ride_edge(40);
            bc.speed_change_secs(
                Some(PrevCtx {
                    dir: (1.0, 0.0),
                    len: 40.0,
                    cruise: bc.cruise_speed(&e),
                    push: false,
                    speed: bc.cruise_speed(&e),
                }),
                &e,
                right,
            )
        };
        let c10 = {
            let e = ride_edge(10);
            bc.speed_change_secs(
                Some(PrevCtx {
                    dir: (1.0, 0.0),
                    len: 10.0,
                    cruise: bc.cruise_speed(&e),
                    push: false,
                    speed: bc.cruise_speed(&e),
                }),
                &e,
                right,
            )
        };
        assert!(c10 > c40, "tighter radius (shorter edges) costs more: {c10} > {c40}");
    }

    fn chain_speed_change(
        bc: &BikeCost,
        seg_len: usize,
        turn_each: (f64, f64),
        n: usize,
    ) -> f64 {
        let e = ride_edge(seg_len);
        let v_c = bc.cruise_speed(&e);
        let mut prev = Some(PrevCtx {
            dir: (1.0, 0.0),
            len: seg_len as f64,
            cruise: v_c,
            push: false,
            speed: v_c,
        });
        let mut dir = (1.0, 0.0);
        let mut total = 0.0;
        for _ in 0..n {
            let (c, s) = turn_each;
            let ndir = (dir.0 * c - dir.1 * s, dir.0 * s + dir.1 * c);
            total += bc.speed_change_secs(prev, &e, ndir);
            let exit = bc.required_speed(prev, &e, ndir);
            prev = Some(PrevCtx {
                dir: ndir,
                len: seg_len as f64,
                cruise: v_c,
                push: false,
                speed: exit,
            });
            dir = ndir;
        }
        total
    }

    #[test]
    fn sustained_curve_charges_one_decel_not_per_segment() {
        let bc = BikeCost::new(BikeProfile::default());
        let seg = 8;
        let deg = 90f64;
        let turn = (deg.to_radians().cos(), deg.to_radians().sin());
        let e = ride_edge(seg);
        let v_c = bc.cruise_speed(&e);
        let p = BikeProfile::default();
        let r = (seg as f64).max(p.corner_min_len_m) / deg.to_radians();
        let v_turn = (p.lateral_accel * r).sqrt().min(v_c);
        let one_decel = bc.decel_secs(v_c, v_turn);
        assert!(one_decel > 0.0, "the bend is tight enough to require slowing");

        let total = chain_speed_change(&bc, seg, turn, 6);
        assert!(
            (total - one_decel).abs() < 1e-6,
            "6-segment same-radius curve costs one decel-in ({one_decel}), got {total}"
        );
        let six_full = 6.0 * (bc.decel_secs(v_c, v_turn) + bc.accel_secs(v_turn, v_c));
        assert!(
            total < six_full / 3.0,
            "sustained curve {total} ≪ 6× full corner {six_full}"
        );
    }

    #[test]
    fn isolated_corner_brakes_in_then_accelerates_out_once() {
        let bc = BikeCost::new(BikeProfile::default());
        let e = ride_edge(8);
        let v_c = bc.cruise_speed(&e);
        let right = (0.0, 1.0);
        let straight_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 8.0,
            cruise: v_c,
            push: false,
            speed: v_c,
        };
        let into = bc.speed_change_secs(Some(straight_prev), &e, right);
        let v_turn = bc.required_speed(Some(straight_prev), &e, right);
        let bend_prev = PrevCtx {
            dir: right,
            len: 8.0,
            cruise: v_c,
            push: false,
            speed: v_turn,
        };
        let out = bc.speed_change_secs(Some(bend_prev), &e, right);
        let single_corner = bc.decel_secs(v_c, v_turn) + bc.accel_secs(v_turn, v_c);
        assert!(
            (into + out - single_corner).abs() < 1e-6,
            "brake-in {into} + accel-out {out} = one classic corner {single_corner}"
        );
    }

    #[test]
    fn dismount_charges_stop_and_restart_once_per_boundary() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut push = attrs(HighwayClass::Footway, false, Surface::Paved);
        push.bikeaccess = false;
        push.footaccess = true;
        let push_edge = edge(push, 30, 0);
        let ride = ride_edge(30);
        let v_c = bc.cruise_speed(&ride);
        let ride_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 30.0,
            cruise: v_c,
            push: false,
            speed: v_c,
        };
        let push_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 30.0,
            cruise: v_c,
            push: true,
            speed: 0.0,
        };
        let stop = bc.speed_change_secs(Some(ride_prev), &push_edge, (1.0, 0.0));
        assert!((stop - bc.decel_secs(v_c, 0.0)).abs() < 1e-9, "stop = decel to 0");
        let interior = bc.speed_change_secs(Some(push_prev), &push_edge, (1.0, 0.0));
        assert_eq!(interior, 0.0, "no extra stop between two consecutive pushes");
        let restart = bc.speed_change_secs(Some(push_prev), &ride, (1.0, 0.0));
        assert!(
            (restart - bc.accel_secs(0.0, v_c)).abs() < 1e-9,
            "restart = accel from 0 to cruise"
        );
    }

    #[test]
    fn micro_dismount_costs_more_than_it_saves() {
        let bc = BikeCost::new(BikeProfile::default());
        let ride = ride_edge(30);
        let v_c = bc.cruise_speed(&ride);
        let mut push = attrs(HighwayClass::Footway, false, Surface::Paved);
        push.bikeaccess = false;
        push.footaccess = true;
        let push_edge = edge(push, 3, 0);
        let ride_prev = PrevCtx {
            dir: (1.0, 0.0),
            len: 30.0,
            cruise: v_c,
            push: false,
            speed: v_c,
        };
        let stop = bc.speed_change_secs(Some(ride_prev), &push_edge, (1.0, 0.0));
        let restart = bc.accel_secs(0.0, v_c);
        let push_time = bc.edge_time(&push_edge) as f64;
        assert!(
            stop + restart > 1.0,
            "a dismount's stop+restart is several seconds, dwarfing a 3 m shortcut: {}",
            stop + restart + push_time
        );
    }

    #[test]
    fn speed_change_zero_without_incoming() {
        let bc = BikeCost::new(BikeProfile::default());
        assert_eq!(bc.speed_change_secs(None, &ride_edge(50), (0.0, 1.0)), 0.0);
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
    fn no_bike_and_no_foot_access_is_impassable() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut a = attrs(HighwayClass::Primary, false, Surface::Paved);
        a.bikeaccess = false;
        a.footaccess = false;
        assert!(bc.edge_cost(&edge(a, 100, 0), None, (1.0, 0.0)).is_none());
        a.footaccess = true;
        assert!(bc.edge_cost(&edge(a, 100, 0), None, (1.0, 0.0)).is_some());
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
        let bc = BikeCost::new(BikeProfile::default());
        let flat = bc
            .edge_cost(
                &edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 0),
                None,
                (1.0, 0.0),
            )
            .unwrap();
        let steep = bc
            .edge_cost(
                &edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), 100, 10),
                None,
                (1.0, 0.0),
            )
            .unwrap();
        assert_eq!(flat, steep);
    }

    #[test]
    fn elevation_two_buffer_model() {
        let bc = BikeCost::new(BikeProfile::default());

        let mut prof = BikeProfile::default();
        prof.consider_elevation = false;
        assert_eq!(
            BikeCost::new(prof).elevation_step(3.0, 3.0, -50.0, 100.0),
            (0.0, 3.0, 3.0)
        );

        let (c, ehbd, _) = bc.elevation_step(0.0, 0.0, -20.0, 100.0);
        assert!((c - 950.0).abs() < 1e-6, "descent cost {c}");
        assert!(
            (ehbd - 10.0).abs() < 1e-6,
            "descent buffer drained to ceiling"
        );

        let (c2, _, ehbu) = bc.elevation_step(0.0, 0.0, 20.0, 100.0);
        assert_eq!(c2, 0.0, "climbs are free when uphillcost = 0");
        assert!(ehbu > 5.0);

        let (c3, ehbd3, _) = bc.elevation_step(0.0, 0.0, -0.4, 100.0);
        assert_eq!(c3, 0.0);
        assert_eq!(ehbd3, 0.0);
    }

    #[test]
    fn kinematic_time_flat_is_reasonable() {
        let bc = BikeCost::new(BikeProfile::default());
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
        let t = bc.edge_time(&edge(
            attrs(HighwayClass::Secondary, true, Surface::Paved),
            1000,
            -200,
        ));
        assert!(t >= 80, "capped descent time {t}s too low");
    }

    fn surf_edge(length: usize, surface_speed: u8) -> StreetEdgeData {
        let mut e = edge(attrs(HighwayClass::Tertiary, true, Surface::Paved), length, 0);
        e.surface_speed = surface_speed;
        e
    }

    #[test]
    fn surface_factor_slows_ride_time_by_the_factor() {
        let bc = BikeCost::new(BikeProfile::default());
        let t_asphalt = bc.edge_time(&surf_edge(300, 100)) as f64;
        let t_gravel = bc.edge_time(&surf_edge(300, 60)) as f64;
        let t_mud = bc.edge_time(&surf_edge(300, 20)) as f64;
        assert!(
            t_asphalt < t_gravel && t_gravel < t_mud,
            "asphalt {t_asphalt} < gravel {t_gravel} < mud {t_mud}"
        );
        assert!(
            (t_gravel / t_asphalt - 100.0 / 60.0).abs() < 0.05,
            "gravel/asphalt ratio {} ≈ 1.667",
            t_gravel / t_asphalt
        );
        assert!(
            (t_mud / t_asphalt - 100.0 / 20.0).abs() < 0.1,
            "mud/asphalt ratio {} ≈ 5.0",
            t_mud / t_asphalt
        );
    }

    #[test]
    fn cruise_speed_scales_with_surface_factor() {
        let bc = BikeCost::new(BikeProfile::default());
        let v_asphalt = bc.cruise_speed(&surf_edge(300, 100));
        let v_gravel = bc.cruise_speed(&surf_edge(300, 60));
        assert!(
            (v_gravel - 0.60 * v_asphalt).abs() < 1e-6,
            "gravel cruise {v_gravel} = 0.60 × asphalt {v_asphalt}"
        );
    }

    #[test]
    fn surface_factor_feeds_corner_speed() {
        let bc = BikeCost::new(BikeProfile::default());
        let right = (0.0, 1.0);
        let mk_prev = |e: &StreetEdgeData| PrevCtx {
            dir: (1.0, 0.0),
            len: e.length as f64,
            cruise: bc.cruise_speed(e),
            push: false,
            speed: bc.cruise_speed(e),
        };
        let asphalt = surf_edge(8, 100);
        let gravel = surf_edge(8, 60);
        let c_asphalt = bc.speed_change_secs(Some(mk_prev(&asphalt)), &asphalt, right);
        let c_gravel = bc.speed_change_secs(Some(mk_prev(&gravel)), &gravel, right);
        assert_ne!(
            c_asphalt, c_gravel,
            "corner cost must use the surface-scaled cruise, not the asphalt cruise"
        );
    }

    #[test]
    fn unset_surface_speed_reads_unknown_default() {
        let bc = BikeCost::new(BikeProfile::default());
        let v_unset = bc.cruise_speed(&surf_edge(300, 0));
        let v_asphalt = bc.cruise_speed(&surf_edge(300, 100));
        assert!(
            (v_unset - crate::structures::UNKNOWN_SURFACE_FACTOR * v_asphalt).abs() < 1e-6,
            "unset surface_speed cruises at 0.90 × asphalt"
        );
    }

    #[test]
    fn push_edge_ignores_surface_factor() {
        let bc = BikeCost::new(BikeProfile::default());
        let mut push = attrs(HighwayClass::Footway, false, Surface::Paved);
        push.bikeaccess = false;
        push.footaccess = true;
        let mut mud_push = edge(push, 120, 0);
        mud_push.surface_speed = 20;
        let mut asphalt_push = edge(push, 120, 0);
        asphalt_push.surface_speed = 100;
        assert_eq!(
            bc.edge_time(&mud_push),
            bc.edge_time(&asphalt_push),
            "push time is surface-independent (foot speed)"
        );
    }
}
