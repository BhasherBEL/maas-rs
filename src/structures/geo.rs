use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct LatLng {
    pub latitude: f64,
    pub longitude: f64,
}

impl Display for LatLng {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, {}", self.latitude, self.longitude)
    }
}

impl LatLng {
    pub fn distance(loc1: &[f64], loc2: &[f64]) -> f64 {
        assert!(loc1.len() == 2);
        assert!(loc2.len() == 2);
        let delta_latitude = (loc1[0] - loc2[0]).to_radians();
        let delta_longitude = (loc1[1] - loc2[1]).to_radians();

        let central_angle_inner = (delta_latitude / 2.0).sin().powi(2)
            + loc1[0].to_radians().cos()
                * loc2[0].to_radians().cos()
                * (delta_longitude / 2.0).sin().powi(2);
        let central_angle = 2.0 * central_angle_inner.sqrt().asin();

        6365396.0_f64 * central_angle
    }

    pub fn dist(&self, other: Self) -> f64 {
        Self::distance(
            &[self.latitude, self.longitude],
            &[other.latitude, other.longitude],
        )
    }
}

pub fn meters_to_degrees(meters: f64) -> f64 {
    let deg = meters / 111_320.0;
    deg * deg
}

pub fn degrees_to_meters(sq_deg: f64, lat: f64) -> f64 {
    let deg = sq_deg.sqrt();
    deg * 111_320.0 * lat.to_radians().cos().max(0.5)
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    #[test]
    fn distance_same_point_is_zero() {
        let d = LatLng::distance(&[50.0, 4.0], &[50.0, 4.0]);
        assert!(d < EPSILON, "Expected 0, got {d}");
    }

    #[test]
    fn distance_brussels_to_amsterdam_approx_174km() {
        // Brussels (50.85, 4.35) → Amsterdam (52.37, 4.90) ≈ 174 km
        let d = LatLng::distance(&[50.85, 4.35], &[52.37, 4.90]);
        assert!((d - 174_000.0).abs() < 5_000.0, "Expected ~174km, got {d}");
    }

    #[test]
    fn distance_is_symmetric() {
        let d1 = LatLng::distance(&[50.85, 4.35], &[52.37, 4.90]);
        let d2 = LatLng::distance(&[52.37, 4.90], &[50.85, 4.35]);
        assert!((d1 - d2).abs() < EPSILON);
    }

    #[test]
    fn dist_method_matches_distance_fn() {
        let a = LatLng { latitude: 50.85, longitude: 4.35 };
        let b = LatLng { latitude: 52.37, longitude: 4.90 };
        let d1 = LatLng::distance(&[50.85, 4.35], &[52.37, 4.90]);
        let d2 = a.dist(b);
        assert!((d1 - d2).abs() < EPSILON);
    }

    #[test]
    fn dist_same_point_is_zero() {
        let loc = LatLng { latitude: 48.8566, longitude: 2.3522 };
        assert!(loc.dist(loc) < EPSILON);
    }

    #[test]
    fn meters_to_degrees_one_degree_roundtrip() {
        // 111_320 m ≈ 1 degree → sq_deg should be 1.0
        let sq = meters_to_degrees(111_320.0);
        assert!((sq - 1.0).abs() < 1e-6, "Expected 1.0 sq_deg, got {sq}");
    }

    #[test]
    fn degrees_to_meters_at_equator() {
        let m = degrees_to_meters(1.0, 0.0);
        assert!((m - 111_320.0).abs() < 1.0, "Expected ~111320m, got {m}");
    }

    #[test]
    fn meters_to_degrees_and_back_at_equator() {
        let original = 500.0_f64;
        let sq = meters_to_degrees(original);
        let back = degrees_to_meters(sq, 0.0);
        assert!((back - original).abs() < 1.0, "Roundtrip failed: {back} != {original}");
    }

    #[test]
    fn degrees_to_meters_clamps_at_high_latitude() {
        // At lat=80°, cos(80°) ≈ 0.174 < 0.5, so it should be clamped to 0.5
        let m_clamped = degrees_to_meters(1.0, 80.0);
        let m_at_min = degrees_to_meters(1.0, 60.0); // cos(60°) = 0.5, exactly at boundary
        // At lat=80° clamped to 0.5, result should equal lat=60°
        assert!((m_clamped - m_at_min).abs() < 1.0, "{m_clamped} != {m_at_min}");
    }

    #[test]
    fn latlng_display_format() {
        let loc = LatLng { latitude: 50.85, longitude: 4.35 };
        assert_eq!(format!("{loc}"), "50.85, 4.35");
    }
}
