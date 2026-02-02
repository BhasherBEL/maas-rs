use std::fmt::Display;

#[derive(Debug, Copy, Clone)]
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

        return 6365396.0_f64 * central_angle;
    }

    pub fn dist(&self, other: Self) -> f64 {
        let delta_latitude = (self.latitude - other.latitude).to_radians();
        let delta_longitude = (self.longitude - other.longitude).to_radians();

        let central_angle_inner = (delta_latitude / 2.0).sin().powi(2)
            + self.latitude.to_radians().cos()
                * other.latitude.to_radians().cos()
                * (delta_longitude / 2.0).sin().powi(2);
        let central_angle = 2.0 * central_angle_inner.sqrt().asin();

        return 6365396.0_f64 * central_angle;
    }
}
