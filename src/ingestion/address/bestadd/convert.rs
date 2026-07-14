use proj4rs::Proj;

const LAMBERT72_PROJ: &str = "+proj=lcc +lat_0=90 +lon_0=4.36748666666667 \
+lat_1=51.1666672333333 +lat_2=49.8333339 +x_0=150000.013 +y_0=5400088.438 \
+ellps=intl +towgs84=-106.8686,52.2978,-103.7239,0.3366,-0.457,1.8422,-1.2747 \
+units=m +no_defs +type=crs";

const WGS84_PROJ: &str = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs";

pub struct Lambert72Converter {
    lambert: Proj,
    wgs84: Proj,
}

impl Lambert72Converter {
    pub fn new() -> Result<Self, String> {
        let lambert = Proj::from_proj_string(LAMBERT72_PROJ)
            .map_err(|e| format!("invalid Lambert72 proj string: {e}"))?;
        let wgs84 = Proj::from_proj_string(WGS84_PROJ)
            .map_err(|e| format!("invalid WGS84 proj string: {e}"))?;
        Ok(Self { lambert, wgs84 })
    }

    /// proj4rs emits longlat in radians with (x=lon, y=lat) axis order, so the
    /// output is converted to degrees and reordered to `(lat, lon)`.
    pub fn to_wgs84(&self, x: f64, y: f64) -> Result<(f64, f64), String> {
        let mut point = (x, y, 0.0);
        proj4rs::transform::transform(&self.lambert, &self.wgs84, &mut point)
            .map_err(|e| format!("Lambert72→WGS84 transform failed: {e}"))?;
        Ok((point.1.to_degrees(), point.0.to_degrees()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P1_L72: (f64, f64) = (148378.77, 172011.96);
    const P1_WGS84: (f64, f64) = (50.85849524, 4.34572624);
    const P2_L72: (f64, f64) = (141332.0, 132433.0);
    const P2_WGS84: (f64, f64) = (50.502627, 4.246559);

    #[test]
    fn lambert72_reference_points_convert_within_tolerance() {
        let c = Lambert72Converter::new().unwrap();

        let (lat1, lon1) = c.to_wgs84(P1_L72.0, P1_L72.1).unwrap();
        assert!(
            (lat1 - P1_WGS84.0).abs() < 1e-5,
            "P1 lat {lat1} vs {}",
            P1_WGS84.0
        );
        assert!(
            (lon1 - P1_WGS84.1).abs() < 1e-5,
            "P1 lon {lon1} vs {}",
            P1_WGS84.1
        );

        let (lat2, lon2) = c.to_wgs84(P2_L72.0, P2_L72.1).unwrap();
        assert!(
            (lat2 - P2_WGS84.0).abs() < 3e-3,
            "P2 lat {lat2} vs {}",
            P2_WGS84.0
        );
        assert!(
            (lon2 - P2_WGS84.1).abs() < 3e-3,
            "P2 lon {lon2} vs {}",
            P2_WGS84.1
        );
    }

    #[test]
    fn lambert72_conversion_is_within_belgium() {
        let c = Lambert72Converter::new().unwrap();
        let (lat, lon) = c.to_wgs84(150000.0, 150000.0).unwrap();
        assert!((49.0..=52.0).contains(&lat), "lat {lat} outside Belgium");
        assert!((2.0..=7.0).contains(&lon), "lon {lon} outside Belgium");
    }
}
