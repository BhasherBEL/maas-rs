//! Lambert Conformal Conic (2SP) forward projection for Belge Lambert 2008
//! (EPSG:3812), used to map OSM node lat/lng onto the GeoTIFF DEM raster.
//! Parameters are embedded in `data/belgium-DTM-20m.tif`.

/// GRS80 ellipsoid + Belge-Lambert-2008 projection constants.
const A: f64 = 6_378_137.0; // semi-major axis (m)
const INV_F: f64 = 298.257_222_101; // inverse flattening
const LAT0: f64 = 50.797_815; // latitude of origin (deg)
const LON0: f64 = 4.359_215_833_333_333; // central meridian (deg)
const LAT1: f64 = 49.833_333_333_333_336; // standard parallel 1 (deg)
const LAT2: f64 = 51.166_666_666_666_664; // standard parallel 2 (deg)
const FALSE_EASTING: f64 = 649_328.0;
const FALSE_NORTHING: f64 = 665_262.0;

/// Projects geographic (lat, lon) in degrees to Belge-Lambert-2008 (easting,
/// northing) in meters, using the standard LCC-2SP closed-form formulas.
pub fn project(lat_deg: f64, lon_deg: f64) -> (f64, f64) {
    let e = {
        let f = 1.0 / INV_F;
        (2.0 * f - f * f).sqrt()
    };
    let d2r = std::f64::consts::PI / 180.0;
    let lat = lat_deg * d2r;
    let lat0 = LAT0 * d2r;
    let lat1 = LAT1 * d2r;
    let lat2 = LAT2 * d2r;
    let lon = lon_deg * d2r;
    let lon0 = LON0 * d2r;

    let m = |phi: f64| phi.cos() / (1.0 - e * e * phi.sin() * phi.sin()).sqrt();
    let t = |phi: f64| {
        ((std::f64::consts::FRAC_PI_4 - phi / 2.0).tan())
            / ((1.0 - e * phi.sin()) / (1.0 + e * phi.sin())).powf(e / 2.0)
    };

    let m1 = m(lat1);
    let m2 = m(lat2);
    let t0 = t(lat0);
    let t1 = t(lat1);
    let t2 = t(lat2);
    let t_lat = t(lat);

    let n = (m1.ln() - m2.ln()) / (t1.ln() - t2.ln());
    let f_cap = m1 / (n * t1.powf(n));
    let r = |tt: f64| A * f_cap * tt.powf(n);
    let r0 = r(t0);
    let r_lat = r(t_lat);

    let theta = n * (lon - lon0);
    let easting = FALSE_EASTING + r_lat * theta.sin();
    let northing = FALSE_NORTHING + r0 - r_lat * theta.cos();
    (easting, northing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projects_origin_to_false_origin() {
        // At the latitude of origin on the central meridian, easting == false
        // easting and northing == false northing (theta = 0, r_lat = r0).
        let (e, n) = project(LAT0, LON0);
        assert!((e - FALSE_EASTING).abs() < 1.0, "easting {e}");
        assert!((n - FALSE_NORTHING).abs() < 1.0, "northing {n}");
    }

    #[test]
    fn brussels_is_in_plausible_range() {
        // Brussels ~ 50.85N, 4.35E should land near the false origin (a few km),
        // with positive, finite coordinates.
        let (e, n) = project(50.85, 4.35);
        assert!(e.is_finite() && n.is_finite());
        assert!((e - FALSE_EASTING).abs() < 50_000.0, "easting {e}");
        assert!((n - FALSE_NORTHING).abs() < 50_000.0, "northing {n}");
    }
}
