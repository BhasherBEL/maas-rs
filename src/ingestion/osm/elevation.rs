use tiff::decoder::{Decoder, DecodingResult, Limits};
use tiff::tags::Tag;

use super::lambert;

#[derive(Debug, Clone, Copy)]
pub enum DemProjection {
    BelgianLambert2008,
}

impl DemProjection {
    fn project(&self, lat: f64, lon: f64) -> (f64, f64) {
        match self {
            DemProjection::BelgianLambert2008 => lambert::project(lat, lon),
        }
    }

    fn epsg(&self) -> u16 {
        match self {
            DemProjection::BelgianLambert2008 => 3812,
        }
    }
}

pub trait ElevationSource {
    fn elevation(&self, lat: f64, lon: f64) -> Option<f32>;
}

pub struct DemSet(pub Vec<Dem>);

impl ElevationSource for DemSet {
    fn elevation(&self, lat: f64, lon: f64) -> Option<f32> {
        self.0.iter().find_map(|d| d.elevation(lat, lon))
    }
}

impl ElevationSource for Dem {
    fn elevation(&self, lat: f64, lon: f64) -> Option<f32> {
        Dem::elevation(self, lat, lon)
    }
}

// Nodata fallback when no GDAL_NODATA tag: Belgian DTM uses ~ -3.4e38, and
// common SRTM/EU-DEM sentinels (-9999, -32768) also fall below this.
const NODATA_SENTINEL: f32 = -1.0e30;

// GeoKey id GTRasterTypeGeoKey (1 = PixelIsArea, 2 = PixelIsPoint).
const RASTER_TYPE_KEY: u16 = 1025;

// GeoKey id ProjectedCSTypeGeoKey, holds the raster's projected EPSG.
const PROJECTED_CS_TYPE_KEY: u16 = 3072;

fn is_nodata(v: f32, nodata: Option<f32>) -> bool {
    if !v.is_finite() {
        return true;
    }
    match nodata {
        Some(nd) => v == nd || (nd.is_finite() && (v - nd).abs() <= nd.abs() * 1e-6),
        None => v <= NODATA_SENTINEL,
    }
}

// GeoKeyDirectory layout: header of 4 u16, then quads (key, location, count,
// value); an inline projected EPSG has location == 0 and the code in value.
fn geokey_projected_epsg(dir: &[u16]) -> Option<u16> {
    for quad in dir[4.min(dir.len())..].chunks_exact(4) {
        if quad[0] == PROJECTED_CS_TYPE_KEY && quad[1] == 0 {
            return Some(quad[3]);
        }
    }
    None
}

enum Transform {
    // model = (raster - raster_point) * pixel_scale + model_point (y flipped).
    TiePointScale {
        raster_x: f64,
        raster_y: f64,
        model_x: f64,
        model_y: f64,
        scale_x: f64,
        scale_y: f64,
    },
    // 2x3 inverse affine mapping model → raster.
    Affine { inverse: [f64; 6] },
}

impl Transform {
    fn to_raster(&self, x: f64, y: f64) -> (f64, f64) {
        match self {
            Transform::TiePointScale {
                raster_x,
                raster_y,
                model_x,
                model_y,
                scale_x,
                scale_y,
            } => (
                (x - model_x) / scale_x + raster_x,
                (y - model_y) / -scale_y + raster_y,
            ),
            Transform::Affine { inverse } => (
                x * inverse[0] + y * inverse[1] + inverse[2],
                x * inverse[3] + y * inverse[4] + inverse[5],
            ),
        }
    }
}

pub struct Dem {
    data: Vec<f32>,
    width: usize,
    height: usize,
    // -0.5 for PixelIsPoint rasters, else 0.0 (OGC raster-space convention).
    raster_offset: f64,
    transform: Transform,
    projection: DemProjection,
    // Per-file GDAL_NODATA value (tag 42113) if present; else fallback sentinel.
    nodata: Option<f32>,
}

impl Dem {
    pub fn load(path: &str, projection: DemProjection) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("open DEM '{path}': {e}"))?;
        let mut decoder = Decoder::new(file)
            .map_err(|e| format!("decode DEM '{path}': {e:?}"))?
            .with_limits(Limits::unlimited());

        let (width, height) = decoder
            .dimensions()
            .map(|(w, h)| (w as usize, h as usize))
            .map_err(|e| format!("DEM dimensions '{path}': {e:?}"))?;

        let transform = Self::read_transform(&mut decoder)
            .map_err(|e| format!("DEM georeferencing '{path}': {e}"))?;
        let raster_offset = Self::read_raster_offset(&mut decoder);
        let nodata = Self::read_nodata(&mut decoder);
        Self::check_crs(&mut decoder, projection, path);

        let data = match decoder
            .read_image()
            .map_err(|e| format!("read DEM raster '{path}': {e:?}"))?
        {
            DecodingResult::F32(d) => d,
            DecodingResult::F64(d) => d.into_iter().map(|v| v as f32).collect(),
            DecodingResult::I16(d) => d.into_iter().map(|v| v as f32).collect(),
            DecodingResult::U16(d) => d.into_iter().map(|v| v as f32).collect(),
            DecodingResult::I32(d) => d.into_iter().map(|v| v as f32).collect(),
            DecodingResult::U32(d) => d.into_iter().map(|v| v as f32).collect(),
            other => return Err(format!("DEM '{path}': unsupported sample type {other:?}")),
        };

        Ok(Dem {
            data,
            width,
            height,
            raster_offset,
            transform,
            projection,
            nodata,
        })
    }

    // GDAL_NODATA (TIFF tag 42113) is an ASCII string holding the nodata number.
    fn read_nodata(decoder: &mut Decoder<std::fs::File>) -> Option<f32> {
        decoder
            .find_tag(Tag::GdalNodata)
            .ok()
            .flatten()
            .and_then(|v| v.into_string().ok())
            .and_then(|s| s.trim().trim_end_matches('\0').trim().parse::<f64>().ok())
            .map(|v| v as f32)
            .filter(|v| v.is_finite())
    }

    fn check_crs(decoder: &mut Decoder<std::fs::File>, projection: DemProjection, path: &str) {
        let Some(dir) = decoder
            .find_tag(Tag::GeoKeyDirectoryTag)
            .ok()
            .flatten()
            .and_then(|v| v.into_u16_vec().ok())
        else {
            return;
        };
        let Some(file_epsg) = geokey_projected_epsg(&dir) else {
            return;
        };
        let expected = projection.epsg();
        if file_epsg != 0 && file_epsg != 32767 && file_epsg != expected {
            tracing::warn!(
                "DEM '{path}' CRS EPSG:{file_epsg} does not match configured projection EPSG:{expected}; elevations may be wrong"
            );
        }
    }

    fn read_transform(decoder: &mut Decoder<std::fs::File>) -> Result<Transform, String> {
        let f64s = |d: &mut Decoder<std::fs::File>, tag: Tag| -> Option<Vec<f64>> {
            d.find_tag(tag)
                .ok()
                .flatten()
                .and_then(|v| v.into_f64_vec().ok())
        };
        if let Some(m) = f64s(decoder, Tag::ModelTransformationTag) {
            if m.len() == 16 {
                let t = [m[0], m[1], m[3], m[4], m[5], m[7]];
                let det = t[0] * t[4] - t[1] * t[3];
                if det.abs() < 1e-15 {
                    return Err("non-invertible ModelTransformation".into());
                }
                let inverse = [
                    t[4] / det,
                    -t[1] / det,
                    (t[1] * t[5] - t[2] * t[4]) / det,
                    -t[3] / det,
                    t[0] / det,
                    (-t[0] * t[5] + t[2] * t[3]) / det,
                ];
                return Ok(Transform::Affine { inverse });
            }
        }
        let tie = f64s(decoder, Tag::ModelTiepointTag)
            .ok_or("missing ModelTiepoint/ModelTransformation")?;
        let scale = f64s(decoder, Tag::ModelPixelScaleTag).ok_or("missing ModelPixelScale")?;
        if tie.len() < 6 || scale.len() < 2 {
            return Err("malformed tie point / pixel scale".into());
        }
        Ok(Transform::TiePointScale {
            raster_x: tie[0],
            raster_y: tie[1],
            model_x: tie[3],
            model_y: tie[4],
            scale_x: scale[0],
            scale_y: scale[1],
        })
    }

    // PixelIsPoint rasters sample at cell centers ⇒ -0.5 offset; else 0.
    fn read_raster_offset(decoder: &mut Decoder<std::fs::File>) -> f64 {
        let Some(dir) = decoder
            .find_tag(Tag::GeoKeyDirectoryTag)
            .ok()
            .flatten()
            .and_then(|v| v.into_u16_vec().ok())
        else {
            return 0.0;
        };
        // Header is 4 u16; entries are quads (key, location, count, value).
        for quad in dir[4.min(dir.len())..].chunks_exact(4) {
            if quad[0] == RASTER_TYPE_KEY && quad[1] == 0 && quad[3] == 2 {
                return -0.5;
            }
        }
        0.0
    }

    pub fn elevation(&self, lat: f64, lon: f64) -> Option<f32> {
        let (mx, my) = self.projection.project(lat, lon);
        let (mut col, mut row) = self.transform.to_raster(mx, my);
        col -= self.raster_offset;
        row -= self.raster_offset;
        if col < 0.0 || row < 0.0 || col >= self.width as f64 || row >= self.height as f64 {
            return None;
        }
        let idx = row as usize * self.width + col as usize;
        let v = *self.data.get(idx)?;
        if is_nodata(v, self.nodata) {
            None
        } else {
            Some(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodata_fallback_filters_extreme_negatives() {
        assert!(is_nodata(-3.4e38, None));
        assert!(is_nodata(f32::NAN, None));
        assert!(is_nodata(f32::NEG_INFINITY, None));
        assert!(!is_nodata(120.0, None));
        assert!(!is_nodata(-5.0, None));
    }

    #[test]
    fn per_file_nodata_masks_that_value() {
        let nd = Some(-9999.0);
        assert!(is_nodata(-9999.0, nd));
        assert!(!is_nodata(120.0, nd));
        assert!(!is_nodata(-3.0, nd));
        assert!(!is_nodata(-1.0e30, nd));
        let nd = Some(-32768.0);
        assert!(is_nodata(-32768.0, nd));
        assert!(!is_nodata(-32767.0, nd));
    }

    #[test]
    fn per_file_nodata_still_rejects_non_finite() {
        assert!(is_nodata(f32::NAN, Some(-9999.0)));
        assert!(is_nodata(f32::INFINITY, Some(-9999.0)));
    }

    #[test]
    fn parses_gdal_nodata_string() {
        // GDAL writes the nodata tag as an ASCII string, sometimes NUL-padded.
        let parse = |s: &str| -> Option<f32> {
            s.trim()
                .trim_end_matches('\0')
                .trim()
                .parse::<f64>()
                .ok()
                .map(|v| v as f32)
                .filter(|v| v.is_finite())
        };
        assert_eq!(parse("-9999"), Some(-9999.0));
        assert_eq!(parse("-32768\0"), Some(-32768.0));
        assert_eq!(parse(" -9999.0 "), Some(-9999.0));
        assert_eq!(parse("nan"), None);
        assert_eq!(parse(""), None);
    }

    #[test]
    fn crs_mismatch_is_detected() {
        // GeoKeyDirectory header (4 u16) then quads (key, location, count, value).
        let dir = |epsg: u16| vec![1, 1, 0, 1, PROJECTED_CS_TYPE_KEY, 0, 1, epsg];
        assert_eq!(geokey_projected_epsg(&dir(3812)), Some(3812));
        assert_eq!(geokey_projected_epsg(&dir(32631)), Some(32631));
        assert_eq!(geokey_projected_epsg(&[1, 1, 0, 1, 1024, 0, 1, 1]), None);
        assert_eq!(geokey_projected_epsg(&[]), None);
        assert_eq!(DemProjection::BelgianLambert2008.epsg(), 3812);
        assert_ne!(
            geokey_projected_epsg(&dir(32631)),
            Some(DemProjection::BelgianLambert2008.epsg())
        );
    }

    #[test]
    fn tie_point_scale_maps_model_to_raster() {
        let t = Transform::TiePointScale {
            raster_x: 0.0,
            raster_y: 0.0,
            model_x: 100_000.0,
            model_y: 200_000.0,
            scale_x: 20.0,
            scale_y: 20.0,
        };
        let (c, r) = t.to_raster(100_000.0, 200_000.0);
        assert!((c).abs() < 1e-9 && (r).abs() < 1e-9, "{c},{r}");
        let (c, r) = t.to_raster(100_020.0, 199_980.0);
        assert!((c - 1.0).abs() < 1e-9 && (r - 1.0).abs() < 1e-9, "{c},{r}");
    }

    #[test]
    #[ignore = "requires data/belgium-DTM-20m.tif"]
    fn real_dem_returns_plausible_belgian_elevation() {
        let dem = Dem::load("data/belgium-DTM-20m.tif", DemProjection::BelgianLambert2008).unwrap();
        let z = dem.elevation(50.85, 4.35).unwrap();
        assert!((0.0..700.0).contains(&z), "elevation {z}");
    }

    struct Fixed(Option<f32>);
    impl ElevationSource for Fixed {
        fn elevation(&self, _lat: f64, _lon: f64) -> Option<f32> {
            self.0
        }
    }

    #[test]
    fn demset_first_hit_wins() {
        let set = vec![Fixed(None), Fixed(Some(10.0)), Fixed(Some(20.0))];
        let hit = set.iter().find_map(|d| d.elevation(0.0, 0.0));
        assert_eq!(hit, Some(10.0));
    }

    #[test]
    fn demset_all_miss_is_none() {
        let set = vec![Fixed(None), Fixed(None)];
        let hit = set.iter().find_map(|d| d.elevation(0.0, 0.0));
        assert_eq!(hit, None);
    }
}
