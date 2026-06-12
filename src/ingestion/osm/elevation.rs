//! Per-node elevation sampling from a local GeoTIFF DEM (Belge Lambert 2008).
//! Absent DEM ⇒ `None` everywhere (elevation cost/time disabled).
//!
//! Decodes the DEM with the `tiff` crate directly (unlimited decode limits, so
//! large national rasters load), replicating the GeoTIFF raster↔model transform
//! (ModelTiepoint+ModelPixelScale or ModelTransformation, plus the PixelIsPoint
//! half-pixel offset). Sampling is nearest-cell in projected (model) space.

use tiff::decoder::{Decoder, DecodingResult, Limits};
use tiff::tags::Tag;

use super::lambert;

/// Sentinel for "no data" in the DEM and for out-of-bounds samples.
const NODATA_SENTINEL: f32 = -1.0e30; // file uses ~ -3.4e38; anything below this is nodata.

/// GeoKey id for `GTRasterTypeGeoKey` (1 = PixelIsArea, 2 = PixelIsPoint).
const RASTER_TYPE_KEY: u16 = 1025;

/// Raster→model georeferencing for a north-up or affine GeoTIFF.
enum Transform {
    /// `model = (raster - raster_point) * pixel_scale + model_point` (y flipped).
    TiePointScale {
        raster_x: f64,
        raster_y: f64,
        model_x: f64,
        model_y: f64,
        scale_x: f64,
        scale_y: f64,
    },
    /// 2x3 inverse affine mapping model → raster.
    Affine { inverse: [f64; 6] },
}

impl Transform {
    /// Maps model-space (x, y) to fractional raster (col, row).
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
    /// -0.5 for PixelIsPoint rasters, else 0.0 (OGC raster-space convention).
    raster_offset: f64,
    transform: Transform,
}

impl Dem {
    /// Loads a GeoTIFF DEM from `path`. Returns `Err` if the file cannot be read,
    /// parsed, or lacks georeferencing, so the caller can proceed without elevation.
    pub fn load(path: &str) -> Result<Self, String> {
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
        })
    }

    fn read_transform(decoder: &mut Decoder<std::fs::File>) -> Result<Transform, String> {
        let f64s = |d: &mut Decoder<std::fs::File>, tag: Tag| -> Option<Vec<f64>> {
            d.find_tag(tag).ok().flatten().and_then(|v| v.into_f64_vec().ok())
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

    /// PixelIsPoint rasters sample at cell centers ⇒ -0.5 offset; else 0.
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
                return -0.5; // RasterPixelIsPoint
            }
        }
        0.0
    }

    /// Elevation (meters) at geographic (lat, lon), or `None` when outside the
    /// raster or over a nodata cell. Projects to Lambert, then samples the DEM
    /// at the nearest raster cell in projected (model) space.
    pub fn elevation(&self, lat: f64, lon: f64) -> Option<f32> {
        let (mx, my) = lambert::project(lat, lon);
        let (mut col, mut row) = self.transform.to_raster(mx, my);
        col -= self.raster_offset;
        row -= self.raster_offset;
        if col < 0.0 || row < 0.0 || col >= self.width as f64 || row >= self.height as f64 {
            return None;
        }
        let idx = row as usize * self.width + col as usize;
        let v = *self.data.get(idx)?;
        if v <= NODATA_SENTINEL || !v.is_finite() {
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
    fn nodata_sentinel_filters_extreme_negatives() {
        // Guard the nodata logic without a real raster: values at/below the
        // sentinel are rejected, plausible elevations pass.
        let filter = |v: f32| (v > NODATA_SENTINEL && v.is_finite()).then_some(v);
        assert_eq!(filter(-3.4e38), None);
        assert_eq!(filter(f32::NAN), None);
        assert_eq!(filter(120.0), Some(120.0));
    }

    #[test]
    fn tie_point_scale_maps_model_to_raster() {
        // Origin tie point at (0,0)->(100000, 200000), 20 m cells. The model
        // point itself maps to raster (0,0); one cell east/south is (1,1).
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
        let dem = Dem::load("data/belgium-DTM-20m.tif").unwrap();
        // Brussels-ish; Belgium elevations are 0..700 m.
        let z = dem.elevation(50.85, 4.35).unwrap();
        assert!((0.0..700.0).contains(&z), "elevation {z}");
    }
}
