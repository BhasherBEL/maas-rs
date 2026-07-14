//! Per-edge bike speed factor baked from OSM `surface=*` (asphalt=1.0), quantized to
//! u8 `round(factor·100)` at ingest, so re-tuning needs a rebuild.

use std::collections::HashMap;

use serde::Deserialize;

pub const UNKNOWN_SURFACE_FACTOR: f64 = 0.90;

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceSpeedFactors(HashMap<String, f64>);

impl Default for SurfaceSpeedFactors {
    fn default() -> Self {
        let pairs: &[(&str, f64)] = &[
            ("asphalt", 1.00),
            ("concrete", 0.95),
            ("paved", 0.90),
            ("concrete:plates", 0.85),
            ("metal", 0.85),
            ("wood", 0.85),
            ("paving_stones", 0.80),
            ("compacted", 0.80),
            ("fine_gravel", 0.80),
            ("grass_paver", 0.70),
            ("unpaved", 0.70),
            ("sett", 0.65),
            ("gravel", 0.60),
            ("pebblestone", 0.60),
            ("ground", 0.60),
            ("dirt", 0.60),
            ("earth", 0.60),
            ("cobblestone", 0.50),
            ("unhewn_cobblestone", 0.50),
            ("grass", 0.45),
            ("sand", 0.25),
            ("mud", 0.20),
        ];
        SurfaceSpeedFactors(pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    }
}

impl SurfaceSpeedFactors {
    /// Missing or unrecognised surfaces fall back to [`UNKNOWN_SURFACE_FACTOR`].
    pub fn factor(&self, surface: Option<&str>) -> f64 {
        surface
            .and_then(|s| self.0.get(s).copied())
            .unwrap_or(UNKNOWN_SURFACE_FACTOR)
    }

    /// Clamped to `[1, 255]`: never 0, which the read side reserves for "unset".
    pub fn quantize(&self, surface: Option<&str>) -> u8 {
        (self.factor(surface) * 100.0).round().clamp(1.0, 255.0) as u8
    }

    /// Deterministic key order, so the build fingerprint hash is stable across runs.
    pub fn sorted_entries(&self) -> Vec<(&str, f64)> {
        let mut entries: Vec<(&str, f64)> = self.0.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_table_maps_known_surfaces() {
        let f = SurfaceSpeedFactors::default();
        assert_eq!(f.quantize(Some("asphalt")), 100);
        assert_eq!(f.quantize(Some("gravel")), 60);
        assert_eq!(f.quantize(Some("mud")), 20);
        assert_eq!(f.quantize(Some("sett")), 65);
        assert_eq!(f.quantize(Some("cobblestone")), 50);
    }

    #[test]
    fn unknown_and_missing_use_default() {
        let f = SurfaceSpeedFactors::default();
        assert_eq!(f.quantize(None), 90, "untagged → unknown default");
        assert_eq!(f.quantize(Some("wibble")), 90, "unrecognised → unknown default");
        assert!((f.factor(None) - UNKNOWN_SURFACE_FACTOR).abs() < 1e-12);
    }

    #[test]
    fn quantize_never_zero() {
        let mut m = HashMap::new();
        m.insert("tar".to_string(), 0.001);
        let f = SurfaceSpeedFactors(m);
        assert_eq!(f.quantize(Some("tar")), 1, "tiny factor clamps to 1, never 0");
    }
}
