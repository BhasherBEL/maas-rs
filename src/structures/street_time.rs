use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StreetTimeModel {
    pub access_percentile: f64,
    pub sigma_alpha: f64,
    pub sigma_floor: f64,
    pub sigma_cap: f64,
}

impl Default for StreetTimeModel {
    fn default() -> Self {
        StreetTimeModel {
            access_percentile: 0.85,
            sigma_alpha: 3.8,
            sigma_floor: 0.12,
            sigma_cap: 0.5,
        }
    }
}

impl StreetTimeModel {
    pub(crate) fn sigma(&self, t50: u32) -> f64 {
        if t50 == 0 {
            return 0.0;
        }
        (self.sigma_alpha / (t50 as f64).sqrt()).clamp(self.sigma_floor, self.sigma_cap)
    }

    pub fn access_secs(&self, t50: u32) -> u32 {
        if t50 == 0 {
            return 0;
        }
        let z = inv_norm(self.access_percentile);
        (t50 as f64 * (self.sigma(t50) * z).exp()).round() as u32
    }

    pub fn egress_secs(&self, t50: u32) -> u32 {
        if t50 == 0 {
            return 0;
        }
        let s = self.sigma(t50);
        (t50 as f64 * (s * s / 2.0).exp()).round() as u32
    }
}

pub(crate) fn inv_norm(p: f64) -> f64 {
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    const A: [f64; 6] = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    const C: [f64; 6] = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];
    let plow = 0.02425;
    let phigh = 1.0 - plow;
    if p < plow {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= phigh {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inv_norm_known_quantiles() {
        assert!((inv_norm(0.5)).abs() < 1e-6);
        assert!((inv_norm(0.975) - 1.959964).abs() < 1e-3);
        assert!((inv_norm(0.85) - 1.036433).abs() < 1e-3);
    }

    #[test]
    fn sigma_shrinks_with_distance_and_is_clamped() {
        let m = StreetTimeModel::default();
        assert!(
            m.sigma(120) > m.sigma(900),
            "spread must shrink with trip time"
        );
        assert!(m.sigma(5) <= m.sigma_cap + 1e-9, "short trips capped");
        assert!(
            m.sigma(100_000) >= m.sigma_floor - 1e-9,
            "long trips floored"
        );
        assert_eq!(m.sigma(0), 0.0);
    }

    #[test]
    fn access_buffers_and_egress_is_honest_mean() {
        let m = StreetTimeModel::default();
        assert!(m.access_secs(120) > 120);
        assert!(m.access_secs(600) > 600);
        // egress = mean = median * exp(σ²/2); for t50=120 with defaults this is 127s.
        assert_eq!(m.egress_secs(120), 127);
        assert!(m.egress_secs(120) >= 120);
        assert!(m.egress_secs(120) < m.access_secs(120));
        assert_eq!(m.access_secs(0), 0);
        assert_eq!(m.egress_secs(0), 0);
    }

    #[test]
    fn access_secs_monotonic_in_median() {
        let m = StreetTimeModel::default();
        assert!(m.access_secs(60) < m.access_secs(300));
        assert!(m.access_secs(300) < m.access_secs(1200));
    }
}
