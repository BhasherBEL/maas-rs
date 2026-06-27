//! Ingestion-time elevation denoising for way profiles.
//!
//! A ~20 m DTM carries ±1–3 m of sub-resolution noise on flat ground, which
//! makes phantom-climb routes win the min-D+ Pareto axis. We denoise ONCE here,
//! per way, so each segment carries a stable, additive, noise-free ascent.
//!
//! The core is Ramer–Douglas–Peucker on the (cumulative-distance, elevation)
//! profile with a vertical epsilon (default 4 m, config-driven), mirroring
//! GraphHopper's `ramer`/`max_elevation`. Endpoints and any vertex deviating
//! more than ε from the simplified line are retained; every original node is then
//! assigned a smoothed elevation by linear interpolation along the retained
//! polyline. Per-segment deltas derived from the smoothed elevations telescope to
//! `smoothed(last) − smoothed(first)`, so additivity along the way is preserved.

/// Smooth a way's elevation profile and return a smoothed elevation per input
/// node. `points` is `(cumulative_distance_m, elevation_m)` for each node in
/// order; `epsilon` is the RDP vertical tolerance in meters. The first and last
/// elevations are always preserved exactly.
pub fn smooth_profile(points: &[(f64, f64)], epsilon: f64) -> Vec<f64> {
    let n = points.len();
    if n <= 2 || epsilon <= 0.0 {
        return points.iter().map(|p| p.1).collect();
    }

    let mut keep = vec![false; n];
    keep[0] = true;
    keep[n - 1] = true;
    rdp(points, 0, n - 1, epsilon, &mut keep);

    let kept: Vec<usize> = (0..n).filter(|&i| keep[i]).collect();
    let mut out = vec![0.0; n];
    let mut seg = 0;
    for i in 0..n {
        while seg + 1 < kept.len() && kept[seg + 1] < i {
            seg += 1;
        }
        let a = kept[seg];
        let b = kept[seg + 1];
        out[i] = interpolate(points, a, b, i);
    }
    out
}

/// Linear-interpolation profile for bridges/tunnels: every node gets the
/// straight end-to-end elevation by distance, ignoring intermediate DEM samples
/// (DTMs read the valley floor / canopy under such ways and fabricate climbs).
pub fn linear_profile(points: &[(f64, f64)]) -> Vec<f64> {
    let n = points.len();
    if n == 0 {
        return Vec::new();
    }
    if n == 1 {
        return vec![points[0].1];
    }
    let a = 0;
    let b = n - 1;
    (0..n).map(|i| interpolate(points, a, b, i)).collect()
}

/// Elevation at node `i` interpolated linearly by distance along segment a→b.
fn interpolate(points: &[(f64, f64)], a: usize, b: usize, i: usize) -> f64 {
    if i == a {
        return points[a].1;
    }
    if i == b {
        return points[b].1;
    }
    let (da, za) = points[a];
    let (db, zb) = points[b];
    let span = db - da;
    if span <= 0.0 {
        return za;
    }
    let t = (points[i].0 - da) / span;
    za + (zb - za) * t
}

/// Recursive RDP on the (distance, elevation) profile: keep the vertex of maximum
/// vertical deviation from the chord a→b when it exceeds ε, then recurse.
fn rdp(points: &[(f64, f64)], a: usize, b: usize, epsilon: f64, keep: &mut [bool]) {
    if b <= a + 1 {
        return;
    }
    let mut max_dev = 0.0;
    let mut max_idx = a;
    for i in (a + 1)..b {
        let chord = interpolate(points, a, b, i);
        let dev = (points[i].1 - chord).abs();
        if dev > max_dev {
            max_dev = dev;
            max_idx = i;
        }
    }
    if max_dev > epsilon {
        keep[max_idx] = true;
        rdp(points, a, max_idx, epsilon, keep);
        rdp(points, max_idx, b, epsilon, keep);
    }
}

/// Smoothed per-segment signed deltas (meters) for consecutive nodes, derived
/// from `smooth_profile`. Returned vector has `points.len() - 1` entries; their
/// sum equals `smoothed(last) − smoothed(first)` (additivity preserved).
#[cfg(test)]
pub fn smoothed_deltas(points: &[(f64, f64)], epsilon: f64) -> Vec<f64> {
    let z = smooth_profile(points, epsilon);
    z.windows(2).map(|w| w[1] - w[0]).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ascent(deltas: &[f64]) -> f64 {
        deltas.iter().filter(|&&d| d > 0.0).sum()
    }

    #[test]
    fn flat_noise_bump_collapses_to_zero_ascent() {
        // Symmetric ±2 m blips on otherwise flat ground, 20 m node spacing.
        let pts = vec![
            (0.0, 100.0),
            (20.0, 102.0),
            (40.0, 100.0),
            (60.0, 98.0),
            (80.0, 100.0),
        ];
        let deltas = smoothed_deltas(&pts, 4.0);
        assert!(
            ascent(&deltas) < 0.01,
            "noise bumps must denoise to ~0 ascent, got {}",
            ascent(&deltas)
        );
    }

    #[test]
    fn real_distributed_climb_is_preserved() {
        // A genuine 8 m climb spread over several nodes (monotone rise).
        let pts = vec![
            (0.0, 100.0),
            (30.0, 102.0),
            (60.0, 104.0),
            (90.0, 106.0),
            (120.0, 108.0),
        ];
        let deltas = smoothed_deltas(&pts, 4.0);
        assert!(
            (ascent(&deltas) - 8.0).abs() < 0.5,
            "an 8 m climb must survive, got {}",
            ascent(&deltas)
        );
    }

    #[test]
    fn short_steep_ramp_not_erased() {
        // A >4 m step (one node up by 4.5 m), flat before and after. ε=4 retains it
        // because the deviation from the flat chord exceeds 4.0. At exactly ε the
        // vertex would be dropped; we use a step that clears ε.
        let pts = vec![
            (0.0, 100.0),
            (15.0, 100.0),
            (30.0, 104.5),
            (45.0, 104.5),
            (60.0, 104.5),
        ];
        let deltas = smoothed_deltas(&pts, 4.0);
        assert!(
            ascent(&deltas) > 4.0,
            "a >4 m step must not be erased, got {}",
            ascent(&deltas)
        );
    }

    #[test]
    fn bridge_tunnel_is_linear_end_to_end() {
        // Intermediate DEM reads the valley floor: a deep dip mid-span. Linear
        // interpolation across the feature must show no phantom intermediate climb.
        let pts = vec![
            (0.0, 100.0),
            (25.0, 70.0),
            (50.0, 68.0),
            (75.0, 72.0),
            (100.0, 101.0),
        ];
        let deltas = smoothed_deltas_linear(&pts);
        // End-to-end rise is 1 m over 100 m, monotone => total ascent ~1 m, no dip.
        assert!(
            (ascent(&deltas) - 1.0).abs() < 0.01,
            "bridge/tunnel must be linear, got ascent {}",
            ascent(&deltas)
        );
        for d in &deltas {
            assert!(*d >= 0.0, "linear monotone rise has no descent segments");
        }
    }

    #[test]
    fn additivity_sum_equals_endpoint_difference() {
        let pts = vec![
            (0.0, 100.0),
            (20.0, 103.0),
            (40.0, 99.0),
            (60.0, 110.0),
            (80.0, 107.0),
        ];
        let z = smooth_profile(&pts, 4.0);
        let deltas = smoothed_deltas(&pts, 4.0);
        let sum: f64 = deltas.iter().sum();
        assert!(
            (sum - (z[z.len() - 1] - z[0])).abs() < 1e-9,
            "Σ deltas must equal smoothed(last) − smoothed(first)"
        );
        // Endpoints are preserved exactly by the smoother.
        assert_eq!(z[0], 100.0);
        assert_eq!(z[z.len() - 1], 107.0);
    }

    fn smoothed_deltas_linear(points: &[(f64, f64)]) -> Vec<f64> {
        linear_profile(points)
            .windows(2)
            .map(|w| w[1] - w[0])
            .collect()
    }
}
