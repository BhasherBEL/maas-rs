use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static PROFILE: RefCell<LatencyProfile> = RefCell::new(LatencyProfile::empty());
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PassProfile {
    pub probe: Duration,
    pub range: Duration,
    pub departures: u32,
}

#[derive(Debug, Clone, Default)]
pub struct LatencyProfile {
    pub discovery: Duration,
    pub grid_alloc: Duration,
    pub forward: Duration,
    pub extract: Duration,
    /// Subset of `extract` — never exceeds it.
    pub backward: Duration,
    pub passes: Vec<PassProfile>,
    total: Option<Duration>,
}

impl LatencyProfile {
    const fn empty() -> Self {
        Self {
            discovery: Duration::ZERO,
            grid_alloc: Duration::ZERO,
            forward: Duration::ZERO,
            extract: Duration::ZERO,
            backward: Duration::ZERO,
            passes: Vec::new(),
            total: None,
        }
    }

    pub fn report(&self) -> String {
        let total = self.total.unwrap_or_default();
        let total_ms = to_ms(total);
        let pct = |d: Duration| {
            if total_ms > 0.0 {
                to_ms(d) / total_ms * 100.0
            } else {
                0.0
            }
        };
        let backward_of_extract = {
            let e = to_ms(self.extract);
            if e > 0.0 { to_ms(self.backward) / e * 100.0 } else { 0.0 }
        };

        let mut out = format!("query latency decomposition (total {:.1}ms)", total_ms);
        out += &format!(
            "\n  discovery    {:>9.1}ms  {:>5.1}%",
            to_ms(self.discovery),
            pct(self.discovery)
        );
        out += &format!(
            "\n  grid_alloc   {:>9.1}ms  {:>5.1}%",
            to_ms(self.grid_alloc),
            pct(self.grid_alloc)
        );
        out += &format!(
            "\n  forward      {:>9.1}ms  {:>5.1}%",
            to_ms(self.forward),
            pct(self.forward)
        );
        out += &format!(
            "\n  extract      {:>9.1}ms  {:>5.1}%",
            to_ms(self.extract),
            pct(self.extract)
        );
        out += &format!(
            "\n    backward   {:>9.1}ms  {:>5.1}%  ({:.1}% of extract)",
            to_ms(self.backward),
            pct(self.backward),
            backward_of_extract
        );
        for (i, pass) in self.passes.iter().enumerate() {
            let label = match i {
                0 => "Pass A".to_string(),
                1 => "Pass B".to_string(),
                n => format!("Pass {}", n + 1),
            };
            out += &format!(
                "\n  {label}: probe {:.1}ms, range {:.1}ms over {} departure(s)",
                to_ms(pass.probe),
                to_ms(pass.range),
                pass.departures
            );
        }
        out
    }
}

fn to_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Must be paired with exactly one `end_query` call before the next query begins
/// on this thread.
pub fn begin_query(on: bool) -> Instant {
    ENABLED.with(|e| e.set(on));
    if on {
        PROFILE.with(|p| *p.borrow_mut() = LatencyProfile::empty());
    }
    Instant::now()
}

pub fn end_query(start: Instant) -> Option<LatencyProfile> {
    let on = ENABLED.with(|e| e.get());
    ENABLED.with(|e| e.set(false));
    if !on {
        return None;
    }
    let mut profile = PROFILE.with(|p| p.borrow().clone());
    profile.total = Some(start.elapsed());
    Some(profile)
}

pub fn begin_pass() {
    if !ENABLED.with(|e| e.get()) {
        return;
    }
    PROFILE.with(|p| p.borrow_mut().passes.push(PassProfile::default()));
}

#[inline]
fn time<T>(acc: impl FnOnce(&mut LatencyProfile, Duration), f: impl FnOnce() -> T) -> T {
    if !ENABLED.with(|e| e.get()) {
        return f();
    }
    let start = Instant::now();
    let out = f();
    let dur = start.elapsed();
    PROFILE.with(|p| acc(&mut p.borrow_mut(), dur));
    out
}

#[inline]
pub fn time_discovery<T>(f: impl FnOnce() -> T) -> T {
    time(|p, d| p.discovery += d, f)
}

#[inline]
pub fn time_grid_alloc<T>(f: impl FnOnce() -> T) -> T {
    time(|p, d| p.grid_alloc += d, f)
}

#[inline]
pub fn time_forward<T>(f: impl FnOnce() -> T) -> T {
    time(|p, d| p.forward += d, f)
}

#[inline]
pub fn time_extract<T>(f: impl FnOnce() -> T) -> T {
    time(|p, d| p.extract += d, f)
}

#[inline]
pub fn time_backward<T>(f: impl FnOnce() -> T) -> T {
    time(|p, d| p.backward += d, f)
}

#[inline]
pub fn time_probe<T>(f: impl FnOnce() -> T) -> T {
    time(
        |p, d| {
            if let Some(last) = p.passes.last_mut() {
                last.probe += d;
            }
        },
        f,
    )
}

#[inline]
pub fn time_range_departure<T>(f: impl FnOnce() -> T) -> T {
    time(
        |p, d| {
            if let Some(last) = p.passes.last_mut() {
                last.range += d;
                last.departures += 1;
            }
        },
        f,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_by_default_and_time_helpers_are_passthrough() {
        assert!(!ENABLED.with(|e| e.get()));
        let mut ran = false;
        let out = time_discovery(|| {
            ran = true;
            42
        });
        assert_eq!(out, 42);
        assert!(ran);
        let start = begin_query(false);
        let _ = time_forward(|| 1);
        assert!(end_query(start).is_none());
    }

    #[test]
    fn on_accumulates_phases_and_nests_backward_under_extract() {
        let start = begin_query(true);
        begin_pass();
        time_probe(|| std::thread::sleep(Duration::from_millis(2)));
        time_discovery(|| std::thread::sleep(Duration::from_millis(1)));
        time_grid_alloc(|| std::thread::sleep(Duration::from_millis(1)));
        time_range_departure(|| {
            time_forward(|| std::thread::sleep(Duration::from_millis(1)));
            time_extract(|| {
                time_backward(|| std::thread::sleep(Duration::from_millis(1)));
            });
        });
        let profile = end_query(start).expect("profiling was enabled");

        assert!(profile.discovery > Duration::ZERO);
        assert!(profile.grid_alloc > Duration::ZERO);
        assert!(profile.forward > Duration::ZERO);
        assert!(profile.extract > Duration::ZERO);
        assert!(profile.backward > Duration::ZERO);
        assert!(
            profile.backward <= profile.extract,
            "backward must nest under extract: {:?} vs {:?}",
            profile.backward,
            profile.extract
        );
        assert_eq!(profile.passes.len(), 1);
        assert_eq!(profile.passes[0].departures, 1);
        assert!(profile.passes[0].probe > Duration::ZERO);
        assert!(profile.passes[0].range > Duration::ZERO);

        let report = profile.report();
        assert!(report.contains("discovery"));
        assert!(report.contains("backward"));
        assert!(report.contains("Pass A"));
    }

    #[test]
    fn end_query_disarms_so_the_next_query_defaults_off() {
        let start = begin_query(true);
        let _ = end_query(start);
        assert!(!ENABLED.with(|e| e.get()));
    }
}
