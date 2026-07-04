//! Blocking HTTP fetcher with a shared rate limiter for realtime polling.
//!
//! All realtime feeds hit the same provider gateway (shared `BMC_PARTNER_KEY`,
//! ~8 req/min / 12k/day). The gateway signals quota-exceeded with HTTP `403`
//! ("Out of call volume quota"), and momentary rate breaches with `429`. One
//! [`Fetcher`] paces every request through a single [`RateLimiter`]; a run of
//! throttle responses (`403`/`429`) engages a backoff that makes all feeds
//! **skip** their requests — issuing only one probe per `throttled_min_interval`
//! until a request succeeds — instead of hammering the gateway. Runs in the
//! poller's `spawn_blocking` context.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::ingestion::secrets::interpolate;

/// Outcome of a fetch attempt, so the poller can tell a throttle skip (silent,
/// expected during a quota backoff) from a genuine failure it should log.
#[derive(Debug)]
pub enum FetchError {
    /// No request was issued (limiter is backing off), or the gateway answered
    /// with a throttle status (403/429). The poller skips this feed silently —
    /// the limiter itself logs the throttle transition once per episode.
    Throttled,
    /// A request was issued and failed for a non-throttle reason (network,
    /// timeout, parse, or an unexpected status). The poller logs these.
    Failed(String),
}

impl From<String> for FetchError {
    fn from(s: String) -> Self {
        FetchError::Failed(s)
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Throttled => write!(f, "throttled (request skipped)"),
            FetchError::Failed(m) => write!(f, "{m}"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitConfig {
    /// Consecutive throttle responses (403/429) before all feeds start skipping.
    pub consecutive_failure_threshold: u32,
    /// While throttled, at most one probe request is issued per this interval.
    pub throttled_min_interval: Duration,
}

/// Result of asking the limiter for permission to issue a request.
enum Permit {
    /// Go ahead and issue the request.
    Proceed,
    /// Skip: throttled and still inside the current backoff window.
    Skip,
}

struct LimiterState {
    /// When a request was last actually issued (a proceed/probe). Used to space
    /// probes while throttled. A skip does not update it.
    last_request: Option<Instant>,
    consecutive_failures: u32,
    throttled: bool,
    /// When the throttle body was last logged. The body WARN is rate-limited to
    /// once per `throttled_min_interval` so a sustained 403 (even one persistently
    /// failing feed among healthy ones) never storms the log.
    last_body_log: Option<Instant>,
}

pub struct RateLimiter {
    state: Mutex<LimiterState>,
    cfg: RateLimitConfig,
}

impl RateLimiter {
    pub fn new(cfg: RateLimitConfig) -> Self {
        Self {
            state: Mutex::new(LimiterState {
                last_request: None,
                consecutive_failures: 0,
                throttled: false,
                last_body_log: None,
            }),
            cfg,
        }
    }

    /// Decide whether to issue the next request. While throttled, all requests
    /// are skipped except one probe per `throttled_min_interval`. Never sleeps.
    fn acquire(&self) -> Permit {
        let mut st = self.state.lock().unwrap();
        if st.throttled
            && let Some(last) = st.last_request
            && last.elapsed() < self.cfg.throttled_min_interval
        {
            return Permit::Skip;
        }
        st.last_request = Some(Instant::now());
        Permit::Proceed
    }

    /// Record a throttle response (403/429). Increments the failure counter,
    /// logs the gateway body once per episode (it distinguishes quota-exceeded
    /// from an invalid key), and engages the throttle at the threshold.
    fn on_throttle(&self, status: u16, body: Option<&str>) {
        let mut st = self.state.lock().unwrap();
        st.consecutive_failures += 1;
        let due = st
            .last_body_log
            .is_none_or(|t| t.elapsed() >= self.cfg.throttled_min_interval);
        if due {
            st.last_body_log = Some(Instant::now());
            tracing::warn!(
                status,
                consecutive = st.consecutive_failures,
                body = body.unwrap_or("").trim(),
                "realtime: gateway throttled the request"
            );
        }
        if !st.throttled && st.consecutive_failures >= self.cfg.consecutive_failure_threshold {
            st.throttled = true;
            tracing::error!(
                status,
                "realtime: repeated throttle responses; backing off all feeds \
                 (one probe per interval until a request succeeds)"
            );
        }
    }

    fn on_success(&self) {
        let mut st = self.state.lock().unwrap();
        if st.throttled {
            tracing::info!("realtime: request succeeded; lifting throttle");
        }
        st.consecutive_failures = 0;
        st.throttled = false;
    }

    #[cfg(test)]
    fn is_throttled(&self) -> bool {
        self.state.lock().unwrap().throttled
    }

    #[cfg(test)]
    fn would_skip(&self) -> bool {
        matches!(self.acquire(), Permit::Skip)
    }
}

/// Shared blocking HTTP client honoring the rate limiter.
pub struct Fetcher {
    limiter: RateLimiter,
    timeout: Duration,
}

impl Fetcher {
    pub fn new(cfg: RateLimitConfig, timeout: Duration) -> Self {
        Self {
            limiter: RateLimiter::new(cfg),
            timeout,
        }
    }

    /// GET `url` with interpolated headers, returning the body bytes. The URL and
    /// header values may contain `${VAR}`/`${file:}` secrets; resolved values are
    /// never logged. Returns [`FetchError::Throttled`] when the limiter is backing
    /// off or the gateway answers 403/429, so the caller skips silently.
    pub fn get(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
    ) -> Result<Vec<u8>, FetchError> {
        match self.limiter.acquire() {
            Permit::Skip => return Err(FetchError::Throttled),
            Permit::Proceed => {}
        }
        let resolved_url = interpolate(url)?;
        let agent = ureq::AgentBuilder::new().timeout(self.timeout).build();
        let mut req = agent.get(&resolved_url);
        for (k, v) in headers {
            req = req.set(k, &interpolate(v)?);
        }
        match req.call() {
            Ok(resp) => {
                self.limiter.on_success();
                let mut buf = Vec::new();
                use std::io::Read;
                resp.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(|e| FetchError::Failed(format!("reading realtime body: {e}")))?;
                Ok(buf)
            }
            Err(ureq::Error::Status(status @ (403 | 429), resp)) => {
                let body = resp.into_string().ok();
                self.limiter.on_throttle(status, body.as_deref());
                Err(FetchError::Throttled)
            }
            Err(e) => Err(FetchError::Failed(format!("realtime fetch failed: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(threshold: u32) -> RateLimitConfig {
        RateLimitConfig {
            consecutive_failure_threshold: threshold,
            throttled_min_interval: Duration::from_secs(60),
        }
    }

    #[test]
    fn throttle_engages_after_threshold_and_clears_on_success() {
        let rl = RateLimiter::new(cfg(2));
        rl.on_throttle(429, None);
        assert!(!rl.is_throttled());
        rl.on_throttle(429, None);
        assert!(rl.is_throttled());
        rl.on_success();
        assert!(!rl.is_throttled());
    }

    #[test]
    fn http_403_engages_backoff_and_increments_failures() {
        let rl = RateLimiter::new(cfg(2));
        rl.on_throttle(403, Some("Out of call volume quota"));
        assert_eq!(rl.state.lock().unwrap().consecutive_failures, 1);
        assert!(!rl.is_throttled(), "one 403 is below threshold");
        rl.on_throttle(403, Some("Out of call volume quota"));
        assert!(rl.is_throttled(), "second 403 crosses threshold into backoff");
    }

    #[test]
    fn throttled_limiter_skips_next_request() {
        let rl = RateLimiter::new(cfg(1));
        rl.on_throttle(403, None);
        assert!(rl.is_throttled());
        assert!(!rl.would_skip(), "first acquire is the single probe");
        assert!(rl.would_skip(), "subsequent acquires within the window skip");
        assert!(rl.would_skip());
    }

    #[test]
    fn backoff_decays_on_success() {
        let rl = RateLimiter::new(cfg(1));
        rl.on_throttle(403, None);
        assert!(rl.is_throttled());
        assert!(!rl.would_skip(), "first acquire after backoff is the probe");
        assert!(rl.would_skip(), "subsequent acquires skip within the window");
        rl.on_success();
        assert!(!rl.is_throttled());
        assert_eq!(rl.state.lock().unwrap().consecutive_failures, 0);
        assert!(!rl.would_skip(), "throttle lifted → requests proceed again");
    }

    #[test]
    fn not_throttled_never_skips() {
        let rl = RateLimiter::new(cfg(3));
        assert!(!rl.would_skip());
        assert!(!rl.would_skip());
    }
}
