use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::ingestion::secrets::interpolate;

#[derive(Debug)]
pub enum FetchError {
    Throttled,
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
    pub consecutive_failure_threshold: u32,
    pub throttled_min_interval: Duration,
}

enum Permit {
    Proceed,
    Skip,
}

struct LimiterState {
    last_request: Option<Instant>,
    consecutive_failures: u32,
    throttled: bool,
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

    /// The URL and header values may contain `${VAR}`/`${file:}` secrets; resolved
    /// values must never be logged.
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
            Err(e) => Err(FetchError::Failed(format!(
                "realtime fetch failed: {}",
                redact_ureq_error(&e)
            ))),
        }
    }
}

// A ureq error's Display can embed the resolved (secret-bearing) URL, which must
// never be logged. Drop everything from the first URL token onward.
fn redact_ureq_error(e: &ureq::Error) -> String {
    redact_url_in(&e.to_string())
}

fn redact_url_in(s: &str) -> String {
    match s.find("http://").or_else(|| s.find("https://")) {
        Some(i) => format!("{}<url redacted>", &s[..i]),
        None => s.to_string(),
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
    fn redact_url_in_strips_secret_bearing_url() {
        let leaked = "Network Error: https://gw.example/rt?subscription-key=SECRET for url";
        let s = redact_url_in(leaked);
        assert!(!s.contains("SECRET"), "key must not survive: {s}");
        assert!(!s.contains("https://"), "no raw URL: {s}");
    }

    #[test]
    fn not_throttled_never_skips() {
        let rl = RateLimiter::new(cfg(3));
        assert!(!rl.would_skip());
        assert!(!rl.would_skip());
    }
}
