//! Blocking HTTP fetcher with a shared rate limiter for realtime polling.
//!
//! All realtime feeds hit the same provider gateway (shared `BMC_PARTNER_KEY`,
//! ~8 req/min / 12k/day). One [`Fetcher`] paces every request through a single
//! [`RateLimiter`]; on repeated `429`s it throttles all feeds to one request per
//! the configured interval until a request succeeds. Mirrors the limiter in the
//! `maas-rt` recorder. Runs in the poller's `spawn_blocking` context.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::ingestion::secrets::interpolate;

#[derive(Debug, Clone, Copy)]
pub struct RateLimitConfig {
    pub consecutive_429_threshold: u32,
    pub throttled_min_interval: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            consecutive_429_threshold: 3,
            throttled_min_interval: Duration::from_secs(60),
        }
    }
}

struct LimiterState {
    last_request: Option<Instant>,
    consecutive_429: u32,
    throttled: bool,
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
                consecutive_429: 0,
                throttled: false,
            }),
            cfg,
        }
    }

    /// Block until the next request is permitted (only delays while throttled).
    fn acquire(&self) {
        let mut st = self.state.lock().unwrap();
        if st.throttled {
            if let Some(last) = st.last_request {
                let elapsed = last.elapsed();
                if elapsed < self.cfg.throttled_min_interval {
                    let wait = self.cfg.throttled_min_interval - elapsed;
                    drop(st);
                    std::thread::sleep(wait);
                    st = self.state.lock().unwrap();
                }
            }
        }
        st.last_request = Some(Instant::now());
    }

    fn on_429(&self) {
        let mut st = self.state.lock().unwrap();
        st.consecutive_429 += 1;
        tracing::warn!(consecutive = st.consecutive_429, "realtime: received HTTP 429");
        if !st.throttled && st.consecutive_429 >= self.cfg.consecutive_429_threshold {
            st.throttled = true;
            tracing::error!("realtime: repeated 429s; throttling all feeds");
        }
    }

    fn on_success(&self) {
        let mut st = self.state.lock().unwrap();
        if st.throttled {
            tracing::info!("realtime: request succeeded; lifting throttle");
        }
        st.consecutive_429 = 0;
        st.throttled = false;
    }

    #[cfg(test)]
    fn is_throttled(&self) -> bool {
        self.state.lock().unwrap().throttled
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
    /// never logged.
    pub fn get(&self, url: &str, headers: &HashMap<String, String>) -> Result<Vec<u8>, String> {
        self.limiter.acquire();
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
                    .map_err(|e| format!("reading realtime body: {e}"))?;
                Ok(buf)
            }
            Err(ureq::Error::Status(429, _)) => {
                self.limiter.on_429();
                Err("realtime feed returned 429 Too Many Requests".to_string())
            }
            Err(e) => Err(format!("realtime fetch failed: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_engages_after_threshold_and_clears_on_success() {
        let rl = RateLimiter::new(RateLimitConfig {
            consecutive_429_threshold: 2,
            throttled_min_interval: Duration::from_secs(60),
        });
        rl.on_429();
        assert!(!rl.is_throttled());
        rl.on_429();
        assert!(rl.is_throttled());
        rl.on_success();
        assert!(!rl.is_throttled());
    }
}
