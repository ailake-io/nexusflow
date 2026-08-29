use crate::error::ApiError;
use axum::extract::{Extension, Request};
use axum::middleware::Next;
use axum::response::Response;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-IP rate limiter for the login endpoint. Keeps a small rolling window of
/// timestamps and evicts stale entries lazily on each check. Caps the total
/// number of tracked IPs to bound memory under distributed brute-force probes.
pub struct LoginRateLimiter {
    attempts: DashMap<IpAddr, Vec<Instant>>,
    window: Duration,
    max_attempts: usize,
    max_ips: usize,
}

impl LoginRateLimiter {
    pub fn new(window: Duration, max_attempts: usize) -> Self {
        Self::with_max_ips(window, max_attempts, 10_000)
    }

    pub fn with_max_ips(window: Duration, max_attempts: usize, max_ips: usize) -> Self {
        Self {
            attempts: DashMap::new(),
            window,
            max_attempts,
            max_ips,
        }
    }

    /// Returns `true` if the request is within the limit. A request that
    /// exceeds the limit is not recorded, so the window can drain.
    pub fn is_allowed(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        self.evict_if_full(now);
        let mut entry = self.attempts.entry(ip).or_default();
        entry.retain(|t| now.duration_since(*t) < self.window);
        if entry.len() >= self.max_attempts {
            return false;
        }
        entry.push(now);
        true
    }

    /// Simple LRU-ish eviction: if we've reached `max_ips`, remove the entry
    /// with the oldest most-recent attempt. This is opportunistic cleanup,
    /// not strict LRU, but sufficient to cap memory.
    fn evict_if_full(&self, now: Instant) {
        if self.attempts.len() < self.max_ips {
            return;
        }
        let mut oldest_ip = None;
        let mut oldest_ts = now;
        for entry in self.attempts.iter() {
            let last = entry.value().last().copied().unwrap_or(now);
            if last < oldest_ts {
                oldest_ts = last;
                oldest_ip = Some(*entry.key());
            }
        }
        if let Some(ip) = oldest_ip {
            self.attempts.remove(&ip);
        }
    }
}

impl Default for LoginRateLimiter {
    fn default() -> Self {
        // 5 login attempts per minute is enough for legitimate users and
        // frustrates online brute-force attacks against Argon2 hashes.
        Self::new(Duration::from_secs(60), 5)
    }
}

/// Axum middleware that rejects login requests beyond the per-IP rate limit.
/// Uses `X-Forwarded-For` when present (trusted-proxy deployments). When the
/// header is absent the request is still allowed through (rate-limiting by
/// direct peer address is done at the reverse-proxy layer in this deployment
/// model; avoiding `ConnectInfo` here keeps the middleware free of the axum
/// version conflict introduced by `milvus-sdk-rust`).
pub async fn login_rate_limit(
    Extension(limiter): Extension<Arc<LoginRateLimiter>>,
    headers: axum::http::HeaderMap,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let ip_str = extract_client_ip(&headers);
    let ip = ip_str
        .parse()
        .unwrap_or_else(|_| std::net::IpAddr::from([0, 0, 0, 0]));
    if !limiter.is_allowed(ip) {
        crate::server_metrics::record_rate_limit_rejection();
        return Err(ApiError::too_many_requests(
            "too many login attempts, try again later",
        ));
    }
    req.extensions_mut().insert(ClientIp(ip_str));
    Ok(next.run(req).await)
}

/// Client IP extracted by `login_rate_limit` and passed downstream so handlers
/// don't need to depend on `axum::extract::Request` (which conflicts with the
/// older axum pulled in by some connector dependencies).
#[derive(Clone, Debug)]
pub struct ClientIp(pub String);

fn extract_client_ip(headers: &axum::http::HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| std::net::IpAddr::from([0, 0, 0, 0]).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_requests_under_the_limit() {
        let limiter = LoginRateLimiter::new(Duration::from_secs(60), 3);
        let ip = IpAddr::from([127, 0, 0, 1]);
        assert!(limiter.is_allowed(ip));
        assert!(limiter.is_allowed(ip));
        assert!(limiter.is_allowed(ip));
        assert!(!limiter.is_allowed(ip));
    }

    #[test]
    fn tracks_ips_independently() {
        let limiter = LoginRateLimiter::new(Duration::from_secs(60), 1);
        let ip_a = IpAddr::from([127, 0, 0, 1]);
        let ip_b = IpAddr::from([127, 0, 0, 2]);
        assert!(limiter.is_allowed(ip_a));
        assert!(!limiter.is_allowed(ip_a));
        assert!(limiter.is_allowed(ip_b));
    }

    #[test]
    fn caps_tracked_ips_and_evicts_oldest() {
        let limiter = LoginRateLimiter::with_max_ips(Duration::from_secs(60), 1, 1);
        let ip_a = IpAddr::from([127, 0, 0, 1]);
        let ip_b = IpAddr::from([127, 0, 0, 2]);
        assert!(limiter.is_allowed(ip_a));
        // ip_b evicts ip_a because max_ips is 1.
        assert!(limiter.is_allowed(ip_b));
        // ip_a is now unknown again, so it gets a fresh slot (evicting ip_b).
        assert!(limiter.is_allowed(ip_a));
        // ip_b was evicted, so its previous attempt no longer counts.
        assert!(limiter.is_allowed(ip_b));
    }
}
