use axum::extract::{ConnectInfo, Extension, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-IP rate limiter for the login endpoint. Keeps a small rolling window of
/// timestamps and evicts stale entries lazily on each check.
pub struct LoginRateLimiter {
    attempts: DashMap<IpAddr, Vec<Instant>>,
    window: Duration,
    max_attempts: usize,
}

impl LoginRateLimiter {
    pub fn new(window: Duration, max_attempts: usize) -> Self {
        Self {
            attempts: DashMap::new(),
            window,
            max_attempts,
        }
    }

    /// Returns `true` if the request is within the limit. A request that
    /// exceeds the limit is not recorded, so the window can drain.
    pub fn is_allowed(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut entry = self.attempts.entry(ip).or_default();
        entry.retain(|t| now.duration_since(*t) < self.window);
        if entry.len() >= self.max_attempts {
            return false;
        }
        entry.push(now);
        true
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
pub async fn login_rate_limit(
    Extension(limiter): Extension<Arc<LoginRateLimiter>>,
    req: Request,
    next: Next,
) -> Response {
    let ip = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or_else(|| std::net::IpAddr::from([0, 0, 0, 0]));
    if !limiter.is_allowed(ip) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, "60")],
            "too many login attempts, try again later",
        )
            .into_response();
    }
    next.run(req).await
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
}
