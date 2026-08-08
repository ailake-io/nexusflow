use crate::error::ApiError;
use axum::extract::{Extension, FromRef, FromRequestParts, State};
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::Response;
use axum::RequestExt;
use dashmap::DashMap;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 4 global roles, hierarchical (`Read` < `Execute` < `Write` < `Admin`) —
/// no per-resource scope yet, see ARCHITECTURE.md §10 "Débito conhecido".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Read,
    Execute,
    Write,
    Admin,
}

/// JWT payload. `role` is trusted as-is once the signature validates — no
/// per-request DB lookup, so a revoked/demoted user stays valid until their
/// token expires (acceptable for MVP self-host; a real revocation list is
/// out of scope here).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: Role,
    pub exp: usize,
}

/// In-memory revocation list for JWTs. Sufficient for single-node
/// deployments; a multi-node setup would need a shared store (Redis, DB).
#[derive(Clone, Default)]
pub struct TokenBlocklist {
    entries: Arc<DashMap<String, Instant>>,
}

impl TokenBlocklist {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }

    /// Records a token as revoked until `exp` (Unix seconds). Called by
    /// `POST /auth/logout` and by role-demotion flows.
    pub fn revoke(&self, token: String, exp: usize) {
        let now = jsonwebtoken::get_current_timestamp();
        let ttl = exp.saturating_sub(now as usize) as u64;
        let expiry = Instant::now() + Duration::from_secs(ttl);
        self.entries.insert(token, expiry);
    }

    pub fn is_revoked(&self, token: &str) -> bool {
        self.cleanup_expired();
        self.entries.contains_key(token)
    }

    /// Removes expired entries opportunistically. Not strictly required for
    /// correctness (expired tokens fail validation anyway), but keeps memory
    /// bounded.
    fn cleanup_expired(&self) {
        let now = Instant::now();
        self.entries.retain(|_, expiry| *expiry > now);
    }
}

/// Encodes/decodes JWTs with a single HS256 secret. The secret comes from
/// `NEXUS_JWT_SECRET` at server startup — never hardcoded (ARCHITECTURE.md §10).
#[derive(Clone)]
pub struct JwtCodec {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
    token_ttl_seconds: u64,
    blocklist: TokenBlocklist,
}

impl JwtCodec {
    #[allow(dead_code)]
    pub fn new(secret: &[u8], token_ttl_seconds: u64) -> Self {
        Self::with_blocklist(secret, token_ttl_seconds, TokenBlocklist::new())
    }

    pub fn with_blocklist(
        secret: &[u8],
        token_ttl_seconds: u64,
        blocklist: TokenBlocklist,
    ) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret),
            decoding_key: DecodingKey::from_secret(secret),
            validation: Validation::new(Algorithm::HS256),
            token_ttl_seconds,
            blocklist,
        }
    }

    pub fn issue(&self, subject: &str, role: Role) -> Result<String, ApiError> {
        let exp = jsonwebtoken::get_current_timestamp() + self.token_ttl_seconds;
        let claims = Claims {
            sub: subject.to_string(),
            role,
            exp: exp as usize,
        };
        encode(&Header::new(Algorithm::HS256), &claims, &self.encoding_key)
            .map_err(|e| ApiError::internal(format!("token issuance failed: {e}")))
    }

    pub fn blocklist(&self) -> &TokenBlocklist {
        &self.blocklist
    }

    /// `pub(crate)` (not private) because the progress WebSocket route
    /// can't use the `Claims` extractor above — browsers' `WebSocket` API
    /// can't set an `Authorization` header, so that route takes the token
    /// as a query param and verifies it directly (see `lib.rs`).
    pub(crate) fn verify(&self, token: &str) -> Result<Claims, ApiError> {
        if self.blocklist.is_revoked(token) {
            return Err(ApiError::unauthorized("token has been revoked"));
        }
        decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| ApiError::unauthorized(format!("invalid token: {e}")))
    }
}

/// Extracts and validates the caller's JWT from `Authorization: Bearer <token>`.
/// Any handler or middleware can pull this in directly; it's also the first
/// step `require_role` runs before checking role sufficiency.
impl<S> FromRequestParts<S> for Claims
where
    JwtCodec: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let jwt = JwtCodec::from_ref(state);
        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;
        let token = header.strip_prefix("Bearer ").ok_or_else(|| {
            ApiError::unauthorized("Authorization header must be 'Bearer <token>'")
        })?;
        jwt.verify(token)
    }
}

/// RBAC middleware — checked before the handler runs, never inside it
/// (ARCHITECTURE.md §10). Mount via `.layer(Extension(Role::X)).layer(from_fn_with_state(state, require_role::<AppState>))`
/// on the sub-router that needs role `X` or higher; `Claims` (JWT validity)
/// is itself an extractor dependency, so an invalid/missing token is
/// rejected here too, before role comparison. Generic over `S` so this
/// module stays decoupled from the concrete `AppState` type defined in `lib.rs`.
pub async fn require_role<S>(
    Extension(min_role): Extension<Role>,
    State(state): State<S>,
    mut req: axum::extract::Request,
    next: Next,
) -> Result<Response, ApiError>
where
    JwtCodec: FromRef<S>,
    S: Clone + Send + Sync + 'static,
{
    let claims: Claims = req.extract_parts_with_state(&state).await?;
    if claims.role < min_role {
        return Err(ApiError::forbidden(format!(
            "requires {min_role:?} role or higher, caller has {:?}",
            claims.role
        )));
    }
    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_ordering_is_hierarchical() {
        assert!(Role::Admin > Role::Write);
        assert!(Role::Write > Role::Execute);
        assert!(Role::Execute > Role::Read);
    }

    #[test]
    fn issued_token_round_trips_with_correct_role() {
        let jwt = JwtCodec::new(b"test-secret", 3600);
        let token = jwt.issue("alice", Role::Write).unwrap();
        let claims = jwt.verify(&token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.role, Role::Write);
    }

    #[test]
    fn tampered_token_is_rejected() {
        let jwt = JwtCodec::new(b"test-secret", 3600);
        let token = jwt.issue("alice", Role::Read).unwrap();
        let other_jwt = JwtCodec::new(b"different-secret", 3600);
        assert!(other_jwt.verify(&token).is_err());
    }

    #[test]
    fn revoked_token_is_rejected() {
        let jwt = JwtCodec::new(b"test-secret", 3600);
        let token = jwt.issue("alice", Role::Read).unwrap();
        let claims = jwt.verify(&token).unwrap();
        jwt.blocklist().revoke(token.clone(), claims.exp);
        assert!(jwt.verify(&token).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        // Built directly (not via `issue`) with `exp` in the distant past,
        // so this doesn't depend on wall-clock timing or sleeping.
        let encoding_key = EncodingKey::from_secret(b"test-secret");
        let expired_claims = Claims {
            sub: "alice".to_string(),
            role: Role::Read,
            exp: 1,
        };
        let expired_token = encode(
            &Header::new(Algorithm::HS256),
            &expired_claims,
            &encoding_key,
        )
        .unwrap();

        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        let decoding_key = DecodingKey::from_secret(b"test-secret");
        assert!(decode::<Claims>(&expired_token, &decoding_key, &validation).is_err());
    }
}
