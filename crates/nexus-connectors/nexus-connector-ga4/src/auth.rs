use crate::config::Ga4ConnectorConfig;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nexus_core::{retry_with_backoff, NexusError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

const SCOPE: &str = "https://www.googleapis.com/auth/analytics.readonly";
const TOKEN_LIFETIME_SECONDS: u64 = 3600;

/// Google's own OAuth2 service-account JWT assertion claims — `scope`
/// instead of a `sub`, since this doesn't impersonate a Workspace
/// user (no domain-wide delegation). `aud` is the token endpoint
/// itself, `iat`/`exp` a short-lived window, same shape Google
/// documents for server-to-server service-account auth.
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Signs the JWT (RS256) and exchanges it for an access token — same
/// two-step flow `nexus-connector-salesforce`'s `auth.rs` (this repo)
/// uses, adapted to Google's claim shape. Retries transient failures.
pub(crate) async fn authenticate(
    client: &reqwest::Client,
    cfg: &Ga4ConnectorConfig,
) -> Result<String, NexusError> {
    let now = jsonwebtoken::get_current_timestamp();
    let claims = Claims {
        iss: &cfg.client_email,
        scope: SCOPE,
        aud: &cfg.token_uri,
        iat: now,
        exp: now + TOKEN_LIFETIME_SECONDS,
    };
    let key = EncodingKey::from_rsa_pem(cfg.private_key.as_bytes())
        .map_err(|e| NexusError::Connector(format!("invalid ga4 private_key PEM: {e}")))?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| NexusError::Connector(format!("failed to sign ga4 JWT: {e}")))?;

    let token_uri = cfg.token_uri.clone();
    let timeout_seconds = cfg.timeout_seconds;
    let retry = cfg.retry.clone();
    let client = client.clone();

    retry_with_backoff(&retry, "ga4 token exchange", || {
        let client = client.clone();
        let jwt = jwt.clone();
        let token_uri = token_uri.clone();
        async move {
            let response = tokio::time::timeout(
                Duration::from_secs(timeout_seconds),
                client
                    .post(&token_uri)
                    .form(&[
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", jwt.as_str()),
                    ])
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("ga4 token request timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("ga4 token request failed: {e}")))?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "ga4 token exchange failed ({status}): {body}"
                )));
            }

            let parsed: TokenResponse = response.json().await.map_err(|e| {
                NexusError::Connector(format!("ga4 token response parse failed: {e}"))
            })?;

            Ok(parsed.access_token)
        }
    })
    .await
}
