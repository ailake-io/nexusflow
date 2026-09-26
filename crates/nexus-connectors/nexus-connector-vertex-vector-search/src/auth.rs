use crate::config::VertexVectorSearchConnectorConfig;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nexus_core::NexusError;
use serde::{Deserialize, Serialize};

const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Same Google service-account JWT assertion flow
/// `nexus-connector-ga4`'s `auth.rs` (this repo) uses, just a
/// different OAuth2 scope for the Vertex AI API.
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

pub(crate) async fn authenticate(
    client: &reqwest::Client,
    cfg: &VertexVectorSearchConnectorConfig,
) -> Result<String, NexusError> {
    let now = jsonwebtoken::get_current_timestamp();
    let claims = Claims {
        iss: &cfg.client_email,
        scope: SCOPE,
        aud: &cfg.token_uri,
        iat: now,
        exp: now + 3600,
    };
    let key = EncodingKey::from_rsa_pem(cfg.private_key.as_bytes()).map_err(|e| {
        NexusError::Connector(format!("invalid vertex-vector-search private_key PEM: {e}"))
    })?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key).map_err(|e| {
        NexusError::Connector(format!("failed to sign vertex-vector-search JWT: {e}"))
    })?;

    let response = client
        .post(&cfg.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", jwt.as_str()),
        ])
        .send()
        .await
        .map_err(|e| {
            NexusError::Connector(format!("vertex-vector-search token request failed: {e}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "vertex-vector-search token exchange failed ({status}): {body}"
        )));
    }

    let parsed: TokenResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!(
            "vertex-vector-search token response parse failed: {e}"
        ))
    })?;

    Ok(parsed.access_token)
}
