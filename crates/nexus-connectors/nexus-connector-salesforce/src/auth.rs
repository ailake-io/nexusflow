use crate::config::SalesforceConnectorConfig;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use nexus_core::NexusError;
use serde::{Deserialize, Serialize};

/// JWT Bearer Flow claims — `iss`/`sub`/`aud`/`exp` are exactly the
/// fields Salesforce's token endpoint expects, confirmed against
/// Salesforce Help's JWT Bearer Flow article (2026-08-19): `iss` is the
/// Connected App's Consumer Key, `sub` the integration user's username,
/// `aud` the token endpoint's own host, `exp` a short-lived expiry (a
/// few minutes is enough — the JWT is exchanged immediately, never
/// reused).
#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    exp: u64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    instance_url: String,
}

pub struct SalesforceSession {
    pub access_token: String,
    pub instance_url: String,
}

/// Signs the JWT (RS256) and exchanges it for an access token +
/// instance URL. This flow never issues a refresh token — v1 just
/// re-authenticates per `connect()` rather than tracking token expiry,
/// same simplicity every other connector in this workspace applies to
/// its auth.
pub(crate) async fn authenticate(
    client: &reqwest::Client,
    cfg: &SalesforceConnectorConfig,
) -> Result<SalesforceSession, NexusError> {
    let exp = jsonwebtoken::get_current_timestamp() + 180;
    let claims = Claims {
        iss: &cfg.client_id,
        sub: &cfg.username,
        aud: &cfg.login_url,
        exp,
    };
    let key = EncodingKey::from_rsa_pem(cfg.private_key.as_bytes())
        .map_err(|e| NexusError::Connector(format!("invalid salesforce private_key PEM: {e}")))?;
    let jwt = encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| NexusError::Connector(format!("failed to sign salesforce JWT: {e}")))?;

    let url = format!("{}/services/oauth2/token", cfg.login_url);
    let response = client
        .post(&url)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", jwt.as_str()),
        ])
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("salesforce token request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce token exchange failed ({status}): {body}"
        )));
    }

    let parsed: TokenResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!("salesforce token response parse failed: {e}"))
    })?;

    Ok(SalesforceSession {
        access_token: parsed.access_token,
        instance_url: parsed.instance_url,
    })
}
