use crate::config::LinkedinAdsConnectorConfig;
use nexus_core::NexusError;
use serde::Deserialize;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Returns a usable access token. If `refresh_token` (MDP-partner-only)
/// is configured, exchanges it for a fresh access token via the
/// standard OAuth2 refresh grant; otherwise returns the externally
/// obtained `access_token` as-is (the common, non-MDP case — see
/// `config.rs`'s doc comment for why this connector can't drive the
/// 3-legged consent flow itself).
pub(crate) async fn resolve_access_token(
    client: &reqwest::Client,
    cfg: &LinkedinAdsConnectorConfig,
) -> Result<String, NexusError> {
    let (Some(refresh_token), Some(client_id), Some(client_secret)) =
        (&cfg.refresh_token, &cfg.client_id, &cfg.client_secret)
    else {
        return Ok(cfg.access_token.clone());
    };

    let response = client
        .post(&cfg.token_uri)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token.as_str()),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
        ])
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("linkedin-ads token refresh failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "linkedin-ads token refresh failed ({status}): {body}"
        )));
    }

    let parsed: TokenResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!("linkedin-ads token response parse failed: {e}"))
    })?;

    Ok(parsed.access_token)
}
