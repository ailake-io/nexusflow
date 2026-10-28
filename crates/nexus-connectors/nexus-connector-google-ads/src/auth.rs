use crate::config::GoogleAdsConnectorConfig;
use nexus_core::NexusError;
use serde::Deserialize;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Standard OAuth2 refresh-token exchange — real mechanism this API
/// uses (confirmed via Google Ads API docs), simpler than GA4's JWT
/// service-account flow since Google Ads API access is tied to a real
/// user/manager account, not a service account.
pub(crate) async fn authenticate(
    client: &reqwest::Client,
    cfg: &GoogleAdsConnectorConfig,
) -> Result<String, NexusError> {
    let response = client
        .post(&cfg.token_uri)
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
            ("refresh_token", cfg.refresh_token.as_str()),
        ])
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("google-ads token request failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "google-ads token exchange failed ({status}): {body}"
        )));
    }

    let parsed: TokenResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!("google-ads token response parse failed: {e}"))
    })?;

    Ok(parsed.access_token)
}
