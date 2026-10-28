use crate::config::YoutubeAnalyticsConnectorConfig;
use nexus_core::NexusError;
use serde::Deserialize;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Same pattern `nexus-connector-linkedin-ads`'s `resolve_access_token`
/// (this repo) uses: if `refresh_token` is configured, exchange it for
/// a fresh access token via the standard OAuth2 refresh grant;
/// otherwise use the externally obtained `access_token` as-is. Unlike
/// LinkedIn, there's no partner-approval gate involved here — this
/// refresh genuinely works for any OAuth2 client with a valid refresh
/// token.
pub(crate) async fn resolve_access_token(
    client: &reqwest::Client,
    cfg: &YoutubeAnalyticsConnectorConfig,
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
        .map_err(|e| {
            NexusError::Connector(format!("youtube-analytics token refresh failed: {e}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "youtube-analytics token refresh failed ({status}): {body}"
        )));
    }

    let parsed: TokenResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!(
            "youtube-analytics token response parse failed: {e}"
        ))
    })?;

    Ok(parsed.access_token)
}
