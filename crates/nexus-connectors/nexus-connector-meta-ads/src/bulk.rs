use crate::config::MetaAdsConnectorConfig;
use nexus_core::NexusError;
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct CreateJobResponse {
    report_run_id: String,
}

#[derive(Deserialize)]
struct JobStatus {
    async_status: String,
}

/// `POST /{ad_account_id}/insights?async=true` — confirmed real
/// pattern for large/heavy pulls, same job-then-poll shape
/// `nexus-connector-salesforce`'s Bulk API 2.0 jobs use, just Graph
/// API instead. Always async in v1 — simpler and more robust for
/// large date ranges than deciding sync vs async per request, matching
/// Meta's own best-practice guidance to always run heavy pulls async.
pub(crate) async fn create_job(
    client: &reqwest::Client,
    cfg: &MetaAdsConnectorConfig,
) -> Result<String, NexusError> {
    let url = format!(
        "{}/{}/act_{}/insights",
        cfg.graph_api_base_url, cfg.api_version, cfg.ad_account_id
    );

    let mut params: Vec<(&str, String)> = vec![
        ("async", "true".to_string()),
        ("fields", cfg.fields.join(",")),
        ("level", cfg.level.as_str().to_string()),
    ];
    if !cfg.breakdowns.is_empty() {
        params.push(("breakdowns", cfg.breakdowns.join(",")));
    }
    if let Some(preset) = &cfg.date_preset {
        params.push(("date_preset", preset.clone()));
    } else if let (Some(since), Some(until)) = (&cfg.since, &cfg.until) {
        params.push((
            "time_range",
            serde_json::json!({ "since": since, "until": until }).to_string(),
        ));
    }

    let response = client
        .post(&url)
        .bearer_auth(&cfg.access_token)
        .query(&params)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("meta-ads create job failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "meta-ads create job failed ({status}): {text}"
        )));
    }

    let parsed: CreateJobResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!("meta-ads create job response parse failed: {e}"))
    })?;

    Ok(parsed.report_run_id)
}

/// Polls `GET /{report_run_id}` until `async_status` is `"Job
/// Completed"` (success) or `"Job Failed"`/`"Job Skipped"` (error).
/// Bounded by `cfg.timeout_seconds` from the moment polling starts —
/// Meta itself times out queued jobs after 60 minutes, so this
/// connector's own timeout should be at or above that (default
/// `3600`, see `config.rs`).
pub(crate) async fn poll_until_complete(
    client: &reqwest::Client,
    cfg: &MetaAdsConnectorConfig,
    report_run_id: &str,
) -> Result<(), NexusError> {
    let url = format!(
        "{}/{}/{report_run_id}",
        cfg.graph_api_base_url, cfg.api_version
    );
    let deadline = Instant::now() + Duration::from_secs(cfg.timeout_seconds);

    loop {
        let response = client
            .get(&url)
            .bearer_auth(&cfg.access_token)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("meta-ads job poll failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "meta-ads job poll failed ({status}): {text}"
            )));
        }

        let text = response.text().await.map_err(|e| {
            NexusError::Connector(format!("meta-ads job poll body read failed: {e}"))
        })?;
        let status: JobStatus = serde_json::from_str(&text)
            .map_err(|e| NexusError::Connector(format!("meta-ads job status parse failed: {e}")))?;

        match status.async_status.as_str() {
            "Job Completed" => return Ok(()),
            "Job Failed" | "Job Skipped" => {
                return Err(NexusError::Connector(format!(
                    "meta-ads job {report_run_id} ended in status {}: {text}",
                    status.async_status
                )))
            }
            _ => {}
        }

        if Instant::now() >= deadline {
            return Err(NexusError::Connector(format!(
                "meta-ads job {report_run_id} did not complete within {}s (last status: {})",
                cfg.timeout_seconds, status.async_status
            )));
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_interval_seconds)).await;
    }
}

#[derive(Deserialize)]
struct InsightsPage {
    #[serde(default)]
    data: Vec<serde_json::Value>,
    #[serde(default)]
    paging: Option<Paging>,
}

#[derive(Deserialize)]
struct Paging {
    #[serde(default)]
    next: Option<String>,
}

/// `GET /{report_run_id}/insights`, paginated via `paging.next` (a
/// full URL Meta returns, already carrying the cursor — confirmed real
/// cursor-based pagination shape).
pub(crate) async fn fetch_results(
    client: &reqwest::Client,
    cfg: &MetaAdsConnectorConfig,
    report_run_id: &str,
) -> Result<Vec<serde_json::Value>, NexusError> {
    let mut url = format!(
        "{}/{}/{report_run_id}/insights",
        cfg.graph_api_base_url, cfg.api_version
    );
    let mut rows = Vec::new();

    loop {
        let response = client
            .get(&url)
            .bearer_auth(&cfg.access_token)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("meta-ads fetch results failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "meta-ads fetch results failed ({status}): {text}"
            )));
        }

        let page: InsightsPage = response.json().await.map_err(|e| {
            NexusError::Connector(format!("meta-ads results page parse failed: {e}"))
        })?;
        rows.extend(page.data);

        match page.paging.and_then(|p| p.next) {
            Some(next) => url = next,
            None => break,
        }
    }

    Ok(rows)
}
