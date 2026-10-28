use crate::auth::SalesforceSession;
use crate::config::SalesforceConnectorConfig;
use nexus_core::NexusError;
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
pub(crate) struct CreatedJob {
    pub id: String,
    #[serde(rename = "contentUrl")]
    pub content_url: Option<String>,
}

#[derive(Deserialize)]
struct JobStatus {
    state: String,
}

/// `POST jobs/{kind}` — `kind` is `"ingest"` or `"query"`, the only two
/// Bulk API 2.0 job families this connector uses. Body shape differs
/// per caller (`source.rs` sends `{operation:"query", query}`,
/// `sink.rs` sends `{object, operation, externalIdFieldName,
/// lineEnding, contentType}`) so it's passed through as-is.
pub(crate) async fn create_job(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
    kind: &str,
    body: &serde_json::Value,
) -> Result<CreatedJob, NexusError> {
    let url = format!(
        "{}/services/data/{}/jobs/{kind}",
        session.instance_url, cfg.api_version
    );
    let response = client
        .post(&url)
        .bearer_auth(&session.access_token)
        .json(body)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("salesforce create {kind} job failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce create {kind} job failed ({status}): {text}"
        )));
    }

    response.json().await.map_err(|e| {
        NexusError::Connector(format!("salesforce {kind} job response parse failed: {e}"))
    })
}

/// Polls `GET jobs/{kind}/{job_id}` until `state` is `JobComplete`
/// (success) or `Failed`/`Aborted` (error, with the raw response body
/// included since Salesforce's exact failure-reason field name across
/// job types isn't confirmed against a real org yet — see README).
/// Bounded by `cfg.timeout_seconds` from the moment polling starts.
pub(crate) async fn poll_until_complete(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
    kind: &str,
    job_id: &str,
) -> Result<(), NexusError> {
    let url = format!(
        "{}/services/data/{}/jobs/{kind}/{job_id}",
        session.instance_url, cfg.api_version
    );
    let deadline = Instant::now() + Duration::from_secs(cfg.timeout_seconds);

    loop {
        let response = client
            .get(&url)
            .bearer_auth(&session.access_token)
            .send()
            .await
            .map_err(|e| {
                NexusError::Connector(format!("salesforce {kind} job poll failed: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "salesforce {kind} job poll failed ({status}): {text}"
            )));
        }

        let text = response.text().await.map_err(|e| {
            NexusError::Connector(format!("salesforce {kind} job poll body read failed: {e}"))
        })?;
        let status: JobStatus = serde_json::from_str(&text).map_err(|e| {
            NexusError::Connector(format!("salesforce {kind} job status parse failed: {e}"))
        })?;

        match status.state.as_str() {
            "JobComplete" => return Ok(()),
            "Failed" | "Aborted" => {
                return Err(NexusError::Connector(format!(
                    "salesforce {kind} job {job_id} ended in state {}: {text}",
                    status.state
                )))
            }
            _ => {}
        }

        if Instant::now() >= deadline {
            return Err(NexusError::Connector(format!(
                "salesforce {kind} job {job_id} did not complete within {}s (last state: {})",
                cfg.timeout_seconds, status.state
            )));
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_interval_seconds)).await;
    }
}

/// Uploads CSV bytes to a job's `contentUrl` (relative to
/// `instance_url`, per Bulk API 2.0's ingest job creation response).
pub(crate) async fn upload_csv(
    client: &reqwest::Client,
    session: &SalesforceSession,
    content_url: &str,
    csv: Vec<u8>,
) -> Result<(), NexusError> {
    let url = format!("{}/{}", session.instance_url, content_url);
    let response = client
        .put(&url)
        .bearer_auth(&session.access_token)
        .header("Content-Type", "text/csv")
        .body(csv)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("salesforce CSV upload failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce CSV upload failed ({status}): {text}"
        )));
    }
    Ok(())
}

/// `PATCH jobs/ingest/{job_id}` with `state: UploadComplete` — tells
/// Salesforce the CSV body is fully uploaded and processing can start.
pub(crate) async fn mark_upload_complete(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
    job_id: &str,
) -> Result<(), NexusError> {
    let url = format!(
        "{}/services/data/{}/jobs/ingest/{job_id}",
        session.instance_url, cfg.api_version
    );
    let response = client
        .patch(&url)
        .bearer_auth(&session.access_token)
        .json(&serde_json::json!({ "state": "UploadComplete" }))
        .send()
        .await
        .map_err(|e| {
            NexusError::Connector(format!("salesforce mark-upload-complete failed: {e}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce mark-upload-complete failed ({status}): {text}"
        )));
    }
    Ok(())
}

/// `GET jobs/ingest/{job_id}/failedResults` — CSV with an `sf__Error`
/// column per failed row. Returns the raw CSV text; the caller decides
/// whether "just a header, no data rows" counts as "no failures".
pub(crate) async fn fetch_failed_results(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
    job_id: &str,
) -> Result<String, NexusError> {
    let url = format!(
        "{}/services/data/{}/jobs/ingest/{job_id}/failedResults",
        session.instance_url, cfg.api_version
    );
    let response = client
        .get(&url)
        .bearer_auth(&session.access_token)
        .send()
        .await
        .map_err(|e| {
            NexusError::Connector(format!("salesforce fetch failedResults failed: {e}"))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "salesforce fetch failedResults failed ({status}): {text}"
        )));
    }

    response.text().await.map_err(|e| {
        NexusError::Connector(format!("salesforce failedResults body read failed: {e}"))
    })
}

/// `GET jobs/query/{job_id}/results`, paginated via the `Sforce-Locator`
/// response header — confirmed against Salesforce Bulk API 2.0 Query
/// docs (2026-08-19): pass the previous response's locator back as
/// `?locator=...` to get the next page; an absent/empty locator means
/// there are no more pages. Every page after the first repeats the CSV
/// header row, so it's stripped before concatenating.
pub(crate) async fn fetch_query_results(
    client: &reqwest::Client,
    session: &SalesforceSession,
    cfg: &SalesforceConnectorConfig,
    job_id: &str,
) -> Result<Vec<u8>, NexusError> {
    let base = format!(
        "{}/services/data/{}/jobs/query/{job_id}/results",
        session.instance_url, cfg.api_version
    );
    let mut combined = Vec::new();
    let mut locator: Option<String> = None;
    let mut first_page = true;

    loop {
        let mut request = client.get(&base).bearer_auth(&session.access_token);
        if let Some(loc) = &locator {
            request = request.query(&[("locator", loc.as_str())]);
        }
        let response = request.send().await.map_err(|e| {
            NexusError::Connector(format!("salesforce query results fetch failed: {e}"))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "salesforce query results fetch failed ({status}): {text}"
            )));
        }

        let next_locator = response
            .headers()
            .get("Sforce-Locator")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = response.bytes().await.map_err(|e| {
            NexusError::Connector(format!("salesforce query results body read failed: {e}"))
        })?;

        if first_page {
            combined.extend_from_slice(&body);
            first_page = false;
        } else {
            combined.extend_from_slice(strip_first_line(&body));
        }

        match next_locator {
            Some(loc) if !loc.is_empty() && loc != "null" => locator = Some(loc),
            _ => break,
        }
    }

    let _ = cfg; // reserved for a future per-page maxRecords override
    Ok(combined)
}

fn strip_first_line(bytes: &[u8]) -> &[u8] {
    match bytes.iter().position(|&b| b == b'\n') {
        Some(idx) => &bytes[idx + 1..],
        None => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_header_line() {
        assert_eq!(strip_first_line(b"a,b\n1,2\n3,4\n"), b"1,2\n3,4\n");
    }

    #[test]
    fn strip_with_no_newline_yields_empty() {
        assert_eq!(strip_first_line(b"a,b"), b"");
    }
}
