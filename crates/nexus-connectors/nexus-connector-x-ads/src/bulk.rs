use crate::config::XAdsConnectorConfig;
use crate::oauth1::sign_request;
use flate2::read::GzDecoder;
use nexus_core::NexusError;
use serde::Deserialize;
use std::io::Read;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct JobEntry {
    id: String,
    status: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Deserialize)]
struct CreateJobResponse {
    data: JobEntry,
}

#[derive(Deserialize)]
struct PollResponse {
    data: Vec<JobEntry>,
}

fn create_job_query(cfg: &XAdsConnectorConfig) -> Vec<(&'static str, String)> {
    let mut params = vec![
        ("entity", cfg.entity.as_str().to_string()),
        ("entity_ids", cfg.entity_ids.join(",")),
        ("metric_groups", cfg.metric_groups.join(",")),
        ("granularity", cfg.granularity.as_str().to_string()),
        ("start_time", cfg.start_time.clone()),
        ("end_time", cfg.end_time.clone()),
    ];
    if let Some(placement) = &cfg.placement {
        params.push(("placement", placement.clone()));
    }
    params
}

/// `POST /12/stats/jobs/accounts/:account_id` — creates an async stats
/// job. Real, confirmed endpoint/params (entity/entity_ids/metric_groups/
/// granularity/start_time/end_time, up to 20 entity_ids, max 90-day
/// unsegmented / 45-day segmented range — validated by the caller, not
/// here). Response shape (`{"data": {"id", "status", ...}}`) confirmed
/// via X's own async-analytics docs/dev-community threads, not against a
/// live account — same "researched, not yet tested against a real
/// account" posture `nexus-connector-databricks`/`nexus-connector-
/// snowflake` document for their own unverified specifics.
pub(crate) async fn create_job(
    client: &reqwest::Client,
    cfg: &XAdsConnectorConfig,
) -> Result<String, NexusError> {
    let url = format!(
        "{}/12/stats/jobs/accounts/{}",
        cfg.api_base_url, cfg.account_id
    );
    let query = create_job_query(cfg);
    let query_refs: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();

    let auth = sign_request(
        "POST",
        &url,
        &query_refs,
        &cfg.consumer_key,
        &cfg.consumer_secret,
        &cfg.access_token,
        &cfg.access_token_secret,
    );

    let response = client
        .post(&url)
        .header("Authorization", auth)
        .query(&query)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("x-ads create job failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "x-ads create job failed ({status}): {text}"
        )));
    }

    let parsed: CreateJobResponse = response.json().await.map_err(|e| {
        NexusError::Connector(format!("x-ads create job response parse failed: {e}"))
    })?;

    Ok(parsed.data.id)
}

/// Polls `GET /12/stats/jobs/accounts/:account_id?job_ids=...` until the
/// job's `status` is `"SUCCESS"` (returning its download `url`), bounded
/// by `cfg.timeout_seconds`. Fixed-interval polling, no exponential
/// backoff — same choice `nexus-connector-meta-ads`'s `poll_until_
/// complete` makes for its own async job. **Unverified**: the exact
/// failure-status string(s) X's API uses (docs/threads confirm
/// `"PROCESSING"`/`"SUCCESS"` but not a documented failure value) — any
/// status other than `"SUCCESS"` is treated as "still processing" until
/// the deadline, so a genuinely failed job surfaces as a timeout error
/// rather than a more specific one until this is confirmed against a
/// real account.
pub(crate) async fn poll_until_complete(
    client: &reqwest::Client,
    cfg: &XAdsConnectorConfig,
    job_id: &str,
) -> Result<String, NexusError> {
    let url = format!(
        "{}/12/stats/jobs/accounts/{}",
        cfg.api_base_url, cfg.account_id
    );
    let deadline = Instant::now() + Duration::from_secs(cfg.timeout_seconds);
    let query_refs: [(&str, &str); 1] = [("job_ids", job_id)];

    loop {
        let auth = sign_request(
            "GET",
            &url,
            &query_refs,
            &cfg.consumer_key,
            &cfg.consumer_secret,
            &cfg.access_token,
            &cfg.access_token_secret,
        );

        let response = client
            .get(&url)
            .header("Authorization", auth)
            .query(&query_refs)
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("x-ads job poll failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(NexusError::Connector(format!(
                "x-ads job poll failed ({status}): {text}"
            )));
        }

        let parsed: PollResponse = response
            .json()
            .await
            .map_err(|e| NexusError::Connector(format!("x-ads job status parse failed: {e}")))?;
        let entry = parsed
            .data
            .into_iter()
            .find(|e| e.id == job_id)
            .ok_or_else(|| {
                NexusError::Connector(format!("x-ads job poll response missing job {job_id}"))
            })?;

        if entry.status == "SUCCESS" {
            return entry.url.ok_or_else(|| {
                NexusError::Connector(format!("x-ads job {job_id} succeeded with no download url"))
            });
        }

        if Instant::now() >= deadline {
            return Err(NexusError::Connector(format!(
                "x-ads job {job_id} did not complete within {}s (last status: {})",
                cfg.timeout_seconds, entry.status
            )));
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_interval_seconds)).await;
    }
}

#[derive(Deserialize)]
struct StatsEntity {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    id_data: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct StatsFile {
    #[serde(default)]
    data: Vec<StatsEntity>,
}

/// Downloads the completed job's result file (gzip-compressed, plain GET
/// — the `url` from `poll_until_complete` is a pre-signed download link,
/// no OAuth signature needed on it, same as any other pre-signed-URL
/// download pattern) and gunzips it. `flate2` is a new dependency here —
/// no existing connector in this repo decodes gzip today (confirmed by
/// grep before writing this).
///
/// **Unverified**: the exact nesting of `id_data`'s per-segment metric
/// values (X's docs describe a `data_type`/`time_series_length` envelope
/// around per-entity `id_data` arrays, but the precise metric-value shape
/// inside each segment needs a real account/response to confirm). Each
/// `id_data` entry is treated here as a flat JSON object and every
/// configured `fields` name is looked up directly inside it (falling
/// back to null if absent) — correct if metrics sit at the top level of
/// each segment object, needs revisiting if X nests them one level
/// deeper (e.g. under a `"metrics"` key) once tested for real.
pub(crate) async fn fetch_results(
    client: &reqwest::Client,
    download_url: &str,
) -> Result<Vec<serde_json::Value>, NexusError> {
    let response = client
        .get(download_url)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("x-ads download failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        return Err(NexusError::Connector(format!(
            "x-ads download failed ({status})"
        )));
    }

    let compressed = response
        .bytes()
        .await
        .map_err(|e| NexusError::Connector(format!("x-ads download body read failed: {e}")))?;

    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = String::new();
    decoder
        .read_to_string(&mut decompressed)
        .map_err(|e| NexusError::Connector(format!("x-ads gunzip failed: {e}")))?;

    let file: StatsFile = serde_json::from_str(&decompressed)
        .map_err(|e| NexusError::Connector(format!("x-ads result file parse failed: {e}")))?;

    let mut rows = Vec::new();
    for entity in file.data {
        for segment in entity.id_data {
            let mut row = match segment {
                serde_json::Value::Object(map) => map,
                _ => serde_json::Map::new(),
            };
            if let Some(id) = &entity.id {
                row.insert("id".to_string(), serde_json::Value::String(id.clone()));
            }
            rows.push(serde_json::Value::Object(row));
        }
    }
    Ok(rows)
}
