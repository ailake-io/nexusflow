use crate::config::ShopifyConnectorConfig;
use nexus_core::{retry_with_backoff, NexusError};
use serde_json::Value;
use std::time::{Duration, Instant};

/// Runs a raw GraphQL request against the Admin API's single
/// `graphql.json` endpoint (confirmed real — GraphQL has one endpoint,
/// operation selection happens in the request body, unlike REST's
/// many paths). Returns the top-level `data` field; GraphQL-level
/// errors (the `errors` array) surface as a connector error rather
/// than being silently ignored.
///
/// Retries transient HTTP errors and Shopify's `429`/`503` rate-limit
/// responses, honoring the `Retry-After` header when present.
async fn run_graphql(
    client: &reqwest::Client,
    cfg: &ShopifyConnectorConfig,
    query: &str,
) -> Result<Value, NexusError> {
    let url = cfg.graphql_url();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;
    let body = serde_json::json!({ "query": query });
    let client = client.clone();
    let query = query.to_string();

    retry_with_backoff(&cfg.retry, "shopify graphql", || {
        let client = client.clone();
        let url = url.clone();
        let access_token = access_token.clone();
        let body = body.clone();
        let query = query.clone();
        async move {
            let response = tokio::time::timeout(
                Duration::from_secs(timeout_seconds),
                client
                    .post(&url)
                    .header("X-Shopify-Access-Token", &access_token)
                    .json(&body)
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("shopify graphql request timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("shopify graphql request failed: {e}")))?;

            // Shopify returns 429 when the bucket is exhausted; the
            // `Retry-After` header is seconds until the next request
            // is allowed.
            if response.status().as_u16() == 429 {
                let delay = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(2);
                tokio::time::sleep(Duration::from_secs(delay)).await;
                return Err(NexusError::Connector(format!(
                    "shopify graphql rate limited (429, retry-after={delay}s)"
                )));
            }

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "shopify graphql request failed ({status}): {text}"
                )));
            }

            let body: Value = response.json().await.map_err(|e| {
                NexusError::Connector(format!("shopify graphql response parse failed: {e}"))
            })?;

            if let Some(errors) = body.get("errors") {
                if !errors.as_array().is_some_and(|a| a.is_empty()) {
                    return Err(NexusError::Connector(format!(
                        "shopify graphql errors for query starting '{:.40}...': {errors}",
                        query
                    )));
                }
            }

            body.get("data").cloned().ok_or_else(|| {
                NexusError::Connector("shopify graphql response had no data field".into())
            })
        }
    })
    .await
}

/// `bulkOperationRunQuery` — real, confirmed async bulk-read job.
/// `query` (the user's raw GraphQL query body) is embedded as a
/// GraphQL string literal via `serde_json::to_string`, which produces
/// the same escaping GraphQL string literals need for the common case
/// (quotes, backslashes, newlines).
pub(crate) async fn start_bulk_query(
    client: &reqwest::Client,
    cfg: &ShopifyConnectorConfig,
    query: &str,
) -> Result<(), NexusError> {
    let escaped = serde_json::to_string(query)
        .map_err(|e| NexusError::Connector(format!("shopify: failed to escape bulk query: {e}")))?;
    let mutation = format!(
        "mutation {{ bulkOperationRunQuery(query: {escaped}) {{ bulkOperation {{ id status }} userErrors {{ field message }} }} }}"
    );
    let data = run_graphql(client, cfg, &mutation).await?;
    check_user_errors(&data, "bulkOperationRunQuery")
}

/// `stagedUploadsCreate` + multipart upload + `bulkOperationRunMutation`
/// — real, confirmed async bulk-write job. `mutation` is the caller's
/// full GraphQL mutation definition string (see
/// `ShopifyConnectorConfig::sink_mutation`'s doc comment).
pub(crate) async fn start_bulk_mutation(
    client: &reqwest::Client,
    cfg: &ShopifyConnectorConfig,
    mutation: &str,
    jsonl: Vec<u8>,
) -> Result<(), NexusError> {
    let staged = create_staged_upload(client, cfg).await?;
    upload_jsonl(client, &staged, jsonl).await?;

    let escaped_mutation = serde_json::to_string(mutation).map_err(|e| {
        NexusError::Connector(format!("shopify: failed to escape sink_mutation: {e}"))
    })?;
    let escaped_path = serde_json::to_string(&staged.resource_path).map_err(|e| {
        NexusError::Connector(format!("shopify: failed to escape staged upload path: {e}"))
    })?;
    let run = format!(
        "mutation {{ bulkOperationRunMutation(mutation: {escaped_mutation}, stagedUploadPath: {escaped_path}) \
         {{ bulkOperation {{ id status }} userErrors {{ field message }} }} }}"
    );
    let data = run_graphql(client, cfg, &run).await?;
    check_user_errors(&data, "bulkOperationRunMutation")
}

struct StagedUpload {
    url: String,
    resource_path: String,
    parameters: Vec<(String, String)>,
}

async fn create_staged_upload(
    client: &reqwest::Client,
    cfg: &ShopifyConnectorConfig,
) -> Result<StagedUpload, NexusError> {
    let query = "mutation { stagedUploadsCreate(input: [{resource: BULK_MUTATION_VARIABLES, \
                 filename: \"bulk_op_vars.jsonl\", mimeType: \"text/jsonl\", httpMethod: POST}]) \
                 { stagedTargets { url resourceUrl parameters { name value } } \
                 userErrors { field message } } }";
    let data = run_graphql(client, cfg, query).await?;
    check_user_errors(&data["stagedUploadsCreate"], "stagedUploadsCreate")?;

    let target = data["stagedUploadsCreate"]["stagedTargets"]
        .get(0)
        .ok_or_else(|| {
            NexusError::Connector("shopify: stagedUploadsCreate returned no stagedTargets".into())
        })?;
    let url = target["url"]
        .as_str()
        .ok_or_else(|| NexusError::Connector("shopify: staged target missing url".into()))?
        .to_string();
    let resource_path = target["resourceUrl"]
        .as_str()
        .ok_or_else(|| NexusError::Connector("shopify: staged target missing resourceUrl".into()))?
        .to_string();
    let parameters = target["parameters"]
        .as_array()
        .ok_or_else(|| NexusError::Connector("shopify: staged target missing parameters".into()))?
        .iter()
        .filter_map(|p| {
            Some((
                p["name"].as_str()?.to_string(),
                p["value"].as_str()?.to_string(),
            ))
        })
        .collect();

    Ok(StagedUpload {
        url,
        resource_path,
        parameters,
    })
}

/// The uploaded JSONL field must be the **last** multipart form field
/// — confirmed real requirement of Shopify's staged upload flow.
async fn upload_jsonl(
    client: &reqwest::Client,
    staged: &StagedUpload,
    jsonl: Vec<u8>,
) -> Result<(), NexusError> {
    let mut form = reqwest::multipart::Form::new();
    for (name, value) in &staged.parameters {
        form = form.text(name.clone(), value.clone());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(jsonl)
            .file_name("bulk_op_vars.jsonl")
            .mime_str("text/jsonl")
            .map_err(|e| NexusError::Connector(format!("shopify: invalid jsonl mime: {e}")))?,
    );

    let response = client
        .post(&staged.url)
        .multipart(form)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("shopify: staged upload failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(NexusError::Connector(format!(
            "shopify: staged upload failed ({status}): {text}"
        )));
    }
    Ok(())
}

/// Polls `currentBulkOperation` until `status` is `COMPLETED`
/// (success) or `FAILED`/`CANCELED`/`EXPIRED` (error). Returns the
/// result file's download URL.
pub(crate) async fn poll_until_complete(
    client: &reqwest::Client,
    cfg: &ShopifyConnectorConfig,
) -> Result<Option<String>, NexusError> {
    let query = "{ currentBulkOperation { id status url errorCode } }";
    let deadline = Instant::now() + Duration::from_secs(cfg.timeout_seconds);

    loop {
        let data = run_graphql(client, cfg, query).await?;
        let op = &data["currentBulkOperation"];
        let status = op["status"].as_str().unwrap_or("");

        match status {
            "COMPLETED" => return Ok(op["url"].as_str().map(str::to_string)),
            "FAILED" | "CANCELED" | "EXPIRED" => {
                return Err(NexusError::Connector(format!(
                    "shopify bulk operation ended in status {status}: {op}"
                )))
            }
            _ => {}
        }

        if Instant::now() >= deadline {
            return Err(NexusError::Connector(format!(
                "shopify bulk operation did not complete within {}s (last status: {status})",
                cfg.timeout_seconds
            )));
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_interval_seconds)).await;
    }
}

/// Downloads the bulk operation's JSONL result file — this is a plain
/// signed URL Shopify hands back, not authenticated with the Admin API
/// token.
pub(crate) async fn download_jsonl(
    client: &reqwest::Client,
    url: &str,
) -> Result<Vec<Value>, NexusError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| NexusError::Connector(format!("shopify: jsonl download failed: {e}")))?;

    if !response.status().is_success() {
        let status = response.status();
        return Err(NexusError::Connector(format!(
            "shopify: jsonl download failed ({status})"
        )));
    }

    let text = response
        .text()
        .await
        .map_err(|e| NexusError::Connector(format!("shopify: jsonl body read failed: {e}")))?;

    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|e| NexusError::Serialization(format!("shopify: invalid jsonl line: {e}")))
        })
        .collect()
}

fn check_user_errors(data: &Value, op_name: &str) -> Result<(), NexusError> {
    let errors = data
        .get("userErrors")
        .or_else(|| data.pointer(&format!("/{op_name}/userErrors")))
        .and_then(|v| v.as_array());
    if let Some(errors) = errors {
        if !errors.is_empty() {
            return Err(NexusError::Connector(format!(
                "shopify {op_name} userErrors: {errors:?}"
            )));
        }
    }
    Ok(())
}
