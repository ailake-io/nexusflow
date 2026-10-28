use crate::config::HubspotConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via the CRM v3 Objects API's list endpoint
/// (`GET /crm/v3/objects/{object_type}`) — real cursor pagination
/// (`paging.next.after`, not offset-based). HubSpot has no bulk
/// export API for arbitrary CRM objects (unlike Salesforce's Bulk API
/// 2.0), so paging over the list endpoint is the real v1 mechanism.
///
/// Every column is projected as Utf8 — HubSpot's list API always
/// renders property values as JSON strings regardless of the
/// property's underlying type (number, date, enumeration, ...), same
/// simplification `nexus-connector-stripe`'s `project` makes for the
/// same reason (a typed schema would need a second call to HubSpot's
/// Properties API to learn each property's real type, out of scope
/// for v1).
pub struct HubspotSource {
    client: reqwest::Client,
    cfg: HubspotConnectorConfig,
    schema: SchemaRef,
}

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    results: Vec<CrmObject>,
    #[serde(default)]
    paging: Option<Paging>,
}

#[derive(Deserialize)]
struct CrmObject {
    id: String,
    #[serde(default)]
    properties: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct Paging {
    next: Option<PagingNext>,
}

#[derive(Deserialize)]
struct PagingNext {
    after: String,
}

impl HubspotSource {
    pub async fn connect(cfg: &HubspotConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let mut fields: Vec<Field> = vec![Field::new("id", DataType::Utf8, false)];
        fields.extend(
            cfg.properties
                .iter()
                .map(|p| Field::new(p, DataType::Utf8, true)),
        );
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn project(&self, object: &CrmObject) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.properties.len() + 1);
        out.insert("id".to_string(), Value::String(object.id.clone()));
        for name in &self.cfg.properties {
            let value = match object.properties.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }

    async fn fetch_page(&self, after: Option<&str>) -> Result<ListResponse, NexusError> {
        let url = format!(
            "{}/crm/v3/objects/{}",
            self.cfg.base_url, self.cfg.object_type
        );
        let properties_csv = self.cfg.properties.join(",");
        let mut params: Vec<(String, String)> = vec![
            ("limit".to_string(), self.cfg.page_size.to_string()),
            ("properties".to_string(), properties_csv),
        ];
        if let Some(after) = after {
            params.push(("after".to_string(), after.to_string()));
        }

        let client = self.client.clone();
        let url = url.clone();
        let access_token = self.cfg.access_token.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();

        retry_with_backoff(&retry, "hubspot list", || {
            let client = client.clone();
            let url = url.clone();
            let params = params.clone();
            let access_token = access_token.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .get(&url)
                        .bearer_auth(&access_token)
                        .query(&params)
                        .send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("hubspot list request timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("hubspot list request failed: {e}")))?;

                if response.status().as_u16() == 429 {
                    let delay = response
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1);
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    return Err(NexusError::Connector(format!(
                        "hubspot list rate limited (429, retry-after={delay}s)"
                    )));
                }

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "hubspot list failed ({status}): {text}"
                    )));
                }

                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("hubspot list response parse failed: {e}"))
                })
            }
        })
        .await
    }
}

impl Clone for HubspotSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl Source for HubspotSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let source = self.clone();
        let schema = self.schema.clone();

        let stream = stream::try_unfold((None::<String>, false), move |(after, done)| {
            let source = source.clone();
            let schema = schema.clone();
            async move {
                if done {
                    return Ok(None);
                }
                let page = source.fetch_page(after.as_deref()).await?;
                let rows: Vec<Value> = page.results.iter().map(|o| source.project(o)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;

                let next_after = page.paging.and_then(|p| p.next).map(|n| n.after);
                let next_done = next_after.is_none();
                Ok(Some((batch, (next_after, next_done))))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
