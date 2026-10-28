use crate::config::ServicenowConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// Reads via the Table API's list endpoint
/// (`GET /api/now/table/{table}`) — real offset pagination
/// (`sysparm_offset`/`sysparm_limit`). ServiceNow's Table API doesn't
/// return a total-count or next-cursor field in the JSON body by
/// default (that lives in a `Link` response header this connector
/// doesn't parse, out of scope for v1) — so the real, commonly-used
/// heuristic is: keep paging while a page comes back full
/// (`sysparm_limit` rows), stop the first time it comes back short.
///
/// Every column is projected as Utf8 — same "nested/typed values
/// rendered as JSON text, no second schema-discovery call"
/// simplification `nexus-connector-stripe`/`hubspot`/`zendesk`
/// document (ServiceNow reference fields are themselves nested
/// objects, e.g. `{"link": "...", "value": "sys_id"}` — rendered as
/// their JSON text form here rather than unwrapped).
pub struct ServicenowSource {
    client: reqwest::Client,
    cfg: ServicenowConnectorConfig,
    schema: SchemaRef,
}

impl ServicenowSource {
    pub async fn connect(cfg: &ServicenowConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let fields: Vec<Field> = cfg
            .fields
            .iter()
            .map(|f| Field::new(f, DataType::Utf8, true))
            .collect();
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn project(&self, object: &Value) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.fields.len());
        for name in &self.cfg.fields {
            let value = match object.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }

    async fn fetch_page(&self, offset: u32) -> Result<Vec<Value>, NexusError> {
        let url = format!("{}/api/now/table/{}", self.cfg.base_url, self.cfg.table);
        let fields_csv = self.cfg.fields.join(",");
        let params: Vec<(String, String)> = vec![
            ("sysparm_limit".to_string(), self.cfg.page_size.to_string()),
            ("sysparm_offset".to_string(), offset.to_string()),
            ("sysparm_fields".to_string(), fields_csv),
        ];

        let client = self.client.clone();
        let url = url.clone();
        let username = self.cfg.username.clone();
        let password = self.cfg.password.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();

        #[derive(Deserialize)]
        struct TableResponse {
            #[serde(default)]
            result: Vec<Value>,
        }

        let response: TableResponse = retry_with_backoff(&retry, "servicenow table list", || {
            let client = client.clone();
            let url = url.clone();
            let params = params.clone();
            let username = username.clone();
            let password = password.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .get(&url)
                        .basic_auth(username, Some(password))
                        .query(&params)
                        .send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("servicenow list timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("servicenow list failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "servicenow list failed ({status}): {text}"
                    )));
                }
                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("servicenow list response parse failed: {e}"))
                })
            }
        })
        .await?;

        Ok(response.result)
    }
}

impl Clone for ServicenowSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl Source for ServicenowSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let source = self.clone();
        let schema = self.schema.clone();
        let page_size = self.cfg.page_size;

        let stream = stream::try_unfold((0u32, false), move |(offset, done)| {
            let source = source.clone();
            let schema = schema.clone();
            async move {
                if done {
                    return Ok(None);
                }
                let results = source.fetch_page(offset).await?;
                let page_len = results.len() as u32;
                let rows: Vec<Value> = results.iter().map(|o| source.project(o)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;

                let next_done = page_len < page_size;
                let next_offset = offset + page_len;
                Ok(Some((batch, (next_offset, next_done))))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
