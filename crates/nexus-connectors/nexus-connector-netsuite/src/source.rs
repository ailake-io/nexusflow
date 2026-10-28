use crate::config::NetsuiteConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
struct SuiteQlResponse {
    #[serde(default)]
    items: Vec<Value>,
    #[serde(rename = "hasMore", default)]
    has_more: bool,
}

/// Reads via SuiteQL (`POST /services/rest/query/v1/suiteql`, `q` in
/// the body, `limit`/`offset` as query params — real NetSuite REST
/// pagination shape, `hasMore` in the response body drives
/// continuation). Column names come from `query`'s own `SELECT` list,
/// not a fixed `fields` config — schema is built from the first
/// page's row keys (all Utf8, same "no second schema-discovery call"
/// simplification every REST connector in this repo documents).
///
/// `connect()` fetches the first page eagerly (same reason
/// `nexus-connector-google-sheets`'s source does an eager fetch: the
/// schema must be known as soon as `connect()` returns, before
/// `read_batches()` is ever called — there's no fixed `fields` config
/// to build one from upfront). That first page is cached and handed
/// out as the first batch when `read_batches()` runs.
pub struct NetsuiteSource {
    client: reqwest::Client,
    cfg: NetsuiteConnectorConfig,
    schema: SchemaRef,
    columns: Vec<String>,
    first_page: Option<SuiteQlResponse>,
}

impl NetsuiteSource {
    pub async fn connect(cfg: &NetsuiteConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        cfg.query_or_err()?;
        let client = reqwest::Client::new();
        let first_page = fetch_page(&client, cfg, 0).await?;
        let schema = first_page
            .items
            .first()
            .map(schema_from_first_row)
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        Ok(Self {
            client,
            cfg: cfg.clone(),
            schema,
            columns,
            first_page: Some(first_page),
        })
    }
}

fn project_row(object: &Value, columns: &[String]) -> Value {
    let mut out = serde_json::Map::with_capacity(columns.len());
    for name in columns {
        let value = match object.get(name) {
            Some(Value::String(s)) => Value::String(s.clone()),
            Some(Value::Null) | None => Value::Null,
            Some(other) => Value::String(other.to_string()),
        };
        out.insert(name.clone(), value);
    }
    Value::Object(out)
}

fn schema_from_first_row(row: &Value) -> SchemaRef {
    let columns: Vec<String> = row
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    Arc::new(Schema::new(
        columns
            .into_iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ))
}

async fn fetch_page(
    client: &reqwest::Client,
    cfg: &NetsuiteConnectorConfig,
    offset: u32,
) -> Result<SuiteQlResponse, NexusError> {
    let url = format!(
        "{}/services/rest/query/v1/suiteql?limit={}&offset={offset}",
        cfg.account_url, cfg.page_size
    );
    let query = cfg.query_or_err()?.to_string();
    let client = client.clone();
    let access_token = cfg.access_token.clone();
    let timeout_seconds = cfg.timeout_seconds;

    retry_with_backoff(&cfg.retry, "netsuite suiteql", || {
        let client = client.clone();
        let url = url.clone();
        let query = query.clone();
        let access_token = access_token.clone();
        async move {
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(timeout_seconds),
                client
                    .post(&url)
                    .bearer_auth(&access_token)
                    .header("Prefer", "transient")
                    .json(&serde_json::json!({ "q": query }))
                    .send(),
            )
            .await
            .map_err(|e| NexusError::Connector(format!("netsuite suiteql timed out: {e}")))?
            .map_err(|e| NexusError::Connector(format!("netsuite suiteql failed: {e}")))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(NexusError::Connector(format!(
                    "netsuite suiteql failed ({status}): {text}"
                )));
            }
            response.json().await.map_err(|e| {
                NexusError::Connector(format!("netsuite suiteql response parse failed: {e}"))
            })
        }
    })
    .await
}

#[async_trait]
impl Source for NetsuiteSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let schema = self.schema.clone();
        let page_size = self.cfg.page_size;
        let client = self.client.clone();
        let cfg = self.cfg.clone();
        let columns = self.columns.clone();

        let first_page = self.first_page.take().unwrap_or(SuiteQlResponse {
            items: Vec::new(),
            has_more: false,
        });
        let initial_state = Some((0u32, first_page.items, first_page.has_more));

        let stream = stream::try_unfold(initial_state, move |state| {
            let schema = schema.clone();
            let client = client.clone();
            let cfg = cfg.clone();
            let columns = columns.clone();
            async move {
                let Some((offset, items, has_more)) = state else {
                    return Ok(None);
                };
                let rows: Vec<Value> = items.iter().map(|o| project_row(o, &columns)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;

                let next_state = if has_more {
                    let next_offset = offset + page_size;
                    let next_page = fetch_page(&client, &cfg, next_offset).await?;
                    Some((next_offset, next_page.items, next_page.has_more))
                } else {
                    None
                };
                Ok(Some((batch, next_state)))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
