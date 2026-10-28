use crate::config::Dynamics365ConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
struct ODataResponse {
    #[serde(default)]
    value: Vec<Value>,
    #[serde(rename = "@odata.nextLink", default)]
    next_link: Option<String>,
}

/// Reads via the Dataverse Web API's OData v4 list endpoint
/// (`GET /api/data/v9.2/{entity_set}`) — real cursor pagination via
/// `@odata.nextLink` (a *full URL* for the next page, standard OData
/// v4 convention, not a bare token like HubSpot/Zendesk's cursors).
///
/// Every column is projected as Utf8 — same "nested/typed values
/// rendered as JSON text, no second schema-discovery call"
/// simplification `nexus-connector-hubspot`/`zendesk`/`servicenow`
/// document (Dataverse lookup/money/optionset fields are themselves
/// structured, e.g. `{"@odata.type": "...", ...}` metadata
/// properties alongside the plain value — rendered as JSON text form
/// here rather than unwrapped).
pub struct Dynamics365Source {
    client: reqwest::Client,
    cfg: Dynamics365ConnectorConfig,
    schema: SchemaRef,
}

impl Dynamics365Source {
    pub async fn connect(cfg: &Dynamics365ConnectorConfig) -> Result<Self, NexusError> {
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

    fn first_page_url(&self) -> String {
        format!(
            "{}/api/data/v9.2/{}?$select={}&$top={}",
            self.cfg.base_url,
            self.cfg.entity_set,
            self.cfg.fields.join(","),
            self.cfg.page_size
        )
    }

    async fn fetch_page(&self, url: &str) -> Result<ODataResponse, NexusError> {
        let client = self.client.clone();
        let url = url.to_string();
        let access_token = self.cfg.access_token.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();

        retry_with_backoff(&retry, "dynamics365 list", || {
            let client = client.clone();
            let url = url.clone();
            let access_token = access_token.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client
                        .get(&url)
                        .bearer_auth(&access_token)
                        .header("OData-MaxVersion", "4.0")
                        .header("OData-Version", "4.0")
                        .header("Accept", "application/json")
                        .send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("dynamics365 list timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("dynamics365 list failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "dynamics365 list failed ({status}): {text}"
                    )));
                }
                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("dynamics365 list response parse failed: {e}"))
                })
            }
        })
        .await
    }
}

impl Clone for Dynamics365Source {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl Source for Dynamics365Source {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let source = self.clone();
        let schema = self.schema.clone();
        let first_url = self.first_page_url();

        let stream = stream::try_unfold(Some(first_url), move |url| {
            let source = source.clone();
            let schema = schema.clone();
            async move {
                let Some(url) = url else {
                    return Ok(None);
                };
                let page = source.fetch_page(&url).await?;
                let rows: Vec<Value> = page.value.iter().map(|o| source.project(o)).collect();
                let batch = RecordBatchBuilder::from_json_rows(schema, &rows)?;
                Ok(Some((batch, page.next_link)))
            }
        });

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
