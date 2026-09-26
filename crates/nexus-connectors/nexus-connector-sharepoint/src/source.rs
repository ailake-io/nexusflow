use crate::config::SharepointConnectorConfig;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

#[derive(Deserialize)]
struct ListItem {
    id: String,
    #[serde(default)]
    fields: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct ItemsResponse {
    #[serde(default)]
    value: Vec<ListItem>,
    #[serde(rename = "@odata.nextLink", default)]
    next_link: Option<String>,
}

/// Reads via Microsoft Graph's SharePoint List items endpoint
/// (`GET /sites/{site_id}/lists/{list_id}/items`, `$expand=fields`
/// to pull column values, which otherwise live under a separate
/// `fields` sub-resource, not the item's top level) — real cursor
/// pagination via `@odata.nextLink` (a full URL, same OData v4
/// convention `nexus-connector-dynamics365` documents — both are
/// Microsoft Graph/Dataverse APIs).
///
/// Every column is projected as Utf8 — same "no second
/// schema-discovery call" simplification every REST connector in this
/// repo documents (a SharePoint column's real type — choice, lookup,
/// person, etc. — would need the list's own column-definitions
/// endpoint to learn).
pub struct SharepointSource {
    client: reqwest::Client,
    cfg: SharepointConnectorConfig,
    schema: SchemaRef,
}

impl SharepointSource {
    pub async fn connect(cfg: &SharepointConnectorConfig) -> Result<Self, NexusError> {
        cfg.validate()?;
        let mut fields: Vec<Field> = vec![Field::new("id", DataType::Utf8, false)];
        fields.extend(
            cfg.fields
                .iter()
                .map(|f| Field::new(f, DataType::Utf8, true)),
        );
        Ok(Self {
            client: reqwest::Client::new(),
            cfg: cfg.clone(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    fn project(&self, item: &ListItem) -> Value {
        let mut out = serde_json::Map::with_capacity(self.cfg.fields.len() + 1);
        out.insert("id".to_string(), Value::String(item.id.clone()));
        for name in &self.cfg.fields {
            let value = match item.fields.get(name) {
                Some(Value::String(s)) => Value::String(s.clone()),
                Some(Value::Null) | None => Value::Null,
                Some(other) => Value::String(other.to_string()),
            };
            out.insert(name.clone(), value);
        }
        Value::Object(out)
    }

    fn first_page_url(&self) -> String {
        let select = self.cfg.fields.join(",");
        format!(
            "{}/sites/{}/lists/{}/items?$expand=fields($select={select})&$top={}",
            self.cfg.base_url, self.cfg.site_id, self.cfg.list_id, self.cfg.page_size
        )
    }

    async fn fetch_page(&self, url: &str) -> Result<ItemsResponse, NexusError> {
        let client = self.client.clone();
        let url = url.to_string();
        let access_token = self.cfg.access_token.clone();
        let timeout_seconds = self.cfg.timeout_seconds;
        let retry = self.cfg.retry.clone();

        retry_with_backoff(&retry, "sharepoint list items", || {
            let client = client.clone();
            let url = url.clone();
            let access_token = access_token.clone();
            async move {
                let response = tokio::time::timeout(
                    std::time::Duration::from_secs(timeout_seconds),
                    client.get(&url).bearer_auth(&access_token).send(),
                )
                .await
                .map_err(|e| NexusError::Connector(format!("sharepoint list timed out: {e}")))?
                .map_err(|e| NexusError::Connector(format!("sharepoint list failed: {e}")))?;

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    return Err(NexusError::Connector(format!(
                        "sharepoint list failed ({status}): {text}"
                    )));
                }
                response.json().await.map_err(|e| {
                    NexusError::Connector(format!("sharepoint list response parse failed: {e}"))
                })
            }
        })
        .await
    }
}

impl Clone for SharepointSource {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            cfg: self.cfg.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait]
impl Source for SharepointSource {
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
