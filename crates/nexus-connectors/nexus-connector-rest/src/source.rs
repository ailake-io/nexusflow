use crate::config::{RestConnectorConfig, RestDataType, RestPagination};
use crate::json_path;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;

/// Generic bridging connector for REST/SaaS APIs — see ARCHITECTURE.md §2/§4.1
/// and `CLAUDE.md §8.2`. No native partitioning: this is the degenerate
/// single-partition case described in ARCHITECTURE.md §4.
pub struct RestSource {
    client: reqwest::Client,
    config: RestConnectorConfig,
    schema: SchemaRef,
}

impl RestSource {
    pub fn connect(config: &RestConnectorConfig) -> Result<Self, NexusError> {
        let schema = build_schema(&config.fields);
        Ok(Self {
            client: reqwest::Client::new(),
            config: config.clone(),
            schema,
        })
    }

    async fn fetch_page(&self, query: &[(String, String)]) -> Result<Value, NexusError> {
        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.path.trim_start_matches('/')
        );

        let mut request = self.client.get(url).query(query);
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        let response = request
            .send()
            .await
            .map_err(|e| NexusError::Connector(format!("REST request failed: {e}")))?
            .error_for_status()
            .map_err(|e| NexusError::Connector(format!("REST request failed: {e}")))?;

        response
            .json::<Value>()
            .await
            .map_err(|e| NexusError::Serialization(format!("REST response not JSON: {e}")))
    }

    fn rows_from_body<'a>(&self, body: &'a Value) -> Result<&'a [Value], NexusError> {
        let rows =
            json_path::navigate(body, self.config.rows_path.as_deref()).ok_or_else(|| {
                NexusError::Schema(format!(
                    "rows_path {:?} not found in REST response",
                    self.config.rows_path
                ))
            })?;

        rows.as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| NexusError::Schema("rows_path did not resolve to a JSON array".into()))
    }

    async fn fetch_all_pages(&self) -> Result<Vec<RecordBatch>, NexusError> {
        let mut batches = Vec::new();

        match &self.config.pagination {
            RestPagination::None => {
                let body = self.fetch_page(&[]).await?;
                let rows = self.rows_from_body(&body)?;
                batches.push(RecordBatchBuilder::from_json_rows(
                    self.schema.clone(),
                    rows,
                )?);
            }
            RestPagination::Offset {
                offset_param,
                limit_param,
                limit,
            } => {
                let mut offset = 0i64;
                for _ in 0..self.config.max_pages {
                    let query = vec![
                        (offset_param.clone(), offset.to_string()),
                        (limit_param.clone(), limit.to_string()),
                    ];
                    let body = self.fetch_page(&query).await?;
                    let rows = self.rows_from_body(&body)?;
                    let page_len = rows.len() as i64;
                    batches.push(RecordBatchBuilder::from_json_rows(
                        self.schema.clone(),
                        rows,
                    )?);

                    if page_len < *limit {
                        break;
                    }
                    offset += limit;
                }
            }
            RestPagination::Cursor {
                cursor_param,
                next_cursor_path,
            } => {
                let mut cursor: Option<String> = None;
                for _ in 0..self.config.max_pages {
                    let query = match &cursor {
                        Some(c) => vec![(cursor_param.clone(), c.clone())],
                        None => vec![],
                    };
                    let body = self.fetch_page(&query).await?;
                    let rows = self.rows_from_body(&body)?;
                    batches.push(RecordBatchBuilder::from_json_rows(
                        self.schema.clone(),
                        rows,
                    )?);

                    cursor = json_path::navigate(&body, Some(next_cursor_path))
                        .and_then(Value::as_str)
                        .map(String::from);
                    if cursor.is_none() {
                        break;
                    }
                }
            }
        }

        Ok(batches)
    }
}

fn build_schema(fields: &[crate::config::RestFieldSpec]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| {
                let data_type = match f.data_type {
                    RestDataType::Int64 => DataType::Int64,
                    RestDataType::Float64 => DataType::Float64,
                    RestDataType::Boolean => DataType::Boolean,
                    RestDataType::Utf8 => DataType::Utf8,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

#[async_trait]
impl Source for RestSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let batches = self.fetch_all_pages().await?;
        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
