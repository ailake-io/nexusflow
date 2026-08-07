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

enum PaginationState {
    None {
        done: bool,
    },
    Offset {
        offset: i64,
        pages_left: usize,
    },
    Cursor {
        cursor: Option<String>,
        pages_left: usize,
    },
}

async fn fetch_page(
    client: &reqwest::Client,
    config: &RestConnectorConfig,
    query: &[(String, String)],
) -> Result<Value, NexusError> {
    let url = format!(
        "{}/{}",
        config.base_url.trim_end_matches('/'),
        config.path.trim_start_matches('/')
    );

    let mut request = client.get(url).query(query);
    for (key, value) in &config.headers {
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

fn rows_from_body<'a>(
    config: &RestConnectorConfig,
    body: &'a Value,
) -> Result<&'a [Value], NexusError> {
    let rows = json_path::navigate(body, config.rows_path.as_deref()).ok_or_else(|| {
        NexusError::Schema(format!(
            "rows_path {:?} not found in REST response",
            config.rows_path
        ))
    })?;

    rows.as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| NexusError::Schema("rows_path did not resolve to a JSON array".into()))
}

#[async_trait]
impl Source for RestSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let client = self.client.clone();
        let config = self.config.clone();
        let schema = self.schema.clone();

        let initial_state = match &config.pagination {
            RestPagination::None => PaginationState::None { done: false },
            RestPagination::Offset {
                offset_param: _,
                limit_param: _,
                limit: _,
            } => PaginationState::Offset {
                offset: 0,
                pages_left: config.max_pages,
            },
            RestPagination::Cursor {
                cursor_param: _,
                next_cursor_path: _,
            } => PaginationState::Cursor {
                cursor: None,
                pages_left: config.max_pages,
            },
        };

        let stream = stream::unfold(
            (initial_state, client, config, schema),
            move |(state, client, config, schema)| async move {
                let (batch, next_state) = match state {
                    PaginationState::None { done: true } => return None,
                    PaginationState::None { done: false } => {
                        let body = match fetch_page(&client, &config, &[]).await {
                            Ok(b) => b,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (PaginationState::None { done: true }, client, config, schema),
                                ))
                            }
                        };
                        let rows = match rows_from_body(&config, &body) {
                            Ok(r) => r,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (PaginationState::None { done: true }, client, config, schema),
                                ))
                            }
                        };
                        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), rows);
                        (batch, PaginationState::None { done: true })
                    }
                    PaginationState::Offset { offset, pages_left } => {
                        if pages_left == 0 {
                            return None;
                        }
                        let RestPagination::Offset {
                            offset_param,
                            limit_param,
                            limit,
                        } = &config.pagination
                        else {
                            return None;
                        };
                        let query = vec![
                            (offset_param.clone(), offset.to_string()),
                            (limit_param.clone(), limit.to_string()),
                        ];
                        let body = match fetch_page(&client, &config, &query).await {
                            Ok(b) => b,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (
                                        PaginationState::Offset {
                                            offset,
                                            pages_left: 0,
                                        },
                                        client,
                                        config,
                                        schema,
                                    ),
                                ))
                            }
                        };
                        let rows = match rows_from_body(&config, &body) {
                            Ok(r) => r,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (
                                        PaginationState::Offset {
                                            offset,
                                            pages_left: 0,
                                        },
                                        client,
                                        config,
                                        schema,
                                    ),
                                ))
                            }
                        };
                        let page_len = rows.len() as i64;
                        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), rows);
                        let next_state = if page_len < *limit || pages_left == 1 {
                            PaginationState::Offset {
                                offset,
                                pages_left: 0,
                            }
                        } else {
                            PaginationState::Offset {
                                offset: offset + limit,
                                pages_left: pages_left - 1,
                            }
                        };
                        (batch, next_state)
                    }
                    PaginationState::Cursor { cursor, pages_left } => {
                        if pages_left == 0 {
                            return None;
                        }
                        let RestPagination::Cursor {
                            cursor_param,
                            next_cursor_path,
                        } = &config.pagination
                        else {
                            return None;
                        };
                        let query = match &cursor {
                            Some(c) => vec![(cursor_param.clone(), c.clone())],
                            None => vec![],
                        };
                        let body = match fetch_page(&client, &config, &query).await {
                            Ok(b) => b,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (
                                        PaginationState::Cursor {
                                            cursor,
                                            pages_left: 0,
                                        },
                                        client,
                                        config,
                                        schema,
                                    ),
                                ))
                            }
                        };
                        let rows = match rows_from_body(&config, &body) {
                            Ok(r) => r,
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (
                                        PaginationState::Cursor {
                                            cursor,
                                            pages_left: 0,
                                        },
                                        client,
                                        config,
                                        schema,
                                    ),
                                ))
                            }
                        };
                        let batch = RecordBatchBuilder::from_json_rows(schema.clone(), rows);
                        let next_cursor = json_path::navigate(&body, Some(next_cursor_path))
                            .and_then(Value::as_str)
                            .map(String::from);
                        let next_state = if next_cursor.is_none() || pages_left == 1 {
                            PaginationState::Cursor {
                                cursor: next_cursor,
                                pages_left: 0,
                            }
                        } else {
                            PaginationState::Cursor {
                                cursor: next_cursor,
                                pages_left: pages_left - 1,
                            }
                        };
                        (batch, next_state)
                    }
                };
                Some((batch, (next_state, client, config, schema)))
            },
        );
        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
