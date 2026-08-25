use crate::config::{RestConnectorConfig, RestDataType, RestPagination};
use crate::json_path;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use url::Url;

/// Generic bridging connector for REST/SaaS APIs — see ARCHITECTURE.md §2/§4.1
/// and `CLAUDE.md §8.2`. No native partitioning: this is the degenerate
/// single-partition case described in ARCHITECTURE.md §4.
pub struct RestSource {
    client: reqwest::Client,
    config: RestConnectorConfig,
    schema: SchemaRef,
}

impl RestSource {
    pub async fn connect(config: &RestConnectorConfig) -> Result<Self, NexusError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            // Disable automatic redirects: the DAG's validate_security() only
            // inspects the configured base_url/path. A redirect to an internal
            // or attacker-controlled host would bypass that check.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| NexusError::Connector(format!("REST client build failed: {e}")))?;

        let schema = if config.fields.is_empty() {
            infer_schema(&client, config).await?
        } else {
            build_schema(&config.fields)
        };

        Ok(Self {
            client,
            config: config.clone(),
            schema,
        })
    }
}

/// Fetches the first page — same unparameterized request every pagination
/// mode starts with (`PaginationState::None`/`Offset { offset: 0 }`/
/// `Cursor { cursor: None }` in `read_batches` below all issue the exact
/// same initial query) — and infers a schema from its rows. Source-side
/// fallback for when `fields` is left empty (see
/// `RestConnectorConfig::fields` doc comment). Reuses `fetch_page_with_retry`
/// so a flaky first request gets the same retry/backoff treatment a real
/// read would.
async fn infer_schema(
    client: &reqwest::Client,
    config: &RestConnectorConfig,
) -> Result<SchemaRef, NexusError> {
    let body = fetch_page_with_retry(client, config, &[]).await?;
    let rows = rows_from_body(config, &body)?;
    Ok(RecordBatchBuilder::infer_schema(rows))
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
    let url = config.url()?;
    // Defence in depth: re-parse and reject non-HTTP(S) schemes even though
    // `config.url()` already validated, so this function stays self-contained.
    let url =
        Url::parse(&url).map_err(|e| NexusError::Schema(format!("REST URL is invalid: {e}")))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(NexusError::Schema(format!(
            "REST URL must use http(s), got {} scheme",
            url.scheme()
        )));
    }

    let method = config.method();
    let mut request = client.request(method, url).query(query);
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

fn is_transient_error(err: &NexusError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("timeout")
        || msg.contains("connect")
        || msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
}

async fn fetch_page_with_retry(
    client: &reqwest::Client,
    config: &RestConnectorConfig,
    query: &[(String, String)],
) -> Result<Value, NexusError> {
    let mut last_err = None;
    for attempt in 0..=config.retries {
        match fetch_page(client, config, query).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt == config.retries || !is_transient_error(&err) {
                    return Err(err);
                }
                let delay = Duration::from_secs(config.retry_backoff_seconds) * 2u32.pow(attempt);
                tracing::warn!(
                    "REST request failed (attempt {}/{}): {err}, retrying in {:?}",
                    attempt + 1,
                    config.retries + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
                last_err = Some(err);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| NexusError::Connector("REST retry exhausted".to_string())))
}

async fn fetch_page_with_rate_limit(
    client: &reqwest::Client,
    config: &RestConnectorConfig,
    query: &[(String, String)],
    last_request: &tokio::sync::Mutex<Option<Instant>>,
) -> Result<Value, NexusError> {
    if config.requests_per_second > 0 {
        let min_interval = Duration::from_secs_f64(1.0 / f64::from(config.requests_per_second));
        let mut guard = last_request.lock().await;
        if let Some(prev) = *guard {
            let elapsed = prev.elapsed();
            if elapsed < min_interval {
                tokio::time::sleep(min_interval - elapsed).await;
            }
        }
        *guard = Some(Instant::now());
    }
    fetch_page_with_retry(client, config, query).await
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
        let last_request = Arc::new(tokio::sync::Mutex::new(None::<Instant>));

        let stream = stream::unfold(
            (initial_state, client, config, schema, last_request),
            move |(state, client, config, schema, last_request)| async move {
                let (batch, next_state) = match state {
                    PaginationState::None { done: true } => return None,
                    PaginationState::None { done: false } => {
                        let body =
                            match fetch_page_with_rate_limit(&client, &config, &[], &last_request)
                                .await
                            {
                                Ok(b) => b,
                                Err(e) => {
                                    return Some((
                                        Err(e),
                                        (
                                            PaginationState::None { done: true },
                                            client,
                                            config,
                                            schema,
                                            last_request,
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
                                        PaginationState::None { done: true },
                                        client,
                                        config,
                                        schema,
                                        last_request,
                                    ),
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
                        let body = match fetch_page_with_rate_limit(
                            &client,
                            &config,
                            &query,
                            &last_request,
                        )
                        .await
                        {
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
                                        last_request,
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
                                        last_request,
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
                        let body = match fetch_page_with_rate_limit(
                            &client,
                            &config,
                            &query,
                            &last_request,
                        )
                        .await
                        {
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
                                        last_request,
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
                                        last_request,
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
                Some((batch, (next_state, client, config, schema, last_request)))
            },
        );
        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
