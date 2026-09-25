use crate::config::{KinesisConnectorConfig, KinesisFieldSpec, StartingPosition};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_kinesis::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_kinesis::error::SdkError;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::Client;
use futures::stream::{self, BoxStream};
use nexus_core::{retry_with_backoff, with_timeout, NexusError, RecordBatchBuilder, Source};
use serde_json::{Map, Value};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

fn format_sdk_error<E, R>(operation: &str, e: SdkError<E, R>) -> NexusError
where
    E: Debug,
    R: Debug,
{
    let detail = match &e {
        SdkError::ServiceError(se) => format!(
            "service error: err={:#?} raw={:#?}",
            se.err(),
            se.raw()
        ),
        SdkError::TimeoutError(_) => "timeout".to_string(),
        SdkError::DispatchFailure(d) => format!("dispatch failure: {d:?}"),
        SdkError::ResponseError(re) => format!("response error: {re:?}"),
        other => format!("SDK error: {other:?}"),
    };
    NexusError::Connector(format!("{operation} failed: {detail}"))
}

/// Native source for Amazon Kinesis Data Streams. No ADBC/ODBC driver
/// involved — the AWS SDK is async-native and `Send`, so unlike
/// `OracleCdcSource` this doesn't need a dedicated `spawn_blocking`
/// thread; the whole poll loop is a plain `stream::unfold` doing
/// `.await` directly, closer in spirit to `mssql-cdc`'s ADBC-is-`Send`
/// simplicity.
///
/// Shard iterators, not Kafka-style offsets, are Kinesis's resume
/// mechanism — a sequence number per shard is tracked in memory only
/// for the lifetime of this source instance (same v1 simplification
/// `mssql-cdc`/`oracle-cdc` already accept: a restart resumes from
/// `starting_position` fresh, no external checkpoint store wired up).
///
/// **Resharding (shard split/merge) isn't handled in v1**: when a
/// shard closes (`GetRecords` returns `next_shard_iterator: None`),
/// this source just stops polling it — it does not discover or start
/// reading the child shards `ListShards`/`GetRecords` would otherwise
/// expose. A real limitation, not a hidden simplification; documented
/// in the README.
pub struct KinesisSource {
    client: Client,
    config: KinesisConnectorConfig,
    schema: SchemaRef,
}

impl KinesisSource {
    pub async fn connect(config: &KinesisConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let schema = build_schema(&config.fields)?;
        let client = build_client(config);

        retry_with_backoff(&config.retry, "kinesis connect", || async {
            with_timeout(config.timeout_seconds, "kinesis list_shards", async {
                client
                    .list_shards()
                    .stream_name(&config.stream_name)
                    .send()
                    .await
                    .map_err(|e| format_sdk_error("kinesis list_shards", e))
            })
            .await
        })
        .await?;

        Ok(Self {
            client,
            config: config.clone(),
            schema,
        })
    }
}

pub(crate) fn build_client(config: &KinesisConnectorConfig) -> Client {
    let credentials = Credentials::new(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        config.session_token.clone(),
        None,
        "nexus-connector-kinesis",
    );
    // Build the shared SdkConfig first: aws-config 1.x only applies a
    // custom endpoint_url when it is set on the shared SdkConfig, not
    // when it is set only on the per-service Config builder. From there
    // the Kinesis service config inherits the endpoint resolver.
    let mut builder = SdkConfig::builder()
        .region(Region::new(config.region.clone()))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .behavior_version(BehaviorVersion::latest());
    if let Some(endpoint) = &config.endpoint {
        tracing::info!(kinesis_endpoint = %endpoint, "using custom Kinesis endpoint");
        builder = builder.endpoint_url(endpoint);
    } else {
        tracing::info!("no custom Kinesis endpoint configured; using AWS default");
    }
    let sdk_config = builder.build();
    Client::new(&sdk_config)
}

fn build_schema(fields: &[KinesisFieldSpec]) -> Result<SchemaRef, NexusError> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.r#type.as_str() {
                "int64" => DataType::Int64,
                "float64" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "utf8" => DataType::Utf8,
                other => {
                    return Err(NexusError::Schema(format!(
                        "kinesis field '{}': unsupported type '{other}' (expected int64, float64, boolean, or utf8)",
                        f.name
                    )))
                }
            };
            Ok(Field::new(&f.name, data_type, true))
        })
        .collect::<Result<_, NexusError>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Per-shard poll state: the iterator to use on the next `GetRecords`
/// call, or `None` once the shard has closed and shouldn't be polled
/// again.
struct ShardState {
    shard_id: String,
    iterator: Option<String>,
    closed: bool,
}

struct PollState {
    client: Client,
    config: KinesisConnectorConfig,
    schema: SchemaRef,
    shards: Vec<ShardState>,
    /// Consecutive poll cycles that returned no records. Used to implement
    /// `stop_after_empty_polls` for batch/integration-test scenarios.
    empty_polls: u64,
}

#[async_trait]
impl Source for KinesisSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let client = self.client.clone();
        let config = self.config.clone();
        let retry = config.retry.clone();
        let shard_ids = retry_with_backoff(&retry, "kinesis read_batches list_shards", move || {
            let client = client.clone();
            let config = config.clone();
            async move {
                with_timeout(config.timeout_seconds, "kinesis list_shards", async {
                    let output = client
                        .list_shards()
                        .stream_name(&config.stream_name)
                        .send()
                        .await
                        .map_err(|e| format_sdk_error("kinesis list_shards", e))?;
                    Ok::<_, NexusError>(output.shards().iter().map(|s| s.shard_id().to_string()).collect::<Vec<_>>())
                })
                .await
            }
        })
        .await?;

        let shards = shard_ids
            .into_iter()
            .map(|shard_id| ShardState {
                shard_id,
                iterator: None,
                closed: false,
            })
            .collect();

        let state = PollState {
            client: self.client.clone(),
            config: self.config.clone(),
            schema: self.schema.clone(),
            shards,
            empty_polls: 0,
        };

        Ok(Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if state.shards.iter().all(|s| s.closed) {
                    return None;
                }

                match poll_all_shards(&mut state).await {
                    Ok(rows) if !rows.is_empty() => {
                        state.empty_polls = 0;
                        match RecordBatchBuilder::from_json_rows(state.schema.clone(), &rows) {
                            Ok(batch) => return Some((Ok(batch), state)),
                            Err(e) => return Some((Err(e), state)),
                        }
                    }
                    Ok(_) => {
                        state.empty_polls += 1;
                        let limit = state.config.stop_after_empty_polls;
                        if limit > 0 && state.empty_polls >= limit {
                            tracing::info!(
                                empty_polls = state.empty_polls,
                                limit = limit,
                                "kinesis source reached empty-poll limit; finishing stream"
                            );
                            return None;
                        }
                        tokio::time::sleep(Duration::from_millis(state.config.poll_interval_ms)).await;
                    }
                    Err(e) => return Some((Err(e), state)),
                }
            }
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// One poll cycle across every still-open shard, returning the
/// decoded rows collected from all of them. A record whose payload
/// isn't valid JSON (or isn't a JSON object) fails the whole cycle
/// loudly rather than being silently skipped — there's no safe way to
/// "skip and resume later" a specific bad record within a shard's
/// sequential iterator.
async fn poll_all_shards(state: &mut PollState) -> Result<Vec<Value>, NexusError> {
    let mut rows = Vec::new();
    let field_names: Vec<&str> = state
        .config
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();

    for shard in state.shards.iter_mut() {
        if shard.closed {
            continue;
        }

        if shard.iterator.is_none() {
            shard.iterator =
                Some(fetch_shard_iterator(&state.client, &state.config, &shard.shard_id).await?);
        }
        let iterator = shard
            .iterator
            .clone()
            .ok_or_else(|| NexusError::Connector(format!("kinesis: no shard iterator for shard {}", shard.shard_id)))?;

        let client = state.client.clone();
        let config = state.config.clone();
        let retry = config.retry.clone();
        let output = retry_with_backoff(&retry, "kinesis get_records", move || {
            let client = client.clone();
            let config = config.clone();
            let iterator = iterator.clone();
            async move {
                with_timeout(config.timeout_seconds, "kinesis get_records", async {
                    client
                        .get_records()
                        .shard_iterator(&iterator)
                        .limit(config.max_records_per_poll)
                        .send()
                        .await
                        .map_err(|e| format_sdk_error("kinesis get_records", e))
                })
                .await
            }
        })
        .await?;

        for record in output.records() {
            let value: Value = serde_json::from_slice(record.data().as_ref()).map_err(|e| {
                NexusError::Serialization(format!(
                    "kinesis: record in shard {} isn't valid JSON: {e}",
                    shard.shard_id
                ))
            })?;
            let object = value.as_object().ok_or_else(|| {
                NexusError::Schema(format!(
                    "kinesis: record in shard {} isn't a JSON object",
                    shard.shard_id
                ))
            })?;

            let mut projected = Map::new();
            for name in &field_names {
                if let Some(v) = object.get(*name) {
                    projected.insert((*name).to_string(), v.clone());
                }
            }
            rows.push(Value::Object(projected));
        }

        match output.next_shard_iterator() {
            Some(next) => shard.iterator = Some(next.to_string()),
            None => shard.closed = true,
        }
    }

    Ok(rows)
}

async fn fetch_shard_iterator(
    client: &Client,
    config: &KinesisConnectorConfig,
    shard_id: &str,
) -> Result<String, NexusError> {
    let iterator_type = match config.starting_position {
        StartingPosition::TrimHorizon => ShardIteratorType::TrimHorizon,
        StartingPosition::Latest => ShardIteratorType::Latest,
    };

    let client = client.clone();
    let config = config.clone();
    let shard_id = shard_id.to_string();
    let retry = config.retry.clone();
    retry_with_backoff(&retry, "kinesis get_shard_iterator", move || {
        let client = client.clone();
        let config = config.clone();
        let shard_id = shard_id.clone();
        let iterator_type = iterator_type.clone();
        async move {
            with_timeout(config.timeout_seconds, "kinesis get_shard_iterator", async {
                let output = client
                    .get_shard_iterator()
                    .stream_name(&config.stream_name)
                    .shard_id(&shard_id)
                    .shard_iterator_type(iterator_type)
                    .send()
                    .await
                    .map_err(|e| format_sdk_error("kinesis get_shard_iterator", e))?;
                output
                    .shard_iterator()
                    .map(|s| s.to_string())
                    .ok_or_else(|| NexusError::Connector(format!("kinesis get_shard_iterator returned no iterator for shard {shard_id}")))
            })
            .await
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_schema_maps_supported_types() {
        let fields = vec![
            KinesisFieldSpec {
                name: "id".into(),
                r#type: "int64".into(),
            },
            KinesisFieldSpec {
                name: "amount".into(),
                r#type: "float64".into(),
            },
            KinesisFieldSpec {
                name: "active".into(),
                r#type: "boolean".into(),
            },
            KinesisFieldSpec {
                name: "name".into(),
                r#type: "utf8".into(),
            },
        ];
        let schema = build_schema(&fields).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
    }

    #[test]
    fn build_schema_rejects_unknown_type() {
        let fields = vec![KinesisFieldSpec {
            name: "ts".into(),
            r#type: "timestamp".into(),
        }];
        let err = build_schema(&fields).unwrap_err();
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn starting_position_defaults_to_latest() {
        assert_eq!(StartingPosition::default(), StartingPosition::Latest);
    }
}
