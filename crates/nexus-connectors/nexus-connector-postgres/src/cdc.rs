use crate::config::{PostgresCdcConfig, PostgresCdcDataType, PostgresCdcFieldSpec};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::BoxStream;
use nexus_core::{NexusError, RecordBatchBuilder, Source, OPCODE_COLUMN};
use pg_walstream::{
    CancellationToken, EventStream, EventType, LogicalReplicationStream, ReplicationStreamConfig,
    RowData, StreamingMode,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// How long `read_batches`' stream waits for the next event before flushing
/// whatever's buffered — unlike a batch/poll source (Kafka, Mongo), going
/// idle here is the normal steady state of a live CDC stream (no writes
/// happening right now), not a stall to error out on.
const FLUSH_ON_IDLE: Duration = Duration::from_millis(500);
const BATCH_SIZE: usize = 500;

/// Native CDC source for Postgres via logical replication (`pgoutput`) — no
/// Debezium/Kafka in front. See `ARCHITECTURE.md §7` and
/// `docs/cdc-reference/README.md` (Debezium+Kafka remains a supported
/// alternative, this isn't a replacement for it).
pub struct PostgresCdcSource {
    event_stream: EventStream,
    schema: SchemaRef,
    fields: Vec<PostgresCdcFieldSpec>,
    table: String,
}

impl PostgresCdcSource {
    pub async fn connect(config: &PostgresCdcConfig) -> Result<Self, NexusError> {
        let replication_uri = with_replication_param(&config.connection_string());

        let stream_config = ReplicationStreamConfig::builder(
            config.slot_name.clone(),
            config.publication_name.clone(),
        )
        .with_streaming_mode(StreamingMode::Off);

        let mut stream = LogicalReplicationStream::new(&replication_uri, stream_config)
            .await
            .map_err(|e| NexusError::Connector(format!("postgres-cdc connect failed: {e}")))?;
        stream.start(None).await.map_err(|e| {
            NexusError::Connector(format!("postgres-cdc start_replication failed: {e}"))
        })?;

        let event_stream = stream.into_stream(CancellationToken::new());

        Ok(Self {
            event_stream,
            schema: build_schema(&config.fields),
            fields: config.fields.clone(),
            table: config.table.clone(),
        })
    }
}

/// Appends the parameter Postgres requires on the connection's startup
/// packet to negotiate replication mode instead of a plain query connection.
fn with_replication_param(uri: &str) -> String {
    let separator = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{separator}replication=database")
}

fn build_schema(fields: &[PostgresCdcFieldSpec]) -> SchemaRef {
    let mut arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let data_type = match f.data_type {
                PostgresCdcDataType::Int64 => DataType::Int64,
                PostgresCdcDataType::Float64 => DataType::Float64,
                PostgresCdcDataType::Boolean => DataType::Boolean,
                PostgresCdcDataType::Utf8 => DataType::Utf8,
            };
            Field::new(&f.name, data_type, f.nullable)
        })
        .collect();
    arrow_fields.push(Field::new(OPCODE_COLUMN, DataType::Utf8, false));
    Arc::new(Schema::new(arrow_fields))
}

/// pgoutput sends every value as text (no `binary: true` requested in the
/// stream config) — coercion to the user-declared target type is the same
/// job every other bridging connector's config already does.
fn row_data_to_json(row: &RowData, fields: &[PostgresCdcFieldSpec], opcode: &str) -> Value {
    let mut map = serde_json::Map::with_capacity(fields.len() + 1);
    for field in fields {
        let text = row
            .get(field.name.as_str())
            .filter(|v| !v.is_null())
            .and_then(|v| v.as_str());
        let value = match text {
            None => Value::Null,
            Some(s) => match field.data_type {
                PostgresCdcDataType::Int64 => {
                    s.parse::<i64>().map(Value::from).unwrap_or(Value::Null)
                }
                PostgresCdcDataType::Float64 => {
                    s.parse::<f64>().map(Value::from).unwrap_or(Value::Null)
                }
                PostgresCdcDataType::Boolean => {
                    s.parse::<bool>().map(Value::from).unwrap_or(Value::Null)
                }
                PostgresCdcDataType::Utf8 => Value::String(s.to_string()),
            },
        };
        map.insert(field.name.clone(), value);
    }
    map.insert(OPCODE_COLUMN.to_string(), Value::String(opcode.to_string()));
    Value::Object(map)
}

#[async_trait]
impl Source for PostgresCdcSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let schema = self.schema.clone();
        let fields = self.fields.clone();
        let table = self.table.clone();

        let stream = futures::stream::unfold(
            (&mut self.event_stream, Vec::<Value>::new(), false),
            move |(event_stream, mut buffer, mut finished)| {
                let schema = schema.clone();
                let fields = fields.clone();
                let table = table.clone();
                async move {
                    loop {
                        if finished {
                            return if buffer.is_empty() {
                                None
                            } else {
                                let batch =
                                    RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                                Some((batch, (event_stream, Vec::new(), finished)))
                            };
                        }
                        if buffer.len() >= BATCH_SIZE {
                            let batch = RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                            return Some((batch, (event_stream, Vec::new(), finished)));
                        }

                        match tokio::time::timeout(FLUSH_ON_IDLE, event_stream.next_event()).await {
                            Ok(Ok(event)) => match event.event_type {
                                EventType::Insert { table: t, data, .. } if *t == *table => {
                                    buffer.push(row_data_to_json(&data, &fields, "I"));
                                }
                                EventType::Update {
                                    table: t, new_data, ..
                                } if *t == *table => {
                                    buffer.push(row_data_to_json(&new_data, &fields, "U"));
                                }
                                EventType::Delete {
                                    table: t, old_data, ..
                                } if *t == *table => {
                                    buffer.push(row_data_to_json(&old_data, &fields, "D"));
                                }
                                // Different table in the same publication, or a
                                // control message (Begin/Commit/Truncate/...) —
                                // no row to emit for this connector.
                                _ => {}
                            },
                            Ok(Err(e)) => {
                                finished = true;
                                return Some((
                                    Err(NexusError::Connector(format!(
                                        "postgres-cdc stream error: {e}"
                                    ))),
                                    (event_stream, buffer, finished),
                                ));
                            }
                            Err(_elapsed) => {
                                if !buffer.is_empty() {
                                    let batch =
                                        RecordBatchBuilder::from_json_rows(schema.clone(), &buffer);
                                    return Some((batch, (event_stream, Vec::new(), finished)));
                                }
                                // Idle, nothing buffered — keep waiting.
                            }
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
