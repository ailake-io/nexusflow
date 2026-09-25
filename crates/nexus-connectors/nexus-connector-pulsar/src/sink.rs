use crate::config::PulsarConnectorConfig;
use crate::source::{build_client, IsolatedRuntime};
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use nexus_core::{
    is_transient_error, retry_with_backoff, with_timeout, CheckpointCursor, NexusError, Sink,
};
use pulsar::executor::TokioExecutor;
use pulsar::Producer;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// Sink for Apache Pulsar. Reuses `build_client` (same broker-handshake
/// dance `PulsarSource::connect` needs — see `source.rs`'s
/// `IsolatedRuntime` doc comment) for the initial connect, then builds a
/// single-topic `Producer` off it — that producer-build step also runs on
/// the same persistent `IsolatedRuntime`, same rationale as connect (the
/// producer's background connection tasks must live on the runtime kept
/// alive in `self._runtime`, not a throwaway one).
///
/// `Producer::send_non_blocking` returns a `SendFuture` immediately (the
/// broker ack can arrive later, especially with batching) — a common
/// footgun with this crate is treating that as "sent". This sink always
/// awaits the inner `SendFuture` too (`write_batch` only returns once
/// every row's receipt has actually come back), matching every other
/// sink in this workspace's contract that `write_batch` means "durably
/// written", not "queued".
///
/// Per-row sends are wrapped in a plain `nexus_core::with_timeout` on the
/// ambient runtime, not routed through `self._runtime`: unlike
/// connect/producer-build, a send only message-passes through the
/// producer's *already-alive* background tasks (hosted on `_runtime`
/// regardless of which runtime issues the `.await`) rather than creating
/// new background state of its own, so there's nothing here for a
/// throwaway/ambient runtime to orphan. No evidence this session that
/// `send_non_blocking`/`SendFuture` share the connect/consumer-build hang
/// this crate worked around elsewhere. Revisit with `_runtime.run(...)` if
/// a wedged producer send is ever seen not to time out in practice.
///
/// No external checkpoint state: `commit_checkpoint` is a no-op, same as
/// `KinesisSink` and every other externally-stateless sink here.
pub struct PulsarSink {
    producer: Producer<TokioExecutor>,
    config: PulsarConnectorConfig,
    /// Kept alive for as long as this sink exists — dropping it would tear
    /// down the producer's background connection tasks. See
    /// `IsolatedRuntime`'s doc comment (`source.rs`).
    _runtime: Arc<IsolatedRuntime>,
}

impl PulsarSink {
    pub async fn connect(config: &PulsarConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let runtime = Arc::new(IsolatedRuntime::new()?);
        let pulsar = build_client(&runtime, config).await?;
        let topic = config.topic.clone();
        let timeout_seconds = config.timeout_seconds;
        let producer = retry_with_backoff(&config.retry, "pulsar producer build", || {
            let pulsar = pulsar.clone();
            let topic = topic.clone();
            let runtime = runtime.clone();
            async move {
                runtime
                    .run(timeout_seconds, "pulsar producer build", async move {
                        pulsar
                            .producer()
                            .with_topic(&topic)
                            .build()
                            .await
                            .map_err(|e| {
                                NexusError::Connector(format!("pulsar producer build failed: {e}"))
                            })
                    })
                    .await
            }
        })
        .await?;
        Ok(Self {
            producer,
            config: config.clone(),
            _runtime: runtime,
        })
    }

    /// Sends one already-serialized row with retry/backoff, manually rather
    /// than via `nexus_core::retry_with_backoff` — that helper takes an
    /// `FnMut() -> Fut` closure, and a closure returning a future that
    /// mutably borrows `self.producer` across repeated calls runs into a
    /// real rustc limitation (a `FnMut`'s captures can't outlive an
    /// individual call when a returned future holds a `&mut` reference
    /// into them). A plain loop in the same async fn body sidesteps that
    /// entirely — same retry/backoff semantics as `RetryConfig`, just
    /// inlined instead of factored out.
    async fn send_one_with_retry(
        &mut self,
        payload: Vec<u8>,
        timeout_seconds: u64,
    ) -> Result<(), NexusError> {
        let retries = self.config.retry.retries;
        let backoff_seconds = self.config.retry.retry_backoff_seconds;
        let mut last_err = None;
        for attempt in 0..=retries {
            let result = with_timeout(timeout_seconds, "pulsar send", async {
                let send_future = self
                    .producer
                    .send_non_blocking(payload.clone())
                    .await
                    .map_err(|e| NexusError::Connector(format!("pulsar send failed: {e}")))?;
                send_future.await.map_err(|e| {
                    NexusError::Connector(format!("pulsar send receipt failed: {e}"))
                })?;
                Ok(())
            })
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt == retries || !is_transient_error(&err) {
                        return Err(err);
                    }
                    let delay = Duration::from_secs(backoff_seconds) * 2u32.pow(attempt);
                    tracing::warn!(
                        "pulsar send failed (attempt {}/{}): {err}, retrying in {:?}",
                        attempt + 1,
                        retries + 1,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    last_err = Some(err);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| NexusError::Connector("pulsar send retry exhausted".into())))
    }
}

/// Turns a `RecordBatch` into row objects for outbound message payloads,
/// projecting only the four primitives `PulsarFieldSpec` supports (same
/// contract as `PulsarSource`'s `decode_and_project`, so a row produced
/// here round-trips back through it on the consumer side).
fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Value>, NexusError> {
    let num_rows = batch.num_rows();
    let mut rows = vec![serde_json::Map::with_capacity(batch.num_columns()); num_rows];

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let name = field.name();

        macro_rules! downcast {
            ($ty:ty) => {
                column.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    NexusError::Connector(format!("pulsar: column {name} type mismatch"))
                })?
            };
        }

        match field.data_type() {
            DataType::Int64 => {
                let arr = downcast!(Int64Array);
                for (row, obj) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(row) {
                        Value::Null
                    } else {
                        Value::from(arr.value(row))
                    };
                    obj.insert(name.clone(), value);
                }
            }
            DataType::Float64 => {
                let arr = downcast!(Float64Array);
                for (row, obj) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(row) {
                        Value::Null
                    } else {
                        Value::from(arr.value(row))
                    };
                    obj.insert(name.clone(), value);
                }
            }
            DataType::Boolean => {
                let arr = downcast!(BooleanArray);
                for (row, obj) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(row) {
                        Value::Null
                    } else {
                        Value::from(arr.value(row))
                    };
                    obj.insert(name.clone(), value);
                }
            }
            DataType::Utf8 => {
                let arr = downcast!(StringArray);
                for (row, obj) in rows.iter_mut().enumerate() {
                    let value = if arr.is_null(row) {
                        Value::Null
                    } else {
                        Value::from(arr.value(row))
                    };
                    obj.insert(name.clone(), value);
                }
            }
            other => {
                return Err(NexusError::Connector(format!(
                    "pulsar: unsupported column type {other:?} for {name}"
                )))
            }
        }
    }

    Ok(rows.into_iter().map(Value::Object).collect())
}

#[async_trait]
impl Sink for PulsarSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), NexusError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = batch_to_json_rows(&batch)?;
        let timeout_seconds = self.config.timeout_seconds;
        for row in rows {
            let payload = serde_json::to_vec(&row)
                .map_err(|e| NexusError::Serialization(format!("pulsar row not JSON: {e}")))?;
            self.send_one_with_retry(payload, timeout_seconds).await?;
        }
        Ok(())
    }

    /// Pulsar has no external checkpoint to advance here — the producer's
    /// send/receipt cycle above is already the durability boundary.
    async fn commit_checkpoint(&mut self, _cursor: CheckpointCursor) -> Result<(), NexusError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::batch_to_json_rows;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn converts_all_four_primitive_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let rows = batch_to_json_rows(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["score"], 1.5);
        assert_eq!(rows[0]["active"], true);
        assert_eq!(rows[0]["name"], "a");
        assert_eq!(rows[1]["id"], 2);
        assert_eq!(rows[1]["name"], "b");
    }

    #[test]
    fn preserves_nulls_as_json_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();

        let rows = batch_to_json_rows(&batch).unwrap();
        assert_eq!(rows[0]["id"], 1);
        assert!(rows[1]["id"].is_null());
    }

    #[test]
    fn empty_batch_yields_empty_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();

        let rows = batch_to_json_rows(&batch).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn rejects_unsupported_column_type() {
        use arrow_array::Int32Array;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        assert!(batch_to_json_rows(&batch).is_err());
    }
}
