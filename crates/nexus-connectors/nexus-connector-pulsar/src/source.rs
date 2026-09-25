use crate::config::{InitialPosition, PulsarConnectorConfig, PulsarFieldSpec, SubscriptionType};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::TryStreamExt;
use nexus_core::{retry_with_backoff, NexusError, RecordBatchBuilder, Source};
use pulsar::consumer::{ConsumerOptions, InitialPosition as PulsarInitialPosition};
use pulsar::executor::TokioExecutor;
use pulsar::message::proto::command_subscribe::SubType;
use pulsar::{Authentication, Consumer, Pulsar};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;

/// Native source for Apache Pulsar. Async-native SDK (`Send`, no ODBC/
/// ADBC handle involved) — same simplicity as `kinesis`'s
/// `stream::unfold`-with-no-`spawn_blocking` shape, not the
/// dedicated-thread workaround `oracle-cdc` needs.
///
/// Unlike `kinesis`/`mssql-cdc`/`oracle-cdc`, this source doesn't track
/// its own resume cursor: Pulsar's broker remembers a subscription's
/// read position across restarts via `ack` — `subscription_name` *is*
/// the durable cursor, a real property of Pulsar, not a v1
/// simplification like the in-memory-only cursors the other streaming
/// connectors in this repo settle for.
pub struct PulsarSource {
    rx: tokio::sync::mpsc::Receiver<PulsarEvent>,
    config: PulsarConnectorConfig,
    schema: SchemaRef,
    /// Kept alive for as long as this source exists — dropping it would
    /// tear down the consumer's background connection tasks. See
    /// `IsolatedRuntime`'s doc comment.
    _runtime: Arc<IsolatedRuntime>,
}

/// One message decoded off the broker, or a terminal condition — sent from
/// `pulsar_reader_task` (its own spawned task) to the `read_batches()`
/// stream. See that task's doc comment for why this indirection exists.
enum PulsarEvent {
    Row(Value),
    Error(NexusError),
    Closed,
}

/// Owns the `Consumer` and does nothing but call `try_next()`/`ack()` in a
/// loop, forwarding decoded rows over an mpsc channel. Exists because
/// `tokio::time::timeout` only fires reliably around futures that actually
/// yield to the runtime at their own await points — `Consumer::try_next()`
/// hung indefinitely in this crate's pulsar client (v6.8.0) even wrapped in
/// `timeout`, with `idle_timeout_ms` never firing (confirmed empirically:
/// zero pulsar-side log output, run stuck in "running" far past
/// `idle_timeout_ms * MAX_CONSECUTIVE_IDLES`). Isolating the read onto its
/// own task means a stuck `try_next()` only pins *that* task's worker
/// thread — `read_batches()`'s `rx.recv()` (a well-behaved, cooperative
/// primitive) keeps polling on a different task, so its `timeout` fires on
/// schedule regardless of what the pulsar client does internally. Same
/// "isolate the misbehaving I/O so timeouts still work" shape as
/// `nexus-connector-oracle`'s dedicated OS thread for its `!Send` ODBC
/// handle — different root cause (blocking-not-yielding vs `!Send`), same
/// fix.
async fn pulsar_reader_task(
    mut consumer: Consumer<Vec<u8>, TokioExecutor>,
    field_names: Vec<String>,
    tx: tokio::sync::mpsc::Sender<PulsarEvent>,
) {
    loop {
        match consumer.try_next().await {
            Ok(Some(msg)) => {
                let payload: Vec<u8> = msg.deserialize();
                let decoded = decode_and_project(&payload, &field_names);
                let ack_result = consumer.ack(&msg).await;
                let event = match (decoded, ack_result) {
                    (Ok(row), Ok(())) => PulsarEvent::Row(row),
                    (Err(e), _) => PulsarEvent::Error(e),
                    (Ok(_), Err(e)) => {
                        PulsarEvent::Error(NexusError::Connector(format!("pulsar ack failed: {e}")))
                    }
                };
                let was_error = matches!(event, PulsarEvent::Error(_));
                if tx.send(event).await.is_err() || was_error {
                    return;
                }
            }
            Ok(None) => {
                let _ = tx.send(PulsarEvent::Closed).await;
                return;
            }
            Err(e) => {
                let _ = tx
                    .send(PulsarEvent::Error(NexusError::Connector(format!(
                        "pulsar receive failed: {e}"
                    ))))
                    .await;
                return;
            }
        }
    }
}

/// A single OS thread running its own dedicated Tokio runtime, kept alive
/// for as long as this value lives (not just for one operation) — required
/// because the `pulsar` crate's client keeps its connection-management
/// tasks (handshake keep-alive, frame I/O, lookup/redirect handling) alive
/// via plain `tokio::spawn` on whatever runtime built it.
///
/// History: the first fix attempted this session ran each operation
/// (`build_client`, then separately the consumer-build) on its own
/// *throwaway* runtime — spawn an OS thread, build a runtime, `block_on`
/// the operation, send the result back over a `std::sync::mpsc` channel,
/// let the thread and its runtime die. `recv_timeout` on that channel is a
/// hard OS-level wait, immune to whatever the awaited future does
/// internally, so in principle it should have fixed the hang described
/// below. It compiled, and `build_client` even kept returning fast and
/// successful — but the *next* operation on that same client (building the
/// consumer) hung forever anyway, with zero error, confirmed empirically
/// against a real broker. Root cause: `build_client`'s throwaway runtime
/// was dropped the moment it returned, which killed the client's
/// background connection tasks along with it — the client value itself
/// survived (it's just a handle), but nothing was left servicing its
/// connection, so the consumer-build's attempt to use it stalled forever
/// waiting for a reply that could never arrive. Same underlying symptom as
/// the *original* bug this type is meant to fix, but a different root
/// cause created by the first fix attempt, not the original one.
///
/// Keeping one runtime alive for the whole lifetime of the client (this
/// type, held in `PulsarSource`/`PulsarSink` for as long as they exist)
/// avoids that: every operation that can create or depend on the client's
/// background state (`build_client`, consumer build, producer build) runs
/// via `self.handle.spawn(...)` on the *same* persistent runtime, so no
/// operation's cleanup can orphan another operation's background tasks.
/// The original hang this type also still fixes (a broker that never
/// replies during the handshake, so a bare `tokio::time::timeout` around
/// the future never fires because the timer and the wedged future share a
/// starved runtime) is still fully covered — `run()`'s `recv_timeout` below
/// is the same hard, executor-independent wait as before.
pub(crate) struct IsolatedRuntime {
    handle: tokio::runtime::Handle,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl IsolatedRuntime {
    pub(crate) fn new() -> Result<Self, NexusError> {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<tokio::runtime::Handle, String>>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let join = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(e.to_string()));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(rt.handle().clone()));
            // Anchors the runtime's lifetime to this thread until told to
            // stop. Everything actually spawned on this runtime runs via
            // `handle.spawn` from the outside — this `block_on` does no
            // work of its own, it just keeps the runtime (and its
            // reactor/timer driver) alive and polled.
            rt.block_on(async move {
                let _ = shutdown_rx.await;
            });
        });

        let handle = ready_rx
            .recv()
            .map_err(|_| {
                NexusError::Connector("isolated runtime thread died before ready".into())
            })?
            .map_err(|e| {
                NexusError::Connector(format!("failed to build isolated runtime: {e}"))
            })?;

        Ok(Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        })
    }

    /// Runs `fut` on this runtime and waits for the result via a
    /// `recv_timeout` on a plain `std::sync::mpsc` channel — see this
    /// type's doc comment for why that's a hard, executor-independent
    /// bound instead of `tokio::time::timeout`.
    pub(crate) async fn run<F, T>(&self, timeout_secs: u64, op_name: &str, fut: F) -> Result<T, NexusError>
    where
        F: std::future::Future<Output = Result<T, NexusError>> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel::<Result<T, NexusError>>();
        self.handle.spawn(async move {
            // If the receiver already gave up (timed out), sending here
            // just fails silently — nothing left to deliver the (late)
            // result to. The task itself keeps running to completion on
            // this persistent runtime either way (it isn't aborted), same
            // as any other background task hosted here.
            let _ = tx.send(fut.await);
        });

        let timeout_secs = timeout_secs.max(1);
        let op_name_owned = op_name.to_string();
        let join_result = tokio::task::spawn_blocking(move || {
            match rx.recv_timeout(Duration::from_secs(timeout_secs)) {
                Ok(result) => result,
                Err(_timeout_or_disconnected) => Err(NexusError::Connector(format!(
                    "{op_name_owned} timed out after {timeout_secs}s (operation abandoned on the \
                     isolated pulsar runtime — the pulsar client never yielded a result, likely a \
                     hang inside its own retry/subscribe logic that no per-operation timeout in \
                     that crate catches)"
                ))),
            }
        })
        .await;

        match join_result {
            Ok(result) => result,
            Err(e) => Err(NexusError::Connector(format!(
                "isolated runtime wait task panicked: {e}"
            ))),
        }
    }
}

impl Drop for IsolatedRuntime {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Builds and connects a `Pulsar` client — shared by `PulsarSource::connect`
/// and `PulsarSink::connect`, since a producer needs the exact same
/// broker-handshake dance (retry + isolated-runtime timeout, see
/// `IsolatedRuntime`'s doc comment) a consumer does. `runtime` must be the
/// *same* `IsolatedRuntime` instance the caller keeps alive for the rest of
/// the client's life — see that type's doc comment for why a throwaway one
/// per call doesn't work.
pub(crate) async fn build_client(
    runtime: &IsolatedRuntime,
    config: &PulsarConnectorConfig,
) -> Result<Pulsar<TokioExecutor>, NexusError> {
    let service_url = config.service_url.clone();
    let auth_token = config.auth_token.clone();
    let timeout_seconds = config.timeout_seconds;
    retry_with_backoff(&config.retry, "pulsar connect", || {
        let service_url = service_url.clone();
        let auth_token = auth_token.clone();
        async move {
            let mut builder = Pulsar::builder(service_url, TokioExecutor);
            if let Some(token) = &auth_token {
                builder = builder.with_auth(Authentication {
                    name: "token".to_string(),
                    data: token.clone().into_bytes(),
                });
            }
            runtime
                .run(timeout_seconds, "pulsar connect", async move {
                    builder
                        .build()
                        .await
                        .map_err(|e| NexusError::Connector(format!("pulsar connect failed: {e}")))
                })
                .await
        }
    })
    .await
}

impl PulsarSource {
    pub async fn connect(config: &PulsarConnectorConfig) -> Result<Self, NexusError> {
        config.validate()?;
        let schema = build_schema(&config.fields)?;

        let timeout_seconds = config.timeout_seconds;
        let runtime = Arc::new(IsolatedRuntime::new()?);
        let pulsar: Pulsar<TokioExecutor> = build_client(&runtime, config).await?;

        let sub_type = match config.subscription_type {
            SubscriptionType::Exclusive => SubType::Exclusive,
            SubscriptionType::Shared => SubType::Shared,
            SubscriptionType::Failover => SubType::Failover,
            SubscriptionType::KeyShared => SubType::KeyShared,
        };
        let initial_position = match config.initial_position {
            InitialPosition::Earliest => PulsarInitialPosition::Earliest,
            InitialPosition::Latest => PulsarInitialPosition::Latest,
        };

        let topic = config.topic.clone();
        let subscription_name = config.subscription_name.clone();
        let consumer: Consumer<Vec<u8>, TokioExecutor> =
            retry_with_backoff(&config.retry, "pulsar consumer build", || {
                let pulsar = pulsar.clone();
                let topic = topic.clone();
                let subscription_name = subscription_name.clone();
                let initial_position = initial_position.clone();
                let runtime = runtime.clone();
                async move {
                    runtime
                        .run(timeout_seconds, "pulsar consumer build", async move {
                            pulsar
                                .consumer()
                                .with_topic(&topic)
                                .with_subscription(&subscription_name)
                                .with_subscription_type(sub_type)
                                .with_options(
                                    ConsumerOptions::default()
                                        .with_initial_position(initial_position),
                                )
                                .build()
                                .await
                                .map_err(|e| {
                                    NexusError::Connector(format!(
                                        "pulsar consumer build failed: {e}"
                                    ))
                                })
                        })
                        .await
                }
            })
            .await?;

        let field_names: Vec<String> = config.fields.iter().map(|f| f.name.clone()).collect();
        let (tx, rx) = tokio::sync::mpsc::channel(config.batch_size.max(1));
        // Plain `tokio::spawn` (ambient runtime) is fine here, unlike the
        // client/consumer-*build* calls above: this task only message-passes
        // through the already-alive consumer (`try_next`/`ack`), it doesn't
        // create any new background state of its own — the persistent
        // `IsolatedRuntime` kept in `self.runtime` is what keeps the
        // consumer's underlying connection tasks alive regardless of which
        // runtime this reader task itself happens to run on.
        tokio::spawn(pulsar_reader_task(consumer, field_names, tx));

        Ok(Self {
            rx,
            config: config.clone(),
            schema,
            _runtime: runtime,
        })
    }
}

fn build_schema(fields: &[PulsarFieldSpec]) -> Result<SchemaRef, NexusError> {
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
                        "pulsar field '{}': unsupported type '{other}' (expected int64, float64, boolean, or utf8)",
                        f.name
                    )))
                }
            };
            Ok(Field::new(&f.name, data_type, true))
        })
        .collect::<Result<_, NexusError>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

/// Decodes a raw message payload (assumed JSON) and projects it
/// through `field_names`. A payload that isn't valid JSON, or isn't a
/// JSON object, fails the whole poll cycle loudly rather than being
/// silently skipped.
fn decode_and_project(payload: &[u8], field_names: &[String]) -> Result<Value, NexusError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|e| NexusError::Serialization(format!("pulsar: message isn't valid JSON: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| NexusError::Schema("pulsar: message isn't a JSON object".to_string()))?;

    let mut projected = Map::new();
    for name in field_names {
        if let Some(v) = object.get(name) {
            projected.insert(name.clone(), v.clone());
        }
    }
    Ok(Value::Object(projected))
}

#[async_trait]
impl Source for PulsarSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let idle_timeout = Duration::from_millis(self.config.idle_timeout_ms);
        let batch_size = self.config.batch_size;
        let schema = self.schema.clone();

        Ok(Box::pin(stream::unfold(self, move |source| {
            let schema = schema.clone();
            async move {
                let mut rows: Vec<Value> = Vec::new();
                // End the stream after this many consecutive idle timeouts
                // with an empty buffer. This lets a finite topic drain
                // instead of hanging forever waiting for new messages.
                let mut consecutive_idles: u32 = 0;
                const MAX_CONSECUTIVE_IDLES: u32 = 3;

                loop {
                    match tokio::time::timeout(idle_timeout, source.rx.recv()).await {
                        Ok(Some(PulsarEvent::Row(row))) => {
                            consecutive_idles = 0;
                            rows.push(row);
                            if rows.len() >= batch_size {
                                break;
                            }
                        }
                        Ok(Some(PulsarEvent::Error(e))) => return Some((Err(e), source)),
                        Ok(Some(PulsarEvent::Closed)) | Ok(None) => {
                            // Reader task closed the channel (consumer
                            // closed, or it dropped the sender on its own
                            // terminal error) — end the stream, flushing
                            // whatever was already buffered first.
                            break;
                        }
                        Err(_elapsed) => {
                            if !rows.is_empty() {
                                break;
                            }
                            consecutive_idles += 1;
                            if consecutive_idles >= MAX_CONSECUTIVE_IDLES {
                                return None;
                            }
                        }
                    }
                }

                if rows.is_empty() {
                    return None;
                }
                match RecordBatchBuilder::from_json_rows(schema, &rows) {
                    Ok(batch) => Some((Ok(batch), source)),
                    Err(e) => Some((Err(e), source)),
                }
            }
        })))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_schema_maps_supported_types() {
        let fields = vec![
            PulsarFieldSpec {
                name: "id".into(),
                r#type: "int64".into(),
            },
            PulsarFieldSpec {
                name: "amount".into(),
                r#type: "float64".into(),
            },
            PulsarFieldSpec {
                name: "active".into(),
                r#type: "boolean".into(),
            },
            PulsarFieldSpec {
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
        let fields = vec![PulsarFieldSpec {
            name: "ts".into(),
            r#type: "timestamp".into(),
        }];
        let err = build_schema(&fields).unwrap_err();
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn subscription_type_defaults_to_exclusive() {
        assert_eq!(SubscriptionType::default(), SubscriptionType::Exclusive);
    }

    #[test]
    fn decode_and_project_extracts_only_requested_fields() {
        let payload = br#"{"id": 1, "name": "a", "extra": true}"#;
        let fields = vec!["id".to_string(), "name".to_string()];
        let value = decode_and_project(payload, &fields).unwrap();
        let obj = value.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert!(obj.contains_key("id"));
        assert!(obj.contains_key("name"));
        assert!(!obj.contains_key("extra"));
    }

    #[test]
    fn decode_and_project_rejects_non_json() {
        let fields = vec!["id".to_string()];
        let err = decode_and_project(b"not json", &fields).unwrap_err();
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn decode_and_project_rejects_non_object_json() {
        let fields = vec!["id".to_string()];
        let err = decode_and_project(b"[1,2,3]", &fields).unwrap_err();
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
