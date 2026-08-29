use crate::config::{MqttConnectorConfig, MqttQos};
use crate::payload::{build_schema, parse_payload, MQTT_TOPIC_COLUMN};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, RecordBatchBuilder, Source};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, Transport};
use std::time::Duration;

/// MQTT subscriber — decodes each message payload as JSON and projects it
/// onto the configured schema, tagging every row with the topic it arrived
/// on. See ARCHITECTURE.md §4.1.
///
/// Resume is broker-side: a persistent session (`clean_session: false` +
/// a stable `client_id`, both set unconditionally below) makes the broker
/// queue QoS 1/2 messages published while this connector is offline and
/// redeliver them on reconnect — no `Source::position_handle`/
/// `CheckpointCursor` involved, same "nothing to persist" story as
/// `postgres-cdc`'s replication slot. See ARCHITECTURE.md §7.
pub struct MqttSource {
    client: AsyncClient,
    eventloop: EventLoop,
    schema: SchemaRef,
    batch_size: usize,
    poll_timeout: Duration,
    max_messages: usize,
}

impl MqttSource {
    pub async fn connect(config: &MqttConnectorConfig) -> Result<Self, NexusError> {
        let schema = if config.fields.is_empty() {
            infer_schema(config).await?
        } else {
            build_schema(&config.fields)
        };

        let options = build_options(config, &config.client_id, false)?;
        let (client, mut eventloop) = AsyncClient::builder(options)
            .capacity(100)
            .try_build()
            .map_err(|e| NexusError::Connector(format!("mqtt client build failed: {e}")))?;
        let qos = match config.qos {
            MqttQos::AtMostOnce => QoS::AtMostOnce,
            MqttQos::AtLeastOnce => QoS::AtLeastOnce,
            MqttQos::ExactlyOnce => QoS::ExactlyOnce,
        };
        client
            .subscribe(config.topic_filter.clone(), qos)
            .await
            .map_err(|e| NexusError::Connector(format!("mqtt subscribe failed: {e}")))?;

        // `subscribe().await` only queues the SUBSCRIBE packet — nothing
        // drives the connection or sends it until `eventloop.poll()` runs,
        // which otherwise only happens inside `read_batches`. Without this,
        // any message published between `connect` returning and the first
        // `read_batches` call races the broker's SUBACK and is silently
        // dropped (the broker doesn't know about the subscription yet).
        // Block here until the SUBACK actually comes back.
        let poll_timeout = Duration::from_millis(config.poll_timeout_ms);
        loop {
            let next = tokio::time::timeout(poll_timeout, eventloop.poll()).await;
            match next {
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => break,
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => {
                    return Err(NexusError::Connector(format!(
                        "mqtt connection failed while awaiting subscribe ack: {e}"
                    )))
                }
                Err(_) => {
                    return Err(NexusError::Connector(
                        "mqtt broker did not ack subscribe within poll_timeout_ms".into(),
                    ))
                }
            }
        }

        Ok(Self {
            client,
            eventloop,
            schema,
            batch_size: config.batch_size,
            poll_timeout: Duration::from_millis(config.poll_timeout_ms),
            max_messages: config.max_messages,
        })
    }
}

/// Builds `MqttOptions` shared by the real connection and the schema-sample
/// one below — same host/port/TLS/credentials, different `client_id` and
/// `clean_session`.
fn build_options(
    config: &MqttConnectorConfig,
    client_id: &str,
    clean_session: bool,
) -> Result<MqttOptions, NexusError> {
    let (host, port, use_tls) = config
        .host_port_tls()
        .map_err(|e| NexusError::Connector(format!("mqtt broker_url invalid: {e}")))?;

    let mut options = MqttOptions::new(client_id, (host, port));
    options.set_keep_alive(30);
    options.set_clean_session(clean_session);

    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username.clone(), password.clone());
    }

    if use_tls {
        options.set_transport(build_transport(config)?);
    }

    Ok(options)
}

/// Samples up to `config.schema_sample_rows` messages and infers a schema
/// — source-side fallback for when `fields` is left empty (see
/// `MqttConnectorConfig::fields` doc comment). Connects with a throwaway
/// `client_id` and `clean_session: true` instead of the real
/// `config.client_id` — the real client always uses a *persistent*
/// session (`clean_session: false`, see the struct doc comment on
/// `MqttSource`), so sampling through it would ack and drain QoS 1/2
/// messages the broker was holding for the real run. A disposable client
/// with its own clean session can't touch that state at all.
async fn infer_schema(config: &MqttConnectorConfig) -> Result<SchemaRef, NexusError> {
    let sample_client_id = format!("{}-schema-sample-{}", config.client_id, std::process::id());
    let options = build_options(config, &sample_client_id, true)?;
    let (client, mut eventloop) = AsyncClient::builder(options)
        .capacity(100)
        .try_build()
        .map_err(|e| {
            NexusError::Connector(format!("mqtt schema-sample client build failed: {e}"))
        })?;
    let qos = match config.qos {
        MqttQos::AtMostOnce => QoS::AtMostOnce,
        MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        MqttQos::ExactlyOnce => QoS::ExactlyOnce,
    };
    client
        .subscribe(config.topic_filter.clone(), qos)
        .await
        .map_err(|e| NexusError::Connector(format!("mqtt schema-sample subscribe failed: {e}")))?;

    let poll_timeout = Duration::from_millis(config.poll_timeout_ms);
    let mut rows = Vec::new();
    while rows.len() < config.schema_sample_rows {
        match tokio::time::timeout(poll_timeout, eventloop.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(publish)))) => {
                if let Ok(row) = parse_payload(&publish.payload) {
                    rows.push(row);
                }
            }
            Ok(Ok(_)) => continue,
            // Idle cutoff, connection error, or the broker never acked —
            // treat the sample as complete with whatever was collected.
            _ => break,
        }
    }
    Ok(RecordBatchBuilder::infer_schema(&rows))
}

fn build_transport(config: &MqttConnectorConfig) -> Result<Transport, NexusError> {
    use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let mut roots = RootCertStore::empty();
    match &config.ca_cert_path {
        Some(path) => {
            let bytes = std::fs::read(path).map_err(|e| {
                NexusError::Connector(format!("mqtt ca_cert_path read failed: {e}"))
            })?;
            for cert in rustls_pemfile::certs(&mut bytes.as_slice()) {
                let cert = cert.map_err(|e| {
                    NexusError::Connector(format!("mqtt ca_cert_path parse failed: {e}"))
                })?;
                roots.add(cert).map_err(|e| {
                    NexusError::Connector(format!("mqtt ca_cert_path not trusted: {e}"))
                })?;
            }
        }
        None => {
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    let client_config = match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert_bytes = std::fs::read(cert_path).map_err(|e| {
                NexusError::Connector(format!("mqtt client_cert_path read failed: {e}"))
            })?;
            let key_bytes = std::fs::read(key_path).map_err(|e| {
                NexusError::Connector(format!("mqtt client_key_path read failed: {e}"))
            })?;
            let cert_chain: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut cert_bytes.as_slice())
                    .collect::<Result<_, _>>()
                    .map_err(|e| {
                        NexusError::Connector(format!("mqtt client_cert_path parse failed: {e}"))
                    })?;
            let key: PrivateKeyDer<'static> =
                rustls_pemfile::private_key(&mut key_bytes.as_slice())
                    .map_err(|e| {
                        NexusError::Connector(format!("mqtt client_key_path parse failed: {e}"))
                    })?
                    .ok_or_else(|| {
                        NexusError::Connector("mqtt client_key_path has no private key".into())
                    })?;
            builder
                .with_client_auth_cert(cert_chain, key)
                .map_err(|e| NexusError::Connector(format!("mqtt client cert invalid: {e}")))?
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(Transport::tls_with_config(client_config.into()))
}

#[async_trait]
impl Source for MqttSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let mut batches = Vec::new();
        let mut buffer = Vec::with_capacity(self.batch_size);
        let mut consumed = 0usize;

        while consumed < self.max_messages {
            let next = tokio::time::timeout(self.poll_timeout, self.eventloop.poll()).await;
            let event = match next {
                Ok(Ok(event)) => event,
                Ok(Err(e)) => return Err(NexusError::Connector(format!("mqtt poll error: {e}"))),
                // Idle cutoff reached — subscription treated as drained for
                // this read (MQTT telemetry has no natural end).
                Err(_) => break,
            };

            let Event::Incoming(Packet::Publish(publish)) = event else {
                continue;
            };

            let mut row = parse_payload(&publish.payload)?;
            if let Some(obj) = row.as_object_mut() {
                // `Publish::topic` is `Bytes`, not `String`, in this crate
                // (unlike the classic `rumqttc` this replaced) — MQTT topics
                // are UTF-8 by protocol spec, so lossy conversion only ever
                // kicks in for a broker sending malformed data.
                obj.insert(
                    MQTT_TOPIC_COLUMN.to_string(),
                    serde_json::Value::String(String::from_utf8_lossy(&publish.topic).into_owned()),
                );
            }
            buffer.push(row);
            consumed += 1;

            if buffer.len() >= self.batch_size {
                batches.push(RecordBatchBuilder::from_json_rows(
                    self.schema.clone(),
                    &buffer,
                )?);
                buffer.clear();
            }
        }
        if !buffer.is_empty() {
            batches.push(RecordBatchBuilder::from_json_rows(
                self.schema.clone(),
                &buffer,
            )?);
        }

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// Silences an unused-field warning: `client` is kept alive on the struct so
// the connection isn't dropped mid-subscription (AsyncClient's handle must
// outlive the eventloop it drives), even though only `subscribe` at connect
// time calls a method on it directly.
#[allow(dead_code)]
fn _assert_client_kept_alive(source: &MqttSource) -> &AsyncClient {
    &source.client
}
