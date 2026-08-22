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
        let (host, port, use_tls) = config
            .host_port_tls()
            .map_err(|e| NexusError::Connector(format!("mqtt broker_url invalid: {e}")))?;

        let mut options = MqttOptions::new(config.client_id.clone(), host, port);
        options.set_keep_alive(Duration::from_secs(30));
        // Persistent session: see the struct doc comment above for why this
        // is never `true`.
        options.set_clean_session(false);

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            options.set_credentials(username.clone(), password.clone());
        }

        if use_tls {
            options.set_transport(build_transport(config)?);
        }

        let (client, eventloop) = AsyncClient::new(options, 100);
        let qos = match config.qos {
            MqttQos::AtMostOnce => QoS::AtMostOnce,
            MqttQos::AtLeastOnce => QoS::AtLeastOnce,
            MqttQos::ExactlyOnce => QoS::ExactlyOnce,
        };
        client
            .subscribe(config.topic_filter.clone(), qos)
            .await
            .map_err(|e| NexusError::Connector(format!("mqtt subscribe failed: {e}")))?;

        Ok(Self {
            client,
            eventloop,
            schema: build_schema(&config.fields),
            batch_size: config.batch_size,
            poll_timeout: Duration::from_millis(config.poll_timeout_ms),
            max_messages: config.max_messages,
        })
    }
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
                obj.insert(
                    MQTT_TOPIC_COLUMN.to_string(),
                    serde_json::Value::String(publish.topic.clone()),
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
