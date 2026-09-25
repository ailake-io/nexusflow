//! Apache Pulsar connector — twenty-fourth crate, last of the tier-6
//! batch. `Bridged` (own binary protocol via the `pulsar` crate, no
//! ADBC/ODBC). Source + sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::{PulsarConnectorConfig, PulsarFieldSpec, SubscriptionType};
pub use sink::PulsarSink;
pub use source::PulsarSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "pulsar",
    ConnectorCapability::Bridged,
    PulsarConnectorConfig
);

fn validate_pulsar_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    let parsed: PulsarConnectorConfig = serde_json::from_value(cfg.clone())
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()
}

nexus_core::submit_source_builder!(
    "pulsar",
    validate_pulsar_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: PulsarConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = PulsarSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "pulsar",
    validate_pulsar_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: PulsarConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = PulsarSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
