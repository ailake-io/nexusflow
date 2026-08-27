//! Apache Pulsar connector — twenty-fourth crate, last of the tier-6
//! batch. `Bridged` (own binary protocol via the `pulsar` crate, no
//! ADBC/ODBC). Source only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod source;

pub use config::{PulsarConnectorConfig, PulsarFieldSpec, SubscriptionType};
pub use source::PulsarSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_enterprise_connector!(
    "pulsar",
    ConnectorCapability::Bridged,
    PulsarConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<PulsarConnectorConfig, NexusError> {
    let parsed: PulsarConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_pulsar_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "pulsar",
    validate_pulsar_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = PulsarSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
