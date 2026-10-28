//! Salesforce connector — fifth crate, first `ConnectorCapability::
//! Bridged` connector migrated from the old enterprise catalog since Excel: no ADBC driver exists for
//! Salesforce, so this talks REST + Bulk API 2.0 directly via
//! `reqwest`, closer in shape to `nexus-connector-rest` (public repo)
//! than to the 4 `AdbcNative` connectors already in this workspace.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod bulk;
mod config;
mod describe;
mod sink;
mod source;

pub use config::SalesforceConnectorConfig;
pub use sink::SalesforceSink;
pub use source::SalesforceSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "salesforce",
    ConnectorCapability::Bridged,
    SalesforceConnectorConfig
);

fn validate_salesforce_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<SalesforceConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "salesforce",
    validate_salesforce_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: SalesforceConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = SalesforceSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "salesforce",
    validate_salesforce_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: SalesforceConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = SalesforceSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
