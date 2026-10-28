//! Workday connector — Report-as-a-Service (RaaS). `Bridged` (REST +
//! JSON, no ADBC/ODBC driver exists for this API). **Source only,
//! permanently** — see `config.rs`'s doc comment for why (Workday's
//! write path is SOAP-based Integration/Web Services, a separate,
//! much larger protocol surface, not a v1 simplification of
//! something smaller).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod source;

pub use config::WorkdayConnectorConfig;
pub use source::WorkdaySource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Source};

nexus_core::submit_connector!(
    "workday",
    ConnectorCapability::Bridged,
    WorkdayConnectorConfig
);

fn validate_workday_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    let parsed: WorkdayConnectorConfig = serde_json::from_value(cfg.clone())
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()
}

nexus_core::submit_source_builder!(
    "workday",
    validate_workday_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: WorkdayConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let source = WorkdaySource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
