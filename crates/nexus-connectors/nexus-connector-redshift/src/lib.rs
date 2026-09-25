//! Redshift connector — fourth crate, second `AdbcNative` reusing an
//! *existing* driver instead of a new one: Redshift is wire-compatible
//! with the Postgres protocol, so this reuses the exact ADBC Postgres
//! driver the public repo's `nexus-connector-postgres` already builds
//! and validates (`scripts/build-adbc-postgresql-driver.sh`), only
//! pointed at a different host/port.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod sink;
mod source;

pub use config::RedshiftConnectorConfig;
pub use sink::RedshiftSink;
pub use source::RedshiftSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "redshift",
    ConnectorCapability::AdbcNative,
    RedshiftConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<RedshiftConnectorConfig, NexusError> {
    let parsed: RedshiftConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_redshift_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "redshift",
    validate_redshift_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = RedshiftSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "redshift",
    validate_redshift_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = RedshiftSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
