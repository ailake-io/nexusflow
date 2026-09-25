//! Snowflake connector — second real crate in this repo, first
//! `ConnectorCapability::AdbcNative` enterprise connector (Excel is
//! `Bridged`). Uses the official Apache Arrow `adbc-driver-snowflake`
//! (prebuilt Go binary, see `scripts/fetch-adbc-snowflake-driver.sh`),
//! same `adbc_driver_manager` crate the public repo's
//! `nexus-connector-postgres`/`nexus-connector-sqlite` already use.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod sink;
mod source;

pub use config::{SnowflakeAuth, SnowflakeConnectorConfig};
pub use sink::SnowflakeSink;
pub use source::SnowflakeSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "snowflake",
    ConnectorCapability::AdbcNative,
    SnowflakeConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<SnowflakeConnectorConfig, NexusError> {
    let parsed: SnowflakeConnectorConfig = serde_json::from_value(cfg)
        .map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_snowflake_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "snowflake",
    validate_snowflake_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = SnowflakeSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "snowflake",
    validate_snowflake_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = SnowflakeSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
