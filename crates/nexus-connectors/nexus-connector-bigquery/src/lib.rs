//! BigQuery connector — third crate in this repo, second
//! `ConnectorCapability::AdbcNative` enterprise connector (alongside
//! Snowflake). Uses the official Apache Arrow `adbc-driver-bigquery`
//! (prebuilt Go binary, see `scripts/fetch-adbc-bigquery-driver.sh`),
//! same `adbc_driver_manager` crate every other ADBC connector in this
//! workspace (and the public repo's `nexus-connector-postgres`/
//! `nexus-connector-sqlite`) already uses.
//!
//! Registers the same two ways every connector in this repo does — see
//! `nexus-connector-excel`'s `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod quoting;
mod sink;
mod source;

pub use config::{BigqueryAuth, BigqueryConnectorConfig};
pub use sink::BigquerySink;
pub use source::BigquerySource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "bigquery",
    ConnectorCapability::AdbcNative,
    BigqueryConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<BigqueryConnectorConfig, NexusError> {
    let parsed: BigqueryConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_bigquery_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "bigquery",
    validate_bigquery_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = BigquerySource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "bigquery",
    validate_bigquery_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = BigquerySink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
