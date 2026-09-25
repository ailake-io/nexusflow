//! Oracle connector — seventh crate, second `Bridged` one (after
//! Salesforce): no ADBC Oracle driver we can ship (the only one found,
//! via Columnar, requires a third-party paid trial account — see
//! README's "Config do conector Oracle" section for why that was
//! rejected). Connects via ODBC (`odbc-api`, same crate
//! `nexus-connector-odbc` already uses in the public repo) + the
//! official Oracle Instant Client ODBC driver.
//!
//! Registers two catalog entries from this one crate:
//! - `"oracle"` (`Bridged`) — batch source + sink.
//! - `"oracle-cdc"` (`Bridged`) — LogMiner-based native CDC source,
//!   same "module inside the batch connector's crate" pattern
//!   `mssql-cdc` (`nexus-connector-mssql`) already uses. See `cdc.rs`'s
//!   doc comment.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod cdc;
mod config;
mod driver;
mod redo_parser;
mod row_mapping;
mod schema;
mod sink;
mod source;
mod sql;

pub use cdc::OracleCdcSource;
pub use config::{OracleCdcConfig, OracleConnectorConfig};
pub use sink::OracleSink;
pub use source::OracleSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "oracle",
    ConnectorCapability::Bridged,
    OracleConnectorConfig
);

fn validate_oracle_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<OracleConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "oracle",
    validate_oracle_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: OracleConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let source = OracleSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "oracle",
    validate_oracle_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: OracleConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let sink = OracleSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);

nexus_core::submit_connector!(
    "oracle-cdc",
    ConnectorCapability::Bridged,
    OracleCdcConfig
);

fn validate_oracle_cdc_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<OracleCdcConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "oracle-cdc",
    validate_oracle_cdc_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: OracleCdcConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = OracleCdcSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);
