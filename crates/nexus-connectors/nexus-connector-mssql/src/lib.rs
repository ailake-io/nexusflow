//! SQL Server / Azure Synapse connector — ninth crate. Uses the free,
//! no-account ADBC Driver Foundry `mssql` driver (confirmed real this
//! session — `dbc install mssql` installed it with no auth needed,
//! under a redistribution-permitting license, unlike Oracle/HANA).
//!
//! Registers three catalog entries from this one crate:
//! - `"mssql"` (`AdbcNative`) — batch source + sink, same driver/config
//!   also used for `"synapse"` (Azure Synapse dedicated SQL pools
//!   speak the same TDS/T-SQL protocol — no separate driver or code
//!   needed, see README for Synapse-specific `MERGE` limitations).
//! - `"mssql-cdc"` (`Bridged`) — native CDC source, SQL Server only
//!   (Synapse has no CDC equivalent, see `cdc.rs`'s doc comment).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod cdc;
mod config;
mod driver;
mod sink;
mod source;

pub use cdc::MssqlCdcSource;
pub use config::{MssqlCdcConfig, MssqlConnectorConfig};
pub use sink::MssqlSink;
pub use source::MssqlSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "mssql",
    ConnectorCapability::AdbcNative,
    MssqlConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<MssqlConnectorConfig, NexusError> {
    let parsed: MssqlConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_mssql_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "mssql",
    validate_mssql_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = MssqlSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "mssql",
    validate_mssql_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = MssqlSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);

nexus_core::submit_connector!("mssql-cdc", ConnectorCapability::Bridged, MssqlCdcConfig);

fn validate_mssql_cdc_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<MssqlCdcConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "mssql-cdc",
    validate_mssql_cdc_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: MssqlCdcConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = MssqlCdcSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

// Azure Synapse dedicated SQL pool speaks the same TDS/T-SQL protocol
// as SQL Server — reuses `MssqlConnectorConfig`/`MssqlSource`/
// `MssqlSink` entirely, just a separate catalog name (no `"synapse-cdc"`
// — Synapse has no native CDC, see `cdc.rs`).
nexus_core::submit_connector!(
    "synapse",
    ConnectorCapability::AdbcNative,
    MssqlConnectorConfig
);

nexus_core::submit_source_builder!(
    "synapse",
    validate_mssql_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: MssqlConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = MssqlSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "synapse",
    validate_mssql_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: MssqlConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = MssqlSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
