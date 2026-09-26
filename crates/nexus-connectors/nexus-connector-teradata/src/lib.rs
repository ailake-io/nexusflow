//! Teradata connector — `Bridged` (ODBC, no ADBC/pure-Rust Teradata
//! driver we can ship, same story as Oracle/HANA in this repo). Same
//! ODBC-batch skeleton as `nexus-connector-hana`, swapped for
//! Teradata's own catalog view (`DBC.ColumnsV`) and upsert idiom
//! (`UPDATE ... ELSE INSERT`, no native `MERGE`/`UPSERT` statement).
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod driver;
mod row_mapping;
mod schema;
mod sink;
mod source;
mod sql;

pub use config::TeradataConnectorConfig;
pub use sink::TeradataSink;
pub use source::TeradataSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "teradata",
    ConnectorCapability::Bridged,
    TeradataConnectorConfig
);

fn validate_teradata_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<TeradataConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "teradata",
    validate_teradata_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: TeradataConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let source = TeradataSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "teradata",
    validate_teradata_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: TeradataConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            parsed.validate()?;
            let sink = TeradataSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
