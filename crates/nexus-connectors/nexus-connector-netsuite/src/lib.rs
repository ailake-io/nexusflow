//! NetSuite connector — SuiteQL for reads
//! (`/services/rest/query/v1/suiteql`), REST Record API for writes
//! (`/services/rest/record/v1/{record_type}`). `Bridged` (REST +
//! JSON, no ADBC/ODBC driver exists for this API). Source + sink.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod config;
mod sink;
mod source;

pub use config::NetsuiteConnectorConfig;
pub use sink::NetsuiteSink;
pub use source::NetsuiteSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "netsuite",
    ConnectorCapability::Bridged,
    NetsuiteConnectorConfig
);

fn parse_and_validate(cfg: serde_json::Value) -> Result<NetsuiteConnectorConfig, NexusError> {
    let parsed: NetsuiteConnectorConfig =
        serde_json::from_value(cfg).map_err(|e| NexusError::Serialization(e.to_string()))?;
    parsed.validate()?;
    Ok(parsed)
}

fn validate_netsuite_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    parse_and_validate(cfg.clone()).map(|_| ())
}

nexus_core::submit_source_builder!(
    "netsuite",
    validate_netsuite_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let source = NetsuiteSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "netsuite",
    validate_netsuite_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed = parse_and_validate(cfg)?;
            let sink = NetsuiteSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
