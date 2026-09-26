//! Excel (`.xlsx`) connector — migrated from the enterprise repo to OSS
//! (Fase 32, 2026-09-25). Uses the plugin builder registry
//! (`nexus-core::registry::{SourceBuilder, SinkBuilder}`, originally added
//! to unblock enterprise connectors registering from outside this
//! workspace, `ROADMAP.md` Fase 12 Bloco 3a) rather than a native match arm
//! in `nexus-server::connectors` — still correct now that this crate lives
//! in-tree, just not yet converted to match every other connector's
//! convention (same as `nexus-connector-starburst`'s note on this).

mod config;
mod rows;
mod schema;
mod sink;
mod source;
mod store;

pub use config::{ExcelConnectorConfig, ExcelDataType, ExcelFieldSpec, StorageType};
pub use sink::ExcelSink;
pub use source::ExcelSource;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink, Source};

nexus_core::submit_connector!(
    "excel",
    ConnectorCapability::Bridged,
    ExcelConnectorConfig
);
// `path` is documented (config.rs) as a local filesystem path exactly like
// csv/parquet's own — opts into nexus-core's `LocalPathConnector` registry
// (added this session specifically to unblock this) so an absolute path
// isn't rejected by dag.rs's SSRF-style guard. See that macro's doc comment
// in the public repo for why this can't just be a hardcoded name over there.
nexus_core::submit_local_path_connector!("excel");

fn validate_excel_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<ExcelConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_source_builder!(
    "excel",
    validate_excel_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Source>, NexusError>> {
        Box::pin(async move {
            let parsed: ExcelConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let source = ExcelSource::connect(&parsed).await?;
            Ok(Box::new(source) as Box<dyn Source>)
        })
    }
);

nexus_core::submit_sink_builder!(
    "excel",
    validate_excel_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: ExcelConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = ExcelSink::connect(&parsed)?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
