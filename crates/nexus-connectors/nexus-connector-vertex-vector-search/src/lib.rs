//! Vertex AI Vector Search connector — twentieth crate, fourth of the
//! tier-6 vector/search group. `Bridged` (REST + JSON, no ADBC/ODBC
//! driver exists for this). Sink only.
//!
//! Registers the same two ways `nexus-connector-excel` does — see that
//! crate's `lib.rs` doc comment for the full rationale.

mod auth;
mod config;
mod rows;
mod sink;

pub use config::VertexVectorSearchConnectorConfig;
pub use sink::VertexVectorSearchSink;

use futures::future::BoxFuture;
use nexus_core::{ConnectorCapability, NexusError, Sink};

nexus_core::submit_connector!(
    "vertex-vector-search",
    ConnectorCapability::Bridged,
    VertexVectorSearchConnectorConfig
);

fn validate_vertex_vector_search_config(cfg: &serde_json::Value) -> Result<(), NexusError> {
    serde_json::from_value::<VertexVectorSearchConnectorConfig>(cfg.clone())
        .map(|_| ())
        .map_err(|e| NexusError::Serialization(e.to_string()))
}

nexus_core::submit_sink_builder!(
    "vertex-vector-search",
    validate_vertex_vector_search_config,
    |cfg: serde_json::Value| -> BoxFuture<'static, Result<Box<dyn Sink>, NexusError>> {
        Box::pin(async move {
            let parsed: VertexVectorSearchConnectorConfig = serde_json::from_value(cfg)
                .map_err(|e| NexusError::Serialization(e.to_string()))?;
            let sink = VertexVectorSearchSink::connect(&parsed).await?;
            Ok(Box::new(sink) as Box<dyn Sink>)
        })
    }
);
