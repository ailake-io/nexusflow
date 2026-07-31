//! AI Lakehouse vector sink #2 (Qdrant) — ROADMAP.md Fase 5 order. See
//! ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.

mod config;
mod rows;
mod sink;

pub use config::QdrantConnectorConfig;
pub use sink::QdrantSink;

nexus_core::submit_connector!("qdrant", nexus_core::ConnectorCapability::Bridged);
