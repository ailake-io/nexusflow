//! AI Lakehouse vector sink #5 (Pinecone) — ROADMAP.md Fase 5 order. See
//! ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.

mod config;
mod rows;
mod sink;

pub use config::PineconeConnectorConfig;
pub use sink::PineconeSink;

nexus_core::submit_connector!("pinecone", nexus_core::ConnectorCapability::Bridged);
