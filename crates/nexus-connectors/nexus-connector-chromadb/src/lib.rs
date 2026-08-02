//! AI Lakehouse vector sink #6 (ChromaDB, last/most complex to operate) —
//! ROADMAP.md Fase 5 order. See ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md
//! Marco 5.

mod config;
mod rows;
mod sink;

pub use config::ChromaConnectorConfig;
pub use sink::ChromaSink;

nexus_core::submit_connector!(
    "chromadb",
    nexus_core::ConnectorCapability::Bridged,
    ChromaConnectorConfig
);
