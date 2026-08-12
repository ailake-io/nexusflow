//! AI Lakehouse vector sink #3 (LanceDB, embedded) — ROADMAP.md Fase 5
//! order. See ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.

mod config;
mod sink;

pub use config::{LanceDbConnectorConfig, LanceDbStorageOptions};
pub use sink::LanceDbSink;

nexus_core::submit_connector!(
    "lancedb",
    nexus_core::ConnectorCapability::Bridged,
    LanceDbConnectorConfig
);
