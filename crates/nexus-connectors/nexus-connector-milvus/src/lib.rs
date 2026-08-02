//! AI Lakehouse vector sink #4 (Milvus) — ROADMAP.md Fase 5 order. See
//! ARCHITECTURE.md §4.3, IMPLEMENTATION_PLAN.md Marco 5.

mod config;
mod sink;

pub use config::MilvusConnectorConfig;
pub use sink::MilvusSink;

nexus_core::submit_connector!(
    "milvus",
    nexus_core::ConnectorCapability::Bridged,
    MilvusConnectorConfig
);
