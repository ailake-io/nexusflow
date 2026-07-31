//! AI Lakehouse vector sink: pgvector-backed Postgres table. First of the 6
//! vector sinks in ROADMAP.md Fase 5 order (simplest to operate first) —
//! see ARCHITECTURE.md §4.3 and IMPLEMENTATION_PLAN.md Marco 5. This is
//! where Marco 5's critério de pronto closes: texto→chunk→embedding(CPU)→
//! pgvector end-to-end.

mod config;
mod rows;
mod sink;
mod sql;

pub use config::PgVectorConnectorConfig;
pub use sink::PgVectorSink;

nexus_core::submit_connector!("pgvector", nexus_core::ConnectorCapability::Bridged);
