//! Generic bridging connector for MQTT (IoT/sensor telemetry): each message
//! payload is JSON, projected onto `fields`. Not CDC — a wildcard topic
//! filter blends many logical sensors into one subscription, so every row
//! carries the concrete topic it arrived on in `__mqtt_topic` (same
//! metadata-column precedent as CDC's `__opcode`). See ARCHITECTURE.md
//! §2/§4.1.
//!
//! `rumqttc` is a real async network dependency, so the client is behind
//! the `client` Cargo feature (CLAUDE.md §8.5) — building this crate with
//! no features enabled compiles config/payload parsing only, no network
//! client linkage.

mod config;
mod payload;
#[cfg(feature = "client")]
mod source;

pub use config::{MqttConnectorConfig, MqttDataType, MqttFieldSpec, MqttQos};
pub use payload::{build_schema, parse_payload, MQTT_TOPIC_COLUMN};
#[cfg(feature = "client")]
pub use source::MqttSource;

#[cfg(feature = "client")]
nexus_core::submit_connector!(
    "mqtt",
    nexus_core::ConnectorCapability::Bridged,
    MqttConnectorConfig
);
