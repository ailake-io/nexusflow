use serde::Deserialize;

/// Native CDC source config (`"mysql-cdc"`) — reads the binlog directly, no
/// Debezium/Kafka in front (`ARCHITECTURE.md §7`). CDC-only: no batch mode,
/// same posture as `nexus-connector-kafka` (a connector inherently built
/// around a streaming protocol, not a table scan).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MySqlCdcConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Needs the `REPLICATION SLAVE`/`REPLICATION CLIENT` privileges.
    pub username: String,
    pub password: String,
    pub database: String,
    pub table: String,
    /// Fake replica server id registered with the MySQL master — must be
    /// unique among every server (real or replica) connected to it.
    #[serde(default = "default_server_id")]
    pub server_id: u32,
    /// Target schema for each row — matched **positionally** to the table's
    /// actual column order, not by name: MySQL's binlog protocol doesn't
    /// carry column names unless the server has `binlog_row_metadata=FULL`
    /// set (off by default), so this connector doesn't depend on it. Same
    /// 4-primitive-type ceiling as every other bridging connector.
    pub fields: Vec<MySqlCdcFieldSpec>,
    /// Resume position — set both to continue from a specific point,
    /// otherwise streaming starts from the current end of the binlog (same
    /// "static config field, not server-injected" resume model as Kafka's
    /// `start_offsets`).
    #[serde(default)]
    pub binlog_filename: Option<String>,
    #[serde(default)]
    pub binlog_position: Option<u32>,
}

fn default_port() -> u16 {
    3306
}

fn default_server_id() -> u32 {
    65535
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MySqlCdcFieldSpec {
    pub name: String,
    pub data_type: MySqlCdcDataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MySqlCdcDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}
