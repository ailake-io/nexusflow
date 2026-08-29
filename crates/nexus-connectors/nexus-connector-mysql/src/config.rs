use serde::Deserialize;

/// Batch connector config (`"mysql"`) — no ADBC driver exists for MySQL
/// upstream (unlike Postgres/SQLite), so this is a bridging connector like
/// `nexus-connector-mongodb`: `mysql_async` for the wire protocol,
/// `RecordBatchBuilder` to get to Arrow. See ARCHITECTURE.md §2/§4.1.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill the individual connection fields (`host`, `port`, `username`, ...).
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MySqlConnectorConfig {
    /// Full `mysql://user:pass@host:port/db` connection URI.
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. Keeps backward compatibility with older
    /// pipelines that already store a complete connection string.
    #[serde(default)]
    pub uri: Option<String>,
    /// Database server host name or IP address (e.g. `localhost` or
    /// `db.example.com`).
    pub host: String,
    /// TCP port the MySQL server is listening on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// User name used to connect to the MySQL server.
    pub username: String,
    /// Password for `username`.
    pub password: String,
    /// Database (schema) name that contains `table`.
    pub database: String,
    /// Table name this connector reads from or writes to.
    pub table: String,
    /// Column used as the upsert key on the sink side — see
    /// ARCHITECTURE.md §5 (idempotency is a `Sink` contract, not optional).
    /// Also used to build the `DELETE ... WHERE` clause for rows carrying the
    /// `__opcode = "D"` marker (same convention as `mongodb`/CDC sinks).
    pub primary_key: String,
    /// Target schema, matched **by name** to `table`'s actual columns (unlike
    /// `mysql-cdc`'s positional matching — a plain `SELECT`/`INSERT` here
    /// names its columns explicitly, so there's no binlog ambiguity to work
    /// around). Same 4-primitive-type ceiling as every other bridging
    /// connector. Left empty **on the source side**, the connector runs
    /// `SHOW COLUMNS FROM table` and maps each MySQL type onto the closest
    /// of the 4 (see `source::mysql_type_to_data_type`) instead of erroring
    /// — real column metadata from MySQL's own catalog, not a sample, so
    /// this one isn't best-effort the way CSV/Mongo/Kafka/REST inference
    /// is.
    #[serde(default)]
    pub fields: Vec<MySqlCdcFieldSpec>,
    /// How many rows to fold into a single `RecordBatch` while scanning, and
    /// the batch size for `exec_batch` writes on the sink side.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Timeout in seconds for each call to MySQL (connect, query, exec) — a
    /// stalled connection would otherwise block the pipeline indefinitely
    /// (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl MySqlConnectorConfig {
    /// Returns the MySQL connection URI.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise a
    /// `mysql://` URI is built from the individual fields.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            return uri.to_string();
        }

        format!(
            "mysql://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            percent_encode(&self.password),
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        )
    }
}

fn default_batch_size() -> usize {
    1000
}

/// Native CDC source config (`"mysql-cdc"`) — reads the binlog directly, no
/// Debezium/Kafka in front (`ARCHITECTURE.md §7`). CDC is the only streaming
/// mode; this crate's batch mode above (`"mysql"`) is the table-scan
/// counterpart, same split as `nexus-connector-mongodb`'s `mongodb`/
/// `mongodb-cdc`.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill the individual connection fields (`host`, `port`, `username`, ...),
/// which are used to open the MySQL replication connection.
#[cfg(feature = "cdc")]
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MySqlCdcConfig {
    /// Full `mysql://user:pass@host:port/db` connection URI.
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. Keeps backward compatibility with older
    /// pipelines that already store a complete connection string.
    #[serde(default)]
    pub uri: Option<String>,
    /// Database server host name or IP address (e.g. `localhost` or
    /// `db.example.com`).
    pub host: String,
    /// TCP port the MySQL server is listening on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// User name used to connect to the MySQL server for replication.
    ///
    /// This account needs the `REPLICATION SLAVE` and `REPLICATION CLIENT`
    /// privileges on the source server.
    pub username: String,
    /// Password for the replication user.
    pub password: String,
    /// Database (schema) name that contains the table being replicated.
    ///
    /// This is used client-side to filter binlog events by database; the
    /// replication user itself typically does not need per-database grants.
    pub database: String,
    /// Table name this connector reads changes for.
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
    /// Resume position within `binlog_filename`. Must be paired with
    /// `binlog_filename`; if either is missing the connector starts from the
    /// current end of the binlog.
    #[serde(default)]
    pub binlog_position: Option<u32>,
    /// Maximum number of change events to collect in a single run. After this
    /// many events the source ends cleanly, letting the run finish and the
    /// scheduler start the next micro-batch. The MySQL replication stream
    /// resumes from the last binlog position, so the next run picks up where
    /// this one left off.
    #[serde(default = "default_max_batch_events")]
    pub max_batch_events: u64,
}

#[cfg(feature = "cdc")]
impl MySqlCdcConfig {
    /// Returns the MySQL connection URI.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise a
    /// `mysql://` URI is built from the individual fields.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            return uri.to_string();
        }

        format!(
            "mysql://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            percent_encode(&self.password),
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        )
    }
}

fn default_port() -> u16 {
    3306
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(feature = "cdc")]
fn default_server_id() -> u32 {
    65535
}

#[cfg(feature = "cdc")]
fn default_max_batch_events() -> u64 {
    1000
}

/// Minimal percent-encoding helper for connection-string components.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' | '@' | ':' | '/' | '?' | '#' | '[' | ']' | ' ' => {
                for b in c.encode_utf8(&mut [0; 4]).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MySqlCdcFieldSpec {
    /// Column name as emitted in the CDC event.
    pub name: String,
    /// Arrow data type this column's value is coerced to.
    pub data_type: MySqlCdcDataType,
    /// Whether the column may contain NULL values.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_connection_string_returns_uri_when_present() {
        let cfg = MySqlConnectorConfig {
            uri: Some("mysql://legacy".to_string()),
            host: "ignored".to_string(),
            port: 3307,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            database: "ignored".to_string(),
            table: "t".to_string(),
            primary_key: "id".to_string(),
            fields: vec![],
            batch_size: 1000,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_string(), "mysql://legacy");
    }

    #[test]
    fn batch_connection_string_builds_from_fields() {
        let cfg = MySqlConnectorConfig {
            uri: None,
            host: "db.example.com".to_string(),
            port: 3306,
            username: "app".to_string(),
            password: "s3cr@t".to_string(),
            database: "production".to_string(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            fields: vec![],
            batch_size: 1000,
            timeout_seconds: 30,
        };
        assert_eq!(
            cfg.connection_string(),
            "mysql://app:s3cr%40t@db.example.com:3306/production"
        );
    }

    #[test]
    #[cfg(feature = "cdc")]
    fn connection_string_returns_uri_when_present() {
        let cfg = MySqlCdcConfig {
            uri: Some("mysql://legacy".to_string()),
            host: "ignored".to_string(),
            port: 3307,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            database: "ignored".to_string(),
            table: "t".to_string(),
            server_id: 1,
            fields: vec![],
            binlog_filename: None,
            binlog_position: None,
            max_batch_events: 1000,
        };
        assert_eq!(cfg.connection_string(), "mysql://legacy");
    }

    #[test]
    #[cfg(feature = "cdc")]
    fn connection_string_builds_from_fields() {
        let cfg = MySqlCdcConfig {
            uri: None,
            host: "db.example.com".to_string(),
            port: 3306,
            username: "repl".to_string(),
            password: "s3cr@t".to_string(),
            database: "production".to_string(),
            table: "events".to_string(),
            server_id: 65535,
            fields: vec![],
            binlog_filename: None,
            binlog_position: None,
            max_batch_events: 1000,
        };
        assert_eq!(
            cfg.connection_string(),
            "mysql://repl:s3cr%40t@db.example.com:3306/production"
        );
    }
}
