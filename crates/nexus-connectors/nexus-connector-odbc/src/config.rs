use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// You can either provide a complete `connection_string` (legacy form, takes
/// precedence) or fill the individual ODBC fields (`driver`, `server`,
/// `database`, ...), which are assembled into an ODBC connection string when
/// `connection_string` is empty.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OdbcConnectorConfig {
    /// Full ODBC connection string (`Driver={...};Server=...;...`).
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. Keeps backward compatibility with older
    /// pipelines that already store a complete connection string.
    #[serde(default)]
    pub connection_string: Option<String>,
    /// ODBC driver name, including the curly braces used by the ODBC driver
    /// manager (e.g. `{PostgreSQL Unicode}` or `{ODBC Driver 18 for SQL Server}`).
    ///
    /// The driver must already be registered with unixODBC (or the platform's
    /// ODBC driver manager) on the machine running this connector.
    pub driver: String,
    /// Database server host name or IP address.
    pub server: String,
    /// TCP port the database server is listening on.
    ///
    /// Optional: many ODBC drivers can resolve the default port from the
    /// `Server` value or use the driver's own default.
    #[serde(default)]
    pub port: Option<u16>,
    /// Database/catalog name to connect to.
    ///
    /// Optional for some drivers (e.g. when the database is selected by a
    /// subsequent `USE` statement or when the DSN already encodes it).
    #[serde(default)]
    pub database: Option<String>,
    /// User name to authenticate with.
    pub username: String,
    /// Password for the provided user name.
    #[serde(default)]
    pub password: String,
    /// Whether the connection should be encrypted.
    ///
    /// Maps to driver-specific attributes such as `Encrypt` (SQL Server) or
    /// `SSLmode` (PostgreSQL). When `None`, the driver's default is used.
    #[serde(default)]
    pub encrypt: Option<bool>,
    /// Whether to trust the server's certificate when encryption is enabled.
    ///
    /// Common attribute names: `TrustServerCertificate` (SQL Server),
    /// `SSLmode=disable/require` (PostgreSQL). When `None`, the driver's
    /// default is used.
    #[serde(default)]
    pub trust_server_certificate: Option<bool>,
    /// Login timeout in seconds.
    ///
    /// Optional: when set, adds a `LoginTimeout` attribute to the connection
    /// string.
    #[serde(default)]
    pub login_timeout_seconds: Option<u32>,
    /// Table name to read from (source) or write to (sink).
    pub table: String,
    /// Column used to partition reads and upsert on write — should be
    /// indexed on the source database.
    pub primary_key: String,
    /// Explicit target schema — generic ODBC introspection varies too much
    /// across legacy drivers to infer types reliably, so the node config
    /// must say what to project each column to.
    pub fields: Vec<OdbcFieldSpec>,
    /// How many rows to fold into a single `RecordBatch` while scanning.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Timeout in seconds for each batch write to fail if the ODBC worker
    /// thread doesn't respond in time (a stalled driver call would
    /// otherwise block the pipeline indefinitely — C15). Only unblocks the
    /// async side: the blocking ODBC call itself, and the OS thread running
    /// it, keeps running regardless (no cross-thread cancellation for raw
    /// ODBC handles).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl OdbcConnectorConfig {
    /// Returns the connection string to hand to the ODBC driver manager.
    ///
    /// If a legacy `connection_string` is present it is returned unchanged;
    /// otherwise an ODBC connection string is built from the individual
    /// fields.
    pub fn connection_string(&self) -> String {
        if let Some(cs) = self.connection_string.as_deref().filter(|s| !s.is_empty()) {
            return cs.to_string();
        }

        let mut parts: Vec<String> = Vec::new();
        parts.push(format!("Driver={}", odbc_escape(&self.driver)));
        parts.push(format!("Server={}", odbc_escape(&self.server)));
        if let Some(port) = self.port {
            parts.push(format!("Port={port}"));
        }
        if let Some(database) = &self.database {
            parts.push(format!("Database={}", odbc_escape(database)));
        }
        parts.push(format!("UID={}", odbc_escape(&self.username)));
        parts.push(format!("PWD={}", odbc_escape(&self.password)));
        if let Some(encrypt) = self.encrypt {
            parts.push(format!("Encrypt={}", if encrypt { "Yes" } else { "No" }));
        }
        if let Some(trust) = self.trust_server_certificate {
            parts.push(format!(
                "TrustServerCertificate={}",
                if trust { "Yes" } else { "No" }
            ));
        }
        if let Some(timeout) = self.login_timeout_seconds {
            parts.push(format!("LoginTimeout={timeout}"));
        }

        parts.join(";")
    }
}

/// Escapes a value that will be placed on the right-hand side of an ODBC
/// `Key=Value` pair. Curly braces and semicolons must be doubled.
fn odbc_escape(s: &str) -> String {
    s.replace(';', ";;").replace('}', "}}")
}

fn default_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct OdbcFieldSpec {
    /// Column name as it appears in the source/sink table.
    pub name: String,
    /// Arrow type this column's value gets converted to.
    pub data_type: OdbcDataType,
    /// Whether a NULL value for this column is allowed.
    #[serde(default)]
    pub nullable: bool,
}

/// Arrow type a column is projected onto — one of these four primitives,
/// matched by name in the node config's `data_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OdbcDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

fn default_batch_size() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_returns_legacy_when_present() {
        let cfg = OdbcConnectorConfig {
            connection_string: Some("DRIVER={Legacy};SERVER=old".to_string()),
            driver: "ignored".to_string(),
            server: "ignored".to_string(),
            port: None,
            database: None,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            encrypt: None,
            trust_server_certificate: None,
            login_timeout_seconds: None,
            table: "events".to_string(),
            primary_key: "id".to_string(),
            fields: vec![],
            batch_size: 1000,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_string(), "DRIVER={Legacy};SERVER=old");
    }

    #[test]
    fn connection_string_builds_from_fields() {
        let cfg = OdbcConnectorConfig {
            connection_string: None,
            driver: "{PostgreSQL Unicode}".to_string(),
            server: "db.example.com".to_string(),
            port: Some(5432),
            database: Some("analytics".to_string()),
            username: "nexus".to_string(),
            password: "p;w}d".to_string(),
            encrypt: Some(true),
            trust_server_certificate: Some(false),
            login_timeout_seconds: Some(10),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            fields: vec![],
            batch_size: 1000,
            timeout_seconds: 30,
        };
        let cs = cfg.connection_string();
        assert!(cs.contains("Driver={PostgreSQL Unicode}"));
        assert!(cs.contains("Server=db.example.com"));
        assert!(cs.contains("Port=5432"));
        assert!(cs.contains("Database=analytics"));
        assert!(cs.contains("UID=nexus"));
        assert!(cs.contains("PWD=p;;w}}d"));
        assert!(cs.contains("Encrypt=Yes"));
        assert!(cs.contains("TrustServerCertificate=No"));
        assert!(cs.contains("LoginTimeout=10"));
    }
}
