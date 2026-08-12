use serde::Deserialize;

/// SSL/TLS mode for the PostgreSQL connection.
///
/// Maps to the `sslmode` parameter in PostgreSQL connection strings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PostgresSslMode {
    /// Only try a non-SSL connection.
    Disable,
    /// First try a non-SSL connection; if that fails, try an SSL connection.
    Allow,
    /// First try an SSL connection; if that fails, try a non-SSL connection.
    #[default]
    Prefer,
    /// Only try an SSL connection. If a root CA file is present, verify the
    /// server certificate in the same way as if `verify-ca` was specified.
    Require,
    /// Only try an SSL connection, and verify that the server certificate is
    /// issued by a trusted certificate authority (CA).
    VerifyCa,
    /// Only try an SSL connection. Verify that the server certificate is
    /// issued by a trusted CA and that the requested server host name matches
    /// that in the certificate.
    VerifyFull,
}

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill the individual connection fields (`host`, `port`, `username`, ...),
/// which are assembled into a PostgreSQL connection string when `uri` is empty.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresConnectorConfig {
    /// Full `postgresql://user:pass@host:port/db` connection URI.
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. Keeps backward compatibility with older
    /// pipelines that already store a complete connection string.
    #[serde(default)]
    pub uri: Option<String>,
    /// Database server host name or IP address (e.g. `localhost` or `db.example.com`).
    #[serde(default = "default_host")]
    pub host: String,
    /// TCP port the PostgreSQL server is listening on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// User name to authenticate with.
    pub username: String,
    /// Password for the provided user name.
    #[serde(default)]
    pub password: String,
    /// Database name to connect to.
    pub database: String,
    /// Schema / `search_path` to use for the target table.
    ///
    /// Defaults to the user's default search path (usually `public`). Only
    /// used when building the connection string from individual fields.
    #[serde(default)]
    pub schema: Option<String>,
    /// SSL/TLS negotiation mode for this connection.
    ///
    /// Defaults to `prefer`: try SSL first, fall back to plaintext if the
    /// server does not support it.
    #[serde(default)]
    pub ssl_mode: PostgresSslMode,
    /// Table name to read from (source) or write to (sink) — no schema
    /// prefix needed unless the table isn't in the connection's default
    /// `search_path`.
    pub table: String,
    /// Column used to partition reads by range and to upsert on write —
    /// must be an indexed, orderable column (integer/UUID/timestamp).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — the
    /// driver is a blocking FFI call run via `spawn_blocking`, so a stalled
    /// connection would otherwise block that call forever (though the
    /// underlying blocking thread keeps running regardless — no
    /// cancellation for in-flight libpq/ADBC calls) (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl PostgresConnectorConfig {
    /// Returns the connection string to hand to the ADBC driver.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise a
    /// `postgresql://` URI is built from the individual fields.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = &self.uri {
            return uri.clone();
        }

        let password_part = percent_encode(&self.password);
        let mut uri = format!(
            "postgresql://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            password_part,
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        );

        let mut params: Vec<String> = Vec::new();
        if let Some(schema) = &self.schema {
            params.push(format!(
                "options=-csearch_path%3D{}",
                percent_encode(schema)
            ));
        }
        let ssl_mode = match self.ssl_mode {
            PostgresSslMode::Disable => "disable",
            PostgresSslMode::Allow => "allow",
            PostgresSslMode::Prefer => "prefer",
            PostgresSslMode::Require => "require",
            PostgresSslMode::VerifyCa => "verify-ca",
            PostgresSslMode::VerifyFull => "verify-full",
        };
        if ssl_mode != "prefer" {
            params.push(format!("sslmode={ssl_mode}"));
        }

        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    5432
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Minimal percent-encoding helper for connection-string components.
///
/// Only encodes characters that are unsafe inside the userinfo/path segments
/// of a `postgresql://` URI.
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

/// Config for the native logical-replication CDC source (`"postgres-cdc"`) —
/// a separate connector name from `"postgres"` rather than a mode flag, so
/// the batch connector's config/behavior never changes. See
/// `ARCHITECTURE.md §7`.
///
/// Like [`PostgresConnectorConfig`], you may provide a complete `uri` (legacy
/// form, takes precedence) or the individual connection fields, from which a
/// connection string is assembled. The replication parameter is appended
/// automatically when connecting.
#[cfg(feature = "cdc")]
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresCdcConfig {
    /// Full `postgres://user:pass@host:port/db` connection URI.
    ///
    /// When provided, this value is used exactly as-is and all other
    /// connection fields are ignored. The `?replication=database` parameter
    /// is appended automatically at connect time, so you do not need to
    /// include it here.
    #[serde(default)]
    pub uri: Option<String>,
    /// Database server host name or IP address.
    #[serde(default = "default_host")]
    pub host: String,
    /// TCP port the PostgreSQL server is listening on.
    #[serde(default = "default_port")]
    pub port: u16,
    /// User name to authenticate with.
    pub username: String,
    /// Password for the provided user name.
    #[serde(default)]
    pub password: String,
    /// Database name to connect to.
    pub database: String,
    /// Schema used to qualify the target table when matching replication
    /// events. Defaults to the user's default search path (usually `public`).
    #[serde(default)]
    pub schema: Option<String>,
    /// SSL/TLS negotiation mode for this connection.
    #[serde(default)]
    pub ssl_mode: PostgresSslMode,
    /// Table this connector reads changes for. The publication (see
    /// `publication_name`) must already cover this table — run
    /// `CREATE PUBLICATION <publication_name> FOR TABLE <table>` by hand
    /// once before starting this connector; it isn't created automatically.
    pub table: String,
    /// Replication publication name that covers `table`.
    pub publication_name: String,
    /// Replication slot name — created automatically on first connect if it
    /// doesn't exist yet. Reconnecting later with the same name resumes
    /// from where this connector last left off: Postgres tracks the
    /// confirmed position server-side, so there's no separate LSN/offset to
    /// persist on the nexus-server side for this to work.
    pub slot_name: String,
    /// Target schema for each change event's row — same 4-primitive-type
    /// ceiling as every other bridging connector (Kafka/MongoDB); Postgres
    /// column types beyond these aren't supported yet.
    pub fields: Vec<PostgresCdcFieldSpec>,
    /// Timeout in seconds for each replication connection call (connect,
    /// read slot, drop slot) — a stalled connection would otherwise block the
    /// pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

#[cfg(feature = "cdc")]
impl PostgresCdcConfig {
    /// Returns the connection string to hand to the replication driver.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise a
    /// `postgresql://` URI is built from the individual fields.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = &self.uri {
            return uri.clone();
        }

        let password_part = percent_encode(&self.password);
        let mut uri = format!(
            "postgresql://{}:{}@{}:{}/{}",
            percent_encode(&self.username),
            password_part,
            percent_encode(&self.host),
            self.port,
            percent_encode(&self.database)
        );

        let mut params: Vec<String> = Vec::new();
        if let Some(schema) = &self.schema {
            params.push(format!(
                "options=-csearch_path%3D{}",
                percent_encode(schema)
            ));
        }
        let ssl_mode = match self.ssl_mode {
            PostgresSslMode::Disable => "disable",
            PostgresSslMode::Allow => "allow",
            PostgresSslMode::Prefer => "prefer",
            PostgresSslMode::Require => "require",
            PostgresSslMode::VerifyCa => "verify-ca",
            PostgresSslMode::VerifyFull => "verify-full",
        };
        if ssl_mode != "prefer" {
            params.push(format!("sslmode={ssl_mode}"));
        }

        if !params.is_empty() {
            uri.push('?');
            uri.push_str(&params.join("&"));
        }

        uri
    }
}

#[cfg(feature = "cdc")]
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PostgresCdcFieldSpec {
    /// Column name as emitted in the CDC event.
    pub name: String,
    /// Arrow data type this column's value is coerced to.
    pub data_type: PostgresCdcDataType,
    /// Whether the column may contain NULL values.
    #[serde(default)]
    pub nullable: bool,
}

#[cfg(feature = "cdc")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PostgresCdcDataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_returns_uri_when_present() {
        let cfg = PostgresConnectorConfig {
            uri: Some("postgresql://legacy".to_string()),
            host: "ignored".to_string(),
            port: 9999,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            database: "ignored".to_string(),
            schema: None,
            ssl_mode: PostgresSslMode::Disable,
            table: "t".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_string(), "postgresql://legacy");
    }

    #[test]
    fn connection_string_builds_from_fields() {
        let cfg = PostgresConnectorConfig {
            uri: None,
            host: "db.example.com".to_string(),
            port: 5433,
            username: "nexus".to_string(),
            password: "s3cr@t".to_string(),
            database: "analytics".to_string(),
            schema: Some("staging".to_string()),
            ssl_mode: PostgresSslMode::Require,
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        let cs = cfg.connection_string();
        assert!(cs.starts_with("postgresql://nexus:s3cr%40t@db.example.com:5433/analytics"));
        assert!(cs.contains("options=-csearch_path%3Dstaging"));
        assert!(cs.contains("sslmode=require"));
    }

    #[test]
    fn connection_string_omits_default_sslmode() {
        let cfg = PostgresConnectorConfig {
            uri: None,
            host: "localhost".to_string(),
            port: 5432,
            username: "nexus".to_string(),
            password: "nexus".to_string(),
            database: "nexus".to_string(),
            schema: None,
            ssl_mode: PostgresSslMode::Prefer,
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        let cs = cfg.connection_string();
        assert_eq!(cs, "postgresql://nexus:nexus@localhost:5432/nexus");
    }

    #[cfg(feature = "cdc")]
    #[test]
    fn cdc_connection_string_returns_uri_when_present() {
        let cfg = PostgresCdcConfig {
            uri: Some("postgresql://legacy".to_string()),
            host: "ignored".to_string(),
            port: 9999,
            username: "ignored".to_string(),
            password: "ignored".to_string(),
            database: "ignored".to_string(),
            schema: None,
            ssl_mode: PostgresSslMode::Disable,
            table: "t".to_string(),
            publication_name: "pub".to_string(),
            slot_name: "slot".to_string(),
            fields: vec![],
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_string(), "postgresql://legacy");
    }
}
