use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not runtime).
/// Deserialized from the DAG node's raw `config` JSON — see ARCHITECTURE.md §3.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill the individual connection fields (`host`, `port`, `username`, ...),
/// which are assembled into a ClickHouse HTTP interface URI when `uri` is
/// empty.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ClickHouseConnectorConfig {
    /// Full `http://user:pass@host:port/` connection URI. When provided,
    /// this value is used exactly as-is and all other connection fields are
    /// ignored.
    #[serde(default)]
    pub uri: Option<String>,
    /// ClickHouse server host name or IP address.
    #[serde(default = "default_host")]
    pub host: String,
    /// HTTP port the ClickHouse server is listening on (the ADBC driver
    /// speaks ClickHouse's HTTP interface, not the native TCP protocol).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Database name.
    #[serde(default = "default_database")]
    pub database: String,
    /// User name to authenticate with.
    #[serde(default)]
    pub username: String,
    /// Password for the provided user name.
    #[serde(default)]
    pub password: String,
    /// Table name to read from (source) or write to (sink).
    pub table: String,
    /// Column used to partition reads by range for parallelism — any
    /// orderable column, not necessarily unique (ClickHouse doesn't enforce
    /// a primary key the way Postgres does; this is typically a column from
    /// the table's `ORDER BY`). `None` reads the whole table with no
    /// `WHERE` clause and no partitioning.
    #[serde(default)]
    pub partition_column: Option<String>,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — the
    /// driver is a blocking FFI call run via `spawn_blocking`, so a stalled
    /// connection would otherwise block that call forever.
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl ClickHouseConnectorConfig {
    /// Returns the connection URI to hand to the ADBC driver.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise an
    /// `http://` URI is built from the individual fields. Credentials go in
    /// the URI's userinfo component — ClickHouse's HTTP interface supports
    /// this natively (`http://user:password@host:8123/`), same mechanism
    /// documented at https://clickhouse.com/docs/interfaces/http.
    pub fn connection_string(&self) -> String {
        if let Some(uri) = self.uri.as_deref().filter(|s| !s.is_empty()) {
            return uri.to_string();
        }

        if self.username.is_empty() {
            format!(
                "http://{}:{}/{}",
                percent_encode(&self.host),
                self.port,
                percent_encode(&self.database)
            )
        } else {
            format!(
                "http://{}:{}@{}:{}/{}",
                percent_encode(&self.username),
                percent_encode(&self.password),
                percent_encode(&self.host),
                self.port,
                percent_encode(&self.database)
            )
        }
    }
}

fn default_host() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    8123
}

fn default_database() -> String {
    "default".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

/// Minimal percent-encoding helper for URI components — same unsafe-char set
/// as `nexus-connector-postgres`'s equivalent helper.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> ClickHouseConnectorConfig {
        ClickHouseConnectorConfig {
            uri: None,
            host: "localhost".to_string(),
            port: 8123,
            database: "default".to_string(),
            username: String::new(),
            password: String::new(),
            table: "events".to_string(),
            partition_column: None,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn connection_string_returns_uri_when_present() {
        let mut cfg = base_cfg();
        cfg.uri = Some("http://legacy:8123/".to_string());
        cfg.host = "ignored".to_string();
        assert_eq!(cfg.connection_string(), "http://legacy:8123/");
    }

    #[test]
    fn connection_string_falls_back_to_fields_when_uri_is_empty_string() {
        let mut cfg = base_cfg();
        cfg.uri = Some(String::new());
        assert_eq!(cfg.connection_string(), "http://localhost:8123/default");
    }

    #[test]
    fn connection_string_omits_userinfo_when_username_is_empty() {
        let cfg = base_cfg();
        assert_eq!(cfg.connection_string(), "http://localhost:8123/default");
    }

    #[test]
    fn connection_string_includes_userinfo_when_username_is_set() {
        let mut cfg = base_cfg();
        cfg.username = "nexus".to_string();
        cfg.password = "s3cr@t".to_string();
        cfg.host = "ch.example.com".to_string();
        cfg.port = 8443;
        cfg.database = "analytics".to_string();
        assert_eq!(
            cfg.connection_string(),
            "http://nexus:s3cr%40t@ch.example.com:8443/analytics"
        );
    }
}
