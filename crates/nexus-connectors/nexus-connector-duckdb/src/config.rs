use serde::Deserialize;

/// Static connector config resolved at node-configuration time. `path` is a
/// file path or `:memory:` — same shape as `nexus-connector-sqlite`'s config
/// (DuckDB is also embedded, no host/port/user/password). See
/// ARCHITECTURE.md §3.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill `path`, which defaults to `:memory:` for an ephemeral database.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct DuckdbConnectorConfig {
    /// Full DuckDB connection URI.
    ///
    /// When provided, this value is used exactly as-is and `path` is
    /// ignored. Keeps backward compatibility with older pipelines that store
    /// a complete path or `:memory:` string.
    #[serde(default)]
    pub uri: Option<String>,
    /// File path to the `.duckdb` file, or `:memory:` for an ephemeral
    /// database that only exists for this process's lifetime.
    ///
    /// Use an absolute path if the DuckDB file lives outside the
    /// nexus-server working directory. Ignored when `uri` is set.
    #[serde(default = "default_path")]
    pub path: String,
    /// Table name to read from (source) or write to (sink) — created
    /// automatically on the sink side if it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write — should be an indexed, unique column
    /// (integer or text primary key).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — a
    /// concurrent writer holding the DuckDB file lock can otherwise stall a
    /// call indefinitely (though the underlying blocking thread keeps
    /// running regardless — no cancellation for in-flight ADBC calls).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl DuckdbConnectorConfig {
    /// Returns the connection URI to hand to the ADBC driver.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise
    /// `path` is returned.
    pub fn connection_url(&self) -> String {
        self.uri
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| self.path.clone())
    }
}

fn default_path() -> String {
    ":memory:".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_url_returns_uri_when_present() {
        let cfg = DuckdbConnectorConfig {
            uri: Some("/legacy/db.duckdb".to_string()),
            path: "/ignored.duckdb".to_string(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), "/legacy/db.duckdb");
    }

    #[test]
    fn connection_url_falls_back_to_path() {
        let cfg = DuckdbConnectorConfig {
            uri: None,
            path: "/data/events.duckdb".to_string(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), "/data/events.duckdb");
    }

    #[test]
    fn default_path_is_memory() {
        let cfg = DuckdbConnectorConfig {
            uri: None,
            path: default_path(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), ":memory:");
    }
}
