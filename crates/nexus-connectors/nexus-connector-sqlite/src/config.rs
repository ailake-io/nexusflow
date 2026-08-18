use serde::Deserialize;

/// Static connector config resolved at node-configuration time. `uri` is a
/// file path or `:memory:` — see ARCHITECTURE.md §3.
///
/// You can either provide a complete `uri` (legacy form, takes precedence) or
/// fill `file_path`, which defaults to `:memory:` for an ephemeral database.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct SqliteConnectorConfig {
    /// Full SQLite connection URI.
    ///
    /// When provided, this value is used exactly as-is and `file_path` is
    /// ignored. Keeps backward compatibility with older pipelines that store
    /// a complete path or `:memory:` string.
    #[serde(default)]
    pub uri: Option<String>,
    /// File path to the `.db` file, or `:memory:` for an ephemeral database
    /// that only exists for this process's lifetime.
    ///
    /// Use an absolute path if the SQLite file lives outside the nexus-server
    /// working directory. Ignored when `uri` is set.
    #[serde(default = "default_file_path")]
    pub file_path: String,
    /// Table name to read from (source) or write to (sink) — created
    /// automatically on the sink side if it doesn't exist yet.
    pub table: String,
    /// Column used to upsert on write — should be an indexed, unique column
    /// (integer or text primary key).
    pub primary_key: String,
    /// Timeout in seconds for each ADBC call (connect, query, insert) — a
    /// concurrent writer holding the SQLite file lock can otherwise stall a
    /// call indefinitely (though the underlying blocking thread keeps
    /// running regardless — no cancellation for in-flight ADBC calls) (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl SqliteConnectorConfig {
    /// Returns the connection URI to hand to the ADBC driver.
    ///
    /// If a legacy `uri` is present it is returned unchanged; otherwise
    /// `file_path` is returned.
    pub fn connection_url(&self) -> String {
        self.uri
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| self.file_path.clone())
    }
}

fn default_file_path() -> String {
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
        let cfg = SqliteConnectorConfig {
            uri: Some("/legacy/db.sqlite".to_string()),
            file_path: "/ignored.sqlite".to_string(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), "/legacy/db.sqlite");
    }

    #[test]
    fn connection_url_falls_back_to_file_path() {
        let cfg = SqliteConnectorConfig {
            uri: None,
            file_path: "/data/events.sqlite".to_string(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), "/data/events.sqlite");
    }

    #[test]
    fn default_file_path_is_memory() {
        let cfg = SqliteConnectorConfig {
            uri: None,
            file_path: default_file_path(),
            table: "events".to_string(),
            primary_key: "id".to_string(),
            timeout_seconds: 30,
        };
        assert_eq!(cfg.connection_url(), ":memory:");
    }
}
