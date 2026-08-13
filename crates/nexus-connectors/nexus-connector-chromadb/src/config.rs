use serde::Deserialize;

/// Configuration for the ChromaDB vector sink.
///
/// The connector talks to ChromaDB's v2 REST API. The collection must already
/// exist; this sink only writes rows. See ARCHITECTURE.md §4.3/§8,
/// IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ChromaConnectorConfig {
    /// ChromaDB server address.
    ///
    /// This field accepts either a complete base URL such as
    /// `"http://localhost:8000"` or just a hostname such as `"localhost"`.
    /// When a bare hostname is provided, [`ChromaConnectorConfig::base_url`]
    /// combines it with [`Self::port`] to build the final URL.
    ///
    /// For backwards compatibility, this field takes priority over the
    /// separated `host`/`port` pair: if it looks like a URL, it is used
    /// verbatim.
    pub host: String,

    /// TCP port of the ChromaDB HTTP server.
    ///
    /// Only used when [`Self::host`] is a bare hostname. Defaults to `8000`.
    #[serde(default)]
    pub port: Option<u16>,

    /// Optional API key for authenticated ChromaDB instances.
    ///
    /// When set, every request is sent with a `Authorization: Bearer <key>`
    /// header.
    #[serde(default)]
    pub api_key: Option<String>,

    /// Tenant name.
    ///
    /// Leave unset to use ChromaDB's default tenant (`default_tenant`).
    #[serde(default = "default_tenant")]
    pub tenant: String,

    /// Database name within the tenant.
    ///
    /// Leave unset to use ChromaDB's default database (`default_database`).
    #[serde(default = "default_database")]
    pub database: String,

    /// Name of an existing collection.
    ///
    /// The collection must already be created on the ChromaDB server; this
    /// sink only writes rows.
    pub collection: String,

    /// Column used as the Chroma document ID.
    pub primary_key: String,

    /// Name of the `FixedSizeList<Float32>` column the embedding is written to.
    pub embedding_column: String,

    /// Vector size.
    ///
    /// Must match the collection's configured dimension.
    pub dimension: usize,

    /// Per-request timeout in seconds.
    ///
    /// `reqwest::Client` has no timeout by default, so a stalled connection to
    /// ChromaDB would otherwise block the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_tenant() -> String {
    "default_tenant".to_string()
}

fn default_database() -> String {
    "default_database".to_string()
}

fn default_timeout_seconds() -> u64 {
    30
}

impl ChromaConnectorConfig {
    /// Returns the base URL used to reach the ChromaDB REST API.
    ///
    /// If [`Self::host`] is already a complete URL (starts with `http://` or
    /// `https://`), it is returned as-is. Otherwise, the host is combined with
    /// [`Self::port`], defaulting to `8000` when no port is provided.
    pub fn base_url(&self) -> String {
        let trimmed = self.host.trim_end_matches('/');
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_string()
        } else {
            let port = self.port.unwrap_or(8000);
            format!("http://{}:{}", trimmed, port)
        }
    }

    /// Returns the tenant name.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Returns the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the collection name.
    pub fn collection_name(&self) -> &str {
        &self.collection
    }

    /// Returns an `Authorization` header value when [`Self::api_key`] is set.
    pub fn authorization_header(&self) -> Option<String> {
        self.api_key.as_ref().map(|key| format!("Bearer {key}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_uses_complete_host_verbatim() {
        let cfg = ChromaConnectorConfig {
            host: "http://localhost:8000".to_string(),
            port: None,
            api_key: None,
            tenant: default_tenant(),
            database: default_database(),
            collection: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(cfg.base_url(), "http://localhost:8000");
    }

    #[test]
    fn base_url_builds_from_host_and_port() {
        let cfg = ChromaConnectorConfig {
            host: "chromadb.internal".to_string(),
            port: Some(9000),
            api_key: None,
            tenant: default_tenant(),
            database: default_database(),
            collection: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(cfg.base_url(), "http://chromadb.internal:9000");
    }

    #[test]
    fn base_url_defaults_port_to_8000() {
        let cfg = ChromaConnectorConfig {
            host: "localhost".to_string(),
            port: None,
            api_key: None,
            tenant: default_tenant(),
            database: default_database(),
            collection: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(cfg.base_url(), "http://localhost:8000");
    }

    #[test]
    fn base_url_strips_trailing_slash_from_complete_host() {
        let cfg = ChromaConnectorConfig {
            host: "https://chroma.example.com/".to_string(),
            port: Some(1234),
            api_key: None,
            tenant: default_tenant(),
            database: default_database(),
            collection: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(cfg.base_url(), "https://chroma.example.com");
    }

    #[test]
    fn authorization_header_returns_bearer_token() {
        let cfg = ChromaConnectorConfig {
            host: "localhost".to_string(),
            port: None,
            api_key: Some("secret-key".to_string()),
            tenant: default_tenant(),
            database: default_database(),
            collection: "docs".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(
            cfg.authorization_header(),
            Some("Bearer secret-key".to_string())
        );
    }

    #[test]
    fn accessors_return_configured_values() {
        let cfg = ChromaConnectorConfig {
            host: "http://localhost:8000".to_string(),
            port: None,
            api_key: None,
            tenant: "my_tenant".to_string(),
            database: "my_database".to_string(),
            collection: "my_collection".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: default_timeout_seconds(),
        };
        assert_eq!(cfg.tenant(), "my_tenant");
        assert_eq!(cfg.database(), "my_database");
        assert_eq!(cfg.collection_name(), "my_collection");
    }
}
