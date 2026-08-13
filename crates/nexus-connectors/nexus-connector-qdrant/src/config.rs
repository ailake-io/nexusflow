use serde::Deserialize;

/// AI Lakehouse sink #2 (ROADMAP.md Fase 5 order). See ARCHITECTURE.md
/// §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct QdrantConnectorConfig {
    /// Full Qdrant gRPC URL.
    ///
    /// Example: `"http://localhost:6334"`. When provided, this value takes
    /// precedence over the separate `host`/`port`/`grpc_url` fields and is
    /// used exactly as-is. Kept for backward compatibility with existing
    /// canvas configurations.
    #[serde(default)]
    pub url: String,

    /// Qdrant server host or IP address.
    ///
    /// Example: `"localhost"` or `"127.0.0.1"`. Used only when `url` is not
    /// set. If the value already contains a scheme such as `http://` or
    /// `https://`, it is used directly; otherwise `http://` is assumed.
    #[serde(default)]
    pub host: String,

    /// Qdrant gRPC port.
    ///
    /// The Qdrant gRPC interface listens on port `6334` by default. This port
    /// is used together with `host` to build the connection URL when `url` is
    /// not provided.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Optional explicit gRPC URL.
    ///
    /// When `url` is empty and `grpc_url` is set, this value is used as the
    /// connection URL. It overrides any `host`/`port` combination and is
    /// useful when the gRPC endpoint differs from the default location (for
    /// example, behind a TLS-terminating proxy).
    #[serde(default)]
    pub grpc_url: String,

    /// API key for authenticated Qdrant clusters.
    ///
    /// Qdrant Cloud and on-premise deployments with authentication enabled
    /// require an API key. Leave empty for unauthenticated local instances.
    /// When set, the key is passed to the Qdrant client on connection.
    #[serde(default)]
    pub api_key: String,

    /// Name of an existing Qdrant collection.
    ///
    /// The collection must already be created (with the right vector size) on
    /// the Qdrant server; this sink only writes points. When provided, it
    /// takes precedence over `collection_name`. Kept for backward
    /// compatibility with existing canvas configurations.
    #[serde(default)]
    pub collection: String,

    /// Name of an existing Qdrant collection.
    ///
    /// Alternative, more explicit field for the collection name. Used only
    /// when `collection` is empty.
    #[serde(default)]
    pub collection_name: String,

    /// Must be an `Int64` column.
    ///
    /// Qdrant point IDs are unsigned integers or UUIDs; arbitrary string keys
    /// are not supported. The values in this column are converted to `u64`
    /// point IDs.
    pub primary_key: String,

    /// Name of the `FixedSizeList<Float32>` column the embedding is written to.
    pub embedding_column: String,

    /// Vector size.
    ///
    /// Must match the embedding column's actual length and the vector
    /// configuration of the target Qdrant collection.
    pub dimension: usize,

    /// Timeout in seconds for each gRPC call to Qdrant.
    ///
    /// The client library exposes no connection/request timeout of its own, so
    /// a stalled connection would otherwise block the pipeline indefinitely
    /// (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_port() -> u16 {
    6334
}

fn default_timeout_seconds() -> u64 {
    30
}

impl QdrantConnectorConfig {
    /// Returns the effective Qdrant gRPC connection URL.
    ///
    /// Resolution order (first non-empty wins):
    /// 1. `url`
    /// 2. `grpc_url`
    /// 3. `host` + `port`
    ///
    /// When building from `host`/`port`, the host is normalized to start with
    /// `http://` if no scheme is present.
    pub fn url(&self) -> String {
        if !self.url.is_empty() {
            return self.url.clone();
        }
        if !self.grpc_url.is_empty() {
            return self.grpc_url.clone();
        }

        let host = self.host.trim();
        let normalized_host = if host.is_empty() {
            "http://localhost".to_string()
        } else if host.starts_with("http://") || host.starts_with("https://") {
            host.to_string()
        } else {
            format!("http://{host}")
        };

        format!("{normalized_host}:{}", self.port)
    }

    /// Returns the effective collection name.
    ///
    /// Resolution order (first non-empty wins):
    /// 1. `collection`
    /// 2. `collection_name`
    pub fn collection_name(&self) -> String {
        if !self.collection.is_empty() {
            self.collection.clone()
        } else {
            self.collection_name.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_url(url: &str) -> QdrantConnectorConfig {
        QdrantConnectorConfig {
            url: url.to_string(),
            host: String::new(),
            port: 0,
            grpc_url: String::new(),
            api_key: String::new(),
            collection: String::new(),
            collection_name: String::new(),
            primary_key: String::new(),
            embedding_column: String::new(),
            dimension: 0,
            timeout_seconds: 30,
        }
    }

    #[test]
    fn url_prefers_legacy_url_field() {
        let cfg = QdrantConnectorConfig {
            url: "http://legacy:6334".to_string(),
            host: "host".to_string(),
            port: 1234,
            grpc_url: "http://grpc:6334".to_string(),
            ..config_with_url("")
        };
        assert_eq!(cfg.url(), "http://legacy:6334");
    }

    #[test]
    fn url_falls_back_to_grpc_url() {
        let cfg = QdrantConnectorConfig {
            url: String::new(),
            host: "host".to_string(),
            port: 1234,
            grpc_url: "http://grpc:6334".to_string(),
            ..config_with_url("")
        };
        assert_eq!(cfg.url(), "http://grpc:6334");
    }

    #[test]
    fn url_builds_from_host_and_port() {
        let cfg = QdrantConnectorConfig {
            url: String::new(),
            host: "localhost".to_string(),
            port: 6334,
            grpc_url: String::new(),
            ..config_with_url("")
        };
        assert_eq!(cfg.url(), "http://localhost:6334");
    }

    #[test]
    fn url_builds_from_host_with_scheme() {
        let cfg = QdrantConnectorConfig {
            url: String::new(),
            host: "https://qdrant.example.com".to_string(),
            port: 6334,
            grpc_url: String::new(),
            ..config_with_url("")
        };
        assert_eq!(cfg.url(), "https://qdrant.example.com:6334");
    }

    #[test]
    fn url_uses_default_port_when_host_is_empty() {
        let cfg = QdrantConnectorConfig {
            url: String::new(),
            host: String::new(),
            port: 6334,
            grpc_url: String::new(),
            ..config_with_url("")
        };
        assert_eq!(cfg.url(), "http://localhost:6334");
    }

    #[test]
    fn collection_name_prefers_legacy_collection_field() {
        let cfg = QdrantConnectorConfig {
            collection: "legacy_collection".to_string(),
            collection_name: "new_collection".to_string(),
            ..config_with_url("")
        };
        assert_eq!(cfg.collection_name(), "legacy_collection");
    }

    #[test]
    fn collection_name_falls_back_to_collection_name_field() {
        let cfg = QdrantConnectorConfig {
            collection: String::new(),
            collection_name: "new_collection".to_string(),
            ..config_with_url("")
        };
        assert_eq!(cfg.collection_name(), "new_collection");
    }
}
