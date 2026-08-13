use serde::Deserialize;

/// AI Lakehouse sink #4 (ROADMAP.md Fase 5 order — more complex to operate
/// than pgvector/qdrant/lancedb). The collection must already exist (created
/// externally with the right schema) — the sink only writes. See
/// ARCHITECTURE.md §4.3/§8, IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MilvusConnectorConfig {
    /// **Legacy field.** Full Milvus server URL, e.g. `"http://localhost:19530"`.
    ///
    /// If this field is provided it takes precedence over the separate
    /// `host`/`port` fields, so existing pipelines keep working unchanged.
    /// For new canvas sources/sinks prefer `host` and `port`.
    #[serde(default)]
    pub url: Option<String>,
    /// Milvus server hostname or IP address, e.g. `"localhost"` or
    /// `"milvus.example.com"`. Used only when the legacy `url` field is not
    /// set.
    #[serde(default)]
    pub host: Option<String>,
    /// Milvus server port. Defaults to `19530` when `host` is used and `port`
    /// is omitted.
    #[serde(default)]
    pub port: Option<u16>,
    /// API key or token for authenticated Milvus instances (e.g. Zilliz Cloud).
    ///
    /// This field is kept in the config so the frontend can collect it; the
    /// underlying `milvus-sdk-rust` 0.1.0 client currently accepts only a
    /// username/password pair, so the sink does not wire it automatically yet.
    #[serde(default)]
    pub api_key: Option<String>,
    /// **Legacy field.** Name of an existing collection — must already be
    /// created (with schema and index) on the Milvus server; this sink only
    /// writes rows.
    ///
    /// If this field is provided it takes precedence over `collection_name`,
    /// so existing pipelines keep working unchanged. For new canvas
    /// sources/sinks prefer `collection_name`.
    #[serde(default)]
    pub collection: Option<String>,
    /// Name of an existing collection — must already be created (with schema
    /// and index) on the Milvus server; this sink only writes rows. Used only
    /// when the legacy `collection` field is not set.
    #[serde(default)]
    pub collection_name: Option<String>,
    /// Must be an `Int64` column — matches the primary key type this
    /// connector supports (Milvus also allows `VarChar` primary keys, not
    /// implemented here).
    pub primary_key: String,
    /// Name of the vector field in the collection the embedding is
    /// written to.
    pub embedding_column: String,
    /// Must match the vector field's declared dimension in the collection
    /// schema.
    pub dimension: usize,
    /// Timeout in seconds for each call to Milvus (connect, insert, delete,
    /// collection lookup) — the SDK exposes no timeout of its own, so a
    /// stalled connection would otherwise block the pipeline indefinitely
    /// (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl MilvusConnectorConfig {
    /// Returns the effective Milvus server URL.
    ///
    /// If the legacy `url` field is set, it is returned verbatim. Otherwise the
    /// URL is built from `host` and `port`, defaulting the port to `19530` and
    /// the host to `"localhost"` when not provided.
    pub fn url(&self) -> String {
        if let Some(url) = &self.url {
            return url.clone();
        }
        let host = self.host.as_deref().unwrap_or("localhost");
        let port = self.port.unwrap_or(19530);
        format!("http://{host}:{port}")
    }

    /// Returns the effective collection name.
    ///
    /// If the legacy `collection` field is set, it is returned verbatim.
    /// Otherwise the value of `collection_name` is used, falling back to an
    /// empty string if neither is set.
    pub fn collection_name(&self) -> String {
        self.collection
            .clone()
            .or_else(|| self.collection_name.clone())
            .unwrap_or_default()
    }
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_url_takes_precedence() {
        let cfg = MilvusConnectorConfig {
            url: Some("http://legacy:19530".to_string()),
            host: Some("newhost".to_string()),
            port: Some(9999),
            api_key: None,
            collection: Some("legacy_docs".to_string()),
            collection_name: Some("new_docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.url(), "http://legacy:19530");
        assert_eq!(cfg.collection_name(), "legacy_docs");
    }

    #[test]
    fn builds_url_and_collection_from_separate_fields() {
        let cfg = MilvusConnectorConfig {
            url: None,
            host: Some("milvus.example.com".to_string()),
            port: Some(9999),
            api_key: None,
            collection: None,
            collection_name: Some("new_docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.url(), "http://milvus.example.com:9999");
        assert_eq!(cfg.collection_name(), "new_docs");
    }

    #[test]
    fn defaults_host_and_port() {
        let cfg = MilvusConnectorConfig {
            url: None,
            host: None,
            port: None,
            api_key: None,
            collection: None,
            collection_name: Some("docs".to_string()),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 384,
            timeout_seconds: 30,
        };
        assert_eq!(cfg.url(), "http://localhost:19530");
        assert_eq!(cfg.collection_name(), "docs");
    }

    #[test]
    fn deserializes_from_legacy_fields() {
        let json = serde_json::json!({
            "url": "http://milvus:19530",
            "collection": "docs",
            "primary_key": "id",
            "embedding_column": "embedding",
            "dimension": 384,
        });
        let cfg: MilvusConnectorConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.url(), "http://milvus:19530");
        assert_eq!(cfg.collection_name(), "docs");
    }

    #[test]
    fn deserializes_from_separate_fields() {
        let json = serde_json::json!({
            "host": "milvus.example.com",
            "port": 9999,
            "collection_name": "docs",
            "primary_key": "id",
            "embedding_column": "embedding",
            "dimension": 384,
        });
        let cfg: MilvusConnectorConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.url(), "http://milvus.example.com:9999");
        assert_eq!(cfg.collection_name(), "docs");
    }
}
