use serde::Deserialize;

/// AI Lakehouse sink #5 (ROADMAP.md Fase 5 order). Pinecone has no
/// self-hosted/Docker option — this connector talks to the real managed
/// service's data-plane REST API, so it's only mockable via `wiremock`
/// (no real end-to-end integration test, unlike the other 5 vector sinks —
/// user decision 2026-07-31). See ARCHITECTURE.md §4.3/§8,
/// IMPLEMENTATION_PLAN.md Marco 5.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct PineconeConnectorConfig {
    /// Index-specific data-plane host, e.g.
    /// `https://my-index-xxxx.svc.us-east1-aws.pinecone.io` (from Pinecone's
    /// `describe_index` control-plane response — not built from `index` +
    /// `environment` here, that addressing scheme is deprecated).
    ///
    /// Kept for backward compatibility with existing canvas nodes. For new
    /// configurations prefer `index_name` plus `grpc_url`/`port`.
    #[serde(default)]
    pub host: String,
    /// Pinecone API key with write access to this index.
    pub api_key: String,
    /// Column used as the Pinecone vector ID.
    pub primary_key: String,
    /// Name of the `FixedSizeList<Float32>` column the embedding is
    /// written to.
    pub embedding_column: String,
    /// Vector size — must match the index's configured dimension.
    pub dimension: usize,
    /// Pinecone namespace to write into within the index — omit to use
    /// the default (unnamed) namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Per-request timeout in seconds — `reqwest::Client` has no timeout by
    /// default, so a stalled connection to Pinecone would otherwise block
    /// the pipeline indefinitely (C15).
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// Optional port to use when `host` is provided as a bare hostname
    /// instead of a full URL. Defaults to 443 for HTTPS data-plane traffic.
    ///
    /// Example: with `host = "my-index.pinecone.io"` and `port = 443`, the
    /// connector targets `https://my-index.pinecone.io:443`.
    #[serde(default)]
    pub port: Option<u16>,
    /// Optional gRPC endpoint for this index, e.g.
    /// `https://my-index-xxxx.svc.us-east1-aws.pinecone.io:443`.
    ///
    /// Used as the fallback data-plane URL when the legacy `host` field is
    /// empty, and reserved for future gRPC-based implementations.
    #[serde(default)]
    pub grpc_url: Option<String>,
    /// Name of the Pinecone index.
    ///
    /// Useful for UI validation, logging, and as a human-readable reference
    /// when the actual data-plane `host` is provisioned later by the
    /// Pinecone control plane.
    #[serde(default)]
    pub index_name: Option<String>,
}

fn default_timeout_seconds() -> u64 {
    30
}

impl PineconeConnectorConfig {
    /// Returns the normalized data-plane host/URL to use for REST requests.
    ///
    /// Resolution order (first match wins):
    /// 1. The legacy `host` field if it is non-empty.
    /// 2. The `grpc_url` field if present.
    /// 3. Otherwise falls back to the (possibly empty) `host` value.
    ///
    /// The returned string has any trailing `/` removed so that path
    /// segments such as `/vectors/upsert` can be appended safely.
    pub fn host(&self) -> String {
        let raw = if !self.host.is_empty() {
            self.host.clone()
        } else if let Some(grpc_url) = &self.grpc_url {
            grpc_url.clone()
        } else {
            self.host.clone()
        };
        raw.trim_end_matches('/').to_string()
    }

    /// Alias for [`Self::host`] — returns the base URL for Pinecone data-plane
    /// REST calls (e.g. `https://my-index-xxxx.svc.us-east1-aws.pinecone.io`).
    pub fn url(&self) -> String {
        self.host()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> PineconeConnectorConfig {
        PineconeConnectorConfig {
            host: "https://my-index.svc.us-east1-aws.pinecone.io".to_string(),
            api_key: "test-key".to_string(),
            primary_key: "id".to_string(),
            embedding_column: "embedding".to_string(),
            dimension: 768,
            namespace: None,
            timeout_seconds: 30,
            port: None,
            grpc_url: None,
            index_name: None,
        }
    }

    #[test]
    fn host_returns_legacy_host_normalized() {
        let cfg = sample_config();
        assert_eq!(
            cfg.host(),
            "https://my-index.svc.us-east1-aws.pinecone.io"
        );
    }

    #[test]
    fn host_trims_trailing_slash() {
        let mut cfg = sample_config();
        cfg.host = "https://my-index.svc.us-east1-aws.pinecone.io/".to_string();
        assert_eq!(
            cfg.host(),
            "https://my-index.svc.us-east1-aws.pinecone.io"
        );
    }

    #[test]
    fn host_falls_back_to_grpc_url_when_legacy_host_is_empty() {
        let mut cfg = sample_config();
        cfg.host = String::new();
        cfg.grpc_url = Some("https://grpc-host.pinecone.io/".to_string());
        assert_eq!(cfg.host(), "https://grpc-host.pinecone.io");
    }

    #[test]
    fn url_is_alias_for_host() {
        let cfg = sample_config();
        assert_eq!(cfg.url(), cfg.host());
    }

    #[test]
    fn legacy_host_takes_priority_over_grpc_url() {
        let mut cfg = sample_config();
        cfg.grpc_url = Some("https://grpc-host.pinecone.io".to_string());
        assert_eq!(
            cfg.host(),
            "https://my-index.svc.us-east1-aws.pinecone.io"
        );
    }
}
