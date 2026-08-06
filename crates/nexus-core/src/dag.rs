use crate::error::NexusError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One node in the DAG: which connector to use and its raw config (validated
/// by the connector itself, not here — nexus-core doesn't know connector
/// internals). `name` is only meaningful for sources in a transform pipeline
/// — it's the table name the transform SQL references; defaults to
/// `source{index}`/`sink{index}` when unset. See ARCHITECTURE.md §3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    #[serde(default)]
    pub name: Option<String>,
    pub connector: String,
    #[serde(default)]
    pub config: Value,
}

impl NodeSpec {
    pub fn resolved_name(&self, index: usize, prefix: &str) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| format!("{prefix}{index}"))
    }
}

/// The "leve transformação SQL em memória" node from `CLAUDE.md §4.4`. One
/// query, N named sources as input tables — see `nexus_core::transform`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformSpec {
    pub sql: String,
}

/// Which dbt command to invoke after the raw load succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DbtCommand {
    Run,
    Build,
    Test,
}

/// ELT mode (Marco 10, CLAUDE.md §4.4): once the raw load into `sinks`
/// succeeds, run dbt against that same warehouse — dbt operates via SQL on
/// already-landed tables, not on this pipeline's in-flight Arrow batches,
/// so this is a post-load step, not a DAG transform node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbtConfig {
    /// Path to the dbt project directory (containing `dbt_project.yml`) —
    /// must be reachable from the nexus-server process (self-hosted
    /// deployment, ARCHITECTURE.md §7).
    pub project_dir: String,
    pub command: DbtCommand,
    /// Optional `--select` model selector.
    #[serde(default)]
    pub select: Option<String>,
}

/// Configuration for the optional embedding/chunking stage. Defined in
/// nexus-core so `PipelineSpec` can carry it without adding an nexus-ai
/// dependency to the core crate; nexus-ai consumes this spec at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingSpec {
    /// Column containing the text to embed.
    pub source_column: String,
    /// Name of the `FixedSizeList<Float32>` column to append.
    pub output_column: String,
    /// Embedding width — must match the model's output dimension.
    pub dimension: usize,
    /// Which ONNX model to load.
    pub model: EmbeddingModelSpec,
    /// How to split `source_column` into chunks before embedding.
    pub chunking: ChunkingSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingModelSpec {
    /// Hugging Face repo id, e.g. "sentence-transformers/all-MiniLM-L6-v2".
    pub repo: String,
    /// ONNX model file name inside the repo/revision.
    pub filename: String,
    /// Tokenizer file name inside the repo/revision.
    pub tokenizer_filename: String,
    /// Max token sequence length fed to the model.
    pub max_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "strategy")]
pub enum ChunkingSpec {
    FixedWindow {
        chunk_size: usize,
        #[serde(default)]
        overlap: usize,
    },
    RecursiveCharacter {
        chunk_size: usize,
        #[serde(default)]
        overlap: usize,
        separators: Option<Vec<String>>,
    },
}

/// Two shapes, both valid DAGs (ARCHITECTURE.md §4):
/// - No transform: strictly linear `1 source -> 1 sink`, partitioned
///   execution (Marco 1's model — `PipelineEngine::run`).
/// - With transform: `N sources -> 1 transform -> M sinks` (fan-in/fan-out),
///   unpartitioned — `PipelineEngine::run_transform_pipeline` (Marco 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub pipeline_id: String,
    pub sources: Vec<NodeSpec>,
    #[serde(default)]
    pub transform: Option<TransformSpec>,
    pub sinks: Vec<NodeSpec>,
    /// Optional chunking + embedding stage, applied to every source batch
    /// before the SQL transform (if present) or before the sinks.
    #[serde(default)]
    pub embedding: Option<EmbeddingSpec>,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// ELT mode — dbt run/build/test against the sink warehouse after the
    /// raw load succeeds (Marco 10). `None` (the default) means "no dbt
    /// step", the same as before this field existed.
    #[serde(default)]
    pub dbt: Option<DbtConfig>,
    /// Cron expression controlling automatic runs — see
    /// `schedule::parse_cron_expression` for the accepted formats. `None`
    /// (the default) means no automatic schedule: the pipeline only runs
    /// when explicitly triggered via `POST /pipelines/{id}/run`, same as
    /// before this field existed.
    #[serde(default)]
    pub schedule: Option<String>,
}

fn default_channel_capacity() -> usize {
    100
}

fn default_partitions() -> u32 {
    1
}

impl PipelineSpec {
    pub fn parse(json: &str) -> Result<Self, NexusError> {
        let spec: PipelineSpec =
            serde_json::from_str(json).map_err(|e| NexusError::Serialization(e.to_string()))?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn has_transform(&self) -> bool {
        self.transform.is_some()
    }

    /// Public so callers that skip [`PipelineSpec::parse`] (e.g. an Axum
    /// `Json<PipelineSpec>` extractor, which deserializes directly) can still
    /// run the same validation explicitly.
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.pipeline_id.trim().is_empty() {
            return Err(NexusError::Schema("pipeline_id must not be empty".into()));
        }
        if self.sources.is_empty() {
            return Err(NexusError::Schema("sources must not be empty".into()));
        }
        if self.sinks.is_empty() {
            return Err(NexusError::Schema("sinks must not be empty".into()));
        }
        for (i, node) in self.sources.iter().enumerate() {
            if node.connector.trim().is_empty() {
                return Err(NexusError::Schema(format!(
                    "sources[{i}].connector must not be empty"
                )));
            }
        }
        for (i, node) in self.sinks.iter().enumerate() {
            if node.connector.trim().is_empty() {
                return Err(NexusError::Schema(format!(
                    "sinks[{i}].connector must not be empty"
                )));
            }
        }

        match &self.transform {
            None => {
                if self.sources.len() != 1 || self.sinks.len() != 1 {
                    return Err(NexusError::Schema(
                        "without a transform, the pipeline must be strictly linear: \
                         exactly 1 source and 1 sink (fan-in/fan-out requires a transform)"
                            .into(),
                    ));
                }
            }
            Some(t) => {
                if t.sql.trim().is_empty() {
                    return Err(NexusError::Schema("transform.sql must not be empty".into()));
                }
            }
        }

        if let Some(embedding) = &self.embedding {
            if embedding.source_column.trim().is_empty() {
                return Err(NexusError::Schema(
                    "embedding.source_column must not be empty".into(),
                ));
            }
            if embedding.output_column.trim().is_empty() {
                return Err(NexusError::Schema(
                    "embedding.output_column must not be empty".into(),
                ));
            }
            if embedding.dimension == 0 {
                return Err(NexusError::Schema("embedding.dimension must be > 0".into()));
            }
            if embedding.model.repo.trim().is_empty() {
                return Err(NexusError::Schema(
                    "embedding.model.repo must not be empty".into(),
                ));
            }
            if embedding.model.filename.trim().is_empty() {
                return Err(NexusError::Schema(
                    "embedding.model.filename must not be empty".into(),
                ));
            }
            if embedding.model.tokenizer_filename.trim().is_empty() {
                return Err(NexusError::Schema(
                    "embedding.model.tokenizer_filename must not be empty".into(),
                ));
            }
            if embedding.model.max_length == 0 {
                return Err(NexusError::Schema(
                    "embedding.model.max_length must be > 0".into(),
                ));
            }
            match &embedding.chunking {
                ChunkingSpec::FixedWindow { chunk_size, .. }
                | ChunkingSpec::RecursiveCharacter { chunk_size, .. } => {
                    if *chunk_size == 0 {
                        return Err(NexusError::Schema(
                            "embedding.chunking.chunk_size must be > 0".into(),
                        ));
                    }
                }
            }
        }

        if self.channel_capacity == 0 {
            return Err(NexusError::Schema("channel_capacity must be > 0".into()));
        }
        if self.partitions == 0 {
            return Err(NexusError::Schema("partitions must be > 0".into()));
        }
        if let Some(expr) = &self.schedule {
            crate::schedule::parse_cron_expression(expr)
                .map_err(|e| NexusError::Schema(format!("invalid schedule: {e}")))?;
        }
        Ok(())
    }

    /// Validates that the spec does not attempt to read local files or reach
    /// internal networks (SSRF) via connector configs. Intended for ad-hoc
    /// runs submitted by callers with `Write`/`Admin`; callers with lower
    /// roles should not be allowed to submit arbitrary specs at all.
    pub fn validate_security(&self) -> Result<(), NexusError> {
        for (i, node) in self.sources.iter().enumerate() {
            validate_node_security(&node.config, &node.connector, "sources", i)?;
        }
        for (i, node) in self.sinks.iter().enumerate() {
            validate_node_security(&node.config, &node.connector, "sinks", i)?;
        }
        if let Some(dbt) = &self.dbt {
            validate_dbt_project_dir(&dbt.project_dir)?;
        }
        Ok(())
    }
}

/// Rejects absolute local paths and internal/private URLs anywhere inside a
/// connector config (recursively, including nested objects and arrays).
/// This is a defense-in-depth guard for pipeline specs.
fn validate_node_security(
    config: &Value,
    connector: &str,
    context: &str,
    index: usize,
) -> Result<(), NexusError> {
    match config {
        Value::String(s) => validate_string_security(s, connector, context, index, "value"),
        Value::Object(map) => {
            for (key, value) in map {
                if let Value::String(s) = value {
                    validate_string_security(s, connector, context, index, key)?;
                } else {
                    validate_node_security(value, connector, context, index)?;
                }
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                validate_node_security(item, connector, context, index)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_string_security(
    s: &str,
    connector: &str,
    context: &str,
    index: usize,
    key: &str,
) -> Result<(), NexusError> {
    // Absolute local path -> reject. URIs with schemes (postgres://,
    // http://, etc.) don't start with '/', so this only catches
    // filesystem paths.
    if s.starts_with('/') {
        return Err(NexusError::Schema(format!(
            "{context}[{index}] ({connector}): {key} must not be an absolute path"
        )));
    }
    // HTTP(S) URLs pointing to internal/reserved endpoints -> reject.
    if let Some(host) = http_host(s) {
        if is_internal_host(&host) {
            return Err(NexusError::Schema(format!(
                "{context}[{index}] ({connector}): {key} points to an internal host"
            )));
        }
    }
    Ok(())
}

/// Rejects dbt project_dir that uses parent-dir components or is absolute.
fn validate_dbt_project_dir(project_dir: &str) -> Result<(), NexusError> {
    if project_dir.starts_with('/') {
        return Err(NexusError::Schema(
            "dbt.project_dir must be a relative path".into(),
        ));
    }
    for component in std::path::Path::new(project_dir).components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(NexusError::Schema(
                "dbt.project_dir must not contain parent-dir references (..)".into(),
            ));
        }
    }
    Ok(())
}

fn http_host(s: &str) -> Option<String> {
    let url = url::Url::parse(s).ok()?;
    url.host_str().map(|h| h.to_lowercase())
}

fn is_internal_host(host: &str) -> bool {
    // Strip optional port.
    let host = host.split(':').next().unwrap_or(host);

    // Named localhost / link-local / internal endpoints.
    if host == "localhost"
        || host == "127.0.0.1"
        || host == "::1"
        || host.ends_with(".local")
        || host == "metadata.google.internal"
        || host == "metadata.google.internal."
        || host == "instance-data.azuremetadata"
        || host == "kubernetes.default.svc"
        || host == "kubernetes.default.svc.cluster.local"
        || host == "0.0.0.0"
        || host == "::"
    {
        return true;
    }

    // Cloud metadata endpoints.
    if host == "169.254.169.254" {
        return true;
    }

    // Parse as IP and check private/reserved ranges.
    if let Ok(addr) = host.parse::<std::net::IpAddr>() {
        match addr {
            std::net::IpAddr::V4(v4) => {
                let o = v4.octets();
                // 10.0.0.0/8
                if o[0] == 10 {
                    return true;
                }
                // 172.16.0.0/12
                if o[0] == 172 && (16..=31).contains(&o[1]) {
                    return true;
                }
                // 192.168.0.0/16
                if o[0] == 192 && o[1] == 168 {
                    return true;
                }
                // 127.0.0.0/8 (localhost range)
                if o[0] == 127 {
                    return true;
                }
                // 100.64.0.0/10 (CGNAT / shared address space)
                if o[0] == 100 && (64..=127).contains(&o[1]) {
                    return true;
                }
                // 169.254.0.0/16 (link-local IPv4)
                if o[0] == 169 && o[1] == 254 {
                    return true;
                }
            }
            std::net::IpAddr::V6(v6) => {
                if v6.is_loopback()
                    || v6.is_unique_local()
                    || v6.is_unspecified()
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
                {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_linear_json() -> &'static str {
        r#"{
            "pipeline_id": "pg-to-pg-demo",
            "sources": [{"connector": "postgres", "config": {"table": "events"}}],
            "sinks": [{"connector": "postgres", "config": {"table": "events_copy"}}],
            "partitions": 4
        }"#
    }

    #[test]
    fn parses_valid_linear_pipeline() {
        let spec = PipelineSpec::parse(valid_linear_json()).expect("valid spec parses");
        assert_eq!(spec.pipeline_id, "pg-to-pg-demo");
        assert_eq!(spec.sources[0].connector, "postgres");
        assert_eq!(spec.partitions, 4);
        assert_eq!(spec.channel_capacity, 100, "default channel capacity");
        assert!(!spec.has_transform());
    }

    #[test]
    fn parses_valid_fan_in_transform_pipeline() {
        let json = r#"{
            "pipeline_id": "join-demo",
            "sources": [
                {"name": "events", "connector": "postgres", "config": {}},
                {"name": "regions", "connector": "postgres", "config": {}}
            ],
            "transform": {"sql": "SELECT * FROM events JOIN regions ON events.region = regions.region"},
            "sinks": [{"connector": "sqlite", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).expect("valid fan-in spec parses");
        assert!(spec.has_transform());
        assert_eq!(spec.sources.len(), 2);
        assert_eq!(spec.sources[0].resolved_name(0, "source"), "events");
        assert_eq!(spec.sinks[0].resolved_name(0, "sink"), "sink0");
    }

    #[test]
    fn rejects_empty_pipeline_id() {
        let json = r#"{
            "pipeline_id": "",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let err = PipelineSpec::parse(json).expect_err("empty pipeline_id must fail");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_zero_partitions() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}],
            "partitions": 0
        }"#;
        let err = PipelineSpec::parse(json).expect_err("zero partitions must fail");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_malformed_json() {
        let err = PipelineSpec::parse("{not json").expect_err("malformed json must fail");
        assert!(matches!(err, NexusError::Serialization(_)));
    }

    #[test]
    fn rejects_multiple_sources_without_a_transform() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [
                {"connector": "postgres", "config": {}},
                {"connector": "postgres", "config": {}}
            ],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let err =
            PipelineSpec::parse(json).expect_err("fan-in without a transform must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_empty_transform_sql() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [
                {"connector": "postgres", "config": {}},
                {"connector": "postgres", "config": {}}
            ],
            "transform": {"sql": "   "},
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let err = PipelineSpec::parse(json).expect_err("blank transform SQL must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn parses_pipeline_with_embedding_node() {
        let json = r#"{
            "pipeline_id": "embed-demo",
            "sources": [{"connector": "postgres", "config": {}}],
            "transform": {"sql": "SELECT * FROM source0"},
            "sinks": [{"connector": "lancedb", "config": {}}],
            "embedding": {
                "source_column": "body",
                "output_column": "embedding",
                "dimension": 384,
                "model": {
                    "repo": "sentence-transformers/all-MiniLM-L6-v2",
                    "filename": "model.onnx",
                    "tokenizer_filename": "tokenizer.json",
                    "max_length": 128
                },
                "chunking": {
                    "strategy": "fixed_window",
                    "chunk_size": 256,
                    "overlap": 32
                }
            }
        }"#;
        let spec = PipelineSpec::parse(json).expect("valid embedding spec parses");
        assert!(spec.embedding.is_some());
        let embedding = spec.embedding.unwrap();
        assert_eq!(embedding.source_column, "body");
        assert_eq!(embedding.dimension, 384);
        assert_eq!(embedding.model.max_length, 128);
    }

    #[test]
    fn rejects_embedding_spec_with_empty_source_column() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "postgres", "config": {}}],
            "transform": {"sql": "SELECT 1"},
            "sinks": [{"connector": "lancedb", "config": {}}],
            "embedding": {
                "source_column": "",
                "output_column": "embedding",
                "dimension": 384,
                "model": {"repo": "r", "filename": "m.onnx", "tokenizer_filename": "t.json", "max_length": 128},
                "chunking": {"strategy": "fixed_window", "chunk_size": 256}
            }
        }"#;
        let err = PipelineSpec::parse(json).expect_err("empty source column must fail");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn validate_security_rejects_ssrf_via_userinfo() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "rest", "config": {"uri": "http://user:pass@169.254.169.254/latest/meta-data"}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("SSRF must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("internal host"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_security_rejects_absolute_path() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "sqlite", "config": {"path": "/etc/passwd"}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("absolute path must be rejected");
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn validate_security_rejects_nested_internal_url() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "rest", "config": {"endpoint": {"base_url": "http://localhost:8080"}}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("nested internal URL must be rejected");
        assert!(err.to_string().contains("internal host"));
    }

    #[test]
    fn validate_security_rejects_internal_url_in_array() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "rest", "config": {"uris": ["http://10.0.0.1/api"]}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("internal URL in array must be rejected");
        assert!(err.to_string().contains("internal host"));
    }

    #[test]
    fn validate_security_rejects_dbt_path_traversal() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "postgres", "config": {}}],
            "sinks": [{"connector": "postgres", "config": {}}],
            "dbt": {"project_dir": "../etc", "command": "run"}
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("dbt path traversal must be rejected");
        assert!(err.to_string().contains("parent-dir"));
    }

    #[test]
    fn validate_security_accepts_external_url() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "rest", "config": {"uri": "https://example.com/api"}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        spec.validate_security().expect("external URL must be accepted");
    }

    #[test]
    fn validate_security_rejects_cgnat_host() {
        let json = r#"{
            "pipeline_id": "p",
            "sources": [{"connector": "rest", "config": {"uri": "http://100.64.0.1/api"}}],
            "sinks": [{"connector": "postgres", "config": {}}]
        }"#;
        let spec = PipelineSpec::parse(json).unwrap();
        let err = spec.validate_security().expect_err("CGNAT host must be rejected");
        assert!(err.to_string().contains("internal host"));
    }
}
