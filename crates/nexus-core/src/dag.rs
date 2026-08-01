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
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// ELT mode — dbt run/build/test against the sink warehouse after the
    /// raw load succeeds (Marco 10). `None` (the default) means "no dbt
    /// step", the same as before this field existed.
    #[serde(default)]
    pub dbt: Option<DbtConfig>,
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

        if self.channel_capacity == 0 {
            return Err(NexusError::Schema("channel_capacity must be > 0".into()));
        }
        if self.partitions == 0 {
            return Err(NexusError::Schema("partitions must be > 0".into()));
        }
        Ok(())
    }
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
}
