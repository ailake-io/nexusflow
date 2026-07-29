use crate::error::NexusError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One node in the DAG: which connector to use and its raw config (validated
/// by the connector itself, not here — nexus-core doesn't know connector
/// internals). See ARCHITECTURE.md §3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub connector: String,
    #[serde(default)]
    pub config: Value,
}

/// MVP DAG: strictly linear `source -> sink`, no transform/fan-out/fan-in yet
/// (that's Marco 2). See IMPLEMENTATION_PLAN.md Marco 1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub pipeline_id: String,
    pub source: NodeSpec,
    pub sink: NodeSpec,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
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

    /// Public so callers that skip [`PipelineSpec::parse`] (e.g. an Axum
    /// `Json<PipelineSpec>` extractor, which deserializes directly) can still
    /// run the same validation explicitly.
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.pipeline_id.trim().is_empty() {
            return Err(NexusError::Schema("pipeline_id must not be empty".into()));
        }
        if self.source.connector.trim().is_empty() {
            return Err(NexusError::Schema(
                "source.connector must not be empty".into(),
            ));
        }
        if self.sink.connector.trim().is_empty() {
            return Err(NexusError::Schema(
                "sink.connector must not be empty".into(),
            ));
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

    fn valid_json() -> &'static str {
        r#"{
            "pipeline_id": "pg-to-pg-demo",
            "source": {"connector": "postgres", "config": {"table": "events"}},
            "sink": {"connector": "postgres", "config": {"table": "events_copy"}},
            "partitions": 4
        }"#
    }

    #[test]
    fn parses_valid_linear_pipeline() {
        let spec = PipelineSpec::parse(valid_json()).expect("valid spec parses");
        assert_eq!(spec.pipeline_id, "pg-to-pg-demo");
        assert_eq!(spec.source.connector, "postgres");
        assert_eq!(spec.partitions, 4);
        assert_eq!(spec.channel_capacity, 100, "default channel capacity");
    }

    #[test]
    fn rejects_empty_pipeline_id() {
        let json = r#"{
            "pipeline_id": "",
            "source": {"connector": "postgres", "config": {}},
            "sink": {"connector": "postgres", "config": {}}
        }"#;
        let err = PipelineSpec::parse(json).expect_err("empty pipeline_id must fail");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn rejects_zero_partitions() {
        let json = r#"{
            "pipeline_id": "p",
            "source": {"connector": "postgres", "config": {}},
            "sink": {"connector": "postgres", "config": {}},
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
}
