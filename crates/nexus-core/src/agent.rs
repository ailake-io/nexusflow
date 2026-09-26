//! `AgentSpec` — an LLM that decides itself which tool to call, in what
//! order, until it either answers or exhausts `max_steps` (ROADMAP.md Fase
//! 31, "Agente de IA com tool-calling (estilo n8n AI Agent)"). Not a DAG:
//! a `PipelineSpec` is source->transform->sink with a fixed order the user
//! draws; an agent's order is decided by the model at run time, so it gets
//! its own spec shape instead of being forced into `PipelineSpec.llm`
//! (which is 1-call-per-row, no tools, no loop).

use crate::dag::{http_host, is_internal_host, validate_llm_security, LlmModelConfig, NodeSpec};
use crate::error::NexusError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    pub agent_id: String,
    pub name: String,
    /// Reuses `PipelineSpec.llm`'s own `PromptRef` — a named, versioned
    /// system prompt, resolved at run time the same way the `llm` node
    /// already resolves one (`prompt_template_store.rs`).
    pub prompt: crate::dag::PromptRef,
    /// Reuses `PipelineSpec.llm.model` verbatim (`Api` | `Anthropic`) — an
    /// agent talks to the same two backends the batch `llm` node does,
    /// just with `tools` attached to the request.
    pub model: LlmModelConfig,
    pub tools: Vec<AgentToolConfig>,
    /// Required, no "unlimited" default — a long loop over expensive
    /// tools (vector search + an LLM call per step) must have a hard
    /// ceiling (ROADMAP.md Fase 31 "Riscos").
    pub max_steps: u32,
    /// Same cron the pipeline scheduler already parses
    /// (`nexus_core::parse_cron_expression`) — `None` means the agent
    /// only runs on demand (`POST /agents/{id}/run`).
    #[serde(default)]
    pub schedule: Option<String>,
}

/// One tool made available to the agent, plus whether it needs a human's
/// sign-off before it actually runs. Per-tool, not per-agent — the same
/// agent can auto-run a read-only search while requiring approval before
/// it triggers a pipeline with real side effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentToolConfig {
    pub tool: AgentToolKind,
    pub approval: ApprovalMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    Auto,
    RequireApproval,
}

/// The v1 tool catalog (ROADMAP.md Fase 31 checklist) — each variant is a
/// thin wrapper over code that already exists elsewhere in the workspace
/// (`nexus-server`'s preview/search/run-pipeline/webhook/python-transform
/// machinery); no new execution engine, see `agent_tools.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AgentToolKind {
    /// Runs a SQL query (or a plain preview) over a source, same engine
    /// as a saved pipeline's preview (`DataFusionTransform`).
    QueryData {
        source: NodeSpec,
        #[serde(default)]
        sql: Option<String>,
    },
    /// Vector similarity search against one of the connectors RAG
    /// already knows how to search (`rag.rs`'s own dispatch table).
    SearchVectors {
        connector: String,
        #[serde(default)]
        config: Value,
        #[serde(default = "default_top_k")]
        top_k: usize,
    },
    /// Triggers a saved pipeline the same way `POST /pipelines/{id}/run`
    /// does — real side effect, defaults to `RequireApproval` in
    /// practice (not enforced by the type, a config choice).
    RunPipeline {
        pipeline_id: String,
        #[serde(default)]
        wait_for_result: bool,
    },
    /// Generic outbound HTTP call — same SSRF posture as every
    /// `rest`/`webhook` connector and alert channel already in this
    /// codebase (`validate_security_with`/`dns_guard.rs`).
    CallWebhook {
        url: String,
        #[serde(default = "default_webhook_method")]
        method: String,
        #[serde(default)]
        body_template: Option<String>,
    },
    /// Runs a user's Python `visualize(df)` in an isolated subprocess
    /// (sibling of `PipelineSpec.python`/`python_transform.rs`) and
    /// returns the resulting image bytes instead of appending a column.
    GenerateChart {
        source: NodeSpec,
        #[serde(default)]
        sql: Option<String>,
        script: String,
        #[serde(default)]
        timeout_seconds: Option<u64>,
    },
}

fn default_top_k() -> usize {
    5
}

fn default_webhook_method() -> String {
    "POST".to_string()
}

impl AgentToolKind {
    /// Stable name the model sees as the tool's function name in a
    /// tool-calling request, and the value persisted in
    /// `agent_steps.tool` — never renamed once an agent using it has run
    /// (would break resuming a `pending_approval` step recorded under the
    /// old name).
    pub fn name(&self) -> &'static str {
        match self {
            AgentToolKind::QueryData { .. } => "query_data",
            AgentToolKind::SearchVectors { .. } => "search_vectors",
            AgentToolKind::RunPipeline { .. } => "run_pipeline",
            AgentToolKind::CallWebhook { .. } => "call_webhook",
            AgentToolKind::GenerateChart { .. } => "generate_chart",
        }
    }
}

impl AgentSpec {
    pub fn validate(&self) -> Result<(), NexusError> {
        let id = self.agent_id.trim();
        if id.is_empty() {
            return Err(NexusError::Schema("agent_id must not be empty".into()));
        }
        if id.len() > 128 {
            return Err(NexusError::Schema(
                "agent_id must not exceed 128 characters".into(),
            ));
        }
        if !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(NexusError::Schema(
                "agent_id must only contain ASCII letters, digits, '_' or '-'".into(),
            ));
        }
        if self.name.trim().is_empty() {
            return Err(NexusError::Schema("name must not be empty".into()));
        }
        if self.max_steps == 0 {
            return Err(NexusError::Schema(
                "max_steps must be at least 1 — an agent with no step budget has no guard-rail \
                 against an infinite tool-calling loop"
                    .into(),
            ));
        }
        if self.tools.is_empty() {
            return Err(NexusError::Schema(
                "tools must not be empty — an agent with no tools can only ever answer from its \
                 own prompt, same as the plain llm node, and doesn't need this spec"
                    .into(),
            ));
        }
        for (i, tool_cfg) in self.tools.iter().enumerate() {
            match &tool_cfg.tool {
                AgentToolKind::RunPipeline { pipeline_id, .. } => {
                    if pipeline_id.trim().is_empty() {
                        return Err(NexusError::Schema(format!(
                            "tools[{i}]: run_pipeline.pipeline_id must not be empty"
                        )));
                    }
                }
                AgentToolKind::CallWebhook { url, method, .. } => {
                    if url.trim().is_empty() {
                        return Err(NexusError::Schema(format!(
                            "tools[{i}]: call_webhook.url must not be empty"
                        )));
                    }
                    if !matches!(
                        method.to_ascii_uppercase().as_str(),
                        "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
                    ) {
                        return Err(NexusError::Schema(format!(
                            "tools[{i}]: call_webhook.method {method:?} is not a supported HTTP \
                             method"
                        )));
                    }
                }
                AgentToolKind::GenerateChart { script, .. } => {
                    if script.trim().is_empty() {
                        return Err(NexusError::Schema(format!(
                            "tools[{i}]: generate_chart.script must not be empty"
                        )));
                    }
                }
                AgentToolKind::QueryData { .. } | AgentToolKind::SearchVectors { .. } => {}
            }
        }
        Ok(())
    }

    pub fn validate_security(&self) -> Result<(), NexusError> {
        self.validate_security_with(false)
    }

    /// Same `allow_internal_hosts` escape hatch as
    /// `PipelineSpec::validate_security_with` (ARCHITECTURE.md §10) — a
    /// self-hosted deployment's own private network is a legitimate
    /// target, so this only ever runs when the operator hasn't opted out.
    pub fn validate_security_with(&self, allow_internal_hosts: bool) -> Result<(), NexusError> {
        validate_llm_security(&self.model, allow_internal_hosts)?;
        if !allow_internal_hosts {
            for (i, tool_cfg) in self.tools.iter().enumerate() {
                if let AgentToolKind::CallWebhook { url, .. } = &tool_cfg.tool {
                    if url.starts_with('/') {
                        return Err(NexusError::Schema(format!(
                            "tools[{i}]: call_webhook.url must not be an absolute path"
                        )));
                    }
                    if let Some(host) = http_host(url) {
                        if is_internal_host(&host) {
                            return Err(NexusError::Schema(format!(
                                "tools[{i}]: call_webhook.url points to an internal host"
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::PromptRef;

    fn base_spec() -> AgentSpec {
        AgentSpec {
            agent_id: "support-agent".to_string(),
            name: "Support Agent".to_string(),
            prompt: PromptRef {
                name: "support-system-prompt".to_string(),
                version: None,
            },
            model: LlmModelConfig::Api {
                base_url: "https://api.openai.com/v1".to_string(),
                model: "gpt-4o-mini".to_string(),
                api_key_env: Some("OPENAI_API_KEY".to_string()),
                cost_per_1k_prompt_tokens: None,
                cost_per_1k_completion_tokens: None,
            },
            tools: vec![AgentToolConfig {
                tool: AgentToolKind::SearchVectors {
                    connector: "qdrant".to_string(),
                    config: serde_json::json!({}),
                    top_k: 5,
                },
                approval: ApprovalMode::Auto,
            }],
            max_steps: 8,
            schedule: None,
        }
    }

    #[test]
    fn valid_spec_passes() {
        assert!(base_spec().validate().is_ok());
    }

    #[test]
    fn rejects_empty_agent_id() {
        let mut spec = base_spec();
        spec.agent_id = "  ".to_string();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_zero_max_steps() {
        let mut spec = base_spec();
        spec.max_steps = 0;
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_empty_tools() {
        let mut spec = base_spec();
        spec.tools.clear();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_run_pipeline_with_empty_pipeline_id() {
        let mut spec = base_spec();
        spec.tools.push(AgentToolConfig {
            tool: AgentToolKind::RunPipeline {
                pipeline_id: "".to_string(),
                wait_for_result: true,
            },
            approval: ApprovalMode::RequireApproval,
        });
        assert!(spec.validate().is_err());
    }

    #[test]
    fn rejects_call_webhook_with_unsupported_method() {
        let mut spec = base_spec();
        spec.tools.push(AgentToolConfig {
            tool: AgentToolKind::CallWebhook {
                url: "https://example.com/hook".to_string(),
                method: "TRACE".to_string(),
                body_template: None,
            },
            approval: ApprovalMode::RequireApproval,
        });
        assert!(spec.validate().is_err());
    }

    #[test]
    fn validate_security_rejects_internal_llm_base_url() {
        let mut spec = base_spec();
        spec.model = LlmModelConfig::Api {
            base_url: "http://169.254.169.254".to_string(),
            model: "gpt-4o-mini".to_string(),
            api_key_env: None,
            cost_per_1k_prompt_tokens: None,
            cost_per_1k_completion_tokens: None,
        };
        assert!(spec.validate_security().is_err());
    }

    #[test]
    fn validate_security_rejects_internal_webhook_url() {
        let mut spec = base_spec();
        spec.tools.push(AgentToolConfig {
            tool: AgentToolKind::CallWebhook {
                url: "http://10.0.0.5/internal".to_string(),
                method: "POST".to_string(),
                body_template: None,
            },
            approval: ApprovalMode::RequireApproval,
        });
        assert!(spec.validate_security().is_err());
    }

    #[test]
    fn validate_security_allows_internal_hosts_when_opted_in() {
        let mut spec = base_spec();
        spec.tools.push(AgentToolConfig {
            tool: AgentToolKind::CallWebhook {
                url: "http://10.0.0.5/internal".to_string(),
                method: "POST".to_string(),
                body_template: None,
            },
            approval: ApprovalMode::RequireApproval,
        });
        assert!(spec.validate_security_with(true).is_ok());
    }

    #[test]
    fn tool_names_are_stable() {
        assert_eq!(
            AgentToolKind::QueryData {
                source: NodeSpec {
                    name: None,
                    connector: "postgres".to_string(),
                    config: serde_json::json!({}),
                },
                sql: None,
            }
            .name(),
            "query_data"
        );
        assert_eq!(
            AgentToolKind::GenerateChart {
                source: NodeSpec {
                    name: None,
                    connector: "postgres".to_string(),
                    config: serde_json::json!({}),
                },
                sql: None,
                script: "def visualize(df): ...".to_string(),
                timeout_seconds: None,
            }
            .name(),
            "generate_chart"
        );
    }
}
