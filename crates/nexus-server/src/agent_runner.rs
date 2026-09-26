//! The agent loop (ROADMAP.md Fase 31): calls the model, decides whether it
//! wants a tool or has a final answer, executes/persists/repeats until a
//! `LlmTurn::Text` or `max_steps`. Pausing/resuming on `RequireApproval` is
//! the one piece with no equivalent elsewhere in this codebase — everything
//! else (persistence, cost, tools) reuses `agent_store.rs`/
//! `agent_run_store.rs`/`agent_tools.rs`.

use crate::agent_run_store::{AgentRunStoreError, ApprovalStatus, RunStatus};
use crate::agent_tools::{self, ToolOutput};
use crate::AppState;
use nexus_ai::llm::{LlmBackend, LlmTurn, ToolCall, ToolDef, ToolMessage, ToolTurn};
use nexus_core::{AgentSpec, ApprovalMode, LlmModelConfig};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum AgentRunnerError {
    #[error("agent run: {0}")]
    Store(#[from] AgentRunStoreError),
    #[error("agent run: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("agent run: prompt {0:?} not found")]
    PromptNotFound(String),
    #[error("agent run: {0}")]
    Internal(String),
    #[error("agent run: step {0} is not pending approval")]
    StepNotPending(i64),
    #[error("agent run: step {0} names unknown tool {1:?}")]
    UnknownTool(i64, String),
}

/// Starts a new run in a detached task and returns its id immediately —
/// same 202-style contract as `start_pipeline_run` (a tool-calling
/// conversation can take much longer than one HTTP request should block
/// on).
pub async fn start_run(
    state: &AppState,
    agent: &AgentSpec,
    question: String,
    model_override: Option<LlmModelConfig>,
) -> Result<i64, AgentRunnerError> {
    let model_override_json = model_override
        .as_ref()
        .map(|m| serde_json::to_string(m).expect("LlmModelConfig always serializes"));
    let run_id = state
        .agent_runs
        .start_run(&agent.agent_id, &question, model_override_json.as_deref())
        .await?;

    let state = state.clone();
    let agent = agent.clone();
    tokio::spawn(async move {
        let system_prompt = match resolve_system_prompt(&state, &agent).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(run_id, error = %e, "agent run failed to resolve prompt");
                record_error_step(&state, run_id, &e.to_string()).await;
                let _ = state.agent_runs.finish_run(run_id, RunStatus::Failed).await;
                return;
            }
        };
        let history = vec![
            ToolMessage::System(system_prompt),
            ToolMessage::User(question),
        ];
        run_loop(&state, &agent, run_id, history, model_override, 0).await;
    });

    Ok(run_id)
}

/// Resumes a run whose `RequireApproval` tool call is pending — runs (or
/// skips, if rejected) the tool, persists the outcome, and continues the
/// loop from exactly where it paused. Spawns the continuation the same way
/// `start_run` does, for the same reason (may take a while).
pub async fn resume_after_approval(
    state: &AppState,
    agent: &AgentSpec,
    run_id: i64,
    step_id: i64,
    approved: bool,
    approved_by: &str,
) -> Result<(), AgentRunnerError> {
    let step = state.agent_runs.get_step(step_id).await?;
    if step.approval_status != ApprovalStatus::Pending {
        return Err(AgentRunnerError::StepNotPending(step_id));
    }
    let tool_name = step
        .tool
        .clone()
        .ok_or_else(|| AgentRunnerError::UnknownTool(step_id, "(none)".to_string()))?;
    let tool_cfg = agent
        .tools
        .iter()
        .find(|tc| tc.tool.name() == tool_name)
        .ok_or_else(|| AgentRunnerError::UnknownTool(step_id, tool_name.clone()))?;

    let (_, arguments) = parse_step_args(step.args.as_deref());

    let result_text = if approved {
        match agent_tools::execute_tool(state, &tool_cfg.tool, &arguments).await {
            Ok(output) => summarize_tool_output(output),
            Err(e) => format!("tool execution failed: {e}"),
        }
    } else {
        "rejected by human reviewer — tool was not executed".to_string()
    };
    state
        .agent_runs
        .resolve_step(
            step_id,
            if approved {
                ApprovalStatus::Approved
            } else {
                ApprovalStatus::Rejected
            },
            approved_by,
            Some(&result_text),
        )
        .await?;

    let run = state.agent_runs.get_run(run_id).await?;
    // `list_steps`/`rebuild_history_prefix` run *after* `resolve_step`
    // above, so the step just resolved already carries its `result` and is
    // included like any other completed turn — no separate manual push
    // needed here.
    let steps = state.agent_runs.list_steps(run_id).await?;
    let system_prompt = resolve_system_prompt(state, agent).await?;
    let history = rebuild_history_prefix(&system_prompt, &run.question, &steps);
    let model_override = run
        .model_override_json
        .as_deref()
        .map(serde_json::from_str::<LlmModelConfig>)
        .transpose()
        .map_err(|e| {
            AgentRunnerError::Internal(format!("stored model_override is corrupt: {e}"))
        })?;

    let step_start = steps.len() as u32;
    let state = state.clone();
    let agent = agent.clone();
    tokio::spawn(async move {
        run_loop(&state, &agent, run_id, history, model_override, step_start).await;
    });
    Ok(())
}

async fn resolve_system_prompt(
    state: &AppState,
    agent: &AgentSpec,
) -> Result<String, AgentRunnerError> {
    state
        .prompt_templates
        .resolve(&agent.prompt.name, agent.prompt.version)
        .await
        .map_err(|e| AgentRunnerError::Internal(e.to_string()))?
        .ok_or_else(|| AgentRunnerError::PromptNotFound(agent.prompt.name.clone()))
}

/// Rebuilds every *completed* turn from a run's persisted steps — a step
/// with no `result` yet (the one currently pending approval) is left out,
/// its turn is completed by whoever is resuming it. Returns just the
/// system+user prefix when `steps` is empty (a fresh run has none).
fn rebuild_history_prefix(
    system_prompt: &str,
    question: &str,
    steps: &[crate::agent_run_store::AgentStep],
) -> Vec<ToolMessage> {
    let mut history = vec![
        ToolMessage::System(system_prompt.to_string()),
        ToolMessage::User(question.to_string()),
    ];
    for step in steps {
        if step.kind != "tool_call" {
            continue;
        }
        let Some(tool_name) = &step.tool else {
            continue;
        };
        let Some(result) = &step.result else {
            // Still pending — its turn isn't in history yet.
            continue;
        };
        let (tool_call_id, arguments) = parse_step_args(step.args.as_deref());
        history.push(ToolMessage::AssistantToolCalls(vec![ToolCall {
            id: tool_call_id.clone(),
            name: tool_name.clone(),
            arguments,
        }]));
        history.push(ToolMessage::ToolResult {
            call_id: tool_call_id,
            content: result.clone(),
        });
    }
    history
}

fn parse_step_args(args: Option<&str>) -> (String, Value) {
    let Some(args) = args else {
        return (String::new(), Value::Null);
    };
    let parsed: Value = serde_json::from_str(args).unwrap_or(Value::Null);
    let tool_call_id = parsed
        .get("tool_call_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let arguments = parsed.get("arguments").cloned().unwrap_or(Value::Null);
    (tool_call_id, arguments)
}

fn encode_step_args(call: &ToolCall) -> String {
    serde_json::json!({"tool_call_id": call.id, "arguments": call.arguments}).to_string()
}

fn summarize_tool_output(output: ToolOutput) -> String {
    match output {
        ToolOutput::Text(text) => text,
        ToolOutput::Chart(chart) => format!(
            "chart rendered ({} bytes, {})",
            chart.bytes.len(),
            chart.content_type
        ),
    }
}

fn cost_rates(model: &LlmModelConfig) -> (f64, f64) {
    match model {
        LlmModelConfig::Api {
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
            ..
        }
        | LlmModelConfig::Anthropic {
            cost_per_1k_prompt_tokens,
            cost_per_1k_completion_tokens,
            ..
        } => (
            cost_per_1k_prompt_tokens.unwrap_or(0.0),
            cost_per_1k_completion_tokens.unwrap_or(0.0),
        ),
    }
}

async fn record_error_step(state: &AppState, run_id: i64, message: &str) {
    if let Err(e) = state
        .agent_runs
        .append_step(
            run_id,
            "error",
            None,
            None,
            Some(message),
            ApprovalStatus::NotApplicable,
        )
        .await
    {
        tracing::warn!(run_id, error = %e, "failed to persist agent error step");
    }
}

/// The loop itself — model call, decide, execute-or-pause, repeat.
/// `step_start` is how many tool-calling turns already happened (0 for a
/// fresh run, `steps.len()` after a resume) — the remaining budget is
/// `agent.max_steps - step_start`, so a resumed run can't reset the clock
/// by pausing for approval.
async fn run_loop(
    state: &AppState,
    agent: &AgentSpec,
    run_id: i64,
    mut history: Vec<ToolMessage>,
    model_override: Option<LlmModelConfig>,
    step_start: u32,
) {
    let model = model_override.as_ref().unwrap_or(&agent.model);
    let backend = nexus_ai::llm::load_llm_backend_for_model(model);
    let (cost_per_1k_prompt, cost_per_1k_completion) = cost_rates(model);
    let tool_defs: Vec<ToolDef> = agent
        .tools
        .iter()
        .map(|tc| ToolDef {
            name: tc.tool.name().to_string(),
            description: tc.tool.description().to_string(),
            schema: tc.tool.json_schema(),
        })
        .collect();

    for _ in step_start..agent.max_steps {
        let call_result = match &backend {
            LlmBackend::Api(client) => {
                client
                    .call_with_tools(&history, &tool_defs, None, None)
                    .await
            }
            LlmBackend::Anthropic(client) => {
                client
                    .call_with_tools(&history, &tool_defs, None, None)
                    .await
            }
        };
        let ToolTurn {
            turn,
            tokens_prompt,
            tokens_completion,
        } = match call_result {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(run_id, error = %e, "agent run: llm call failed");
                record_error_step(state, run_id, &e.to_string()).await;
                let _ = state.agent_runs.finish_run(run_id, RunStatus::Failed).await;
                return;
            }
        };
        let cost = cost_per_1k_prompt * (tokens_prompt as f64 / 1000.0)
            + cost_per_1k_completion * (tokens_completion as f64 / 1000.0);
        if let Err(e) = state
            .agent_runs
            .add_usage(run_id, tokens_prompt + tokens_completion, cost)
            .await
        {
            tracing::warn!(run_id, error = %e, "failed to record agent run usage");
        }

        match turn {
            LlmTurn::Text(answer) => {
                if let Err(e) = state
                    .agent_runs
                    .append_step(
                        run_id,
                        "final_answer",
                        None,
                        None,
                        Some(&answer),
                        ApprovalStatus::NotApplicable,
                    )
                    .await
                {
                    tracing::warn!(run_id, error = %e, "failed to persist final answer step");
                }
                let _ = state
                    .agent_runs
                    .finish_run(run_id, RunStatus::Completed)
                    .await;
                return;
            }
            LlmTurn::ToolCalls(calls) => {
                // v1 limitation (documented in nexus-ai's Anthropic client):
                // one tool call per turn. A model requesting several is
                // unusual; take the first and ignore the rest rather than
                // failing the run outright.
                let Some(call) = calls.into_iter().next() else {
                    record_error_step(state, run_id, "model returned an empty tool_calls list")
                        .await;
                    let _ = state.agent_runs.finish_run(run_id, RunStatus::Failed).await;
                    return;
                };
                let Some(tool_cfg) = agent.tools.iter().find(|tc| tc.tool.name() == call.name)
                else {
                    record_error_step(
                        state,
                        run_id,
                        &format!("model requested unknown tool {:?}", call.name),
                    )
                    .await;
                    let _ = state.agent_runs.finish_run(run_id, RunStatus::Failed).await;
                    return;
                };

                match tool_cfg.approval {
                    ApprovalMode::Auto => {
                        let output =
                            agent_tools::execute_tool(state, &tool_cfg.tool, &call.arguments).await;
                        let result_text = match output {
                            Ok(o) => summarize_tool_output(o),
                            Err(e) => format!("tool execution failed: {e}"),
                        };
                        if let Err(e) = state
                            .agent_runs
                            .append_step(
                                run_id,
                                "tool_call",
                                Some(&call.name),
                                Some(&encode_step_args(&call)),
                                Some(&result_text),
                                ApprovalStatus::NotApplicable,
                            )
                            .await
                        {
                            tracing::warn!(run_id, error = %e, "failed to persist tool_call step");
                        }
                        history.push(ToolMessage::AssistantToolCalls(vec![call.clone()]));
                        history.push(ToolMessage::ToolResult {
                            call_id: call.id,
                            content: result_text,
                        });
                    }
                    ApprovalMode::RequireApproval => {
                        let step_id = match state
                            .agent_runs
                            .append_step(
                                run_id,
                                "tool_call",
                                Some(&call.name),
                                Some(&encode_step_args(&call)),
                                None,
                                ApprovalStatus::Pending,
                            )
                            .await
                        {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::warn!(run_id, error = %e, "failed to persist pending tool_call step");
                                let _ =
                                    state.agent_runs.finish_run(run_id, RunStatus::Failed).await;
                                return;
                            }
                        };
                        if let Err(e) = state.agent_runs.set_status_waiting_approval(run_id).await {
                            tracing::warn!(run_id, error = %e, "failed to mark run waiting_approval");
                        }
                        state.alerts.notify_agent_approval_needed(
                            &agent.agent_id,
                            run_id,
                            step_id,
                            &call.name,
                        );
                        return;
                    }
                }
            }
        }
    }

    let _ = state
        .agent_runs
        .finish_run(run_id, RunStatus::MaxStepsReached)
        .await;
}
