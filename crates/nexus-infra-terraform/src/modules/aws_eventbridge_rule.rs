//! 2-resource custom emit: `aws_cloudwatch_event_rule` +
//! `aws_cloudwatch_event_target` — a scheduled or event-pattern trigger
//! pointed at a downstream target (Lambda, Step Functions, ECS task,
//! etc). `target_arn` takes any invokable ARN; permission to be invoked
//! by EventBridge (e.g. `aws_lambda_permission`) isn't modeled here —
//! add it by hand if the target needs one (Lambda does, Step Functions
//! doesn't when invoked via an already-scoped `target_role_arn`).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// Cron/rate expression, e.g. "rate(1 hour)" or
    /// "cron(0 6 * * ? *)" — mutually exclusive with `event_pattern` in
    /// real usage, but only one is normally set at a time.
    #[serde(default)]
    schedule_expression: Option<String>,
    /// EventBridge event pattern JSON — alternative to
    /// `schedule_expression` for event-driven (non-scheduled) triggers.
    #[serde(default)]
    event_pattern: Option<String>,
    /// ARN of the resource invoked when the rule fires — wire from a
    /// downstream node's ARN output (e.g. `aws-lambda-function`'s
    /// `arn`, `aws-stepfunctions-state-machine`'s `arn`).
    target_arn: String,
    /// ARN of the role EventBridge assumes to invoke the target — needed
    /// for Step Functions/ECS targets, not for Lambda (which uses
    /// resource-based permissions instead).
    #[serde(default)]
    target_role_arn: Option<String>,
}

submit_infra_module!(
    "aws-eventbridge-rule",
    "EventBridge Rule",
    "cicd",
    "aws",
    Config,
    &["rule_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let target_arn = get("target_arn", "\"\"");

    let schedule_line = args
        .get("schedule_expression")
        .map(|s| format!("\n  schedule_expression = {s}"))
        .unwrap_or_default();
    let pattern_line = args
        .get("event_pattern")
        .map(|p| format!("\n  event_pattern = {p}"))
        .unwrap_or_default();
    let role_line = args
        .get("target_role_arn")
        .map(|r| format!("\n  role_arn = {r}"))
        .unwrap_or_default();

    let rule_id = format!("{id}_rule");

    format!(
        "resource \"aws_cloudwatch_event_rule\" \"{rule_id}\" {{\n  name = {name}{schedule_line}{pattern_line}\n}}\n\nresource \"aws_cloudwatch_event_target\" \"{id}\" {{\n  rule = aws_cloudwatch_event_rule.{rule_id}.name\n  arn  = {target_arn}{role_line}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "rule_arn" => Some(format!("aws_cloudwatch_event_rule.{id}_rule.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
