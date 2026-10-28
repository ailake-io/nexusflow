//! Plain `aws_glue_crawler`, S3-target only (the common case pairing
//! with `aws-s3-data-lake` + `aws-glue-catalog`). JDBC/DynamoDB targets
//! aren't modeled here.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    /// ARN of the role the crawler assumes — wire from an `aws-iam-role`
    /// node's `iam_role_arn` output (`trusted_role_services:
    /// ["glue.amazonaws.com"]`).
    role_arn: String,
    /// Name (not ARN) of the target Glue Catalog database — wire from an
    /// `aws-glue-catalog` node's `name` config value (no graph output for
    /// it, so pass the same literal on both).
    database_name: String,
    /// s3:// path the crawler scans.
    s3_target_path: String,
    /// Cron expression, e.g. "cron(0 6 * * ? *)" — omit for on-demand
    /// runs only.
    #[serde(default)]
    schedule: Option<String>,
}

submit_infra_module!(
    "aws-glue-crawler",
    "Glue Crawler",
    "data",
    "aws",
    Config,
    &["name", "arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let role_arn = get("role_arn", "\"\"");
    let database_name = get("database_name", "\"\"");
    let s3_target_path = get("s3_target_path", "\"\"");

    let schedule_line = args
        .get("schedule")
        .map(|s| format!("\n  schedule = {s}"))
        .unwrap_or_default();

    format!(
        "resource \"aws_glue_crawler\" \"{id}\" {{\n  name          = {name}\n  role          = {role_arn}\n  database_name = {database_name}{schedule_line}\n\n  s3_target {{\n    path = {s3_target_path}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "name" => Some(format!("aws_glue_crawler.{id}.name")),
        "arn" => Some(format!("aws_glue_crawler.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
