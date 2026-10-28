//! No community module is the standard here either, and this one is
//! inherently a 3-resource unit (model → endpoint configuration →
//! endpoint) — `emit` returns all three wired together rather than one
//! `resource` block. Flat config fields hide that structure from the
//! canvas user.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Base name — the model, endpoint config, and endpoint are all named
    /// off this (`<name>`, `<name>-config`, `<name>` respectively).
    name: String,
    /// ARN of the role SageMaker assumes to load/run the model — wire
    /// this to an `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["sagemaker.amazonaws.com"]`).
    execution_role_arn: String,
    /// Full image URI SageMaker pulls the model container from — wire
    /// this to an `aws-ecr-repo` node's `repository_url` output (append
    /// your own `:tag`).
    image: String,
    /// Instance type backing the endpoint, e.g. "ml.m5.large" — real
    /// hourly cost while the endpoint exists, not just while it's called.
    #[serde(default = "default_instance_type")]
    instance_type: String,
    /// Number of instances behind the endpoint.
    #[serde(default = "default_instance_count")]
    initial_instance_count: u32,
}

fn default_instance_type() -> String {
    "ml.m5.large".to_string()
}
fn default_instance_count() -> u32 {
    1
}

submit_infra_module!(
    "aws-sagemaker-endpoint",
    "SageMaker Endpoint",
    "ai",
    "aws",
    Config,
    &["arn", "name"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let execution_role_arn = get("execution_role_arn", "\"\"");
    let image = get("image", "\"\"");
    let instance_type = get("instance_type", "\"ml.m5.large\"");
    let instance_count = get("initial_instance_count", "1");
    let model_id = format!("{id}_model");
    let config_id = format!("{id}_config");

    // `config_name` is a literal derived from `id` (a plain Rust string
    // this crate controls), not from the user-supplied `name` expression
    // — `name` can itself be an edge reference (`module.x.y`, not a bare
    // string), which can't be safely embedded inside another interpolated
    // string literal the way a real value could.
    let config_name = format!("\"{id}-config\"");

    format!(
        "resource \"aws_sagemaker_model\" \"{model_id}\" {{\n  name               = {name}\n  execution_role_arn = {execution_role_arn}\n\n  primary_container {{\n    image = {image}\n  }}\n}}\n\nresource \"aws_sagemaker_endpoint_configuration\" \"{config_id}\" {{\n  name = {config_name}\n\n  production_variants {{\n    variant_name           = \"AllTraffic\"\n    model_name             = aws_sagemaker_model.{model_id}.name\n    initial_instance_count = {instance_count}\n    instance_type          = {instance_type}\n  }}\n}}\n\nresource \"aws_sagemaker_endpoint\" \"{id}\" {{\n  name                 = {name}\n  endpoint_config_name = aws_sagemaker_endpoint_configuration.{config_id}.name\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" | "name" => Some(format!("aws_sagemaker_endpoint.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
