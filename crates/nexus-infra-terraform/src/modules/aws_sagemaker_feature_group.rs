//! Plain `aws_sagemaker_feature_group` (Feature Store). Online store is
//! always enabled (the common case); offline store (S3-backed) is
//! attached only when `s3_offline_store_uri` is set.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct FeatureDefinition {
    feature_name: String,
    /// "Integral", "Fractional", or "String".
    feature_type: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    feature_group_name: String,
    record_identifier_feature_name: String,
    event_time_feature_name: String,
    /// ARN of the role SageMaker assumes to read/write the store — wire
    /// from an `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["sagemaker.amazonaws.com"]`).
    role_arn: String,
    feature_definitions: Vec<FeatureDefinition>,
    /// S3 URI for the offline store — wire from an `aws-s3-data-lake`
    /// node's `bucket_arn`-derived `s3://<bucket>/prefix` path. Omit to
    /// keep the feature group online-only.
    #[serde(default)]
    s3_offline_store_uri: Option<String>,
}

submit_infra_module!(
    "aws-sagemaker-feature-group",
    "SageMaker Feature Group",
    "ai",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let feature_group_name = get("feature_group_name", &format!("\"{id}\""));
    let record_identifier_feature_name = get("record_identifier_feature_name", "\"\"");
    let event_time_feature_name = get("event_time_feature_name", "\"\"");
    let role_arn = get("role_arn", "\"\"");
    let feature_definitions = get("feature_definitions", "[]");

    let offline_store_block = args
        .get("s3_offline_store_uri")
        .map(|uri| {
            format!(
                "\n\n  offline_store_config {{\n    s3_storage_config {{\n      s3_uri = {uri}\n    }}\n  }}"
            )
        })
        .unwrap_or_default();

    format!(
        "resource \"aws_sagemaker_feature_group\" \"{id}\" {{\n  feature_group_name             = {feature_group_name}\n  record_identifier_feature_name = {record_identifier_feature_name}\n  event_time_feature_name        = {event_time_feature_name}\n  role_arn                       = {role_arn}\n\n  dynamic \"feature_definition\" {{\n    for_each = {feature_definitions}\n    content {{\n      feature_name = feature_definition.value.feature_name\n      feature_type = feature_definition.value.feature_type\n    }}\n  }}\n\n  online_store_config {{\n    enable_online_store = true\n  }}{offline_store_block}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("aws_sagemaker_feature_group.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
