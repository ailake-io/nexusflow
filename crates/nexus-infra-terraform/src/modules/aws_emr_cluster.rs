//! Plain `aws_emr_cluster` — no community module worth pinning to for a
//! generic Spark/Hadoop cluster; nested `ec2_attributes`/
//! `master_instance_group`/`core_instance_group` blocks are flattened
//! into top-level config fields and reconstructed in `emit`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    #[serde(default = "default_release_label")]
    release_label: String,
    /// EMR applications installed on the cluster.
    #[serde(default = "default_applications")]
    applications: Vec<String>,
    /// Instance profile name (not ARN) for cluster EC2 instances — wire
    /// this from an IAM instance-profile resource, not `aws-iam-role`'s
    /// plain role ARN output.
    instance_profile: String,
    /// ARN of the EMR service role — wire from an `aws-iam-role` node's
    /// `iam_role_arn` output (`trusted_role_services:
    /// ["elasticmapreduce.amazonaws.com"]`).
    service_role: String,
    subnet_id: String,
    #[serde(default = "default_instance_type")]
    master_instance_type: String,
    #[serde(default = "default_instance_type")]
    core_instance_type: String,
    #[serde(default = "default_core_instance_count")]
    core_instance_count: u32,
}

fn default_release_label() -> String {
    "emr-6.15.0".to_string()
}
fn default_applications() -> Vec<String> {
    vec!["Spark".to_string(), "Hadoop".to_string()]
}
fn default_instance_type() -> String {
    "m5.xlarge".to_string()
}
fn default_core_instance_count() -> u32 {
    2
}

submit_infra_module!(
    "aws-emr-cluster",
    "EMR Cluster",
    "data",
    "aws",
    Config,
    &["cluster_id", "cluster_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let release_label = get("release_label", "\"emr-6.15.0\"");
    let applications = get("applications", "[\"Spark\", \"Hadoop\"]");
    let instance_profile = get("instance_profile", "\"\"");
    let service_role = get("service_role", "\"\"");
    let subnet_id = get("subnet_id", "\"\"");
    let master_instance_type = get("master_instance_type", "\"m5.xlarge\"");
    let core_instance_type = get("core_instance_type", "\"m5.xlarge\"");
    let core_instance_count = get("core_instance_count", "2");

    format!(
        "resource \"aws_emr_cluster\" \"{id}\" {{\n  name          = {name}\n  release_label = {release_label}\n  applications  = {applications}\n\n  service_role = {service_role}\n\n  ec2_attributes {{\n    subnet_id                         = {subnet_id}\n    instance_profile                 = {instance_profile}\n  }}\n\n  master_instance_group {{\n    instance_type = {master_instance_type}\n  }}\n\n  core_instance_group {{\n    instance_type  = {core_instance_type}\n    instance_count = {core_instance_count}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "cluster_id" => Some(format!("aws_emr_cluster.{id}.id")),
        "cluster_arn" => Some(format!("aws_emr_cluster.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
