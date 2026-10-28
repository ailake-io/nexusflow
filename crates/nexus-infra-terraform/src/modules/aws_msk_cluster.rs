//! Plain `aws_msk_cluster` (provisioned mode) — no community module is
//! widely-adopted enough yet to pin to for MSK.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    cluster_name: String,
    #[serde(default = "default_kafka_version")]
    kafka_version: String,
    #[serde(default = "default_broker_count")]
    number_of_broker_nodes: u32,
    #[serde(default = "default_instance_type")]
    broker_instance_type: String,
    #[serde(default = "default_ebs_volume_size")]
    ebs_volume_size: u32,
    /// Subnet ids the broker ENIs land in — wire from an `aws-vpc` node's
    /// `private_subnets` output. Needs at least 2 across different AZs.
    subnet_ids: Vec<String>,
    security_group_ids: Vec<String>,
}

fn default_kafka_version() -> String {
    "3.6.0".to_string()
}
fn default_broker_count() -> u32 {
    3
}
fn default_instance_type() -> String {
    "kafka.m5.large".to_string()
}
fn default_ebs_volume_size() -> u32 {
    100
}

submit_infra_module!(
    "aws-msk-cluster",
    "MSK (Managed Kafka) Cluster",
    "data",
    "aws",
    Config,
    &["arn", "bootstrap_brokers_tls"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let cluster_name = get("cluster_name", &format!("\"{id}\""));
    let kafka_version = get("kafka_version", "\"3.6.0\"");
    let number_of_broker_nodes = get("number_of_broker_nodes", "3");
    let broker_instance_type = get("broker_instance_type", "\"kafka.m5.large\"");
    let ebs_volume_size = get("ebs_volume_size", "100");
    let subnet_ids = get("subnet_ids", "[]");
    let security_group_ids = get("security_group_ids", "[]");

    format!(
        "resource \"aws_msk_cluster\" \"{id}\" {{\n  cluster_name           = {cluster_name}\n  kafka_version          = {kafka_version}\n  number_of_broker_nodes = {number_of_broker_nodes}\n\n  broker_node_group_info {{\n    instance_type   = {broker_instance_type}\n    client_subnets  = {subnet_ids}\n    security_groups = {security_group_ids}\n\n    storage_info {{\n      ebs_storage_info {{\n        volume_size = {ebs_volume_size}\n      }}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("aws_msk_cluster.{id}.arn")),
        "bootstrap_brokers_tls" => Some(format!("aws_msk_cluster.{id}.bootstrap_brokers_tls")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
