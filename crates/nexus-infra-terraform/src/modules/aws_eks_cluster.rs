//! Wraps `terraform-aws-modules/eks/aws` (the de facto standard
//! community EKS module) with one default managed node group — the
//! module's own `eks_managed_node_groups` map takes an arbitrary set of
//! named groups, but this catalog only exposes the common single-group
//! shape as flat config fields; add more groups by editing the generated
//! `.tf` directly if needed.

use super::{render_module_block, ModuleImpl};
use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

const SOURCE: &str = "terraform-aws-modules/eks/aws";
const VERSION: &str = "~> 20.0";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    cluster_name: String,
    #[serde(default = "default_cluster_version")]
    cluster_version: String,
    vpc_id: String,
    /// Subnet ids for both the control plane ENIs and worker nodes —
    /// wire from an `aws-vpc` node's `private_subnets` output.
    subnet_ids: Vec<String>,
    #[serde(default = "default_endpoint_public_access")]
    cluster_endpoint_public_access: bool,
    #[serde(default = "default_node_instance_types")]
    node_instance_types: Vec<String>,
    #[serde(default = "default_min_size")]
    node_min_size: u32,
    #[serde(default = "default_max_size")]
    node_max_size: u32,
    #[serde(default = "default_desired_size")]
    node_desired_size: u32,
}

fn default_cluster_version() -> String {
    "1.31".to_string()
}
fn default_endpoint_public_access() -> bool {
    true
}
fn default_node_instance_types() -> Vec<String> {
    vec!["t3.medium".to_string()]
}
fn default_min_size() -> u32 {
    1
}
fn default_max_size() -> u32 {
    3
}
fn default_desired_size() -> u32 {
    2
}

submit_infra_module!(
    "aws-eks-cluster",
    "EKS Cluster",
    "compute",
    "aws",
    Config,
    &[
        "cluster_endpoint",
        "cluster_name",
        "cluster_certificate_authority_data",
        "oidc_provider_arn"
    ]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let instance_types = args
        .get("node_instance_types")
        .cloned()
        .unwrap_or_else(|| "[\"t3.medium\"]".to_string());
    let min_size = args
        .get("node_min_size")
        .cloned()
        .unwrap_or_else(|| "1".to_string());
    let max_size = args
        .get("node_max_size")
        .cloned()
        .unwrap_or_else(|| "3".to_string());
    let desired_size = args
        .get("node_desired_size")
        .cloned()
        .unwrap_or_else(|| "2".to_string());

    let mut args = args.clone();
    args.remove("node_instance_types");
    args.remove("node_min_size");
    args.remove("node_max_size");
    args.remove("node_desired_size");
    args.insert(
        "eks_managed_node_groups".to_string(),
        format!(
            "{{\n    default = {{\n      instance_types = {instance_types}\n      min_size       = {min_size}\n      max_size       = {max_size}\n      desired_size   = {desired_size}\n    }}\n  }}"
        ),
    );

    render_module_block(id, SOURCE, VERSION, &args)
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "cluster_endpoint"
        | "cluster_name"
        | "cluster_certificate_authority_data"
        | "oidc_provider_arn" => Some(format!("module.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
