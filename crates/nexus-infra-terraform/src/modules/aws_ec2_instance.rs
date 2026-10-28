//! Plain `aws_instance` — the general-purpose compute building block
//! this catalog was missing (everything else assumed a managed service).
//! `iam_instance_profile` takes a profile *name*, not a role ARN — wire
//! it from a real `aws_iam_instance_profile` resource (not modeled as
//! its own catalog module yet; wrap an `aws-iam-role` output with one by
//! hand if needed).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Value for this instance's "Name" tag.
    name: String,
    ami: String,
    #[serde(default = "default_instance_type")]
    instance_type: String,
    /// Subnet id — wire from an `aws-vpc` node's `private_subnets` or
    /// `public_subnets` output (pick one element).
    subnet_id: String,
    vpc_security_group_ids: Vec<String>,
    #[serde(default)]
    key_name: Option<String>,
    #[serde(default)]
    associate_public_ip_address: bool,
    #[serde(default)]
    iam_instance_profile: Option<String>,
    /// Raw (non-base64) user-data script — Terraform base64-encodes it
    /// itself via `user_data`.
    #[serde(default)]
    user_data: Option<String>,
}

fn default_instance_type() -> String {
    "t3.micro".to_string()
}

submit_infra_module!(
    "aws-ec2-instance",
    "EC2 Instance",
    "compute",
    "aws",
    Config,
    &["id", "arn", "public_ip", "private_ip"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let ami = get("ami", "\"\"");
    let instance_type = get("instance_type", "\"t3.micro\"");
    let subnet_id = get("subnet_id", "\"\"");
    let vpc_security_group_ids = get("vpc_security_group_ids", "[]");
    let associate_public_ip_address = get("associate_public_ip_address", "false");

    let key_name_line = args
        .get("key_name")
        .map(|k| format!("\n  key_name = {k}"))
        .unwrap_or_default();
    let iam_profile_line = args
        .get("iam_instance_profile")
        .map(|p| format!("\n  iam_instance_profile = {p}"))
        .unwrap_or_default();
    let user_data_line = args
        .get("user_data")
        .map(|u| format!("\n  user_data = {u}"))
        .unwrap_or_default();

    format!(
        "resource \"aws_instance\" \"{id}\" {{\n  ami                         = {ami}\n  instance_type               = {instance_type}\n  subnet_id                   = {subnet_id}\n  vpc_security_group_ids      = {vpc_security_group_ids}\n  associate_public_ip_address = {associate_public_ip_address}{key_name_line}{iam_profile_line}{user_data_line}\n\n  tags = {{\n    Name = {name}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "arn" | "public_ip" | "private_ip" => Some(format!("aws_instance.{id}.{output}")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
