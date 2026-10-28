//! 5-resource custom emit: replication subnet group, replication
//! instance, source endpoint, target endpoint, replication task
//! (full-load-and-cdc by default) — the minimum real DMS setup, no
//! community module wraps all of this cleanly.
//!
//! **Credentials note**: `source_password`/`target_password` are plain
//! config fields for v1 simplicity. For anything beyond a disposable test
//! migration, replace them with a `SecretsManagerSecretId` +
//! `SecretsManagerAccessRoleArn` pair on the endpoint instead — not
//! modeled here yet.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    subnet_ids: Vec<String>,
    vpc_security_group_ids: Vec<String>,
    #[serde(default = "default_instance_class")]
    replication_instance_class: String,
    #[serde(default = "default_allocated_storage")]
    allocated_storage: u32,

    #[serde(default = "default_engine")]
    source_engine_name: String,
    source_server_name: String,
    #[serde(default = "default_port")]
    source_port: u32,
    source_database_name: String,
    source_username: String,
    source_password: String,

    #[serde(default = "default_engine")]
    target_engine_name: String,
    target_server_name: String,
    #[serde(default = "default_port")]
    target_port: u32,
    target_database_name: String,
    target_username: String,
    target_password: String,

    #[serde(default = "default_migration_type")]
    migration_type: String,
}

fn default_instance_class() -> String {
    "dms.t3.medium".to_string()
}
fn default_allocated_storage() -> u32 {
    50
}
fn default_engine() -> String {
    "postgres".to_string()
}
fn default_port() -> u32 {
    5432
}
fn default_migration_type() -> String {
    "full-load-and-cdc".to_string()
}

submit_infra_module!(
    "aws-dms-migration",
    "DMS Migration (instance + endpoints + task)",
    "data",
    "aws",
    Config,
    &["replication_instance_arn", "replication_task_arn"]
);

const DEFAULT_TABLE_MAPPINGS: &str = "jsonencode({\n    rules = [{\n      rule-type   = \"selection\"\n      rule-id     = \"1\"\n      rule-name   = \"1\"\n      object-locator = {\n        schema-name = \"%\"\n        table-name  = \"%\"\n      }\n      rule-action = \"include\"\n    }]\n  })";

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let subnet_ids = get("subnet_ids", "[]");
    let vpc_security_group_ids = get("vpc_security_group_ids", "[]");
    let replication_instance_class = get("replication_instance_class", "\"dms.t3.medium\"");
    let allocated_storage = get("allocated_storage", "50");

    let source_engine_name = get("source_engine_name", "\"postgres\"");
    let source_server_name = get("source_server_name", "\"\"");
    let source_port = get("source_port", "5432");
    let source_database_name = get("source_database_name", "\"\"");
    let source_username = get("source_username", "\"\"");
    let source_password = get("source_password", "\"\"");

    let target_engine_name = get("target_engine_name", "\"postgres\"");
    let target_server_name = get("target_server_name", "\"\"");
    let target_port = get("target_port", "5432");
    let target_database_name = get("target_database_name", "\"\"");
    let target_username = get("target_username", "\"\"");
    let target_password = get("target_password", "\"\"");

    let migration_type = get("migration_type", "\"full-load-and-cdc\"");

    let subnet_group_id = format!("{id}_subnet_group");
    let instance_id = format!("{id}_instance");
    let source_id = format!("{id}_source");
    let target_id = format!("{id}_target");

    format!(
        "resource \"aws_dms_replication_subnet_group\" \"{subnet_group_id}\" {{\n  replication_subnet_group_id          = \"{id}-subnet-group\"\n  replication_subnet_group_description = \"Subnet group for {id}\"\n  subnet_ids                           = {subnet_ids}\n}}\n\nresource \"aws_dms_replication_instance\" \"{instance_id}\" {{\n  replication_instance_id    = \"{id}-instance\"\n  replication_instance_class = {replication_instance_class}\n  allocated_storage          = {allocated_storage}\n  replication_subnet_group_id = aws_dms_replication_subnet_group.{subnet_group_id}.id\n  vpc_security_group_ids     = {vpc_security_group_ids}\n}}\n\nresource \"aws_dms_endpoint\" \"{source_id}\" {{\n  endpoint_id   = \"{id}-source\"\n  endpoint_type = \"source\"\n  engine_name   = {source_engine_name}\n  server_name   = {source_server_name}\n  port          = {source_port}\n  database_name = {source_database_name}\n  username      = {source_username}\n  password      = {source_password}\n}}\n\nresource \"aws_dms_endpoint\" \"{target_id}\" {{\n  endpoint_id   = \"{id}-target\"\n  endpoint_type = \"target\"\n  engine_name   = {target_engine_name}\n  server_name   = {target_server_name}\n  port          = {target_port}\n  database_name = {target_database_name}\n  username      = {target_username}\n  password      = {target_password}\n}}\n\nresource \"aws_dms_replication_task\" \"{id}\" {{\n  replication_task_id      = \"{id}-task\"\n  replication_instance_arn = aws_dms_replication_instance.{instance_id}.replication_instance_arn\n  source_endpoint_arn      = aws_dms_endpoint.{source_id}.endpoint_arn\n  target_endpoint_arn      = aws_dms_endpoint.{target_id}.endpoint_arn\n  migration_type           = {migration_type}\n  table_mappings           = {DEFAULT_TABLE_MAPPINGS}\n\n  lifecycle {{\n    ignore_changes = [table_mappings]\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "replication_instance_arn" => Some(format!(
            "aws_dms_replication_instance.{id}_instance.replication_instance_arn"
        )),
        "replication_task_arn" => Some(format!(
            "aws_dms_replication_task.{id}.replication_task_arn"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
