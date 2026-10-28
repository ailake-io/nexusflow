//! 3-resource custom emit (cluster + task definition + service) — no
//! single community module covers this trio cleanly for a generic
//! container-image workload, so it's assembled directly from plain AWS
//! provider resources, same pattern as `aws_sagemaker_endpoint.rs`.
//!
//! `container_definitions` is built via a `jsonencode([...])` HCL
//! expression embedding the resolved `image`/`container_port` args
//! directly as object values inside the `jsonencode(...)` call — this is
//! syntactically safe whether those values are JSON literals already
//! turned into HCL or cross-module reference expressions (unlike plain
//! string interpolation, which broke `aws_sagemaker_endpoint.rs`'s first
//! draft — see that module's history).

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Service/task base name.
    name: String,
    /// Container image URI — wire from an `aws-ecr-repo` node's
    /// `repository_url` output (append your own `:tag`).
    image: String,
    /// Container port the app listens on.
    #[serde(default = "default_container_port")]
    container_port: u32,
    /// Task-level vCPU units (Fargate values: 256/512/1024/2048/4096).
    #[serde(default = "default_cpu")]
    cpu: String,
    /// Task-level memory in MB, must be a valid Fargate cpu/memory pair.
    #[serde(default = "default_memory")]
    memory: String,
    /// ARN of the task execution role (pulls the image, writes logs) —
    /// wire from an `aws-iam-role` node's `iam_role_arn` output
    /// (`trusted_role_services: ["ecs-tasks.amazonaws.com"]`).
    execution_role_arn: String,
    /// Subnet ids the service's ENIs land in — wire from an `aws-vpc`
    /// node's `private_subnets` output.
    subnet_ids: Vec<String>,
    /// Security group ids attached to the service's ENIs.
    security_group_ids: Vec<String>,
    /// Number of running task copies.
    #[serde(default = "default_desired_count")]
    desired_count: u32,
    /// Whether tasks get a public IP (only relevant in a public subnet).
    #[serde(default)]
    assign_public_ip: bool,
}

fn default_container_port() -> u32 {
    8080
}
fn default_cpu() -> String {
    "256".to_string()
}
fn default_memory() -> String {
    "512".to_string()
}
fn default_desired_count() -> u32 {
    1
}

submit_infra_module!(
    "aws-ecs-fargate-service",
    "ECS Fargate Service",
    "cicd",
    "aws",
    Config,
    &["cluster_arn", "service_name", "task_definition_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let image = get("image", "\"\"");
    let container_port = get("container_port", "8080");
    let cpu = get("cpu", "\"256\"");
    let memory = get("memory", "\"512\"");
    let execution_role_arn = get("execution_role_arn", "\"\"");
    let subnet_ids = get("subnet_ids", "[]");
    let security_group_ids = get("security_group_ids", "[]");
    let desired_count = get("desired_count", "1");
    let assign_public_ip = get("assign_public_ip", "false");

    let cluster_id = format!("{id}_cluster");
    let task_id = format!("{id}_task");
    let service_id = format!("{id}_service");

    format!(
        "resource \"aws_ecs_cluster\" \"{cluster_id}\" {{\n  name = \"{id}-cluster\"\n}}\n\nresource \"aws_ecs_task_definition\" \"{task_id}\" {{\n  family                   = \"{id}-task\"\n  requires_compatibilities = [\"FARGATE\"]\n  network_mode             = \"awsvpc\"\n  cpu                      = {cpu}\n  memory                   = {memory}\n  execution_role_arn       = {execution_role_arn}\n\n  container_definitions = jsonencode([\n    {{\n      name      = {name}\n      image     = {image}\n      essential = true\n      portMappings = [\n        {{\n          containerPort = {container_port}\n          hostPort      = {container_port}\n          protocol      = \"tcp\"\n        }}\n      ]\n    }}\n  ])\n}}\n\nresource \"aws_ecs_service\" \"{service_id}\" {{\n  name            = \"{id}-service\"\n  cluster         = aws_ecs_cluster.{cluster_id}.id\n  task_definition = aws_ecs_task_definition.{task_id}.arn\n  desired_count   = {desired_count}\n  launch_type     = \"FARGATE\"\n\n  network_configuration {{\n    subnets          = {subnet_ids}\n    security_groups  = {security_group_ids}\n    assign_public_ip = {assign_public_ip}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "cluster_arn" => Some(format!("aws_ecs_cluster.{id}_cluster.arn")),
        "service_name" => Some(format!("aws_ecs_service.{id}_service.name")),
        "task_definition_arn" => Some(format!("aws_ecs_task_definition.{id}_task.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
