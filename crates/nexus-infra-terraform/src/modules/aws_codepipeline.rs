//! 3-stage `aws_codepipeline`: Source (`CodeStarSourceConnection`,
//! referencing an `aws-codestar-connection` node's `arn` output) → Build
//! (`CodeBuild`, referencing an `aws-codebuild-project` node's project
//! name) → Deploy (`ECS`, referencing an `aws-ecs-fargate-service` node's
//! cluster/service names). `configuration` blocks take plain strings
//! (`ProjectName`, `ClusterName`, `ServiceName` are CodePipeline
//! configuration keys, not resource references) — wire the *name* config
//! fields on those upstream nodes to the same literal value passed here,
//! since none of them expose a "name" graph output to wire automatically.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::{render_resource_block, ModuleImpl};

const RESOURCE_TYPE: &str = "aws_codepipeline";

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Pipeline name.
    name: String,
    /// ARN of the role CodePipeline assumes — wire from an `aws-iam-role`
    /// node's `iam_role_arn` output
    /// (`trusted_role_services: ["codepipeline.amazonaws.com"]`).
    role_arn: String,
    /// S3 bucket name CodePipeline stores stage artifacts in.
    artifact_bucket: String,
    /// ARN of the CodeStar connection — wire from an
    /// `aws-codestar-connection` node's `arn` output.
    connection_arn: String,
    /// "<owner>/<repo>" for the source stage.
    repository_id: String,
    #[serde(default = "default_branch_name")]
    branch_name: String,
    /// Name (not ARN) of the CodeBuild project run in the build stage.
    codebuild_project_name: String,
    /// Name (not ARN) of the ECS cluster targeted by the deploy stage.
    ecs_cluster_name: String,
    /// Name of the ECS service targeted by the deploy stage.
    ecs_service_name: String,
}

fn default_branch_name() -> String {
    "main".to_string()
}

submit_infra_module!(
    "aws-codepipeline",
    "CodePipeline (Source -> Build -> Deploy)",
    "cicd",
    "aws",
    Config,
    &["arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let role_arn = get("role_arn", "\"\"");
    let artifact_bucket = get("artifact_bucket", "\"\"");
    let connection_arn = get("connection_arn", "\"\"");
    let repository_id = get("repository_id", "\"\"");
    let branch_name = get("branch_name", "\"main\"");
    let codebuild_project_name = get("codebuild_project_name", "\"\"");
    let ecs_cluster_name = get("ecs_cluster_name", "\"\"");
    let ecs_service_name = get("ecs_service_name", "\"\"");

    let mut args = BTreeMap::new();
    args.insert("name".to_string(), name);
    args.insert("role_arn".to_string(), role_arn);

    let extra = format!(
        "\n  artifact_store {{\n    location = {artifact_bucket}\n    type     = \"S3\"\n  }}\n\n  stage {{\n    name = \"Source\"\n\n    action {{\n      name             = \"Source\"\n      category         = \"Source\"\n      owner            = \"AWS\"\n      provider         = \"CodeStarSourceConnection\"\n      version          = \"1\"\n      output_artifacts = [\"source_output\"]\n\n      configuration = {{\n        ConnectionArn    = {connection_arn}\n        FullRepositoryId = {repository_id}\n        BranchName       = {branch_name}\n      }}\n    }}\n  }}\n\n  stage {{\n    name = \"Build\"\n\n    action {{\n      name             = \"Build\"\n      category         = \"Build\"\n      owner            = \"AWS\"\n      provider         = \"CodeBuild\"\n      version          = \"1\"\n      input_artifacts  = [\"source_output\"]\n      output_artifacts = [\"build_output\"]\n\n      configuration = {{\n        ProjectName = {codebuild_project_name}\n      }}\n    }}\n  }}\n\n  stage {{\n    name = \"Deploy\"\n\n    action {{\n      name            = \"Deploy\"\n      category        = \"Deploy\"\n      owner           = \"AWS\"\n      provider        = \"ECS\"\n      version         = \"1\"\n      input_artifacts = [\"build_output\"]\n\n      configuration = {{\n        ClusterName = {ecs_cluster_name}\n        ServiceName = {ecs_service_name}\n      }}\n    }}\n  }}\n"
    );

    let mut block = render_resource_block(RESOURCE_TYPE, id, &args);
    block.truncate(block.len() - "}\n".len());
    block.push_str(&extra);
    block.push_str("}\n");
    block
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "arn" => Some(format!("{RESOURCE_TYPE}.{id}.arn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
