//! One file per curated module. Each registers an `InfraModuleDescriptor`
//! (via `nexus_core::submit_infra_module!`, so `GET /infra/modules` lists
//! it) and exposes a `pub(crate) const IMPL: ModuleImpl` this crate's
//! `codegen.rs` dispatches to by module id — kept as a plain `match` here
//! rather than another `inventory` registry since this dispatch is purely
//! internal to this crate (nexus-server never needs it).
//!
//! **Wrapper strategy** (2026-09-10 decision, see the planning doc): every
//! module here either (a) wraps a well-established public
//! `terraform-aws-modules` registry module — a pinned `source`/`version`,
//! this crate never vendors its own copy of that module's HCL — or (b)
//! emits plain AWS provider `resource` block(s) directly, for the handful
//! of services (Glue, Athena, CodeBuild, SageMaker) that don't have one
//! universally-agreed-on community module worth pinning to. Either way,
//! config field names are chosen to match the real Terraform variable/
//! argument names of whatever's being wrapped or emitted — not our own
//! invented names — so a config value serializes straight into a valid HCL
//! argument with zero translation layer to get wrong.

mod aws_athena_workgroup;
mod aws_aurora_serverless;
mod aws_bedrock_knowledge_base;
mod aws_codebuild_project;
mod aws_codepipeline;
mod aws_codestar_connection;
mod aws_dms_migration;
mod aws_dynamodb_table;
mod aws_ec2_instance;
mod aws_ecr_repo;
mod aws_ecs_fargate_service;
mod aws_eks_cluster;
mod aws_emr_cluster;
mod aws_eventbridge_rule;
mod aws_glue_catalog;
mod aws_glue_crawler;
mod aws_glue_job;
mod aws_iam_role;
mod aws_kinesis_firehose;
mod aws_kinesis_stream;
mod aws_lakeformation_registration;
mod aws_lambda_function;
mod aws_msk_cluster;
mod aws_mwaa_environment;
mod aws_quicksight_data_source;
mod aws_rds_postgres;
mod aws_redshift_serverless;
mod aws_s3_data_lake;
mod aws_s3_tables_bucket;
mod aws_sagemaker_endpoint;
mod aws_sagemaker_feature_group;
mod aws_sagemaker_model_package_group;
mod aws_sagemaker_notebook;
mod aws_sagemaker_pipeline;
mod aws_sagemaker_studio_domain;
mod aws_sqs_queue;
mod aws_stepfunctions_state_machine;
mod aws_vpc;

mod azure_aks_cluster;
mod azure_batch_account;
mod azure_cognitive_search;
mod azure_container_app;
mod azure_container_registry;
mod azure_cosmosdb_account;
mod azure_data_factory;
mod azure_databricks_workspace;
mod azure_event_grid_topic;
mod azure_event_hub;
mod azure_function_app;
mod azure_logic_app;
mod azure_managed_identity;
mod azure_ml_compute_instance;
mod azure_ml_workspace;
mod azure_purview_account;
mod azure_service_bus_queue;
mod azure_sql_database;
mod azure_storage_account;
mod azure_stream_analytics_job;
mod azure_synapse_workspace;
mod azure_virtual_machine;
mod azure_vnet;

mod gcp_artifact_registry_repo;
mod gcp_bigquery_dataset;
mod gcp_bigquery_table;
mod gcp_bigtable_instance;
mod gcp_cloud_build_trigger;
mod gcp_cloud_functions;
mod gcp_cloud_run_service;
mod gcp_cloud_scheduler_job;
mod gcp_cloud_sql_postgres;
mod gcp_cloud_tasks_queue;
mod gcp_composer_environment;
mod gcp_compute_instance;
mod gcp_dataflow_job;
mod gcp_dataproc_cluster;
mod gcp_firestore_database;
mod gcp_gcs_bucket;
mod gcp_gke_cluster;
mod gcp_pubsub_topic;
mod gcp_service_account;
mod gcp_spanner_instance;
mod gcp_vertex_ai_dataset;
mod gcp_vertex_ai_endpoint;
mod gcp_vertex_ai_featurestore;
mod gcp_vertex_ai_workbench;
mod gcp_vpc_network;
mod gcp_workflows;

use std::collections::BTreeMap;

/// `emit`: renders this node's full HCL block(s) given its resolved
/// arguments (literal JSON values already turned into HCL, edge-wired
/// inputs already turned into `module.<id>.<output>`-style reference
/// strings — see `codegen.rs`) and its own node id (the HCL block's local
/// name). `output_ref`: given one of this module's declared output names
/// and this node's id, the HCL expression a downstream module references
/// it by — `None` for a name that isn't actually one of this module's
/// outputs (a config/graph error, surfaced to the caller as a clear
/// message rather than generating silently-wrong HCL).
pub(crate) struct ModuleImpl {
    pub emit: fn(args: &BTreeMap<String, String>, id: &str) -> String,
    pub output_ref: fn(output: &str, id: &str) -> Option<String>,
}

pub(crate) fn dispatch(module_id: &str) -> Option<&'static ModuleImpl> {
    match module_id {
        "aws-vpc" => Some(&aws_vpc::IMPL),
        "aws-iam-role" => Some(&aws_iam_role::IMPL),
        "aws-s3-data-lake" => Some(&aws_s3_data_lake::IMPL),
        "aws-ecr-repo" => Some(&aws_ecr_repo::IMPL),
        "aws-glue-catalog" => Some(&aws_glue_catalog::IMPL),
        "aws-athena-workgroup" => Some(&aws_athena_workgroup::IMPL),
        "aws-codebuild-project" => Some(&aws_codebuild_project::IMPL),
        "aws-sagemaker-endpoint" => Some(&aws_sagemaker_endpoint::IMPL),
        "aws-sagemaker-notebook" => Some(&aws_sagemaker_notebook::IMPL),
        "aws-lambda-function" => Some(&aws_lambda_function::IMPL),
        "aws-bedrock-knowledge-base" => Some(&aws_bedrock_knowledge_base::IMPL),
        "aws-codestar-connection" => Some(&aws_codestar_connection::IMPL),
        "aws-ecs-fargate-service" => Some(&aws_ecs_fargate_service::IMPL),
        "aws-codepipeline" => Some(&aws_codepipeline::IMPL),
        "aws-rds-postgres" => Some(&aws_rds_postgres::IMPL),
        "aws-dynamodb-table" => Some(&aws_dynamodb_table::IMPL),
        "aws-kinesis-stream" => Some(&aws_kinesis_stream::IMPL),
        "aws-redshift-serverless" => Some(&aws_redshift_serverless::IMPL),
        "aws-emr-cluster" => Some(&aws_emr_cluster::IMPL),
        "aws-msk-cluster" => Some(&aws_msk_cluster::IMPL),
        "aws-aurora-serverless" => Some(&aws_aurora_serverless::IMPL),
        "aws-dms-migration" => Some(&aws_dms_migration::IMPL),
        "aws-lakeformation-registration" => Some(&aws_lakeformation_registration::IMPL),
        "aws-quicksight-data-source" => Some(&aws_quicksight_data_source::IMPL),
        "aws-stepfunctions-state-machine" => Some(&aws_stepfunctions_state_machine::IMPL),
        "aws-sagemaker-feature-group" => Some(&aws_sagemaker_feature_group::IMPL),
        "aws-sagemaker-model-package-group" => Some(&aws_sagemaker_model_package_group::IMPL),
        "aws-sagemaker-pipeline" => Some(&aws_sagemaker_pipeline::IMPL),
        "aws-sagemaker-studio-domain" => Some(&aws_sagemaker_studio_domain::IMPL),
        "aws-glue-job" => Some(&aws_glue_job::IMPL),
        "aws-glue-crawler" => Some(&aws_glue_crawler::IMPL),
        "aws-s3-tables-bucket" => Some(&aws_s3_tables_bucket::IMPL),
        "aws-kinesis-firehose" => Some(&aws_kinesis_firehose::IMPL),
        "aws-sqs-queue" => Some(&aws_sqs_queue::IMPL),
        "aws-eventbridge-rule" => Some(&aws_eventbridge_rule::IMPL),
        "aws-mwaa-environment" => Some(&aws_mwaa_environment::IMPL),
        "gcp-vpc-network" => Some(&gcp_vpc_network::IMPL),
        "gcp-service-account" => Some(&gcp_service_account::IMPL),
        "gcp-gcs-bucket" => Some(&gcp_gcs_bucket::IMPL),
        "gcp-bigquery-dataset" => Some(&gcp_bigquery_dataset::IMPL),
        "gcp-bigquery-table" => Some(&gcp_bigquery_table::IMPL),
        "gcp-cloud-sql-postgres" => Some(&gcp_cloud_sql_postgres::IMPL),
        "gcp-pubsub-topic" => Some(&gcp_pubsub_topic::IMPL),
        "gcp-dataproc-cluster" => Some(&gcp_dataproc_cluster::IMPL),
        "gcp-composer-environment" => Some(&gcp_composer_environment::IMPL),
        "gcp-bigtable-instance" => Some(&gcp_bigtable_instance::IMPL),
        "gcp-vertex-ai-workbench" => Some(&gcp_vertex_ai_workbench::IMPL),
        "gcp-vertex-ai-endpoint" => Some(&gcp_vertex_ai_endpoint::IMPL),
        "gcp-vertex-ai-featurestore" => Some(&gcp_vertex_ai_featurestore::IMPL),
        "gcp-vertex-ai-dataset" => Some(&gcp_vertex_ai_dataset::IMPL),
        "gcp-artifact-registry-repo" => Some(&gcp_artifact_registry_repo::IMPL),
        "gcp-cloud-build-trigger" => Some(&gcp_cloud_build_trigger::IMPL),
        "gcp-cloud-run-service" => Some(&gcp_cloud_run_service::IMPL),
        "gcp-firestore-database" => Some(&gcp_firestore_database::IMPL),
        "azure-vnet" => Some(&azure_vnet::IMPL),
        "azure-managed-identity" => Some(&azure_managed_identity::IMPL),
        "azure-storage-account" => Some(&azure_storage_account::IMPL),
        "azure-sql-database" => Some(&azure_sql_database::IMPL),
        "azure-cosmosdb-account" => Some(&azure_cosmosdb_account::IMPL),
        "azure-event-hub" => Some(&azure_event_hub::IMPL),
        "azure-data-factory" => Some(&azure_data_factory::IMPL),
        "azure-databricks-workspace" => Some(&azure_databricks_workspace::IMPL),
        "azure-service-bus-queue" => Some(&azure_service_bus_queue::IMPL),
        "azure-ml-workspace" => Some(&azure_ml_workspace::IMPL),
        "azure-ml-compute-instance" => Some(&azure_ml_compute_instance::IMPL),
        "azure-cognitive-search" => Some(&azure_cognitive_search::IMPL),
        "azure-container-registry" => Some(&azure_container_registry::IMPL),
        "azure-container-app" => Some(&azure_container_app::IMPL),
        "azure-function-app" => Some(&azure_function_app::IMPL),
        "aws-ec2-instance" => Some(&aws_ec2_instance::IMPL),
        "gcp-compute-instance" => Some(&gcp_compute_instance::IMPL),
        "gcp-cloud-functions" => Some(&gcp_cloud_functions::IMPL),
        "gcp-dataflow-job" => Some(&gcp_dataflow_job::IMPL),
        "gcp-cloud-scheduler-job" => Some(&gcp_cloud_scheduler_job::IMPL),
        "gcp-spanner-instance" => Some(&gcp_spanner_instance::IMPL),
        "azure-virtual-machine" => Some(&azure_virtual_machine::IMPL),
        "azure-synapse-workspace" => Some(&azure_synapse_workspace::IMPL),
        "azure-stream-analytics-job" => Some(&azure_stream_analytics_job::IMPL),
        "azure-purview-account" => Some(&azure_purview_account::IMPL),
        "azure-logic-app" => Some(&azure_logic_app::IMPL),
        "gcp-workflows" => Some(&gcp_workflows::IMPL),
        "gcp-cloud-tasks-queue" => Some(&gcp_cloud_tasks_queue::IMPL),
        "azure-event-grid-topic" => Some(&azure_event_grid_topic::IMPL),
        "azure-batch-account" => Some(&azure_batch_account::IMPL),
        "aws-eks-cluster" => Some(&aws_eks_cluster::IMPL),
        "gcp-gke-cluster" => Some(&gcp_gke_cluster::IMPL),
        "azure-aks-cluster" => Some(&azure_aks_cluster::IMPL),
        _ => None,
    }
}

/// Shared by every `emit` fn below — renders `module "<id>" { source =
/// ...; version = ...; <args> }`.
pub(crate) fn render_module_block(
    id: &str,
    source: &str,
    version: &str,
    args: &BTreeMap<String, String>,
) -> String {
    let mut s =
        format!("module \"{id}\" {{\n  source  = \"{source}\"\n  version = \"{version}\"\n");
    for (k, v) in args {
        s.push_str(&format!("  {k} = {v}\n"));
    }
    s.push_str("}\n");
    s
}

/// Shared by every `emit` fn that isn't a module wrapper — renders
/// `resource "<type>" "<id>" { <args> }`.
pub(crate) fn render_resource_block(
    resource_type: &str,
    id: &str,
    args: &BTreeMap<String, String>,
) -> String {
    let mut s = format!("resource \"{resource_type}\" \"{id}\" {{\n");
    for (k, v) in args {
        s.push_str(&format!("  {k} = {v}\n"));
    }
    s.push_str("}\n");
    s
}
