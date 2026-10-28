//! Bedrock Knowledge Base backed by an OpenSearch Serverless vector
//! collection. No community module wraps this combination yet (both
//! services are recent) — emits the real AWS provider resources directly:
//! `aws_opensearchserverless_security_policy` (x2: encryption + network,
//! both required before a collection can be created),
//! `aws_opensearchserverless_collection`, and
//! `aws_bedrockagent_knowledge_base`.
//!
//! **Real limitation, documented rather than hidden**: Terraform's AWS
//! provider creates the OpenSearch Serverless *collection* but has no
//! resource for creating a *vector index inside it* — that's an OpenSearch
//! API call, not a plain AWS API call (would need the separate `opensearch`
//! Terraform provider or a manual `curl`/script step, out of scope for an
//! AWS-provider-only v1). The generated `aws_bedrockagent_knowledge_base`
//! resource references an index name that must exist in the collection
//! *before* this applies cleanly — create it once via the OpenSearch
//! Dashboards console or the AWS CLI (`aws opensearchserverless` +
//! `curl` against the collection endpoint) after the collection resource
//! is created, before applying the rest.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    /// Base name — the collection, security policies, and knowledge base
    /// are all named off this.
    name: String,
    /// ARN of the role Bedrock assumes to read the collection and call
    /// the embedding model — wire from an `aws-iam-role` node's
    /// `iam_role_arn` output
    /// (`trusted_role_services: ["bedrock.amazonaws.com"]`).
    role_arn: String,
    /// Embedding model ARN, e.g.
    /// "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0".
    embedding_model_arn: String,
    /// Name of the vector index inside the collection — must already
    /// exist (see this module's doc comment) before Bedrock can use it.
    #[serde(default = "default_index_name")]
    vector_index_name: String,
}

fn default_index_name() -> String {
    "bedrock-knowledge-base-default-index".to_string()
}

submit_infra_module!(
    "aws-bedrock-knowledge-base",
    "Bedrock Knowledge Base",
    "ai",
    "aws",
    Config,
    &["knowledge_base_id", "knowledge_base_arn", "collection_arn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let role_arn = get("role_arn", "\"\"");
    let embedding_model_arn = get("embedding_model_arn", "\"\"");
    let vector_index_name = get(
        "vector_index_name",
        "\"bedrock-knowledge-base-default-index\"",
    );

    let encryption_policy_id = format!("{id}_encryption_policy");
    let network_policy_id = format!("{id}_network_policy");
    let collection_id = format!("{id}_collection");

    format!(
        "resource \"aws_opensearchserverless_security_policy\" \"{encryption_policy_id}\" {{\n  name = \"{id}-enc\"\n  type = \"encryption\"\n  policy = jsonencode({{\n    Rules = [{{\n      ResourceType = \"collection\"\n      Resource     = [\"collection/{id}-*\"]\n    }}]\n    AWSOwnedKey = true\n  }})\n}}\n\nresource \"aws_opensearchserverless_security_policy\" \"{network_policy_id}\" {{\n  name = \"{id}-net\"\n  type = \"network\"\n  policy = jsonencode([{{\n    Rules = [{{\n      ResourceType = \"collection\"\n      Resource     = [\"collection/{id}-*\"]\n    }}]\n    AllowFromPublic = true\n  }}])\n}}\n\nresource \"aws_opensearchserverless_collection\" \"{collection_id}\" {{\n  name = \"{id}-vectors\"\n  type = \"VECTORSEARCH\"\n\n  depends_on = [\n    aws_opensearchserverless_security_policy.{encryption_policy_id},\n    aws_opensearchserverless_security_policy.{network_policy_id},\n  ]\n}}\n\nresource \"aws_bedrockagent_knowledge_base\" \"{id}\" {{\n  name     = {name}\n  role_arn = {role_arn}\n\n  knowledge_base_configuration {{\n    type = \"VECTOR\"\n    vector_knowledge_base_configuration {{\n      embedding_model_arn = {embedding_model_arn}\n    }}\n  }}\n\n  storage_configuration {{\n    type = \"OPENSEARCH_SERVERLESS\"\n    opensearch_serverless_configuration {{\n      collection_arn    = aws_opensearchserverless_collection.{collection_id}.arn\n      vector_index_name = {vector_index_name}\n      field_mapping {{\n        vector_field   = \"bedrock-knowledge-base-default-vector\"\n        text_field     = \"AMAZON_BEDROCK_TEXT_CHUNK\"\n        metadata_field = \"AMAZON_BEDROCK_METADATA\"\n      }}\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "knowledge_base_id" => Some(format!("aws_bedrockagent_knowledge_base.{id}.id")),
        "knowledge_base_arn" => Some(format!("aws_bedrockagent_knowledge_base.{id}.arn")),
        "collection_arn" => Some(format!(
            "aws_opensearchserverless_collection.{id}_collection.arn"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
