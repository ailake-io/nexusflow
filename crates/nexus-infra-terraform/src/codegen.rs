use crate::hcl;
use crate::modules;
use nexus_core::{GeneratedFiles, InfraGraph, InfraModuleDescriptor, NexusError};
use std::collections::{BTreeMap, HashMap, HashSet};

/// The single `InfraGenerator::generate` fn this crate registers
/// (`lib.rs`'s `submit_infra_generator!`). Emits one `.tf` file per node
/// plus a root `providers.tf` — deliberately no `variables.tf`/`outputs.tf`
/// yet (every value is inlined; a future iteration could promote node
/// config to real `variable` blocks for reuse across environments, but
/// that's more machinery than a v1 "generate code correctly, ready to
/// run" needs).
pub fn generate(graph: &InfraGraph) -> Result<GeneratedFiles, NexusError> {
    detect_cycle(graph)?;

    let node_by_id: HashMap<&str, &nexus_core::InfraNode> =
        graph.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let mut edges_by_target: HashMap<&str, Vec<&nexus_core::InfraEdge>> = HashMap::new();
    for e in &graph.edges {
        edges_by_target.entry(e.to.as_str()).or_default().push(e);
    }

    let mut files = BTreeMap::new();

    for node in &graph.nodes {
        let module_impl = modules::dispatch(&node.module).ok_or_else(|| {
            NexusError::Connector(format!("unknown infra module: {}", node.module))
        })?;

        // Literal config values first, as HCL, keyed by their JSON field
        // name (already chosen per-module to match the real Terraform
        // variable/argument name — see modules/mod.rs's doc comment).
        let mut args: BTreeMap<String, String> = node
            .config
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.clone(), hcl::literal(v)))
            .collect();

        // Edge-wired inputs override the literal value for that field —
        // a real dependency reference (`module.<from>.<output>` or
        // `aws_x.<from>.<output>`) instead of whatever literal the config
        // held (typically empty/placeholder for a field meant to be wired).
        for edge in edges_by_target.get(node.id.as_str()).into_iter().flatten() {
            let source = node_by_id.get(edge.from.as_str()).ok_or_else(|| {
                NexusError::Connector(format!(
                    "edge on node '{}' references unknown source node '{}'",
                    node.id, edge.from
                ))
            })?;
            let source_impl = modules::dispatch(&source.module).ok_or_else(|| {
                NexusError::Connector(format!("unknown infra module: {}", source.module))
            })?;
            let reference =
                (source_impl.output_ref)(&edge.output, &edge.from).ok_or_else(|| {
                    NexusError::Connector(format!(
                        "module '{}' (node '{}') has no output named '{}'",
                        source.module, edge.from, edge.output
                    ))
                })?;
            args.insert(edge.input.clone(), reference);
        }

        let block = (module_impl.emit)(&args, &node.id);
        files.insert(format!("{}.tf", node.id), block);
    }

    files.insert("providers.tf".to_string(), render_providers(graph));

    Ok(GeneratedFiles { files })
}

/// Emits one `required_providers` entry + one `provider` block per
/// distinct cloud actually used in the graph (found via each used
/// module's registered `InfraModuleDescriptor.provider`, not a hardcoded
/// list) — a graph with only AWS nodes gets only an `aws` block, one
/// mixing AWS, GCP, and Azure nodes gets all three. Reads settings from
/// `graph.provider["<cloud>"]` rather than hardcoding just `region` — a
/// `profile`/`assume_role` (AWS) or extra key the user adds later works
/// without a crate change. Unknown-module errors are already caught by
/// `generate` before this runs, so every node here resolves.
fn render_providers(graph: &InfraGraph) -> String {
    let mut providers: Vec<&str> = graph
        .nodes
        .iter()
        .filter_map(|n| InfraModuleDescriptor::find(&n.module))
        .map(|d| d.provider)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    providers.sort_unstable();
    if providers.is_empty() {
        providers.push("aws");
    }

    let mut required = String::new();
    let mut blocks = String::new();
    for provider in providers {
        let settings = graph.provider.get(provider).cloned().unwrap_or_default();
        match provider {
            "gcp" => {
                let project = settings
                    .get("project")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let region = settings
                    .get("region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("us-central1");
                required.push_str(
                    "    google = {\n      source  = \"hashicorp/google\"\n      version = \"~> 6.0\"\n    }\n",
                );
                blocks.push_str(&format!(
                    "provider \"google\" {{\n  project = \"{project}\"\n  region  = \"{region}\"\n}}\n\n"
                ));
            }
            "azure" => {
                required.push_str(
                    "    azurerm = {\n      source  = \"hashicorp/azurerm\"\n      version = \"~> 4.0\"\n    }\n",
                );
                blocks.push_str("provider \"azurerm\" {\n  features {}\n}\n\n");
            }
            _ => {
                let region = settings
                    .get("region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("us-east-1");
                required.push_str(
                    "    aws = {\n      source  = \"hashicorp/aws\"\n      version = \"~> 5.0\"\n    }\n",
                );
                blocks.push_str(&format!(
                    "provider \"aws\" {{\n  region = \"{region}\"\n}}\n\n"
                ));
            }
        }
    }

    format!("terraform {{\n  required_providers {{\n{required}  }}\n}}\n\n{blocks}")
}

/// DFS cycle check over the edge graph — a real Terraform cycle (module A
/// referencing module B's output while B also references A's) fails with a
/// confusing Terraform-side error if we don't catch it first; this gives a
/// caller a clear "which nodes" message instead.
fn detect_cycle(graph: &InfraGraph) -> Result<(), NexusError> {
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for e in &graph.edges {
        adjacency
            .entry(e.from.as_str())
            .or_default()
            .push(e.to.as_str());
    }

    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Visiting,
        Done,
    }
    let mut state: HashMap<&str, State> = HashMap::new();

    fn visit<'a>(
        node: &'a str,
        adjacency: &HashMap<&'a str, Vec<&'a str>>,
        state: &mut HashMap<&'a str, State>,
        stack: &mut Vec<&'a str>,
    ) -> Result<(), NexusError> {
        match state.get(node) {
            Some(State::Done) => return Ok(()),
            Some(State::Visiting) => {
                stack.push(node);
                return Err(NexusError::Connector(format!(
                    "cycle detected in infra graph: {}",
                    stack.join(" -> ")
                )));
            }
            None => {}
        }
        state.insert(node, State::Visiting);
        stack.push(node);
        for &next in adjacency.get(node).into_iter().flatten() {
            visit(next, adjacency, state, stack)?;
        }
        stack.pop();
        state.insert(node, State::Done);
        Ok(())
    }

    let all_ids: HashSet<&str> = graph.nodes.iter().map(|n| n.id.as_str()).collect();
    for &id in &all_ids {
        if !state.contains_key(id) {
            let mut stack = Vec::new();
            visit(id, &adjacency, &mut state, &mut stack)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_core::{InfraEdge, InfraNode};
    use serde_json::json;

    fn node(id: &str, module: &str, config: serde_json::Value) -> InfraNode {
        InfraNode {
            id: id.to_string(),
            module: module.to_string(),
            config,
        }
    }

    #[test]
    fn generates_wired_reference_not_literal() {
        let graph = InfraGraph {
            nodes: vec![
                node(
                    "vpc1",
                    "aws-vpc",
                    json!({"name": "test", "cidr": "10.0.0.0/16", "azs": ["us-east-1a"], "private_subnets": ["10.0.1.0/24"], "public_subnets": ["10.0.101.0/24"]}),
                ),
                node("ecr1", "aws-ecr-repo", json!({"repository_name": "app"})),
            ],
            edges: vec![],
            provider: json!({}),
        };
        let result = generate(&graph).expect("generate should succeed");
        assert!(result.files.contains_key("vpc1.tf"));
        assert!(result.files.contains_key("ecr1.tf"));
        assert!(result.files.contains_key("providers.tf"));
        assert!(result.files["vpc1.tf"].contains("terraform-aws-modules/vpc/aws"));
    }

    #[test]
    fn wires_edge_as_reference_expression() {
        let graph = InfraGraph {
            nodes: vec![
                node(
                    "role1",
                    "aws-iam-role",
                    json!({"role_name": "r", "trusted_role_services": ["sagemaker.amazonaws.com"]}),
                ),
                node(
                    "ep1",
                    "aws-sagemaker-endpoint",
                    json!({"name": "ep", "image": "x"}),
                ),
            ],
            edges: vec![InfraEdge {
                from: "role1".to_string(),
                to: "ep1".to_string(),
                output: "iam_role_arn".to_string(),
                input: "execution_role_arn".to_string(),
            }],
            provider: json!({}),
        };
        let result = generate(&graph).expect("generate should succeed");
        assert!(result.files["ep1.tf"].contains("module.role1.iam_role_arn"));
    }

    #[test]
    fn detects_cycle() {
        let graph = InfraGraph {
            nodes: vec![
                node("a", "aws-vpc", json!({})),
                node("b", "aws-vpc", json!({})),
            ],
            edges: vec![
                InfraEdge {
                    from: "a".to_string(),
                    to: "b".to_string(),
                    output: "vpc_id".to_string(),
                    input: "x".to_string(),
                },
                InfraEdge {
                    from: "b".to_string(),
                    to: "a".to_string(),
                    output: "vpc_id".to_string(),
                    input: "y".to_string(),
                },
            ],
            provider: json!({}),
        };
        assert!(generate(&graph).is_err());
    }

    #[test]
    fn renders_a_provider_block_per_cloud_actually_used() {
        let graph = InfraGraph {
            nodes: vec![
                node("ecr1", "aws-ecr-repo", json!({"repository_name": "app"})),
                node(
                    "gcs1",
                    "gcp-gcs-bucket",
                    json!({"name": "bkt", "project": "p"}),
                ),
                node(
                    "acr1",
                    "azure-container-registry",
                    json!({"name": "acr", "resource_group_name": "rg", "location": "eastus"}),
                ),
            ],
            edges: vec![],
            provider: json!({"gcp": {"project": "p", "region": "us-central1"}, "azure": {}}),
        };
        let result = generate(&graph).expect("generate should succeed");
        let providers = &result.files["providers.tf"];
        assert!(providers.contains("hashicorp/aws"));
        assert!(providers.contains("hashicorp/google"));
        assert!(providers.contains("hashicorp/azurerm"));
        assert!(providers.contains("provider \"aws\""));
        assert!(providers.contains("provider \"google\""));
        assert!(providers.contains("provider \"azurerm\""));
    }

    #[test]
    fn unknown_module_is_a_clear_error() {
        let graph = InfraGraph {
            nodes: vec![node("x", "does-not-exist", json!({}))],
            edges: vec![],
            provider: json!({}),
        };
        let err = generate(&graph).unwrap_err();
        assert!(err.to_string().contains("does-not-exist"));
    }
}
