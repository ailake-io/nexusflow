//! Plain `azurerm_kubernetes_cluster` — Azure analog of
//! `aws-eks-cluster`/`gcp-gke-cluster`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    resource_group_name: String,
    location: String,
    dns_prefix: String,
    #[serde(default = "default_node_count")]
    node_count: u32,
    #[serde(default = "default_vm_size")]
    vm_size: String,
}

fn default_node_count() -> u32 {
    2
}
fn default_vm_size() -> String {
    "Standard_D2s_v3".to_string()
}

submit_infra_module!(
    "azure-aks-cluster",
    "AKS Cluster",
    "compute",
    "azure",
    Config,
    &["id", "kube_config_raw", "fqdn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let dns_prefix = get("dns_prefix", &format!("\"{id}\""));
    let node_count = get("node_count", "2");
    let vm_size = get("vm_size", "\"Standard_D2s_v3\"");

    format!(
        "resource \"azurerm_kubernetes_cluster\" \"{id}\" {{\n  name                = {name}\n  resource_group_name = {resource_group_name}\n  location            = {location}\n  dns_prefix          = {dns_prefix}\n\n  default_node_pool {{\n    name       = \"default\"\n    node_count = {node_count}\n    vm_size    = {vm_size}\n  }}\n\n  identity {{\n    type = \"SystemAssigned\"\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "kube_config_raw" | "fqdn" => {
            Some(format!("azurerm_kubernetes_cluster.{id}.{output}"))
        }
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
