//! 2-resource custom emit: `azurerm_container_app_environment` +
//! `azurerm_container_app` — Azure analog of `aws-ecs-fargate-service`/
//! `gcp-cloud-run-service`.

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
    /// Container image URI — wire from an `azure-container-registry`
    /// node's `login_server` output and append your own `:tag`.
    image: String,
    #[serde(default = "default_cpu")]
    cpu: f64,
    #[serde(default = "default_memory")]
    memory: String,
    #[serde(default = "default_container_port")]
    container_port: u32,
    #[serde(default)]
    min_replicas: u32,
    #[serde(default = "default_max_replicas")]
    max_replicas: u32,
}

fn default_cpu() -> f64 {
    0.5
}
fn default_memory() -> String {
    "1Gi".to_string()
}
fn default_container_port() -> u32 {
    8080
}
fn default_max_replicas() -> u32 {
    3
}

submit_infra_module!(
    "azure-container-app",
    "Container App",
    "cicd",
    "azure",
    Config,
    &["environment_id", "fqdn"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let resource_group_name = get("resource_group_name", "\"\"");
    let location = get("location", "\"\"");
    let image = get("image", "\"\"");
    let cpu = get("cpu", "0.5");
    let memory = get("memory", "\"1Gi\"");
    let container_port = get("container_port", "8080");
    let min_replicas = get("min_replicas", "0");
    let max_replicas = get("max_replicas", "3");

    let env_id = format!("{id}_env");

    format!(
        "resource \"azurerm_container_app_environment\" \"{env_id}\" {{\n  name                = \"{id}-env\"\n  resource_group_name = {resource_group_name}\n  location            = {location}\n}}\n\nresource \"azurerm_container_app\" \"{id}\" {{\n  name                         = {name}\n  resource_group_name          = {resource_group_name}\n  container_app_environment_id = azurerm_container_app_environment.{env_id}.id\n  revision_mode                = \"Single\"\n\n  template {{\n    min_replicas = {min_replicas}\n    max_replicas = {max_replicas}\n\n    container {{\n      name   = {name}\n      image  = {image}\n      cpu    = {cpu}\n      memory = {memory}\n    }}\n  }}\n\n  ingress {{\n    external_enabled = true\n    target_port      = {container_port}\n\n    traffic_weight {{\n      latest_revision = true\n      percentage      = 100\n    }}\n  }}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "environment_id" => Some(format!("azurerm_container_app_environment.{id}_env.id")),
        "fqdn" => Some(format!("azurerm_container_app.{id}.latest_revision_fqdn")),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
