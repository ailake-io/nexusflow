//! Plain `google_compute_instance` — GCP analog of `aws-ec2-instance`.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    name: String,
    project: String,
    #[serde(default = "default_zone")]
    zone: String,
    #[serde(default = "default_machine_type")]
    machine_type: String,
    /// Boot disk source image, e.g. "debian-cloud/debian-12".
    image: String,
    /// Subnetwork self-link — wire from a `gcp-vpc-network` node's
    /// `subnets_ids` output (pick one element).
    subnetwork: String,
    #[serde(default)]
    service_account_email: Option<String>,
}

fn default_zone() -> String {
    "us-central1-a".to_string()
}
fn default_machine_type() -> String {
    "e2-medium".to_string()
}

submit_infra_module!(
    "gcp-compute-instance",
    "Compute Engine Instance",
    "compute",
    "gcp",
    Config,
    &["id", "self_link", "network_ip"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let name = get("name", &format!("\"{id}\""));
    let project = get("project", "\"\"");
    let zone = get("zone", "\"us-central1-a\"");
    let machine_type = get("machine_type", "\"e2-medium\"");
    let image = get("image", "\"\"");
    let subnetwork = get("subnetwork", "\"\"");

    let service_account_block = args
        .get("service_account_email")
        .map(|sa| {
            format!(
                "\n\n  service_account {{\n    email  = {sa}\n    scopes = [\"cloud-platform\"]\n  }}"
            )
        })
        .unwrap_or_default();

    format!(
        "resource \"google_compute_instance\" \"{id}\" {{\n  name         = {name}\n  project      = {project}\n  zone         = {zone}\n  machine_type = {machine_type}\n\n  boot_disk {{\n    initialize_params {{\n      image = {image}\n    }}\n  }}\n\n  network_interface {{\n    subnetwork = {subnetwork}\n  }}{service_account_block}\n}}\n"
    )
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "id" | "self_link" => Some(format!("google_compute_instance.{id}.{output}")),
        "network_ip" => Some(format!(
            "google_compute_instance.{id}.network_interface[0].network_ip"
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
