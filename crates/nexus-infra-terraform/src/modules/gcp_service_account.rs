//! `google_service_account` + one `google_project_iam_member` per role
//! in `roles` — the GCP analog of `aws-iam-role`'s "principal + attached
//! policies" pattern. No single community module is standard enough
//! here to wrap; both resources are simple enough to emit directly.

use nexus_core::submit_infra_module;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::ModuleImpl;

#[derive(Debug, Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Config {
    account_id: String,
    #[serde(default)]
    display_name: Option<String>,
    project_id: String,
    /// IAM role ids granted at the project level, e.g.
    /// ["roles/bigquery.dataEditor", "roles/storage.objectAdmin"].
    #[serde(default)]
    roles: Vec<String>,
}

submit_infra_module!(
    "gcp-service-account",
    "Service Account",
    "iam",
    "gcp",
    Config,
    &["email", "member"]
);

fn emit(args: &BTreeMap<String, String>, id: &str) -> String {
    let get = |k: &str, default: &str| args.get(k).cloned().unwrap_or_else(|| default.to_string());
    let account_id = get("account_id", &format!("\"{id}\""));
    let project_id = get("project_id", "\"\"");

    let display_name_line = args
        .get("display_name")
        .map(|d| format!("\n  display_name = {d}"))
        .unwrap_or_default();

    let sa_id = format!("{id}_sa");
    let mut block = format!(
        "resource \"google_service_account\" \"{sa_id}\" {{\n  account_id = {account_id}\n  project    = {project_id}{display_name_line}\n}}\n"
    );

    if let Some(roles) = args.get("roles") {
        block.push_str(&format!(
            "\nresource \"google_project_iam_member\" \"{id}_roles\" {{\n  for_each = toset({roles})\n  project  = {project_id}\n  role     = each.value\n  member   = \"serviceAccount:${{google_service_account.{sa_id}.email}}\"\n}}\n"
        ));
    }

    block
}

fn output_ref(output: &str, id: &str) -> Option<String> {
    match output {
        "email" => Some(format!("google_service_account.{id}_sa.email")),
        "member" => Some(format!(
            "\"serviceAccount:${{google_service_account.{id}_sa.email}}\""
        )),
        _ => None,
    }
}

pub(crate) static IMPL: ModuleImpl = ModuleImpl { emit, output_ref };
