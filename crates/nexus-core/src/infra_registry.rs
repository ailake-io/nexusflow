//! Plugin extension point for the "Infra" canvas (Terraform code
//! generation) — same `inventory`-based distributed-registration shape as
//! `registry.rs`'s `ConnectorDescriptor`/`SourceBuilder`, for the exact same
//! reason: the module catalog and the actual codegen live in a private
//! enterprise crate (`nexus-infra-terraform`, `nexus-connectors-enterprise`
//! repo) that `nexus-server` never depends on directly (`LICENSING.md §3`).
//!
//! Two inventory types:
//! - [`InfraModuleDescriptor`] — one entry per curated Terraform module
//!   (e.g. "aws-vpc", "aws-sagemaker-endpoint"). `nexus-server`'s
//!   `GET /infra/modules` just lists whatever's registered — empty in any
//!   binary that doesn't link the enterprise crate in.
//! - [`InfraGenerator`] — exactly one entry (or zero) exposing the actual
//!   `generate` function. `nexus-server`'s `POST /infra/generate` calls
//!   through this; with nothing registered it returns "feature not
//!   available" regardless of license — the license check
//!   (`check_connector_license("infra-terraform-generator", ...)`, gated on
//!   a slug the enterprise crate registers via `submit_enterprise_connector!`
//!   same as `capability_registry.rs`'s LLMOps slugs) is the *first* gate,
//!   this registry lookup is the second, same two-gate shape every real
//!   enterprise connector already has (`connectors.rs`'s own doc comment).

use crate::error::NexusError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One module instance placed on the Infra canvas — mirrors `dag::NodeSpec`'s
/// shape (name/connector/config) but for an infra module instead of a data
/// connector. Shared between `nexus-server` (deserializes the request body)
/// and the enterprise crate's `generate()` (walks this to emit HCL), so both
/// sides always agree on the wire format without hand-syncing two structs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InfraNode {
    pub id: String,
    pub module: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// A dependency: `to`'s `input` variable is wired to `from`'s `output` —
/// becomes a `module.<from>.<output>` reference in the generated HCL rather
/// than a literal value for that input.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InfraEdge {
    pub from: String,
    pub to: String,
    pub output: String,
    pub input: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct InfraGraph {
    pub nodes: Vec<InfraNode>,
    #[serde(default)]
    pub edges: Vec<InfraEdge>,
}

/// `BTreeMap`, not `HashMap` — deterministic file ordering in the JSON
/// response (nicer for the frontend's tabbed viewer, and for golden-file
/// tests in the enterprise crate).
#[derive(Debug, Clone, Serialize, Default)]
pub struct GeneratedFiles {
    pub files: BTreeMap<String, String>,
}

/// Metadata an infra-module crate publishes about one curated module. Same
/// `config_schema` fn-pointer trick as `ConnectorDescriptor` — the frontend
/// reuses the existing generic `SchemaForm` (already built for connector
/// configs) to render each module's inputs, zero bespoke form per module.
#[derive(Serialize)]
pub struct InfraModuleDescriptor {
    pub id: &'static str,
    pub name: &'static str,
    /// Free-form grouping for the palette UI ("network", "iam", "data",
    /// "ai", "cicd", ...) — not an enum here so the enterprise crate can add
    /// categories without a nexus-core release.
    pub category: &'static str,
    /// "aws" today — kept as a field (not hardcoded anywhere) so a future
    /// GCP/Azure module just registers with a different value here, no
    /// registry change needed.
    pub provider: &'static str,
    #[serde(skip)]
    pub config_schema: fn() -> serde_json::Value,
    pub outputs: &'static [&'static str],
}

inventory::collect!(InfraModuleDescriptor);

#[macro_export]
macro_rules! submit_infra_module {
    ($id:expr, $name:expr, $category:expr, $provider:expr, $config:ty, $outputs:expr) => {
        $crate::registry::inventory::submit! {
            $crate::infra_registry::InfraModuleDescriptor {
                id: $id,
                name: $name,
                category: $category,
                provider: $provider,
                config_schema: || {
                    let schema = $crate::registry::schemars::schema_for!($config);
                    $crate::registry::serde_json::to_value(&schema)
                        .expect("JSON schema always serializes")
                },
                outputs: $outputs,
            }
        }
    };
}

pub type InfraGenerateFn = fn(&InfraGraph) -> Result<GeneratedFiles, NexusError>;

/// Expected to have exactly 0 or 1 entries in any real binary — see this
/// module's doc comment for what each case means.
pub struct InfraGenerator {
    pub generate: InfraGenerateFn,
}

inventory::collect!(InfraGenerator);

#[macro_export]
macro_rules! submit_infra_generator {
    ($generate:expr) => {
        $crate::registry::inventory::submit! {
            $crate::infra_registry::InfraGenerator { generate: $generate }
        }
    };
}

impl InfraModuleDescriptor {
    pub fn all() -> impl Iterator<Item = &'static InfraModuleDescriptor> {
        inventory::iter::<InfraModuleDescriptor>().into_iter()
    }

    pub fn find(id: &str) -> Option<&'static InfraModuleDescriptor> {
        Self::all().find(|d| d.id == id)
    }
}

impl InfraGenerator {
    /// `None` means no infra-terraform crate is linked into this binary —
    /// callers should report "feature not available", not fall through to
    /// treating the license check alone as authoritative (see module doc).
    pub fn get() -> Option<&'static InfraGenerator> {
        inventory::iter::<InfraGenerator>().into_iter().next()
    }
}
