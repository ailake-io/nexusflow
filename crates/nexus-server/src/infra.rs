//! "Infra" canvas — `GET /infra/modules` + `POST /infra/generate` (planning
//! doc: compiled-kindling-puzzle.md). Drag-and-drop curated Terraform
//! modules (AWS/GCP/Azure, data/AI + CI/CD products) on the same
//! React Flow shell the data-pipeline Canvas uses, wire them together, get
//! back real `.tf` files.
//!
//! The module catalog and codegen live in `nexus-infra-terraform`, linked
//! in by the `infra-terraform` feature (part of `connectors-all`). Both
//! handlers go through `nexus_core::infra_registry`'s `inventory`-collected
//! plugin registry (same shape as `ConnectorRegistry`/`SourceBuilder` in
//! `crates/nexus-core/src/registry.rs`) — in a binary built without that
//! feature the registry is simply empty and both handlers report the
//! feature as unavailable.

use crate::auth::{require_role, Role};
use crate::error::ApiError;
use crate::AppState;
use axum::routing::{get, post};
use axum::{middleware, Extension, Json, Router};
use nexus_core::{GeneratedFiles, InfraGraph, InfraModuleDescriptor};
use serde::Serialize;

#[derive(Serialize)]
struct InfraModuleDto {
    id: &'static str,
    name: &'static str,
    category: &'static str,
    provider: &'static str,
    config_schema: serde_json::Value,
    outputs: &'static [&'static str],
}

/// `GET /infra/modules` — the palette's data source, same shape as
/// `list_connectors_handler` (`lib.rs`). Empty list (not an error) when the
/// feature isn't compiled into this build.
async fn list_modules_handler() -> Json<Vec<InfraModuleDto>> {
    Json(
        InfraModuleDescriptor::all()
            .map(|d| InfraModuleDto {
                id: d.id,
                name: d.name,
                category: d.category,
                provider: d.provider,
                config_schema: (d.config_schema)(),
                outputs: d.outputs,
            })
            .collect(),
    )
}

/// `POST /infra/generate` — body is an `InfraGraph` (nodes + edges, see
/// `nexus_core::infra_registry`'s doc comment for the wire shape). Returns
/// the generated `.tf` files as `{"files": {"main.tf": "...", ...}}`.
async fn generate_handler(Json(graph): Json<InfraGraph>) -> Result<Json<GeneratedFiles>, ApiError> {
    let generator = nexus_core::InfraGenerator::get().ok_or_else(|| {
        ApiError::bad_request(
            "infra-terraform generation isn't available in this build".to_string(),
        )
    })?;

    let files = (generator.generate)(&graph).map_err(ApiError::internal)?;
    Ok(Json(files))
}

pub fn routes(state: AppState) -> Router {
    let read_routes = Router::new()
        .route("/infra/modules", get(list_modules_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Read));

    // Generating files is read-only server-side (no state mutated, nothing
    // provisioned — codegen only, see the planning doc's "fora de escopo"),
    // but gated at `Execute` anyway since it's the action a user takes to
    // *do* something with the canvas, same posture as `POST /rag/query`.
    let execute_routes = Router::new()
        .route("/infra/generate", post(generate_handler))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_role::<AppState>,
        ))
        .layer(Extension(Role::Execute));

    Router::new()
        .merge(read_routes)
        .merge(execute_routes)
        .with_state(state)
}
