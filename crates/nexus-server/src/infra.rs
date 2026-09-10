//! "Infra" canvas — `GET /infra/modules` + `POST /infra/generate` (planning
//! doc: compiled-kindling-puzzle.md). Drag-and-drop curated Terraform
//! modules (AWS-only for now, data/AI + CI/CD products) on the same
//! React Flow shell the data-pipeline Canvas uses, wire them together, get
//! back real `.tf` files.
//!
//! Deliberately **not** feature-gated (`#[cfg(feature = "...")]`) like an
//! OSS connector's match arm would be — this whole domain lives in a
//! private enterprise crate (`nexus-infra-terraform`,
//! `nexus-connectors-enterprise` repo) that `nexus-server` never depends on
//! directly (`LICENSING.md §3`). Both handlers go through
//! `nexus_core::infra_registry`'s `inventory`-collected plugin registry
//! instead (same shape as `ConnectorRegistry`/`SourceBuilder` in
//! `crates/nexus-core/src/registry.rs`) — in an OSS binary, or an
//! enterprise binary that didn't link this crate in, the registry is
//! simply empty and both handlers report the feature as unavailable.
//!
//! Single enterprise gate for the whole feature (`docs/ENTERPRISE_LICENSING.md`
//! decision, 2026-09-10 — no per-module OSS/paid split like the Store's
//! connector catalog): one slug, `"infra-terraform-generator"`, registered
//! by the enterprise crate itself via `submit_enterprise_connector!`
//! (`capability_registry.rs`'s pattern, but this time genuinely absent
//! from every OSS binary rather than always-compiled-just-license-checked
//! — this is closer to a real enterprise connector than to the LLMOps
//! capabilities). `check_connector_license` is the *first* gate (does the
//! installed license cover the slug); the registry lookup below is the
//! *second* (does this binary even have the generator linked in at all) —
//! same two-gate shape every real enterprise connector already has, see
//! `connectors.rs`'s own doc comment on why "slug not found → allow" is
//! safe there.

use crate::auth::{require_role, Role};
use crate::connectors::check_connector_license;
use crate::error::ApiError;
use crate::AppState;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{middleware, Extension, Json, Router};
use nexus_core::{GeneratedFiles, InfraGraph, InfraModuleDescriptor};
use serde::Serialize;

const INFRA_LICENSE_SLUG: &str = "infra-terraform-generator";

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
/// feature isn't available at all, licensed or not — lets the frontend
/// show "not available in this build" distinctly from "buy a license"
/// later if that distinction ever matters; today the Store-style paywall
/// panel covers both the same way (`ARCHITECTURE.md`'s Infra section).
async fn list_modules_handler(State(state): State<AppState>) -> Json<Vec<InfraModuleDto>> {
    let active_license = state.license_store.active().await.ok().flatten();
    if check_connector_license(INFRA_LICENSE_SLUG, active_license.as_ref()).is_err() {
        return Json(Vec::new());
    }
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
async fn generate_handler(
    State(state): State<AppState>,
    Json(graph): Json<InfraGraph>,
) -> Result<Json<GeneratedFiles>, ApiError> {
    let active_license = state.license_store.active().await.unwrap_or(None);
    check_connector_license(INFRA_LICENSE_SLUG, active_license.as_ref())
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

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
