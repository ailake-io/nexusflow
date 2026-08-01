use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

/// Single-binary deployment (Marco 11, CLAUDE.md §7) — `npm run build` must
/// have populated `frontend/dist` *before* this crate compiles with
/// `--features embed-ui`: `#[folder]` is read by the proc-macro at compile
/// time, not at runtime.
#[derive(RustEmbed)]
#[folder = "../../frontend/dist"]
struct Assets;

/// Router fallback: serves the embedded frontend build for any GET that
/// didn't match an API route. Falls back to `index.html` for paths with no
/// exact embedded match, so a hard refresh on a client-side route (e.g.
/// `/pipelines/foo`) still hands the SPA its shell instead of 404ing.
pub async fn handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    serve(path).unwrap_or_else(|| serve("index.html").unwrap_or(StatusCode::NOT_FOUND.into_response()))
}

fn serve(path: &str) -> Option<Response> {
    let file = Assets::get(path)?;
    let mime = file.metadata.mimetype();
    Some(([(header::CONTENT_TYPE, mime)], file.data).into_response())
}
