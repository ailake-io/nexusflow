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
    serve(path)
        .unwrap_or_else(|| serve("index.html").unwrap_or(StatusCode::NOT_FOUND.into_response()))
}

fn serve(path: &str) -> Option<Response> {
    let file = Assets::get(path)?;
    let mime = file.metadata.mimetype();
    // Never cache the SPA shell so updates are picked up on hard refresh;
    // versioned static assets (JS/CSS with hashed filenames) can be cached
    // aggressively because their URL changes when the content changes.
    let cache_control = if path == "index.html" || path.is_empty() {
        "no-cache, no-store, must-revalidate"
    } else {
        "public, max-age=3600, immutable"
    };
    Some(
        (
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, cache_control),
            ],
            file.data,
        )
            .into_response(),
    )
}
