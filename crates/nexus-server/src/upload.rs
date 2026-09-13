//! `POST /system/upload` — the only place bytes from the browser land on
//! disk server-side. Every file-based connector (csv/parquet/sqlite/the
//! enterprise `pdf-ocr`) needs a real filesystem path to read from; this
//! endpoint is what lets the Canvas's "Enviar arquivo(s)"/"Enviar pasta"
//! buttons (`frontend/src/components/SchemaForm.tsx`) turn a browser file
//! picker (or a dropped file/folder) into one of those paths, the same way
//! `browse_fs_handler` turns a click into a path already on the server.
//!
//! One request = one batch, grouped under `{NEXUS_UPLOAD_DIR}/{uuid}/` so
//! concurrent uploads (or two uploads of files with the same name) never
//! collide. A folder upload sends every file in one multipart request with
//! each part's filename set to the file's path *relative to the chosen
//! folder* (`webkitRelativePath` on the frontend) — that's what lets this
//! handler recreate the folder's structure under the batch directory
//! instead of flattening everything into one level.

use crate::error::ApiError;
use axum::extract::Multipart;
use axum::Json;
use serde::Serialize;
use std::path::{Component, Path, PathBuf};

/// 500MB — generous for a folder of scanned PDFs, small enough that one
/// upload can't casually fill the disk. Applied only to this route (see
/// `lib.rs`'s `DefaultBodyLimit::max` layer on it), not the whole server.
pub const MAX_UPLOAD_BYTES: usize = 500 * 1024 * 1024;

fn upload_dir() -> PathBuf {
    std::env::var("NEXUS_UPLOAD_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/data/uploads"))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UploadResult {
    /// The file's path if exactly one file was uploaded, otherwise the
    /// batch directory containing all of them — either way, a value ready
    /// to drop straight into a connector's `path`/`file_path` config field.
    pub path: String,
}

/// Rejects a relative path that tries to escape the batch directory (`..`
/// components) or that's rooted/absolute — a multipart filename is
/// attacker-controlled input, not something to trust blindly even though
/// it's the same browser session that's authenticated.
fn safe_relative_path(name: &str) -> Result<PathBuf, ApiError> {
    let path = Path::new(name);
    if path
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
        || path.is_absolute()
    {
        return Err(ApiError::bad_request(format!(
            "invalid upload filename: {name:?}"
        )));
    }
    Ok(path.to_path_buf())
}

pub async fn upload_handler(mut multipart: Multipart) -> Result<Json<UploadResult>, ApiError> {
    let batch_dir = upload_dir().join(uuid::Uuid::new_v4().to_string());
    let mut saved_paths = Vec::new();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::bad_request(format!("invalid multipart upload: {e}")))?
    {
        let Some(file_name) = field.file_name().map(str::to_string) else {
            // A non-file form field (shouldn't happen given the frontend
            // only ever appends files) — skip rather than fail the whole
            // batch over one stray part.
            continue;
        };
        let relative_path = safe_relative_path(&file_name)?;
        let dest = batch_dir.join(&relative_path);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(ApiError::internal)?;
        }
        let bytes = field
            .bytes()
            .await
            .map_err(|e| ApiError::bad_request(format!("failed reading upload: {e}")))?;
        tokio::fs::write(&dest, &bytes)
            .await
            .map_err(ApiError::internal)?;
        saved_paths.push(dest);
    }

    if saved_paths.is_empty() {
        return Err(ApiError::bad_request("upload contained no files"));
    }

    let path = if saved_paths.len() == 1 {
        saved_paths[0].to_string_lossy().to_string()
    } else {
        batch_dir.to_string_lossy().to_string()
    };

    Ok(Json(UploadResult { path }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_dir_traversal() {
        assert!(safe_relative_path("../etc/passwd").is_err());
        assert!(safe_relative_path("a/../../b").is_err());
    }

    #[test]
    fn rejects_absolute_paths() {
        assert!(safe_relative_path("/etc/passwd").is_err());
    }

    #[test]
    fn accepts_plain_and_nested_relative_paths() {
        assert_eq!(
            safe_relative_path("report.pdf").unwrap(),
            PathBuf::from("report.pdf")
        );
        assert_eq!(
            safe_relative_path("invoices/2026/january.pdf").unwrap(),
            PathBuf::from("invoices/2026/january.pdf")
        );
    }
}
