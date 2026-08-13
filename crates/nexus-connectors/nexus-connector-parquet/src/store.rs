use nexus_core::NexusError;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{parse_url_opts, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Resolves `uri` to an `ObjectStore` plus the path of the target file within
/// it. `s3://`/`gs://`/`az://` URLs go through `object_store::parse_url_opts`
/// with `storage_options` forwarded straight through; everything else is
/// treated as a local filesystem path (its parent directory is created if it
/// doesn't exist yet, matching the "created on first write" contract of the
/// other local-path connectors).
pub(crate) fn open_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), NexusError> {
    if let Ok(url) = Url::parse(uri) {
        if url.scheme() != "file" {
            let (store, path) = parse_url_opts(&url, storage_options)
                .map_err(|e| NexusError::Connector(format!("parquet store open failed: {e}")))?;
            return Ok((Arc::from(store), path));
        }
    }

    let path_buf = std::path::PathBuf::from(uri);
    let parent = path_buf
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|e| NexusError::Connector(format!("parquet could not create parent dir: {e}")))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|e| NexusError::Connector(format!("parquet could not resolve path: {e}")))?;
    let file_name = path_buf
        .file_name()
        .ok_or_else(|| NexusError::Connector("parquet uri has no file name".to_string()))?;

    let store = LocalFileSystem::new_with_prefix(&canonical_parent)
        .map_err(|e| NexusError::Connector(format!("parquet local store open failed: {e}")))?;
    let object_path = ObjectPath::from(file_name.to_string_lossy().as_ref());
    Ok((Arc::new(store), object_path))
}
