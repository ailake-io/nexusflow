use nexus_core::NexusError;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{parse_url_opts, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Resolves `uri` to an `ObjectStore` + the path of the target file within
/// it. Copy of `nexus-connector-csv`'s `open_store` (`LICENSING.md §3` —
/// this crate can't depend on that OSS crate's internal module across the
/// repo boundary, so the small amount of duplication is intentional).
pub(crate) fn open_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), NexusError> {
    if let Ok(url) = Url::parse(uri) {
        if url.scheme() != "file" {
            let (store, path) = parse_url_opts(&url, storage_options)
                .map_err(|e| NexusError::Connector(format!("excel store open failed: {e}")))?;
            return Ok((Arc::from(store), path));
        }
    }

    let path_buf = std::path::PathBuf::from(uri);
    let parent = path_buf
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|e| NexusError::Connector(format!("excel could not create parent dir: {e}")))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|e| NexusError::Connector(format!("excel could not resolve path: {e}")))?;
    let file_name = path_buf
        .file_name()
        .ok_or_else(|| NexusError::Connector("excel uri has no file name".to_string()))?;

    let store = LocalFileSystem::new_with_prefix(&canonical_parent)
        .map_err(|e| NexusError::Connector(format!("excel local store open failed: {e}")))?;
    let object_path = ObjectPath::from(file_name.to_string_lossy().as_ref());
    Ok((Arc::new(store), object_path))
}
