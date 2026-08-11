use nexus_core::NexusError;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{parse_url_opts, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Resolves `uri` to an `ObjectStore` + the path of the target file within
/// it — `s3://`/`gs://`/`az://`/`http(s)://` go through `object_store`'s own
/// cloud builders (`storage_options` forwarded straight through, e.g.
/// `aws_access_key_id`/`google_service_account`/`azure_storage_account_key`
/// — see each backend's `ConfigKey` docs for the full option list), anything
/// else is treated as a local filesystem path (created if its parent
/// directory doesn't exist yet, same "created on first write" contract
/// every other local-path connector in this workspace has).
pub(crate) fn open_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), NexusError> {
    if let Ok(url) = Url::parse(uri) {
        if url.scheme() != "file" {
            let (store, path) = parse_url_opts(&url, storage_options)
                .map_err(|e| NexusError::Connector(format!("csv store open failed: {e}")))?;
            return Ok((Arc::from(store), path));
        }
    }

    let path_buf = std::path::PathBuf::from(uri);
    let parent = path_buf
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|e| NexusError::Connector(format!("csv could not create parent dir: {e}")))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|e| NexusError::Connector(format!("csv could not resolve path: {e}")))?;
    let file_name = path_buf
        .file_name()
        .ok_or_else(|| NexusError::Connector("csv uri has no file name".to_string()))?;

    let store = LocalFileSystem::new_with_prefix(&canonical_parent)
        .map_err(|e| NexusError::Connector(format!("csv local store open failed: {e}")))?;
    let object_path = ObjectPath::from(file_name.to_string_lossy().as_ref());
    Ok((Arc::new(store), object_path))
}
