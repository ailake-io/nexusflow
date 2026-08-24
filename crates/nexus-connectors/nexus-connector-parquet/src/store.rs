use nexus_core::NexusError;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{parse_url_opts, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Resolves `uri` to an `ObjectStore` plus the target file path(s) within
/// it. `s3://`/`gs://`/`az://` URLs go through `object_store::parse_url_opts`
/// with `storage_options` forwarded straight through and always resolve to
/// exactly one path — cloud prefix-listing isn't supported yet. A local
/// filesystem path that already exists as a **directory** resolves to every
/// regular file directly inside it (non-recursive, dotfiles skipped, sorted
/// by name) — the caller reads/concatenates each in turn. A local path that
/// doesn't exist yet, or exists as a file, resolves to that single file (its
/// parent directory is created if it doesn't exist yet, matching the
/// "created on first write" contract of the other local-path connectors).
pub(crate) fn open_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, Vec<ObjectPath>), NexusError> {
    if let Ok(url) = Url::parse(uri) {
        if url.scheme() != "file" {
            let (store, path) = parse_url_opts(&url, storage_options)
                .map_err(|e| NexusError::Connector(format!("parquet store open failed: {e}")))?;
            return Ok((Arc::from(store), vec![path]));
        }
    }

    let path_buf = std::path::PathBuf::from(uri);

    if path_buf.is_dir() {
        let canonical_dir = path_buf
            .canonicalize()
            .map_err(|e| NexusError::Connector(format!("parquet could not resolve path: {e}")))?;
        let mut file_names: Vec<String> = std::fs::read_dir(&canonical_dir)
            .map_err(|e| NexusError::Connector(format!("parquet could not list directory: {e}")))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| !name.starts_with('.'))
            .collect();
        if file_names.is_empty() {
            return Err(NexusError::Connector(format!(
                "parquet: directory '{}' has no readable files",
                canonical_dir.display()
            )));
        }
        file_names.sort();

        let store = LocalFileSystem::new_with_prefix(&canonical_dir)
            .map_err(|e| NexusError::Connector(format!("parquet local store open failed: {e}")))?;
        let paths = file_names
            .into_iter()
            .map(|name| ObjectPath::from(name.as_str()))
            .collect();
        return Ok((Arc::new(store), paths));
    }

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
    Ok((Arc::new(store), vec![object_path]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn open_store_single_file_returns_one_path() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("events.parquet");
        fs::write(&file_path, b"not really parquet, just needs to exist").unwrap();

        let (_store, paths) = open_store(file_path.to_str().unwrap(), &HashMap::new()).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].as_ref(), "events.parquet");
    }

    #[test]
    fn open_store_directory_returns_every_regular_file_sorted() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("b.parquet"), b"b").unwrap();
        fs::write(dir.path().join("a.parquet"), b"a").unwrap();
        fs::write(dir.path().join(".hidden.parquet"), b"hidden").unwrap();
        fs::create_dir(dir.path().join("subdir")).unwrap();

        let (_store, paths) = open_store(dir.path().to_str().unwrap(), &HashMap::new()).unwrap();
        let names: Vec<String> = paths.iter().map(|p| p.as_ref().to_string()).collect();
        assert_eq!(
            names,
            vec!["a.parquet".to_string(), "b.parquet".to_string()]
        );
    }

    #[test]
    fn open_store_empty_directory_errors() {
        let dir = tempfile::tempdir().unwrap();
        let err = open_store(dir.path().to_str().unwrap(), &HashMap::new())
            .expect_err("empty directory must be rejected");
        assert!(matches!(err, NexusError::Connector(msg) if msg.contains("no readable files")));
    }
}
