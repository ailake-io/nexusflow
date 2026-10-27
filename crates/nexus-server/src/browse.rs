use serde::Serialize;
use std::path::{Component, Path, PathBuf};

/// One entry (file or subdirectory) inside a browsed directory.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BrowseEntry {
    pub name: String,
    pub is_dir: bool,
    /// File size in bytes — `None` for directories.
    pub size: Option<u64>,
}

/// Response for `GET /system/browse-fs` — the canonicalized path actually
/// listed, plus its entries (directories first, then files, both
/// alphabetical by name).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BrowseListing {
    /// Canonical root that bounds browsing and directory creation.
    pub root: String,
    /// Canonical directory currently listed.
    pub path: String,
    pub entries: Vec<BrowseEntry>,
}

/// Configured root for the file picker. Defaults to the upload directory so
/// uploaded files remain browsable without exposing the server's full disk.
pub fn configured_root() -> PathBuf {
    std::env::var_os("NEXUS_BROWSE_ROOT")
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var_os("NEXUS_UPLOAD_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data/uploads"))
}

fn canonical_root(root: &Path) -> std::io::Result<PathBuf> {
    // The default upload root is created lazily on first upload; create the
    // configured browse root here so the picker also works before uploads.
    std::fs::create_dir_all(root)?;
    root.canonicalize()
}

fn resolve_inside_root(root: &Path, requested: &Path) -> std::io::Result<(PathBuf, PathBuf)> {
    let canonical_root = canonical_root(root)?;
    let candidate = if requested.as_os_str().is_empty() || requested == Path::new("/") {
        canonical_root.clone()
    } else {
        let relative = requested.strip_prefix(&canonical_root).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "path is outside the configured file browser root",
            )
        })?;
        if relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "path is outside the configured file browser root",
            ));
        }
        canonical_root.join(relative)
    };
    let canonical = candidate.canonicalize()?;
    if !canonical.starts_with(&canonical_root) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "path resolves outside the configured file browser root",
        ));
    }
    Ok((canonical_root, canonical))
}

/// Lists a directory's immediate contents (non-recursive), constrained to
/// the operator-configured root. Canonicalization plus a containment check
/// also rejects symlinks that point outside that root.
///
/// A single unreadable entry (permission denied, broken symlink) is skipped
/// rather than failing the whole listing — same UX every real file browser
/// gives; the caller can still see and use everything else in the
/// directory.
pub fn list_directory(root: &Path, path: &Path) -> std::io::Result<BrowseListing> {
    let (canonical_root, canonical) = resolve_inside_root(root, path)?;
    let read_dir = std::fs::read_dir(&canonical)?;

    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in read_dir.filter_map(|e| e.ok()) {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().to_string();
        if metadata.is_dir() {
            dirs.push(BrowseEntry {
                name,
                is_dir: true,
                size: None,
            });
        } else if metadata.is_file() {
            files.push(BrowseEntry {
                name,
                is_dir: false,
                size: Some(metadata.len()),
            });
        }
    }
    dirs.sort_by(|a, b| a.name.cmp(&b.name));
    files.sort_by(|a, b| a.name.cmp(&b.name));
    dirs.extend(files);

    Ok(BrowseListing {
        root: canonical_root.to_string_lossy().to_string(),
        path: canonical.to_string_lossy().to_string(),
        entries: dirs,
    })
}

/// Creates a new subdirectory `name` directly inside `parent` — backs the
/// FileBrowserDialog's "New folder" button, so a sink pointed at a fresh
/// destination (e.g. a sqlite `file_path` under a directory that doesn't
/// exist yet) doesn't require shelling into the server first.
///
/// `name` must be one normal path component, and the parent is resolved
/// inside the configured browse root before creating the directory.
pub fn create_directory(root: &Path, parent: &Path, name: &str) -> std::io::Result<PathBuf> {
    let name_path = Path::new(name);
    if name.is_empty()
        || name_path.components().count() != 1
        || !matches!(name_path.components().next(), Some(Component::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "folder name must be a single path segment",
        ));
    }
    let (_, canonical_parent) = resolve_inside_root(root, parent)?;
    let new_dir = canonical_parent.join(name);
    std::fs::create_dir(&new_dir)?;
    Ok(new_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn create_directory_makes_a_new_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        let created = create_directory(dir.path(), dir.path(), "new-folder").unwrap();
        assert!(created.is_dir());
        assert_eq!(
            created,
            dir.path().canonicalize().unwrap().join("new-folder")
        );
    }

    #[test]
    fn create_directory_rejects_path_separators_and_dot_segments() {
        let dir = tempfile::tempdir().unwrap();
        for bad_name in ["a/b", "..", ".", "", "/etc"] {
            assert!(
                create_directory(dir.path(), dir.path(), bad_name).is_err(),
                "{bad_name:?} should have been rejected"
            );
        }
    }

    #[test]
    fn create_directory_fails_if_it_already_exists() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("existing")).unwrap();
        assert!(create_directory(dir.path(), dir.path(), "existing").is_err());
    }

    #[test]
    fn lists_directories_before_files_both_alphabetical() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("z.csv"), "x").unwrap();
        fs::write(dir.path().join("a.csv"), "x").unwrap();
        fs::create_dir(dir.path().join("zdir")).unwrap();
        fs::create_dir(dir.path().join("adir")).unwrap();

        let listing = list_directory(dir.path(), dir.path()).unwrap();
        let names: Vec<(String, bool)> = listing
            .entries
            .iter()
            .map(|e| (e.name.clone(), e.is_dir))
            .collect();
        assert_eq!(
            names,
            vec![
                ("adir".to_string(), true),
                ("zdir".to_string(), true),
                ("a.csv".to_string(), false),
                ("z.csv".to_string(), false),
            ]
        );
    }

    #[test]
    fn reports_file_size_but_not_for_directories() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("data.csv"), "12345").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();

        let listing = list_directory(dir.path(), dir.path()).unwrap();
        let file_entry = listing
            .entries
            .iter()
            .find(|e| e.name == "data.csv")
            .unwrap();
        assert_eq!(file_entry.size, Some(5));
        let dir_entry = listing.entries.iter().find(|e| e.name == "sub").unwrap();
        assert_eq!(dir_entry.size, None);
    }

    #[test]
    fn rejects_parent_traversal_outside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        let traversal_path = sub.join("..").join("..");
        assert_eq!(
            list_directory(dir.path(), &traversal_path)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn virtual_root_lists_the_configured_root() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("data.csv"), "x").unwrap();

        let listing = list_directory(dir.path(), Path::new("/")).unwrap();
        assert_eq!(
            listing.path,
            dir.path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(listing.root, listing.path);
        assert_eq!(listing.entries[0].name, "data.csv");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinks_that_resolve_outside_the_root() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join("escape")).unwrap();

        assert_eq!(
            list_directory(root.path(), &root.path().join("escape"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn nonexistent_path_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(list_directory(dir.path(), &missing).is_err());
    }
}
