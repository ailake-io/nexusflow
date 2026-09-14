use serde::Serialize;
use std::path::{Path, PathBuf};

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
    pub path: String,
    pub entries: Vec<BrowseEntry>,
}

/// Lists `path`'s immediate contents (non-recursive). Canonicalizes first —
/// neutralizes a literal `..` in the requested path the same way every
/// local-path connector's own resolution already does.
///
/// A single unreadable entry (permission denied, broken symlink) is skipped
/// rather than failing the whole listing — same UX every real file browser
/// gives; the caller can still see and use everything else in the
/// directory.
pub fn list_directory(path: &Path) -> std::io::Result<BrowseListing> {
    let canonical = path.canonicalize()?;
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
        path: canonical.to_string_lossy().to_string(),
        entries: dirs,
    })
}

/// Creates a new subdirectory `name` directly inside `parent` — backs the
/// FileBrowserDialog's "New folder" button, so a sink pointed at a fresh
/// destination (e.g. a sqlite `file_path` under a directory that doesn't
/// exist yet) doesn't require shelling into the server first.
///
/// `name` must be a single path component: no `/`, and not `.`/`..` — it
/// only ever names one new directory *inside* the already-browsed
/// (already-canonicalized) `parent`, never a path of its own, so this can't
/// be used to climb or jump elsewhere in the filesystem.
pub fn create_directory(parent: &Path, name: &str) -> std::io::Result<PathBuf> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "folder name must be a single path segment (no '/', '.', or '..')",
        ));
    }
    let canonical_parent = parent.canonicalize()?;
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
        let created = create_directory(dir.path(), "new-folder").unwrap();
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
                create_directory(dir.path(), bad_name).is_err(),
                "{bad_name:?} should have been rejected"
            );
        }
    }

    #[test]
    fn create_directory_fails_if_it_already_exists() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("existing")).unwrap();
        assert!(create_directory(dir.path(), "existing").is_err());
    }

    #[test]
    fn lists_directories_before_files_both_alphabetical() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("z.csv"), "x").unwrap();
        fs::write(dir.path().join("a.csv"), "x").unwrap();
        fs::create_dir(dir.path().join("zdir")).unwrap();
        fs::create_dir(dir.path().join("adir")).unwrap();

        let listing = list_directory(dir.path()).unwrap();
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

        let listing = list_directory(dir.path()).unwrap();
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
    fn resolves_dot_dot_via_canonicalize() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(dir.path().join("outer.csv"), "x").unwrap();

        let traversal_path = sub.join("..");
        let listing = list_directory(&traversal_path).unwrap();
        assert!(listing.entries.iter().any(|e| e.name == "outer.csv"));
        assert!(listing.entries.iter().any(|e| e.name == "sub"));
    }

    #[test]
    fn nonexistent_path_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(list_directory(&missing).is_err());
    }
}
