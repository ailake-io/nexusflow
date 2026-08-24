use serde::Serialize;
use std::path::Path;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
