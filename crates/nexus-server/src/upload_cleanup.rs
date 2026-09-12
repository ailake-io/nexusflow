//! Background sweeper for `NEXUS_UPLOAD_DIR` (see `upload.rs`) — same shape
//! as `scheduler::spawn`/`resource_stats::spawn` (only started from
//! `run()`'s real boot path, never `build_app`, so tests don't get a
//! surprise background task). Uploaded files have no other lifecycle —
//! nothing ever explicitly deletes a batch directory once a pipeline has
//! read it — so without this sweep, `NEXUS_UPLOAD_DIR` grows forever.

use crate::AppState;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

fn upload_dir() -> PathBuf {
    std::env::var("NEXUS_UPLOAD_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/data/uploads"))
}

fn ttl() -> Duration {
    let hours = std::env::var("NEXUS_UPLOAD_TTL_HOURS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(24);
    Duration::from_secs(hours * 3600)
}

fn interval() -> Duration {
    let hours = std::env::var("NEXUS_UPLOAD_CLEANUP_INTERVAL_HOURS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1);
    Duration::from_secs(hours.max(1) * 3600)
}

/// Removes every immediate subdirectory of `dir` whose `mtime` is older
/// than `ttl` (relative to `now`). A single unreadable/racy entry (already
/// removed by a concurrent sweep, permission error) is skipped rather than
/// failing the whole pass — same "one bad entry doesn't ruin the rest"
/// posture as `browse.rs::list_directory`.
fn sweep(dir: &Path, ttl: Duration, now: SystemTime) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.filter_map(|e| e.ok()) {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age >= ttl && std::fs::remove_dir_all(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

pub fn spawn(_state: AppState) {
    tokio::spawn(async move {
        let dir = upload_dir();
        let ttl_duration = ttl();
        let mut tick = tokio::time::interval(interval());
        loop {
            tick.tick().await;
            let dir = dir.clone();
            let removed =
                tokio::task::spawn_blocking(move || sweep(&dir, ttl_duration, SystemTime::now()))
                    .await
                    .unwrap_or(0);
            if removed > 0 {
                tracing::info!(removed, "swept expired upload batches");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn removes_only_directories_older_than_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old-batch");
        let fresh = dir.path().join("fresh-batch");
        fs::create_dir(&old).unwrap();
        fs::create_dir(&fresh).unwrap();

        let now = SystemTime::now();
        let ttl = Duration::from_secs(3600);
        // "old" is aged by asking sweep() to treat `now` as far enough in
        // the future that its real (fresh) mtime already exceeds the TTL —
        // avoids needing filesystem mtime manipulation in a unit test.
        let future_now = now + Duration::from_secs(7200);

        let removed = sweep(dir.path(), ttl, future_now);

        assert_eq!(removed, 2);
        assert!(!old.exists());
        assert!(!fresh.exists());
    }

    #[test]
    fn leaves_directories_within_ttl_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh-batch");
        fs::create_dir(&fresh).unwrap();

        let removed = sweep(dir.path(), Duration::from_secs(3600), SystemTime::now());

        assert_eq!(removed, 0);
        assert!(fresh.exists());
    }

    #[test]
    fn ignores_plain_files_at_the_top_level() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("stray.txt"), "x").unwrap();

        let future_now = SystemTime::now() + Duration::from_secs(7200);
        let removed = sweep(dir.path(), Duration::from_secs(3600), future_now);

        assert_eq!(removed, 0);
        assert!(dir.path().join("stray.txt").exists());
    }

    #[test]
    fn missing_upload_dir_is_a_no_op() {
        let removed = sweep(
            Path::new("/does/not/exist/anywhere"),
            Duration::from_secs(3600),
            SystemTime::now(),
        );
        assert_eq!(removed, 0);
    }
}
