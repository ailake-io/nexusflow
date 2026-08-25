use crate::db::{rewrite_placeholders, MetadataPool};
use crate::AppState;
use chrono::{DateTime, Utc};
use std::path::{Path, PathBuf};
use std::time::Duration;
use sysinfo::Disks;

/// One CPU/memory/disk sample, taken once per `spawn`'s tick. Disk fields
/// are `None` when the data directory's containing mount point couldn't be
/// determined (exotic environment, e.g. a filesystem `sysinfo` doesn't
/// enumerate) — CPU/memory are still recorded rather than dropping the
/// whole sample.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceSample {
    pub sampled_at: DateTime<Utc>,
    pub cpu_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
}

/// Averaged over one time bucket — what the `GET /system/resource-stats`
/// endpoint actually returns (never raw samples, see `bucket_samples`).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ResourceStatsBucket {
    pub bucket_start: DateTime<Utc>,
    pub cpu_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
}

/// Minimal view of a `sysinfo::Disk` — extracted so the mount-point-matching
/// logic below is testable without a real disk (`sysinfo::Disk` has no
/// public constructor).
struct DiskInfo {
    mount_point: PathBuf,
    total_bytes: u64,
    used_bytes: u64,
}

/// Finds the disk whose mount point is the longest matching prefix of
/// `path` — same algorithm `df`/`findmnt` use to resolve "which filesystem
/// is this path actually on" when mounts are nested (e.g. `/` and `/data`
/// both mounted, a path under `/data` must resolve to `/data`, not `/`).
fn find_containing_disk<'a>(disks: &'a [DiskInfo], path: &Path) -> Option<&'a DiskInfo> {
    disks
        .iter()
        .filter(|d| path.starts_with(&d.mount_point))
        .max_by_key(|d| d.mount_point.as_os_str().len())
}

/// Wraps a `sysinfo::System` refreshed in place across samples (CPU usage
/// is only meaningful as a delta between two refreshes — same rationale as
/// `hardware_stats::HardwareMonitor`, which this reuses directly for the
/// CPU/memory half of each sample).
pub struct ResourceMonitor {
    hardware: crate::hardware_stats::HardwareMonitor,
    data_dir: PathBuf,
}

impl ResourceMonitor {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            hardware: crate::hardware_stats::HardwareMonitor::new(),
            data_dir,
        }
    }

    pub fn sample(&mut self) -> ResourceSample {
        let hw = self.hardware.sample();
        let disks = Disks::new_with_refreshed_list();
        let disk_infos: Vec<DiskInfo> = disks
            .list()
            .iter()
            .map(|d| DiskInfo {
                mount_point: d.mount_point().to_path_buf(),
                total_bytes: d.total_space(),
                used_bytes: d.total_space().saturating_sub(d.available_space()),
            })
            .collect();
        let disk = find_containing_disk(&disk_infos, &self.data_dir);

        ResourceSample {
            sampled_at: Utc::now(),
            cpu_percent: hw.cpu_percent,
            memory_used_bytes: hw.memory_used_bytes,
            memory_total_bytes: hw.memory_total_bytes,
            disk_used_bytes: disk.map(|d| d.used_bytes),
            disk_total_bytes: disk.map(|d| d.total_bytes),
        }
    }
}

/// Rejects anything longer than 30 days — matches `spawn`'s retention
/// (`prune_older_than`, 31 days), so the endpoint never promises history
/// the backend doesn't actually keep.
const MAX_RANGE: Duration = Duration::from_secs(30 * 24 * 3600);
const MIN_BUCKET: Duration = Duration::from_secs(60);
/// Target point count for any range — keeps the chart payload bounded
/// regardless of how long a period the user picks (a 30-day range at 1
/// sample/min would otherwise be ~43,200 raw points).
const TARGET_BUCKETS: u64 = 120;

#[derive(Debug, PartialEq, Eq)]
pub enum RangeParseError {
    Empty,
    UnknownUnit(char),
    InvalidNumber,
    Zero,
    TooLong,
}

impl std::fmt::Display for RangeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "range must not be empty"),
            Self::UnknownUnit(c) => write!(f, "unknown range unit {c:?}, expected m/h/d"),
            Self::InvalidNumber => write!(f, "range must start with a number"),
            Self::Zero => write!(f, "range must be greater than zero"),
            Self::TooLong => write!(f, "range must not exceed 30 days"),
        }
    }
}

/// Parses `<number><unit>` (`5m`, `45m`, `3h`, `12d`, ...) — free-form, not
/// just the 5 preset shortcuts the frontend also offers, since the user can
/// type a custom value too.
pub fn parse_range(input: &str) -> Result<Duration, RangeParseError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(RangeParseError::Empty);
    }
    let unit = input.chars().last().expect("checked non-empty above");
    let multiplier = match unit {
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        other => return Err(RangeParseError::UnknownUnit(other)),
    };
    let number_part = &input[..input.len() - unit.len_utf8()];
    let n: u64 = number_part
        .parse()
        .map_err(|_| RangeParseError::InvalidNumber)?;
    if n == 0 {
        return Err(RangeParseError::Zero);
    }
    let secs = n
        .checked_mul(multiplier)
        .ok_or(RangeParseError::InvalidNumber)?;
    let duration = Duration::from_secs(secs);
    if duration > MAX_RANGE {
        return Err(RangeParseError::TooLong);
    }
    Ok(duration)
}

/// Bucket width that keeps the number of returned points close to
/// `TARGET_BUCKETS` regardless of `lookback` — no fixed per-preset table,
/// so a custom range gets the same treatment as the 5 shortcuts.
pub fn bucket_width_for(lookback: Duration) -> Duration {
    let width = Duration::from_secs(lookback.as_secs() / TARGET_BUCKETS);
    width.max(MIN_BUCKET)
}

fn average_option(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut sum = 0u128;
    let mut count = 0u64;
    for v in values.flatten() {
        sum += v as u128;
        count += 1;
    }
    if count == 0 {
        None
    } else {
        Some((sum / count as u128) as u64)
    }
}

/// Groups `samples` (assumed already sorted by `sampled_at`, which is how
/// `ResourceStatsStore::range` returns them) into fixed-width windows
/// anchored at the first sample, averaging every field per window. Pure and
/// DB-free by design, same reasoning as the SQL builders in the connector
/// crates (`nexus-connector-postgres`'s `build_select_query`, etc.) —
/// testable without any store/hardware involved.
pub fn bucket_samples(
    samples: &[ResourceSample],
    bucket_width: Duration,
) -> Vec<ResourceStatsBucket> {
    if samples.is_empty() || bucket_width.is_zero() {
        return Vec::new();
    }
    let first = samples[0].sampled_at;
    let width_secs = bucket_width.as_secs().max(1);

    let mut indexed: Vec<(usize, &ResourceSample)> = samples
        .iter()
        .map(|s| {
            let offset = (s.sampled_at - first).num_seconds().max(0) as u64;
            ((offset / width_secs) as usize, s)
        })
        .collect();
    indexed.sort_by_key(|(idx, _)| *idx);

    let mut buckets: Vec<ResourceStatsBucket> = Vec::new();
    let mut i = 0;
    while i < indexed.len() {
        let idx = indexed[i].0;
        let mut j = i;
        while j < indexed.len() && indexed[j].0 == idx {
            j += 1;
        }
        let group = &indexed[i..j];
        let n = group.len() as f64;
        let cpu_percent = (group.iter().map(|(_, s)| s.cpu_percent as f64).sum::<f64>() / n) as f32;
        let memory_used_bytes = (group
            .iter()
            .map(|(_, s)| s.memory_used_bytes as f64)
            .sum::<f64>()
            / n) as u64;
        let memory_total_bytes = (group
            .iter()
            .map(|(_, s)| s.memory_total_bytes as f64)
            .sum::<f64>()
            / n) as u64;
        let disk_used_bytes = average_option(group.iter().map(|(_, s)| s.disk_used_bytes));
        let disk_total_bytes = average_option(group.iter().map(|(_, s)| s.disk_total_bytes));
        let bucket_start = first + chrono::Duration::seconds((idx as u64 * width_secs) as i64);

        buckets.push(ResourceStatsBucket {
            bucket_start,
            cpu_percent,
            memory_used_bytes,
            memory_total_bytes,
            disk_used_bytes,
            disk_total_bytes,
        });
        i = j;
    }
    buckets
}

/// Persisted history for the `resources` tab — SQLite (default) or Postgres,
/// same dual-dialect pattern as `checkpoint_store.rs`. Shares the checkpoint
/// database connection (`NEXUS_CHECKPOINT_DB`) rather than getting its own
/// env var, same call already made for `run_logs`/`license_store` in
/// `build_state`: not worth a dedicated connection pool for one small table.
#[derive(Clone)]
pub struct ResourceStatsStore {
    pool: MetadataPool,
}

impl ResourceStatsStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        match &pool {
            MetadataPool::Sqlite(p) => {
                // SQLite's `REAL` has no fixed width (dynamic type
                // affinity) — binding/reading it as `f64` throughout is
                // fine.
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS resource_stats_samples (
                        sampled_at TEXT NOT NULL PRIMARY KEY,
                        cpu_percent REAL NOT NULL,
                        memory_used_bytes BIGINT NOT NULL,
                        memory_total_bytes BIGINT NOT NULL,
                        disk_used_bytes BIGINT,
                        disk_total_bytes BIGINT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
            MetadataPool::Postgres(p) => {
                // Postgres's `REAL` is FLOAT4 (4 bytes) — sqlx enforces
                // exact type matching, and every bind/read here uses `f64`,
                // so this needs `DOUBLE PRECISION` (FLOAT8) instead.
                sqlx::query(
                    r#"
                    CREATE TABLE IF NOT EXISTS resource_stats_samples (
                        sampled_at TEXT NOT NULL PRIMARY KEY,
                        cpu_percent DOUBLE PRECISION NOT NULL,
                        memory_used_bytes BIGINT NOT NULL,
                        memory_total_bytes BIGINT NOT NULL,
                        disk_used_bytes BIGINT,
                        disk_total_bytes BIGINT
                    )
                    "#,
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    pub async fn record(&self, sample: &ResourceSample) -> anyhow::Result<()> {
        let sql = self.q(
            "INSERT INTO resource_stats_samples \
             (sampled_at, cpu_percent, memory_used_bytes, memory_total_bytes, disk_used_bytes, disk_total_bytes) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (sampled_at) DO NOTHING",
        );
        let cpu = sample.cpu_percent as f64;
        let mem_used = sample.memory_used_bytes as i64;
        let mem_total = sample.memory_total_bytes as i64;
        let disk_used = sample.disk_used_bytes.map(|v| v as i64);
        let disk_total = sample.disk_total_bytes.map(|v| v as i64);
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(sample.sampled_at.to_rfc3339())
                    .bind(cpu)
                    .bind(mem_used)
                    .bind(mem_total)
                    .bind(disk_used)
                    .bind(disk_total)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(sample.sampled_at.to_rfc3339())
                    .bind(cpu)
                    .bind(mem_used)
                    .bind(mem_total)
                    .bind(disk_used)
                    .bind(disk_total)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Raw samples with `sampled_at >= cutoff`, ordered oldest-first (what
    /// `bucket_samples` expects).
    pub async fn range(&self, cutoff: DateTime<Utc>) -> anyhow::Result<Vec<ResourceSample>> {
        let sql = self.q(
            "SELECT sampled_at, cpu_percent, memory_used_bytes, memory_total_bytes, \
             disk_used_bytes, disk_total_bytes \
             FROM resource_stats_samples WHERE sampled_at >= ? ORDER BY sampled_at ASC",
        );
        #[allow(clippy::type_complexity)]
        let rows: Vec<(String, f64, i64, i64, Option<i64>, Option<i64>)> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(cutoff.to_rfc3339())
                    .fetch_all(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(cutoff.to_rfc3339())
                    .fetch_all(p)
                    .await?
            }
        };

        Ok(rows
            .into_iter()
            .filter_map(
                |(sampled_at, cpu, mem_used, mem_total, disk_used, disk_total)| {
                    DateTime::parse_from_rfc3339(&sampled_at)
                        .ok()
                        .map(|dt| ResourceSample {
                            sampled_at: dt.with_timezone(&Utc),
                            cpu_percent: cpu as f32,
                            memory_used_bytes: mem_used as u64,
                            memory_total_bytes: mem_total as u64,
                            disk_used_bytes: disk_used.map(|v| v as u64),
                            disk_total_bytes: disk_total.map(|v| v as u64),
                        })
                },
            )
            .collect())
    }

    pub async fn prune_older_than(&self, cutoff: DateTime<Utc>) -> anyhow::Result<()> {
        let sql = self.q("DELETE FROM resource_stats_samples WHERE sampled_at < ?");
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(cutoff.to_rfc3339())
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(cutoff.to_rfc3339())
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }
}

const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);
/// Kept 1 day past `MAX_RANGE` so a query for the full 30-day range never
/// races the prune that would otherwise delete its oldest edge.
const RETENTION: Duration = Duration::from_secs(31 * 24 * 3600);

/// Background sampler — same shape as `scheduler::spawn` (only started from
/// `run()`'s real boot path, never `build_app`, so tests don't get a
/// surprise background task). No leader election (unlike the scheduler):
/// each replica measures its *own* host, there's nothing to deduplicate —
/// and this app is documented as single-node-only for now anyway
/// (`ROADMAP.md`'s "Débitos conhecidos").
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        let data_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let mut monitor = ResourceMonitor::new(data_dir);
        let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
        // First CPU sample right after a fresh `System::new_all()` is
        // meaningless (no prior refresh to diff against) — discard it,
        // same reasoning as `hardware_stats`'s own first-tick handling.
        interval.tick().await;
        monitor.sample();

        loop {
            interval.tick().await;
            let sample = monitor.sample();
            crate::server_metrics::record_resource_sample(&sample);
            if let Err(e) = state.resource_stats.record(&sample).await {
                tracing::warn!(error = %e, "failed to record resource stats sample");
            }
            let cutoff = Utc::now() - chrono::Duration::from_std(RETENTION).expect("fits i64");
            if let Err(e) = state.resource_stats.prune_older_than(cutoff).await {
                tracing::warn!(error = %e, "failed to prune old resource stats samples");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(mount: &str, total: u64, used: u64) -> DiskInfo {
        DiskInfo {
            mount_point: PathBuf::from(mount),
            total_bytes: total,
            used_bytes: used,
        }
    }

    fn sample_at(secs_from_epoch: i64, cpu: f32) -> ResourceSample {
        ResourceSample {
            sampled_at: DateTime::from_timestamp(secs_from_epoch, 0).unwrap(),
            cpu_percent: cpu,
            memory_used_bytes: 1000,
            memory_total_bytes: 2000,
            disk_used_bytes: Some(100),
            disk_total_bytes: Some(200),
        }
    }

    #[test]
    fn find_containing_disk_picks_the_longest_matching_mount() {
        let disks = vec![disk("/", 100, 50), disk("/data", 200, 100)];
        let found = find_containing_disk(&disks, Path::new("/data/nexusflow.db")).unwrap();
        assert_eq!(found.mount_point, PathBuf::from("/data"));
    }

    #[test]
    fn find_containing_disk_falls_back_to_root_when_no_deeper_mount_matches() {
        let disks = vec![disk("/", 100, 50), disk("/data", 200, 100)];
        let found = find_containing_disk(&disks, Path::new("/var/lib/nexusflow.db")).unwrap();
        assert_eq!(found.mount_point, PathBuf::from("/"));
    }

    #[test]
    fn find_containing_disk_returns_none_when_nothing_matches() {
        let disks = vec![disk("/mnt/external", 100, 50)];
        assert!(find_containing_disk(&disks, Path::new("/home/user")).is_none());
    }

    #[test]
    fn parse_range_accepts_presets_and_custom_values() {
        assert_eq!(parse_range("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_range("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_range("45m").unwrap(), Duration::from_secs(2700));
        assert_eq!(parse_range("7d").unwrap(), Duration::from_secs(7 * 86400));
        assert_eq!(parse_range("30d").unwrap(), Duration::from_secs(30 * 86400));
    }

    #[test]
    fn parse_range_rejects_unknown_unit() {
        assert_eq!(parse_range("5s"), Err(RangeParseError::UnknownUnit('s')));
    }

    #[test]
    fn parse_range_rejects_zero() {
        assert_eq!(parse_range("0m"), Err(RangeParseError::Zero));
    }

    #[test]
    fn parse_range_rejects_empty() {
        assert_eq!(parse_range(""), Err(RangeParseError::Empty));
    }

    #[test]
    fn parse_range_rejects_malformed_number() {
        assert_eq!(parse_range("abch"), Err(RangeParseError::InvalidNumber));
    }

    #[test]
    fn parse_range_rejects_over_30_days() {
        assert_eq!(parse_range("31d"), Err(RangeParseError::TooLong));
        assert!(parse_range("30d").is_ok());
    }

    #[test]
    fn bucket_width_for_targets_about_120_points() {
        assert_eq!(
            bucket_width_for(Duration::from_secs(3600)),
            Duration::from_secs(60)
        );
        assert_eq!(
            bucket_width_for(Duration::from_secs(30 * 86400)),
            Duration::from_secs(30 * 86400 / 120)
        );
    }

    #[test]
    fn bucket_width_for_never_goes_below_one_minute() {
        assert_eq!(
            bucket_width_for(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn bucket_samples_on_empty_input_returns_empty() {
        assert!(bucket_samples(&[], Duration::from_secs(60)).is_empty());
    }

    #[test]
    fn bucket_samples_groups_by_fixed_width_window_and_averages() {
        let samples = vec![sample_at(0, 10.0), sample_at(10, 20.0), sample_at(70, 40.0)];
        let buckets = bucket_samples(&samples, Duration::from_secs(60));
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].cpu_percent, 15.0);
        assert_eq!(buckets[1].cpu_percent, 40.0);
    }

    #[test]
    fn bucket_samples_averages_disk_fields_and_skips_none() {
        let mut s1 = sample_at(0, 1.0);
        s1.disk_used_bytes = None;
        s1.disk_total_bytes = None;
        let s2 = sample_at(5, 1.0);
        let buckets = bucket_samples(&[s1, s2], Duration::from_secs(60));
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].disk_used_bytes, Some(100));
        assert_eq!(buckets[0].disk_total_bytes, Some(200));
    }

    #[tokio::test]
    async fn store_records_and_returns_samples_in_range() {
        let store = ResourceStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let sample = sample_at(1_700_000_000, 12.5);
        store.record(&sample).await.unwrap();

        let cutoff = DateTime::from_timestamp(1_699_999_000, 0).unwrap();
        let rows = store.range(cutoff).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cpu_percent, 12.5);
    }

    #[tokio::test]
    async fn store_range_excludes_samples_before_cutoff() {
        let store = ResourceStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.record(&sample_at(1_700_000_000, 1.0)).await.unwrap();

        let cutoff = DateTime::from_timestamp(1_700_000_001, 0).unwrap();
        assert!(store.range(cutoff).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn store_prune_older_than_removes_old_rows_only() {
        let store = ResourceStatsStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.record(&sample_at(1_700_000_000, 1.0)).await.unwrap();
        store.record(&sample_at(1_700_100_000, 2.0)).await.unwrap();

        let cutoff = DateTime::from_timestamp(1_700_050_000, 0).unwrap();
        store.prune_older_than(cutoff).await.unwrap();

        let rows = store
            .range(DateTime::from_timestamp(0, 0).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cpu_percent, 2.0);
    }

    #[tokio::test]
    async fn postgres_backend_records_and_ranges() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = ResourceStatsStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        store.record(&sample_at(1_700_000_000, 33.0)).await.unwrap();
        let rows = store
            .range(DateTime::from_timestamp(0, 0).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cpu_percent, 33.0);
    }
}
