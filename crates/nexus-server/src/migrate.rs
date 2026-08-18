//! One-off SQLite → Postgres migration for the 3 metadata stores (see
//! `db::MetadataPool`) — for deployments moving from a single-replica
//! SQLite setup to a multi-replica Postgres one (k8s) without losing
//! existing users, saved pipelines, run history or checkpoints.
//!
//! **`spec_ciphertext` is copied byte-for-byte, not re-encrypted** — the
//! destination server must run with the *same* `NEXUS_ENCRYPTION_KEY` as the
//! source, or every migrated pipeline's spec fails to decrypt on first load.
//!
//! Idempotent: every insert is `ON CONFLICT ... DO NOTHING` on the row's
//! natural or migrated primary key, so running this twice (e.g. after fixing
//! a connection string) only skips already-migrated rows, never duplicates.

use crate::auth_store::AuthStore;
use crate::checkpoint_store::CheckpointStore;
use crate::db::MetadataPool;
use crate::license_store::LicenseStore;
use crate::pipeline_store::PipelineStore;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{PgPool, Row, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Default)]
pub struct MigrationSummary {
    pub users: usize,
    pub audit_log: usize,
    pub license: usize,
    pub pipelines: usize,
    pub pipeline_runs: usize,
    pub checkpoints: usize,
}

/// Migrates all 3 stores. Each `*_postgres_url` is connected through that
/// store's own `connect()` first (unused here beyond that), so the exact
/// same `CREATE TABLE IF NOT EXISTS` DDL the server itself would run is
/// already in place before any row is copied — this function never
/// duplicates schema knowledge.
pub async fn run(
    auth_sqlite_url: &str,
    auth_postgres_url: &str,
    pipelines_sqlite_url: &str,
    pipelines_postgres_url: &str,
    checkpoint_sqlite_url: &str,
    checkpoint_postgres_url: &str,
) -> anyhow::Result<MigrationSummary> {
    AuthStore::connect(auth_postgres_url).await?;
    LicenseStore::connect(auth_postgres_url).await?;
    PipelineStore::connect(pipelines_postgres_url).await?;
    CheckpointStore::connect(checkpoint_postgres_url).await?;

    let auth_pg = connect_postgres(auth_postgres_url).await?;
    let pipelines_pg = connect_postgres(pipelines_postgres_url).await?;
    let checkpoint_pg = connect_postgres(checkpoint_postgres_url).await?;

    let auth_sqlite = connect_sqlite(auth_sqlite_url).await?;
    let pipelines_sqlite = connect_sqlite(pipelines_sqlite_url).await?;
    let checkpoint_sqlite = connect_sqlite(checkpoint_sqlite_url).await?;

    let users = migrate_users(&auth_sqlite, &auth_pg).await?;
    let audit_log = migrate_audit_log(&auth_sqlite, &auth_pg).await?;
    let license = migrate_license(&auth_sqlite, &auth_pg).await?;
    let pipelines = migrate_pipelines(&pipelines_sqlite, &pipelines_pg).await?;
    let pipeline_runs = migrate_pipeline_runs(&pipelines_sqlite, &pipelines_pg).await?;
    let checkpoints = migrate_checkpoints(&checkpoint_sqlite, &checkpoint_pg).await?;

    Ok(MigrationSummary {
        users,
        audit_log,
        license,
        pipelines,
        pipeline_runs,
        checkpoints,
    })
}

async fn connect_postgres(url: &str) -> anyhow::Result<PgPool> {
    match MetadataPool::connect(url).await? {
        MetadataPool::Postgres(pool) => Ok(pool),
        MetadataPool::Sqlite(_) => {
            anyhow::bail!("expected a postgres:// URL, got a sqlite one: {url:?}")
        }
    }
}

async fn connect_sqlite(url: &str) -> anyhow::Result<SqlitePool> {
    let options = SqliteConnectOptions::from_str(url)?;
    Ok(SqlitePool::connect_with(options).await?)
}

async fn migrate_users(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query("SELECT username, password_hash, role FROM users")
        .fetch_all(sqlite)
        .await?;
    let mut migrated = 0usize;
    for row in rows {
        let username: String = row.try_get("username")?;
        let password_hash: String = row.try_get("password_hash")?;
        let role: String = row.try_get("role")?;
        let result = sqlx::query(
            "INSERT INTO users (username, password_hash, role) VALUES ($1, $2, $3) \
             ON CONFLICT (username) DO NOTHING",
        )
        .bind(username)
        .bind(password_hash)
        .bind(role)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    Ok(migrated)
}

async fn migrate_audit_log(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query("SELECT id, happened_at, username, action, success, ip FROM audit_log")
        .fetch_all(sqlite)
        .await?;
    let mut migrated = 0usize;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let happened_at: String = row.try_get("happened_at")?;
        let username: Option<String> = row.try_get("username")?;
        let action: String = row.try_get("action")?;
        let success: bool = row.try_get("success")?;
        let ip: Option<String> = row.try_get("ip")?;
        // Explicit id, not a fresh BIGSERIAL one — the sequence is caught
        // up via setval() below once every row is in.
        let result = sqlx::query(
            "INSERT INTO audit_log (id, happened_at, username, action, success, ip) \
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(happened_at)
        .bind(username)
        .bind(action)
        .bind(success)
        .bind(ip)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    reset_serial_sequence(pg, "audit_log", "id").await?;
    Ok(migrated)
}

async fn migrate_license(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query("SELECT id, jwt, installed_at FROM license")
        .fetch_all(sqlite)
        .await?;
    let mut migrated = 0usize;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let jwt: String = row.try_get("jwt")?;
        let installed_at: String = row.try_get("installed_at")?;
        let result = sqlx::query(
            "INSERT INTO license (id, jwt, installed_at) VALUES ($1, $2, $3::timestamptz) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(jwt)
        .bind(installed_at)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    Ok(migrated)
}

async fn migrate_pipelines(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query("SELECT id, spec_ciphertext, created_at, updated_at FROM pipelines")
        .fetch_all(sqlite)
        .await?;
    let mut migrated = 0usize;
    for row in rows {
        let id: String = row.try_get("id")?;
        let spec_ciphertext: String = row.try_get("spec_ciphertext")?;
        let created_at: String = row.try_get("created_at")?;
        let updated_at: String = row.try_get("updated_at")?;
        let result = sqlx::query(
            "INSERT INTO pipelines (id, spec_ciphertext, created_at, updated_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(spec_ciphertext)
        .bind(created_at)
        .bind(updated_at)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    Ok(migrated)
}

async fn migrate_pipeline_runs(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        "SELECT id, pipeline_id, started_at, finished_at, status, error, stats_json, \
         dbt_summary_json FROM pipeline_runs",
    )
    .fetch_all(sqlite)
    .await?;
    let mut migrated = 0usize;
    for row in rows {
        let id: i64 = row.try_get("id")?;
        let pipeline_id: String = row.try_get("pipeline_id")?;
        let started_at: String = row.try_get("started_at")?;
        let finished_at: Option<String> = row.try_get("finished_at")?;
        let status: String = row.try_get("status")?;
        let error: Option<String> = row.try_get("error")?;
        let stats_json: Option<String> = row.try_get("stats_json")?;
        let dbt_summary_json: Option<String> = row.try_get("dbt_summary_json")?;
        // Explicit id (see migrate_audit_log's comment) — run_id values
        // already surfaced via the API/logs must keep meaning the same run.
        let result = sqlx::query(
            "INSERT INTO pipeline_runs \
             (id, pipeline_id, started_at, finished_at, status, error, stats_json, dbt_summary_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(pipeline_id)
        .bind(started_at)
        .bind(finished_at)
        .bind(status)
        .bind(error)
        .bind(stats_json)
        .bind(dbt_summary_json)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    reset_serial_sequence(pg, "pipeline_runs", "id").await?;
    Ok(migrated)
}

async fn migrate_checkpoints(sqlite: &SqlitePool, pg: &PgPool) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        r#"SELECT pipeline_id, partition_id, last_updated_at, "offset", updated_at FROM checkpoints"#,
    )
    .fetch_all(sqlite)
    .await?;
    let mut migrated = 0usize;
    for row in rows {
        let pipeline_id: String = row.try_get("pipeline_id")?;
        let partition_id: String = row.try_get("partition_id")?;
        let last_updated_at: Option<String> = row.try_get("last_updated_at")?;
        let offset: Option<i64> = row.try_get("offset")?;
        let updated_at: String = row.try_get("updated_at")?;
        let result = sqlx::query(
            r#"INSERT INTO checkpoints (pipeline_id, partition_id, last_updated_at, "offset", updated_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (pipeline_id, partition_id) DO NOTHING"#,
        )
        .bind(pipeline_id)
        .bind(partition_id)
        .bind(last_updated_at)
        .bind(offset)
        .bind(updated_at)
        .execute(pg)
        .await?;
        migrated += result.rows_affected() as usize;
    }
    Ok(migrated)
}

/// After copying rows with their original (SQLite-assigned) ids into a
/// Postgres `BIGSERIAL` column, the sequence backing that column doesn't
/// know those ids were used — without this, the next *real* insert (a new
/// audit log entry, a new pipeline run) could collide with a migrated id.
/// `pg_get_serial_sequence` looks up the sequence name rather than
/// hardcoding `"{table}_{column}_seq"`, so this stays correct even if
/// Postgres ever names it differently.
async fn reset_serial_sequence(
    pg: &PgPool,
    table: &'static str,
    column: &'static str,
) -> anyhow::Result<()> {
    // `table`/`column` are always hardcoded call-site literals (never user
    // input) — identifiers can't be bind parameters in Postgres, so this
    // has to be a formatted string rather than `$1`/`$2` placeholders.
    let sql = format!(
        "SELECT setval(pg_get_serial_sequence('{table}', '{column}'), \
         COALESCE((SELECT MAX({column}) FROM {table}), 1))"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql)).execute(pg).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Role;
    use crate::crypto::SecretCipher;
    use crate::license::test_support::{claims, sign};
    use nexus_core::{NodeSpec, PipelineSpec};
    use testcontainers_modules::postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    fn sample_spec(id: &str) -> PipelineSpec {
        PipelineSpec {
            pipeline_id: id.to_string(),
            sources: vec![NodeSpec {
                name: None,
                connector: "postgres".to_string(),
                config: serde_json::json!({"uri": "postgres://user:pw@host/db"}),
            }],
            transform: None,
            sinks: vec![NodeSpec {
                name: None,
                connector: "sqlite".to_string(),
                config: serde_json::json!({"path": "/tmp/out.db"}),
            }],
            embedding: None,
            python: None,
            channel_capacity: 100,
            partitions: 1,
            dbt: None,
            post_dbt_sinks: Vec::new(),
            schedule: None,
            draft: false,
        }
    }

    /// Seeds real SQLite files (via the actual stores, so the DDL/data shape
    /// matches production exactly), runs the migration against a real
    /// Postgres, and confirms every row — including a deliberately
    /// non-sequential `pipeline_runs.id` simulating a gap from past
    /// deletions/resets — lands intact with a sequence that won't collide
    /// with the next real insert.
    #[tokio::test]
    async fn migrates_all_three_stores_preserving_ids_and_resetting_sequences() {
        let tmp = tempfile::tempdir().unwrap();
        let auth_sqlite_url = format!("sqlite://{}/auth.db", tmp.path().display());
        let pipelines_sqlite_url = format!("sqlite://{}/pipelines.db", tmp.path().display());
        let checkpoint_sqlite_url = format!("sqlite://{}/checkpoints.db", tmp.path().display());

        // --- seed via the real stores ---
        let auth_store = AuthStore::connect(&auth_sqlite_url).await.unwrap();
        auth_store
            .create_user("alice", "hunter2", Role::Write)
            .await
            .unwrap();

        let license_store = LicenseStore::connect(&auth_sqlite_url).await.unwrap();
        license_store
            .install(&sign(&claims(vec!["snowflake"])))
            .await
            .unwrap();

        let cipher = SecretCipher::from_hex_key(&"ab".repeat(32)).unwrap();
        let pipeline_store = PipelineStore::connect(&pipelines_sqlite_url).await.unwrap();
        pipeline_store
            .create(&sample_spec("p1"), &cipher)
            .await
            .unwrap();
        let run_id = pipeline_store.start_run("p1").await.unwrap();
        pipeline_store
            .finish_run_success(run_id, &[], None)
            .await
            .unwrap();
        // Simulate a historical gap: a run row with a non-sequential id far
        // ahead of the auto-incremented ones above.
        sqlx::query(
            "INSERT INTO pipeline_runs (id, pipeline_id, status) VALUES (100, 'p1', 'success')",
        )
        .execute(&connect_sqlite(&pipelines_sqlite_url).await.unwrap())
        .await
        .unwrap();

        let checkpoint_store = CheckpointStore::connect(&checkpoint_sqlite_url)
            .await
            .unwrap();
        checkpoint_store
            .commit("p1", &nexus_core::CheckpointCursor::new("part-0"))
            .await
            .unwrap();

        // --- migrate to a real Postgres ---
        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let pg_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let summary = run(
            &auth_sqlite_url,
            &pg_url,
            &pipelines_sqlite_url,
            &pg_url,
            &checkpoint_sqlite_url,
            &pg_url,
        )
        .await
        .unwrap();

        assert_eq!(summary.users, 1);
        assert_eq!(summary.license, 1);
        assert_eq!(summary.pipelines, 1);
        assert_eq!(summary.pipeline_runs, 2);
        assert_eq!(summary.checkpoints, 1);

        // --- verify against the real stores, now pointed at Postgres ---
        let auth_pg = AuthStore::connect(&pg_url).await.unwrap();
        assert_eq!(
            auth_pg.verify("alice", "hunter2").await.unwrap(),
            Some(Role::Write)
        );

        let pipelines_pg = PipelineStore::connect(&pg_url).await.unwrap();
        let runs = pipelines_pg.list_runs("p1", 100, 0).await.unwrap();
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|r| r.id == 100));

        // The sequence must be caught up past the migrated id=100 — a fresh
        // start_run must not collide with it.
        let new_run_id = pipelines_pg.start_run("p1").await.unwrap();
        assert!(
            new_run_id > 100,
            "new run id {new_run_id} must be greater than the migrated id 100"
        );

        let checkpoints_pg = CheckpointStore::connect(&pg_url).await.unwrap();
        assert!(checkpoints_pg
            .done_partitions("p1")
            .await
            .unwrap()
            .contains("part-0"));

        // --- idempotent: running it again must not duplicate or error ---
        let second_summary = run(
            &auth_sqlite_url,
            &pg_url,
            &pipelines_sqlite_url,
            &pg_url,
            &checkpoint_sqlite_url,
            &pg_url,
        )
        .await
        .unwrap();
        assert_eq!(second_summary.users, 0, "already-migrated row is skipped");
        assert_eq!(second_summary.pipeline_runs, 0);
    }
}
