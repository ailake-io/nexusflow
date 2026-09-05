use crate::db::{rewrite_placeholders, MetadataPool};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One column's name and Arrow type (`DataType::to_string()`, e.g. `"Int64"`,
/// `"Utf8"`) — plain/serializable, not `arrow_schema::Field` directly, so
/// this store (like `DbtLineageStore`) never depends on a feature-gated or
/// heavyweight type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// One output column's provenance, mirroring
/// `nexus_core::transform::ColumnLineage` but serializable — `source_columns:
/// None` means "not determined" (see `transform.rs::find_column_exprs`'s
/// doc comment for which query shapes that covers), never a guess.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnLineageInfo {
    pub output_column: String,
    pub source_columns: Option<Vec<String>>,
}

/// Latest captured schema for one pipeline — source columns, the schema
/// that actually reaches the sink(s), and (only when the pipeline has a SQL
/// transform) column-level lineage between the two. One row per
/// `pipeline_id`, upserted on every run (current state, not a run history —
/// same posture as `DbtLineageStore`/`resource_stats.rs`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PipelineSchema {
    pub pipeline_id: String,
    pub source_columns: Vec<ColumnInfo>,
    pub output_columns: Vec<ColumnInfo>,
    pub column_lineage: Option<Vec<ColumnLineageInfo>>,
    pub captured_at: String,
    /// Human-readable drift summary between the *previous* capture and
    /// this one (`diff_columns`/`drift_message`, computed in `record`) —
    /// `None` on the first-ever capture or when the last run's schema
    /// matched the one before it. Same message `runner.rs` already fires
    /// as an alert; kept here too so the Lineage tab can show a
    /// "changed since last run" badge without needing its own history.
    pub last_drift: Option<String>,
}

/// Compares two column sets by name and reports what changed, in a
/// stable (sorted) order so the resulting message is deterministic for
/// tests and doesn't jitter between runs when column order alone
/// changes. Returns an empty `Vec` when nothing changed.
fn diff_columns(old: &[ColumnInfo], new: &[ColumnInfo]) -> Vec<String> {
    let old_by_name: HashMap<&str, &str> = old
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();
    let new_by_name: HashMap<&str, &str> = new
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();

    let mut added: Vec<&str> = new_by_name
        .keys()
        .filter(|name| !old_by_name.contains_key(*name))
        .copied()
        .collect();
    let mut removed: Vec<&str> = old_by_name
        .keys()
        .filter(|name| !new_by_name.contains_key(*name))
        .copied()
        .collect();
    let mut retyped: Vec<(&str, &str, &str)> = old_by_name
        .iter()
        .filter_map(|(name, old_ty)| {
            let new_ty = new_by_name.get(name)?;
            (old_ty != new_ty).then_some((*name, *old_ty, *new_ty))
        })
        .collect();
    added.sort_unstable();
    removed.sort_unstable();
    retyped.sort_unstable();

    let mut changes = Vec::new();
    changes.extend(added.into_iter().map(|n| format!("+{n}")));
    changes.extend(removed.into_iter().map(|n| format!("-{n}")));
    changes.extend(
        retyped
            .into_iter()
            .map(|(n, old_ty, new_ty)| format!("{n}: {old_ty}→{new_ty}")),
    );
    changes
}

/// Human-readable drift summary between a pipeline's previous and current
/// captured schema, suitable as an alert message — `None` when nothing
/// changed (including when there's no previous schema to compare against,
/// i.e. this is the pipeline's first run).
fn drift_message(
    previous: &PipelineSchema,
    source: &[ColumnInfo],
    output: &[ColumnInfo],
) -> Option<String> {
    let mut parts = Vec::new();
    let source_diff = diff_columns(&previous.source_columns, source);
    if !source_diff.is_empty() {
        parts.push(format!("fonte: {}", source_diff.join(", ")));
    }
    let output_diff = diff_columns(&previous.output_columns, output);
    if !output_diff.is_empty() {
        parts.push(format!("saída: {}", output_diff.join(", ")));
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("schema mudou — {}", parts.join("; ")))
    }
}

#[derive(Clone)]
pub struct PipelineSchemaStore {
    pool: MetadataPool,
}

impl PipelineSchemaStore {
    fn q(&self, sql: &'static str) -> std::borrow::Cow<'static, str> {
        rewrite_placeholders(sql, self.pool.is_postgres())
    }

    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = MetadataPool::connect(database_url).await?;

        let create = r#"
            CREATE TABLE IF NOT EXISTS pipeline_schemas (
                pipeline_id TEXT NOT NULL PRIMARY KEY,
                source_columns_json TEXT NOT NULL,
                output_columns_json TEXT NOT NULL,
                column_lineage_json TEXT,
                captured_at TEXT NOT NULL
            )
        "#;
        match &pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(create).execute(p).await?;
                // Same additive-migration pattern as
                // `checkpoint_store.rs`'s `resume_state` column — no
                // `IF NOT EXISTS` support for `ADD COLUMN` on older
                // SQLite, so this just runs it and ignores the error
                // ("column already exists" on a second/later boot).
                let _ = sqlx::query("ALTER TABLE pipeline_schemas ADD COLUMN last_drift TEXT")
                    .execute(p)
                    .await;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
                sqlx::query(
                    "ALTER TABLE pipeline_schemas ADD COLUMN IF NOT EXISTS last_drift TEXT",
                )
                .execute(p)
                .await?;
            }
        }

        Ok(Self { pool })
    }

    // Called from `runner.rs` after a successful schema/lineage capture —
    // fire-and-forget from the caller's perspective, doesn't fail the run
    // itself if this errors (same posture as dbt lineage/test-result
    // persistence). Returns a human-readable drift message when this
    // run's schema differs from the pipeline's previously captured one
    // (`None` on the first-ever capture, or when nothing changed) — the
    // caller decides what to do with it (today: fire an alert via the
    // same channels a run success/failure already uses).
    #[allow(dead_code)]
    pub async fn record(
        &self,
        pipeline_id: &str,
        source_columns: &[ColumnInfo],
        output_columns: &[ColumnInfo],
        column_lineage: Option<&[ColumnLineageInfo]>,
    ) -> anyhow::Result<Option<String>> {
        let previous = self.get(pipeline_id).await?;
        let drift = previous
            .as_ref()
            .and_then(|prev| drift_message(prev, source_columns, output_columns));

        let source_columns_json = serde_json::to_string(source_columns)?;
        let output_columns_json = serde_json::to_string(output_columns)?;
        let column_lineage_json = column_lineage.map(serde_json::to_string).transpose()?;
        let captured_at = chrono::Utc::now().to_rfc3339();

        let sql = self.q(
            "INSERT INTO pipeline_schemas \
                (pipeline_id, source_columns_json, output_columns_json, column_lineage_json, captured_at, last_drift) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT (pipeline_id) DO UPDATE SET \
                 source_columns_json = excluded.source_columns_json, \
                 output_columns_json = excluded.output_columns_json, \
                 column_lineage_json = excluded.column_lineage_json, \
                 captured_at = excluded.captured_at, \
                 last_drift = excluded.last_drift",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(&source_columns_json)
                    .bind(&output_columns_json)
                    .bind(&column_lineage_json)
                    .bind(&captured_at)
                    .bind(&drift)
                    .execute(p)
                    .await?;
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(&source_columns_json)
                    .bind(&output_columns_json)
                    .bind(&column_lineage_json)
                    .bind(&captured_at)
                    .bind(&drift)
                    .execute(p)
                    .await?;
            }
        }
        Ok(drift)
    }

    /// The Lineage tab's `GET /lineage/{pipeline_id}/schema` endpoint —
    /// `None` when the pipeline has never run (nothing captured yet), not
    /// an error.
    pub async fn get(&self, pipeline_id: &str) -> anyhow::Result<Option<PipelineSchema>> {
        let sql = self.q(
            "SELECT pipeline_id, source_columns_json, output_columns_json, \
                    column_lineage_json, captured_at, last_drift \
             FROM pipeline_schemas WHERE pipeline_id = ?",
        );
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            String,
            Option<String>,
            String,
            Option<String>,
        )> = match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
            MetadataPool::Postgres(p) => {
                sqlx::query_as(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .fetch_optional(p)
                    .await?
            }
        };

        let Some((pipeline_id, source_json, output_json, lineage_json, captured_at, last_drift)) =
            row
        else {
            return Ok(None);
        };

        Ok(Some(PipelineSchema {
            pipeline_id,
            source_columns: serde_json::from_str(&source_json)?,
            output_columns: serde_json::from_str(&output_json)?,
            column_lineage: lineage_json.map(|j| serde_json::from_str(&j)).transpose()?,
            captured_at,
            last_drift,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(names: &[&str], types: &[&str]) -> Vec<ColumnInfo> {
        names
            .iter()
            .zip(types)
            .map(|(n, t)| ColumnInfo {
                name: n.to_string(),
                data_type: t.to_string(),
            })
            .collect()
    }

    #[tokio::test]
    async fn record_and_get_round_trips_without_lineage() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let source = cols(&["id", "amount"], &["Int64", "Int64"]);
        let output = source.clone();
        store
            .record("pipe-1", &source, &output, None)
            .await
            .unwrap();

        let got = store.get("pipe-1").await.unwrap().expect("row exists");
        assert_eq!(got.source_columns, source);
        assert_eq!(got.output_columns, output);
        assert_eq!(got.column_lineage, None);
    }

    #[tokio::test]
    async fn record_and_get_round_trips_with_lineage() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let source = cols(&["id", "amount"], &["Int64", "Int64"]);
        let output = cols(&["id", "total"], &["Int64", "Int64"]);
        let lineage = vec![
            ColumnLineageInfo {
                output_column: "id".to_string(),
                source_columns: Some(vec!["id".to_string()]),
            },
            ColumnLineageInfo {
                output_column: "total".to_string(),
                source_columns: None,
            },
        ];
        store
            .record("pipe-1", &source, &output, Some(&lineage))
            .await
            .unwrap();

        let got = store.get("pipe-1").await.unwrap().expect("row exists");
        assert_eq!(got.column_lineage, Some(lineage));
    }

    #[tokio::test]
    async fn record_is_an_upsert_not_a_history() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let first = cols(&["a"], &["Int64"]);
        store.record("pipe-1", &first, &first, None).await.unwrap();

        let second = cols(&["b"], &["Utf8"]);
        store
            .record("pipe-1", &second, &second, None)
            .await
            .unwrap();

        let got = store.get("pipe-1").await.unwrap().expect("row exists");
        assert_eq!(got.source_columns, second);
    }

    #[tokio::test]
    async fn record_returns_no_drift_on_first_capture() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let source = cols(&["id"], &["Int64"]);
        let drift = store
            .record("pipe-1", &source, &source, None)
            .await
            .unwrap();
        assert_eq!(drift, None);
    }

    #[tokio::test]
    async fn record_returns_no_drift_when_schema_is_unchanged() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let source = cols(&["id", "amount"], &["Int64", "Int64"]);
        store
            .record("pipe-1", &source, &source, None)
            .await
            .unwrap();
        let drift = store
            .record("pipe-1", &source, &source, None)
            .await
            .unwrap();
        assert_eq!(drift, None);
    }

    #[tokio::test]
    async fn record_reports_added_removed_and_retyped_columns() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let first = cols(&["id", "amount", "status"], &["Int64", "Int64", "Utf8"]);
        store.record("pipe-1", &first, &first, None).await.unwrap();

        let second = cols(&["id", "amount", "region"], &["Int64", "Utf8", "Utf8"]);
        let drift = store
            .record("pipe-1", &second, &second, None)
            .await
            .unwrap()
            .expect("schema changed, drift expected");

        assert!(drift.contains("+region"), "drift message: {drift}");
        assert!(drift.contains("-status"), "drift message: {drift}");
        assert!(
            drift.contains("amount: Int64→Utf8"),
            "drift message: {drift}"
        );
    }

    #[tokio::test]
    async fn record_diffs_source_and_output_independently() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let source1 = cols(&["id", "amount"], &["Int64", "Int64"]);
        let output1 = cols(&["id", "total"], &["Int64", "Int64"]);
        store
            .record("pipe-1", &source1, &output1, None)
            .await
            .unwrap();

        // Source gains a column, output is untouched.
        let source2 = cols(&["id", "amount", "region"], &["Int64", "Int64", "Utf8"]);
        let drift = store
            .record("pipe-1", &source2, &output1, None)
            .await
            .unwrap()
            .expect("source changed, drift expected");

        assert!(drift.contains("fonte:"), "drift message: {drift}");
        assert!(!drift.contains("saída:"), "drift message: {drift}");
    }

    #[tokio::test]
    async fn get_reflects_last_drift_after_a_changing_run() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let first = cols(&["id", "amount"], &["Int64", "Int64"]);
        store.record("pipe-1", &first, &first, None).await.unwrap();
        assert_eq!(store.get("pipe-1").await.unwrap().unwrap().last_drift, None);

        let second = cols(&["id", "region"], &["Int64", "Utf8"]);
        store
            .record("pipe-1", &second, &second, None)
            .await
            .unwrap();
        let last_drift = store.get("pipe-1").await.unwrap().unwrap().last_drift;
        assert!(last_drift.is_some(), "expected last_drift to be set");

        // A third run with no further change clears it back to None — the
        // field reflects "did the *last* capture change anything", not a
        // sticky flag.
        store
            .record("pipe-1", &second, &second, None)
            .await
            .unwrap();
        assert_eq!(store.get("pipe-1").await.unwrap().unwrap().last_drift, None);
    }

    #[tokio::test]
    async fn get_returns_none_for_unknown_pipeline() {
        let store = PipelineSchemaStore::connect("sqlite::memory:")
            .await
            .unwrap();
        assert!(store.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn postgres_backend_records_and_gets() {
        use testcontainers_modules::postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let container = postgres::Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

        let store = PipelineSchemaStore::connect(&url).await.unwrap();
        assert!(matches!(store.pool, MetadataPool::Postgres(_)));

        let source = cols(&["id"], &["Int64"]);
        store
            .record("pipe-1", &source, &source, None)
            .await
            .unwrap();

        let got = store.get("pipe-1").await.unwrap().expect("row exists");
        assert_eq!(got.source_columns, source);
    }
}
