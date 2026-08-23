use crate::db::{rewrite_placeholders, MetadataPool};
use serde::{Deserialize, Serialize};

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
            }
            MetadataPool::Postgres(p) => {
                sqlx::query(create).execute(p).await?;
            }
        }

        Ok(Self { pool })
    }

    // Called from `runner.rs` after a successful schema/lineage capture —
    // fire-and-forget from the caller's perspective, doesn't fail the run
    // itself if this errors (same posture as dbt lineage/test-result
    // persistence).
    #[allow(dead_code)]
    pub async fn record(
        &self,
        pipeline_id: &str,
        source_columns: &[ColumnInfo],
        output_columns: &[ColumnInfo],
        column_lineage: Option<&[ColumnLineageInfo]>,
    ) -> anyhow::Result<()> {
        let source_columns_json = serde_json::to_string(source_columns)?;
        let output_columns_json = serde_json::to_string(output_columns)?;
        let column_lineage_json = column_lineage.map(serde_json::to_string).transpose()?;
        let captured_at = chrono::Utc::now().to_rfc3339();

        let sql = self.q(
            "INSERT INTO pipeline_schemas \
                (pipeline_id, source_columns_json, output_columns_json, column_lineage_json, captured_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (pipeline_id) DO UPDATE SET \
                 source_columns_json = excluded.source_columns_json, \
                 output_columns_json = excluded.output_columns_json, \
                 column_lineage_json = excluded.column_lineage_json, \
                 captured_at = excluded.captured_at",
        );
        match &self.pool {
            MetadataPool::Sqlite(p) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(pipeline_id)
                    .bind(&source_columns_json)
                    .bind(&output_columns_json)
                    .bind(&column_lineage_json)
                    .bind(&captured_at)
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
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// The Lineage tab's `GET /lineage/{pipeline_id}/schema` endpoint —
    /// `None` when the pipeline has never run (nothing captured yet), not
    /// an error.
    pub async fn get(&self, pipeline_id: &str) -> anyhow::Result<Option<PipelineSchema>> {
        let sql = self.q(
            "SELECT pipeline_id, source_columns_json, output_columns_json, \
                    column_lineage_json, captured_at \
             FROM pipeline_schemas WHERE pipeline_id = ?",
        );
        #[allow(clippy::type_complexity)]
        let row: Option<(String, String, String, Option<String>, String)> = match &self.pool {
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

        let Some((pipeline_id, source_json, output_json, lineage_json, captured_at)) = row else {
            return Ok(None);
        };

        Ok(Some(PipelineSchema {
            pipeline_id,
            source_columns: serde_json::from_str(&source_json)?,
            output_columns: serde_json::from_str(&output_json)?,
            column_lineage: lineage_json.map(|j| serde_json::from_str(&j)).transpose()?,
            captured_at,
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
