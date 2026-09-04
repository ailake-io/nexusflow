//! Native data-quality checks — no dbt required. Companion to dbt-based
//! testing (`dag::DbtConfig`) for CLAUDE.md §4.4's "Modo Padrão", which
//! previously had no per-row quality signal at all beyond a row-count trend
//! (`QualityPanel.tsx`'s `listRuns` history). Evaluated the same way
//! `transform::DataFusionTransform` evaluates a SQL transform: register the
//! batch(es) as a DataFusion `MemTable` and run one aggregate query per
//! check, rather than hand-rolling a scan over Arrow arrays per check type.
use crate::dag::{QualityCheckKind, QualityCheckSpec, QualityFailureAction};
use crate::error::NexusError;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityCheckStatus {
    Pass,
    Fail,
}

impl QualityCheckStatus {
    /// dbt's own status vocabulary ("pass"/"fail"/"warn") — used so a native
    /// check's outcome can be persisted through the same
    /// `dbt_test_result_store` table as a dbt test result, distinguished
    /// only by its `unique_id` prefix (`nexus-server::runner` does this).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QualityCheckOutcome {
    pub column: String,
    /// "not_null" / "unique" / "min" / "max" / "accepted_values".
    pub check: String,
    pub status: QualityCheckStatus,
    pub message: String,
    pub on_failure: QualityFailureAction,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn scalar_count(ctx: &SessionContext, sql: &str) -> Result<i64, NexusError> {
    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| NexusError::Connector(format!("quality check query: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| NexusError::Connector(format!("quality check execution: {e}")))?;
    Ok(batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|arr| arr.value(0))
        .unwrap_or(0))
}

/// Evaluates every check against the given batches (all sharing `schema`,
/// same "one partition, many batches" shape `transform::DataFusionTransform`
/// registers). Known v1 limitation: CDC delete rows (`__opcode = "D"`) are
/// not excluded, so a `not_null`/`min`/`max` check on a column a delete
/// event leaves null/default can report a false failure — checks on a CDC
/// pipeline's output should account for this until a future pass filters
/// them out here.
pub async fn evaluate_checks(
    schema: SchemaRef,
    batches: &[RecordBatch],
    checks: &[QualityCheckSpec],
) -> Result<Vec<QualityCheckOutcome>, NexusError> {
    if checks.is_empty() {
        return Ok(Vec::new());
    }

    let ctx = SessionContext::new();
    let table = MemTable::try_new(schema, vec![batches.to_vec()])
        .map_err(|e| NexusError::Schema(format!("quality check table: {e}")))?;
    ctx.register_table("batch", Arc::new(table))
        .map_err(|e| NexusError::Connector(format!("registering quality check table: {e}")))?;

    let mut outcomes = Vec::with_capacity(checks.len());
    for spec in checks {
        let col = quote_ident(&spec.column);
        let (check_name, failing) = match &spec.check {
            QualityCheckKind::NotNull => {
                let sql = format!("SELECT count(*) FROM batch WHERE {col} IS NULL");
                ("not_null", scalar_count(&ctx, &sql).await?)
            }
            QualityCheckKind::Unique => {
                let sql = format!("SELECT count(*) - count(DISTINCT {col}) FROM batch");
                ("unique", scalar_count(&ctx, &sql).await?)
            }
            QualityCheckKind::Min { value } => {
                let sql = format!("SELECT count(*) FROM batch WHERE {col} < {value}");
                ("min", scalar_count(&ctx, &sql).await?)
            }
            QualityCheckKind::Max { value } => {
                let sql = format!("SELECT count(*) FROM batch WHERE {col} > {value}");
                ("max", scalar_count(&ctx, &sql).await?)
            }
            QualityCheckKind::AcceptedValues { values } => {
                let list = values
                    .iter()
                    .map(|v| quote_literal(v))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!("SELECT count(*) FROM batch WHERE {col} NOT IN ({list})");
                ("accepted_values", scalar_count(&ctx, &sql).await?)
            }
        };

        let status = if failing == 0 {
            QualityCheckStatus::Pass
        } else {
            QualityCheckStatus::Fail
        };
        let message = if failing == 0 {
            format!("{check_name} check on {} passed", spec.column)
        } else {
            format!(
                "{failing} row(s) failed {check_name} check on {}",
                spec.column
            )
        };
        outcomes.push(QualityCheckOutcome {
            column: spec.column.clone(),
            check: check_name.to_string(),
            status,
            message,
            on_failure: spec.on_failure,
        });
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array as Int64Col, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Col::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a@x.com"), None, Some("a@x.com")])),
                Arc::new(Float64Array::from(vec![10.0, -5.0, 999.0])),
                Arc::new(StringArray::from(vec!["ok", "ok", "weird"])),
            ],
        )
        .unwrap()
    }

    fn check(column: &str, kind: QualityCheckKind) -> QualityCheckSpec {
        QualityCheckSpec {
            column: column.to_string(),
            check: kind,
            on_failure: QualityFailureAction::Warn,
        }
    }

    #[tokio::test]
    async fn not_null_reports_a_failure_for_a_null_row() {
        let b = batch();
        let outcomes = evaluate_checks(
            b.schema(),
            &[b],
            &[check("email", QualityCheckKind::NotNull)],
        )
        .await
        .unwrap();
        assert_eq!(outcomes[0].status, QualityCheckStatus::Fail);
        assert!(outcomes[0].message.contains("1 row"));
    }

    #[tokio::test]
    async fn not_null_passes_when_every_row_is_populated() {
        let b = batch();
        let outcomes = evaluate_checks(b.schema(), &[b], &[check("id", QualityCheckKind::NotNull)])
            .await
            .unwrap();
        assert_eq!(outcomes[0].status, QualityCheckStatus::Pass);
    }

    #[tokio::test]
    async fn unique_reports_a_failure_for_a_duplicate_value() {
        let b = batch();
        let outcomes = evaluate_checks(
            b.schema(),
            &[b],
            &[check("email", QualityCheckKind::Unique)],
        )
        .await
        .unwrap();
        // 3 rows, 2 distinct non-null-comparable values ("a@x.com" x2, null) —
        // count(*) - count(DISTINCT col) counts the duplicate pair as 1 over.
        assert_eq!(outcomes[0].status, QualityCheckStatus::Fail);
    }

    #[tokio::test]
    async fn min_and_max_bound_a_numeric_column() {
        let b = batch();
        let outcomes = evaluate_checks(
            b.schema(),
            &[b.clone()],
            &[
                check("amount", QualityCheckKind::Min { value: 0.0 }),
                check("amount", QualityCheckKind::Max { value: 100.0 }),
            ],
        )
        .await
        .unwrap();
        assert_eq!(outcomes[0].status, QualityCheckStatus::Fail); // -5.0 < 0
        assert_eq!(outcomes[1].status, QualityCheckStatus::Fail); // 999.0 > 100
    }

    #[tokio::test]
    async fn accepted_values_rejects_an_out_of_set_value() {
        let b = batch();
        let outcomes = evaluate_checks(
            b.schema(),
            &[b],
            &[check(
                "status",
                QualityCheckKind::AcceptedValues {
                    values: vec!["ok".to_string(), "pending".to_string()],
                },
            )],
        )
        .await
        .unwrap();
        assert_eq!(outcomes[0].status, QualityCheckStatus::Fail); // "weird"
    }

    #[tokio::test]
    async fn empty_checks_short_circuit_without_a_query() {
        let b = batch();
        let outcomes = evaluate_checks(b.schema(), &[b], &[]).await.unwrap();
        assert!(outcomes.is_empty());
    }

    #[tokio::test]
    async fn on_failure_action_is_carried_through_to_the_outcome() {
        let b = batch();
        let mut c = check("email", QualityCheckKind::NotNull);
        c.on_failure = QualityFailureAction::Block;
        let outcomes = evaluate_checks(b.schema(), &[b], &[c]).await.unwrap();
        assert_eq!(outcomes[0].on_failure, QualityFailureAction::Block);
    }
}
