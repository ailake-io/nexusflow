use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// One quality rule to evaluate against a pipeline's output — dbt-independent,
/// same shape a dbt schema test would express but computed directly over
/// the in-memory `RecordBatch`es this pipeline just produced, no dbt project
/// required. Only covers pipelines that fully materialize their output in
/// memory before writing (today: `run_transform_pipeline`, i.e. any
/// pipeline with a Transform node) — the partitioned/passthrough fast paths
/// stream straight to the sink without ever holding the whole result set at
/// once, same reason CDC pipelines already need a `SELECT * FROM source0`
/// Transform node to reach the generic engine (ARCHITECTURE.md's
/// materializing vs. streaming split). Adding a Transform node is the
/// documented way to opt into this, not a silent limitation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QualityCheckSpec {
    pub column: String,
    pub check: QualityCheckKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QualityCheckKind {
    /// Fails if any row has a null in this column.
    NotNull,
    /// Fails if any non-null value in this column repeats.
    Unique,
    /// Fails if any non-null numeric value in this column is below `min`.
    Min { min: f64 },
    /// Fails if any non-null numeric value in this column is above `max`.
    Max { max: f64 },
    /// Fails if any non-null value in this column (compared as its string
    /// display form) isn't one of `values`.
    AcceptedValues { values: Vec<String> },
    /// Fails if the pipeline's total output row count is below `min` or
    /// above `max` (either bound optional — `None` means unbounded on that
    /// side). Unlike every other kind, this doesn't reference a real
    /// output column — `QualityCheckSpec.column` is ignored for this kind
    /// (Fase 27: a coarse, user-configured volume guardrail, distinct from
    /// the automatic per-run volume tracking `pipeline_run_volume_store.rs`
    /// feeds the anomaly detector — that one has no fixed thresholds to
    /// configure, this one does).
    RowCount {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
    },
}

/// One check's result — same "pass"/"fail" vocabulary
/// `dbt_test_result_store::DbtTestOutcome` already uses, so the Quality tab
/// can render both in one list without a third status vocabulary.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QualityCheckOutcome {
    pub column: String,
    /// A short label for the check, e.g. `"not_null"`, `"unique"`,
    /// `"min"`, `"max"`, `"accepted_values"` — matches `QualityCheckKind`'s
    /// serde tag, used as this outcome's stable identity across runs
    /// (paired with `column`) the same way a dbt test's `unique_id` is.
    pub check: String,
    /// "pass" or "fail" — same vocabulary `DbtTestOutcome::status` uses.
    pub status: String,
    /// Present on failure: how many rows violated the check and (for
    /// `accepted_values`) which unexpected values were seen, capped so a
    /// wildly-wrong column doesn't produce a multi-megabyte message.
    pub message: Option<String>,
    /// Structured violation count (Fase 27) — `message` above stays for
    /// human display, this is for aggregation/trending without parsing
    /// that string. `Some(0)` on pass, `Some(n)` on a real per-row
    /// violation count, `None` for the "column not found" config-error
    /// case (not a row-violation count at all).
    pub violation_count: Option<i64>,
    /// Total output rows this check was evaluated against (same for every
    /// check in one `evaluate_quality_checks` call, since they all see the
    /// same `batches`) — the denominator for a violation percentage, and
    /// enough on its own to reconstruct a pass/fail ratio over time
    /// without re-deriving it from `pipeline_run_volume_store`.
    pub sample_size: i64,
}

const MAX_REPORTED_VALUES: usize = 10;

fn column_index(batch: &RecordBatch, name: &str) -> Option<usize> {
    batch.schema().index_of(name).ok()
}

/// Renders one array value at `row` as a display string for messages/
/// accepted-value comparison — reuses Arrow's own `Debug`-free display via
/// `arrow_cast::pretty` would pull in a heavier dependency for a one-line
/// need, so this covers the primitive types `RecordBatchBuilder` (this
/// crate) already supports (int64/float64/boolean/utf8) plus a generic
/// fallback, same "never lose the value, worst case stringify it" posture
/// the bridging connectors already apply.
fn display_value(batch: &RecordBatch, col: usize, row: usize) -> Option<String> {
    let array = batch.column(col);
    if array.is_null(row) {
        return None;
    }
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    let s = match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row).to_string()),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string()),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string()),
        _ => None,
    };
    s
}

fn numeric_value(batch: &RecordBatch, col: usize, row: usize) -> Option<f64> {
    let array = batch.column(col);
    if array.is_null(row) {
        return None;
    }
    use arrow_array::{Float64Array, Int64Array};
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row) as f64),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(row)),
        _ => None,
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn evaluate_one(batches: &[RecordBatch], spec: &QualityCheckSpec) -> QualityCheckOutcome {
    let check_label = match &spec.check {
        QualityCheckKind::NotNull => "not_null",
        QualityCheckKind::Unique => "unique",
        QualityCheckKind::Min { .. } => "min",
        QualityCheckKind::Max { .. } => "max",
        QualityCheckKind::AcceptedValues { .. } => "accepted_values",
        QualityCheckKind::RowCount { .. } => "row_count",
    };
    let sample_size = total_rows(batches) as i64;
    let fail = |message: String, violation_count: i64| QualityCheckOutcome {
        column: spec.column.clone(),
        check: check_label.to_string(),
        status: "fail".to_string(),
        message: Some(message),
        violation_count: Some(violation_count),
        sample_size,
    };
    let fail_no_count = |message: String| QualityCheckOutcome {
        column: spec.column.clone(),
        check: check_label.to_string(),
        status: "fail".to_string(),
        message: Some(message),
        violation_count: None,
        sample_size,
    };
    let pass = || QualityCheckOutcome {
        column: spec.column.clone(),
        check: check_label.to_string(),
        status: "pass".to_string(),
        message: None,
        violation_count: Some(0),
        sample_size,
    };

    // `RowCount` operates on the whole batch set, not a named column — it
    // never reaches the column lookup below (and doesn't need `batches`
    // to be non-empty either: an empty pipeline output is a row count of
    // 0, a real value to check bounds against, not "nothing to check").
    if let QualityCheckKind::RowCount { min, max } = &spec.check {
        let rows = total_rows(batches) as i64;
        let below = min.is_some_and(|m| rows < m);
        let above = max.is_some_and(|m| rows > m);
        return if below || above {
            fail(format!("row count {rows} out of configured bounds"), rows)
        } else {
            QualityCheckOutcome {
                violation_count: Some(rows),
                ..pass()
            }
        };
    }

    // Zero output rows: every remaining check here (not_null/unique/min/
    // max/accepted_values) is vacuously true over an empty set — there's
    // no row to have violated anything, and no schema to validate the
    // column name against either (an empty `Vec<RecordBatch>` carries no
    // schema at all, unlike a batch with 0 rows). Pass rather than fail.
    let Some(first) = batches.first() else {
        return pass();
    };

    // A column the check references but that doesn't exist in this
    // pipeline's output is itself a failure — same "loud, not silent"
    // posture `resource_identifier`'s allowlist takes the opposite way
    // (there, an unmatched connector silently stays unlinked; here, a
    // user explicitly configured a check against a specific column name,
    // so a typo should surface, not vanish). Not a row-violation count,
    // so `violation_count` stays `None`.
    let Some(col_idx) = column_index(first, &spec.column) else {
        return fail_no_count(format!(
            "column {:?} not found in pipeline output",
            spec.column
        ));
    };

    match &spec.check {
        QualityCheckKind::NotNull => {
            let null_count: usize = batches.iter().map(|b| b.column(col_idx).null_count()).sum();
            if null_count == 0 {
                pass()
            } else {
                fail(
                    format!("{null_count} row(s) with a null value"),
                    null_count as i64,
                )
            }
        }
        QualityCheckKind::Unique => {
            let mut seen = HashSet::new();
            let mut duplicates = Vec::new();
            let mut violation_count = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    if let Some(v) = display_value(batch, col_idx, row) {
                        if !seen.insert(v.clone()) {
                            violation_count += 1;
                            if duplicates.len() < MAX_REPORTED_VALUES {
                                duplicates.push(v);
                            }
                        }
                    }
                }
            }
            if duplicates.is_empty() {
                pass()
            } else {
                fail(
                    format!("duplicate value(s) found: {}", duplicates.join(", ")),
                    violation_count,
                )
            }
        }
        QualityCheckKind::Min { min } => {
            let mut violations = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    if let Some(v) = numeric_value(batch, col_idx, row) {
                        if v < *min {
                            violations += 1;
                        }
                    }
                }
            }
            if violations == 0 {
                pass()
            } else {
                fail(format!("{violations} row(s) below minimum {min}"), violations)
            }
        }
        QualityCheckKind::Max { max } => {
            let mut violations = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    if let Some(v) = numeric_value(batch, col_idx, row) {
                        if v > *max {
                            violations += 1;
                        }
                    }
                }
            }
            if violations == 0 {
                pass()
            } else {
                fail(format!("{violations} row(s) above maximum {max}"), violations)
            }
        }
        QualityCheckKind::AcceptedValues { values } => {
            let accepted: HashSet<&str> = values.iter().map(String::as_str).collect();
            let mut unexpected = Vec::new();
            let mut violation_count = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    if let Some(v) = display_value(batch, col_idx, row) {
                        if !accepted.contains(v.as_str()) {
                            violation_count += 1;
                            if !unexpected.contains(&v) && unexpected.len() < MAX_REPORTED_VALUES {
                                unexpected.push(v);
                            }
                        }
                    }
                }
            }
            if unexpected.is_empty() {
                pass()
            } else {
                fail(
                    format!("unexpected value(s): {}", unexpected.join(", ")),
                    violation_count,
                )
            }
        }
        QualityCheckKind::RowCount { .. } => unreachable!("handled above before the column lookup"),
    }
}

/// Evaluates every check against a pipeline's fully-materialized output
/// batches. Pure and DB-free (batches/checks passed in already loaded),
/// same testability rationale as the SQL builders in the connector crates
/// and `transform.rs::column_lineage`. Empty `batches` (a run that
/// produced zero rows) still runs every check — `not_null`/`unique` etc.
/// trivially pass against zero rows, which is correct, not a skip.
pub fn evaluate_quality_checks(
    batches: &[RecordBatch],
    checks: &[QualityCheckSpec],
) -> Vec<QualityCheckOutcome> {
    checks
        .iter()
        .map(|spec| evaluate_one(batches, spec))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[i64], amounts: &[f64], statuses: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
                Arc::new(StringArray::from(statuses.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn not_null_passes_with_no_nulls() {
        let b = batch(&[1, 2], &[1.0, 2.0], &["ok", "ok"]);
        let spec = QualityCheckSpec {
            column: "id".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "pass");
    }

    #[test]
    fn not_null_fails_with_a_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let spec = QualityCheckSpec {
            column: "id".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
        assert!(result[0].message.as_ref().unwrap().contains('1'));
    }

    #[test]
    fn unique_fails_on_a_duplicate() {
        let b = batch(&[1, 1], &[1.0, 2.0], &["a", "b"]);
        let spec = QualityCheckSpec {
            column: "id".to_string(),
            check: QualityCheckKind::Unique,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
    }

    #[test]
    fn min_and_max_flag_out_of_range_values() {
        let b = batch(&[1, 2, 3], &[-5.0, 50.0, 150.0], &["a", "b", "c"]);
        let checks = vec![
            QualityCheckSpec {
                column: "amount".to_string(),
                check: QualityCheckKind::Min { min: 0.0 },
            },
            QualityCheckSpec {
                column: "amount".to_string(),
                check: QualityCheckKind::Max { max: 100.0 },
            },
        ];
        let result = evaluate_quality_checks(&[b], &checks);
        assert_eq!(result[0].status, "fail"); // -5.0 < 0.0
        assert_eq!(result[1].status, "fail"); // 150.0 > 100.0
    }

    #[test]
    fn accepted_values_flags_unexpected_entries() {
        let b = batch(&[1, 2], &[1.0, 2.0], &["active", "bogus"]);
        let spec = QualityCheckSpec {
            column: "status".to_string(),
            check: QualityCheckKind::AcceptedValues {
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
        assert!(result[0].message.as_ref().unwrap().contains("bogus"));
    }

    #[test]
    fn unknown_column_fails_loudly_not_silently() {
        let b = batch(&[1], &[1.0], &["a"]);
        let spec = QualityCheckSpec {
            column: "does_not_exist".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
        assert!(result[0].message.as_ref().unwrap().contains("not found"));
    }

    #[test]
    fn empty_batches_pass_trivially() {
        let result = evaluate_quality_checks(
            &[],
            &[QualityCheckSpec {
                column: "id".to_string(),
                check: QualityCheckKind::NotNull,
            }],
        );
        // Zero output rows can't have violated anything — pass, not fail,
        // and not treated as a "column not found" error either.
        assert_eq!(result[0].status, "pass");
    }

    #[test]
    fn violation_count_and_sample_size_are_structured_on_pass_and_fail() {
        let b = batch(&[1, 2], &[1.0, 2.0], &["ok", "ok"]);
        let spec = QualityCheckSpec {
            column: "id".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].violation_count, Some(0));
        assert_eq!(result[0].sample_size, 2);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, None]))],
        )
        .unwrap();
        let spec = QualityCheckSpec {
            column: "id".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].violation_count, Some(2));
        assert_eq!(result[0].sample_size, 3);
    }

    #[test]
    fn unknown_column_has_no_violation_count() {
        let b = batch(&[1], &[1.0], &["a"]);
        let spec = QualityCheckSpec {
            column: "does_not_exist".to_string(),
            check: QualityCheckKind::NotNull,
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].violation_count, None);
        assert_eq!(result[0].sample_size, 1);
    }

    #[test]
    fn row_count_passes_within_bounds() {
        let b = batch(&[1, 2, 3], &[1.0, 2.0, 3.0], &["a", "b", "c"]);
        let spec = QualityCheckSpec {
            column: String::new(),
            check: QualityCheckKind::RowCount {
                min: Some(1),
                max: Some(10),
            },
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "pass");
        assert_eq!(result[0].violation_count, Some(3));
        assert_eq!(result[0].sample_size, 3);
    }

    #[test]
    fn row_count_fails_below_min() {
        let b = batch(&[1], &[1.0], &["a"]);
        let spec = QualityCheckSpec {
            column: String::new(),
            check: QualityCheckKind::RowCount {
                min: Some(5),
                max: None,
            },
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
        assert_eq!(result[0].violation_count, Some(1));
    }

    #[test]
    fn row_count_fails_above_max() {
        let b = batch(&[1, 2, 3], &[1.0, 2.0, 3.0], &["a", "b", "c"]);
        let spec = QualityCheckSpec {
            column: String::new(),
            check: QualityCheckKind::RowCount {
                min: None,
                max: Some(2),
            },
        };
        let result = evaluate_quality_checks(&[b], &[spec]);
        assert_eq!(result[0].status, "fail");
    }

    #[test]
    fn row_count_on_empty_output_checks_zero_not_vacuous_pass() {
        let spec = QualityCheckSpec {
            column: String::new(),
            check: QualityCheckKind::RowCount {
                min: Some(1),
                max: None,
            },
        };
        let result = evaluate_quality_checks(&[], &[spec]);
        assert_eq!(result[0].status, "fail", "0 rows is below min=1");
        assert_eq!(result[0].violation_count, Some(0));
    }
}
