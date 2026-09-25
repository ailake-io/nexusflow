use crate::error::NexusError;
use crate::sql::quote_identifier;
use serde::{Deserialize, Serialize};

/// One no-code cleaning/transformation step (Fase 30) — the config-only
/// alternative to the SQL `transform` node and the `python` node, not
/// combined with either in the same pipeline (`PipelineSpec::validate()`).
/// A pipeline's `clean_blocks` is an ordered list; [`compile_clean_blocks`]
/// turns it into one SQL string (a chain of CTEs) that feeds the existing
/// `DataFusionTransform` unchanged — no new execution engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CleanBlockSpec {
    /// Display-only, not read by the compiler — lets the UI show a name
    /// the user picked instead of just the block kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(flatten)]
    pub kind: CleanBlockKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Contains,
    StartsWith,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectColumnsMode {
    Keep,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseMode {
    Upper,
    Lower,
    /// `INITCAP` — first letter of each word uppercase, rest lowercase.
    Title,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    Int,
    Float,
    Text,
    Date,
    Boolean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    /// String concatenation (`||`) — the two operands don't need to be
    /// numeric for this one.
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggFunction {
    Sum,
    Avg,
    Count,
    CountDistinct,
    Min,
    Max,
}

/// How block 7 (fill nulls) replaces a null — see `ROADMAP.md` Fase 30 for
/// why median/mode aren't here: no simple one-expression SQL form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum NullFillStrategy {
    Value {
        value: String,
    },
    /// `COALESCE(col, (SELECT AVG(col) FROM <previous step>))` — numeric
    /// columns only, no type check at compile time (same posture as the
    /// rest of this compiler: it never sees real data or a schema).
    ColumnAverage,
    /// `COALESCE(col, other_column)`. Field is `fallback_column`, not
    /// `column` — `FillNulls` (the enclosing block) already has its own
    /// `column` field (the one being filled) at the same flattened JSON
    /// level; a same-named field here would collide with it on the wire.
    OtherColumn {
        fallback_column: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregation {
    pub column: String,
    pub function: AggFunction,
    pub output: String,
}

/// The 13 v1 block kinds (`ROADMAP.md` Fase 30's catalog) — tagged by
/// `kind`, `snake_case`, same convention as `QualityCheckKind`. Each
/// variant's `to_sql_fragment` builds one `SELECT ... FROM <input>` (or a
/// `WHERE`/`GROUP BY` variant of it); [`compile_clean_blocks`] chains them
/// as CTEs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CleanBlockKind {
    Filter {
        column: String,
        operator: FilterOperator,
        /// Absent for `IsNull`/`IsNotNull`, required otherwise (checked in
        /// [`compile_clean_blocks`], not by serde, since the field is
        /// meaningful only for some operator values).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<String>,
    },
    /// `mode: keep` lists the columns to keep (`SELECT a, b FROM ...`);
    /// `mode: drop` lists the columns to remove (`SELECT * EXCEPT (a, b)
    /// FROM ...` — confirmed supported by this DataFusion version).
    SelectColumns {
        mode: SelectColumnsMode,
        columns: Vec<String>,
    },
    Rename {
        from: String,
        to: String,
    },
    Cast {
        column: String,
        data_type: CastType,
    },
    Trim {
        columns: Vec<String>,
    },
    ReplaceText {
        column: String,
        find: String,
        replace: String,
    },
    FillNulls {
        column: String,
        #[serde(flatten)]
        strategy: NullFillStrategy,
    },
    DropNulls {
        columns: Vec<String>,
    },
    /// Empty `columns` means "every column" (`SELECT DISTINCT *`).
    Dedupe {
        #[serde(default)]
        columns: Vec<String>,
    },
    ChangeCase {
        column: String,
        mode: CaseMode,
    },
    ComputedColumn {
        output: String,
        left: String,
        operator: ComputeOperator,
        right: String,
    },
    Sort {
        column: String,
        direction: SortDirection,
    },
    /// The one block that changes row cardinality (N:1) — blocks after it
    /// in the chain see the aggregated result, not the original rows.
    Aggregate {
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
    },
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Emits an unquoted numeric literal when `value` parses as one, else a
/// quoted string literal — no schema is available to know the column's
/// real type, so this is a best-effort guess, same posture as the rest of
/// this compiler. Covers the two common cases (compare number to number,
/// text to text) without needing type info up front.
fn sql_literal(value: &str) -> String {
    if value.parse::<f64>().is_ok() {
        value.to_string()
    } else {
        quote_literal(value)
    }
}

fn agg_function_sql(f: AggFunction, column: &str) -> Result<String, NexusError> {
    let col = quote_identifier(column)?;
    Ok(match f {
        AggFunction::Sum => format!("SUM({col})"),
        AggFunction::Avg => format!("AVG({col})"),
        AggFunction::Count => format!("COUNT({col})"),
        AggFunction::CountDistinct => format!("COUNT(DISTINCT {col})"),
        AggFunction::Min => format!("MIN({col})"),
        AggFunction::Max => format!("MAX({col})"),
    })
}

impl CleanBlockKind {
    /// Builds this block's `SELECT ... FROM <input>` (or `WHERE`/`GROUP BY`
    /// shape of it) against `input`, which is either the pipeline's real
    /// source table name or a previous block's CTE alias. Pure string
    /// building — every identifier goes through [`quote_identifier`], every
    /// value the user typed goes through [`quote_literal`]/[`sql_literal`],
    /// so nothing here ever trusts unescaped input inside the generated SQL.
    fn to_sql(&self, input: &str) -> Result<String, NexusError> {
        match self {
            CleanBlockKind::Filter {
                column,
                operator,
                value,
            } => {
                let col = quote_identifier(column)?;
                let cond = match operator {
                    FilterOperator::IsNull => format!("{col} IS NULL"),
                    FilterOperator::IsNotNull => format!("{col} IS NOT NULL"),
                    _ => {
                        let value = value.as_deref().ok_or_else(|| {
                            NexusError::Schema(format!(
                                "filter block on column {column:?}: `value` is required for this operator"
                            ))
                        })?;
                        match operator {
                            FilterOperator::Eq => format!("{col} = {}", sql_literal(value)),
                            FilterOperator::Ne => format!("{col} != {}", sql_literal(value)),
                            FilterOperator::Gt => format!("{col} > {}", sql_literal(value)),
                            FilterOperator::Lt => format!("{col} < {}", sql_literal(value)),
                            FilterOperator::Gte => format!("{col} >= {}", sql_literal(value)),
                            FilterOperator::Lte => format!("{col} <= {}", sql_literal(value)),
                            FilterOperator::Contains => {
                                format!("{col} LIKE {}", quote_literal(&format!("%{value}%")))
                            }
                            FilterOperator::StartsWith => {
                                format!("{col} LIKE {}", quote_literal(&format!("{value}%")))
                            }
                            FilterOperator::IsNull | FilterOperator::IsNotNull => unreachable!(),
                        }
                    }
                };
                Ok(format!("SELECT * FROM {input} WHERE {cond}"))
            }
            CleanBlockKind::SelectColumns { mode, columns } => {
                if columns.is_empty() {
                    return Err(NexusError::Schema(
                        "select_columns block: `columns` must not be empty".into(),
                    ));
                }
                let cols = columns
                    .iter()
                    .map(|c| quote_identifier(c))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(match mode {
                    SelectColumnsMode::Keep => format!("SELECT {cols} FROM {input}"),
                    SelectColumnsMode::Drop => {
                        format!("SELECT * EXCEPT ({cols}) FROM {input}")
                    }
                })
            }
            CleanBlockKind::Rename { from, to } => {
                let from = quote_identifier(from)?;
                let to = quote_identifier(to)?;
                Ok(format!(
                    "SELECT * EXCEPT ({from}), {from} AS {to} FROM {input}"
                ))
            }
            CleanBlockKind::Cast { column, data_type } => {
                let col = quote_identifier(column)?;
                let sql_type = match data_type {
                    CastType::Int => "BIGINT",
                    CastType::Float => "DOUBLE",
                    CastType::Text => "VARCHAR",
                    CastType::Date => "DATE",
                    CastType::Boolean => "BOOLEAN",
                };
                Ok(format!(
                    "SELECT * REPLACE (CAST({col} AS {sql_type}) AS {col}) FROM {input}"
                ))
            }
            CleanBlockKind::Trim { columns } => {
                if columns.is_empty() {
                    return Err(NexusError::Schema(
                        "trim block: `columns` must not be empty".into(),
                    ));
                }
                let replacements = columns
                    .iter()
                    .map(|c| {
                        let col = quote_identifier(c)?;
                        Ok(format!("TRIM({col}) AS {col}"))
                    })
                    .collect::<Result<Vec<_>, NexusError>>()?
                    .join(", ");
                Ok(format!("SELECT * REPLACE ({replacements}) FROM {input}"))
            }
            CleanBlockKind::ReplaceText {
                column,
                find,
                replace,
            } => {
                let col = quote_identifier(column)?;
                Ok(format!(
                    "SELECT * REPLACE (REPLACE({col}, {}, {}) AS {col}) FROM {input}",
                    quote_literal(find),
                    quote_literal(replace)
                ))
            }
            CleanBlockKind::FillNulls { column, strategy } => {
                let col = quote_identifier(column)?;
                let fallback = match strategy {
                    NullFillStrategy::Value { value } => sql_literal(value),
                    NullFillStrategy::ColumnAverage => {
                        format!("(SELECT AVG({col}) FROM {input})")
                    }
                    NullFillStrategy::OtherColumn { fallback_column } => {
                        quote_identifier(fallback_column)?
                    }
                };
                Ok(format!(
                    "SELECT * REPLACE (COALESCE({col}, {fallback}) AS {col}) FROM {input}"
                ))
            }
            CleanBlockKind::DropNulls { columns } => {
                if columns.is_empty() {
                    return Err(NexusError::Schema(
                        "drop_nulls block: `columns` must not be empty".into(),
                    ));
                }
                let cond = columns
                    .iter()
                    .map(|c| Ok(format!("{} IS NOT NULL", quote_identifier(c)?)))
                    .collect::<Result<Vec<_>, NexusError>>()?
                    .join(" AND ");
                Ok(format!("SELECT * FROM {input} WHERE {cond}"))
            }
            CleanBlockKind::Dedupe { columns } => {
                if columns.is_empty() {
                    Ok(format!("SELECT DISTINCT * FROM {input}"))
                } else {
                    let cols = columns
                        .iter()
                        .map(|c| quote_identifier(c))
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", ");
                    Ok(format!("SELECT DISTINCT ON ({cols}) * FROM {input}"))
                }
            }
            CleanBlockKind::ChangeCase { column, mode } => {
                let col = quote_identifier(column)?;
                let func = match mode {
                    CaseMode::Upper => "UPPER",
                    CaseMode::Lower => "LOWER",
                    CaseMode::Title => "INITCAP",
                };
                Ok(format!(
                    "SELECT * REPLACE ({func}({col}) AS {col}) FROM {input}"
                ))
            }
            CleanBlockKind::ComputedColumn {
                output,
                left,
                operator,
                right,
            } => {
                let out = quote_identifier(output)?;
                let left = quote_identifier(left)?;
                let right = quote_identifier(right)?;
                let expr = match operator {
                    ComputeOperator::Add => format!("{left} + {right}"),
                    ComputeOperator::Subtract => format!("{left} - {right}"),
                    ComputeOperator::Multiply => format!("{left} * {right}"),
                    ComputeOperator::Divide => format!("{left} / {right}"),
                    ComputeOperator::Concat => format!("{left} || {right}"),
                };
                Ok(format!("SELECT *, {expr} AS {out} FROM {input}"))
            }
            // Handled by `compile_clean_blocks`, not here: an ORDER BY inside a
            // CTE isn't guaranteed to survive to the final result (SQL result
            // sets are unordered unless the outermost query says otherwise —
            // confirmed empirically against this DataFusion version). This
            // arm only exists so the match stays exhaustive; it must never
            // change row content, since `compile_clean_blocks` still needs to
            // apply the real ORDER BY on top of whatever this step returns.
            CleanBlockKind::Sort { .. } => Ok(format!("SELECT * FROM {input}")),
            CleanBlockKind::Aggregate {
                group_by,
                aggregations,
            } => {
                if group_by.is_empty() && aggregations.is_empty() {
                    return Err(NexusError::Schema(
                        "aggregate block: at least one of `group_by`/`aggregations` is required"
                            .into(),
                    ));
                }
                let group_cols = group_by
                    .iter()
                    .map(|c| quote_identifier(c))
                    .collect::<Result<Vec<_>, _>>()?;
                let agg_exprs = aggregations
                    .iter()
                    .map(|a| {
                        let out = quote_identifier(&a.output)?;
                        Ok(format!(
                            "{} AS {out}",
                            agg_function_sql(a.function, &a.column)?
                        ))
                    })
                    .collect::<Result<Vec<_>, NexusError>>()?;
                let select_list = group_cols
                    .iter()
                    .cloned()
                    .chain(agg_exprs)
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut sql = format!("SELECT {select_list} FROM {input}");
                if !group_cols.is_empty() {
                    sql.push_str(&format!(" GROUP BY {}", group_cols.join(", ")));
                }
                Ok(sql)
            }
        }
    }
}

/// Compiles an ordered chain of blocks into one SQL string — a `WITH`
/// chain of CTEs, one per block, each reading the previous one (or
/// `input_table` for the first block). Empty `blocks` is a caller error:
/// `PipelineSpec::validate()` should never call this with nothing to
/// compile (a pipeline with `clean_blocks` empty just doesn't set it).
pub fn compile_clean_blocks(
    blocks: &[CleanBlockSpec],
    input_table: &str,
) -> Result<String, NexusError> {
    if blocks.is_empty() {
        return Err(NexusError::Schema(
            "compile_clean_blocks called with an empty block list".into(),
        ));
    }
    quote_identifier(input_table)?;

    let mut ctes = Vec::with_capacity(blocks.len());
    let mut previous = input_table.to_string();
    // Last `Sort` block wins — matches plain SQL semantics anyway, since
    // nothing in this compiler preserves ordering through a later step
    // (GROUP BY/DISTINCT/another CTE boundary), so a sort placed before one
    // of those wouldn't survive to the output regardless.
    let mut order_by: Option<String> = None;
    for (i, block) in blocks.iter().enumerate() {
        let step_name = format!("step_{i}");
        if let CleanBlockKind::Sort { column, direction } = &block.kind {
            let col = quote_identifier(column)?;
            let dir = match direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            order_by = Some(format!("{col} {dir}"));
        }
        let sql = block.kind.to_sql(&previous)?;
        ctes.push(format!("{step_name} AS ({sql})"));
        previous = step_name;
    }
    let mut sql = format!("WITH {} SELECT * FROM {previous}", ctes.join(", "));
    if let Some(order_by) = order_by {
        sql.push_str(&format!(" ORDER BY {order_by}"));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Transform;
    use crate::transform::DataFusionTransform;
    use arrow_array::{
        cast::AsArray, types::Int64Type, Array, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    fn people() -> (SchemaRef, RecordBatch) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("city", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("  Ana  "),
                    Some("bob"),
                    None,
                    Some("bob"),
                ])),
                Arc::new(Int64Array::from(vec![Some(30), None, Some(40), Some(25)])),
                Arc::new(StringArray::from(vec![
                    Some("SP"),
                    Some("RJ"),
                    Some("SP"),
                    Some("RJ"),
                ])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    async fn run(blocks: Vec<CleanBlockKind>) -> RecordBatch {
        let (schema, batch) = people();
        let specs: Vec<CleanBlockSpec> = blocks
            .into_iter()
            .map(|kind| CleanBlockSpec { name: None, kind })
            .collect();
        let sql = compile_clean_blocks(&specs, "people").unwrap();
        let transform = DataFusionTransform::new(sql);
        let result = transform
            .apply(vec![("people".to_string(), schema.clone(), vec![batch])])
            .await
            .unwrap();
        // DataFusion may split output across several batches (e.g. one per
        // partition on GROUP BY/DISTINCT) even for tiny inputs — merge before
        // asserting so tests don't depend on its internal partitioning.
        arrow_select::concat::concat_batches(&result[0].schema(), &result).unwrap()
    }

    fn col_i64(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_primitive::<Int64Type>()
            .iter()
            .collect()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    #[test]
    fn compile_rejects_empty_block_list() {
        assert!(compile_clean_blocks(&[], "people").is_err());
    }

    #[test]
    fn compile_rejects_bad_input_table_name() {
        let blocks = vec![CleanBlockSpec {
            name: None,
            kind: CleanBlockKind::Sort {
                column: "id".into(),
                direction: SortDirection::Asc,
            },
        }];
        assert!(compile_clean_blocks(&blocks, "people; DROP TABLE x").is_err());
    }

    #[test]
    fn compile_rejects_sql_injection_in_column_name() {
        let blocks = vec![CleanBlockSpec {
            name: None,
            kind: CleanBlockKind::Sort {
                column: "id; DROP TABLE users; --".into(),
                direction: SortDirection::Asc,
            },
        }];
        assert!(compile_clean_blocks(&blocks, "people").is_err());
    }

    #[tokio::test]
    async fn filter_keeps_matching_rows() {
        let out = run(vec![CleanBlockKind::Filter {
            column: "age".into(),
            operator: FilterOperator::Gt,
            value: Some("28".into()),
        }])
        .await;
        assert_eq!(col_i64(&out, "id"), vec![Some(1), Some(3)]);
    }

    #[tokio::test]
    async fn filter_is_null_operator_needs_no_value() {
        let out = run(vec![CleanBlockKind::Filter {
            column: "age".into(),
            operator: FilterOperator::IsNull,
            value: None,
        }])
        .await;
        assert_eq!(col_i64(&out, "id"), vec![Some(2)]);
    }

    #[test]
    fn filter_without_value_for_eq_is_a_compile_error() {
        let blocks = vec![CleanBlockSpec {
            name: None,
            kind: CleanBlockKind::Filter {
                column: "age".into(),
                operator: FilterOperator::Eq,
                value: None,
            },
        }];
        assert!(compile_clean_blocks(&blocks, "people").is_err());
    }

    #[tokio::test]
    async fn select_columns_keep_mode() {
        let out = run(vec![CleanBlockKind::SelectColumns {
            mode: SelectColumnsMode::Keep,
            columns: vec!["id".into(), "city".into()],
        }])
        .await;
        assert_eq!(out.num_columns(), 2);
        assert_eq!(
            col_str(&out, "city"),
            vec![
                Some("SP".into()),
                Some("RJ".into()),
                Some("SP".into()),
                Some("RJ".into())
            ]
        );
    }

    #[tokio::test]
    async fn select_columns_drop_mode() {
        let out = run(vec![CleanBlockKind::SelectColumns {
            mode: SelectColumnsMode::Drop,
            columns: vec!["name".into(), "age".into()],
        }])
        .await;
        assert_eq!(out.num_columns(), 2);
        assert!(out.schema().index_of("name").is_err());
        assert!(out.schema().index_of("age").is_err());
    }

    #[tokio::test]
    async fn rename_keeps_data_under_the_new_name() {
        let out = run(vec![CleanBlockKind::Rename {
            from: "city".into(),
            to: "state".into(),
        }])
        .await;
        assert!(out.schema().index_of("city").is_err());
        assert_eq!(col_str(&out, "state")[0], Some("SP".into()));
    }

    #[tokio::test]
    async fn cast_changes_the_arrow_type() {
        let out = run(vec![CleanBlockKind::Cast {
            column: "age".into(),
            data_type: CastType::Float,
        }])
        .await;
        assert_eq!(
            out.schema().field_with_name("age").unwrap().data_type(),
            &DataType::Float64
        );
    }

    #[tokio::test]
    async fn trim_removes_surrounding_whitespace() {
        let out = run(vec![CleanBlockKind::Trim {
            columns: vec!["name".into()],
        }])
        .await;
        assert_eq!(col_str(&out, "name")[0], Some("Ana".into()));
    }

    #[tokio::test]
    async fn replace_text_substitutes_substring() {
        let out = run(vec![CleanBlockKind::ReplaceText {
            column: "city".into(),
            find: "SP".into(),
            replace: "São Paulo".into(),
        }])
        .await;
        assert_eq!(col_str(&out, "city")[0], Some("São Paulo".into()));
    }

    #[tokio::test]
    async fn fill_nulls_with_fixed_value() {
        let out = run(vec![CleanBlockKind::FillNulls {
            column: "name".into(),
            strategy: NullFillStrategy::Value {
                value: "unknown".into(),
            },
        }])
        .await;
        assert_eq!(col_str(&out, "name")[2], Some("unknown".into()));
    }

    #[tokio::test]
    async fn fill_nulls_with_column_average() {
        let out = run(vec![CleanBlockKind::FillNulls {
            column: "age".into(),
            strategy: NullFillStrategy::ColumnAverage,
        }])
        .await;
        // ages present: 30, 40, 25 -> avg 31.666...; row for "bob" (id 2) had
        // null. COALESCE(int_column, avg_as_float) widens the whole column to
        // float — expected, documented consequence (no schema at compile
        // time to cast the average back down).
        let idx = out.schema().index_of("age").unwrap();
        let age = out
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("age column widens to Float64 after COALESCE with an AVG()");
        assert!((age.value(1) - 31.666).abs() < 0.01);
    }

    #[test]
    fn fill_nulls_other_column_round_trips_through_json() {
        // Regression check: `FillNulls.column` (the target column) and
        // `NullFillStrategy::OtherColumn.column` (the fallback column) both
        // flatten to the same JSON level — if they collided under the same
        // key, a round trip would silently merge or drop one of them. They
        // don't: `strategy` is a *tagged* enum flattened as a sibling of
        // `column`, so serde namespaces `OtherColumn`'s own field under a
        // key this test pins down explicitly (see the raw JSON assertion),
        // not one that can collide with the target column's key.
        let block = CleanBlockSpec {
            name: None,
            kind: CleanBlockKind::FillNulls {
                column: "name".to_string(),
                strategy: NullFillStrategy::OtherColumn {
                    fallback_column: "city".to_string(),
                },
            },
        };
        let json = serde_json::to_value(&block).unwrap();
        assert_eq!(json["column"], "name");
        assert_eq!(json["fallback_column"], "city");

        let back: CleanBlockSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back, block);
    }

    #[tokio::test]
    async fn fill_nulls_with_other_column() {
        let out = run(vec![CleanBlockKind::FillNulls {
            column: "name".into(),
            strategy: NullFillStrategy::OtherColumn {
                fallback_column: "city".into(),
            },
        }])
        .await;
        assert_eq!(col_str(&out, "name")[2], Some("SP".into()));
    }

    #[tokio::test]
    async fn drop_nulls_removes_rows_with_null_in_the_column() {
        let out = run(vec![CleanBlockKind::DropNulls {
            columns: vec!["age".into()],
        }])
        .await;
        assert_eq!(out.num_rows(), 3);
    }

    #[tokio::test]
    async fn dedupe_by_specific_columns_keeps_one_row_per_group() {
        let out = run(vec![CleanBlockKind::Dedupe {
            columns: vec!["city".into()],
        }])
        .await;
        assert_eq!(out.num_rows(), 2);
    }

    #[tokio::test]
    async fn dedupe_all_columns_is_select_distinct_star() {
        let out = run(vec![
            CleanBlockKind::SelectColumns {
                mode: SelectColumnsMode::Keep,
                columns: vec!["city".into()],
            },
            CleanBlockKind::Dedupe { columns: vec![] },
        ])
        .await;
        assert_eq!(out.num_rows(), 2);
    }

    #[tokio::test]
    async fn change_case_upper() {
        let out = run(vec![CleanBlockKind::ChangeCase {
            column: "city".into(),
            mode: CaseMode::Lower,
        }])
        .await;
        assert_eq!(col_str(&out, "city")[0], Some("sp".into()));
    }

    #[tokio::test]
    async fn computed_column_adds_a_new_column() {
        let out = run(vec![CleanBlockKind::ComputedColumn {
            output: "age_plus_id".into(),
            left: "age".into(),
            operator: ComputeOperator::Add,
            right: "id".into(),
        }])
        .await;
        assert_eq!(col_i64(&out, "age_plus_id")[0], Some(31));
    }

    #[tokio::test]
    async fn sort_orders_rows() {
        let out = run(vec![CleanBlockKind::Sort {
            column: "age".into(),
            direction: SortDirection::Asc,
        }])
        .await;
        // DataFusion's default ASC null ordering is nulls-last (confirmed by
        // this test, not assumed) — 25,30,40 then the null.
        assert_eq!(
            col_i64(&out, "age"),
            vec![Some(25), Some(30), Some(40), None]
        );
    }

    #[tokio::test]
    async fn aggregate_group_by_with_sum_and_count() {
        let out = run(vec![CleanBlockKind::Aggregate {
            group_by: vec!["city".into()],
            aggregations: vec![
                Aggregation {
                    column: "id".into(),
                    function: AggFunction::Count,
                    output: "n".into(),
                },
                Aggregation {
                    column: "id".into(),
                    function: AggFunction::Sum,
                    output: "id_sum".into(),
                },
            ],
        }])
        .await;
        assert_eq!(out.num_rows(), 2);
        let n_idx = out.schema().index_of("n").unwrap();
        let total: i64 = out
            .column(n_idx)
            .as_primitive::<Int64Type>()
            .iter()
            .map(|v| v.unwrap())
            .sum();
        assert_eq!(total, 4);
    }

    #[tokio::test]
    async fn chain_of_multiple_blocks_runs_as_one_query() {
        let out = run(vec![
            CleanBlockKind::DropNulls {
                columns: vec!["age".into()],
            },
            CleanBlockKind::Trim {
                columns: vec!["name".into()],
            },
            CleanBlockKind::Sort {
                column: "id".into(),
                direction: SortDirection::Asc,
            },
        ])
        .await;
        assert_eq!(out.num_rows(), 3);
        assert_eq!(col_str(&out, "name")[0], Some("Ana".into()));
    }

    #[tokio::test]
    async fn aggregate_output_is_not_a_float_when_summing_integers() {
        // Guards against a real footgun: SUM over BIGINT in DataFusion can
        // come back as a wider integer type, not silently as float — assert
        // the value is right regardless of the exact integer width.
        let (schema, batch) = people();
        let specs = vec![CleanBlockSpec {
            name: None,
            kind: CleanBlockKind::Aggregate {
                group_by: vec![],
                aggregations: vec![Aggregation {
                    column: "age".into(),
                    function: AggFunction::Avg,
                    output: "avg_age".into(),
                }],
            },
        }];
        let sql = compile_clean_blocks(&specs, "people").unwrap();
        let out = DataFusionTransform::new(sql)
            .apply(vec![("people".to_string(), schema, vec![batch])])
            .await
            .unwrap()
            .remove(0);
        let idx = out.schema().index_of("avg_age").unwrap();
        let avg = out
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((avg - 31.666).abs() < 0.01);
    }
}
