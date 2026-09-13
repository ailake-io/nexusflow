use crate::error::NexusError;
use crate::traits::Transform;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// One output column's provenance: which input column(s) its expression
/// references. `None` means the walker in [`DataFusionTransform::column_lineage`]
/// couldn't find a `Projection`/`Aggregate` node to read it from (e.g. a
/// query shaped in a way the walker doesn't cover yet) — always an honest
/// "not determined", never a guess.
pub struct ColumnLineage {
    pub output_column: String,
    pub source_columns: Option<Vec<String>>,
}

/// Descends a `LogicalPlan` looking for the node that actually assigns
/// output column expressions — `Projection` (`SELECT a, b+c AS x`) or
/// `Aggregate` (`SELECT a, SUM(b) GROUP BY a`, exprs = group_expr ++
/// aggr_expr, same order as `Aggregate::schema`'s fields). Nodes that just
/// reshape rows without introducing new output columns (`Sort`, `Limit`,
/// `Filter`, ...) are transparent — descend into their single input and
/// keep looking. Anything else (`Join`, `Union`, ...) stops the walk: the
/// caller reports "not determined" rather than misattributing lineage.
fn find_column_exprs(plan: &LogicalPlan) -> Option<(&LogicalPlan, Vec<Expr>)> {
    match plan {
        LogicalPlan::Projection(p) => Some((plan, p.expr.clone())),
        LogicalPlan::Aggregate(a) => Some((
            plan,
            a.group_expr.iter().chain(&a.aggr_expr).cloned().collect(),
        )),
        LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Filter(_) => plan
            .inputs()
            .first()
            .and_then(|input| find_column_exprs(input)),
        _ => None,
    }
}

fn plain_column_refs(expr: &Expr) -> Vec<String> {
    expr.column_refs()
        .into_iter()
        .map(|c| c.name.clone())
        .collect()
}

/// `Aggregate`'s `group_expr`/`aggr_expr` and `Window`'s `window_expr` are
/// the two plan nodes whose own exprs get a DataFusion-synthesized display
/// name as their *output* column (e.g. `SUM(amount)` becomes the column name
/// `sum(events.amount)`; a window function becomes its full `OVER (...)`
/// text) — see `resolve_source_columns`'s doc comment for why a `Projection`
/// sitting on top of either needs to trace through that name instead of
/// taking it at face value.
fn synthesized_exprs(plan: &LogicalPlan) -> Option<Vec<Expr>> {
    match plan {
        LogicalPlan::Aggregate(a) => {
            Some(a.group_expr.iter().chain(&a.aggr_expr).cloned().collect())
        }
        LogicalPlan::Window(w) => Some(w.window_expr.clone()),
        _ => None,
    }
}

/// A `Projection` sitting directly on top of an `Aggregate` or `Window`
/// references that node's own synthesized output names rather than the
/// underlying table columns — plain `column_refs()` on the projection's expr
/// would report that synthesized name as if it were a real source column
/// (worse than "not determined": a wrong-looking value, e.g. the literal
/// text `rank() ORDER BY [events.amount ASC ...]` reported as a "source
/// column"). This traces one level through: for each name `expr`
/// references, if it matches one of that node's own output names, substitute
/// that node's expression's *own* `column_refs()` instead.
fn resolve_source_columns(expr: &Expr, plan: &LogicalPlan) -> Vec<String> {
    let LogicalPlan::Projection(p) = plan else {
        return plain_column_refs(expr);
    };
    let Some(synthesized) = synthesized_exprs(p.input.as_ref()) else {
        return plain_column_refs(expr);
    };

    let mut resolved: Vec<String> = expr
        .column_refs()
        .into_iter()
        .flat_map(|c| {
            let matched: Vec<String> = synthesized
                .iter()
                .filter(|e| e.schema_name().to_string() == c.name)
                .flat_map(|e| e.column_refs().into_iter().map(|c| c.name.clone()))
                .collect();
            if matched.is_empty() {
                vec![c.name.clone()]
            } else {
                matched
            }
        })
        .collect();
    resolved.sort();
    resolved.dedup();
    resolved
}

/// The "leve transformação SQL em memória" mode from `CLAUDE.md §4.4`: one
/// SQL query over N named, fully-materialized inputs. Each entry becomes a
/// table the query can reference by name — this is how fan-in (N sources → 1
/// transform) becomes a join/union in practice.
pub struct DataFusionTransform {
    sql: String,
}

impl DataFusionTransform {
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

    /// Resolves what schema `apply` would produce for these named inputs,
    /// without any actual row data — registers each as an empty table (0
    /// partitions, real schema) and reads the query plan's resulting
    /// schema, no execution/`collect()` involved. Used by a CDC pipeline
    /// streaming batches through this transform one at a time
    /// (`nexus-server::runner::run_streaming_cdc_pipeline`): sinks need to
    /// know their column list before the first real batch arrives, and
    /// `apply`'s own `collect()` isn't guaranteed to return a batch at all
    /// for a 0-row probe (DataFusion may just return an empty `Vec`).
    pub async fn output_schema(
        &self,
        input_schemas: Vec<(String, SchemaRef)>,
    ) -> Result<SchemaRef, NexusError> {
        let ctx = SessionContext::new();

        for (name, schema) in input_schemas {
            // `MemTable::try_new` requires at least one partition — an
            // empty `Vec` errors with "No partitions provided", even
            // though the partition itself is allowed to hold zero batches.
            let table = MemTable::try_new(schema, vec![vec![]])
                .map_err(|e| NexusError::Schema(format!("table {name:?}: {e}")))?;
            ctx.register_table(&name, Arc::new(table))
                .map_err(|e| NexusError::Connector(format!("registering table {name:?}: {e}")))?;
        }

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| NexusError::Connector(format!("transform SQL: {e}")))?;

        let schema: Schema = df.schema().as_arrow().clone();
        Ok(Arc::new(schema))
    }

    /// Column-level provenance for this transform's SQL: for each output
    /// column, which input column(s) its expression reads. Reuses the same
    /// 0-row probe as [`Self::output_schema`] — DataFusion already builds a
    /// full `LogicalPlan` for `ctx.sql()` regardless, this just reads it
    /// instead of throwing it away. See [`find_column_exprs`] for what
    /// query shapes this covers.
    pub async fn column_lineage(
        &self,
        input_schemas: Vec<(String, SchemaRef)>,
    ) -> Result<Vec<ColumnLineage>, NexusError> {
        let ctx = SessionContext::new();

        for (name, schema) in input_schemas {
            let table = MemTable::try_new(schema, vec![vec![]])
                .map_err(|e| NexusError::Schema(format!("table {name:?}: {e}")))?;
            ctx.register_table(&name, Arc::new(table))
                .map_err(|e| NexusError::Connector(format!("registering table {name:?}: {e}")))?;
        }

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| NexusError::Connector(format!("transform SQL: {e}")))?;

        let output_names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let found = find_column_exprs(df.logical_plan());

        Ok(match found {
            Some((plan, exprs)) if exprs.len() == output_names.len() => output_names
                .into_iter()
                .zip(exprs)
                .map(|(output_column, expr)| ColumnLineage {
                    output_column,
                    source_columns: Some(resolve_source_columns(&expr, plan)),
                })
                .collect(),
            _ => output_names
                .into_iter()
                .map(|output_column| ColumnLineage {
                    output_column,
                    source_columns: None,
                })
                .collect(),
        })
    }
}

#[async_trait]
impl Transform for DataFusionTransform {
    async fn apply(
        &self,
        inputs: Vec<(String, SchemaRef, Vec<RecordBatch>)>,
    ) -> Result<Vec<RecordBatch>, NexusError> {
        let ctx = SessionContext::new();

        for (name, schema, batches) in inputs {
            let table = MemTable::try_new(schema, vec![batches])
                .map_err(|e| NexusError::Schema(format!("table {name:?}: {e}")))?;
            ctx.register_table(&name, Arc::new(table))
                .map_err(|e| NexusError::Connector(format!("registering table {name:?}: {e}")))?;
        }

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| NexusError::Connector(format!("transform SQL: {e}")))?;

        df.collect()
            .await
            .map_err(|e| NexusError::Connector(format!("transform execution: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn events_batch() -> (SchemaRef, RecordBatch) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["us", "us", "eu", "eu"])),
                Arc::new(Int64Array::from(vec![10, 20, 5, 7])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    #[tokio::test]
    async fn filters_a_single_source() {
        let (schema, batch) = events_batch();
        let transform = DataFusionTransform::new("SELECT id, amount FROM events WHERE amount > 8");

        let output = transform
            .apply(vec![("events".to_string(), schema, vec![batch])])
            .await
            .expect("transform runs");

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "only amount > 8 rows: id 1 (10) and id 2 (20)"
        );
    }

    #[tokio::test]
    async fn joins_two_fan_in_sources() {
        let (schema_a, batch_a) = events_batch();
        let schema_b: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("region_name", DataType::Utf8, false),
        ]));
        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![
                Arc::new(StringArray::from(vec!["us", "eu"])),
                Arc::new(StringArray::from(vec!["United States", "Europe"])),
            ],
        )
        .unwrap();

        let transform = DataFusionTransform::new(
            "SELECT a.id, b.region_name, a.amount \
             FROM events a JOIN regions b ON a.region = b.region \
             ORDER BY a.id",
        );

        let output = transform
            .apply(vec![
                ("events".to_string(), schema_a, vec![batch_a]),
                ("regions".to_string(), schema_b, vec![batch_b]),
            ])
            .await
            .expect("join runs");

        let total_rows: usize = output.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "every event row finds its region match");

        let first_batch = &output[0];
        let region_names = first_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(region_names.value(0), "United States");
    }

    #[tokio::test]
    async fn column_lineage_maps_straight_passthrough() {
        let (schema, _batch) = events_batch();
        let transform = DataFusionTransform::new("SELECT id, amount FROM events");

        let lineage = transform
            .column_lineage(vec![("events".to_string(), schema)])
            .await
            .expect("lineage resolves");

        assert_eq!(lineage.len(), 2);
        assert_eq!(lineage[0].output_column, "id");
        assert_eq!(
            lineage[0].source_columns.as_deref(),
            Some(&["id".to_string()][..])
        );
        assert_eq!(lineage[1].output_column, "amount");
        assert_eq!(
            lineage[1].source_columns.as_deref(),
            Some(&["amount".to_string()][..])
        );
    }

    #[tokio::test]
    async fn column_lineage_maps_derived_expression() {
        let (schema, _batch) = events_batch();
        let transform = DataFusionTransform::new("SELECT id, amount + id AS total FROM events");

        let lineage = transform
            .column_lineage(vec![("events".to_string(), schema)])
            .await
            .expect("lineage resolves");

        let total = lineage
            .iter()
            .find(|l| l.output_column == "total")
            .expect("total column present");
        let mut sources = total.source_columns.clone().expect("determined");
        sources.sort();
        assert_eq!(sources, vec!["amount".to_string(), "id".to_string()]);
    }

    #[tokio::test]
    async fn column_lineage_maps_aggregate() {
        let (schema, _batch) = events_batch();
        let transform = DataFusionTransform::new(
            "SELECT region, SUM(amount) AS total FROM events GROUP BY region",
        );

        let lineage = transform
            .column_lineage(vec![("events".to_string(), schema)])
            .await
            .expect("lineage resolves");

        assert_eq!(lineage.len(), 2);
        assert_eq!(
            lineage[0].source_columns.as_deref(),
            Some(&["region".to_string()][..])
        );
        assert_eq!(
            lineage[1].source_columns.as_deref(),
            Some(&["amount".to_string()][..])
        );
    }

    #[tokio::test]
    async fn column_lineage_resolves_cross_table_join() {
        let (schema_a, _batch_a) = events_batch();
        let schema_b: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("region_name", DataType::Utf8, false),
        ]));

        let transform = DataFusionTransform::new(
            "SELECT a.id, b.region_name FROM events a JOIN regions b ON a.region = b.region",
        );

        let lineage = transform
            .column_lineage(vec![
                ("events".to_string(), schema_a),
                ("regions".to_string(), schema_b),
            ])
            .await
            .expect("lineage resolves");

        assert_eq!(
            lineage[0].source_columns.as_deref(),
            Some(&["id".to_string()][..])
        );
        assert_eq!(
            lineage[1].source_columns.as_deref(),
            Some(&["region_name".to_string()][..])
        );
    }

    #[tokio::test]
    async fn column_lineage_reports_not_determined_for_uncovered_shape() {
        let (schema, _batch) = events_batch();
        let transform =
            DataFusionTransform::new("SELECT id FROM events UNION ALL SELECT id FROM events");

        let lineage = transform
            .column_lineage(vec![("events".to_string(), schema)])
            .await
            .expect("query itself is valid, even if lineage isn't determined");

        assert!(
            lineage.iter().all(|l| l.source_columns.is_none()),
            "Union isn't a shape find_column_exprs covers — must say so, not guess"
        );
    }

    #[tokio::test]
    async fn column_lineage_resolves_window_function_synthesized_name() {
        let (schema, _batch) = events_batch();
        let transform =
            DataFusionTransform::new("SELECT id, RANK() OVER (ORDER BY amount) AS rnk FROM events");

        let lineage = transform
            .column_lineage(vec![("events".to_string(), schema)])
            .await
            .expect("lineage resolves");

        let rnk = lineage
            .iter()
            .find(|l| l.output_column == "rnk")
            .expect("rnk column present");
        assert_eq!(
            rnk.source_columns.as_deref(),
            Some(&["amount".to_string()][..]),
            "must trace through the window function's synthesized display \
             name to the real underlying column, not report that name as-is"
        );
    }

    #[tokio::test]
    async fn rejects_invalid_sql() {
        let (schema, batch) = events_batch();
        let transform = DataFusionTransform::new("SELECT this is not sql");

        let err = transform
            .apply(vec![("events".to_string(), schema, vec![batch])])
            .await
            .expect_err("malformed SQL must error, not panic");
        assert!(matches!(err, NexusError::Connector(_)));
    }
}
