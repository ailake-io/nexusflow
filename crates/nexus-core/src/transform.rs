use crate::error::NexusError;
use crate::traits::Transform;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

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
