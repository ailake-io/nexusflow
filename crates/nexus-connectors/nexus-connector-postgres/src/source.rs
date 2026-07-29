use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{NexusError, Source};
use std::sync::Arc;

/// Bounds of one partition's primary-key range. `upper_exclusive: None` means
/// "to the end" — the last partition. Partitioning is the unit of parallelism,
/// see ARCHITECTURE.md §4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRange {
    pub lower_inclusive: i64,
    pub upper_exclusive: Option<i64>,
}

/// Splits `[min, max]` into `n` contiguous PK ranges. The last range is
/// unbounded above (`upper_exclusive: None`) so it also catches any row
/// inserted after the bounds were computed — see IMPLEMENTATION_PLAN.md
/// Marco 1.
pub fn split_into_partitions(min: i64, max: i64, n: u32) -> Vec<PartitionRange> {
    let n = n.max(1) as i64;
    let span = (max - min + 1).max(1);
    let chunk = (span + n - 1) / n;

    let mut ranges = Vec::new();
    let mut lower = min;
    for i in 0..n {
        if i == n - 1 {
            ranges.push(PartitionRange {
                lower_inclusive: lower,
                upper_exclusive: None,
            });
            break;
        }
        let upper = lower + chunk;
        ranges.push(PartitionRange {
            lower_inclusive: lower,
            upper_exclusive: Some(upper),
        });
        lower = upper;
    }
    ranges
}

pub struct PostgresSource {
    connection: ManagedConnection,
    table: String,
    primary_key: String,
    range: PartitionRange,
    schema: SchemaRef,
}

impl PostgresSource {
    pub fn connect(
        cfg: &PostgresConnectorConfig,
        range: PartitionRange,
    ) -> Result<Self, NexusError> {
        let connection = open_connection(&cfg.uri)?;
        let schema = connection
            .get_table_schema(None, None, &cfg.table)
            .map_err(|e| NexusError::Schema(e.to_string()))?;

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            primary_key: cfg.primary_key.clone(),
            range,
            schema: Arc::new(schema),
        })
    }

    fn build_query(&self) -> String {
        build_select_query(&self.table, &self.primary_key, self.range)
    }
}

/// Pure query-string builder, kept free of any connection state so it's
/// testable without a live driver (`CLAUDE.md §8.6`).
fn build_select_query(table: &str, primary_key: &str, range: PartitionRange) -> String {
    match range.upper_exclusive {
        Some(upper) => format!(
            "SELECT * FROM {table} WHERE {primary_key} >= {} AND {primary_key} < {upper}",
            range.lower_inclusive
        ),
        None => format!(
            "SELECT * FROM {table} WHERE {primary_key} >= {}",
            range.lower_inclusive
        ),
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = self.build_query();
        let mut connection = self.connection.clone();

        // ADBC calls are blocking FFI (libpq under the hood); run off the
        // async executor. Partitions are bounded PK ranges, so eagerly
        // collecting is acceptable for M1 — see IMPLEMENTATION_PLAN.md Marco 1.
        let batches =
            tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, NexusError> {
                let mut statement = connection
                    .new_statement()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                statement
                    .set_sql_query(&query)
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                let reader = statement
                    .execute()
                    .map_err(|e| NexusError::Connector(e.to_string()))?;
                reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| NexusError::Serialization(e.to_string()))
            })
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))??;

        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_bounded_partition() {
        let query = build_select_query(
            "events",
            "id",
            PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: Some(1000),
            },
        );
        assert_eq!(query, "SELECT * FROM events WHERE id >= 0 AND id < 1000");
    }

    #[test]
    fn build_query_last_partition_is_unbounded_above() {
        let query = build_select_query(
            "events",
            "id",
            PartitionRange {
                lower_inclusive: 9000,
                upper_exclusive: None,
            },
        );
        assert_eq!(query, "SELECT * FROM events WHERE id >= 9000");
    }

    #[test]
    fn split_into_partitions_covers_the_whole_range_contiguously() {
        let ranges = split_into_partitions(0, 999, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(
            ranges[0],
            PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: Some(250)
            }
        );
        assert_eq!(
            ranges[3],
            PartitionRange {
                lower_inclusive: 750,
                upper_exclusive: None
            },
            "last partition is unbounded above"
        );
        // Every boundary lines up: partition i's upper == partition i+1's lower.
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].upper_exclusive, Some(pair[1].lower_inclusive));
        }
    }

    #[test]
    fn split_into_partitions_handles_fewer_rows_than_partitions() {
        let ranges = split_into_partitions(5, 6, 4);
        assert!(!ranges.is_empty());
        assert_eq!(ranges.last().unwrap().upper_exclusive, None);
    }

    #[test]
    fn split_into_partitions_single_partition_is_fully_unbounded() {
        let ranges = split_into_partitions(0, 100, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].lower_inclusive, 0);
        assert_eq!(ranges[0].upper_exclusive, None);
    }
}
