use crate::config::PostgresConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::ManagedConnection;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, with_timeout, NexusError, Source};
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
    /// `None` means "read the whole table, no WHERE clause" — used by the
    /// Marco 2 transform fan-in path (`ARCHITECTURE.md §6`), which doesn't
    /// partition and can't assume an `i64`-range-friendly primary key (a
    /// text PK, for instance, can't be bounded by a numeric sentinel).
    range: Option<PartitionRange>,
    schema: SchemaRef,
    timeout_seconds: u64,
}

impl PostgresSource {
    pub async fn connect(
        cfg: &PostgresConnectorConfig,
        range: Option<PartitionRange>,
    ) -> Result<Self, NexusError> {
        // Fail fast on a malformed table/primary_key before opening any
        // connection — see nexus_core::sql for why this can't be a bind param.
        quote_identifier(&cfg.table)?;
        if range.is_some() {
            quote_identifier(&cfg.primary_key)?;
        }

        let uri = cfg.uri.clone();
        let table = cfg.table.clone();
        let (connection, schema) = with_timeout(cfg.timeout_seconds, "postgres connect", async {
            tokio::task::spawn_blocking(
                move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                    let connection = open_connection(&uri)?;
                    let schema = connection
                        .get_table_schema(None, None, &table)
                        .map_err(|e| NexusError::Schema(e.to_string()))?;
                    Ok((connection, schema))
                },
            )
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;

        Ok(Self {
            connection,
            table: cfg.table.clone(),
            primary_key: cfg.primary_key.clone(),
            range,
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    fn build_query(&self) -> Result<String, NexusError> {
        build_select_query(&self.table, &self.primary_key, self.range)
    }
}

/// Pure query-string builder, kept free of any connection state so it's
/// testable without a live driver (`CLAUDE.md §8.6`). `table`/`primary_key`
/// are validated and quoted here — this is the only place that's allowed to
/// splice them into SQL text.
fn build_select_query(
    table: &str,
    primary_key: &str,
    range: Option<PartitionRange>,
) -> Result<String, NexusError> {
    let table = quote_identifier(table)?;
    Ok(match range {
        None => format!("SELECT * FROM {table}"),
        Some(range) => {
            let primary_key = quote_identifier(primary_key)?;
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
    })
}

#[async_trait]
impl Source for PostgresSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = self.build_query()?;
        let mut connection = self.connection.clone();

        // ADBC calls are blocking FFI (libpq under the hood); run off the
        // async executor and yield each batch as it is produced instead of
        // collecting the whole partition before the downstream pipeline starts.
        let reader = with_timeout(self.timeout_seconds, "postgres query", async {
            tokio::task::spawn_blocking(
                move || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, NexusError> {
                    let mut statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .set_sql_query(&query)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .execute()
                        .map_err(|e| NexusError::Connector(e.to_string()))
                },
            )
            .await
            .map_err(|e| NexusError::Connector(format!("blocking task panicked: {e}")))?
        })
        .await?;

        Ok(Box::pin(stream::iter(reader.map(|r| {
            r.map_err(|e| NexusError::Serialization(e.to_string()))
        }))))
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
            Some(PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: Some(1000),
            }),
        )
        .unwrap();
        assert_eq!(
            query,
            "SELECT * FROM \"events\" WHERE \"id\" >= 0 AND \"id\" < 1000"
        );
    }

    #[test]
    fn build_query_last_partition_is_unbounded_above() {
        let query = build_select_query(
            "events",
            "id",
            Some(PartitionRange {
                lower_inclusive: 9000,
                upper_exclusive: None,
            }),
        )
        .unwrap();
        assert_eq!(query, "SELECT * FROM \"events\" WHERE \"id\" >= 9000");
    }

    #[test]
    fn build_query_no_range_reads_whole_table_unconditionally() {
        let query = build_select_query("regions", "region", None).unwrap();
        assert_eq!(query, "SELECT * FROM \"regions\"");
    }

    #[test]
    fn build_query_rejects_sql_injection_in_table_name() {
        let err = build_select_query(
            "events; DROP TABLE users; --",
            "id",
            Some(PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: None,
            }),
        )
        .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
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
