use crate::config::ClickHouseConnectorConfig;
use crate::driver::open_connection;
use adbc_core::{Connection as _, Statement as _};
use adbc_driver_manager::{ManagedConnection, ManagedStatement};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, with_timeout, NexusError, Source};
use std::sync::Arc;

/// Same rationale as `nexus-connector-postgres`'s equivalent: the reader
/// returned by `Statement::execute` isn't actually independent of the
/// `Statement` it came from, so both must be kept alive together.
struct StatementBoundReader {
    _statement: ManagedStatement,
    reader: Box<dyn arrow_array::RecordBatchReader + Send>,
}

impl Iterator for StatementBoundReader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl arrow_array::RecordBatchReader for StatementBoundReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// Bounds of one partition's `partition_column` range. `upper_exclusive:
/// None` means "to the end" — the last partition. Same mechanics as
/// `nexus-connector-postgres::PartitionRange`, duplicated here rather than
/// shared so each connector crate stays independent (same choice already
/// made between Snowflake/BigQuery/Databricks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRange {
    pub lower_inclusive: i64,
    pub upper_exclusive: Option<i64>,
}

/// Splits `[min, max]` into `n` contiguous ranges. The last range is
/// unbounded above so it also catches any row inserted after the bounds
/// were computed.
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

pub struct ClickHouseSource {
    connection: ManagedConnection,
    table: String,
    partition_column: Option<String>,
    range: Option<PartitionRange>,
    schema: SchemaRef,
    timeout_seconds: u64,
}

impl ClickHouseSource {
    pub async fn connect(
        cfg: &ClickHouseConnectorConfig,
        range: Option<PartitionRange>,
    ) -> Result<Self, NexusError> {
        quote_identifier(&cfg.table)?;
        if let Some(column) = &cfg.partition_column {
            quote_identifier(column)?;
        }

        let uri = cfg.connection_string();
        let username = cfg.username.clone();
        let password = cfg.password.clone();
        let table = cfg.table.clone();
        let database = cfg.database.clone();
        let (connection, schema) = with_timeout(cfg.timeout_seconds, "clickhouse connect", async {
            tokio::task::spawn_blocking(
                move || -> Result<(ManagedConnection, arrow_schema::Schema), NexusError> {
                    let connection = open_connection(&uri, &username, &password)?;
                    // The ClickHouse ADBC driver is early/WIP (v0.1.0 at
                    // time of writing) — get_table_schema's metadata path
                    // isn't verified against a real instance. If this
                    // proves unreliable in practice, fall back to
                    // `SELECT * FROM table LIMIT 0` the way
                    // nexus-connector-bigquery does for the same reason.
                    let schema = connection
                        .get_table_schema(None, Some(&database), &table)
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
            partition_column: cfg.partition_column.clone(),
            range,
            schema: Arc::new(schema),
            timeout_seconds: cfg.timeout_seconds,
        })
    }

    fn build_query(&self) -> Result<String, NexusError> {
        build_select_query(&self.table, self.partition_column.as_deref(), self.range)
    }
}

/// Pure query-string builder, kept free of any connection state so it's
/// testable without a live driver. `table`/`partition_column` are validated
/// and quoted here — this is the only place allowed to splice them into SQL
/// text.
fn build_select_query(
    table: &str,
    partition_column: Option<&str>,
    range: Option<PartitionRange>,
) -> Result<String, NexusError> {
    let table = quote_identifier(table)?;
    Ok(match (partition_column, range) {
        (Some(column), Some(range)) => {
            let column = quote_identifier(column)?;
            match range.upper_exclusive {
                Some(upper) => format!(
                    "SELECT * FROM {table} WHERE {column} >= {} AND {column} < {upper}",
                    range.lower_inclusive
                ),
                None => format!(
                    "SELECT * FROM {table} WHERE {column} >= {}",
                    range.lower_inclusive
                ),
            }
        }
        _ => format!("SELECT * FROM {table}"),
    })
}

#[async_trait]
impl Source for ClickHouseSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = self.build_query()?;
        let mut connection = self.connection.clone();

        let reader = with_timeout(self.timeout_seconds, "clickhouse query", async {
            tokio::task::spawn_blocking(
                move || -> Result<Box<dyn arrow_array::RecordBatchReader + Send>, NexusError> {
                    let mut statement = connection
                        .new_statement()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    statement
                        .set_sql_query(&query)
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    let reader = statement
                        .execute()
                        .map_err(|e| NexusError::Connector(e.to_string()))?;
                    Ok(Box::new(StatementBoundReader {
                        _statement: statement,
                        reader,
                    })
                        as Box<dyn arrow_array::RecordBatchReader + Send>)
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
            Some("id"),
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
            Some("id"),
            Some(PartitionRange {
                lower_inclusive: 9000,
                upper_exclusive: None,
            }),
        )
        .unwrap();
        assert_eq!(query, "SELECT * FROM \"events\" WHERE \"id\" >= 9000");
    }

    #[test]
    fn build_query_no_partition_column_reads_whole_table_unconditionally() {
        let query = build_select_query("regions", None, None).unwrap();
        assert_eq!(query, "SELECT * FROM \"regions\"");
    }

    #[test]
    fn build_query_rejects_sql_injection_in_table_name() {
        let err = build_select_query(
            "events; DROP TABLE users; --",
            Some("id"),
            Some(PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: None,
            }),
        )
        .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn build_query_rejects_sql_injection_in_partition_column() {
        let err = build_select_query(
            "events",
            Some("id; DROP TABLE users; --"),
            Some(PartitionRange {
                lower_inclusive: 0,
                upper_exclusive: None,
            }),
        )
        .expect_err("malicious partition column must be rejected");
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
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].upper_exclusive, Some(pair[1].lower_inclusive));
        }
    }

    #[test]
    fn split_into_partitions_single_partition_is_fully_unbounded() {
        let ranges = split_into_partitions(0, 100, 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].lower_inclusive, 0);
        assert_eq!(ranges[0].upper_exclusive, None);
    }
}
