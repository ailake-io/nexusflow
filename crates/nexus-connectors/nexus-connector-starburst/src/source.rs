use crate::client::StarburstClient;
use crate::config::StarburstConnectorConfig;
use crate::rows::build_record_batch;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nexus_core::{quote_identifier, NexusError, Source};

/// Bounds of one partition's `partition_column` range — identical mechanics
/// to `nexus-connector-trino::PartitionRange` in the public repo (this
/// crate can't depend on it across the public/private boundary,
/// `LICENSING.md §3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRange {
    pub lower_inclusive: i64,
    pub upper_exclusive: Option<i64>,
}

/// Validates and quotes each of the 3 identifier segments individually
/// (`nexus_core::quote_identifier` rejects `.`) and joins them with `.` —
/// same as Trino's own `qualify_table`.
fn qualify_table(catalog: &str, schema: &str, table: &str) -> Result<String, NexusError> {
    Ok(format!(
        "{}.{}.{}",
        quote_identifier(catalog)?,
        quote_identifier(schema)?,
        quote_identifier(table)?
    ))
}

/// Pure query-string builder — identical shape to Trino's
/// `build_select_query_for_table`, duplicated for the same
/// public/private-boundary reason as `qualify_table` above.
fn build_select_query_for_table(
    qualified_table: &str,
    partition_column: Option<&str>,
    range: Option<PartitionRange>,
) -> Result<String, NexusError> {
    Ok(match (partition_column, range) {
        (Some(column), Some(range)) => {
            let column = quote_identifier(column)?;
            match range.upper_exclusive {
                Some(upper) => format!(
                    "SELECT * FROM {qualified_table} WHERE {column} >= {} AND {column} < {upper}",
                    range.lower_inclusive
                ),
                None => format!(
                    "SELECT * FROM {qualified_table} WHERE {column} >= {}",
                    range.lower_inclusive
                ),
            }
        }
        _ => format!("SELECT * FROM {qualified_table}"),
    })
}

#[cfg(test)]
fn build_select_query(
    catalog: &str,
    schema: &str,
    table: &str,
    partition_column: Option<&str>,
    range: Option<PartitionRange>,
) -> Result<String, NexusError> {
    let qualified = qualify_table(catalog, schema, table)?;
    build_select_query_for_table(&qualified, partition_column, range)
}

pub struct StarburstSource {
    client: StarburstClient,
    qualified_table: String,
    partition_column: Option<String>,
    range: Option<PartitionRange>,
    schema: SchemaRef,
}

impl StarburstSource {
    /// `range` mirrors `nexus-connector-trino::TrinoSource::connect`'s own
    /// signature — kept for a future parallel-partitioned scheduler to
    /// pass a real range through. `nexus-server`'s `build_source` today
    /// always passes `None`, same as it does for Trino.
    pub async fn connect(
        cfg: &StarburstConnectorConfig,
        range: Option<PartitionRange>,
    ) -> Result<Self, NexusError> {
        let qualified_table = qualify_table(&cfg.catalog, &cfg.schema_name, &cfg.table_name)?;
        if let Some(column) = &cfg.partition_column {
            quote_identifier(column)?;
        }

        let client = StarburstClient::new(cfg)?;
        // Same rationale as the Trino connector's `connect()`: a 0-row
        // `SELECT * ... LIMIT 0` through the exact same execute path
        // `read_batches` uses, rather than a `DESCRIBE`/metadata call, so
        // schema resolution is exercised by the same code path being
        // trusted for the real read.
        let probe_query = format!("SELECT * FROM {qualified_table} LIMIT 0");
        let (columns, _rows) = client.execute(&probe_query).await?;

        Ok(Self {
            client,
            qualified_table,
            partition_column: cfg.partition_column.clone(),
            range,
            schema: crate::rows::build_schema(&columns),
        })
    }
}

#[async_trait]
impl Source for StarburstSource {
    async fn read_batches(
        &mut self,
    ) -> Result<BoxStream<'_, Result<RecordBatch, NexusError>>, NexusError> {
        let query = build_select_query_for_table(
            &self.qualified_table,
            self.partition_column.as_deref(),
            self.range,
        )?;
        let (columns, data) = self.client.execute(&query).await?;
        let batch = build_record_batch(&columns, &data)?;
        Ok(Box::pin(stream::iter(vec![Ok(batch)])))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_qualifies_catalog_schema_table_with_ansi_quotes() {
        let query = build_select_query("hive", "default", "events", None, None).unwrap();
        assert_eq!(query, "SELECT * FROM \"hive\".\"default\".\"events\"");
    }

    #[test]
    fn build_query_bounded_partition() {
        let query = build_select_query(
            "hive",
            "default",
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
            "SELECT * FROM \"hive\".\"default\".\"events\" WHERE \"id\" >= 0 AND \"id\" < 1000"
        );
    }

    #[test]
    fn build_query_last_partition_is_unbounded_above() {
        let query = build_select_query(
            "hive",
            "default",
            "events",
            Some("id"),
            Some(PartitionRange {
                lower_inclusive: 9000,
                upper_exclusive: None,
            }),
        )
        .unwrap();
        assert_eq!(
            query,
            "SELECT * FROM \"hive\".\"default\".\"events\" WHERE \"id\" >= 9000"
        );
    }

    #[test]
    fn build_query_no_partition_column_reads_whole_table_unconditionally() {
        let query = build_select_query("postgresql", "public", "regions", None, None).unwrap();
        assert_eq!(query, "SELECT * FROM \"postgresql\".\"public\".\"regions\"");
    }

    #[test]
    fn build_query_rejects_sql_injection_in_catalog() {
        let err = build_select_query(
            "hive; DROP TABLE users; --",
            "default",
            "events",
            None,
            None,
        )
        .expect_err("malicious catalog name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn build_query_rejects_sql_injection_in_schema() {
        let err = build_select_query(
            "hive",
            "default; DROP TABLE users; --",
            "events",
            None,
            None,
        )
        .expect_err("malicious schema name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn build_query_rejects_sql_injection_in_table_name() {
        let err = build_select_query(
            "hive",
            "default",
            "events; DROP TABLE users; --",
            None,
            None,
        )
        .expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn build_query_rejects_sql_injection_in_partition_column() {
        let err = build_select_query(
            "hive",
            "default",
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
}
