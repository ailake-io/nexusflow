use crate::config::DatabricksConnectorConfig;
use nexus_core::{validate_identifier, NexusError};

/// Databricks SQL quotes identifiers with backticks, not `nexus-core`'s
/// `quote_identifier`'s double quotes — same choice `nexus-connector-
/// bigquery`'s `quote_backtick` makes, reused here verbatim (dialect
/// wrapper only, `validate_identifier`'s charset check is dialect-
/// agnostic and already rules out a backtick appearing in the name).
pub(crate) fn quote_backtick(name: &str) -> Result<String, NexusError> {
    let valid = validate_identifier(name)?;
    Ok(format!("`{valid}`"))
}

/// Fully-qualified `` `catalog`.`schema`.`table` `` form Unity Catalog
/// expects in query text — 3 parts, unlike BigQuery's `project.dataset.
/// table` (which needs a separate GCP-project-id validator because real
/// project IDs contain hyphens); Databricks catalog/schema/table names
/// follow ordinary SQL identifier rules, so `validate_identifier` (no
/// hyphen exception needed) covers all 3 parts directly.
pub(crate) fn qualified_table(cfg: &DatabricksConnectorConfig) -> Result<String, NexusError> {
    let catalog = validate_identifier(&cfg.catalog)?;
    let schema = validate_identifier(&cfg.schema)?;
    let table = validate_identifier(&cfg.table)?;
    Ok(format!("`{catalog}`.`{schema}`.`{table}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> DatabricksConnectorConfig {
        DatabricksConnectorConfig {
            host: "host".to_string(),
            port: 443,
            http_path: "/sql/1.0/warehouses/abc".to_string(),
            auth: crate::config::DatabricksAuth::OauthU2M,
            catalog: "main".to_string(),
            schema: "default".to_string(),
            table: "events".to_string(),
            primary_key: None,
            timeout_seconds: 60,
            retry: Default::default(),
        }
    }

    #[test]
    fn qualified_table_has_three_backtick_quoted_parts() {
        let cfg = base_config();
        assert_eq!(qualified_table(&cfg).unwrap(), "`main`.`default`.`events`");
    }

    #[test]
    fn qualified_table_rejects_sql_injection_in_table() {
        let mut cfg = base_config();
        cfg.table = "events`; DROP TABLE users; --".to_string();
        let err = qualified_table(&cfg).expect_err("malicious table name must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }

    #[test]
    fn quote_backtick_rejects_a_backtick_in_the_name() {
        // validate_identifier's charset can't produce a backtick, so
        // there's nothing to escape — confirm the reject path instead.
        let err = quote_backtick("col`; DROP TABLE users; --")
            .expect_err("malicious identifier must be rejected");
        assert!(matches!(err, NexusError::Schema(_)));
    }
}
