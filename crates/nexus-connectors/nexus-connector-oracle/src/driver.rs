use crate::config::OracleConnectorConfig;

/// Builds the ODBC connection string for the Oracle Instant Client
/// ODBC driver. `DBQ` carries Oracle's "Easy Connect" form
/// (`host:port/service_name`) — this is the documented behavior of
/// Oracle's own ODBC driver, but **not confirmed against a real
/// installation in this session** (unlike Snowflake/BigQuery's option
/// keys, which came from reading the real Python binding source out of
/// the PyPI wheel). Treat the exact driver name/`DBQ` attribute as a
/// verification item, not a settled fact, until checked against a real
/// Oracle Instant Client install.
pub(crate) fn connection_string(cfg: &OracleConnectorConfig) -> String {
    connection_string_parts(&cfg.host, cfg.port, &cfg.service_name, &cfg.username, &cfg.password)
}

/// Same connection string, built from loose parts instead of
/// `OracleConnectorConfig` — reused by `OracleCdcConfig` (`cdc.rs`),
/// which has the same connection fields but isn't the same struct
/// (no `primary_key`, plus CDC-specific polling fields).
pub(crate) fn connection_string_parts(
    host: &str,
    port: u16,
    service_name: &str,
    username: &str,
    password: &str,
) -> String {
    format!(
        "Driver={{Oracle in instantclient}};DBQ={}:{}/{};UID={};PWD={}",
        odbc_escape(host),
        port,
        odbc_escape(service_name),
        odbc_escape(username),
        odbc_escape(password),
    )
}

/// Escapes a value placed on the right-hand side of an ODBC
/// `Key=Value` pair — same rule `nexus-connector-odbc` (public repo)
/// uses: curly braces and semicolons must be doubled.
fn odbc_escape(s: &str) -> String {
    s.replace(';', ";;").replace('}', "}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_easy_connect_dbq() {
        let cfg = OracleConnectorConfig {
            host: "db.example.com".into(),
            port: 1521,
            service_name: "FREEPDB1".into(),
            username: "nexus".into(),
            password: "secret".into(),
            table: "events".into(),
            primary_key: None,
            timeout_seconds: 30,
            retry: Default::default(),
        };
        let cs = connection_string(&cfg);
        assert!(cs.contains("DBQ=db.example.com:1521/FREEPDB1"));
        assert!(cs.contains("UID=nexus"));
        assert!(cs.contains("PWD=secret"));
    }

    #[test]
    fn escapes_special_characters_in_password() {
        let cfg = OracleConnectorConfig {
            host: "db".into(),
            port: 1521,
            service_name: "FREEPDB1".into(),
            username: "nexus".into(),
            password: "p;w}d".into(),
            table: "events".into(),
            primary_key: None,
            timeout_seconds: 30,
            retry: Default::default(),
        };
        assert!(connection_string(&cfg).contains("PWD=p;;w}}d"));
    }
}
