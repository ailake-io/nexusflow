use crate::config::TeradataConnectorConfig;

/// Builds the ODBC connection string for the Teradata Database ODBC
/// Driver. Real, documented attribute names (Teradata ODBC Driver
/// user guide): `DBCName` (system name/IP), `UID`/`PWD`, `Database`
/// (default database), `DBS_PORT` (service port, driver-version
/// dependent). **Not confirmed against a real installation in this
/// session** — same honesty flag the Oracle/HANA connectors'
/// `driver.rs` carry for their own attributes.
pub(crate) fn connection_string(cfg: &TeradataConnectorConfig) -> String {
    let mut cs = format!(
        "Driver={{Teradata Database ODBC Driver}};DBCName={};DBS_PORT={};UID={};PWD={}",
        odbc_escape(&cfg.host),
        cfg.port,
        odbc_escape(&cfg.username),
        odbc_escape(&cfg.password),
    );
    if let Some(database) = &cfg.database {
        cs.push_str(&format!(";Database={}", odbc_escape(database)));
    }
    cs
}

/// Escapes a value placed on the right-hand side of an ODBC
/// `Key=Value` pair — same rule every other ODBC connector in this
/// repo (and `nexus-connector-odbc`, public repo) uses.
fn odbc_escape(s: &str) -> String {
    s.replace(';', ";;").replace('}', "}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> TeradataConnectorConfig {
        TeradataConnectorConfig {
            host: "teradata.example.com".into(),
            port: 1025,
            database: None,
            username: "nexus".into(),
            password: "secret".into(),
            table: "events".into(),
            primary_key: None,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn builds_dbcname_without_database() {
        let cs = connection_string(&base_cfg());
        assert!(cs.contains("DBCName=teradata.example.com"));
        assert!(cs.contains("DBS_PORT=1025"));
        assert!(cs.contains("UID=nexus"));
        assert!(cs.contains("PWD=secret"));
        assert!(!cs.contains("Database="));
    }

    #[test]
    fn includes_database_when_set() {
        let mut cfg = base_cfg();
        cfg.database = Some("nexus_db".into());
        assert!(connection_string(&cfg).contains("Database=nexus_db"));
    }

    #[test]
    fn escapes_special_characters_in_password() {
        let mut cfg = base_cfg();
        cfg.password = "p;w}d".into();
        assert!(connection_string(&cfg).contains("PWD=p;;w}}d"));
    }
}
