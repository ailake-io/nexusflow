use crate::config::HanaConnectorConfig;

/// Builds the ODBC connection string for the SAP HANA Client ODBC
/// driver (`HDBODBC`). Real, confirmed format:
/// `Driver={HDBODBC};ServerNODE=host:port;UID=...;PWD=...;
/// DATABASENAME=...` (`DATABASENAME` optional — omitting it works
/// against single-container instances). **Not confirmed against a
/// real installation in this session** — same honesty flag the Oracle
/// connector's `driver.rs` carries for its own `DBQ` attribute.
pub(crate) fn connection_string(cfg: &HanaConnectorConfig) -> String {
    let mut cs = format!(
        "Driver={{HDBODBC}};ServerNODE={}:{};UID={};PWD={}",
        odbc_escape(&cfg.host),
        cfg.port,
        odbc_escape(&cfg.username),
        odbc_escape(&cfg.password),
    );
    if let Some(database) = &cfg.database {
        cs.push_str(&format!(";DATABASENAME={}", odbc_escape(database)));
    }
    cs
}

/// Escapes a value placed on the right-hand side of an ODBC
/// `Key=Value` pair — same rule the Oracle connector's `driver.rs`
/// (this repo) and `nexus-connector-odbc` (public repo) use.
fn odbc_escape(s: &str) -> String {
    s.replace(';', ";;").replace('}', "}}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> HanaConnectorConfig {
        HanaConnectorConfig {
            host: "hana.example.com".into(),
            port: 30015,
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
    fn builds_servernode_without_database() {
        let cs = connection_string(&base_cfg());
        assert!(cs.contains("ServerNODE=hana.example.com:30015"));
        assert!(cs.contains("UID=nexus"));
        assert!(cs.contains("PWD=secret"));
        assert!(!cs.contains("DATABASENAME"));
    }

    #[test]
    fn includes_databasename_when_set() {
        let mut cfg = base_cfg();
        cfg.database = Some("SBODEMODB".into());
        assert!(connection_string(&cfg).contains("DATABASENAME=SBODEMODB"));
    }

    #[test]
    fn escapes_special_characters_in_password() {
        let mut cfg = base_cfg();
        cfg.password = "p;w}d".into();
        assert!(connection_string(&cfg).contains("PWD=p;;w}}d"));
    }
}
