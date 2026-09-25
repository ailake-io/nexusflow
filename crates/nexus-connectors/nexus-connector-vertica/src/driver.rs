use crate::config::VerticaConnectorConfig;

/// Builds the ODBC connection string for the Vertica ODBC Driver.
/// Real, documented attribute names (Vertica ODBC driver
/// configuration guide): `Server`, `Port`, `Database`, `UID`, `PWD` —
/// a simpler DSN-less connection string than Oracle/HANA/Teradata's,
/// since Vertica's driver takes plain standard-looking keys. **Not
/// confirmed against a real installation in this session** — same
/// honesty flag every ODBC connector's `driver.rs` in this repo
/// carries.
pub(crate) fn connection_string(cfg: &VerticaConnectorConfig) -> String {
    format!(
        "Driver={{Vertica}};Server={};Port={};Database={};UID={};PWD={}",
        odbc_escape(&cfg.host),
        cfg.port,
        odbc_escape(&cfg.database),
        odbc_escape(&cfg.username),
        odbc_escape(&cfg.password),
    )
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

    fn base_cfg() -> VerticaConnectorConfig {
        VerticaConnectorConfig {
            host: "vertica.example.com".into(),
            port: 5433,
            database: "nexus_db".into(),
            username: "nexus".into(),
            password: "secret".into(),
            table: "events".into(),
            primary_key: None,
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn builds_full_connection_string() {
        let cs = connection_string(&base_cfg());
        assert!(cs.contains("Server=vertica.example.com"));
        assert!(cs.contains("Port=5433"));
        assert!(cs.contains("Database=nexus_db"));
        assert!(cs.contains("UID=nexus"));
        assert!(cs.contains("PWD=secret"));
    }

    #[test]
    fn escapes_special_characters_in_password() {
        let mut cfg = base_cfg();
        cfg.password = "p;w}d".into();
        assert!(connection_string(&cfg).contains("PWD=p;;w}}d"));
    }
}
