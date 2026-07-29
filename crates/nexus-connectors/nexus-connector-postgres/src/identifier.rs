use nexus_core::NexusError;

/// Table/column names come from the pipeline spec (attacker-controlled HTTP
/// request body) and get spliced into SQL text — ADBC's `bind` only covers
/// *values*, not identifiers, so there's no parameterized-query escape hatch
/// for them. Every identifier must go through here before it touches a
/// `format!` that builds SQL: reject anything outside a strict safe subset,
/// then double-quote it (Postgres identifier quoting, doubling any embedded
/// `"`) so mixed-case names and reserved words round-trip correctly too.
pub fn quote_identifier(name: &str) -> Result<String, NexusError> {
    let valid = !name.is_empty()
        && name.len() <= 63 // Postgres NAMEDATALEN limit
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');

    if !valid {
        return Err(NexusError::Schema(format!(
            "invalid identifier {name:?}: must match [A-Za-z_][A-Za-z0-9_]*, max 63 chars"
        )));
    }

    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_identifiers() {
        assert_eq!(quote_identifier("events").unwrap(), "\"events\"");
        assert_eq!(quote_identifier("_id").unwrap(), "\"_id\"");
        assert_eq!(quote_identifier("Events_2").unwrap(), "\"Events_2\"");
    }

    #[test]
    fn rejects_sql_injection_attempts() {
        assert!(quote_identifier("events; DROP TABLE users; --").is_err());
        assert!(quote_identifier("events\" OR \"1\"=\"1").is_err());
        assert!(quote_identifier("id) UNION SELECT password FROM pg_shadow--").is_err());
        assert!(quote_identifier("events--").is_err());
        assert!(quote_identifier("").is_err());
        assert!(quote_identifier(" events").is_err());
        assert!(quote_identifier("events;").is_err());
        assert!(quote_identifier("public.events").is_err());
        assert!(quote_identifier("1events").is_err());
    }

    #[test]
    fn rejects_names_longer_than_namedatalen() {
        let too_long = "a".repeat(64);
        assert!(quote_identifier(&too_long).is_err());
        assert!(quote_identifier(&"a".repeat(63)).is_ok());
    }
}
