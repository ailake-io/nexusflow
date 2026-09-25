use crate::config::BigqueryConnectorConfig;
use nexus_core::{validate_identifier, NexusError};

/// BigQuery quotes identifiers with backticks, not `nexus-core`'s
/// `quote_identifier`'s double quotes — but the underlying safety check
/// (`validate_identifier`, restricts to `[A-Za-z_][A-Za-z0-9_]*`) is
/// dialect-agnostic and reusable *for dataset/table/column names*: a
/// name that passes it can never contain a backtick, so there's no
/// escaping to do, just a different wrapper character.
pub(crate) fn quote_backtick(name: &str) -> Result<String, NexusError> {
    let valid = validate_identifier(name)?;
    Ok(format!("`{valid}`"))
}

/// GCP project IDs are NOT valid `nexus_core::validate_identifier`
/// identifiers — real project IDs routinely contain hyphens (e.g.
/// `my-project-123`, the common auto-generated GCP form), which that
/// validator rejects. A hyphen can't be used to break out of a
/// backtick-quoted identifier, so it's safe to allow here; this is a
/// narrower, BigQuery-project-ID-specific check (letters/digits/hyphens,
/// must start with a letter, 6-30 chars — real GCP constraints), not a
/// relaxation of `validate_identifier`'s own contract for everything
/// else.
fn validate_project_id(project_id: &str) -> Result<&str, NexusError> {
    let valid = project_id.len() >= 6
        && project_id.len() <= 30
        && project_id
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase())
        && project_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !project_id.ends_with('-');

    if !valid {
        return Err(NexusError::Schema(format!(
            "invalid GCP project_id {project_id:?}: must be 6-30 chars, lowercase letters/digits/hyphens, start with a letter, not end with a hyphen"
        )));
    }
    Ok(project_id)
}

/// Fully-qualified `` `project.dataset.table` `` form BigQuery expects in
/// query text — a single backtick pair around the whole dotted path.
pub(crate) fn qualified_table(cfg: &BigqueryConnectorConfig) -> Result<String, NexusError> {
    let project = validate_project_id(&cfg.project_id)?;
    let dataset = validate_identifier(&cfg.dataset_id)?;
    let table = validate_identifier(&cfg.table)?;
    Ok(format!("`{project}.{dataset}.{table}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_real_gcp_project_id_with_hyphens() {
        assert!(validate_project_id("my-project-123").is_ok());
    }

    #[test]
    fn rejects_project_id_too_short() {
        assert!(validate_project_id("abcde").is_err());
    }

    #[test]
    fn rejects_project_id_starting_with_a_digit() {
        assert!(validate_project_id("1abcde").is_err());
    }

    #[test]
    fn rejects_project_id_ending_with_a_hyphen() {
        assert!(validate_project_id("abcdef-").is_err());
    }

    #[test]
    fn rejects_project_id_with_sql_injection_attempt() {
        let err = validate_project_id("abc`; DROP TABLE users; --")
            .expect_err("malicious project id must be rejected");
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
