use nexus_core::NexusError;
use serde::Deserialize;

/// Static connector config resolved at node-configuration time (not
/// runtime). Deserialized from the DAG node's raw `config` JSON — see
/// ARCHITECTURE.md §3 (public repo).
///
/// **Read-only by design, permanently** — this connector never gains
/// a sink, same standing-instruction shape
/// `nexus-connector-stripe`'s config documents (there, "never execute
/// a financial transaction"; here, "no generic REST write surface
/// exists to build one against"). Workday's write path is its SOAP-
/// based Integration/Web Services (e.g. `Put_Worker`), a
/// fundamentally different, much larger XML/WSDL protocol surface
/// than every other connector in this repo — a real, separate effort,
/// not a v1 simplification of something smaller.
///
/// Reads via Workday's Report-as-a-Service (RaaS) — a Workday
/// custom/standard report published as a web service, real and
/// commonly used for exactly this integration pattern. Auth is Basic
/// (Workday integration system user), the documented default for
/// RaaS. `base_url` is the tenant's full RaaS host
/// (`https://{wd-instance}.workday.com`) — a field, not assembled
/// from a separate instance key, same testability contract every
/// REST connector in this repo follows.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct WorkdayConnectorConfig {
    pub base_url: String,
    pub username: String,
    pub password: String,
    /// Workday tenant name — part of the RaaS URL path
    /// (`/ccx/service/customreport2/{tenant}/{report_owner}/{report_name}`).
    pub tenant: String,
    /// The report's owner (the Workday user account it was published
    /// under) — real, required RaaS URL segment.
    pub report_owner: String,
    /// The report's name, exactly as published.
    pub report_name: String,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(flatten)]
    pub retry: nexus_core::RetryConfig,
}

impl WorkdayConnectorConfig {
    pub fn validate(&self) -> Result<(), NexusError> {
        if self.base_url.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: base_url is required".to_string(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: username is required".to_string(),
            ));
        }
        if self.password.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: password is required".to_string(),
            ));
        }
        if self.tenant.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: tenant is required".to_string(),
            ));
        }
        if self.report_owner.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: report_owner is required".to_string(),
            ));
        }
        if self.report_name.trim().is_empty() {
            return Err(NexusError::Connector(
                "workday: report_name is required".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_timeout_seconds() -> u64 {
    30
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> WorkdayConnectorConfig {
        WorkdayConnectorConfig {
            base_url: "https://wd2-impl.workday.com".into(),
            username: "isu_integration".into(),
            password: "secret".into(),
            tenant: "acme".into(),
            report_owner: "isu_integration@acme".into(),
            report_name: "Custom_Worker_Report".into(),
            timeout_seconds: 30,
            retry: Default::default(),
        }
    }

    #[test]
    fn rejects_empty_base_url() {
        let mut cfg = base_config();
        cfg.base_url = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_empty_report_name() {
        let mut cfg = base_config();
        cfg.report_name = "".into();
        assert!(cfg.validate().is_err());
    }
}
