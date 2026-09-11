//! License key verification for enterprise connectors (`LICENSING.md §2`,
//! `docs/ENTERPRISE_LICENSING.md`). A license key is a JWT signed with
//! EdDSA (Ed25519) by `nexus-licensing` (a private service that does not
//! exist in this repo — see the doc above). This module only ever holds
//! the **public** key, needed to verify a signature; the private signing
//! key never appears here.
//!
//! No enterprise connector crate exists yet, so [`LicenseClaims::covers`]
//! never actually returns `true` outside tests today — but the gate itself
//! is wired: `connectors.rs::check_connector_license` calls it for every
//! source/sink build and pipeline-config validation (ROADMAP.md Fase 12,
//! Bloco 1), exercised here and in `license_store.rs` against a
//! `#[cfg(test)]`-only signing key.
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

/// Public half of `nexus-licensing`'s real Ed25519 signing key — the
/// matching private key lives only in that service's
/// `LICENSE_SIGNING_PRIVATE_KEY_PEM` env var (never committed anywhere).
/// Rotating this constant invalidates every license signed under the old
/// key, so treat a change here as a breaking event for existing customers.
///
/// Swapped out for the deliberately-public `dev_key::PUBLIC_KEY_PEM`
/// under `#[cfg(test)]` or the opt-in `test-license-key` Cargo feature
/// (see that module's and that feature's doc comments) — never on by
/// default, and never something a real release build should enable.
#[cfg(not(any(test, feature = "test-license-key")))]
const LICENSE_PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEApAng7Ch5LpOWjGWRO3+cUGtypkLWChoDFTE9eVaS4kY=\n\
-----END PUBLIC KEY-----\n";
#[cfg(any(test, feature = "test-license-key"))]
const LICENSE_PUBLIC_KEY_PEM: &str = dev_key::PUBLIC_KEY_PEM;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LicenseClaims {
    /// Customer identifier, opaque to nexus-server.
    pub sub: String,
    /// Connector slugs this license unlocks (e.g. `["salesforce", "snowflake"]`).
    pub connectors: Vec<String>,
    pub seats: u32,
    pub iat: i64,
    pub exp: i64,
    /// Signing key identifier, for key rotation on the issuer side.
    pub kid: String,
}

impl LicenseClaims {
    pub fn covers(&self, connector_slug: &str) -> bool {
        self.connectors.iter().any(|c| c == connector_slug)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LicenseError {
    #[error("license key is malformed or has an invalid signature: {0}")]
    Invalid(String),
    #[error("license key expired")]
    Expired,
}

/// Verifies a license key's signature and expiry, returning its claims.
/// Does not check which connectors it covers — callers do that with
/// [`LicenseClaims::covers`].
pub fn verify(jwt: &str) -> Result<LicenseClaims, LicenseError> {
    let decoding_key = DecodingKey::from_ed_pem(LICENSE_PUBLIC_KEY_PEM.as_bytes())
        .map_err(|e| LicenseError::Invalid(e.to_string()))?;
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_required_spec_claims(&["exp"]);
    let data = decode::<LicenseClaims>(jwt, &decoding_key, &validation).map_err(|e| {
        if matches!(e.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) {
            LicenseError::Expired
        } else {
            LicenseError::Invalid(e.to_string())
        }
    })?;
    Ok(data.claims)
}

/// Deliberately-public Ed25519 keypair — both halves committed here in
/// this OSS repo's git history, which is the opposite of secret.
/// `PUBLIC_KEY_PEM` is what `LICENSE_PUBLIC_KEY_PEM` resolves to under
/// `#[cfg(test)]` or `feature = "test-license-key"`; `PRIVATE_KEY_PEM` is
/// only ever used by `test_support::sign` below and by a local
/// `nexus-licensing` dev instance's own `LICENSE_SIGNING_PRIVATE_KEY_PEM`
/// env var (never this repo) — **never use this key for a real license**.
#[cfg(any(test, feature = "test-license-key"))]
mod dev_key {
    pub(crate) const PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAG4CuT0Rpk474C57eMF+CfZ57VDtFORdcDtc7c64eBTM=\n\
-----END PUBLIC KEY-----\n";
    #[cfg(test)]
    pub(crate) const PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIPByldYeti11Ln8Z2hkQXRrST+PoTsO/sycPsIAI24gm\n\
-----END PRIVATE KEY-----\n";
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Signing helper for this crate's own unit tests, paired with
    //! `dev_key` above.
    use super::dev_key;
    use super::LicenseClaims;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    pub fn sign(claims: &LicenseClaims) -> String {
        let key = EncodingKey::from_ed_pem(dev_key::PRIVATE_KEY_PEM.as_bytes())
            .expect("valid test PEM");
        encode(&Header::new(Algorithm::EdDSA), claims, &key).expect("signing test claims")
    }

    pub fn claims(connectors: Vec<&str>) -> LicenseClaims {
        let now = jsonwebtoken::get_current_timestamp() as i64;
        LicenseClaims {
            sub: "test-customer".into(),
            connectors: connectors.into_iter().map(String::from).collect(),
            seats: 5,
            iat: now,
            exp: now + 3600,
            kid: "test-v1".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{claims, sign};
    use super::*;

    #[test]
    fn verifies_a_correctly_signed_license() {
        let claims = claims(vec!["salesforce", "snowflake"]);
        let jwt = sign(&claims);

        let verified = verify(&jwt).unwrap();
        assert_eq!(verified, claims);
    }

    #[test]
    fn covers_checks_the_connectors_list() {
        let claims = claims(vec!["salesforce"]);
        assert!(claims.covers("salesforce"));
        assert!(!claims.covers("snowflake"));
    }

    #[test]
    fn rejects_a_tampered_signature() {
        let jwt = sign(&claims(vec!["salesforce"]));
        let mut tampered = jwt.clone();
        // Flip a char inside the signature segment (last dot-separated part).
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });

        assert!(matches!(verify(&tampered), Err(LicenseError::Invalid(_))));
    }

    #[test]
    fn rejects_an_expired_license() {
        let mut claims = claims(vec!["salesforce"]);
        // Comfortably past jsonwebtoken's default 60s leeway, not just 1s —
        // a bare `iat - 1` is inside the leeway window and wouldn't
        // actually exercise the expired-signature path.
        claims.exp = claims.iat - 120;
        let jwt = sign(&claims);

        assert!(matches!(verify(&jwt), Err(LicenseError::Expired)));
    }
}
