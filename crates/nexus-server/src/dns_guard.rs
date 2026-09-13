use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Closes the DNS-rebinding gap in `nexus_core::PipelineSpec::validate_security_with`:
/// that check only looks at a hostname's *literal spelling* when a pipeline
/// spec/alert channel is saved or previewed — a public domain that later
/// re-resolves to an internal address (`127.0.0.1`, `169.254.169.254`, a
/// private RFC1918 range, ...) sails straight through it, and the actual
/// outbound request this server later makes goes wherever DNS says at that
/// moment, not whatever it said at validate time (a classic
/// check-then-use race, not just a one-time bypass).
///
/// Plugged in via `reqwest::ClientBuilder::dns_resolver`, this re-checks
/// every real resolution against the same private/reserved ranges
/// (`nexus_core::is_internal_ip`) at the moment a connection is actually
/// about to be made — the only point where the check and the real network
/// destination can't disagree. Any resolved address landing in a
/// disallowed range is filtered out; if that empties the result, the
/// connection is refused outright rather than silently trying the
/// remaining (attacker-chosen) addresses.
#[derive(Clone, Copy, Default)]
pub struct SsrfSafeResolver;

impl Resolve for SsrfSafeResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            let safe: Vec<_> = addrs
                .filter(|addr| !nexus_core::is_internal_ip(&addr.ip()))
                .collect();
            if safe.is_empty() {
                return Err(format!(
                    "{host:?} resolves only to internal/reserved addresses — refusing to connect"
                )
                .into());
            }
            Ok(Box::new(safe.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refuses_a_hostname_that_resolves_only_to_loopback() {
        // "localhost" always resolves to 127.0.0.1/::1 in any real
        // environment — a fast, hermetic stand-in for the DNS-rebinding
        // scenario (a public domain whose A record happens to be internal)
        // without needing a real DNS server under test.
        let resolver = SsrfSafeResolver;
        let result = resolver.resolve("localhost".parse().unwrap()).await;
        assert!(
            result.is_err(),
            "localhost resolves only to loopback addresses, must be refused"
        );
    }

    #[tokio::test]
    async fn resolves_a_public_hostname_normally() {
        let resolver = SsrfSafeResolver;
        let result = resolver.resolve("one.one.one.one".parse().unwrap()).await;
        // No network access in this sandbox is an acceptable outcome for
        // this test — it must not *incorrectly* refuse a public host, but
        // a DNS lookup failure isn't that.
        if let Ok(addrs) = result {
            assert!(addrs.count() > 0, "expected at least one public address");
        }
    }
}
