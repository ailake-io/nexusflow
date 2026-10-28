//! "Infra" canvas backend — catalog of curated Terraform modules (AWS-only
//! for now) plus the codegen that turns a drawn graph into real `.tf`
//! files. See `nexus-server`'s `infra.rs` for the HTTP
//! surface, and `nexus-core`'s `infra_registry.rs` for the shared
//! plugin/wire types both sides use.
//!
//! **Wrapper-modules strategy** (2026-09-10, changed mid-session from an
//! earlier "vendor our own opinionated HCL" plan): every module here wraps
//! a well-established public `terraform-aws-modules` registry module
//! (pinned source+version) or, where no clear community standard exists,
//! emits plain AWS provider `resource` blocks directly — never our own
//! from-scratch "better than the community" module content. Two reasons:
//! (1) security — a hand-rolled VPC/S3/IAM module written in one session
//! can't match years of community hardening (least-privilege defaults,
//! encryption-by-default, edge cases), and getting this wrong is a
//! customer's real cloud security posture, not just a broken pipeline run;
//! (2) it keeps the door open to other clouds later — wrapping a GCP/Azure
//! community module is the same shape of work as adding another AWS one,
//! no rewrite of a proprietary-HCL strategy needed.

mod codegen;
mod hcl;
mod modules;

use nexus_core::submit_infra_generator;

submit_infra_generator!(codegen::generate);

#[cfg(test)]
mod tests {
    use nexus_core::{InfraGenerator, InfraModuleDescriptor};

    #[test]
    fn generator_is_registered() {
        assert!(InfraGenerator::get().is_some());
    }

    #[test]
    fn all_modules_have_a_working_dispatch_entry() {
        // Every InfraModuleDescriptor registered must also resolve through
        // `modules::dispatch` — a module registered for the catalog but
        // missing from the dispatch table would list in GET /infra/modules
        // yet fail generation with "unknown infra module", a confusing gap
        // this test catches at compile-test time instead.
        for descriptor in InfraModuleDescriptor::all() {
            assert!(
                crate::modules::dispatch(descriptor.id).is_some(),
                "module '{}' is registered but has no dispatch entry",
                descriptor.id
            );
        }
    }
}
