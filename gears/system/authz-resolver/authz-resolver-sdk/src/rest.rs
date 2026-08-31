//! REST projection of [`AuthZResolverApi`].
//!
//! Carries the HTTP method/path annotations consumed by
//! `#[toolkit::rest_contract]`. When the `rest-client` feature is enabled the
//! macro also emits `AuthZResolverApiRestClient` (and its directory-resolving
//! wrapper `AuthZResolverApiRestResolvingClient`) that implement
//! [`AuthZResolverApi`] over HTTP; when `rest-server` is enabled it emits
//! `register_auth_z_resolver_api_rest_routes` for the gear to host.
//!
//! The `evaluate` route is **internal** — authenticated on the tenant plane
//! (the caller's `SecurityContext` bearer, which the generated client attaches)
//! but deliberately not marked public, so the edge api-gateway does not expose
//! it to external clients. Only in-cluster PEPs reach it directly via directory
//! resolution.

use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;

use crate::api::AuthZResolverApi;
use crate::models::{EvaluationRequest, EvaluationResponse};

/// HTTP projection of [`AuthZResolverApi`].
#[toolkit::rest_contract(base_path = "/authz-resolver/v1")]
pub trait AuthZResolverApiRest: AuthZResolverApi {
    /// `POST /authz-resolver/v1/evaluate` — evaluate an `AuthZEN` request.
    #[post("/evaluate")]
    async fn evaluate(
        &self,
        ctx: SecurityContext,
        req: EvaluationRequest,
    ) -> Result<EvaluationResponse, CanonicalError>;
}
