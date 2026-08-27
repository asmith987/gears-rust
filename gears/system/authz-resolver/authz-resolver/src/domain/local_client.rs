//! Local (in-process) client for the `AuthZ` resolver.

use std::sync::Arc;

use async_trait::async_trait;
use authz_resolver_sdk::{AuthZResolverApi, EvaluationRequest, EvaluationResponse};
use toolkit_canonical_errors::CanonicalError;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;

use super::{DomainError, Service};

/// Local client wrapping the service.
#[domain_model]
pub struct AuthZResolverLocalClient {
    svc: Arc<Service>,
}

impl AuthZResolverLocalClient {
    #[must_use]
    pub fn new(svc: Arc<Service>) -> Self {
        Self { svc }
    }
}

/// Map an infrastructure `DomainError` onto the contract's `CanonicalError`.
/// Access denial is never surfaced here — it rides in `EvaluationResponse`.
fn log_and_convert(op: &str, e: &DomainError) -> CanonicalError {
    tracing::error!(operation = op, error = ?e, "authz_resolver call failed");
    CanonicalError::internal(e.to_string()).create()
}

#[async_trait]
impl AuthZResolverApi for AuthZResolverLocalClient {
    async fn evaluate(
        &self,
        _ctx: SecurityContext,
        req: EvaluationRequest,
    ) -> Result<EvaluationResponse, CanonicalError> {
        // The subject identity travels inside `req` (AuthZEN Subject); the
        // in-process PDP does not need the caller's `SecurityContext`.
        self.svc
            .evaluate(req)
            .await
            .map_err(|e| log_and_convert("evaluate", &e))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use authz_resolver_sdk::models::{Action, EvaluationRequestContext, Resource, Subject};
    use toolkit::client_hub::ClientHub;

    use super::*;

    fn sample_request() -> EvaluationRequest {
        EvaluationRequest {
            subject: Subject {
                id: uuid::Uuid::nil(),
                subject_type: None,
                properties: HashMap::new(),
            },
            action: Action {
                name: "list".to_owned(),
            },
            resource: Resource {
                resource_type: "gts.cf.core.users.user.v1~".to_owned(),
                id: None,
                properties: HashMap::new(),
            },
            context: EvaluationRequestContext {
                tenant_context: None,
                token_scopes: Vec::new(),
                require_constraints: false,
                capabilities: Vec::new(),
                supported_properties: Vec::new(),
                bearer_token: None,
            },
        }
    }

    /// With an empty `ClientHub` the service cannot resolve types-registry (and
    /// hence no plugin), so `evaluate` surfaces a `DomainError` that
    /// `log_and_convert` maps onto `CanonicalError::Internal`. This exercises
    /// `new`, the `AuthZResolverApi::evaluate` impl, and `log_and_convert`.
    #[tokio::test]
    async fn evaluate_maps_domain_error_to_canonical_internal() {
        let svc = Arc::new(Service::new(
            Arc::new(ClientHub::default()),
            "constructorfabric".to_owned(),
        ));
        let client = AuthZResolverLocalClient::new(svc);

        let err = client
            .evaluate(SecurityContext::anonymous(), sample_request())
            .await
            .expect_err("evaluation must fail without a resolvable plugin");

        assert!(
            matches!(err, CanonicalError::Internal { .. }),
            "domain errors must map to CanonicalError::Internal, got: {err:?}"
        );
    }
}
