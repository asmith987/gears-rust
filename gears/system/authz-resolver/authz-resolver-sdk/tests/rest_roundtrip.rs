//! REST round-trip test for the `AuthZResolverApi` contract projection.
//!
//! Hosts the generated server routes
//! (`register_auth_z_resolver_api_rest_routes`) on an ephemeral axum server and
//! drives them with the generated `AuthZResolverApiRestClient`, asserting the
//! allow/deny decision survives the HTTP boundary. This locks in the wire
//! surface introduced by the REST-contract migration.

#![cfg(all(feature = "rest-client", feature = "rest-server"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use uuid::Uuid;

use authz_resolver_sdk::models::{
    Action, EvaluationRequestContext, EvaluationResponseContext, Resource, Subject,
};
use authz_resolver_sdk::rest::register_auth_z_resolver_api_rest_routes;
use authz_resolver_sdk::{
    AuthZResolverApi, AuthZResolverApiRestClient, EvaluationRequest, EvaluationResponse,
};

use toolkit::api::OpenApiRegistryImpl;
use toolkit_canonical_errors::CanonicalError;
use toolkit_contract::runtime::config::ClientConfig;
use toolkit_security::SecurityContext;

/// Mock PDP: allows `get`, denies everything else. Mirrors the historical
/// gRPC round-trip fixture.
struct AllowGetResolver;

#[async_trait]
impl AuthZResolverApi for AllowGetResolver {
    async fn evaluate(
        &self,
        _ctx: SecurityContext,
        req: EvaluationRequest,
    ) -> Result<EvaluationResponse, CanonicalError> {
        Ok(EvaluationResponse {
            decision: req.action.name == "get",
            context: EvaluationResponseContext::default(),
        })
    }
}

fn sample_request(action: &str) -> EvaluationRequest {
    EvaluationRequest {
        subject: Subject {
            id: Uuid::new_v4(),
            subject_type: Some("user".to_owned()),
            properties: HashMap::new(),
        },
        action: Action {
            name: action.to_owned(),
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

async fn start_test_server() -> String {
    let service: Arc<dyn AuthZResolverApi> = Arc::new(AllowGetResolver);

    // The gateway normally injects the SecurityContext; in tests a per-request
    // layer materializes an anonymous one for every request.
    let secctx_layer = axum::middleware::from_fn(
        |mut req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| async move {
            req.extensions_mut().insert(SecurityContext::anonymous());
            next.run(req).await
        },
    );

    let openapi = OpenApiRegistryImpl::new();
    let app: Router = register_auth_z_resolver_api_rest_routes(Router::new(), &openapi, service)
        .layer(secctx_layer);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn http_client(base_url: &str) -> AuthZResolverApiRestClient {
    AuthZResolverApiRestClient::new(ClientConfig::new(base_url.to_owned()))
        .expect("default toolkit-http client build is infallible in tests")
}

#[tokio::test]
async fn rest_evaluate_allows_get() {
    let base_url = start_test_server().await;
    let client = http_client(&base_url);

    let resp =
        AuthZResolverApi::evaluate(&client, SecurityContext::anonymous(), sample_request("get"))
            .await
            .unwrap();
    assert!(resp.decision, "get should be allowed over REST");
}

#[tokio::test]
async fn rest_evaluate_denies_delete() {
    let base_url = start_test_server().await;
    let client = http_client(&base_url);

    let resp = AuthZResolverApi::evaluate(
        &client,
        SecurityContext::anonymous(),
        sample_request("delete"),
    )
    .await
    .unwrap();
    assert!(!resp.decision, "delete should be denied over REST");
}
