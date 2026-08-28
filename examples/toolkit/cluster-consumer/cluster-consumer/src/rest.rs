//! The consumer's single REST route: a cache round-trip through the cluster gear.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::{Json, Router};
use toolkit::api::operation_builder::OperationBuilder;
use toolkit::api::OpenApiRegistry;

use crate::domain::CacheRoundTripService;

const API_TAG: &str = "Cluster Consumer";

/// Response for `GET /cluster-consumer/v1/ping`.
///
/// A cluster-free liveness route: it touches no coordination plane, so it
/// answers immediately. Used to confirm cross-process edge proxying and route
/// sync without triggering the (deliberately slow when cluster is unreachable)
/// coordination call.
#[derive(Debug, Clone)]
#[toolkit_macros::api_dto(response)]
pub struct PingResponse {
    /// Always `"pong"`.
    pub message: String,
    /// The serving process (proves edge -> `OoP` pod proxying).
    pub served_by: String,
}

/// Request body for `POST /cluster-consumer/v1/roundtrip`.
#[derive(Debug, Clone)]
#[toolkit_macros::api_dto(request)]
pub struct RoundTripRequest {
    /// The cache key to write and read back.
    pub key: String,
    /// The value to store under `key`.
    pub value: String,
}

/// Response for a successful cache round-trip.
#[derive(Debug, Clone)]
#[toolkit_macros::api_dto(response)]
pub struct RoundTripResponse {
    /// The key that was written and read back.
    pub key: String,
    /// The value read back from the cache.
    pub value: String,
    /// The entry's monotonic version (`>= 1`).
    pub version: u64,
    /// The serving process (proves edge -> `OoP` pod proxying).
    pub served_by: String,
}

/// Register the consumer's single round-trip route on `router`.
///
/// `POST /cluster-consumer/v1/roundtrip` resolves `ClusterCacheV1` from the hub
/// and does a `put` + `get`. `.anonymous()` (no bearer needed — cluster calls
/// carry a platform-plane internal token attached by the runtime, not a tenant
/// context) and `.exposed()` (reverse-proxied by the api-gateway edge).
#[allow(clippy::needless_pass_by_value)]
pub fn register_routes(
    router: Router,
    openapi: &dyn OpenApiRegistry,
    service: Arc<CacheRoundTripService>,
) -> Router {
    // GET /cluster-consumer/v1/ping — cluster-free liveness (fast, no coordination
    // call). Anonymous + exposed, so route-sync at the edge is observable without
    // driving the slow-when-unreachable round-trip below.
    let router = OperationBuilder::get("/cluster-consumer/v1/ping")
        .operation_id("cluster_consumer.ping")
        .summary("Liveness ping (no cluster call)")
        .description("Returns `pong` and the serving process id. Touches no cluster plane.")
        .tag(API_TAG)
        .exposed()
        .anonymous()
        .handler(|| async {
            Ok::<_, toolkit_canonical_errors::CanonicalError>(Json(PingResponse {
                message: "pong".to_owned(),
                served_by: format!("cluster-consumer-oop (pid {})", std::process::id()),
            }))
        })
        .json_response_with_schema::<PingResponse>(openapi, StatusCode::OK, "Pong response")
        .register(router, openapi);

    OperationBuilder::post("/cluster-consumer/v1/roundtrip")
        .operation_id("cluster_consumer.roundtrip")
        .summary("Cache round-trip through the cluster gear")
        .description(
            "Writes then reads back a key via the `ClusterCacheV1` facade resolved \
             from the ClientHub. The consumer->cluster hop travels over gRPC to the \
             cluster pod. Returns 503 (with the underlying cluster error) when the \
             cluster coordination plane is unreachable.",
        )
        .tag(API_TAG)
        .exposed()
        .anonymous()
        .json_request::<RoundTripRequest>(openapi, "")
        .handler({
            let service = Arc::clone(&service);
            move |Json(req): Json<RoundTripRequest>| {
                let service = Arc::clone(&service);
                async move {
                    service.round_trip(req.key, req.value).await.map(|out| {
                        Json(RoundTripResponse {
                            key: out.key,
                            value: out.value,
                            version: out.version,
                            served_by: out.served_by,
                        })
                    })
                }
            }
        })
        .json_response_with_schema::<RoundTripResponse>(openapi, StatusCode::OK, "Round-trip result")
        .standard_errors(openapi)
        .register(router, openapi)
}
