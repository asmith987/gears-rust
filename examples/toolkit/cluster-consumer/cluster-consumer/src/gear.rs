//! Gear definition, cluster profile marker, and REST wiring.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;

use cluster_sdk::ClusterProfile;
use toolkit::api::OpenApiRegistry;
use toolkit::{Gear, GearCtx, RestApiCapability};

use crate::domain::CoordinationService;
use crate::rest;

/// The cluster profile this consumer binds. Its [`NAME`](ClusterProfile::NAME)
/// must match a profile the cluster gear serves (see `config/oop-cluster.yaml`,
/// which configures a `demo` profile on the `standalone` backend).
#[derive(Clone, Copy)]
pub struct DemoProfile;

impl ClusterProfile for DemoProfile {
    const NAME: &'static str = "demo";
}

// The consumer's entire cluster-facing declaration: this line, plus the
// `.profile(DemoProfile)` call in the resolver (see `domain.rs`). No wiring
// call, no endpoint, no mode flag — the framework replays cluster-sdk's
// `ConsumerRegistration` for us (DESIGN §4.9.2).
cluster_sdk::register_cluster_profile!(DemoProfile);

/// Cluster-consuming demo gear.
///
/// - `#[toolkit::gear]` registers it under `cluster-consumer` (the kebab-case of
///   the struct ident — required, or the framework's static consumer-wiring
///   override cannot resolve it) with a REST capability.
/// - It deliberately declares **no** `deps = [cluster]`: a Profile-3 consumer
///   does not link the cluster gear, and `deps` is a hard topo-sort edge that
///   would make the process refuse to start. Ordering (cluster is a `system`
///   gear) and readiness gating come from cluster-sdk's `ConsumerRegistration`
///   without it.
#[toolkit::gear(name = "cluster-consumer", capabilities = [rest])]
#[derive(Default)]
pub struct ClusterConsumer;

#[async_trait]
impl Gear for ClusterConsumer {
    async fn init(&self, _ctx: &GearCtx) -> Result<()> {
        // Lazy consumption: nothing is resolved at init (§4.9.1 forbids it — no
        // provider's `start` has run yet). The cache is resolved from the hub
        // per request, so startup never blocks on cluster reachability.
        tracing::info!("cluster-consumer initialized");
        Ok(())
    }
}

impl RestApiCapability for ClusterConsumer {
    fn register_rest(
        &self,
        ctx: &GearCtx,
        router: Router,
        openapi: &dyn OpenApiRegistry,
    ) -> Result<Router> {
        tracing::info!("Registering cluster-consumer REST routes");
        // The service holds the hub and resolves the cluster facades per call.
        let service = Arc::new(CoordinationService::new(ctx.client_hub()));
        Ok(rest::register_routes(router, openapi, service))
    }
}
