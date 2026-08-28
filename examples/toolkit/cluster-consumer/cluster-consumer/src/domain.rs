//! Domain service: resolves `ClusterCacheV1` from the hub and does a cache
//! round-trip, proving a cross-pod gRPC coordination call.

use std::sync::Arc;

use cluster_sdk::cache::{PutRequest, Ttl};
use cluster_sdk::{ClusterCacheV1, ClusterError};
use toolkit::ClientHub;
use toolkit_canonical_errors::CanonicalError;

use crate::gear::DemoProfile;

/// The keyspace this gear writes under, carved out of the `demo` profile so the
/// demo never collides with another consumer's keys (DESIGN §3.8).
const SCOPE: &str = "cluster-consumer";

/// The outcome of a successful round-trip, returned to the REST layer.
pub struct RoundTripOutcome {
    /// The key that was written and read back (as the caller supplied it).
    pub key: String,
    /// The value read back from the cache.
    pub value: String,
    /// The entry's monotonic version (`>= 1`).
    pub version: u64,
    /// The serving process — proof the request reached this `OoP` pod.
    pub served_by: String,
}

/// Resolves `ClusterCacheV1` from the hub per call and performs a `put` + `get`.
///
/// Holding only an `Arc<ClientHub>` keeps this binding-mode-agnostic: whether the
/// resolved client is a co-located cluster gear (Profile 1, local-wins) or the
/// remote gRPC proxy (Profile 3) is invisible here.
pub struct CacheRoundTripService {
    hub: Arc<ClientHub>,
}

impl CacheRoundTripService {
    /// Build the service over a `ClientHub` handle (typically
    /// `ctx.client_hub()`).
    #[must_use]
    pub fn new(hub: Arc<ClientHub>) -> Self {
        Self { hub }
    }

    /// Resolve the cache for the `demo` profile, write `value` under `key`, then
    /// read it back.
    ///
    /// # Errors
    /// Returns [`CanonicalError::service_unavailable`] when the cluster cache
    /// cannot be resolved or reached (on loopback the k8s DNS endpoint does not
    /// resolve, so the call returns a retryable `Provider{ConnectionLost}`),
    /// carrying the underlying [`ClusterError`] in the detail. Returns
    /// [`CanonicalError::internal`] if the key vanished between `put` and `get`.
    pub async fn round_trip(
        &self,
        key: String,
        value: String,
    ) -> Result<RoundTripOutcome, CanonicalError> {
        // Resolve the facade. This is `Ok` even when cluster is unreachable — the
        // only await is a bounded descriptor fetch, and a missing descriptor
        // defers validation to readiness rather than failing here (invariant I6).
        let cache = ClusterCacheV1::resolver(&self.hub)
            .profile(DemoProfile)
            .resolve()
            .await
            .map_err(|e| unavailable(&e))?
            .scoped(SCOPE)
            .map_err(|e| unavailable(&e))?;

        cache
            .put(PutRequest {
                key: &key,
                value: value.as_bytes(),
                ttl: Ttl::Indefinite,
            })
            .await
            .map_err(|e| unavailable(&e))?;

        let entry = cache.get(&key).await.map_err(|e| unavailable(&e))?.ok_or_else(|| {
            CanonicalError::internal(
                "cache reported the key absent immediately after a successful put",
            )
            .create()
        })?;

        Ok(RoundTripOutcome {
            key,
            value: String::from_utf8_lossy(&entry.value).into_owned(),
            version: entry.version,
            served_by: format!("cluster-consumer-oop (pid {})", std::process::id()),
        })
    }
}

/// Maps a [`ClusterError`] to a 503 whose detail names the failure.
///
/// The error renders its own message (an endpoint, a provider name, a typed
/// kind) with no caller-supplied data, so it is safe for the public `Problem`
/// body per the `with_detail` contract.
fn unavailable(err: &ClusterError) -> CanonicalError {
    CanonicalError::service_unavailable()
        .with_detail(format!("cluster cache call failed: {err}"))
        .create()
}
