//! Domain service: exercises all three cluster coordination primitives —
//! distributed lock (acquire + release), cache (put + get), and leader election
//! — proving cross-pod coordination over gRPC.

use std::sync::Arc;
use std::time::Duration;

use cluster_sdk::cache::{PutRequest, Ttl};
use cluster_sdk::{ClusterCacheV1, ClusterError, DistributedLockV1, LeaderElectionV1};
use toolkit::ClientHub;
use toolkit_canonical_errors::CanonicalError;

use crate::gear::DemoProfile;

/// The keyspace this gear works under, carved out of the `demo` profile so the
/// demo never collides with another consumer's keys/locks/elections (DESIGN §3.8).
const SCOPE: &str = "cluster-consumer";
/// The lock and election coordinate under this name. Unlike cache *keys* (which
/// allow `/` for scoping), lock/leader *names* must match `[a-zA-Z0-9_-]`
/// (`validate_cluster_name`), so this is a flat, slash-free name — do NOT apply
/// `.scoped()` to the lock/leader facades (its `prefix + "/"` would be rejected
/// on the remote path).
const COORD_NAME: &str = "cluster-consumer-reservation";
/// TTL for the demo lock — a crashed holder cannot block others past this.
const LOCK_TTL: Duration = Duration::from_secs(10);
/// How long `lock()` waits to acquire before giving up.
const LOCK_WAIT: Duration = Duration::from_secs(5);
/// How long to let leadership settle (the snapshot lags backend truth by up to a
/// renewal interval); a single participant should win within this window.
const LEADER_SETTLE: Duration = Duration::from_secs(8);

/// The outcome of one coordination cycle, returned to the REST layer.
pub struct CoordinationOutcome {
    /// The cache key that was written and read back.
    pub key: String,
    /// The value read back from the cache.
    pub value: String,
    /// The entry's monotonic version (`>= 1`).
    pub version: u64,
    /// The distributed lock that was held (a non-empty name proves acquisition).
    pub lock_name: String,
    /// Whether the lock was explicitly released (vs. left to lapse via TTL).
    pub lock_released: bool,
    /// Whether this participant observed itself as leader of the election.
    pub is_leader: bool,
    /// The observed leadership status (`Leader` / `Follower` / ...).
    pub leader_status: String,
    /// The serving process — proof the request reached this `OoP` pod.
    pub served_by: String,
}

/// Resolves the three cluster facades from the hub per call and runs one
/// coordination cycle. Holding only an `Arc<ClientHub>` keeps this
/// binding-mode-agnostic: co-located cluster gear (Profile 1) or the remote gRPC
/// proxy (Profile 3) is invisible here.
pub struct CoordinationService {
    hub: Arc<ClientHub>,
}

impl CoordinationService {
    /// Build the service over a `ClientHub` handle (typically `ctx.client_hub()`).
    #[must_use]
    pub fn new(hub: Arc<ClientHub>) -> Self {
        Self { hub }
    }

    /// Run a full coordination cycle for `key`/`value`:
    ///
    /// 1. **Lock** — acquire the `reservation` lock, then release it. Per ADR-002
    ///    no remote I/O happens while the guard is held, so the cache write below
    ///    is performed *after* release (the lock serializes; it does not wrap the
    ///    mutation).
    /// 2. **Cache** — `put` then `get` the reservation value.
    /// 3. **Leader election** — join the `reservation` election, observe whether
    ///    this participant is leader, then resign.
    ///
    /// # Errors
    /// Returns [`CanonicalError::service_unavailable`] when any cluster call
    /// cannot be resolved or reached (on loopback the k8s DNS endpoint does not
    /// resolve, so the first call returns a retryable `Provider{ConnectionLost}`),
    /// carrying the underlying [`ClusterError`] in the detail. Returns
    /// [`CanonicalError::internal`] if the key vanished between `put` and `get`.
    pub async fn coordinate(
        &self,
        key: String,
        value: String,
    ) -> Result<CoordinationOutcome, CanonicalError> {
        // 1) LOCK: acquire, then release. The critical section holds no remote I/O
        //    (ADR-002) — it exists here to serialize concurrent coordinators.
        let lock = DistributedLockV1::resolver(&self.hub)
            .profile(DemoProfile)
            .resolve()
            .await
            .map_err(|e| unavailable(&e))?;
        let guard = lock
            .lock(COORD_NAME, LOCK_TTL, LOCK_WAIT)
            .await
            .map_err(|e| unavailable(&e))?;
        let lock_name = guard.name().to_owned();
        guard.release().await.map_err(|e| unavailable(&e))?;

        // 2) CACHE: the remote effect, performed outside the lock window.
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

        // 3) LEADER ELECTION: join, let leadership settle, then resign.
        let leader = LeaderElectionV1::resolver(&self.hub)
            .profile(DemoProfile)
            .resolve()
            .await
            .map_err(|e| unavailable(&e))?;
        let mut watch = leader.elect(COORD_NAME).await.map_err(|e| unavailable(&e))?;
        // The snapshot lags backend truth; poll (via `changed`) until we observe
        // leadership or the settle window elapses. A single participant wins.
        let settle_by = tokio::time::Instant::now() + LEADER_SETTLE;
        while !watch.is_leader() && tokio::time::Instant::now() < settle_by {
            // Advance to the next status event (or tick on timeout), then re-check
            // the snapshot; the outcome itself is intentionally ignored.
            let _elapsed = tokio::time::timeout(Duration::from_secs(1), watch.changed()).await;
        }
        let is_leader = watch.is_leader();
        let leader_status = format!("{:?}", watch.status());
        watch.resign().await.map_err(|e| unavailable(&e))?;

        Ok(CoordinationOutcome {
            key,
            value: String::from_utf8_lossy(&entry.value).into_owned(),
            version: entry.version,
            lock_name,
            lock_released: true,
            is_leader,
            leader_status,
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
        .with_detail(format!("cluster coordination call failed: {err}"))
        .create()
}
