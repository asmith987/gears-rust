//! cluster-consumer — a minimal Profile-3 gear that *consumes* the cluster gear.
//!
//! It exposes one anonymous, externally-exposed route:
//!
//! ```text
//! POST /cluster-consumer/v1/roundtrip  { "key": "...", "value": "..." }
//!   -> { "key", "value", "version", "lock_name", "lock_acquired",
//!        "lock_released", "is_leader", "leader_status", "served_by" }
//!   -> 503 with a detail naming the unreachable endpoint (cluster unreachable)
//! ```
//!
//! The handler exercises all three cluster primitives against the cluster pod:
//! [`DistributedLockV1`](cluster_sdk::DistributedLockV1) (acquire + release),
//! [`ClusterCacheV1`](cluster_sdk::ClusterCacheV1) (put + get), and
//! [`LeaderElectionV1`](cluster_sdk::LeaderElectionV1) (join + observe + resign),
//! all resolved from the `ClientHub`. Cluster's data plane is gRPC, so — unlike
//! the REST examples in this demo — the consumer→cluster hop travels over gRPC to
//! the cluster pod, discovered by Kubernetes DNS convention
//! (`cluster.{POD_NAMESPACE}.svc.cluster.local:50051`, see
//! `cluster_sdk::wiring`). The gear itself links no cluster code (no
//! `deps = [cluster]`): the framework's proxy-wiring phase registers a remote
//! `dyn ClusterClient` before this gear's routes ever run.
//!
//! On plain loopback the DNS name does not resolve, so the round-trip returns a
//! typed `Provider{ConnectionLost}` — which the handler surfaces as a 503. That
//! is the seam proof (the gRPC path was exercised end-to-end to the socket); the
//! successful round-trip happens in Kubernetes, where the DNS convention holds.

mod domain;
mod gear;
mod rest;

pub use gear::{ClusterConsumer, DemoProfile};
