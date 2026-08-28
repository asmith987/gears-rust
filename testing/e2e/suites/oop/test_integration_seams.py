"""Out-of-process (loopback) integration seams.

Each test targets exactly one cross-process seam that only manifests when the
platform-host and the gears run as separate processes wired by the
DirectoryService — none is reachable by a unit test or an in-process E2E.

Topology (booted by conftest.oop_cluster):
    platform-host   edge :8087  + DirectoryService :50051
    hello-oop       :9091   (anonymous REST)
    api-contracts-oop            :9097  (PaymentApi REST provider)
    api-contracts-consumer-oop   :9098  (resolves PaymentApi from the provider)
    cluster-oop     :9092 (probes) + :50061 (coordination gRPC, standalone)
    cluster-consumer-oop         :9093  (resolves ClusterCacheV1 over gRPC)
"""
from __future__ import annotations

import httpx
import pytest

TIMEOUT = 5.0
# The cluster-unreachable round-trip call takes ~8s (DNS failure + connector
# backoff). The edge wraps proxied routes in a 30s timeout, so it forwards the
# call and returns the gear's 503 well within budget — no need to bypass it.
SLOW_SEAM_TIMEOUT = 20.0


@pytest.mark.smoke
def test_edge_healthz(oop_cluster):
    """Seam: the platform-host edge is up and serving."""
    r = httpx.get(f"{oop_cluster}/healthz", timeout=TIMEOUT)
    assert r.status_code == 200, r.text


@pytest.mark.smoke
def test_hello_anonymous_cross_process_proxy(oop_cluster):
    """Seam: edge reverse-proxies an anonymous route to a separate gear process.

    `served_by` is the serving process id — proof the request crossed the
    process boundary (edge -> hello-oop) rather than being handled in-process.
    """
    r = httpx.get(f"{oop_cluster}/hello/v1/ping", timeout=TIMEOUT)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("message") == "pong", body
    assert "hello-oop" in str(body.get("served_by", "")), body


def test_missing_bearer_rejected_at_edge(oop_cluster):
    """Seam: tenant-plane gating — an authenticated route needs a bearer."""
    r = httpx.post(
        f"{oop_cluster}/api-contracts-consumer/v1/charge",
        json={"amount_cents": 1000, "currency": "USD", "description": "no-token"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"


@pytest.mark.smoke
def test_oop_to_oop_charge_over_rest(oop_cluster, auth):
    """Seam: OoP -> OoP contract call over REST, discovered via the directory.

    A single edge request drives ingress -> consumer pod -> provider pod: the
    consumer resolves `PaymentApi` from the SEPARATE provider process (its
    binary does not link the provider), so the charge can only travel over REST.
    """
    r = httpx.post(
        f"{oop_cluster}/api-contracts-consumer/v1/charge",
        headers={**auth, "Content-Type": "application/json"},
        json={"amount_cents": 1000, "currency": "USD", "description": "oop-e2e charge"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.text}"
    body = r.json()
    assert body.get("payment_id"), body
    assert body.get("status") == "pending", body


def test_provider_reachable_only_through_consumer(oop_cluster, auth):
    """Seam: the provider's PaymentApi route is `#[internal]`, not edge-exposed.

    The provider executes charges (verified via the consumer path above), but
    its `/api-contracts/v1/charge` is not published to the edge — only the
    consumer's `.exposed()` route is. A direct edge call must not succeed.
    """
    r = httpx.post(
        f"{oop_cluster}/api-contracts/v1/charge",
        headers={**auth, "Content-Type": "application/json"},
        json={"amount_cents": 1000, "currency": "USD", "description": "direct"},
        timeout=TIMEOUT,
    )
    assert r.status_code in (401, 403, 404), (
        f"provider charge should not be edge-exposed, got {r.status_code}: {r.text}"
    )


@pytest.mark.smoke
def test_cluster_consumer_ping_cross_process(oop_cluster):
    """Seam: edge reverse-proxies the cluster consumer's anonymous ping route.

    A cluster-free route, so it proves cross-process proxying (edge ->
    cluster-consumer-oop) without touching the coordination plane. `served_by`
    is the serving process id.
    """
    r = httpx.get(f"{oop_cluster}/cluster-consumer/v1/ping", timeout=TIMEOUT)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("message") == "pong", body
    assert "cluster-consumer-oop" in str(body.get("served_by", "")), body


def test_cluster_consumer_grpc_seam_connectionlost(oop_cluster):
    """Seam: the consumer resolves the REMOTE cluster client and drives the gRPC
    coordination path end-to-end to the socket.

    The consumer links no cluster code (no `deps = [cluster]`); the framework's
    proxy-wiring phase registers a remote `dyn ClusterClient` from cluster-sdk's
    `ConsumerRegistration` before the gear's routes run. The endpoint is derived
    from k8s DNS convention (`cluster.{POD_NAMESPACE}.svc.cluster.local:50051`),
    which does not resolve on loopback — so the `put`/`get` returns a typed,
    retryable `Provider{ConnectionLost}`, surfaced by the handler as a 503. That
    is the seam proof; the successful round-trip is the Kubernetes demo's job.

    Driven through the edge (like every other seam here): the unreachable call
    takes ~8s, comfortably inside the edge's 30s proxy timeout, so this exercises
    the full ingress -> consumer pod -> cluster-gRPC path.
    """
    r = httpx.post(
        f"{oop_cluster}/cluster-consumer/v1/roundtrip",
        json={"key": "seat/12", "value": "held"},
        timeout=SLOW_SEAM_TIMEOUT,
    )
    assert r.status_code == 503, f"expected 503 (cluster unreachable on loopback), got {r.status_code}: {r.text}"
    # The detail names the failure, so an operator sees the seam was exercised.
    assert "cluster cache call failed" in r.text, r.text
