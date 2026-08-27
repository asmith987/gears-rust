# Implementation plan — platform-plane mTLS for the gRPC transport

**Status:** proposed (implementation plan)
**Implements:** [ADR-0006 — Platform-Plane Authentication](ADR/0006-cpt-cf-adr-platform-plane-auth.md)
(the mTLS + SPIFFE end state), with rationale from
[ADR-0008 — Two-Plane Authentication](ADR/0008-cpt-cf-adr-two-plane-auth.md).
**Scope:** framework only — `libs/toolkit-transport-grpc` (client) and `gears/system/grpc-hub`
(server). **No gear source code changes**, save one small relocation in `cluster-sdk` (Workstream E).

---

## 1. Summary

The OoP gRPC transport is **plaintext (h2c) end-to-end** today. ADR-0006 sanctions this as the
*Phase-1* state — the ServiceAccount (SA) token authenticates the caller, and transport
confidentiality is delegated to the platform network / TLS termination — and defines an **end state**
in which the OoP runtime listener requires mTLS, derives peer identity from the TLS handshake, and
refuses plain-TCP inbound.

This plan implements that end state at the layer ADR-0006 assigns it to: the **runtime transport**,
never a gear. Because `grpc-hub` is a mandatory co-located gear for every process that serves gRPC,
and because outbound channels are built by a single shared helper, landing TLS in these two places
gives **every OoP gear** confidentiality with (almost) no gear-side change.

mTLS is added *underneath* the existing SA-token path, not swapped in a flag day: the two co-exist,
and plain-TCP is refused only in the final, opt-in enforcement step.

---

## 2. Motivation — why now, and why the gRPC data plane specifically

For REST/platform-plane gears, ADR-0006 permits shipping on SA-tokens and deferring mTLS: infra TLS
termination provides confidentiality in the interim (ADR-0006 §"In the SA-token first phase,
transport confidentiality comes from the platform network / TLS termination").

The **gRPC coordination plane is the exception the design itself calls out.** The `cluster` gear's
design records that its gRPC data plane has *"no viable SA-token phase"* and *"gets per-connection
authentication — mTLS + SPIFFE, not 'later' — or ships behind a `NetworkPolicy` only"*
(`gears/system/cluster/docs/DESIGN.md`), and flags ADR-0006's cached-TokenReview assumption as a
defect for a high-rate gRPC plane. In other words: for gRPC coordination, "the mTLS end state" and
"a correctly-secured transport" are the same milestone. This plan is that milestone.

Concretely, a multi-node deployment with no external TLS termination currently sends a fleet-wide
bearer credential (`X-ToolKit-Internal-Token`) over cleartext gRPC. Until this plan lands, that gap
must be closed out of band (platform-network policy / TLS termination) — this plan closes it in the
runtime, where ADR-0006 places it.

---

## 3. Assigned design (from ADR-0006 / ADR-0008)

| Aspect | Phase 1 (shipped) | End state (this plan) |
|---|---|---|
| Caller authentication | SA-token `X-ToolKit-Internal-Token` + TokenReview | TLS handshake (client-cert) |
| Confidentiality owner | Platform network / TLS termination (**infra**) | OoP **runtime listener** (**mTLS**) |
| Plain-TCP inbound | Allowed | **Rejected** on every multi-process profile once mTLS lands |
| Inbound identity | `PlatformIdentity::KubernetesServiceAccount` | `PlatformIdentity::Spiffe { trust_domain, name, version }`, parsed from the X.509 SAN |
| Outbound credential | `InternalCredential::KubeServiceAccountToken` | `InternalCredential::MtlsIdentity { cert, key, ca }` |
| Per-request header | Present | **Dropped**; the auth middleware degrades to a connection-identity shim |

Fixed points this plan must honor:

- **Stable abstraction across the swap.** `PlatformSecurityContext` is stable; only the populating
  `PlatformIdentity` variant changes (ADR-0006 §"SA token and mTLS are two backends of one stable
  abstraction — swapping does not change gear code"). Gear code changes nothing.
- **SPIFFE ID format:** `spiffe://<trust_domain>/gear/<gear>/<version>`, carried in the workload
  cert's SAN URI.
- **Cert source is a well-known path, not a config key.** Profile 2: a file watched with `notify`.
  Profile 3: a projected volume. Cert *lifecycle* (cert-manager / SPIRE-like minting and rotation) is
  **out of scope** — the framework only *consumes* rotated material.
- **The Phase-1 credential doubles as the enrollment credential** for obtaining the workload cert, so
  no new secret is introduced by the migration.
- **Profiles:** Profile 1 (embedded) — no transport, no TLS. Profile 2 single-node over UDS may skip
  mTLS (OS uid + filesystem perms is the trust root). **Profile 2 multi-node — mTLS required.**
  Profile 3 — SA-token now, mTLS + SPIFFE next.
- **Gears own none of this** (ADR-0006 §"no gear source code touches `InternalCredential` or auth
  headers … all handled by the ToolKit runtime"). The `cluster` gear formalizes this as invariant I9
  (*"no cluster-side client configuration exists"*).

> Terminology: `ServerTlsConfig` / `ClientTlsConfig` are **tonic** types used in code; the ADRs
> express the shape only as `MtlsIdentity { cert, key, ca }` and "well-known path". Keep the docs'
> vocabulary in config and identity; use the tonic types at the call sites.

---

## 4. Current state — what exists, what is missing

### Missing (the entire gRPC TLS path)
- gRPC is **plaintext h2c end-to-end**; no `tls_config` / `ClientTlsConfig` / `ServerTlsConfig`
  anywhere in the gRPC path.
- **tonic is compiled with the `transport` feature only** (workspace `Cargo.toml`, `tonic = "0.14"`).
  No TLS feature is linked — TLS is not even buildable today. This is the first blocker.
- Client builder `toolkit-transport-grpc::build_endpoint` (`libs/toolkit-transport-grpc/src/client.rs`,
  ~L157-170) configures timeouts/keepalive only; `GrpcClientConfig` (~L28-69) has no CA/cert fields.
- `grpc-hub` serves plaintext at all three paths — `serve_tcp`, `serve_uds`, `serve_named_pipe`
  (`gears/system/grpc-hub/src/gear.rs`, ~L574 / ~L612 / ~L652) — each
  `Server::builder().layer(auth).add_routes(...)`, no `.tls_config()`. `GrpcHubConfig` (~L61-107) has
  no TLS fields, and the advertised scheme is hardcoded `http://` (~L343, L345, L586).
- The generated gRPC-contract client (`libs/toolkit-contract-macros/src/grpc_contract.rs`, ~L356-380)
  has a `require_tls` **scheme-string guard** that rejects non-`https://` but attaches **no**
  tls_config — so `require_tls=true` on a gRPC client is currently unusable.
- Identity placeholders are **modeled but never populated**:
  `PlatformIdentity::Spiffe { trust_domain, name, version }` and
  `InternalCredential::MtlsIdentity { cert, key, ca }` (`libs/toolkit-security/src/internal_auth.rs`,
  ~L55 and ~L89). Both `#[non_exhaustive]`, both explicitly reserved for a later phase; `Spiffe` is
  constructed only in a unit test today.

### Exists — reuse, do not reinvent
- **`InternalAuthGrpcLayer` — the wire-once-at-the-hub precedent.** Defined in
  `libs/toolkit-transport-grpc/src/internal_auth_server.rs`; `grpc-hub` stores it in an
  `OnceLock<InternalAuthGrpcLayer>`, builds it once in `init` (an `assemble_auth_layer` helper), and
  applies `.layer(self.effective_auth_layer()?)` identically in all three serve paths. **TLS wiring
  mirrors this exactly.**
- **A complete rustls / FIPS stack already lives in `toolkit-http`.**
  `ClientAuthConfig { cert_chain, key }` and `TlsConfig { min_version, client_auth }`
  (`libs/toolkit-http/src/config.rs`, ~L531-575); the shared crypto-provider selector
  `get_crypto_provider()` (`libs/toolkit-http/src/tls.rs`, ~L96) with FIPS backends
  (`rustls-corecrypto-provider`, `rustls-cng-crypto`, `rustls-fips-shim`). Workspace rustls is `0.23`
  on `aws_lc_rs`. **gRPC TLS must build its rustls configs through the same provider selector** so the
  FIPS posture stays identical across HTTP and gRPC.
- **`ServiceAccountTokenReader`** (`libs/toolkit-transport-grpc/src/sa_token.rs`) already reads and
  periodically re-reads a projected file — reuse its refresh shape for projected-volume certs rather
  than inventing a new watcher for Profile 3.

### The one crack in "every gear inherits for free"
- **Server side — fully free.** Gears serve nothing themselves; `grpc-hub` serves their routes via
  the shared `GrpcInstallerStore` and is a **mandatory** gear for any gRPC-serving process
  (`GrpcRequiresHub` in `libs/toolkit/src/runtime/host_runtime.rs`, ~L800-802). Adding `.tls_config()`
  to grpc-hub's serve paths encrypts inbound for every gear with zero gear edits.
- **Client side — not quite free today.** The `http://` scheme literal and the channel builder live
  *inside* consuming SDKs. In `cluster-sdk`, the scheme is a literal in `derive_endpoint`
  (`gears/system/cluster/cluster-sdk/src/wiring.rs`, ~L147) and the channel is hand-built in
  `connect_lazy` (`.../src/client/remote.rs`, ~L207-219), which takes only `endpoint` +
  `internal_token_provider` — **no TLS seam.** These are code literals, not config keys (so I9 is not
  violated), but they must move to the framework for the client half to inherit TLS. This is the only
  gear-side change in the plan — **Workstream E**.

---

## 5. Solution architecture

Four framework-owned pieces plus one small SDK relocation. Organizing principle: **TLS is a property
of the transport crate and the hub, keyed off a well-known cert path — never off gear config.**

```
                       ┌──────────────────────── libs/toolkit-transport-grpc ────────────────────────┐
   well-known path ──► │  TlsMaterial (PEM loader + notify/projected-volume watcher, hot-reload)      │
   (P2 file / P3 vol)  │        │                                                                      │
                       │        ├──► ServerTlsConfig ───────────────┐                                 │
                       │        └──► ClientTlsConfig ───────┐       │  (both built via the shared     │
                       │                                    │       │   toolkit-http get_crypto_provider) │
                       └─────────────────────────────────────┼───────┼─────────────────────────────────┘
                                                             │       │
                        build_endpoint(...).tls_config(client)◄┘       └──► grpc-hub Server::builder()
                        (all outbound gRPC channels)                         .tls_config(server) in the
                                                                              3 serve paths (mirror of
                                                                              effective_auth_layer)
                                                                                   │
   inbound handshake ──► SAN spiffe://td/gear/<name>/<ver> ──► PlatformIdentity::Spiffe ──► PlatformSecurityContext
                          (parsed in the auth layer; per-request token check degrades to a shim)
```

### A. Cert material — a framework-owned loader + watcher (`toolkit-transport-grpc`)
New module (e.g. `src/tls.rs`):
- `struct TlsMaterial { cert: PathBuf, key: PathBuf, ca: PathBuf }`, mirroring
  `InternalCredential::MtlsIdentity` field-for-field so the outbound credential maps 1:1.
- Loads PEM from the **well-known path** and builds rustls `ServerConfig` / `ClientConfig`
  **through `toolkit_http::tls::get_crypto_provider()`** (FIPS-consistent), not tonic's convenience
  constructors. tonic 0.14 accepts a preconstructed rustls config — that is what keeps HTTP and gRPC
  on one crypto provider.
- Hot-reload: Profile 2 watches the path with `notify`; Profile 3 relies on the projected volume
  (reuse the `sa_token.rs` refresh shape).
- `min_version` defaults to TLS 1.2, matching `toolkit-http`'s `TlsConfig`.

### B. Server side — grpc-hub gets `ServerTlsConfig` (mirror `InternalAuthGrpcLayer`)
- Add TLS fields to `GrpcHubConfig`: well-known cert path(s) + a mode (`disabled | permissive |
  required`), parallel to the existing `internal_auth` / `internal_auth_enforcement` keys.
- Add a `tls: OnceLock<Option<ServerTlsConfig>>` gear field (mirroring the `auth_layer` field),
  populated in `init` by an `assemble_tls` helper alongside `assemble_auth_layer`, read via an
  `effective_tls()` accessor that fails closed exactly like `effective_auth_layer()`.
- In each serve path, add `.tls_config(cfg)?` on `Server::builder()` **before** routes are added
  (tonic 0.14 requires TLS configured ahead of `add_routes`). Composes with the existing `.layer()`.
- Derive the advertised scheme `http://` → `https://` when TLS is enabled (do not hardcode). UDS
  keeps `unix://` (single-node P2 may legitimately skip mTLS).

### C. Client side — the shared endpoint builder gets `ClientTlsConfig`
- Add CA/identity fields to `GrpcClientConfig`.
- In `build_endpoint`, call `.tls_config(...)` from the cert material (A) when the target scheme is
  `https`.
- Fix the generated-client path (`grpc_contract.rs`): the `require_tls` guard must *attach* a
  tls_config, not merely reject non-https — otherwise the two client entry points diverge.

### D. Identity — populate `PlatformIdentity::Spiffe` from the handshake
- In the inbound auth layer (`internal_auth_server.rs`), when the connection is mTLS, parse the peer
  cert's SAN URI `spiffe://<trust_domain>/gear/<name>/<version>` into
  `PlatformIdentity::Spiffe { .. }` and stamp the **same** `PlatformSecurityContext` extension the
  SA-token path already stamps. Downstream resolvers cannot tell which variant produced it — that is
  the stable-abstraction guarantee, and it is why no gear changes here.
- Degrade the per-request token check to a shim when the connection is already mTLS-authenticated.
  Keep SA-token validation for non-mTLS connections during co-existence.

### E. The single SDK relocation (unavoidable, small)
To let the *client* half inherit TLS, prefer lifting endpoint/channel construction into the framework:
`cluster-sdk`'s `derive_endpoint` asks a framework transport helper for the scheme, and `connect_lazy`
builds its channel through `toolkit-transport-grpc::build_endpoint` (which now owns TLS) instead of
hand-rolling `Endpoint::from_shared(...)`. Net effect: the SDK stops owning the scheme string and the
channel builder and gains TLS transparently. This *strengthens* invariant I9 (the gear owns even
less), it does not violate it. A smaller fallback — adding a framework-supplied `tls_config` parameter
to `connect_lazy` — leaves the scheme literal in the gear and is weaker; prefer the relocation.

---

## 6. Workstreams & sequencing

Each ships independently and leaves the tree green; nothing rejects plain-TCP until the final step.

1. **W0 — Enable tonic TLS + pick the provider.** Add the tonic 0.14 TLS feature aligned to the
   workspace rustls provider (`tls-aws-lc` to match `aws_lc_rs`; verify the FIPS corecrypto/cng
   providers still satisfy tonic via the shared `get_crypto_provider()`). Pure build change.
   *Gate: `cargo check --workspace` + FIPS build.*
2. **W1 — Cert material module (A).** Loader + watcher, unit-tested against fixture PEMs. No wiring.
3. **W2 — Server TLS in grpc-hub (B).** Default `disabled` → **no runtime change** until a deployment
   opts in. Test: hub serves TLS; a TLS client connects; plaintext still works while `disabled`.
4. **W3 — Client TLS in the shared builder (C).** `build_endpoint` + generated-client path. Test: TLS
   client ↔ TLS hub end-to-end.
5. **W4 — SPIFFE identity population (D).** Parse SAN → `Spiffe`, stamp `PlatformSecurityContext`,
   degrade the token shim. Test: an mTLS caller resolves to a `Spiffe` identity and existing
   ownership checks work against it unchanged.
6. **W5 — SDK relocation (E).** The one gear-side diff. Test: the gear picks up `https://` + TLS with
   no gear config; the cluster conformance suite stays green against both plugins.
7. **W6 — Enforcement / plain-TCP rejection.** Only now flip the hub mode `permissive → required` and
   enable plain-TCP rejection. Stage it: `permissive` with metrics on unencrypted/uncertified
   connections first, then `required`.

**Ordering rationale — capability before enforcement.** Servers must accept TLS (W2), clients must
present it (W3-W5), *then* plaintext is refused (W6) — never the reverse, or the fleet cannot
handshake. (A TLS client against a plaintext server fails every call; this is precisely why the fix
cannot be attempted from the client/gear side alone.)

---

## 7. Config surface (new, all framework-owned)

`GrpcHubConfig` (server) and the transport client config gain a small, symmetric block, modeled on
the existing `internal_auth*` keys so operators see one consistent shape:

```yaml
gears.grpc-hub.config:
  tls:
    mode: disabled | permissive | required   # default: disabled (backward-compatible)
    cert_path: <well-known path>              # PEM, leaf-first; P3 projected volume, P2 notify-watched
    key_path:  <well-known path>
    ca_path:   <well-known path>              # platform CA for client-cert verification
    min_version: "1.2"
```

**No gear owns any of this.** Consuming gears gain no config (invariant I9 preserved). The only
gear-*code* change is Workstream E.

---

## 8. Testing

- **Unit (W1):** cert loader parses PEM, rejects malformed input, hot-reloads on file change.
- **Transport integration (W2-W3):** TLS client ↔ TLS hub round-trips; a plaintext client against a
  `required` hub is refused at the handshake, before any application payload is read; a `disabled`
  hub still serves plaintext (backward-compat).
- **Identity (W4):** a cert with SAN `spiffe://td/gear/foo/v1` yields
  `PlatformIdentity::Spiffe { trust_domain: "td", name: "foo", version: "v1" }`, and the stamped
  `PlatformSecurityContext` is shape-identical to the SA-token path (proves the stable abstraction).
- **Gear inheritance (W5):** with framework TLS enabled and **no gear config**, the consuming gear's
  outbound uses `https://` and its ownership checks work against a `Spiffe` caller. Run the cluster
  conformance suite against both plugins; the existing baseline must hold.
- **Cross-profile parity:** Profile 1 (in-process, no TLS) and Profile 3 (mTLS) resolve callers
  through the same `PlatformSecurityContext` seam — assert no behavioral divergence at the gear
  boundary.
- **FIPS:** the whole path builds and handshakes under the corecrypto provider; mirror the existing
  `toolkit-http` FIPS test posture.

---

## 9. Risks & open questions

- **tonic 0.14 ↔ shared FIPS provider (the one real technical unknown).** Confirm tonic 0.14 accepts a
  rustls config built via `get_crypto_provider()` (corecrypto/cng) rather than forcing its own
  `aws_lc_rs`/`ring` default. If it does not, W0 needs a shim so gRPC and HTTP do not end up on two
  crypto providers. **Spike this first — it can reshape W0-W1.**
- **Cert provisioning is out of scope** (ADR-0006). This plan consumes material from the well-known
  path; the SA-token→cert enrollment exchange and the cert-manager/SPIRE component still need an
  owner. Name that owner before W6, or `required` has no certs to require.
- **`Spiffe` / `MtlsIdentity` are `#[non_exhaustive]` placeholders** — populating them is net-new
  `toolkit-security` work (W4), not a wire-up. Fields exist; parsing/validation does not.
- **UDS single-node** may legitimately skip mTLS. Keep `mode: disabled` valid, and do not let W6's
  plain-TCP rejection leak onto the UDS path.
- **The gRPC data plane wants mTLS sooner than a REST gear would.** If the platform sequences mTLS
  after other gears' SA-token rollout, the gRPC coordination plane is the consumer that should pull it
  forward.

---

## 10. Relationship to other work

- **Complements, does not depend on, the inbound-auth layer** (`InternalAuthGrpcLayer`). That work
  added SA-token *authentication*; this adds transport *confidentiality* and the mTLS *identity*
  backend. They share the `PlatformSecurityContext` seam and the wire-once-at-the-hub pattern, and
  land at the same layer.
- **Retires the platform half of the coordination-plane confidentiality gap.** Until W6, deployments
  needing confidentiality before this lands must provide it out of band (platform-network policy / TLS
  termination), per ADR-0006's Phase-1 posture.
