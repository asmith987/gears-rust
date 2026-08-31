# OoP Readiness Inventory

Point-in-time sweep of every gear in the repo against out-of-process (OoP)
readiness requirements, including the gears that intentionally remain in
`platform-host`.

**Legend:** 🟢 ready & committed · 🔴 not ready / blocking · 🟡 ready but
demo-grade (works in the committed demo, not production-hardened — e.g. static
authn) · ⚪ N/A (requirement doesn't apply to this gear)

This version reflects the OoP scaffolding now committed to the repo
(platform-host app, OoP gear binaries, Helm platform chart, and deploy tooling),
last validated by a full tear-down and re-deploy of the `cf-gears`/`platform`
release in minikube. Both runtime paths are covered by committed tests: the
self-managed loopback suite `testing/e2e/suites/oop` and the scripted Kubernetes
smoke `deploy/oop-smoke.sh`.

**Columns:**

- **Provides Contract** — *provider* side: does this gear expose a REST/gRPC
  contract that *other* gears can consume cross-process (vs. only an
  in-process trait nothing outside its own process can reach)? (⚪ if nothing
  downstream needs to reach this gear)
- **Consumes Cleanly** — *consumer* side: does this gear reach *its own*
  dependencies via `#[toolkit::consumes]` (works local or remote), rather
  than a compile-time `deps=[...]` link that forces co-location? (⚪ if the
  gear has no gear-to-gear dependencies at all)
- **OoP Binary** — has an `oop_module`-gated bin (or dedicated OoP crate)
- **DB Isolation** — DB-backed gears get their own database
- **Authn Stack** — embedded tenant-plane authn wired (⚪ if the gear is
  anonymous)
- **k8s-auth** — platform-plane TokenReview wired
- **Helm Chart** — standalone deployable chart exists
- **Notes / Staged changes** — current blocker, or what changed this session

## Readiness by gear

| Gear | Category | Provides Contract | Consumes Cleanly | OoP Binary | DB Isolation | Authn Stack | k8s-auth | Helm | Notes / Staged changes |
|---|---|---|---|---|---|---|---|---|---|
| `authz-resolver` | Trust-coupled core | 🟢 REST (`rest.rs`) | 🔴 `deps=[types_registry]` | 🔴 none | ⚪ no DB | ⚪ | 🔴 not wired | ⚪ (bundled in platform-host chart) | Provider contract consumed via `#[toolkit::consumes]` by `users-info` (exercised remotely in the OoP demo); the in-process gears (`simple-user-settings`, `file-storage`, `chat-engine`, `usage-collector`) still hard-link it via `deps=[authz_resolver]`. Still blocked by `types_registry` hard-dep and synthetic `SecurityContext::anonymous()` in its internal chain. |
| `tenant-resolver` | Trust-coupled core | 🔴 none | 🔴 `deps=[types_registry]` | 🔴 none | ⚪ no DB | ⚪ | 🔴 not wired | ⚪ | No contract yet; same `types_registry` blocker. `rg-tr-plugin` reads `resource-group` DB directly. |
| `resource-group` | Trust-coupled core | 🔴 none | 🔴 `deps=[authz_resolver, types_registry]` | 🔴 none | 🟢 pg (own database on shared Postgres) | ⚪ | 🔴 not wired | ⚪ | Missing REST contract; unblocks `authz-resolver` + `tenant-resolver` once added. |
| `account-management` | Trust-coupled core | 🔴 none | 🔴 `deps=[authz_resolver, types_registry, resource_group, tenant_resolver]` | 🔴 none | 🟢 pg (own database on shared Postgres) | ⚪ | 🔴 not wired | ⚪ | No contract; synthetic `am.system` credential needs real S2S migration. |
| `gear-orchestrator` | System / platform plumbing | 🟢 gRPC `DirectoryService` — consumed by every gear for discovery | ⚪ (no deps) | 🔴 none | ⚪ no DB | ⚪ | 🔴 not wired | ⚪ | Discovery mechanism; stays in host by definition. |
| `grpc-hub` | System / platform plumbing | ⚪ transport layer, not an app-level contract | ⚪ (no deps) | 🔴 none | ⚪ no DB | ⚪ | 🟢 inbound TokenReview validator | ⚪ | Plumbing; stays in host. |
| `api-gateway` | System / platform plumbing | ⚪ edge, external HTTP traffic, not `ClientHub` consumption | 🔴 `deps=[grpc_hub, authn_resolver]` | 🔴 none | ⚪ no DB | ⚪ | 🟢 outbound proxy credential (`gateway_proxy.internal_auth`) + `advertise_uri` | ⚪ | Reverse-proxy to OoP gears; stays co-located with host. |
| `types-registry` | System / platform plumbing | 🔴 manual REST endpoints but no `#[toolkit::consumes]` contract | ⚪ (no deps) | 🔴 none | ⚪ no DB (link-time inventory) | ⚪ | 🔴 not wired | ⚪ | Biggest cross-cutting blocker: `usage-collector` residual hard-dep, `oagw`, `bss-rate-provider`, `mini-chat`. |
| `credstore` | System / platform plumbing | 🔴 none | 🔴 `deps=[authz_resolver, tenant_resolver, types_registry]` | 🔴 none | 🟢 pg (own database on shared Postgres) | ⚪ | 🔴 not wired | ⚪ | No REST contract; blocks `oagw`. |
| `authn-resolver` | System / platform plumbing | ⚪ embeds per OoP pod, not consumed via `ClientHub` | 🔴 `deps=[types_registry]` | ⚪ (rides in every OoP bin's `oop_module`) | ⚪ no DB | ⚪ | ❓ not verified | ⚪ | Intentionally never extracted; every OoP pod embeds its own copy. |
| `hello` | Fully OoP | ⚪ nothing downstream consumes it | ⚪ (no deps) | 🟢 | ⚪ | ⚪ | 🟢 | 🟢 | New minimal reference gear; verified end-to-end (loopback e2e + k8s smoke). |
| `users-info` | Fully OoP | ⚪ nothing downstream consumes it | 🟢 consumes `AuthZResolverApi` via `#[toolkit::consumes]` | 🟢 | 🟢 pg (own database on shared Postgres) | 🟢 | 🟢 | 🟢 | OoP binary, own database on shared Postgres, routes `.exposed()`, verified e2e through the edge. |
| `api-contracts` | Fully OoP | 🟢 REST `PaymentApi`/`PaymentApiV2` — consumed by `api-contracts-consumer` | ⚪ (no deps) | 🟢 | ⚪ | 🟢 | 🟢 | 🟢 | OoP↔OoP REST reference pair; verified (loopback e2e + k8s smoke). |
| `api-contracts-consumer` | Fully OoP | ⚪ nothing downstream consumes it | 🟢 consumes `PaymentApi`/`PaymentApiV2` | 🟢 | ⚪ | 🟢 | 🟢 | 🟢 | OoP binary + Helm; still calls v1 alongside v2. |
| `simple-user-settings` | Not started | ⚪ nothing downstream consumes it | 🔴 `deps=[authz_resolver]` | 🔴 none | 🔴 pg-capable, not activated | 🔴 not wired | 🔴 not wired | 🔴 | In-process gear; hard `deps=[authz_resolver]` PEP link. No OoP binary, Helm chart, or platform-plane wiring. |
| `file-storage` | Not started | ⚪ nothing downstream consumes it | 🔴 `deps=[authz_resolver]` | 🔴 none | 🔴 pg-capable, not activated | 🔴 not wired | 🔴 not wired | 🔴 | In-process gear; hard `deps=[authz_resolver]` PEP link. No OoP binary, Helm chart, or platform-plane wiring. |
| `chat-engine` | Not started | ⚪ nothing downstream consumes it | 🔴 `deps=[authz_resolver]` | 🔴 none | 🔴 pg-capable, not activated | 🔴 not wired | 🔴 not wired | 🔴 | In-process gear; hard `deps=[authz_resolver]` PEP link. Has its own separate `k8s` (leader-election) feature unrelated to `k8s-auth`. No OoP binary, Helm chart, or platform-plane wiring. |
| `usage-collector` | Not started | ⚪ nothing downstream consumes it | 🔴 `deps=[types_registry, authz_resolver]` | 🔴 none | ⚪ (plugin-owned storage) | 🔴 not wired | 🔴 not wired | 🔴 | Hard `deps=[types_registry, authz_resolver]`; no OoP binary, Helm chart, or platform-plane wiring. |
| `oagw` | Not started | 🔴 none | 🔴 `deps=[types_registry, authz_resolver, credstore, tenant_resolver]` | 🔴 | ⚪ | 🔴 | 🔴 | 🔴 | Reverted to baseline this session. |
| `mini-chat` | Not started | 🔴 none | 🔴 `deps=[types_registry, authn_resolver, authz_resolver, oagw]` | 🔴 | 🔴 pg-capable, not activated | 🔴 | 🔴 | 🔴 | Hardest blocker is `oagw`. |
| `bss-ledger` | Not started | 🔴 none | 🔴 `deps=[types_registry, authz_resolver, account_management]` | 🔴 | 🔴 pg-capable, not activated | 🔴 | 🔴 | 🔴 | Hard-deps on trust-coupled-core `account_management`. |
| `bss-rate-provider` (+`ecb`/`http-json` plugins) | Not started | ⚪ n/a, internal only | 🔴 `deps=[types_registry]` | 🔴 | ⚪ | ⚪ anonymous | ⚪ | 🔴 | Simplest remaining blocker — only `types-registry`. |
| `file-parser` | Not started | ⚪ nothing downstream consumes it | 🟢 (no `deps` declared) | 🔴 | ⚪ | 🔴 needs embedded authn stack | 🔴 | 🔴 | No hard deps; otherwise a `hello`-shape candidate. |
| `nodes-registry` | Not started | 🔴 plain REST, not `#[toolkit::consumes]`-wireable | 🟢 (no `deps`) | 🔴 | ⚪ | ⚪ anonymous | 🔴 | 🔴 | Simplest gear to convert — zero deps, zero auth. |
| `event-broker` | Not started | ⚪ n/a | 🔴 hard Rust crate dep on `cluster` | 🔴 | ⚪ | ❓ | 🔴 | 🔴 | Can't split without `cluster` gaining a remote API. |
| `cluster` | Not started | 🔴 none, and likely shouldn't have one | ⚪ (no deps) | 🔴 | ⚪ | ⚪ | ⚪ | 🔴 | Distributed primitive; likely stays linked-in. |

> Other gear-shaped directories exist (`approval-service`,
> `infrastructure-resource-manager`, `llm-gateway`, `model-registry`,
> `serverless-runtime`, `settings-service`, `simple-resource-registry`, plus
> `bss/{products,subscriptions,rating}`), but none declare a `#[toolkit::gear]`
> gear yet, so they are outside the scope of this readiness inventory.

## Plugins — always embedded, inherit host's status

`static-authn-plugin`, `oidc-authn-plugin`, `static-authz-plugin`,
`tr-authz-plugin`, `static-tr-plugin`, `single-tenant-tr-plugin`,
`rg-tr-plugin`, `static-credstore-plugin`, `static-idp-plugin`,
`keycloak-idp-plugin`, `noop-usage-collector-plugin`,
`timescaledb-usage-collector-plugin`, `static-mini-chat-audit-plugin`,
`static-mini-chat-model-policy-plugin` — all hard-dep `types_registry` and
ride inside whatever process hosts their parent gear. Not independently
assessable.

## Key takeaways

1. **Single biggest lever:** give `resource-group` a REST contract →
   unblocks `authz-resolver` and `tenant-resolver` (3 of 4 trust-coupled-core
   gears) at once.
2. **Second biggest lever:** `types-registry` `#[toolkit::consumes]` wiring →
   unblocks `usage-collector`'s residual hard-dep, `bss-rate-provider`, and is
   a prerequisite for `mini-chat`.
3. **Easiest untouched wins:** `nodes-registry` (zero deps, zero auth) and
   `file-parser` (zero deps, just needs the authn-stack pattern already
   proven 6 times) — both far easier than `oagw`/`mini-chat`/`bss-ledger`.
4. **Structurally stuck, not just "not started":** `cluster`, `event-broker`
   hard-link their dependency's Rust library directly — that's an
   architecture change, not a mechanical `deps→consumes` swap.
5. **`account-management`'s credential migration** (`am.system` → real S2S)
   is the only blocker in the whole inventory that isn't solvable by "add a
   REST contract" — it needs an actual identity design decision.
