# OAGW e2e tests: correctness review

This document reviews all 81 tests in [`testing/e2e/suites/oagw`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw). It asks three questions of each test. Does it check what its name and scenario say? Would it fail if that behaviour broke? Is its result actually produced by OAGW? Tests are grouped by the [scenario](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios) folder they cover. A test with no scenario goes under the folder it most likely belongs to.

- **Baseline:** `main` at [`66da80e57`](https://github.com/constructorfabric/gears-rust/commit/66da80e5715e10c08c6fafd5e9832137389a3355). All links point at this commit. `make e2e-local SUITE=oagw` gives **81 passed**.
- **Evidence:** each finding is backed by one of three kinds of evidence:
  - a live probe: a modified copy of the test, run against the e2e server;
  - a verbatim code quote;
  - a code mutation: the OAGW code was changed, rebuilt and the test re-run ([Mutation results](#mutation-results), 38 runs).
- **Companion:** decision IDs such as `P-06`, `PLG-13` and `R-09` refer to [`OAGW_DOCS_VS_IMPL_CROSS_REFERENCE.md`](OAGW_DOCS_VS_IMPL_CROSS_REFERENCE.md).
- **Reply** per test ID, for example `RT-3: agree`.

## Index

- [Summary](#summary)
  - [The ten HIGH findings](#the-ten-high-findings)
  - [Results produced outside OAGW](#results-produced-outside-oagw)
  - [Tests a decided change will break or leave stale](#tests-a-decided-change-will-break-or-leave-stale)
  - [Product findings](#product-findings)
- [How to read an entry](#how-to-read-an-entry)
- [Common problems](#common-problems)
- [Dependencies outside OAGW](#dependencies-outside-oagw)
- [Findings by scenario area](#findings-by-scenario-area)
  - [management-api](#management-api)
    - [Upstreams](#upstreams): [MGMT-1](#mgmt-1-test_create_minimal_upstream_returns_201), [MGMT-2](#upstreams), [MGMT-3](#upstreams), [MGMT-4](#mgmt-4-test_update_upstream_alias_immutable), [MGMT-5](#upstreams), [MGMT-7](#mgmt-7-test_delete_upstream_cascades_routes), [MGMT-8](#mgmt-8-test_create_upstream_with_tags), [ERR-2](#err-2-test_disabled_upstream_returns_503_gateway)
    - [Routes](#routes): [MGMT-6](#mgmt-6-test_create_route_returns_201)
    - [Plugin management](#plugin-management): [XFORM-3](#xform-3-test_unknown_transform_does_not_block_pipeline)
    - [Type registration (no scenario; closest: the plugin catalog)](#type-registration-no-scenario-closest-the-plugin-catalog): [TYPES-1](#type-registration-no-scenario-closest-the-plugin-catalog), [TYPES-2](#type-registration-no-scenario-closest-the-plugin-catalog), [TYPES-3](#types-3-test_oagw_entity_count), [TYPES-4](#types-4-test_schemas_have_is_schema_true), [TYPES-5](#types-5-test_instances_have_is_schema_false), [TYPES-6](#types-6-test_gts_ids_have_valid_format)
    - [Management auth](#management-auth) (no tests)
  - [proxy-api](#proxy-api)
    - [Alias resolution](#alias-resolution): [ERR-1](#err-1-test_nonexistent_alias_returns_404_gateway)
    - [Authentication](#authentication): [APIKEY-1](#apikey-1-test_apikey_auth_injects_bearer_header), [OAUTH-1](#oauth-1-test_oauth2_client_cred_form_injects_bearer), [OAUTH-2](#oauth-2-test_oauth2_client_cred_basic_injects_bearer), [OAUTH-3](#oauth-3-test_oauth2_client_cred_missing_secret_returns_error)
    - [Authz](#authz): [AUTHZ-1](#authz), [AUTHZ-2](#authz-2-test_proxy_authz_forbidden_nil_tenant)
    - [Body validation](#body-validation): [BODY-2](#body-2-test_body_exceeding_limit_returns_413)
    - [Request transforms](#request-transforms): [RT-3](#rt-3-test_hop_by_hop_headers_stripped), [RT-4](#rt-4-test_host_header_replaced), [BODY-1](#body-1-test_invalid_content_length_returns_400), [XFORM-1](#request-transforms), [XFORM-2](#xform-2-test_request_id_transform_preserves_existing)
    - [Custom header routing](#custom-header-routing) (no tests)
  - [plugins](#plugins)
    - [Guards](#guards): [GUARD-1](#guard-1-test_required_headers_allows_when_present), [GUARD-2](#guards), [GUARD-3](#guard-3-test_required_headers_allows_unconfigured), [GUARD-4](#guard-4-test_required_headers_case_insensitive), [CORS-1…7](#cors-tests-all-stale-under-plg-13)
    - [Transforms](#transforms) (no tests)
  - [protocols](#protocols)
    - [HTTP](#http): [RT-1](#rt-1-test_post_proxy_returns_upstream_response), [RT-2](#http), [ERR-3](#err-3-test_upstream_500_passthrough), [ERR-4](#err-4-test_upstream_timeout_returns_504_gateway)
    - [SSE](#sse): [SSE-1](#sse-1-test_sse_proxy_content_type_and_done), [SSE-2](#sse-2-test_sse_proxy_contains_json_chunks)
    - [WebSocket](#websocket): [WS-1](#websocket), [WS-2](#websocket), [WS-3](#ws-3-test_websocket_upgrade_rejected_by_upstream), [WS-4](#websocket), [WS-5](#ws-5-test_websocket_concurrent_bidirectional), [WS-6](#websocket), [WS-7](#ws-7-test_websocket_rapid_small_message_burst), [WS-8](#websocket)
    - [gRPC and WebTransport](#grpc-and-webtransport) (no tests)
  - [rate-limiting](#rate-limiting)
    - [Rate limits (18.1–18.6)](#rate-limits-181186): [RL-1](#rl-1-test_rate_limit_first_request_succeeds), [RL-2](#rl-2-test_rate_limit_exceeded_returns_429), [RL-3](#rl-3-test_token_bucket_burst_capacity_and_headers), [RL-4](#rl-4-test_response_headers_disabled), [RL-5](#rl-5-test_sliding_window_basic_enforcement), [RL-6](#rl-6-test_scope_global), [RL-7](#rl-7-test_scope_route_isolation), [RL-8](#rl-8-test_scope_tenant_isolation), [RL-9](#rl-9-test_weighted_cost), [RL-10](#rate-limits-181186)
    - [Budgets (18.7)](#budgets-187): [BUD-1](#budgets-187), [BUD-2](#budgets-187), [BUD-3](#bud-3-test_budget_overcommit_ratio_below_range), [BUD-4](#bud-4-test_budget_overcommit_ratio_above_range), [BUD-5](#budgets-187), [BUD-6](#bud-6-test_budget_allocated_valid_config_accepted), [BUD-7](#bud-7-test_allocated_child_within_budget), [BUD-8](#bud-8-test_allocated_two_children_within_budget), [BUD-9](#budgets-187), [BUD-10](#budgets-187), [BUD-11](#bud-11-test_allocated_overcommit_still_has_limit), [BUD-12](#budgets-187), [BUD-13](#bud-13-test_allocated_different_windows_normalized), [BUD-14](#budgets-187), [BUD-15](#bud-15-test_shared_budget_config_accepted), [BUD-16](#bud-16-test_unlimited_no_child_validation), [BUD-17](#bud-17-test_no_budget_defaults_unlimited)
- [Mutation results](#mutation-results)
- [Proposed fix order](#proposed-fix-order)

## Summary

**81 tests.** Each test is counted once, under its main verdict. The "Also as a secondary verdict" column counts tests whose main verdict is different.

| Verdict | Meaning | Tests | Also as a secondary verdict | IDs |
|---|---|---|---|---|
| `SOUND` | Tests what it claims, and fails when it breaks | **23** | — | [RT-2](#http), [AUTHZ-1](#authz), [TYPES-1](#type-registration-no-scenario-closest-the-plugin-catalog), [TYPES-2](#type-registration-no-scenario-closest-the-plugin-catalog), [GUARD-2](#guards), [XFORM-1](#request-transforms), [CORS-5](#cors-tests-all-stale-under-plg-13), [MGMT-2](#upstreams), [MGMT-3](#upstreams), [MGMT-5](#upstreams), [WS-1](#websocket), [WS-2](#websocket), [WS-4](#websocket), [WS-6](#websocket), [WS-8](#websocket), [RL-10](#rate-limits-181186), [BUD-1](#budgets-187), [BUD-2](#budgets-187), [BUD-5](#budgets-187), [BUD-9](#budgets-187), [BUD-10](#budgets-187), [BUD-12](#budgets-187), [BUD-14](#budgets-187) |
| `WEAK` | Right target; some broken implementations still pass | **37** | 4 | [RT-1](#rt-1-test_post_proxy_returns_upstream_response), [RT-4](#rt-4-test_host_header_replaced), [ERR-1](#err-1-test_nonexistent_alias_returns_404_gateway), [ERR-2](#err-2-test_disabled_upstream_returns_503_gateway), [ERR-3](#err-3-test_upstream_500_passthrough), [OAUTH-1](#oauth-1-test_oauth2_client_cred_form_injects_bearer), [OAUTH-2](#oauth-2-test_oauth2_client_cred_basic_injects_bearer), [AUTHZ-2](#authz-2-test_proxy_authz_forbidden_nil_tenant), [TYPES-3](#types-3-test_oagw_entity_count), [GUARD-1](#guard-1-test_required_headers_allows_when_present), [GUARD-3](#guard-3-test_required_headers_allows_unconfigured), [GUARD-4](#guard-4-test_required_headers_case_insensitive), [XFORM-2](#xform-2-test_request_id_transform_preserves_existing), [MGMT-1](#mgmt-1-test_create_minimal_upstream_returns_201), [MGMT-4](#mgmt-4-test_update_upstream_alias_immutable), [MGMT-6](#mgmt-6-test_create_route_returns_201), [MGMT-7](#mgmt-7-test_delete_upstream_cascades_routes), [MGMT-8](#mgmt-8-test_create_upstream_with_tags), [SSE-1](#sse-1-test_sse_proxy_content_type_and_done), [SSE-2](#sse-2-test_sse_proxy_contains_json_chunks), [WS-5](#ws-5-test_websocket_concurrent_bidirectional), [WS-7](#ws-7-test_websocket_rapid_small_message_burst), [RL-2](#rl-2-test_rate_limit_exceeded_returns_429), [RL-3](#rl-3-test_token_bucket_burst_capacity_and_headers), [RL-4](#rl-4-test_response_headers_disabled), [RL-5](#rl-5-test_sliding_window_basic_enforcement), [RL-6](#rl-6-test_scope_global), [RL-9](#rl-9-test_weighted_cost), [BUD-3](#bud-3-test_budget_overcommit_ratio_below_range), [BUD-4](#bud-4-test_budget_overcommit_ratio_above_range), [BUD-6](#bud-6-test_budget_allocated_valid_config_accepted), [BUD-7](#bud-7-test_allocated_child_within_budget), [BUD-8](#bud-8-test_allocated_two_children_within_budget), [BUD-11](#bud-11-test_allocated_overcommit_still_has_limit), [BUD-13](#bud-13-test_allocated_different_windows_normalized), [BUD-16](#bud-16-test_unlimited_no_child_validation), [BUD-17](#bud-17-test_no_budget_defaults_unlimited) |
| `VACUOUS` | Cannot fail, or fails only on unrelated breakage | **11** | 4 | [RT-3](#rt-3-test_hop_by_hop_headers_stripped), [ERR-4](#err-4-test_upstream_timeout_returns_504_gateway), [APIKEY-1](#apikey-1-test_apikey_auth_injects_bearer_header), [TYPES-4](#types-4-test_schemas_have_is_schema_true), [TYPES-5](#types-5-test_instances_have_is_schema_false), [TYPES-6](#types-6-test_gts_ids_have_valid_format), [WS-3](#ws-3-test_websocket_upgrade_rejected_by_upstream), [RL-1](#rl-1-test_rate_limit_first_request_succeeds), [RL-7](#rl-7-test_scope_route_isolation), [RL-8](#rl-8-test_scope_tenant_isolation), [BUD-15](#bud-15-test_shared_budget_config_accepted) |
| `MISATTRIBUTED` | The result comes from a component other than OAGW | **2** | 4 | [BODY-1](#body-1-test_invalid_content_length_returns_400), [BODY-2](#body-2-test_body_exceeding_limit_returns_413) |
| `WRONG-EXPECTATION` | Asserts behaviour a decision or scenario contradicts | **2** | 3 | [OAUTH-3](#oauth-3-test_oauth2_client_cred_missing_secret_returns_error), [XFORM-3](#xform-3-test_unknown_transform_does_not_block_pipeline) |
| `STALE` | Feature being removed (CORS, PLG-13) | **6** | — | [CORS-1](#cors-tests-all-stale-under-plg-13), [CORS-2](#cors-tests-all-stale-under-plg-13), [CORS-3](#cors-tests-all-stale-under-plg-13), [CORS-4](#cors-tests-all-stale-under-plg-13), [CORS-6](#cors-tests-all-stale-under-plg-13), [CORS-7](#cors-tests-all-stale-under-plg-13) |
| **Total** | | **81** | | |

| Severity | Tests | IDs |
|---|---|---|
| HIGH | **10** | [RT-3](#rt-3-test_hop_by_hop_headers_stripped), [ERR-4](#err-4-test_upstream_timeout_returns_504_gateway), [BODY-1](#body-1-test_invalid_content_length_returns_400), [APIKEY-1](#apikey-1-test_apikey_auth_injects_bearer_header), [OAUTH-2](#oauth-2-test_oauth2_client_cred_basic_injects_bearer), [GUARD-1](#guard-1-test_required_headers_allows_when_present), [WS-3](#ws-3-test_websocket_upgrade_rejected_by_upstream), [RL-5](#rl-5-test_sliding_window_basic_enforcement), [RL-6](#rl-6-test_scope_global), [RL-8](#rl-8-test_scope_tenant_isolation) |
| MED | **18** | [RT-1](#rt-1-test_post_proxy_returns_upstream_response), [RT-4](#rt-4-test_host_header_replaced), [BODY-2](#body-2-test_body_exceeding_limit_returns_413), [OAUTH-1](#oauth-1-test_oauth2_client_cred_form_injects_bearer), [OAUTH-3](#oauth-3-test_oauth2_client_cred_missing_secret_returns_error), [GUARD-4](#guard-4-test_required_headers_case_insensitive), [XFORM-2](#xform-2-test_request_id_transform_preserves_existing), [XFORM-3](#xform-3-test_unknown_transform_does_not_block_pipeline), [MGMT-6](#mgmt-6-test_create_route_returns_201), [MGMT-7](#mgmt-7-test_delete_upstream_cascades_routes), [SSE-1](#sse-1-test_sse_proxy_content_type_and_done), [SSE-2](#sse-2-test_sse_proxy_contains_json_chunks), [RL-3](#rl-3-test_token_bucket_burst_capacity_and_headers), [RL-4](#rl-4-test_response_headers_disabled), [RL-7](#rl-7-test_scope_route_isolation), [RL-9](#rl-9-test_weighted_cost), [BUD-15](#bud-15-test_shared_budget_config_accepted), [BUD-16](#bud-16-test_unlimited_no_child_validation) |
| LOW | **35** | — |
| none | **18** | — |

### The ten HIGH findings

Each of these passes when the behaviour it is named after is removed or broken.

| ID | Test | Why it can't catch the bug |
|---|---|---|
| [RT-3](#rt-3-test_hop_by_hop_headers_stripped) | `test_hop_by_hop_headers_stripped` | The default passthrough drops every client header anyway. It passes with stripping removed. |
| [ERR-4](#err-4-test_upstream_timeout_returns_504_gateway) | `test_upstream_timeout_returns_504_gateway` | Any status other than 504 passes (`else: pass`). It fails only through pytest-timeout. |
| [BODY-1](#body-1-test_invalid_content_length_returns_400) | `test_invalid_content_length_returns_400` | hyper answers the 400 before OAGW runs. |
| [APIKEY-1](#apikey-1-test_apikey_auth_injects_bearer_header) | `test_apikey_auth_injects_bearer_header` | A missing plugin gives 401, which the test turns into a skip. The injected value is not pinned. |
| [OAUTH-2](#oauth-2-test_oauth2_client_cred_basic_injects_bearer) | `test_oauth2_client_cred_basic_injects_bearer` | The mock can't tell Basic from Form client auth. |
| [GUARD-1](#guard-1-test_required_headers_allows_when_present) | `test_required_headers_allows_when_present` | It passes with no guard. `passthrough: all` hides PLG-05. |
| [WS-3](#ws-3-test_websocket_upgrade_rejected_by_upstream) | `test_websocket_upgrade_rejected_by_upstream` | `status != 101` inside `pytest.raises(InvalidStatus)` can never fail. |
| [RL-5](#rl-5-test_sliding_window_basic_enforcement) | `test_sliding_window_basic_enforcement` | It passes when a token bucket is used instead. |
| [RL-6](#rl-6-test_scope_global) | `test_scope_global` | The sharing comes from the shared-budget pool, not from `scope: global`. |
| [RL-8](#rl-8-test_scope_tenant_isolation) | `test_scope_tenant_isolation` | It passes when tenant keys are made global. |

The rate-limit tests as a whole cannot detect a limiter that ignores scope, never refills, uses the wrong algorithm or sends wrong header values. Each of those mutations leaves all 10 green.

### Results produced outside OAGW

| ID | Actually produced by |
|---|---|
| [BODY-1](#body-1-test_invalid_content_length_returns_400) | hyper, in the api-gateway listener (a bare `400`) |
| [BODY-2](#body-2-test_body_exceeding_limit_returns_413) | The api-gateway 64 MB body limit plus the toolkit error middleware (`about:blank` problem+json, no `x-oagw-error-source`) |
| [TYPES-4](#types-4-test_schemas_have_is_schema_true), [5](#types-5-test_instances_have_is_schema_false), [6](#types-6-test_gts_ids_have_valid_format) | types-registry |
| [XFORM-2](#xform-2-test_request_id_transform_preserves_existing) | `passthrough: all` plus api-gateway's request-id layer; the plugin isn't needed |
| [MGMT-7](#mgmt-7-test_delete_upstream_cascades_routes) | Alias resolution, not the route cascade |
| [AUTHZ-2](#authz-2-test_proxy_authz_forbidden_nil_tenant) | The deny decision is static-authz's nil-UUID special case |
| [CORS tests](#cors-tests-all-stale-under-plg-13) | The preflight "no auth" part is api-gateway's authn bypass; `ACAO: *` would come from api-gateway if its CORS layer were on |

### Tests a decided change will break or leave stale

| Decision | Tests | Effect |
|---|---|---|
| PLG-10, M-10 | [OAUTH-3](#oauth-3-test_oauth2_client_cred_missing_secret_returns_error) | Fails (500 → 401, or 400 at write). Rewrite. |
| PLG-03, M-11 | [XFORM-3](#xform-3-test_unknown_transform_does_not_block_pipeline) | Fails (200 → 503, or 400 at write). Replace. |
| PLG-13 | [CORS tests](#cors-tests-all-stale-under-plg-13) | Remove all but CORS-5. Add a write-time rejection test. |
| P-11 | [XFORM-1](#request-transforms) | Fails its UUID check. |
| R-17 | [RL-3](#rl-3-test_token_bucket_burst_capacity_and_headers), [RL-4](#rl-4-test_response_headers_disabled) | RL-3 fails. RL-4 becomes vacuous unless renamed in the same PR. |
| R-09, R-08, R-14 | [RL-6](#rl-6-test_scope_global), [RL-7](#rl-7-test_scope_route_isolation), [RL-9](#rl-9-test_weighted_cost), [BUD-15](#bud-15-test_shared_budget_config_accepted) | Unchanged either way. Add the replacement tests. |
| STR-06, P-06 (#4911), PLG-05 | [WS-3](#ws-3-test_websocket_upgrade_rejected_by_upstream), [BODY-2](#body-2-test_body_exceeding_limit_returns_413), [GUARD-1](#guard-1-test_required_headers_allows_when_present) | Keep passing either way. That is the problem. |

### Product findings

These are not test defects. They were found or confirmed while probing.

- **F-1 (new):** a PUT that changes a parent's `sustained.window` (for example minute → hour) with the same budget returns 200 and skips the budget re-check ([`mod.rs:281-291`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/domain/services/management/mod.rs#L281-L291)). Existing children are left over budget, and every later child is rejected.
- **F-2:** R-15 reproduces on the current e2e rig. An unchanged PUT of hierarchy-root returns 400, because its grandchild's rate is counted against tenant-a.
- **F-3:** a guard that requires `authorization` always rejects, because that header is stripped before guards run. This extends PLG-05.
- **F-4:** the api-gateway 413 is `about:blank` problem+json, not plain text. This corrects P-06's wording.
- **F-5:** a route `rate_limit` with only `cost` gets **422**, not 400. This refines R-14.
- **F-6:** a Content-Length larger than the body is reported as "exceeds maximum" / `PAYLOAD_TOO_LARGE` (P-05, live).
- **F-7:** R-08 and R-09 reproduce live. With an upstream-level `scope: route`, two routes share one bucket. `budget.total` of 1, 2 or 100 behaves the same.
- **F-8 (low):** a clean WebSocket close logs `WARN … dropped unexpectedly`.
- **F-9:** the nil-tenant token can list upstreams (`200 []`). Management makes no PEP call (M-01).

## How to read an entry

| Verdict | Meaning |
|---|---|
| `SOUND` | Tests what it claims, and fails when it breaks. Listed in a table per area, not as an entry. |
| `WEAK` | Right target, but some broken implementations still pass. |
| `VACUOUS` | Cannot fail, or fails only on unrelated gross breakage. |
| `MISATTRIBUTED` | The result comes from a component other than the one claimed. |
| `WRONG-EXPECTATION` | Asserts behaviour a scenario, ADR or decision contradicts, or maps to the wrong scenario. |
| `STALE` | The feature is being removed (CORS, PLG-13). |

- **Severity:**
  - **HIGH**: the test reports coverage it can't provide, for behaviour that matters.
  - **MED**: a plausible regression or decided change would go unnoticed.
  - **LOW**: a narrow gap or hardening.
- **Proof labels:** *Verified (mutation / probe / code)* or *Reasoned*.
- **ID prefixes** are the test file:

  | Prefix | File |
  |---|---|
  | `RT` | `test_proxy_round_trip.py` |
  | `ERR` | `test_error_handling.py` |
  | `BODY` | `test_body_validation.py` |
  | `APIKEY` | `test_auth_injection.py` |
  | `OAUTH` | `test_oauth2_auth.py` |
  | `AUTHZ` | `test_authz.py` |
  | `TYPES` | `test_oagw_types_registered.py` |
  | `GUARD` | `test_guard_plugins.py` |
  | `XFORM` | `test_transform_plugins.py` |
  | `CORS` | `test_cors.py` |
  | `MGMT` | `test_management_lifecycle.py` |
  | `SSE` | `test_sse_streaming.py` |
  | `WS` | `test_websocket.py` |
  | `RL` | `test_rate_limiting.py` |
  | `BUD` | `test_budget_allocation.py` |

## Common problems

Fixing a pattern fixes several tests at once.

| Pattern | Tests |
|---|---|
| **Escape hatches:** the test skips or passes on exactly the status a broken feature returns. The conftest credstore provisioning also only prints a warning when it fails. | APIKEY-1, OAUTH-1, OAUTH-2, AUTHZ-2 (the 401 skip), ERR-4 |
| **The passthrough confound, both ways.** Without `headers`, client headers are dropped, so absence checks pass by default. With `passthrough: all`, presence checks pass without the plugin. | RT-3, RT-4, GUARD-1, GUARD-4, XFORM-2 |
| **Status and source only.** The canonical `type` or `reason` that identifies the cause is not asserted. | ERR-1, ERR-2, GUARD-2, OAUTH-3, MGMT-4, RL-2, CORS-4, CORS-6 |
| **Shape checked, value not pinned.** | APIKEY-1, OAUTH-1, OAUTH-2, RT-4, RL-3, RL-9 |
| **Create-response echo only.** The in-memory repos return their input. | MGMT-1, MGMT-6, MGMT-8, BUD-6 |
| **The defining property is not exercised:** algorithm, scope, boundary, streaming, concurrency. | RL-5 to RL-8, SSE-1, WS-5, WS-7, BUD-3, BUD-7, BUD-13, BUD-16 |
| **Vacuous on empty.** | TYPES-3 to TYPES-6, RL-1 |
| **The mock can't observe the property:** `/oauth2/token` ignores client authentication, `/v1/chat/completions` ignores the request body, the WS echo drops fragments and doesn't record handshake headers, SSE sends only `data:` lines, and `/echo-401-once` is unused. | OAUTH-2, RT-1, WS-3, and gaps 9.5, 13.x, 14.2, 14.7 |
| **Cleanup is not in `finally`.** Failures leak upstreams into tenant-a, which also feeds MGMT-3's first-page flake. | most files |

## Dependencies outside OAGW

| Component | What the suite relies on it for | Risk |
|---|---|---|
| api-gateway: hyper | Parsing. A bad `Content-Length` gets a bare 400. Header names are lowercased. Any `Host` is forwarded. | Produces BODY-1's result. |
| api-gateway body limit (64 MB) + error middleware | The 413 for large bodies, before authn. | Produces BODY-2's result. OAGW's 100 MB cap is unreachable. |
| api-gateway authn | Token → 401. CORS preflights skip authn. | The 401 is skipped in AUTHZ-2 and accepted in WS-3. |
| api-gateway request-id layer | Keeps or generates `x-request-id` on every request and response. | Produces XFORM-2's value. Decides XFORM-1 after P-11. |
| api-gateway CORS layer (off) | Nothing, as long as it stays off. | If on, CORS-1, 2, 3 and 5 fail and CORS-7 passes without OAGW. |
| api-gateway 30 s timeout | A 504 without `x-oagw-error-source`. | A latent mask for ERR-4. |
| static-authn-plugin | Tokens → tenants: tenant-a, the hierarchy tokens (root, l1a, l1b), the nil-tenant token. | Drives RL-6, RL-8 and BUD-7 to BUD-17. A missing token makes AUTHZ-2 skip. |
| static-authz-plugin | Allows every tenant except the nil UUID. Allows `override_rate`. | Makes the AUTHZ-2 decision. There is no permission model, so 5.1 is untestable. |
| static-tr-plugin | The tenant tree. Unknown tenants → 404. | An empty chain would let positive budget tests pass. `tenant-b` is unusable for 5.2. |
| credstore | The conftest provisions three secrets. | A provisioning failure makes the auth tests skip. |
| types-registry | Stores OAGW's 21 GTS entities. Computes `is_schema`. | Produces the results of TYPES-4 to TYPES-6. |
| Mock upstream | Every upstream response. | See the mock pattern in [Common problems](#common-problems). |
| e2e config, pytest.ini | `proxy_timeout_secs: 2`, plain HTTP allowed, SSRF off. pytest `timeout = 10`. | pytest-timeout is ERR-4's only failure path. HTTPS-only and SSRF are never tested. |

## Findings by scenario area

For each area there are entries for the tests with problems, a table of the sound tests, and the scenarios with no useful test.

No test maps primarily to [`flows/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/flows). Those files describe sequences that the other areas' tests exercise; XFORM-1 also cites `flows/proxy-operations.md`.

## management-api

### Upstreams

Scenarios: [`management-api/upstreams/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/upstreams)

#### MGMT-1: `test_create_minimal_upstream_returns_201`

**`WEAK`** · **LOW** · asserts [`test_management_lifecycle.py:L20-L23`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_management_lifecycle.py#L20-L23) · scenario [2.1 create minimal HTTP upstream](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/upstreams/positive-2.1-create-minimal-http-upstream.md)

- **Problem:** The test claims a minimal create returns 201 with `enabled=true` and the alias, but it only checks the create response, which the in-memory repo echoes from the request. The body is not minimal: the helper sends `enabled: True` and an explicit alias, so neither the default `enabled` nor alias derivation (the core of 2.1) is tested, and 201 is never asserted. An upstream that is never stored still passes.
- **Proof:** Verified (mutation) [MG-M3](#mutation-results): with the upstream never stored, this test still passes (`test_get_upstream_by_id` fails).
- **Outside OAGW:** —
- **Fix:** Send only `server` + `protocol` with host `localhost`, assert `201`, `enabled is True` and `alias == "localhost:19876"`, then GET by id. Clean up in `finally`.

#### MGMT-4: `test_update_upstream_alias_immutable`

**`WEAK`** · **LOW** · asserts [`test_management_lifecycle.py:L94-L108`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_management_lifecycle.py#L94-L108) · scenario: none ([ADR-0010:206-209](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/docs/ADR/0010-resource-identification.md#L206-L209) "The alias is immutable once set")

- **Problem:** The test claims a PUT that changes the alias gets 400 and the alias is kept. The rule is real ([mod.rs:218-230](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/domain/services/management/mod.rs#L218-L230)), but the test has no control PUT and does not pin the error, so an update that returns 400 for any reason passes. The alias-plus-endpoint-change path ([alias.rs:245](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/domain/services/management/alias.rs#L245)) is not covered.
- **Proof:** Verified (mutation) [MG-M5](#mutation-results): with every PUT returning 400, this test still passes and the proposed fixed test fails.
- **Outside OAGW:** —
- **Fix:** Add a same-alias PUT that must return 200, and assert the 400 is problem+json with `"alias cannot be changed"` in `detail`. Then GET and check the alias is unchanged. Add a scenario file for the rule.
  ```python
  ok = await update_upstream_raw(c, base, h, uid, mock, alias=alias, tags=["v2"])
  assert ok.status_code == 200
  bad = await update_upstream_raw(c, base, h, uid, mock, alias=unique_alias("x"))
  assert bad.status_code == 400 and "alias cannot be changed" in bad.json()["detail"]
  ```
- **Related:** M-05, ADR-0010

#### MGMT-7: `test_delete_upstream_cascades_routes`

**`WEAK`** (+ `MISATTRIBUTED`) · **MED** · asserts [`test_management_lifecycle.py:L178-L187`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_management_lifecycle.py#L178-L187) · scenario [2.7 delete upstream cascades routes](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/upstreams/positive-2.7-delete-upstream-cascades-routes.md)

- **Problem:** The test claims the delete cascades to routes, but it discards the route id and only checks a proxy 404 with ESrc gateway. That 404 comes from alias resolution (`upstream not found`), which is the same answer as for an alias that never existed. Orphan route rows would pass.
- **Proof:** Verified (mutation) [MG-M1](#mutation-results): with the route cascade removed, this test still passes; the proposed fixed test fails because the route is still returned by `GET /routes/{id}`.
- **Outside OAGW:** —
- **Fix:** Capture `rid`. After the delete, assert `GET /routes/{rid}` gives 404 problem+json and `GET /routes?upstream_id=<uid>` gives `[]`. The filter is `?upstream_id=`, not the scenario's `$filter`.
- **Related:** M-14, M-07

#### MGMT-8: `test_create_upstream_with_tags`

**`WEAK`** · **LOW** · asserts [`test_management_lifecycle.py:L206-L207`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_management_lifecycle.py#L206-L207) · scenario [2.9 tags support discovery and filtering](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/upstreams/positive-2.9-tags-support-discovery-filtering.md)

- **Problem:** The test claims created tags appear in the response, but it only checks the create response and never GETs back. Tags dropped when stored but kept in the response would pass. The filtering half of 2.9 cannot be tested: there is no tag filter, and `$filter`, `?tags=` and `?tag=` all return untagged upstreams.
- **Proof:** Verified (mutation) [MG-M4](#mutation-results): with tags dropped when stored but kept in the create response, this test still passes and the proposed fixed test fails.
- **Outside OAGW:** —
- **Fix:** GET the upstream by id and assert `sorted(tags) == ["llm", "openai"]`, then check the same on the list entry. Add a filter test only once a filter exists; after M-07, assert that `?$filter=` gives 400.
- **Related:** M-07, ADR-0010

#### ERR-2: `test_disabled_upstream_returns_503_gateway`

**`WEAK`** · **LOW** · asserts [`test_error_handling.py:L49-L50`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_error_handling.py#L49-L50) · scenario [2.6 disable upstream blocks proxy traffic](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/upstreams/negative-2.6-disable-upstream-blocks-proxy-traffic.md)

- **Problem:** The test catches an ignored `enabled` flag only because it creates no route; an ignored flag then gives a route-not-found 404. `503` plus `gateway` is also what an unreachable endpoint returns, and leaving out the route also pins the order of checks. Only disabled-at-create is covered: no PUT toggle, no descendant case, no Problem Details check.
- **Proof:** Verified (mutation). [PX-M6](#mutation-results) is a control: deleting the `!upstream.enabled` branch makes the test fail with 404. Separately, the test's assertions pass on an enabled upstream with an unreachable endpoint (`503`, `gateway`, `"retry_after_seconds":5`).
- **Outside OAGW:** —
- **Fix:** Follow the scenario: create a route, get a 200, disable with `PUT enabled=false`, then assert `503`, `problem+json` and `Retry-After: 30`. The last one separates it from the transient `5`.
- **Related:** M-12

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| MGMT-2 | `test_get_upstream_by_id` | GET reads the store; upstream not stored → 404 → fails (MG-M3). | Compare `server.endpoints` and `enabled` with the create response. |
| MGMT-3 | `test_list_upstreams_includes_created` | Missing from the list fails; LOW flake once tenant-a exceeds 50 upstreams. | `limit=100` plus paging; add a disabled upstream (2.12). |
| MGMT-5 | `test_delete_upstream_returns_204` | 204, then GET must be 404; a no-op delete fails. | Pin problem+json `not_found` type. |

**Not covered**

- 2.2 alias derivation (`host:port`) and 2.5 positive update (mutable field → GET reflects): no e2e test.
- 2.8 alias uniqueness 409 and 2.12 disabled in list: no e2e test; 2.9 tag filter (not implemented).
- 2.6 disabled upstream: PUT-toggle and descendant variants are not covered (M-12).

### Routes

Scenarios: [`management-api/routes/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/routes)

#### MGMT-6: `test_create_route_returns_201`

**`WEAK`** · **MED** · asserts [`test_management_lifecycle.py:L156-L157`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_management_lifecycle.py#L156-L157) · scenario [3.1 create HTTP route (method + path)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/routes/positive-3.1-create-http-route-method-path.md)

- **Problem:** The test claims POST /routes returns 201 with a GTS id, but it checks only the presence and prefix of the `id` in the create response. It never GETs the route, never proxies through it, and never checks `upstream_id` or `match`. A route repo that drops writes passes.
- **Proof:** Verified (mutation) [MG-M2](#mutation-results): with the route never stored, this test still passes.
- **Outside OAGW:** —
- **Fix:** Assert `upstream_id` and `match.http` in the response, then assert `GET /oagw/v1/routes/{id}` returns 200. Also send one proxy call through the route and expect 200 with `x-oagw-error-source: upstream`.

**Not covered**

- 3.10 routes list includes disabled routes, and 3.2–3.9 route matching, priority and disable/re-enable: no e2e test.

### Plugin management

Scenarios: [`management-api/plugins/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/plugins)

#### XFORM-3: `test_unknown_transform_does_not_block_pipeline`

**`WRONG-EXPECTATION`** + **`VACUOUS`** · **MED** · asserts [`test_transform_plugins.py:L157-L160`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_transform_plugins.py#L157-L160) · scenario [4.5-B plugin not found](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/plugins/positive-4.5-plugin-resolution-supports-builtin-named-ids-custom-uuid.md#L19-L23) (contradicted)

- **Problem:** It asserts 200 for an unknown transform, but PLG-03 decides 503 `plugin_not_found`. It is also vacuous: an upstream with no plugins, or with a `plugin_ref` matching no schema, gets the same 200. The bogus ref is stored on write (M-11). An unknown guard gets 500 (probe G7).
- **Proof:** Verified (probe):
  ```
  --- T3a unknown transform (as test): status=200 ... esrc=upstream
  --- T3b no plugins at all: status=200 ... esrc=upstream
  ```
  Verified (code): [service.rs:1361-1368](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/infra/proxy/service.rs#L1361-L1368) `"transform plugin resolution failed, continuing"` … `continue;`
- **Outside OAGW:** —
- **Fix:** Replace it, xfail-strict until the fixes land. At write time (M-11), assert 400 problem+json for the unknown `plugin_ref`. At proxy time (PLG-03), assert 503 with `x-oagw-error-source: gateway` and a `type` containing `plugin.not_found`.
- **Related:** PLG-03, M-11

**Not covered**

- 4.5-B: unknown **guard** gives 500 today (PLG-03 decides 503); no test.

### Type registration (no scenario; closest: the plugin catalog)

These tests check that OAGW registers its GTS schemas and built-in plugin ids in types-registry at startup (F0001 GTS provisioning).

#### TYPES-3: `test_oagw_entity_count`

**`WEAK`** · **LOW** · asserts [`test_oagw_types_registered.py:L46-L50`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oagw_types_registered.py#L46-L50) · scenario: none (F0001 GTS provisioning)

- **Problem:** The docstring says "at least 21", but the test asserts `len(registered_ids & set(ALL_OAGW_GTS_IDS)) == 21`. That intersection can never exceed 21, so the check is exactly TYPES-1 and TYPES-2 combined. It cannot detect extra or unexpected `oagw` entities.
- **Proof:** Verified (mutation): [AU-M6](#mutation-results) removes OAGW's registration, and the test fails, as TYPES-1 and TYPES-2 do. Verified (code): the set intersection at L46-47.
- **Outside OAGW:** —
- **Fix:** Assert `registered_ids == set(ALL_OAGW_GTS_IDS)`, or delete the test as redundant.

#### TYPES-4: `test_schemas_have_is_schema_true`

**`VACUOUS`** (+ `MISATTRIBUTED`) · **LOW** · asserts [`test_oagw_types_registered.py:L60-L64`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oagw_types_registered.py#L60-L64) · scenario: none (F0001 GTS provisioning)

- **Problem:** The test asserts `is_schema` only inside a loop guarded by `if`, so an empty list, or a list with every schema missing, passes. `is_schema` is computed by types-registry from the trailing `~` of the id, so no OAGW bug can make this test fail.
- **Proof:** Verified (mutation): [AU-M6](#mutation-results) removes OAGW's registration, and the test still passes. Verified (code): [in_memory_repo.rs:55](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/types-registry/types-registry/src/infra/storage/in_memory_repo.rs#L55), `let is_schema = gts_id.ends_with('~');`.
- **Outside OAGW:** types-registry produces the asserted value.
- **Fix:** Delete the test, or fold it into TYPES-1 as `assert set(OAGW_SCHEMAS) <= by_id.keys()` followed by a check on each `is_schema`.
- **Related:** SCH-01

#### TYPES-5: `test_instances_have_is_schema_false`

**`VACUOUS`** (+ `MISATTRIBUTED`) · **LOW** · asserts [`test_oagw_types_registered.py:L74-L78`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oagw_types_registered.py#L74-L78) · scenario: none (F0001 GTS provisioning)

- **Problem:** This is TYPES-4 for instances. The guarded loop passes on an empty list, and the value is `!ends_with('~')` computed by types-registry.
- **Proof:** Verified (mutation): [AU-M6](#mutation-results) removes OAGW's registration, and the test still passes.
- **Outside OAGW:** types-registry produces the asserted value.
- **Fix:** Delete the test, or fold it into TYPES-2 with a non-empty `by_id` check.

#### TYPES-6: `test_gts_ids_have_valid_format`

**`VACUOUS`** · **LOW** · asserts [`test_oagw_types_registered.py:L85-L115`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oagw_types_registered.py#L85-L115) · scenario: none (F0001 GTS provisioning)

- **Problem:** The test loops over the returned entities, so an empty list passes. The ids are compile-time constants that types-registry parses at registration, and OAGW aborts startup if any fail, so a malformed id can never reach the test. The "dynamic UUID instance" branch is dead: runtime upstreams and routes are not registered in types-registry.
- **Proof:** Verified (mutation): [AU-M6](#mutation-results) removes OAGW's registration, and the test still passes. Verified (code): [gear.rs:171-186](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/gear.rs#L171-L186) runs `anyhow::bail!("OAGW type registration failed …")`.
- **Outside OAGW:** The types-registry id parser enforces the format.
- **Fix:** Delete the test; `type_catalog.rs` unit tests already cover id shape.

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| TYPES-1 | `test_all_oagw_schemas_registered` | Fails when registration is removed (AU-M6); entities are in-memory, with no stale-DB confound. | Delete the unused `register_oagw_types` helper |
| TYPES-2 | `test_all_oagw_instances_registered` | Fails when registration is removed (AU-M6). | Note that 6 ids are catalog-only (PLG-01) |

### Management auth

Scenarios: [`management-api/auth/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/management-api/auth)

No test. Scenarios 1.1–1.3 are unexercised; the management API has no per-operation PEP check (M-01, F-9).

## proxy-api

### Alias resolution

Scenarios: [`proxy-api/alias-resolution/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/alias-resolution)

#### ERR-1: `test_nonexistent_alias_returns_404_gateway`

**`WEAK`** · **LOW** · asserts [`test_error_handling.py:L19-L28`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_error_handling.py#L19-L28) · scenario [6.4 alias not found returns stable 404](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/alias-resolution/negative-6.4-alias-not-found-returns-stable-404.md)

- **Problem:** The test checks a gateway 404 with some `type` and `status`, and accepts `application/json` as well as `problem+json`. Nothing ties the response to "upstream alias not found", so any OAGW 404 passes, including route-not-found for a known alias.
- **Proof:** Verified (probe). The test's assertions pass on a known alias with no route:
  ```
  404 x-oagw-error-source: gateway
  {"detail":"route not found: 00000000-...","context":{"resource_type":"gts.cf.core.oagw.route.v1~",...}}
  ```
- **Outside OAGW:** —
- **Fix:** Assert `content-type` starts with `application/problem+json`, `body["context"]["resource_type"] == UPSTREAM_SCHEMA`, and `"upstream" in body["detail"]`.
- **Related:** P-07

### Authentication

Scenarios: [`proxy-api/authentication/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authentication)

#### APIKEY-1: `test_apikey_auth_injects_bearer_header`

**`VACUOUS`** (+ `WEAK`) · **HIGH** · asserts [`test_auth_injection.py:L51-L67`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_auth_injection.py#L51-L67) · scenario [9.2 API key injection](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authentication/positive-9.2-api-key-injection.md)

- **Problem:** The test claims the apikey plugin injects `Bearer <resolved secret>`. It skips on a 401 or 500 from the proxy and on a 400 or 500 at create. A missing plugin gives 401 `AUTH_PLUGIN_NOT_FOUND` and an unprovisioned secret gives 400 `failed_precondition`, so both end in SKIP. The only assertion is `startswith("Bearer ")` plus non-empty, so a plugin that injects the wrong value still passes.
- **Proof:** Verified (mutation): [AU-M2](#mutation-results) removes the apikey plugin from the registry, and the test is SKIPPED with `Auth injection failed (cred_store may not have test secret): 401`. [AU-M1](#mutation-results) makes the plugin inject `Bearer WRONG`, and the test still passes.
- **Outside OAGW:** credstore and static-credstore-plugin. When the conftest provisioning fails, the result is a 400 or 500, which the test turns into a SKIP.
- **Fix:** Delete both skip blocks and make conftest provisioning fail the session. Pin `echoed.get("authorization") == "Bearer sk-test-e2e-fake-key"`.
- **Related:** PLG-10, M-10

#### OAUTH-1: `test_oauth2_client_cred_form_injects_bearer`

**`WEAK`** · **MED** · asserts [`test_oauth2_auth.py:L58-L75`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oauth2_auth.py#L58-L75) · scenario [9.5 OAuth2 client credentials](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authentication/positive-9.5-oauth2-client-credentials.md)

- **Problem:** The test claims the Form plugin fetches a token and injects it. It checks only that a non-empty Bearer header arrives upstream, and it skips on the 500 that unprovisioned credentials produce (OAuth2 refs are not checked at write time). The token cache is keyed by tenant, subject, method and config, not by upstream, so a rerun within 300 s never calls the token endpoint.
- **Proof:** Verified (mutation): [AU-M4](#mutation-results) makes the plugin inject `Bearer WRONG`, and the test still passes. Verified (probe): with the credential refs unprovisioned, the test is SKIPPED with a 500 `internal`.
- **Outside OAGW:** The mock `/oauth2/token` produces the token. It accepts any credentials and returns a constant, and credstore failures are converted to SKIP.
- **Fix:** Delete the 500-skips and pin `"Bearer mock-e2e-token-form"`, using the mock change in OAUTH-2. Add a per-test nonce to `scopes` so every run misses the cache.
- **Related:** M-10, PLG-10

#### OAUTH-2: `test_oauth2_client_cred_basic_injects_bearer`

**`WEAK`** · **HIGH** · asserts [`test_oauth2_auth.py:L123-L141`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oauth2_auth.py#L123-L141) · scenario [9.6 OAuth2 client credentials (basic-auth client)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authentication/positive-9.6-oauth2-client-credentials.md)

- **Problem:** Scenario 9.6 is defined by "Token request authenticates client via HTTP Basic (not form params)". The mock token endpoint checks only `grant_type`, so the Basic and Form plugins look identical to it. A Basic plugin wired as Form, or one that injects any token, still passes, and the test has the same 500-skip as OAUTH-1.
- **Proof:** Verified (mutation): [AU-M3](#mutation-results) builds the Basic plugin with `ClientAuthMethod::Form`, and the test still passes. [AU-M4](#mutation-results) injects `Bearer WRONG`, and it still passes. OAGW does send the two variants differently on the wire (probe):
  ```
  form  token req: 'authorization': None, 'body': '...&client_id=test-client-id&client_secret=test-client-secret'
  basic token req: 'authorization': 'Basic dGVzdC1jbGllbnQtaWQ6dGVzdC1jbGllbnQtc2VjcmV0', 'body': 'grant_type=client_credentials&scope=read+write'
  ```
- **Outside OAGW:** The mock `/oauth2/token` cannot observe how the client authenticates. `toolkit-auth` builds the request correctly.
- **Fix:** Have the mock branch on `Authorization: Basic` vs form `client_id`, check the credentials, and return a token per mode. Assert `"Bearer mock-e2e-token-basic"` here (Form asserts `…-form`).
  ```python
  if basic.startswith("Basic ") and "client_id" not in form: mode = "basic"
  elif not basic and "client_id" in form: mode = "form"
  else: return 401 invalid_client
  # check (client_id, secret) == ("test-client-id", "test-client-secret")
  token = f"mock-e2e-token-{mode}"
  ```
- **Related:** M-10, PLG-10

#### OAUTH-3: `test_oauth2_client_cred_missing_secret_returns_error`

**`WRONG-EXPECTATION`** (+ `WEAK`) · **MED** · asserts [`test_oauth2_auth.py:L189-L193`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_oauth2_auth.py#L189-L193) · scenario [9.7-B secret missing](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authentication/negative-9.7-secret-access-control-cred-store.md)

- **Problem:** The test asserts only `status == 500` for nonexistent OAuth2 refs. PLG-10 decides 401 for this case, and M-10 will reject these refs at write time with 400. Even against the current scenario it under-asserts: the wire `type` is `internal.v1`, not `secret.not_found`, and `context` is empty, so any other internal error also passes.
- **Proof:** Verified (probe):
  ```
  proxy 500 ct=application/problem+json esrc=gateway
  body: {"type":"gts://gts.cf.core.errors.err.v1~cf.core.err.internal.v1~","title":"Internal","status":500,...,"context":{}}
  ```
  Verified (code): [error.rs:224-230](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/api/rest/error.rs#L224-L230) maps `SecretNotFound` to `CanonicalError::internal(detail)`.
- **Outside OAGW:** credstore returning `None` is the trigger. A credstore outage gives 401 instead, and the test would fail with no hint that credstore is the cause.
- **Fix:** Split it into two tests. One asserts 400 `failed_precondition` at create (M-10). The other creates a secret, creates the upstream, deletes the secret, then proxies and asserts 401 `unauthenticated` with ESrc=gateway (PLG-10). Until PLG-10 lands, pin `type`/ESrc and mark the test `xfail(strict=True)`.
- **Related:** PLG-10, M-10

**Not covered**

- 9.6: Basic client auth is effectively uncovered until the mock can tell Basic from Form.
- 9.5: no test for token refresh on an upstream 401; `/echo-401-once` exists but is unused, and token caching is not asserted.
- 9.7-A: no test for credstore denying access (401); a denied secret currently looks the same as a missing one.
- 9.2: no test for the scenario's `X-Api-Key` header without a prefix, or for "secret not logged".
- 9.3 / 9.4: no tests for Basic or Bearer auth plugins (not implemented; catalog-only per PLG-01).
- 9.8 / 9.9 / 9.10: no e2e tests for hierarchical auth sharing, override permissions or cross-subject credential isolation.

### Authz

Scenarios: [`proxy-api/authz/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authz)

#### AUTHZ-2: `test_proxy_authz_forbidden_nil_tenant`

**`WEAK`** · **LOW** · asserts [`test_authz.py:L74-L91`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_authz.py#L74-L91) · scenario [5.1 proxy invoke permission required](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/authz/negative-5.1-proxy-invoke-permission-required.md) (different trigger)

- **Problem:** The test does check OAGW's PEP call and its 403 problem+json mapping. The deny *decision*, though, is static-authz-plugin's nil-UUID special case, not a missing proxy-invoke permission, so scenario 5.1 is not covered. The 401-skip hides authn config drift, and the test pins neither `context.reason` nor `resource_type`. The 403 comes before upstream resolution (a nonexistent alias gets the same 403), so the upstream setup is irrelevant.
- **Proof:** Verified (mutation): [AU-M5](#mutation-results) deletes OAGW's proxy PEP call, and the test fails with `Expected 403, got 404: … "tenant not found: 0…"`. So the test discriminates the PEP call, and its 200-skip cannot be reached on this rig. The live 403 body carries `"context":{"reason":"AUTHZ_DENIED","resource_type":"gts.cf.core.oagw.proxy.v1~"}`.
- **Outside OAGW:** static-authz-plugin produces the deny decision ([service.rs:74-80](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/authz-resolver/plugins/static-authz-plugin/src/domain/service.rs#L74-L80)). static-authn could mask a regression: if the nil-tenant token disappears, the test gets a 401 and SKIPs.
- **Fix:** Remove both skips and pin `context.reason == "AUTHZ_DENIED"` and `context.resource_type == "gts.cf.core.oagw.proxy.v1~"`. Also assert the identical 403 for a nonexistent alias.

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| AUTHZ-1 | `test_proxy_authz_allowed` | An over-denying PEP call gives 403 and fails it; a 200 comes only from upstream. | Assert `x-oagw-error-source == "upstream"` |

**Not covered**

- 5.1: no test for a token that lacks the proxy-invoke permission; static-authz has no permission model.
- 5.2: no cross-tenant test. `e2e-token-tenant-b` gets 404 `tenant not found` from tenant resolution, so use the sibling tenants hierarchy-l1a and l1b.

### Body validation

Scenarios: [`proxy-api/body-validation/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/body-validation)

#### BODY-2: `test_body_exceeding_limit_returns_413`

**`MISATTRIBUTED`** (+ `WRONG-EXPECTATION`) · **MED** · asserts [`test_body_validation.py:L116-L120`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_body_validation.py#L116-L120) · scenario [8.1 maximum body size limit enforced](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/body-validation/negative-8.1-maximum-body-size-limit-enforced.md)

- **Problem:** The 413 comes from the api-gateway `RequestBodyLimitLayer` (64 MB), wrapped by `canonical_error_middleware` as `about:blank` problem+json. It arrives with no auth token, so it is produced before OAGW runs. OAGW's 100 MB cap is unreachable in e2e and would answer 400 `out_of_range` on main. The docstring's "plain-text 413" and `failed_precondition` are both wrong.
- **Proof:** Verified (mutation). [PX-M7](#mutation-results): OAGW's precheck was deleted, and the test still passes. The probe got this with no token:
  ```
  HTTP/1.1 413 Payload Too Large / content-type: application/problem+json
  {"type":"about:blank","title":"Payload Too Large","status":413,...}   (no x-oagw-error-source)
  ```
- **Outside OAGW:** the api-gateway `RequestBodyLimitLayer` plus the toolkit `canonical_error_middleware` produce the whole result.
- **Fix:** Rename the test as an api-gateway test that asserts `about:blank` and no `x-oagw-error-source`. For OAGW, set `oagw.config.max_body_size_bytes: 1048576` in the e2e config and send `Content-Length: 2000000`. Assert `413`, `problem+json` and `x-oagw-error-source: gateway`, marked `xfail` until #4911 lands.
- **Related:** P-06 (its "plain-text 413" wording is also wrong)

**Not covered**

- 8.1 OAGW's own body cap: unreachable while `max_body_size_bytes` (100 MB) is above the gateway's `body_limit_bytes` (64 MB).

### Request transforms

Scenarios: [`proxy-api/request-transforms/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/request-transforms)

#### RT-3: `test_hop_by_hop_headers_stripped`

**`VACUOUS`** · **HIGH** · asserts [`test_proxy_round_trip.py:L112-L124`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_proxy_round_trip.py#L112-L124) · scenario [7.2 hop-by-hop headers stripped](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/request-transforms/positive-7.2-hop-hop-headers-stripped.md)

- **Problem:** The upstream is created without `headers`, so the default passthrough `None` drops every client header before `strip_hop_by_hop` runs. `proxy-authorization` is also removed by `STRIPPED_HEADERS`, and the bridge always rewrites `Connection` to `close`, so every assertion holds whether or not stripping works. There is no positive control showing that a non-hop header is forwarded.
- **Proof:** Verified (mutation). [PX-M1](#mutation-results): `strip_hop_by_hop` made a no-op, and the test still passes. The proposed fixed test fails with `AssertionError: keep-alive`.
- **Outside OAGW:** —
- **Fix:** Use `passthrough: all`, add an end-to-end control header, and add a header named in `Connection`. Drop the `proxy-authorization` and `connection` assertions, which can't fail.
  ```python
  upstream = await create_upstream(..., upstream_headers={"request": {"passthrough": "all"}})
  headers = {..., "connection": "keep-alive, x-conn-nominated", "x-conn-nominated": "x",
             "keep-alive": "timeout=5", "te": "trailers", "trailer": "X-Checksum", "x-e2e-control": "arrives"}
  assert echoed.get("x-e2e-control") == "arrives"
  for h in ("keep-alive", "te", "trailer", "x-conn-nominated"):
      assert h not in echoed, h
  ```
- **Related:** P-12

#### RT-4: `test_host_header_replaced`

**`WEAK`** · **MED** · asserts [`test_proxy_round_trip.py:L151-L158`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_proxy_round_trip.py#L151-L158) · scenario [7.3 Host header replaced by upstream host](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/request-transforms/positive-7.3-host-header-replaced-upstream-host.md)

- **Problem:** The test only asserts that the echoed Host `!= "localhost:8086"`. Under the default passthrough the client's Host never reaches the upstream anyway, so a missing or wrong Host passes. It never sends a hostile Host, which is the point of the scenario.
- **Proof:** Verified (mutation). [PX-M2](#mutation-results): the `set_host_header` call was deleted, and the test still passes. The proposed fixed test fails with `assert 'evil.example.com' == '127.0.0.1:19876'`.
- **Outside OAGW:** —
- **Fix:** Use `passthrough: all`, send `Host: evil.example.com`, and assert `echo["headers"]["host"] == urlparse(mock_upstream_url).netloc`.
- **Related:** P-12, P-18

#### BODY-1: `test_invalid_content_length_returns_400`

**`MISATTRIBUTED`** + **`VACUOUS`** · **HIGH** · asserts [`test_body_validation.py:L65-L68`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_body_validation.py#L65-L68) · scenario [7.4 well-known header validation (A)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/request-transforms/negative-7.4-well-known-header-validation-errors-400.md)

- **Problem:** The test is meant to check OAGW's Content-Length validation, but it only checks for a 400 status line. hyper in the api-gateway listener rejects `Content-Length: not-a-number` with a bare 400 before routing, authn or OAGW run, so OAGW's check is unreachable over REST.
- **Proof:** Verified (mutation). [PX-M7](#mutation-results): OAGW's Content-Length validation was deleted, and the test still passes. The probe got the same response with no token and with an unknown alias:
  ```
  HTTP/1.1 400 Bad Request / connection: close / content-length: 0
  ```
- **Outside OAGW:** hyper (the api-gateway HTTP server) produces the whole result.
- **Fix:** Rename the test as an HTTP-server test that asserts the bare-400 signature, or delete it. Cover OAGW's check in a Rust `proxy_handler` test, and add an e2e test for 7.4-B (mismatch), which does reach OAGW.
- **Related:** P-05

#### XFORM-2: `test_request_id_transform_preserves_existing`

**`WEAK`** + **`MISATTRIBUTED`** · **MED** · asserts [`test_transform_plugins.py:L110-L118`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_transform_plugins.py#L110-L118) · scenario [7.6 correlation headers (A)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/request-transforms/positive-7.6-request-correlation-headers-propagate-end-end.md)

- **Problem:** It claims the plugin preserves a client `x-request-id`. The preserved value comes from `passthrough: all`, and the result is the same with the plugin removed. Under the default passthrough the plugin replaces the client id with a new UUID (P-11); the test's config hides this, and dropping passthrough makes the test fail rather than fixing it.
- **Proof:** Verified (mutation): [GT-M3](#mutation-results) makes the plugin always inject, and the test **fails**, so it does catch overwriting. Verified (probe):
  ```
  --- T2b NO plugin + passthrough all: upstream x-request-id: 'e2e-trace-abc123'
  --- T2c plugin + DEFAULT passthrough (none): resp-x-request-id=e2e-trace-abc123
      upstream x-request-id: '13f8c835-3b72-42d0-a1fb-a6b8a00cceab'
  ```
- **Outside OAGW:** api-gateway `SetRequestIdLayer` keeps the client id; it produces the asserted value together with OAGW's passthrough. The response `x-request-id` is never OAGW's.
- **Fix:** Rename it to "must not clobber an existing id". Add a default-passthrough test, xfail-strict until P-11 lands, that asserts the upstream id equals the client id.
- **Related:** P-11, PLG-05, O-04

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| XFORM-1 | `test_request_id_transform_injects_header` | No plugin → upstream gets no id; `UUID_RE` also rejects api-gateway's nanoid. | After P-11, assert upstream id equals the response id. |

**Not covered**

- 7.6: no check that the upstream `x-request-id` equals the response `x-request-id`; they differ today (P-11).
- 7.4-B Content-Length mismatch: no e2e test; it reaches OAGW, and the probe shows the P-05 mislabel (`"request body exceeds maximum of 104857600 bytes"`, `PAYLOAD_TOO_LARGE`).
- 7.2 hop-by-hop: the scenario's own request is a WebSocket upgrade with `Transfer-Encoding`, which gets 400 today; the scenario needs rewriting (P-12).

### Custom header routing

Scenarios: [`proxy-api/custom-header-routing/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/proxy-api/custom-header-routing)

No test covers `X-OAGW-Target-Host` routing (P-03).

## plugins

### Guards

Scenarios: [`plugins/guards/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/plugins/guards)

#### GUARD-1: `test_required_headers_allows_when_present`

**`WEAK`** · **HIGH** · asserts [`test_guard_plugins.py:L65-L69`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_guard_plugins.py#L65-L69) · scenario: none (ADR-0017; closest [10.4 Starlark guard](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/plugins/guards/negative-10.4-custom-starlark-guard-rejects-based-headers-body.md))

- **Problem:** It claims a request that carries the required header passes the guard, but it checks only `200` and `"headers" in body`. Both also hold with no guard bound. It sets `passthrough: all`, the only config where PLG-05 can't show: under the default passthrough, the guard rejects a client that *sent* the header.
- **Proof:** Verified (mutation): [GT-M1](#mutation-results) makes guards read the client's headers (the PLG-05 fix), and the test still passes. Verified (probe):
  ```
  --- G1 ... WITHOUT plugin: status=200 ct=application/json esrc=upstream
  --- G2 guard + default passthrough, client SENDS x-correlation-id: status=400 ... "reason":"REQUIRED_HEADER_MISSING"
  ```
- **Outside OAGW:** —
- **Fix:** Use the default passthrough (xfail-strict until PLG-05 lands). Assert the header reached the upstream, and on the same upstream send a request without the header and assert 400 `REQUIRED_HEADER_MISSING`. Add a test that a guard requiring `authorization` passes when the client sends it (today it gets 400, F-3).
- **Related:** PLG-05, F-3

#### GUARD-3: `test_required_headers_allows_unconfigured`

**`WEAK`** · **LOW** · asserts [`test_guard_plugins.py:L140-L142`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_guard_plugins.py#L140-L142) · scenario: none ([required_headers_guard.rs:38-41](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/infra/plugin/required_headers_guard.rs#L38-L41) fail-open)

- **Problem:** It claims a guard with an empty config allows all requests. It fails only if the empty config makes the guard reject or error. A dropped or never-collected binding still gives the same 200.
- **Proof:** Verified (probe): `--- G5 ... WITHOUT plugin: status=200 ct=application/json esrc=upstream`
- **Outside OAGW:** —
- **Fix:** Assert that `GET /oagw/v1/upstreams/{id}` shows the binding. Then `PUT` `required_request_headers: "x-foo"` and assert 400, which proves the binding is live.

#### GUARD-4: `test_required_headers_case_insensitive`

**`WEAK`** · **MED** · asserts [`test_guard_plugins.py:L181-L183`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_guard_plugins.py#L181-L183) · scenario: none (ADR-0017)

- **Problem:** It claims matching is case-insensitive, but it is an allow-only test and passes with no guard. hyper lowercases inbound names, so only the config case (`X-Correlation-ID`) is exercised, and that is normalised twice. `passthrough: all` hides PLG-05, as in GUARD-1.
- **Proof:** Verified (mutation): [GT-M2](#mutation-results) removes both normalisations and the test **fails**; [GT-M2a](#mutation-results) removes only `.to_lowercase()` and it **still passes**; [GT-M1](#mutation-results) still passes.
- **Outside OAGW:** hyper lowercases header names, so the client's case never matters.
- **Fix:** Use the default passthrough and send `X-CORRELATION-ID` (expect 200). On the same upstream, send no header and expect 400 `REQUIRED_HEADER_MISSING`.
- **Related:** PLG-05

#### CORS tests (all STALE under PLG-13)

PLG-13 removes CORS from OAGW: `cors` will be rejected with 400 at write time, and preflights will no longer be answered by OAGW. Every test below exercises code being deleted, and each depends on api-gateway's CORS layer being off (`cors_enabled: false`, checked by the CFG-CORS run).

| ID | test | today | after PLG-13 |
|---|---|---|---|
| CORS-1 | `test_cors_preflight_fully_permissive` | Headers are OAGW's, but a nonexistent alias gets the same 204 (setup unused). Asserts `credentials: true` (PLG-15). "No auth" is api-gateway's preflight bypass. Fails if api-gateway CORS is on. | Delete; replace with "OPTIONS is proxied" once P-18 lands |
| CORS-2 | `test_cors_preflight_permissive_echoes_any_method` | Method is echoed before the alias or config is read, so the setup is unused. Fails if api-gateway CORS is on. | Delete |
| CORS-3 | `test_cors_actual_request_includes_headers` | Discriminates (no `cors` → no ACAO). Fails if api-gateway CORS is on (ACAO overwritten). | Delete |
| CORS-4 | `test_cors_actual_request_disallowed_origin_rejected` | Status-only 403, identical to an authz 403 except `context.reason`. Passes with api-gateway CORS on. | Delete; until then pin `CORS_ORIGIN_NOT_ALLOWED` |
| CORS-5 | `test_cors_disabled_no_headers` | SOUND: catches OAGW emitting CORS by default. Fails if api-gateway CORS is on. | Keep; rename to "OAGW emits no `access-control-*`" |
| CORS-6 | `test_cors_credentials_with_wildcard_rejected` | Status-only 400; another CORS 400 looks the same. Passes with api-gateway CORS on. | Keeps passing for the wrong reason; replace |
| CORS-7 | `test_cors_wildcard_origin` | Discriminates today. Passes with api-gateway CORS on even if OAGW's CORS is removed (misattributed). | Delete |

Replacement (xfail-strict until PLG-13 lands; route create/update take the same `cors`):
```python
@pytest.mark.parametrize("cors", [{"enabled": True, "allowed_origins": ["https://app.example.com"],
                                   "allowed_methods": ["GET"]}, {"enabled": False}])
async def test_cors_field_rejected_not_supported(cors, oagw_base_url, oagw_headers, mock_upstream_url, mock_upstream):
    alias = unique_alias("cors-rejected")
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await create_upstream_raw(client, oagw_base_url, oagw_headers, mock_upstream_url, alias=alias, cors=cors)
        assert resp.status_code == 400, resp.text
        assert resp.headers["content-type"].startswith("application/problem+json")
        assert any(v["field"] == "cors" for v in resp.json()["context"]["field_violations"])
        listed = (await client.get(f"{oagw_base_url}/oagw/v1/upstreams", headers=oagw_headers,
                                   params={"limit": 1000})).json()
        assert alias not in [u["alias"] for u in listed]   # nothing persisted
```

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| GUARD-2 | `test_required_headers_rejects_when_missing` | Guard removed → 200, so the 400 assertion fails. | Pin `field_violations[0].reason == "REQUIRED_HEADER_MISSING"` and problem+json. |

**Not covered**

- PLG-05: no allow-path guard test under the default passthrough. A guard requiring `authorization` rejects clients that send it (F-3).
- Response-phase `required_response_headers` guard (502, U-3): no e2e test.
- 10.1 timeout guard plugin (not implemented).
- 10.4 Starlark guard, 11.1–11.8 Starlark transforms (not implemented).
- PLG-13: route-level `cors` is accepted and ignored under `private`; the write-time rejection must cover routes as well as upstreams.

### Transforms

Scenarios: [`plugins/transforms/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/plugins/transforms)

No test: the Starlark transforms in 11.1–11.8 are not implemented. The request_id transform tests are under [Request transforms](#request-transforms) (scenario 7.6); the unknown-transform test is under [Plugin management](#plugin-management) (scenario 4.5).

## protocols

### HTTP

Scenarios: [`protocols/http/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/http)

#### RT-1: `test_post_proxy_returns_upstream_response`

**`WEAK`** · **MED** · asserts [`test_proxy_round_trip.py:L29-L32`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_proxy_round_trip.py#L29-L32) · scenario [12.1 plain HTTP passthrough](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/http/positive-12.1-plain-http-request-response-passthrough.md)

- **Problem:** The test claims a POST round trip, but it only checks for the keys `id` and `choices` in the mock's fixed response. The mock's `/v1/chat/completions` handler ignores the request body, so an OAGW that drops or truncates the body still passes. None of the 4 round-trip tests checks that the body reaches the upstream.
- **Proof:** Verified (mutation). [PX-M3](#mutation-results): OAGW sends `Bytes::new()` upstream instead of the request body, and the test still passes.
- **Outside OAGW:** mock `/v1/chat/completions` produces every asserted key.
- **Fix:** POST to `/echo` and assert the upstream received the exact body. Also assert `x-oagw-error-source: upstream`.
  ```python
  echo = resp.json()
  assert json.loads(echo["body"]) == payload
  assert echo["headers"]["content-type"] == "application/json"
  assert resp.headers["x-oagw-error-source"] == "upstream"
  ```
- **Related:** P-12, U-6

#### ERR-3: `test_upstream_500_passthrough`

**`WEAK`** · **LOW** · asserts [`test_error_handling.py:L75-L80`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_error_handling.py#L75-L80) · scenario [12.2 upstream error passthrough](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/http/negative-12.2-upstream-error-passthrough-esrc-upstream.md)

- **Problem:** Status, `x-oagw-error-source: upstream` and the presence of an `error` key are all checked. But the scenario's "Content-Type preserved" and "Body forwarded unchanged" are not, so an OAGW that rewrote `content-type` to `problem+json` and kept the body would pass. Only GET 500 is covered.
- **Proof:** Verified (probe). The upstream 500 arrives as:
  ```
  500 content-type: application/json x-oagw-error-source: upstream
  {"error": {"message": "Simulated error 500", "type": "server_error", "code": "error_500"}}
  ```
- **Outside OAGW:** mock `/error/{code}` produces the body.
- **Fix:** Assert `content-type == "application/json"` and body equality with the mock payload. Parametrise over 400/404/429/500/503 and add a POST variant.
- **Related:** U-6

#### ERR-4: `test_upstream_timeout_returns_504_gateway`

**`VACUOUS`** (+ `WRONG-EXPECTATION`) · **HIGH** · asserts [`test_error_handling.py:L105-L125`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_error_handling.py#L105-L125) · scenario: none (global `proxy_timeout_secs`; test docstring maps to 10.1 timeout guard, which is not implemented)

- **Problem:** The test skips on a client `ReadTimeout` or a 200, and accepts any other non-504 status with `else: pass`. On a 504 it checks the error source only if the header is present. It passes today because OAGW's 2 s timeout returns a gateway 504. The only way the "no timeout" case fails is the suite's pytest-timeout (10 s), and an api-gateway 30 s 504 without the header would be accepted.
- **Proof:** Verified (mutation). [PX-M4](#mutation-results): `RequestTimeout` mapped to 503, and the test still passes. [CFG-TIMEOUT](#mutation-results): with `proxy_timeout_secs: 60` the test fails only through pytest-timeout (`Timeout (>10.0s)`), and with `--timeout=0` it is SKIPPED.
- **Outside OAGW:** e2e `proxy_timeout_secs: 2`; pytest-timeout (the only thing that makes it fail); api-gateway 30 s timeout layer (a latent 504 without `x-oagw-error-source`).
- **Fix:** Remove every skip and `pass` branch. Assert the full gateway 504 and that it came from OAGW's 2 s timer, not the api-gateway's 30 s one.
  ```python
  t0 = time.monotonic(); resp = await client.get(url, headers=oagw_headers)
  assert resp.status_code == 504 and resp.headers["x-oagw-error-source"] == "gateway"
  assert resp.json()["type"].endswith("deadline_exceeded.v1~")
  assert 1.5 <= time.monotonic() - t0 < 5
  ```
- **Related:** P-12, P-16, PLG-01

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| RT-2 | `test_get_proxy_returns_upstream_response` | A 200 with a `data` list only comes from the mock's `GET /v1/models`. | Assert exact body, `content-type`, `x-oagw-error-source: upstream`. |

**Not covered**

- 12.1 request body forwarding: no test checks that the upstream receives the body.
- Global `proxy_timeout_secs` 504: no scenario exists; 10.1 per-route timeout guard (not implemented).
- 12.2 upstream error passthrough: only GET 500; no 4xx, POST, or Content-Type check (U-6).

### SSE

Scenarios: [`protocols/sse/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/sse)

#### SSE-1: `test_sse_proxy_content_type_and_done`

**`WEAK`** · **MED** · asserts [`test_sse_streaming.py:L32-L37`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_sse_streaming.py#L32-L37) · scenario [13.1 SSE stream forwarded without buffering](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/sse/positive-13.1-sse-stream-forwarded-buffering.md)

- **Problem:** The test claims `text/event-stream` and a closing `data: [DONE]`, but it reads the fully buffered `resp.text`, so incremental delivery (the point of 13.1) is not checked. Both asserted values come from the mock and are only forwarded. A proxy that buffers the whole stream passes.
- **Proof:** Verified (mutation) [MG-M6](#mutation-results): with the response fully buffered before sending, this test still passes; the proposed fixed test fails (`content-length` present). On main OAGW does stream (probe with `client.stream`):
  ```
  [oagw#0] status=200 ct=text/event-stream te=chunked cl=None headers_at=2.1ms chunks(ms,len,events)=[(2.1, 221, 1), (13.8, 200, 1), (26.0, 200, 1), (38.1, 202, 1), (50.6, 198, 2)]
  ```
- **Outside OAGW:** mock produces the content-type and `[DONE]`; api-gateway in the response path could also buffer.
- **Fix:** Use `client.stream`, split events on `\n\n` and record arrival times. Assert there is no `content-length`, the exact event sequence, and `arrivals[-1] - arrivals[0] >= 25ms`. The mock's 10 ms sleeps allow this, but raising them to about 200 ms gives a robust margin.
- **Related:** STR-14, STR-04

#### SSE-2: `test_sse_proxy_contains_json_chunks`

**`WEAK`** · **MED** · asserts [`test_sse_streaming.py:L64-L75`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_sse_streaming.py#L64-L75) · scenario [13.1 SSE stream forwarded without buffering](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/sse/positive-13.1-sse-stream-forwarded-buffering.md)

- **Problem:** The test claims every data line is JSON with `choices`, but it asserts only `len(data_lines) > 0` over the buffered body. Several broken behaviours still pass: buffering, truncation after the first event, dropped or reordered events, lost `\n\n` framing (`splitlines()` accepts `\n`), and STR-01 trailing Problem JSON (no `data: ` prefix, so the filter at L66-70 skips it).
- **Proof:** Verified (mutation) [MG-M6](#mutation-results): with the response fully buffered before sending, this test still passes.
- **Outside OAGW:** mock produces all event content.
- **Fix:** Assert the exact sequence: deltas `["Hello"," from"," mock"," server"]`, then `finish_reason == "stop"`, 5 JSON events and `[DONE]`, with no non-`data:` bytes. Merge with SSE-1 into one streaming test.
- **Related:** STR-14, STR-01

**Not covered**

- 13.2 client disconnect aborts upstream: no e2e test; the mock stream is too short (about 50 ms) to cut mid-stream.
- F0007:309 `event:`/`id:`/`retry:` preservation: no e2e test; the mock emits only `data:`.

### WebSocket

Scenarios: [`protocols/websocket/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/websocket)

#### WS-3: `test_websocket_upgrade_rejected_by_upstream`

**`VACUOUS`** · **HIGH** · asserts [`test_websocket.py:L123-L127`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_websocket.py#L123-L127) · scenario: none ([F0007:135 inst-ws-5a](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/docs/features/0007-cpt-cf-oagw-feature-streaming.md#L135), overridden by STR-06)

- **Problem:** The test claims an upgrade to a non-WS upstream path errors, but `status_code != 101` inside `pytest.raises(InvalidStatus)` is a tautology. Any rejection from any layer passes: a missing route, a missing alias, an api-gateway 401, or the upstream's own 404. The mock has no `GET /echo` ([mock_upstream.py:229](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/mock_upstream.py#L229)), so the refusal is its 404 fallback, which OAGW surfaces as 503.
- **Proof:** Verified (probe): every counterfactual satisfies the test.
  ```
  [test as-written ...] InvalidStatus 503 esrc=gateway ct=application/problem+json body=...service_unavailable...
  [counterfactual: no route for path] InvalidStatus 404 esrc=gateway ... "route not found: ..."
  [counterfactual: bad token] InvalidStatus 401 esrc=None ct=application/problem+json ...unauthenticated...
  ```
- **Outside OAGW:** api-gateway authn 401 (no ESrc) satisfies the test; the mock's 404 fallback is the upstream answer.
- **Fix:** Add a control connect to `/ws/echo` on the same alias that must return 101 with `x-oagw-error-source: upstream`. Then pin the rejection on `/v1/models`:
  ```python
  r = ei.value.response
  assert r.status_code == 503                               # main; 502 after #4911
  assert r.headers.get("x-oagw-error-source") == "gateway"
  assert r.headers.get("content-type","").startswith("application/problem+json")
  # after STR-06, on /status/403: assert r.status_code == 403 and esrc == "upstream"
  ```
- **Related:** STR-06, PR #4911

#### WS-5: `test_websocket_concurrent_bidirectional`

**`WEAK`** · **LOW** · asserts [`test_websocket.py:L201-L217`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_websocket.py#L201-L217) · scenario: none (closest [14.1 WebSocket upgrade proxied](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/websocket/positive-14.1-websocket-upgrade-proxied.md))

- **Problem:** The test claims "concurrent read/write pressure", but it sends 20 messages in a row, then reads 20. That is 230 wire bytes, under one 8 KiB relay read. It adds nothing over WS-1, which already catches a half-duplex relay.
- **Proof:** Verified (probe): `concurrent wire bytes 230`. The relay is [`copy_bidirectional`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/infra/proxy/websocket.rs#L380), which has an 8 KiB default buffer.
- **Outside OAGW:** —
- **Fix:** Send and receive in separate tasks via `asyncio.gather(sender(), receiver())`, with more than 1 MiB in flight (for example 2000 × 1 KiB frames).

#### WS-7: `test_websocket_rapid_small_message_burst`

**`WEAK`** · **LOW** · asserts [`test_websocket.py:L325-L332`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_websocket.py#L325-L332) · scenario: none (closest [14.1 WebSocket upgrade proxied](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/protocols/websocket/positive-14.1-websocket-upgrade-proxied.md))

- **Problem:** The test claims 150 rapid messages survive "sustained write pressure", but the total is 4880 wire bytes, still under one 8 KiB read. It applies no pressure and adds nothing over WS-1 plus WS-4.
- **Proof:** Verified (probe): `burst client->upstream wire bytes 4880`.
- **Outside OAGW:** —
- **Fix:** Push well past socket buffers (more than 4 MiB total), or make the client read slowly to force backpressure through the relay.

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| WS-1 | `test_websocket_echo_text` | Goes through OAGW (101 carries `x-oagw-error-source`); exact echo catches corruption. | Assert `x-oagw-error-source == "upstream"` and `close_code == 1000`. |
| WS-2 | `test_websocket_echo_binary` | Byte-exact 256-byte binary echo through the relay. | Merge into WS-6. |
| WS-4 | `test_websocket_echo_large_payload` | A 64 KiB frame spans many 8 KiB relay reads, compared byte-exact. | Add a raw-socket header/delay/payload split (14.6). |
| WS-6 | `test_websocket_mixed_text_binary_interleaved` | Exact echo per frame; low value, since OAGW relays bytes without parsing frames. | — |
| WS-8 | `test_websocket_utf8_multibyte_integrity` | Exact echo catches corruption; low value, since UTF-8 handling is the client library's. | — |

**Not covered**

- 14.7 fragmented message: impossible with this mock, which drops continuation frames (probe: `["frag-a-","frag-b-","frag-c"]` echoes `'frag-a-'`).
- 14.2 auth injected at handshake: untestable, because the mock never records or echoes handshake headers.
- 14.1 `Sec-WebSocket-Protocol` forwarding: untestable, because the mock never negotiates a subprotocol (probe: `negotiated None`).
- 14.3 rate limit on connect, 14.4 idle timeout (STR-02), 14.5 stalled caller, 14.6 deliberate split read: no e2e test.

### gRPC and WebTransport

No test. gRPC (15.x) and WebTransport (17.x) are not implemented; their configs are accepted (STR-09).

## rate-limiting

### Rate limits (18.1–18.6)

Scenarios: [`rate-limiting/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting)

#### RL-1: `test_rate_limit_first_request_succeeds`

**`VACUOUS`** · **LOW** · asserts [`test_rate_limiting.py:L36-L36`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L36) · scenario [18.1 token bucket sustained + burst (step 1)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.1-token-bucket-sustained-burst.md#L22)

- **Problem:** The test claims the first request within the limit succeeds, but it checks only `status == 200`. Nothing links that to the limiter, so it passes with no `rate_limit` configured and with a limiter that isn't wired in.
- **Proof:** Verified (mutation) [RL-M1…M5](#mutation-results): every rate-limit mutation still passes. Probe with `rate_limit` removed:
  ```
  [no_rate_limit] 200 {'content-type': 'application/json', 'x-oagw-error-source': 'upstream'}
  ```
- **Outside OAGW:** —
- **Fix:** Merge it into RL-2, or assert `x-ratelimit-limit == "1"` and `x-ratelimit-remaining == "0"` (`response_headers` defaults to true). Under R-17, check the IETF `RateLimit` header instead.
- **Related:** R-17

#### RL-2: `test_rate_limit_exceeded_returns_429`

**`WEAK`** · **LOW** · asserts [`test_rate_limiting.py:L70-L81`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L70-L81) · scenario [18.1 token bucket sustained + burst](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.1-token-bucket-sustained-burst.md#L32-L36)

- **Problem:** The asserts are 429, `x-oagw-error-source: gateway` and a `retry-after` header being present. That proves the source, since an upstream 429 carries `ESrc=upstream`. But `Content-Type: application/problem+json` (required by 18.1) is not checked, and `Retry-After` is checked only for presence, so `Retry-After: 0`, a garbage value, or a limiter that never refills still passes.
- **Proof:** Verified (mutation) [RL-M2](#mutation-results): token refill disabled, still passes. Live, OAGW's 429 and an upstream 429 look like this:
  ```
  [second] 429 {'content-type': 'application/problem+json', 'x-oagw-error-source': 'gateway', 'retry-after': '60', ...}
  [upstream-429] 429 {'content-type': 'application/json', 'x-oagw-error-source': 'upstream'}
  ```
- **Outside OAGW:** —
- **Fix:** Assert that `content-type` starts with `application/problem+json`, that `55 <= int(retry-after) <= 60`, and that `json()["status"] == 429`.
- **Related:** P-10

#### RL-3: `test_token_bucket_burst_capacity_and_headers`

**`WEAK`** · **MED** · asserts [`test_rate_limiting.py:L112-L135`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L112-L135) · scenario [18.1 token bucket sustained + burst](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.1-token-bucket-sustained-burst.md)

- **Problem:** The capacity is pinned (10 allowed, the 11th gets 429), but the `x-ratelimit-*` headers are checked for presence only, and only on the 10th response. Wrong `remaining`, `limit` or `reset` values pass. The 18.1 refill step is dropped (`5/hour`), and R-17's header rename will break L122-124.
- **Proof:** Verified (mutation) [RL-M4](#mutation-results): `x-ratelimit-remaining` sent as the limit value, still passes. [RL-M2](#mutation-results): refill disabled, still passes. Live values the test ignores:
  ```
  [req10] 200 {... 'x-ratelimit-limit': '10', 'x-ratelimit-remaining': '0', ...}
  [req11] 429 {'content-type': 'application/problem+json', ..., 'retry-after': '720', ...}
  ```
- **Outside OAGW:** —
- **Fix:** In the loop, assert `x-ratelimit-limit == "10"` and `x-ratelimit-remaining == str(9 - i)`. On the 429, assert `problem+json` and `retry-after == "720"`. Add a refill leg on a separate upstream at `5/second`: exhaust it, sleep 0.3 s, expect 200.
- **Related:** R-17, R-12

#### RL-4: `test_response_headers_disabled`

**`WEAK`** · **MED** · asserts [`test_rate_limiting.py:L170-L185`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L170-L185) · scenario [18.1.1 rate-limit response headers can be disabled](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.1.1-rate-limit-response-headers-can-be-disabled.md)

- **Problem:** The test checks that `x-ratelimit-*` is absent on the success and that `retry-after` is present on the 429. It does not check that `x-ratelimit-*` is absent on the 429, or the `ESrc`/`problem+json` that 18.1.1 requires. Once R-17 renames the headers, the absence checks become trivially true unless they are renamed in the same PR.
- **Proof:** Verified (mutation) [RL-M5](#mutation-results): deny path ignores `response_headers: false`, still passes.
- **Outside OAGW:** —
- **Fix:** On the 429, assert `not any(k.startswith("x-ratelimit-") for k in resp.headers)`, `ESrc == "gateway"` and `problem+json`. Add a `response_headers: true` positive control so the absence check is known to mean something.
- **Related:** R-17

#### RL-5: `test_sliding_window_basic_enforcement`

**`WEAK`** · **HIGH** · asserts [`test_rate_limiting.py:L214-L232`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L214-L232) · scenario [N18.2 sliding window strictness](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/negative-18.2-sliding-window-strictness.md)

- **Problem:** The test claims sliding-window enforcement but only checks "2 then 429", which a token bucket with capacity = rate also produces. The scenario's subject, no 2× burst at the boundary, is never exercised. The `Retry-After` value, which differs between the algorithms (60 vs 30), is not asserted.
- **Proof:** Verified (mutation) [RL-M3](#mutation-results): `sliding_window` configs run a token bucket, still passes. Verified (probe) discriminating design at 2/second with a 0.7 s wait, stable over 3 runs:
  ```
  [swtb sliding_window] [(200, None), (200, None), (429, '1')]
  [swtb token_bucket]   [(200, None), (200, None), (200, None)]
  ```
- **Outside OAGW:** —
- **Fix:** Replace the test with the 2/second design: two requests, wait 0.7 s, expect 429 with `Retry-After: 1`. For the minute window, also assert `59 <= int(retry-after) <= 60`.

#### RL-6: `test_scope_global`

**`WEAK`** · **HIGH** · asserts [`test_rate_limiting.py:L286-L308`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L286-L308) · scenario [18.3 rate-limit scope variants (`global`)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.3-rate-limit-scope-variants.md#L24)

- **Problem:** The test claims `scope: global` shares one bucket across tenants. The sharing actually comes from `budget: shared`, which swaps each child's binding id for the pool owner's id (`pool_owner_id`); without the budget, `global` does not share at all. Scopes `ip` and `route` pass the same way, and `total == capacity` hides that `budget.total` is ignored.
- **Proof:** Verified (mutation) [RL-M6](#mutation-results): `pool_owner_id` ignored, **fails** (control); [RL-M1](#mutation-results): tenant keys made global, still passes. Verified (probe), expecting `[200, 200, 429]`:
  ```
  [scope_ip] [200, 200, 429] PASS   [scope_route] [200, 200, 429] PASS
  [no_budget] [200, 200, 200] FAIL  [scope_tenant] [200, 200, 200] FAIL (R-09)
  [budget total=1] [200, 200, 429]  (same for total=2 and total=100)
  ```
- **Outside OAGW:** static-authn hierarchy tokens and static-tr-plugin tree (l1a/l1b under root): an empty ancestor chain would give 404 or no 429, which would be blamed on the limiter.
- **Fix:** Use one parent upstream with no child bindings; children call it directly. That gives `scope: global` → l1a 200, l1b 429 (verified `[200, 429, 429]`) and `scope: tenant` → `[200, 429, 200]` (verified). Add 18.7-B (`tenant` + shared, `total` 1 < capacity) as `xfail(strict=True)` until R-09 is fixed.
- **Related:** R-09, R-06

#### RL-7: `test_scope_route_isolation`

**`VACUOUS`** · **MED** · asserts [`test_rate_limiting.py:L334-L369`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L334-L369) · scenario [18.3 rate-limit scope variants (`route`)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.3-rate-limit-scope-variants.md#L28)

- **Problem:** The test claims `scope: route` isolates routes, but it puts the limit on each route, and route-level limits are keyed on `route.id` whatever the scope. All 5 scopes pass. The case where the scope matters, `scope: route` on an upstream-level limit, is broken (R-08) and untested.
- **Proof:** Verified (mutation) [RL-M1](#mutation-results): tenant keys made global, still passes. Verified (probe): `global`/`tenant`/`user`/`ip` all give `[200, 200, 429, 429]`. Upstream-level `scope: route` gives one shared bucket:
  ```
  [upstream-level scope=route] statuses=[200, 429]
  ```
- **Outside OAGW:** —
- **Fix:** Rename the test to `test_route_level_limits_are_per_route`. Add a test with `scope: route` on the upstream and two routes, expecting `/v1/models` 200, `/health` 200, `/v1/models` 429, marked `xfail(strict=True, reason="R-08")`.
- **Related:** R-08

#### RL-8: `test_scope_tenant_isolation`

**`VACUOUS`** · **HIGH** · asserts [`test_rate_limiting.py:L418-L439`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L418-L439) · scenario [18.3 rate-limit scope variants (`tenant`)](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.3-rate-limit-scope-variants.md#L25)

- **Problem:** The test claims tenant-scoped buckets, but each tenant resolves to its own child binding, so the bucket key differs per tenant whatever the scope. A single global bucket per upstream passes, and the key's tenant segment is never exercised.
- **Proof:** Verified (mutation) [RL-M1](#mutation-results): tenant keys made global, **still passes**. Verified (probe): scopes `global`/`route`/`ip` all give `[200, 429, 200]`. With no child bindings, tenant and global differ:
  ```
  [tenant-nochild scope=tenant] statuses=[200, 429, 200]
  [tenant-nochild scope=global] statuses=[200, 429, 429]
  ```
- **Outside OAGW:** static-authn hierarchy tokens and static-tr-plugin tree: inheritance of the parent's route and limit depends on them, and a failure there looks like a limiter bug.
- **Fix:** Drop the child bindings and add a paired `scope: global` control that expects l1b to get 429. Or cover `scope: user` inside one tenant with `e2e-token-tenant-a` against `e2e-token-tenant-a-reviewer`.
- **Related:** R-09

#### RL-9: `test_weighted_cost`

**`WEAK`** · **MED** · asserts [`test_rate_limiting.py:L470-L481`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_rate_limiting.py#L470-L481) · scenario [18.4 weighted cost per route](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.4-weighted-cost-per-route.md)

- **Problem:** The test puts `cost: 10` on the upstream with one route and asserts 200 then 429, so any cost from 6 to 10 passes. The scenario's subject, route A at cost 10 and route B at cost 1 drawing on one budget, is not tested. It can't be configured either: a route `rate_limit` with only `cost` is rejected with 422.
- **Proof:** Verified (probe), expecting `[200, 429]`:
  ```
  [cost=10] [200, 429] PASS  [cost=6] [200, 429] PASS  [cost=5] [200, 200] FAIL
  [route cost-only] 422 {"type":"about:blank","title":"Unprocessable Entity",...}
  ```
- **Outside OAGW:** the axum/toolkit JSON extractor produces R-14's 422 (`about:blank`), not OAGW validation.
- **Fix:** Pin the exact cost with capacity 10 and cost 4: expect `[200, 200, 429]` and `x-ratelimit-remaining` of `6` then `2`. After R-14, add the two-route test: A (cost 10) returns 200, then B (cost 1) returns 429.
- **Related:** R-14, R-12, R-17

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| RL-10 | `test_route_level_rate_limit` | Upstream has no limit, so the route bucket is the only 429 source. | Pin `Retry-After` value and `problem+json`. |

**Not covered**

- 18.3 `user`/`ip` scopes: no test. `e2e-token-tenant-a` and `-reviewer` give two subjects in one tenant; `ip` waits on R-07.
- 18.3 `route` scope on an upstream-level limit: broken live (`[200, 429]`, R-08), no test.
- 18.1 step 3 refill after wait: never exercised (every window is minute/hour). Works live at 1/second.
- N18.2 boundary strictness: not tested; the 2/second + 0.7 s design separates sliding window from token bucket.
- 18.4 per-route cost against one upstream budget: cannot be configured (422, R-14) (not implemented).
- N18.5-B/C `queue`/`degrade`: accepted but behave as `reject` (R-10) (not implemented).
- 18.6 min-merge under `enforce`: blocked by R-01; R-06 bucket reset on a merged limit is also untested.

### Budgets (18.7)

Scenarios: [`rate-limiting/`](https://github.com/constructorfabric/gears-rust/tree/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting)

#### BUD-3: `test_budget_overcommit_ratio_below_range`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L93-L94`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L93-L94) · scenario: none (ADR-0004 budget schema)

- **Problem:** Claims that a ratio below 1.0 is rejected. It only probes 0.5, far from the bound. A lower bound anywhere in (0.5, 1.0], or an exclusive 1.0, still passes.
- **Proof:** Verified (mutation). [BA-M6](#mutation-results): ratio range widened to `0.9..=2.4` → still passes.
- **Outside OAGW:** —
- **Fix:** Parametrize `0.99` → 400 and `1.0` → 201, and check that 201 echoes `overcommit_ratio == 1.0`.

#### BUD-4: `test_budget_overcommit_ratio_above_range`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L109-L110`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L109-L110) · scenario: none (ADR-0004 budget schema)

- **Problem:** Claims that a ratio above 2.0 is rejected. It only probes 2.5, so the inclusive 2.0 bound is not pinned and a widened range still passes. Separately, the ratio is range-checked for every mode (`unlimited` + 5.0 → 400), although the model says it applies only to `allocated` (F-10).
- **Proof:** Verified (mutation). [BA-M6](#mutation-results): ratio range widened to `0.9..=2.4` → still passes.
- **Outside OAGW:** —
- **Fix:** Parametrize `2.0` → 201 and `2.01` → 400.
- **Related:** F-10

#### BUD-6: `test_budget_allocated_valid_config_accepted`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L135-L139`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L135-L139) · scenario: none (ADR-0004 budget schema)

- **Problem:** Claims that a valid allocated config is accepted, but it checks only for a 2xx. The DTOs have no `deny_unknown_fields`, so a renamed or lost `overcommit_ratio` would be dropped silently and the create would still succeed.
- **Proof:** Verified (probe). The same body with `"overcommit": 9.9` was accepted and the field dropped:
  ```
  misspelled field overcommit: 201 ...
      echoed budget: {'mode': 'allocated', 'total': 100}
  ```
- **Outside OAGW:** —
- **Fix:** Assert `upstream["rate_limit"]["budget"] == {"mode": "allocated", "total": 1000, "overcommit_ratio": 1.5}`, and repeat the check via GET.

#### BUD-7: `test_allocated_child_within_budget`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L158-L171`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L158-L171) · scenario [18.7-A Allocated budget](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L3-L20)

- **Problem:** Claims that a child within budget is accepted, using 50 of 100 under an `inherit` parent (the scenario uses `enforce`, which R-02 blocks). The value is far from the boundary, so an off-by-one `>=` passes. It also passes if hierarchy resolution returns no ancestor.
- **Proof:** Verified (mutation). [BA-M1](#mutation-results) `>` → `>=` → still passes. [BA-M5](#mutation-results) child rate not normalised → fails, so it does catch over-rejection.
- **Outside OAGW:** tenant-resolver/static-tr: an empty ancestor chain skips validation, so a hierarchy bug would be masked.
- **Fix:** Test the exact boundary: child 100/min under `total: 100` → 201.
- **Related:** R-02

#### BUD-8: `test_allocated_two_children_within_budget`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L187-L207`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L187-L207) · scenario [18.7-A Allocated budget](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L3-L20)

- **Problem:** Claims that two children summing within budget are accepted, using 40 + 40 = 80 of 100. The `>=` off-by-one passes, and so does dropping sibling summation (40 alone fits); only BUD-9 catches the latter.
- **Proof:** Verified (mutation). [BA-M1](#mutation-results) `>` → `>=` → still passes. [BA-M5](#mutation-results) → fails.
- **Outside OAGW:** tenant-resolver/static-tr: an empty ancestor chain would mask a bug.
- **Fix:** Use 60 + 40 = 100 exactly (accepted today), then a third child at 1/min → 400 `"budget allocation exceeded"`.
- **Related:** R-02

#### BUD-11: `test_allocated_overcommit_still_has_limit`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L321-L324`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L321-L324) · scenario [18.7-A Allocated budget](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L3-L20)

- **Problem:** Claims that total × ratio is still a ceiling: 100 + 60 = 160 > 150 is rejected. With the ratio ignored (1.0), 160 > 100 is rejected too, so the test cannot tell whether the ratio was applied. It relies on BUD-10 for that. Checking at write time only, with no traffic, is correct for allocated mode.
- **Proof:** Verified (mutation). [BA-M2](#mutation-results) ratio forced to 1.0 → still passes.
- **Outside OAGW:** —
- **Fix:** Pin the computed ceiling: `assert "× 1.5 overcommit ratio (allowed: 2.50 req/s)" in resp.text`.

#### BUD-13: `test_allocated_different_windows_normalized`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L396-L399`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L396-L399) · scenario [18.7-A Allocated budget](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L3-L20)

- **Problem:** Claims that rates are normalised to req/s. The child window is `second`, so child-side normalisation is the identity. Only the budget side is exercised, and only in one direction, so "reject whenever the windows differ" would also pass.
- **Proof:** Verified (mutation). [BA-M5](#mutation-results) child rate not normalised → still passes (while BUD-7, 8, 9, 10, 11 and 12 fail).
- **Outside OAGW:** —
- **Fix:** Use windows that aren't the identity on both sides, in both directions: under parent 60/min `total: 60`, child `7200/hour` → 400 and child `3500/hour` → 201.

#### BUD-15: `test_shared_budget_config_accepted`

**`VACUOUS`** · **MED** · asserts [`test_budget_allocation.py:L452-L465`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L452-L465) · scenario [18.7-B Shared pool](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L22-L29)

- **Problem:** It is the only test for 18.7-B ("Multiple tenants share the same pool"), but it checks only that the parent and child creates return 2xx. The same body passes with the budget `unlimited` or absent. At runtime the pool is not shared and `budget.total` is ignored (R-09).
- **Proof:** Verified (probe). Shared `total: 100` with burst 2, scope tenant; each tenant got its own 2-token bucket:
  ```
  GET as l1b: 200 remaining=1 limit=2   <- after l1a used its 2
  GET as l1a: 429 remaining=0 limit=2
  ```
  [BA-M3](#mutation-results): the mode gate removed → fails. That is the only thing this test catches.
- **Outside OAGW:** —
- **Fix:** Replace it with a runtime test, marked `xfail(strict=True, reason="R-09")` until R-09 is fixed. Keep the default `scope` so that any sharing must come from the budget.
  ```python
  rl = _rl(100, sharing="inherit", budget={"mode": "shared", "total": 3}); rl["burst"] = {"capacity": 100}
  # parent under root; l1a + l1b bind with no rate_limit; route on parent
  codes = [(await client.get(f"{base}/oagw/v1/proxy/{alias}/v1/models", headers=h)).status_code
           for h in (l1a, l1b, l1a, l1b)]
  assert codes == [200, 200, 200, 429]
  ```
- **Related:** R-09

#### BUD-16: `test_unlimited_no_child_validation`

**`WEAK`** · **MED** · asserts [`test_budget_allocation.py:L486-L500`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L486-L500) · scenario [18.7-C Unlimited](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L31-L38)

- **Problem:** Claims that unlimited mode skips allocation validation. The parent has no `total`, and validation with no total returns `Ok`, so deleting the unlimited-mode check still passes.
- **Proof:** Verified (mutation). [BA-M3](#mutation-results): the mode gate removed → still passes. The code path is [`budget.rs:88-91`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/domain/services/management/budget.rs#L88-L91) `None => return Ok(())`.
- **Outside OAGW:** tenant-resolver/static-tr: an empty ancestor chain would mask a bug.
- **Fix:** Give the parent `budget={"mode": "unlimited", "total": 10}` (accepted today), so the 9999 child passes only if the mode check works. Add a control: the same parent with `mode: "allocated"` → child 400.

#### BUD-17: `test_no_budget_defaults_unlimited`

**`WEAK`** · **LOW** · asserts [`test_budget_allocation.py:L516-L528`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/testing/e2e/suites/oagw/test_budget_allocation.py#L516-L528) · scenario [18.7-C Unlimited](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/scenarios/rate-limiting/positive-18.7-budget-modes-behave-specified.md#L31-L38)

- **Problem:** Claims that a missing budget defaults to unlimited. With no budget object, `budget` stays `None` and there is nothing to default, so the serde default of `mode` is never exercised.
- **Proof:** Verified (mutation). [BA-M7](#mutation-results): default budget mode changed to `allocated` → still passes.
- **Outside OAGW:** tenant-resolver/static-tr: an empty ancestor chain would mask a bug.
- **Fix:** Add a parent with `budget={"total": 100}` and no mode. Assert that the echo's `mode == "unlimited"` and that child 9999 → 201.

**Sound tests**

| ID | Test | Why it discriminates | Optional hardening |
|---|---|---|---|
| BUD-1 | `test_budget_allocated_requires_total` | Pins OAGW detail; other defects give different text or 422 | add `total: 0` and the PUT path |
| BUD-2 | `test_budget_shared_requires_total` | Pins OAGW detail; dropping the `Shared` arm → 201 → fails | — |
| BUD-5 | `test_budget_unlimited_accepts_no_total` | Only the allocated/shared arm can reject it | assert echoed `mode: unlimited` |
| BUD-9 | `test_allocated_exceeded_rejected` | Requires sibling summation (50 alone fits); error text pinned | pin `children total 1.83 req/s` |
| BUD-10 | `test_allocated_overcommit_allows_excess` | Ratio forced to 1.0 fails it (BA-M2) | — |
| BUD-12 | `test_allocated_update_revalidates` | Removing update re-validation fails it (BA-M8) | add 50→90 self-exclusion case; GET after 400 |
| BUD-14 | `test_allocated_rejects_child_without_rate_limit` | Pins `rate_limit is required`; matches the R-02 decision | add `enforce`-parent variant after R-01 |

**Not covered**

- 18.7-B: no runtime shared-pool test. Tenants get separate buckets and `budget.total` is ignored (R-09); add the BUD-15 replacement as `xfail(strict)`.
- 18.7-A (F-1): changing a parent's `sustained.window` minute→hour with the same budget returns 200. This leaves existing children over budget and blocks all later ones ([`mod.rs:281-291`](https://github.com/constructorfabric/gears-rust/blob/66da80e5715e10c08c6fafd5e9832137389a3355/gears/system/oagw/oagw/src/domain/services/management/mod.rs#L281-L291)). No test.
- 18.7-A (R-15): no three-level test. It is reachable today via tenant-a → hierarchy-root → l1a: an unchanged PUT of root returns 400 `"children total 1.83 req/s exceeds ancestor budget 1.67 req/s"`. Add it as `xfail(strict)`.
- 18.7-A literal `enforce` parent: untested and impossible today, since a child is rejected both with and without `rate_limit` (R-02, blocked by R-01).
- 18.7-A: no exact-boundary test (sum == total is accepted today); the `>=` mutation BA-M1 passes all 17 tests.
- 18.7-A: no test that the parent can't lower its budget (`validate_descendants_within_budget`), and no test of self-exclusion on update (BA-M4 passes all 17).
- Budget config validation (ADR-0004 schema): the checks on update, and `total: 0` → `"budget.total must be at least 1"` are untested.

## Mutation results

Each run changed the baseline, rebuilt the server, ran the named tests and reverted. The server was a second instance on port 8087 with its own data directory. Paths are under `gears/system/oagw/oagw/src/`. "Passes" on a test that claims the behaviour is the finding; *control* rows confirm that a test does catch the change.

| ID | Change | Result |
|---|---|---|
| PX-M1 | `strip_hop_by_hop` is a no-op | RT-3 passes; the fixed test fails |
| PX-M2 | `set_host_header` deleted | RT-4 passes; the fixed test fails |
| PX-M3 | an empty body is forwarded upstream | RT-1 passes |
| PX-M4 | `RequestTimeout` → 503 | ERR-4 passes |
| PX-M6 | the disabled-upstream check is deleted (*control*) | ERR-2 fails (404) |
| PX-M7 | the Content-Length checks in `handlers/proxy.rs` are skipped | BODY-1 and BODY-2 pass |
| AU-M1 | apikey injects `WRONG` | APIKEY-1 passes |
| AU-M2 | apikey plugin not registered | APIKEY-1 is **skipped** |
| AU-M3 | the Basic OAuth2 plugin is built as Form | OAUTH-1, 2 and 3 pass |
| AU-M4 | OAuth2 injects `Bearer WRONG` | OAUTH-1 and 2 pass |
| AU-M5 | the proxy PEP call is deleted | AUTHZ-2 fails (404 tenant not found); AUTHZ-1 passes |
| AU-M6 | GTS entities not registered | TYPES-1, 2 and 3 fail; 4, 5 and 6 pass |
| GT-M1 | guards read the client's headers (the PLG-05 fix) | GUARD-1 to 4 pass |
| GT-M2 / M2a | both case normalisations removed / only one removed | GUARD-4 fails / passes |
| GT-M3 | request_id always injects a new id | XFORM-2 fails |
| MG-M1 | route cascade skipped | MGMT-7 passes; the fixed test fails |
| MG-M2 | route not stored | MGMT-6 passes |
| MG-M3 | upstream not stored | MGMT-1 passes; MGMT-2 fails |
| MG-M4 | tags dropped when stored | MGMT-8 passes; the fixed test fails |
| MG-M5 | every upstream PUT returns 400 | MGMT-4 passes; the fixed test fails |
| MG-M6 | the SSE response is fully buffered | SSE-1 and 2 pass; the fixed test fails |
| RL-M1 | tenant scope uses the global key | all 10 RL pass |
| RL-M2 | the token bucket never refills | all 10 RL pass |
| RL-M3 | sliding window runs as a token bucket | all 10 RL pass |
| RL-M4 | `x-ratelimit-remaining` is sent as the limit | all 10 RL pass |
| RL-M5 | the deny path ignores `response_headers: false` | all 10 RL pass |
| RL-M6 | `pool_owner_id` ignored (*control*) | only RL-6 fails |
| BA-M1 | budget check `>` → `>=` | all 17 BUD pass |
| BA-M2 | overcommit ratio forced to 1.0 | only BUD-10 fails |
| BA-M3 | budget mode gate removed | only BUD-15 fails |
| BA-M4 | the updating tenant is counted twice | all 17 BUD pass |
| BA-M5 | child rate not normalised | BUD-7 to 12 fail; BUD-13 passes |
| BA-M6 | ratio range widened to 0.9–2.4 | BUD-3 and 4 pass |
| BA-M7 | default budget mode `allocated` | all 17 BUD pass |
| BA-M8 | re-validation on update removed (*control*) | BUD-12 fails |
| CFG-CORS | api-gateway `cors_enabled: true` | CORS-1, 2, 3 and 5 fail; CORS-4, 6 and 7 pass |
| CFG-TIMEOUT | `proxy_timeout_secs: 60` | ERR-4 fails only through pytest-timeout; skipped with `--timeout=0` |

RL-M2 and RL-M3 each needed a second attempt. `Bucket::matches_config` compares `refill_rate` and the bucket type, so changing either rebuilds the bucket on every request. The valid variants change `refill()` and `matches_config` instead.

## Proposed fix order

1. Remove the escape hatches: APIKEY-1, OAUTH-1, OAUTH-2, AUTHZ-2 and ERR-4. Make conftest's credstore provisioning fail the session instead of warning.
2. Adopt the verified replacement tests for RT-3, RT-4, WS-3, MGMT-4, MGMT-7, MGMT-8 and SSE-1. They pass on `main` and fail under their mutation.
3. Rate limiting: change one variable per test. Rewrite RL-5, RL-6 and RL-8, pin the header values, add a refill leg, and add R-08 and R-09 tests as `xfail(strict=True)`.
4. Mock changes:
   - Make `/oauth2/token` distinguish Basic from form credentials.
   - Echo the request body and path.
   - Record the WebSocket handshake headers and support fragmented messages.
   - Add `event:` and `id:` SSE lines and a slow SSE endpoint.
5. Pin the error identity (content-type, `type`, `reason`) on every negative test.
6. Rename or delete the misattributed tests (BODY-1, BODY-2, TYPES-4 to TYPES-6). Lower `max_body_size_bytes` in the e2e config so scenario 8.1 is reachable (xfail until #4911).
7. When PLG-03, PLG-05, PLG-10, PLG-13, P-11, R-14 or R-17 lands, update its tests in the same PR.
8. Put cleanup in `finally`, check the delete results, and page through list results.

