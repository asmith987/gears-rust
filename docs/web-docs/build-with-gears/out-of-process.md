---
title: Run a gear out-of-process
description: Run a gear as its own process behind the same generated contract client, selected by configuration.
sidebar:
  label: Run a gear out-of-process
  order: 10
---

A gear can run **in the host process** (resolved through `ClientHub` as a direct call) or
**out-of-process** as its own binary. Consumers are unaffected: they depend on the same
contract and call the same generated client — configuration and discovery decide whether the
call is a local method call or a remote request. Out-of-process is a **deployment mode, not a
different kind of gear**.

This guide follows the `api-contracts` example (`examples/toolkit/api-contracts/`), where a
`PaymentApi` **provider** gear and a separate **consumer** gear talk to each other across
process boundaries over REST.

## The contract is the single source of truth

Declare the contract once and project it onto a transport. The macros generate a typed client
that works identically in-process or out-of-process:

```rust
#[toolkit::contract]
pub trait PaymentApi {
    async fn charge(&self, ctx: SecurityContext, req: ChargeRequest)
        -> Result<ChargeResponse, CanonicalError>;
}

// Project the contract onto REST (an OpenAPI-described HTTP surface).
#[toolkit::rest_contract(base_path = "/payments/v1")]
pub trait PaymentApiRest: PaymentApi { /* ... */ }
```

The same `ContractIr` also drives OpenAPI generation (and, via `#[toolkit::grpc_contract]` +
`toolkit-contract-protogen`, a `.proto` projection) — so you never hand-roll wire types.

## Provide and consume through the contract

The provider marks its implementation with `#[toolkit::provides]`; the consumer declares the
dependency with `#[toolkit::consumes]`. The framework wires the right backend at runtime:

- **In-process** — `ClientHub` hands back a direct call to the provider's implementation.
- **Out-of-process** — the framework hands back a **directory-resolving client** that finds the
  provider through the `DirectoryService` and calls it over REST. The consumer code is byte-for-byte
  identical.

No bespoke gateway and no per-service proto/tonic plumbing are required.

## Run as a separate process

An out-of-process gear ships an OoP binary that boots via `run_oop_with_options`. In the example
crates this is a feature-gated `[[bin]]` (build with `--features oop_module`, e.g.
`api-contracts-oop`, `users-info-oop`, `hello-oop`):

```rust title="src/main.rs"
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = OopRunOptions { gear_name: "api-contracts".into(), ..Default::default() };
    toolkit::bootstrap::oop::run_oop_with_options(opts).await
}
```

At startup the binary loads its config, creates a **lazy** `DirectoryService` client (it starts
even if the directory is not yet reachable — `cpt-cf-adr-eventual-readiness`), self-registers, and
runs the normal gear lifecycle.

## Discovery and addressing

Out-of-process gears find each other through the `DirectoryService`: every pod self-registers its
reachable endpoint, and consumers resolve providers by name. For **in-process REST providers**, the
REST host (`api-gateway`) advertises the base URL other pods use to reach it via
`ApiGatewayConfig.advertise_uri` (falling back to the bound address). Set `advertise_uri` explicitly
in Kubernetes, where the pod binds `0.0.0.0`:

```yaml
gears:
  api-gateway:
    config:
      advertise_uri: "http://api-contracts:8087"
```

## Switch modes with configuration

The deployment shape is a config decision, not a code change. Selecting `runtime.type: oop` runs
the gear out-of-process; the consumer's client lookup is identical either way:

```yaml
gears:
  api-contracts:
    runtime:
      type: oop
```

## Deploying out-of-process

`deploy/` provides a generic per-gear OoP Docker image (`deploy/docker/oop-gear.Dockerfile`,
parameterized by build args) and Helm charts (`deploy/helm/*`) that deploy each gear as its own
Deployment/Service, wired to the platform-host and `DirectoryService`. See `deploy/README.md`.

## See also

- [Gears & composition](../../concepts/gears-and-composition/) — in-process vs out-of-process.
- Reference: `docs/toolkit_unified_system/09_oop_grpc_sdk_pattern.md` — OoP runtime, bootstrap, and SDK pattern.
- Full code: `examples/toolkit/api-contracts/` (provider + OoP consumer), `examples/toolkit/users-info/`, `examples/toolkit/hello/`.
