# Task Engine and the market: scope feedback

How the planned Task Engine (`gears/task-engine/docs`, `main` at `6efb0f856`) compares with the leading tools that offer the same functionality, which parts of its scope to change, and why someone would choose it over Hatchet, Temporal or a SQL job queue.

Research date 2026-10-28. Tools surveyed: Temporal, Restate, DBOS (durable execution); Hatchet, Inngest, Trigger.dev (orchestration platforms); River, Oban, Solid Queue, graphile-worker, pg-boss (SQL-backed queues); BullMQ, Sidekiq, AWS SQS and Step Functions, Google Cloud Tasks (broker and cloud queues). The research notes, with a source URL per row, are in [`task-engine-landscape-research/`](task-engine-landscape-research/). Some rows there are marked lower confidence (from product knowledge rather than a page fetched for this review); the claims below use rows backed by a cited page, and say so where they do not.


## Contents

- [1. What the Task Engine is, compared with the market](#1-what-the-task-engine-is-compared-with-the-market)
- [2. Capability comparison](#2-capability-comparison)
- [3. Where the Task Engine goes beyond every surveyed tool](#3-where-the-task-engine-goes-beyond-every-surveyed-tool)
- [4. Proposed changes](#4-proposed-changes)
- [5. Why use the Task Engine instead of Hatchet, Temporal or a SQL queue](#5-why-use-the-task-engine-instead-of-hatchet-temporal-or-a-sql-queue)
- [6. Proposed MVP scope](#6-proposed-mvp-scope)
- [7. API surface: what to keep, change and add](#7-api-surface-what-to-keep-change-and-add)
- [8. Heartbeat mechanism](#8-heartbeat-mechanism)
- [9. Questions for the design owner](#9-questions-for-the-design-owner)
- [Sources](#sources)

## 1. What the Task Engine is, compared with the market

| Category | Tools | Model | Relation to the Task Engine |
|---|---|---|---|
| Durable execution | Temporal, Restate, DBOS | Your code is a workflow; the engine journals and replays it, so steps resume after a crash | Different product. The Task Engine has no replay; resuming is the worker's job (checkpoints). Inside the platform, the Durable Execution library fills this role |
| Orchestration platforms | Hatchet, Inngest, Trigger.dev | Queue + flow control (concurrency, rate limits, priority, debounce) + durable waits; DAGs (Hatchet) or code steps | **Closest peers.** Same feature families: fairness keys, per-key limits, waits, child tasks |
| SQL-backed job queues | River, Oban, Solid Queue, graphile-worker, pg-boss | A jobs table in your database, claimed with `SKIP LOCKED`; paid tiers add workflows and limits | **Closest technical peers** for storage and claiming |
| Broker / cloud queues | BullMQ, Sidekiq, SQS, Step Functions, Cloud Tasks | Redis or managed service queues; Step Functions adds a state machine | References for fairness groups, task tokens, dedup windows |

The Task Engine's own framing (PRD §1.2) is a shared lifecycle, tenant isolation, durable waits, GTS typing, central timeouts and controlled retries. ADR-0001 rejected wrapping Temporal, Hatchet or Conductor because the platform rules would still need a database in front, there would be an extra server, and there would be two sources of truth. That ADR is the only build-versus-buy analysis in the docs; it does not compare features.

---

## 2. Capability comparison

Legend: **Yes** native; **Partial** limited or via a pattern; **Paid** only in a paid or enterprise tier; **No** not available (or not found in the docs). "TE" is the Task Engine as designed on `main`.

| Capability | TE | Temporal | Hatchet | Inngest | Trigger.dev | River / Oban | pg-boss | BullMQ | SQS / Step Fn |
|---|---|---|---|---|---|---|---|---|---|
| Store and extra infra | SQL DB only (PG, MySQL, SQLite) | Cluster + Cassandra/SQL + Elasticsearch | Postgres (+ optional RabbitMQ) | SQLite or PG + Redis | PG, Redis, ClickHouse, more | Postgres (Oban also MySQL, SQLite) | Postgres | Redis | Managed |
| Authorization per end-tenant (who may create, claim, cancel which task) | **Yes** (PDP per task, tenant subtree, barriers, creator checks) | No (namespace roles) | No (Hatchet-tenant tokens) | No ("your application must enforce tenant authorization") | No (environment keys) | No (DB access) | No | No | IAM per queue / state machine |
| Fair share between tenants on one queue | Yes (round-robin) | Yes, weighted (GA 2026; paid toggle in Cloud) | Yes (`GROUP_ROUND_ROBIN`) | Partial (best effort; strict mode Enterprise) | Partial (opt-in since Sep 2026) | No (none documented) | No | Paid (Pro groups) | Partial (SQS fair queues reorder) |
| Per-tenant concurrency limit | Yes (queue × tenant, type × tenant) | No (semaphore-workflow pattern) | Yes (`max_runs` per key) | Yes (≤ 2 keys per function) | Yes (`perKey`) | Paid (River Pro, Oban Pro) | Yes (`groupConcurrency`) | Paid | No |
| Per-tenant queue-depth limit (exact) | Yes (exact counters) | No | No | Plan-level caps only | Plan-level caps only | No | No | Paid (max group size) | No |
| Per-key rate limit | Yes (p2) | Partial (dispatch RPS per key) | Yes | Yes (throttle, rate limit) | No | Paid (Oban Pro only) | Partial (enqueue throttle) | Paid | No |
| Priority | Urgency + tenant-local priority + aging (p3) | 1–5 | 1–3, within one workflow | Time offset | Time offset | Integer | Integer | Integer | No |
| Stale-worker fencing | Yes (token + epoch, bound to subject) | Yes (task token per attempt) | Not documented | Not documented | Not documented | Oban: per attempt; River: state only | Per attempt | Lock token | Step Fn: new token on timeout |
| Timeouts | Queue, assignment, execution, heartbeat, lifetime, SLA | Schedule-to-start, start-to-close, heartbeat, overall | Schedule, execution | Start, finish | TTL, max duration | Execution | Expiry | Lock duration | Visibility / state timeouts |
| Retry budgets | Two (reported failures, lost workers) | One | One | One | One | One | One | One | One |
| Dead-letter | DLQ queue + admin re-enqueue (revives the task) | Failed status + reset | Failed status + bulk replay | Bulk replay | Bulk replay | River Pro DLQ; Oban retry | Dead-letter queue | Failed set | SQS DLQ + redrive |
| Durable timer / input / event waits | Yes (4 kinds, event waits with CEL and PDP checks) | Yes (timers, signals, updates) | Yes (durable tasks, `wait_for`) | Yes (`waitForEvent`) | Yes (waitpoint tokens) | Paid (River Pro signals, Oban Pro `await_signal`) | No | No | Step Fn task token |
| Wait declared before the task starts | No (create, then park: race) | Signal-with-start | Yes (`wait_for` on DAG tasks) | No (documented race) | Partial (token first) | No | No | No | Yes (in the definition) |
| Dependencies | ALL/ANY at create **and added later**; parent/child | Code (child workflows) | DAG at definition + runtime children | Code | Code | Paid: at insert, new tasks can join later | At creation only (on purpose) | Flows; children from inside | Static |
| Adding an edge between two existing tasks | **Yes** | No | No | No | No | No | No (refused because of the race) | Only parent's own children | No |
| Dedup / cancel previous | Caller key (mandatory) + derived identity + order-value supersession | ID reuse/conflict policies, terminate-existing | TTL/status idempotency, cancel-in-progress | Idempotency, singleton, debounce | Idempotency key, debounce | Unique jobs; Oban `replace` | Singleton key | Dedup, debounce | FIFO 5-min dedup |
| Progress / checkpoints / notes | Progress (flush modes), merge-patch checkpoints, typed entries | Heartbeat details, history | Logs, streams | Realtime publish | `metadata` | Oban Pro recorded output | Output | Progress | No |
| Search over history | OData over 500M tasks, detail levels | Search attributes (Elasticsearch) | Metadata filters | SQL "Insights" | SQL "Query" (ClickHouse) | UI over table | Basic | No index | Basic |
| Retention | Ring partitions, per-tenant policy, legal-hold rescue | Namespace 1–90 days | 30 days default | Plan-based | Plan-based | Pruning | Pruning | Memory | 14–90 days |
| Lifecycle events out | Every transition, via outbox to the Event Broker | Partial | Partial | System events | Alert webhooks | Telemetry | No | Events | SQS: none per message |
| Published throughput | Target 1,000 creates/s, 500 claims/s | Per-namespace action limits | 10k tasks/s load test | "Hundreds of millions" in flight | Not published | River ~46k/s, Oban ~17.7k/s (benchmarks) | Not published | Not published | Managed |

Notes:
- "Not documented" for fencing means the research found no description of rejecting a stale worker's completion; Hatchet's docs explicitly accept duplicate execution after a worker is marked inactive.
- Temporal rows on timeouts, retries and cancellation come partly from product knowledge (see the research notes); re-check before quoting externally.
- Benchmark figures are the vendors' own, under different conditions; they are not comparable with the Task Engine's targets, only an indication of headroom.

---

## 3. Where the Task Engine goes beyond every surveyed tool

These capabilities were not found in any surveyed tool. Each is a place where the Task Engine pays complexity that the market has not found necessary.

| Capability in the Task Engine | Closest thing elsewhere | Cost in the design |
|---|---|---|
| Per-end-tenant authorization of every task operation, creator checks on referenced resources | None | Real cost, and the main reason to build (section 5) |
| Adding and removing dependency edges between existing tasks | New tasks joining a workflow (River Pro, Oban Pro); children added by the parent itself (BullMQ, Sidekiq) | Per-tenant graph lock, cycle walk, `queued`/`scheduled → blocked`, race with claim (PRD.md:841, DESIGN.md:2904) |
| API consumer parking a `claimed` or `running` task | Cancel; Temporal signals are delivered to running code, not used to evict it | Lease revocation mid-run, duplicate execution (DESIGN.md:532) |
| Exact per-tenant queue-depth counters | Plan-level caps; BullMQ Pro max group size | Counter writes on every create and claim, sharding, reconciliation (DESIGN.md:1633–1671) |
| Supersession by order value, derived dedup identities including an authorization-scope hash | Keyed "cancel previous" (Temporal terminate-existing, Hatchet cancel-in-progress, Inngest singleton cancel, DBOS debouncer) | `te_supersede_heads`, PDP pre-check before the transaction, retry loop, ToolKit `AccessScope` serialization dependency |
| Admin reassignment of a running task to a named worker, with accept | Not found | Hand-off state, `handoff_until`, deadline scan branch |
| Worker blacklist per queue | Not found | Blacklist table, coalescing per outage, claim filtering |
| Priority aging | Not found | Maintenance loop, base and effective priority |
| Two retry budgets (reported failures vs lost workers) | Not found (one budget everywhere) | Second counter, second backoff path; useful, but unusual |
| Exact claim order merged across several queues | Temporal: strict priority tiers per task queue; others claim per queue | k-way merge, cross-queue ordering rules (ADR-0008) |
| Event waits that re-run the broker's consume authorization and evaluate CEL per tenant | Inngest and Hatchet match events by expression without per-tenant authorization | Consumer, inbox per domain, copied PDP rules, CEL budgets, three open broker issues |
| Partition-ring retention with rescue of long-lived and legal-hold rows | Time-based pruning; Oban Pro tried state/day partitioning and **deprecated it** as too edge-case-prone | Ring state, fence, `_retained` tables, MySQL partition handling |
| Six timeout kinds plus SLA events | Temporal has a comparable set (four activity, three workflow) | Comparable to the best in class; no change proposed on its own |

---

## 4. Proposed changes

Each item names one specific feature of the current design and makes one specific change to it. Every item has the same parts:

- **Today**: what the design does now, with the endpoints, fields and PRD sections involved.
- **Proposal**: what stays, what changes, and what goes.
- **Not affected**: related features this item does **not** touch, so it is not read more broadly than it is.
- **Why**: the reason on the design's own merits (a race, a defect, or cost without a use case).
- **Evidence**: what the surveyed tools do. This only supports the Why; "no other tool does it" is not a reason on its own, because the Task Engine's main reason to exist is also something no other tool does.
- **Strength**: how strongly to argue for it.

The PRD tags each requirement p1, p2 or p3; this review reads p1 as the first release. Several items are tagged p2 or p3, but the DESIGN already builds them into the core that every task goes through (lock order, `finalize()` steps, schema). The aim is to design the core for p1 without them and add them later as modules.

| Group | Items |
|---|---|
| Strong objections (a race, a defect or a critical-path risk) | P1, P2, P3 |
| Cost-versus-value trade-offs | P4, P5, P6, P7, P8, P9 |
| Questions for the design owner | P10 |

### P1. Dependencies can only be set when a task is created

- **Today** (PRD §5.9, p2). A task can name predecessors in two ways:
  - at creation: `predecessors` and a mode (`ALL` or `ANY`) on `POST /tasks`;
  - later: `POST /tasks/{id}/dependencies` adds predecessors and `DELETE /tasks/{id}/dependencies/{predecessor_id}` removes one, while the task is `queued`, `scheduled` or `blocked`.
- **Proposal.**
  - Keep `predecessors` and the `ALL`/`ANY` mode on `POST /tasks`.
  - Remove `POST /tasks/{id}/dependencies`.
  - Replace `DELETE …/dependencies/{predecessor_id}` with an admin action that marks one dependency as satisfied (to unblock a task stuck on a predecessor that will never finish).
- **Not affected.** Parent/child tasks, the `children` wait, `dependency_failed` when a predecessor fails, and the rule that a re-enqueued predecessor does not revive its successors.
- **Why.**
  - Creating a task and then adding a dependency is a race: a worker can claim the task before the second call arrives, and the task runs without waiting. Dependencies can be added only while the task is unfinished and not yet running (PRD.md:841), so a claim that wins the race leaves the second call nothing to gate.
  - Adding dependencies later is the only reason the design needs a per-tenant lock on the dependency graph and a cycle check. A new task cannot create a cycle, because nothing depends on it yet.
  - It adds the transitions `queued → blocked` and `scheduled → blocked`, and a defect with them: adding a dependency to an `ANY` task that already has a finished predecessor blocks it again (DESIGN.md:2904, `if any unmet: s → blocked`).
  - The PRD gives no use case for it. Work discovered at runtime is covered by parent/child plus the `children` wait.
- **Evidence.** No surveyed tool lets an outside caller add a dependency between two existing tasks. pg-boss allows dependencies only at creation and says why: adding them through separate calls races with completion. BullMQ, which lets a parent add children later, has a run of open bugs from that path (#3120, #3122, #4855, #4944). River Pro and Oban Pro let *new* tasks join a running workflow, which is the same as creating a task with predecessors.
- **Strength.** Strong.

### P2. Waits are set by the worker during execution or declared at creation; no outside parking

- **Today** (PRD §5.4). `POST /tasks/{id}/wait-conditions` can be called by:
  - the worker that holds the task (with its lease token), while the task is `running`;
  - an API consumer with `update` permission and no token, on a task that is `queued`, `claimed` or `running`. On a claimed or running task this cancels the worker's lease.

  `POST /tasks` has no way to start a task in `waiting`.
- **Proposal.**
  1. **Keep** waits set by the worker during execution, including arm-then-park.
  2. **Add** an optional wait group to `POST /tasks`, so a task can start in `waiting`.
  3. **Remove** the API-consumer path to `wait-conditions`. If operators need to hold work, add an admin hold of **queued** work per queue or per tenant (later, as its own feature). Never take a `claimed` or `running` task away from its worker.
- **Not affected.** The four wait kinds, `resolve` for input waits, wait timeouts, arm-then-park, and the children wait.
- **Why.** There are three reasons to make a task wait, and only the first needs a wait set after creation:
  - **The worker finds out during the run that it must wait.** For example, an approval is needed only above an amount it computed, it started an action in another service and waits for the reply, an upstream system said "retry in 2 hours", or it created children from the input it read. This is not known at creation, so the worker sets the wait. This is the core of the feature.
  - **The creator wants the task to wait before it ever runs** ("run when approved"). The creator knows this upfront, so it belongs in the create request. Today it takes two calls, because create can only place a task in `blocked`, `scheduled` or `queued` (PRD.md:258), and a worker can claim the task in between. There is no race-free workaround for waits.
  - **Someone else wants to hold the task** (an incident, a maintenance window). A hold of queued work covers this. Parking a task that a worker is already running cancels its lease, and the work runs again after the task resumes (DESIGN.md:532: an API consumer may park a `queued`, `claimed` or `running` task, which clears its lease).

  The PRD lets API consumers set waits but gives no reason for it; its rationale only describes the worker case.
- **Evidence.** Waits set during execution: Temporal `await` and signals, Hatchet durable tasks, Inngest `waitForEvent`, River Pro and Oban Pro signals. Waits declared before the task starts: Hatchet `wait_for`, Step Functions task tokens, Temporal signal-with-start. Holds of a group or invocation instead of a single running task: BullMQ Pro can pause a group, Restate can pause invocations. No surveyed tool lets an API caller take a running task away from its worker into a wait.
- **Strength.** Strong.

### P3. Claims pick candidates with `SKIP LOCKED`

- **Today** (ADR-0008, DESIGN §4.6). A claim reads candidate tasks without locks, reserves concurrency counters, then tries `UPDATE … WHERE state = queued` on each candidate. ADR-0008:108 asks the ToolKit to use `SKIP LOCKED` by default, but ADR-0008:129 and the DESIGN pseudo-code still describe it as optional.
- **Proposal.** Inside the ToolKit claim primitive, after counters are reserved, select candidates with `FOR UPDATE SKIP LOCKED` on PostgreSQL and MySQL/MariaDB. Keep plain guarded updates on SQLite. Make ADR-0008:129 and DESIGN §4.6 match ADR-0008:108. Define the order guarantee as "in PRD order, among tasks that no other claim is taking at that moment".
- **Not affected.** The PRD dispatch order (urgency, tenant round-robin, priority, age), the order across several queues (PRD §5.6), counters reserved before task rows, batch claims, long-poll.
- **Why.**
  - Without `SKIP LOCKED`, every claimer reads the same first tasks and waits on the winner's row lock, while holding its counter locks. On a busy queue, claims then run one after another. The phase plan rates this the top schedule risk.
  - PRD §5.6 already asks for this behaviour: "A task that another claim is taking at that moment is skipped". The pseudo-code does not do that.
  - The documents contradict each other on it.
- **Evidence.** All five SQL queues surveyed (River, Oban, Solid Queue, graphile-worker, pg-boss) claim with `FOR UPDATE SKIP LOCKED`.
- **Strength.** Strong.

### P4. Per-tenant queue-depth limits are checked against a periodic count

- **Today** (PRD §5.2, p1). Two per-tenant depth limits are exact counters in `te_tenant_queue_backlog`:
  - `tenant_max_queue_depth`: `queued` tasks per tenant per queue (default 100,000);
  - `tenant_max_deferred_tasks`: `scheduled` + `blocked` + `waiting` tasks per tenant (default 100,000).

  Every create, claim and move between these states updates a counter row (up to 8 shards each, with the shard recorded on the task and a reconciliation job). The queue-wide and domain-wide depth limits are already checked against a count taken every 5 s.
- **Proposal.** Check the two per-tenant depth limits the same way as the queue-wide ones: the maintenance leader counts per tenant every few seconds, and creates are refused (`429`) while the latest count is at or above the limit. Remove the `te_tenant_queue_backlog` table, the `backlog_shard` column and its reconciliation.
- **Not affected.** All **concurrency** limits (per queue, per type, per tenant) stay exact counters. Rate limits are unchanged.
- **Why.**
  - With the default limits, every create and every claim writes a counter row, only to enforce a limit that is rarely reached.
  - The counter design has open defects: turning a limit on at runtime has no defined cutover (DESIGN.md:1667 says only "before enforcement starts"), and `limit_value = 0` means both "unlimited" and "being drained" (DESIGN.md:1645, 1677).
  - A depth limit is there to stop one tenant flooding a queue. Going over by a few seconds of creates, which the per-tenant create rate limit already bounds, does not defeat that. Concurrency limits protect other systems, so they stay exact.
- **Evidence.** No surveyed tool enforces an exact per-tenant backlog limit. Inngest and Trigger.dev cap queue depth per plan, and BullMQ Pro has a maximum group size. Per-tenant concurrency limits are common.
- **Strength.** Medium. Withdraw it if exact depth enforcement is a product requirement.

### P5. Event waits come after timer, input and children waits

- **Today** (PRD §5.4: event waits p2, match expressions p3). An `event` wait resumes a task when a matching Event Broker event arrives. To do that, the engine:
  - consumes every event on the configured topics with platform rights;
  - when the wait is set, repeats the broker's subscribe authorization checks with the caller's identity and stores the result as a filter;
  - matches events with an optional CEL expression under a cost budget;
  - records applied events in an inbox per database.

  It cannot work for tenant tasks until broker issues #5493 (delivery to tenants, `DESIGN.md:912`) and #5494 (the broker's own consume checks) are fixed. Both were open on 2026-10-28.

  Approvals depend on event waits. The PRD plans no approval wait kind: "approval gates use the `approval-service` gear: the task sets an **event** wait for its approval event" (`PRD.md:233`, also `PRD.md:1070`). The approval service has no design yet: its `PRD.md` and `DESIGN.md` are `TODO`, and its only related requirement is a status-changed notification (`gears/approval-service/docs/UPSTREAM_REQS.md:50`).

  `POST /wait-conditions/{id}/resolve` provides the value for an `input` wait. It needs the `update` action on the task (`DESIGN.md:1098`, PRD §5.4 Input Wait Condition). The same `update` action also allows tag changes, dependency add/remove, the API-consumer wait set, and adding a child without the parent's lease (`DESIGN.md:986`). The docs do not say what a repeated `resolve` returns.
- **Proposal.**
  - Ship `timer`, `input` and `children` waits first.
  - Treat `event` waits as a firm p2 commitment, because approval gates depend on them.
  - Design the core now so that event waits plug in as a module that calls the same internal `resolve` path. Build the module once #5493 and #5494 are closed and the approval service has a design.
  - For "wait for the reply to my own action", where the replying service knows the task, document that it calls `resolve` with the wait ID. This does not fit approvals: the approval service should not depend on the Task Engine.

  Because `resolve` carries request/reply until event waits arrive, fix two gaps in it:
  - **Permission.** Give `resolve` its own action (for example `resolve`), or accept a per-wait token returned when the wait is set, so the replying service needs nothing more. Today a service that only answers a question can also change the task's tags and dependencies.
  - **Repeated calls.** Specify that a repeated `resolve` with the same value returns success (as completion replay does, PRD §5.6 Idempotent completion), and that a different value is rejected. Today a replying service that retries after a network error cannot tell "already accepted" from a real failure.
- **Not affected.** The wait model, arm-then-park, wait timeouts, and the other three wait kinds.
- **Why.**
  - The engine reads events with platform rights and must copy another service's permission rules exactly. If the copy and the broker drift apart, a tenant can learn about events it may not read.
  - It adds a consumer, an inbox per database, CEL budgets and per-tenant wait counters to the core.
  - It is blocked by two open broker issues, and its main user, the approval service, has no design yet. Building it in p1 means hardening a feature that cannot resolve a single tenant wait.
  - Until then, request/reply between services that know the task is covered by `input` waits resolved by ID. Approvals are not covered: they need event waits, or a simple `input` wait as a stopgap (section 9).
  - Once `resolve` carries request/reply, its permission and retry behaviour matter more. A replying service gets more rights than it needs, and a lost response leaves it unable to tell whether its answer was taken.
- **Evidence.** Temporal signals, Restate awakeables, DBOS `send`, Step Functions task tokens and Trigger.dev waitpoint tokens all resume by ID. Only Inngest and Hatchet match events by expression, and neither checks per-tenant permissions on the match. In Step Functions the replying side needs only the task token and the separate `states:SendTaskSuccess` permission, not the right to change the execution. Trigger.dev waitpoint tokens can be completed through their own callback URL. I have not checked how these tools answer a repeated completion.
- **Strength.** Medium. This is about timing and dependencies; the feature itself is needed.

### P6. Dedup and supersession: drop the `authz_scope` mode; keep supersession within `subject` scope

- **Today** (PRD §5.1, p2; ADR-0012). Three related features:
  1. **Caller idempotency key**: required on every create; a retry with the same key returns the existing task.
  2. **Derived identity** (`dedup` trait on the task type): the engine builds the identity from chosen input fields, so independent producers land on one task. Options: `scope` = `subject` / `authz_scope` / `tenant`, and `window` = `active` / `active_or_succeeded`.
  3. **Supersession** (`supersede` trait): an integer field such as a revision number. A higher value cancels the group's unfinished task and replaces it; an equal value is a duplicate; a lower value is rejected as stale, even after the newer task has finished.
- **Proposal.**
  - Keep 1 and 2 as designed.
  - Drop the `authz_scope` value of `scope`.
  - Keep 3, but only within `subject` scope: a creator can replace only its own tasks.
- **Not affected.** The caller idempotency key, derived identities with `subject` or `tenant` scope, both `window` options, and the supersession rules themselves (higher value replaces, equal is a duplicate, lower is rejected as stale).
- **Why.**
  - **`authz_scope`** builds the identity from a hash of the caller's compiled permissions. That needs a stable serialization of the permission scope that ToolKit does not have, so the design already has to reject this mode for now. No other tool does anything like it.
  - **`authz_scope` does not do what it is for.** It is meant to let callers with the same rights share a task, so a result computed for one caller is never handed to a caller with different rights. Two facts break that:
    - **It hashes the wrong scope.** The hashed `AccessScope` is the result of `authorize(create, type, queue)` (`DESIGN.md:2534`, used at `:2564`): the caller's right to create tasks of this type in this queue. It says nothing about the caller's rights on the data the task reads, so two callers with the same task-create rights but different data rights still share one task.
    - **The work does not run with the caller's rights anyway.** Workers act as themselves (`PRD.md:956`), because there is no on-behalf-of token yet (`PRD.md:1609`). For identical input the result is the same whoever asked. The caller's rights matter only at creation, and two checks already cover that on every request, duplicates included: creator checks on the resources the task references, and a duplicate returns the existing task only if the caller may read it (otherwise `403` without the ID).

    A type whose result depends on the caller should use `subject` scope, or put what it depends on (for example a role or group ID) in `key_fields`. A type whose result does not should use `tenant` scope.
  - **Supersession** is the expensive part, and most of the cost comes from replacing another user's task:
    - a separate table of the latest revision per group, with its own retention;
    - a permission check to cancel the old task, done **before** the create transaction, because permission calls are not allowed inside one;
    - a re-check inside the transaction, and a retry from the start if another submission got in first;
    - in graceful mode, the old and new task can run at the same time for up to the grace period.

    Restricting it to `subject` scope removes the cross-user permission case: a creator may always cancel its own task.
  - **Supersession is kept** because it protects against late arrival. If revision 7 is submitted and revision 6 arrives afterwards, supersession rejects 6 as stale, so the newer work is never replaced by older work.
- **Evidence.** Derived identities are common: Hatchet builds idempotency keys from an expression over the input, Inngest has idempotency expressions, Temporal users derive the workflow ID from the input. Keyed "cancel previous" is common too: Temporal `TERMINATE_EXISTING`, Hatchet `CANCEL_IN_PROGRESS`, Inngest singleton `cancel`, DBOS debouncer, Oban `replace`. No surveyed tool rejects stale revisions in the engine, so supersession goes beyond the market; it stays because of the late-arrival protection, not because others have it. None builds the identity from the caller's permissions.
- **Strength.** Medium. `authz_scope` hashes the wrong rights and is blocked on ToolKit anyway; the `subject`-scope limit removes the cross-user permission check.

### P7. Re-running a failed task creates a new task

- **Today** (PRD §5.8, p2). An admin can re-enqueue a `failed` or `dead_lettered` task. The same task goes back to `queued`: its row moves from the history table back to the live table, retry counters reset, the result is cleared, the dedup identity is taken again and the lifetime timer restarts.
- **Proposal.** "Re-run" creates a **new** task with the same type, queue, input and context. The new task records `rerun_of = <old id>`, and the old task records `rerun_as = <new id>` and stays terminal.
- **Not affected.** Dead-lettering itself (the `dead_lettered` state and DLQ routing), retries with backoff, and the `failed` state.
- **Why.**
  - It is the only case where a finished task becomes live again. That one exception needs special handling in history, dedup, timers and retention.
  - A re-enqueued child can come back to life under a parent that already completed: the re-enqueue rules (PRD.md:803) do not check the parent.
  - Event consumers see "failed", then "queued" for the same task, so anything that treats "failed" as final is wrong.
  - With a new task, all of this disappears. The cost is that the task ID changes; the link makes it easy to follow.
- **Evidence.** Temporal reset, Hatchet bulk replay, Inngest replay and Trigger.dev replay all start a new run and leave the failed one as it was.
- **Strength.** Medium.

### P8. Retention partitions on PostgreSQL only

- **Today** (ADR-0009, DESIGN §3.2 RetentionService). History tables on PostgreSQL **and** MySQL/MariaDB use a ring of 17 partitions of 7 days each. When a partition is reused, rows that must live longer (live tasks, longer retention policies, legal holds) are first copied to `_retained` tables, then the partition is truncated. Every write for an old task must first lock a ring-state row. SQLite uses bounded `DELETE`s instead.
- **Proposal.**
  - Keep the ring on PostgreSQL.
  - On MySQL/MariaDB, use bounded `DELETE`s, the same path SQLite already has.
  - Ask whether keeping data beyond the normal retention (legal holds, very long policies) is needed in p1. If not, leave out the `_retained` copy step until it is.
- **Not affected.** The split between live and history tables, per-tenant and per-type retention policies, and the retention period itself.
- **Why.**
  - The ring with copying is the most intricate storage mechanism in the design.
  - The docs have gaps in it: DESIGN's retention steps (DESIGN.md:850–860) have no step that frees the slot, while ADR-0009 step 5 does (ADR-0009:90), so the step numbers differ; nothing says when a freed slot becomes `active` again; and the global lock order (DESIGN.md:1597–1604) leaves out the `te_ring_state` row that history writers must lock (DESIGN.md:1439).
  - MySQL partitions add a second set of rules (every unique key must include the partition key).
  - PostgreSQL alone keeps the main benefit, cheap removal of a week of data, where the volume is.
- **Evidence.** Oban Pro tried a partitioned jobs table and deprecated it as having too many edge cases. The SQL queues delete by time (River, Oban), delete on completion (graphile-worker) or partition simply by queue (pg-boss).
- **Strength.** Medium.

### P9. Engine search covers live and recent tasks

- **Today** (PRD §5.17 p1, search-latency requirement p1). OData search over **all** stored tasks (target 500 million), with filters on state, type, queue, tags, resource references and dependencies, four detail levels, and cursors that span several databases.
- **Proposal.**
  - In p1, search covers live tasks and finished tasks from a recent window. The length of the window is a decision for the design owner.
  - `GET /tasks/{id}` and a task's own history keep working for every stored task.
  - Long-range search moves to a separate analytics store fed by the event outbox, later. ADR-0001 already plans this offload.
- **Not affected.** Reading a task by ID, a task's event history, retention, and lifecycle events.
- **Why.** Meeting a search-speed target over 500 million rows in the transactional database drives extra history indexes, cross-database cursors and the large capacity test, all on the p1 critical path. Most searches are about live or recent tasks.
- **Evidence.** Inngest and Trigger.dev search history in ClickHouse, and Temporal in Elasticsearch; none searches long history in the transactional store.
- **Strength.** Medium.

### P10. Admin reassignment, worker blacklist and priority aging: ask for a use case

- **Today** (all p3):
  - **Reassignment**: `POST /tasks/{id}/reassign` hands a claimed or running task to a named worker, which must call `accept` within a time window. Needs the `handoff_until` column and a branch in the deadline scan.
  - **Worker blacklist**: a worker that fails repeatedly on a queue is blocked from that queue for a while. Needs the `te_worker_blacklist` table and a filter on every claim.
  - **Priority aging**: a background job raises the priority of tasks that wait too long. Needs a separate "base" and "effective" priority and a maintenance loop.
- **Proposal.** Ask the design owner which adopter needs each one. Remove from the PRD and the p1 design any of them that has no named need. Add it back later as a module.
- **Not affected.** Voluntary release, lease expiry and requeue, tenant round-robin, and normal priority.
- **Why.** None of the three has a stated use case. Although tagged p3, parts are already built into the p1 design (a column, a scan branch, a claim filter). Each adds code paths that must be tested against everything else. Their goals are mostly covered already: release plus claim instead of reassignment, fixing or redeploying the worker instead of blacklisting, tenant round-robin instead of aging across tenants.
- **Evidence.** None of the three was found in any surveyed tool.
- **Strength.** A question. It rests on "no one has asked for it", not on a defect.

### Kept as designed

- **Leases with token, epoch and subject binding.** Stronger than most surveyed tools; matches Temporal and Step Functions. Keep.
- **Worker-session heartbeats.** The common pattern (Sidekiq, Oban, Hatchet track liveness per process). Keep.
- **Timeout set.** Comparable to Temporal. Keep.
- **Tenant fairness and per-tenant concurrency limits.** Matches Hatchet and the paid tiers elsewhere; central to the multi-tenant goal. Keep.
- **Two retry budgets** (one for reported errors, one for lost workers). Unlike every surveyed tool, but useful: a task does not lose its retries because a worker crashed. Keep.
- **Outbox events for every transition.** More complete than most tools, and needed for platform integration. Keep, but reconsider writing every event twice (`te_events` and the outbox).

---

## 5. Why use the Task Engine instead of Hatchet, Temporal or a SQL queue

### Reasons that hold

1. **End-tenant security inside the engine.** Every operation is authorized per task by the platform PDP (tenant, subtree, barriers, type, queue). Tenant-owned task types and tenant-operated workers are first-class; platform workers cross barriers only on platform queues; creator checks stop a tenant from using a worker as a confused deputy (ADR-0016). Every surveyed tool scopes authorization to its own customer, namespace, environment or database credentials, and expects the application to enforce end-tenant rules. Building this in front of Hatchet or Temporal means a gateway that duplicates task state for authorization and queries, which is the two-sources-of-truth problem ADR-0001 describes.
2. **Workers that belong to tenants.** Tenants can run their own workers against shared queues, fenced by tokens bound to their subject. Temporal would need a namespace per tenant to isolate them; Hatchet and Inngest workers belong to the platform customer, not to its end-tenants.
3. **Platform-native integration.** GTS-typed tasks with schemas registered at runtime, lifecycle events on the platform Event Broker, ToolKit SecureORM tenant scoping, canonical errors, one REST and SDK surface for every gear. No new server type for operators: it is a gear on the platform database.
4. **Fairness and per-tenant limits without a paid tier.** Temporal fairness is a paid toggle in Temporal Cloud, Inngest's strict fairness is Enterprise-only, River Pro and Oban Pro charge for per-key limits, BullMQ groups are Pro. Self-hosted Temporal and Hatchet (MIT) do offer fairness for free, so this reason is weaker than 1–3.
5. **One store for state and audit.** Task state, history and events commit in one transaction in the task's database, with per-tenant retention and legal holds.

### Reasons that do not hold, or favour the existing tools

1. **Durable execution of code.** If a team wants multi-step workflows written as code that resume after a crash, Temporal, Restate, DBOS or Hatchet durable tasks do that and the Task Engine does not. Inside the platform that need belongs to the Durable Execution library.
2. **Maturity and tooling.** Dashboards, SDKs in many languages, replay tooling, years of production use. The Task Engine starts from zero; the review found dozens of specification gaps before any code exists.
3. **Throughput headroom.** Vendor figures (Hatchet 10k tasks/s load test, River and Oban tens of thousands of jobs/s in benchmarks) are well above the Task Engine's targets (1,000 creates/s, 500 claims/s). The targets may be enough; the point is that scale is not a reason to build.
4. **Cost.** The phase plan estimates roughly 45–70 engineer-weeks for the gear plus 12–22 for ToolKit and Event Broker prerequisites. A tool adoption would cost a fraction of that, if the security model could be met another way.

### When an existing tool is the better choice

- **Only platform-operated workers, tenants never run workers or claim tasks.** Then end-tenant authorization reduces to "who may create and read tasks", which a thin API in front of Hatchet (MIT, Postgres, round-robin fairness, per-key limits, `wait_for`) could enforce. The build case weakens considerably.
- **Workflows as code inside one gear.** Use the Durable Execution library, or Temporal / Restate / DBOS.
- **Simple background jobs inside one gear's own database.** A SQL queue pattern (River-style `SKIP LOCKED` table) is enough and needs no shared service.

**Verdict.** Building is justified by reasons 1–3 if tenant-operated workers and per-task end-tenant authorization are firm requirements. The scope beyond that (section 3) is not justified by the market and should be cut or deferred.

---

## 6. Proposed MVP scope

| Area | p1 (keep) | Later, as modules, when a first adopter asks | Drop |
|---|---|---|---|
| Create | Idempotency key, derived identities (`subject` or `tenant` scope), typed input, `run_after`, predecessors on create, initial wait group | Supersession limited to `subject` scope (P6) | `authz_scope` identity mode; adding or removing dependencies after creation |
| Dispatch | `SKIP LOCKED` claim in PRD order, urgency, tenant round-robin, per-queue and per-tenant concurrency, per-tenant depth limits checked against a periodic count | Type concurrency, rate limits, priority aging (only if an adopter needs it) | Exact per-tenant depth counters |
| Execution | Session heartbeats, token + epoch fencing, start / progress / checkpoint / complete / release, timeouts, two retry budgets | Reassignment, worker blacklist (only if an adopter needs them) | |
| Waits | Timer, input (`resolve`), children, set by the worker during execution or declared at creation | Event waits, a firm p2 commitment because approvals depend on them (after #5493 / #5494 and the approval-service design); admin hold of queued work at queue or tenant level | API-consumer parking (replaced by create-time waits and the admin hold) |
| Failure | Retries with backoff, `failed` outcome | DLQ routing, re-run as a new linked task | Reviving finished tasks |
| Data | Live/history split, retention partitions on PostgreSQL (bounded deletes on MySQL and SQLite), outbox events, search over live and recent tasks | Long-range search in an analytics store; keeping data beyond normal retention (legal holds) | MySQL partitions |
| Security | Full ADR-0016 model | | |

---

## 7. API surface: what to keep, change and add

DESIGN §3.3 defines 36 REST endpoints, and the SDK trait mirrors them. The PRD treats any change to the trait as a breaking change, so every endpoint that ships in p1 is hard to remove later.

For each endpoint, the tables below give:
- **Verdict**: **Keep**, **Remove**, **Merge** (fold into another endpoint), **Defer** (not in p1; add when an adopter needs it) or **Question** (ask the design owner).
- **Size bounded?** For read routes: whether a documented limit caps what one request returns or has to compute (**Yes**, **Partly**, **No**, **Not specified**). Writes are already bounded: claims by `claim_max_batch`, batch heartbeats by 500 leases, bulk operations by 1,000 task IDs, payloads by 64 KB per task.
- **What to change or add** to make the endpoint safe or simpler.

### API consumers (gears and users that create and manage tasks)

| Endpoint | Verdict | Size bounded? | What to change or add |
|---|---|---|---|
| `POST /tasks` | Keep | — | Add an optional wait group, so a task can start in `waiting` (P2) |
| `GET /tasks/{id}` | Keep | **Not specified** | Define the response: the `long` view by default, with no embedded lists. Today the `view` levels exist only for search; if the single read embeds the `full` relations, it returns all entries, tags, successors and wait conditions, none of which has a limit on `main` |
| `GET /tasks` (search) | Keep | Partly | Pages are capped (100 items, 20 at `full`) and embedded events are capped at 50 per item, but embedded **entries** have no cap (20 items × all entries, up to 64 KB each), and whether "dependencies" includes successors (no limit) is not specified. Cap every embedded list, or embed only counts. Add filters `parent_task_id` and `predecessor_id`, because there is no other way to list a task's children or successors. Limit p1 to live and recent tasks (P9) |
| `GET /tasks/count` | Keep | Yes | None. The ToolKit OData layer refuses `$count` on purpose (a page carries no total), so a separate, capped count endpoint is the platform-compliant design |
| `GET /tasks/{id}/events` | Keep | **No** | Add cursor paging with a default and maximum page size, and filters by event type and time. Today one task's history has no limit: with progress events every 5 s, a task that runs for 100 days collects about 1.7 million events, all returned in one response |
| `POST /tasks/{id}/cancel` | Keep | — | None |
| `PATCH /tasks/{id}/tags` | Keep (p2) | — | Add a limit on tags per task (none on `main`) |
| `POST /tasks/{id}/dependencies` | Remove | — | P1 |
| `DELETE /tasks/{id}/dependencies/{predecessor_id}` | Remove | — | Replace with an admin "mark dependency satisfied" (P1) |
| `POST /tasks/{id}/wait-conditions`, API-consumer path (no lease token) | Remove the path | — | P2. The endpoint stays for workers |
| `GET /tasks/{id}/wait-conditions` | Keep | Partly | Only the current wait group is kept, but a group has no limit on conditions and each can carry a 16 KB input schema. Add a limit per group (for example 16) |
| `POST /wait-conditions/{id}/resolve` | Keep | — | Make an identical repeated call return success, so a retry after a network error is safe (not specified on `main`). Consider a permission narrower than `update` for the replying service (P5) |
| `POST /tasks/{id}/entries`, API-consumer path | Defer | — | See "Task entries" below |
| `GET /tasks/{id}/entries` | Defer | **No** | When it ships: cursor paging and a per-task entry limit (neither exists on `main`; each entry can be 64 KB) |
| `GET /tasks/{id}/entries/{entry_id}` | Remove | Yes | Entries never change, so the list call returns everything the single read would |
| `GET /queues` | Keep | **No** | No paging is specified, and each visible queue reports depth, running count and the number of tasks in each state. Counted live, each request counts the live rows of every queue, and a deep backlog has millions of `queued` rows; tenant callers also need counts filtered to their scope. Serve the numbers from the periodic counts the maintenance job already takes for depth limits (every 5 s), page the queue list, and cap counts |
| `GET /queues/{id}` | Keep | **No** (cost) | Same as `GET /queues` |

**Task entries** (PRD §5.5) are tagged p1: typed, immutable records attached to a task, with five built-in GTS entry types, a 64 KB payload each, filters by type, time and creator, and storage in the history partitions. They overlap with progress (how far the task is), checkpoints (where to resume) and the event history (what happened to the task). The comparable feature elsewhere is a per-run log (Hatchet `ctx.log`, Trigger.dev logs and metadata). Proposal: move entries to p2, let only the worker write them, and drop the single-entry read.

### Admins and operators

| Endpoint | Verdict | Size bounded? | What to change or add |
|---|---|---|---|
| `POST /tasks/{id}/reassign`, `POST /tasks/{id}/accept` | Remove (or Question) | — | P10. Release plus claim covers handing work back |
| `POST /tasks/bulk/cancel` | Keep (p2) | — | None |
| `POST /tasks/bulk/reprioritize` | Keep (p2) | — | None. A list of one covers the single-task case |
| `POST /tasks/bulk/retry` and `POST /admin/tasks/{id}/reenqueue` | Merge | — | Keep one bulk "re-run" that takes a list of IDs and creates a new linked task per ID (P7) |
| `POST /tasks/bulk/move` | Question | — | The most involved bulk operation: admission against the destination queue's limits, retry and timeout settings taken again from the destination, a same-database rule, and leased tasks refused. Ask which operator scenario needs it; if none, defer |
| `GET /admin/dlq` | Remove | Depends on search paging | Fans out to every task database over dead-lettered tasks kept for the full retention period (possibly millions). Search on `state = dead_lettered` or the dead-letter queue already does this, with paging across databases |
| `POST /admin/config`, `GET /admin/config/{version}` | Defer | Yes | Runtime configuration is p3, but the p1 design already carries its tables, versions and `If-Match` handling. Until then, queue settings change through GTS queue registration |

### Workers

| Endpoint | Verdict | What to change or add |
|---|---|---|
| `POST /tasks/claim` | Keep | None |
| `POST /tasks/{id}/start` | Question | `start` and the `claimed` state exist to catch claim responses that never reached the worker. A session heartbeat that lists the worker's leases could catch that instead (section 8, H2), which would let a claim go straight to `running`. ADR-0002 rejected merging the two states for this reason, so it is a design question, not a clear cut |
| `POST /tasks/{id}/heartbeat` (single task) | Merge | A heartbeat renews the whole worker session, not one task. Keep only the session heartbeat `POST /tasks/heartbeat`, which the SDK already uses |
| `POST /tasks/heartbeat` (batch) | Keep | Renew without a PDP call (section 8, H1); treat leases missing from the list as lost (H2) |
| `POST /tasks/{id}/progress`, `/checkpoint`, `/complete`, `/release` | Keep | None. `complete` also reports failures |
| `POST /tasks/{id}/wait-conditions` (worker path), `/wait-conditions/park` | Keep | None |
| `POST /tasks/{id}/entries` (worker path) | Defer | With entries |

### Things to add

Everything this review proposes adding to the API, in one place:

| Addition | Kind | Source |
|---|---|---|
| Optional wait group on `POST /tasks` | New request field | P2 |
| Admin "mark dependency satisfied" | New admin action | P1 |
| Admin hold of queued work per queue or tenant | New admin action (later, only if needed) | P2 |
| Bulk "re-run" creating a new linked task (`rerun_of` / `rerun_as`) | Replaces two endpoints | P7 |
| `parent_task_id` and `predecessor_id` filters on search | New search filters | Children and successors cannot be listed today |
| Cursor paging on `GET /tasks/{id}/events` and `GET /tasks/{id}/entries` | Paging | Unbounded responses |
| Limits: tags per task, wait conditions per group, entries per task | New config limits | Unbounded lists |
| Caps on lists embedded at search `full` | Response limit | Unbounded embedded entries |
| A defined response for `GET /tasks/{id}` | Spec | Not specified today |
| Queue statistics from periodic counts, plus paging on `GET /queues` | Implementation and paging | Live counting cost |
| Idempotent repeated `resolve` | Behaviour | Safe retries for replying services |
| A narrower permission for `resolve` (to consider) | Authorization | `update` also allows tag and dependency changes |

### Totals

Of the 36 endpoints: 21 keep (one of them, `POST /tasks/{id}/wait-conditions`, loses its API-consumer path), 6 remove, 3 merged into 2, 4 defer, and 2 questions (`bulk/move`, `start`).

---

## 8. Heartbeat mechanism

### How it works today (ADR-0017, DESIGN §3.2, PRD §5.6)

- A worker process opens one **session** per task database with its first claim. Every task it claims in that database is attached to the session.
- The session has a lease length, `lease_secs`: the shortest heartbeat timeout of any task the session has held (30 s by default). Each claim can **lower** it; it never goes back up.
- A heartbeat (`POST /tasks/heartbeat`, up to 500 leases per call) does three things:
  1. authorizes the worker's `claim` permission with the PDP (once per call);
  2. renews the session with one row update: `lease_until = now + lease_secs`;
  3. reads the listed tasks once and returns, per task, `ok` plus control signals (cancel requested, state) or `LEASE_LOST`.

  It writes nothing to the task rows. Workers send it every `lease_secs / 3` (10 s by default).
- A sharded **session scan** finds sessions with `lease_until < now`, marks them `expired`, then requeues all their tasks with backoff, as one liveness failure per worker and queue.
- Every worker write (start, progress, checkpoint, complete, release, set wait) is fenced by the task's `lease_token` and the worker's subject, so a worker whose lease ended cannot change the task.
- A task that is stuck while its process keeps heartbeating is ended only by its **execution timeout**.

### Comparison with other tools

| Tool | What is kept alive | How a stuck single task is detected | Fencing of a stale worker |
|---|---|---|---|
| **Task Engine** | The worker process (one session per database) | Only by the execution timeout (engine default 3,600 s; 86,400 s on the shipped `default` queue) | Lease token + epoch, bound to the worker's subject |
| Temporal | Each activity, through `RecordActivityHeartbeat` and a per-activity heartbeat timeout (optional) | Per activity: a missed heartbeat timeout, or start-to-close timeout | Task token per attempt |
| Hatchet | The worker process (heartbeat every 4 s, inactive after 30 s) | Per task: `execution_timeout` (default 60 s), which the task extends itself with `refresh_timeout` | Not documented; duplicates accepted |
| BullMQ | Each job: a lock renewed by the worker | Per job: the lock expires and the stalled-job checker moves it back | Lock token checked when the job finishes |
| AWS SQS | Each message: visibility timeout, extended with `ChangeMessageVisibility` | Per message: the timeout expires | A new receipt handle per receive |
| AWS Step Functions | Each task: `HeartbeatSeconds`, renewed with `SendTaskHeartbeat` | Per task | A new task token after a timeout |
| pg-boss | Each job: `heartbeatSeconds` | Per job | Completion only for the attempt the worker holds |
| Solid Queue | The worker process | No per-job check; jobs of a dead process are marked failed | Not documented |
| Oban / River | The node (Oban Pro Lifeline) or nothing | Rescue after a fixed time (1 hour by default) | Oban: per attempt; River: a state check only |

The session model is the same idea as Hatchet, Solid Queue and Sidekiq Pro: keep the process alive, not each task. That cuts heartbeat writes from one per task to one per process, which is the right choice at 100,000 running tasks. Its fencing is stronger than most tools. But Hatchet, the closest match, pairs process heartbeats with a **short per-task deadline that the task must extend**, and every per-task design (Temporal, BullMQ, SQS, Step Functions, pg-boss) catches a single stuck task within seconds or minutes. The Task Engine has no equivalent short per-task check.

### Findings at a glance

None of these breaks safety: leases are fenced, so a worker that lost its lease cannot change a task, and a re-run after a requeue is allowed by the at-least-once contract. They differ in what they cost: availability, how fast a stuck task is noticed, or a gap in the spec. Ordered by when to act.

| ID | Finding | Kind | Severity | Fix | When |
|---|---|---|---|---|---|
| H1 | A PDP outage requeues every running task on the platform | Availability | High | Renew the session on authentication alone; check `claim` rights on writes | Before p1 |
| H6 | Whether a task counts as timed out or as a lost worker depends on which scan reaches it first | Correctness | Medium | Apply the same cause precedence in the session scan as in the deadline scan | Before p1 |
| H5 | What a denied heartbeat releases is not specified | Spec gap | Low–medium | Apply the PDP constraints per lease; release only the leases outside them | Specify before coding |
| H3 | Lowering the session lease at claim can expire a healthy session | Defect (race) | Medium | Fix a session's lease length when it opens; give short-lease tasks only to a session with a short enough lease | Should fix |
| H2 | A stuck task inside a live process is caught only by the execution timeout | Liveness | Medium–high | Treat a lease missing from two consecutive heartbeats as lost and requeue it | Should fix |
| H4 | One missed renewal requeues everything the process holds | Accepted trade-off | Medium | Reserved heartbeat permits; SDK stops handlers on `LEASE_LOST`; session-expiry metric | Decide |
| H7 | A completion between "session expired" and "requeue" still succeeds | Harmless | Low | Fix the ADR-0017 test wording | Doc fix |

### Findings

**H1. A PDP outage requeues every running task on the platform (high).**
Every heartbeat calls the PDP. If the PDP cannot be reached, the heartbeat returns `503` "and changes nothing" (DESIGN §3.3), so the session is **not renewed**. After one lease length (30 s by default), every session on the platform expires. The session scan then requeues every running task as a liveness failure, so work in progress runs again and every task loses one requeue from its budget. Each expiry also counts as one strike toward the worker blacklist for every worker and queue, so repeated or flapping outages can blacklist workers across the fleet. A PDP outage turns into a platform-wide task outage.
- **Fix.** Renew the session on authentication alone (the session is already bound to the worker's subject, and every write is fenced and authorized separately). Re-check `claim` authorization on a slower cadence or on the next write. If the PDP is unreachable, keep renewing, but refuse writes. Revoked rights still take effect at the next write, such as `complete`.
- **Elsewhere.** None of the surveyed tools documents a remote policy call on every heartbeat. Temporal authorizes API calls with a local claim mapper (JWT); Hatchet uses tenant API tokens.

**H2. A stuck task inside a live process is caught only by the execution timeout (medium–high).**
If a task's handler hangs, or the SDK drops a task without releasing it, the process keeps heartbeating and the task stays leased, holding its concurrency slot, until the execution timeout. That is 24 hours on the shipped `default` queue. ADR-0017 accepts this trade-off explicitly; the issue is that nothing narrower backs it up.
- **Fix (recommended).** The batch heartbeat already lists the leases the process still holds. Treat a task of the session that is missing from the list for, say, two consecutive heartbeats as lost and requeue it. This is a read the heartbeat already does, with no extra writes. It also catches claim responses that never reached the worker, which could make `start` unnecessary (section 7).
- **Alternative.** Do what Hatchet does: a short default execution timeout that the task extends while it works.

**H3. Lowering the session lease at claim can expire a healthy session (medium).**
A claim of a task with a short heartbeat timeout lowers `lease_secs` for the **whole** session immediately. The worker learns the new heartbeat interval only from that claim response. If the response is lost, or the process's heartbeat loop does not pick the new interval up before its next tick, the session expires and all its tasks are requeued. The lease also never goes back up (ADR-0017:56), so one short-lease task makes the process heartbeat faster for the rest of its life.
- **Fix.** Fix a session's lease length when the session opens. Hand a task with a shorter heartbeat timeout only to a session whose lease is short enough (the SDK can open a second session for such tasks).

**H4. One missed renewal requeues everything the process holds (medium; accepted by ADR-0017).**
A GC pause, a network blip or a shed heartbeat longer than the lease (two missed intervals by default) expires the session, and every task of the process is requeued and runs again. A task that crashes its process does the same to its neighbours: every task of the session is requeued as a liveness failure (DESIGN.md:2756) and uses up one of its liveness requeues (queue default 20, PRD.md:786). Under load, heartbeats are shed last (DESIGN.md:2032), but the docs do not say which admission class claims use, so a claim storm may still take the permits heartbeats need.
- **Fix.** Reserve admission capacity for heartbeats that claims cannot use. Have the SDK stop handlers as soon as a heartbeat reports `LEASE_LOST`. Track session expiries per worker as a metric.

**H5. What a denied heartbeat releases is not specified (low–medium).**
A heartbeat is authorized once for all its leases. On an explicit denial "the engine releases the task". For a batch, the docs do not say whether that means only the tasks the worker may no longer claim (for example, one queue), or every task in the call.
- **Fix.** Apply the PDP constraints per lease and release only the leases outside them.

**H6. Whether a task counts as timed out or as a lost worker depends on which scan reaches it first (medium).**
The deadline scan gives an execution timeout precedence over liveness: a `running` task past its execution deadline ends `timed_out` (DESIGN.md:2770–2772). The session scan has no such check: it requeues every `claimed` or `running` task of an expired session as a liveness failure (DESIGN.md:2755–2759). A task whose execution deadline passed and whose session expired gets either outcome, and uses either retry budget, depending on which scan runs first.
- **Fix.** Apply the same precedence in the session scan: a task past its execution deadline is finalized as `timed_out`, not requeued.

**H7. A completion between "session expired" and "requeue" still succeeds (low).**
The session scan first marks a session `expired`, then requeues its tasks in later batches (DESIGN.md:2753–2759). Worker writes are fenced on token, subject and state only (DESIGN.md:734), not on session status, so a completion in that window succeeds. ADR-0017 says that after expiry "every call from the old worker returns `LEASE_LOST`" (ADR-0017:80). The outcome is harmless (the work did finish); the ADR's wording is wrong.
- **Fix.** Correct the ADR-0017 statement and its test, or add the session status to the fence.

**Verdict.** The design is sound in principle: process-level liveness plus per-task fencing is a good fit for the target scale. **H1 and H6 must be fixed before p1**, H5 must be specified before coding, H2 and H3 should be fixed, H4 needs a decision, and H7 is a documentation fix.

---

## 9. Questions for the design owner

1. Is there a use case for **adding or removing dependencies after a task is created**, that predecessors on create and parent/child cannot cover? (P1)
2. Is there a use case for an **API consumer making someone else's task wait**, beyond holding queued work during an incident or maintenance? (P2)
3. Is **exact** per-tenant queue-depth enforcement a product requirement, or is a few seconds of overshoot acceptable? (P4)
4. How should **approval gates** work before event waits exist: a simple `input` wait as a stopgap, or do approvals wait for p2? (P5)
5. How long a **recent window** should engine search cover, and can long-range search move to an analytics store? (P9)
6. Which adopter needs **typed task entries** in p1, and why can the event history not serve it? Which operator scenario needs **`bulk/move`**? (section 7)
7. Can a heartbeat renew the session **without a PDP call**, so that a PDP outage does not expire every session? (section 8, H1)
8. Should a task that a live process **stops listing** in its heartbeats be treated as lost, instead of waiting for the execution timeout? If so, is `start` still needed? (sections 7 and 8, H2)
9. Should **`resolve`** need its own action or a per-wait token instead of `update`, and should a repeated `resolve` with the same value return success? (P5, section 7)

---

## Sources

Per-row sources are in the research notes:
- [Durable execution: Temporal, Restate, DBOS](task-engine-landscape-research/research-durable.md)
- [Orchestration platforms: Hatchet, Inngest, Trigger.dev](task-engine-landscape-research/research-platforms.md)
- [SQL-backed queues: River, Oban, Solid Queue, graphile-worker, pg-boss](task-engine-landscape-research/research-sqlqueues.md)
- [Broker and cloud queues: BullMQ, Sidekiq, SQS, Step Functions, Cloud Tasks](task-engine-landscape-research/research-queues.md)

Key pages cited in this document:
- Temporal fairness and priority: https://docs.temporal.io/develop/task-queue-priority-fairness ; https://docs.temporal.io/design-patterns/fairness
- Temporal ID reuse and conflict policies: https://docs.temporal.io/workflow-execution/workflowid-runid#workflow-id-reuse-policy
- Hatchet concurrency (`GROUP_ROUND_ROBIN`): https://docs.hatchet.run/home/concurrency
- Hatchet DAGs and `wait_for`: https://docs.hatchet.run/home/conditional-workflows
- Hatchet guarantees (duplicate execution after worker loss): https://docs.hatchet.run/v1/architecture-and-guarantees
- Inngest multi-tenancy: https://www.inngest.com/docs/durable-execution/flow-control/multi-tenancy
- Inngest `waitForEvent` (events before the wait are not seen): https://www.inngest.com/docs/features/inngest-functions/steps-workflows/wait-for-event
- Trigger.dev per-key fairness (opt-in): https://github.com/triggerdotdev/trigger.dev/pull/4367
- DBOS partitioned queues: https://docs.dbos.dev/python/tutorials/queue-tutorial
- Restate flow control: https://docs.restate.dev/services/flow-control
- pg-boss flows: https://pgboss.io
- BullMQ flows and issues: https://docs.bullmq.io/guide/flows/ ; https://github.com/taskforcesh/bullmq/issues/3120 ; https://github.com/taskforcesh/bullmq/issues/3122 ; https://github.com/taskforcesh/bullmq/issues/4855 ; https://github.com/taskforcesh/bullmq/issues/4944
- AWS Step Functions task tokens: https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html
- Sidekiq batches: https://github.com/sidekiq/sidekiq/wiki/Batches
