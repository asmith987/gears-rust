# Task Engine: architecture review and alternatives

Review of `gears/task-engine/docs/` (PRD.md, DESIGN.md, ADR-0001..0017) at `main` `6efb0f856`, 2026-10-28.

This review covers the original documents on `main`. The design-closure decisions of 2026-10-27 (branch `docs/task-engine-design-closure`, currently stashed) are **not** part of the baseline. Where one of them changes a finding, the finding says so.

Line references (`DESIGN.md:958`) point to `gears/task-engine/docs/` at `6efb0f856`.

The companion document `TASK_ENGINE_LANDSCAPE_REVIEW.md` compares the design with the market and holds the current list of proposed changes (P1–P10). This review follows its decisions: P1 (dependencies at creation only), P2 (no API-consumer parking), P3 (`SKIP LOCKED` inside the claim, PRD order kept), P5 (event waits after the `resolve` path), P6 (`authz_scope` dropped, supersession kept within `subject` scope) and P7 (re-run as a new linked task).

## Contents

- [Summary](#summary)
- [1. One gear bundles three products](#1-one-gear-bundles-three-products)
- [2. The claim protocol is home-grown](#2-the-claim-protocol-is-home-grown)
- [3. Too many exact counters](#3-too-many-exact-counters)
- [4. Three deadline mechanisms, and the timer table used as a work queue](#4-three-deadline-mechanisms-and-the-timer-table-used-as-a-work-queue)
- [5. Re-enqueue revives a terminal task](#5-re-enqueue-revives-a-terminal-task)
- [6. Event waits put a broker consumer and a copy of the PDP checks in the core](#6-event-waits-put-a-broker-consumer-and-a-copy-of-the-pdp-checks-in-the-core)
- [7. Cross-node wake-up polls per waiter group](#7-cross-node-wake-up-polls-per-waiter-group)
- [8. Database portability is expensive](#8-database-portability-is-expensive)
- [9. Correctness is specified in prose and pseudo-code](#9-correctness-is-specified-in-prose-and-pseudo-code)
- [10. Dependencies and waits can be added after creation](#10-dependencies-and-waits-can-be-added-after-creation)
- [Recommended order](#recommended-order)
- [Appendix A: race and state-machine findings](#appendix-a-race-and-state-machine-findings)
- [Appendix B: reference systems](#appendix-b-reference-systems)

## Summary

The core choices are sound and should stay: the live / history table split with ring-partition truncation (ADR-0009), the single terminal path `finalize()`, the global lock order with counters before task rows, holder-bound lease tokens with an epoch (ADR-0011), worker-session heartbeats (ADR-0017), and the transactional outbox.

The problems are in the size and shape of what is built around them. Most of the race and state-machine bugs found in this review (Appendix A) come from features interacting, not from any single mechanism being wrong.

| # | Issue | Verdict | Recommended alternative |
|---|---|---|---|
| 1 | One gear bundles a job queue, a workflow engine and a policy layer | Holds | Queue core + feature modules on explicit hooks |
| 2 | Claims wait on each other's task rows instead of skipping them (`SKIP LOCKED`) | Holds, narrower: ADR-0008:108 already asks for `SKIP LOCKED` by default, but the ADR and DESIGN contradict it | Keep counter-first order and PRD dispatch order; add `SKIP LOCKED` candidate selection; make ADR-0008 and DESIGN §4.6 consistent (landscape P3) |
| 3 | Too many exact counters | Holds (stronger: on by default) | Sampled per-tenant depth limits; exact, unsharded concurrency counters |
| 4 | Three deadline mechanisms; timer table used as a work queue | Holds (savings depend on config) | One `next_due_at` column; `te_timers` only for wait members; separate cascade work table |
| 5 | Re-enqueue revives a terminal task | Holds (smaller impact) | No reviving; re-run as a new linked task, added after p1 (landscape P7) |
| 6 | Event waits put a broker consumer and PDP-check copy in the core | Holds | One resolve path in the core; event waits as an adapter, gated on the broker (landscape P5) |
| 7 | Cross-node wake-up polls per waiter group | Holds (pub/sub already noted as future work) | Scoped / negatively cached probe + reserved heartbeat permits; `LISTEN/NOTIFY` on PostgreSQL |
| 8 | Database portability is expensive | Weaker | PostgreSQL first, or MySQL without the partition ring |
| 9 | Correctness specified in prose and pseudo-code | Holds | Concurrency tests on real databases + a small formal model of the core |
| 10 | Dependencies and waits can be added after creation; create-then-gate races with claim | New (verified against `main`) | Predecessors at creation only; optional initial wait group on `POST /tasks`; no API-consumer park, optional admin hold of queued work later (landscape P1, P2) |

---

## 1. One gear bundles three products

**Verdict: holds.**

### What the design does

The Task Engine is three systems sharing one state machine and one transaction model:

- **Job queue**: create, claim, lease, heartbeat, retry with backoff, timeouts, concurrency limits, dead-lettering.
- **Workflow engine**: four wait kinds (timer, event, input, children), parent/child with cancel propagation, predecessor dependencies with `ALL`/`ANY` modes and cascades, supersession.
- **Policy layer**: caller idempotency keys and derived dedup, queue-depth limits, rate limiting (PRD §5.21), worker blacklist, priority aging, trust limits, creator authority checks.

The PRD has 87 functional-requirement sections. Every feature is implemented inside the same transactions as every other: a terminal transition (`finalize()`, DESIGN.md:2919) releases counters, deletes timers and waits, releases dedup identities, flushes progress, cascades to successors and children, schedules a children check for the parent, and writes history and outbox rows.

The boundary with Durable Execution is drawn by **who runs the code** (PRD.md:71, 216: the Task Engine runs no task code; in-process workflows are Durable Execution's job), not by **which features each gear owns**.

### Why it is a problem

Each feature is reasonable on its own. The bugs are at the seams, and the number of seams grows with the square of the feature count. Examples found in this review (Appendix A):

- cancel request × requeue paths: a pending graceful cancel is lost when the task is released, parked, retried or requeued after lease loss (A1);
- cascades × the timer loop: the cascade continuation timer is skipped because its task is terminal (A2);
- ANY-mode dependencies × dependency add: a task with a satisfied predecessor is blocked again (A6);
- runtime limit changes × counter rows (A4).

Every new feature must be checked against every existing one, in every transaction. Reviews of 7,400 lines of prose (issue 9) do not reliably catch this.

### Alternatives

**1a. Status quo, plus an interaction matrix.** Keep one gear; add a documented feature × transition matrix (rows: transitions; columns: cancel request, supersession, armed wait, dependencies, children, counters, dedup) and require every cell to be specified and tested.
- Good: no redesign; the matrix alone would have surfaced A1, A2 and A6.
- Bad: the coupling stays; the matrix grows with every feature.

**1b. Queue core plus feature modules (recommended).** Split the code (not necessarily the gear) into:
- a **core** that owns the state machine, claim, lease, retry, timeouts, concurrency counters and `finalize()`;
- **modules** (dependencies, children, waits, supersession, dedup, aging) that act on the core only through a small set of hooks, for example:
  - `admit(task) -> Hold | Ready` at create (dependencies return `Hold`, which is `blocked`);
  - `on_terminal(task, outcome)` after `finalize()` (cascades, children checks, dedup release);
  - `resume(task, payload)` (wait resolution, unblocking);
  - `may_requeue(task) -> bool` (cancel requests, supersession).

  The core calls hooks in a fixed order inside its transaction. A module never writes `te_task_state` directly.
- Good: interactions become explicit hook contracts and can be tested per hook; a p1 release can ship the core plus only the modules it needs.
- Good: the gear and its API do not change; this is an internal structure, so it reverses no ADR.
- Bad: hook design takes effort up front; some features (supersession, which cancels another task at create) touch two tasks and need care.

**1c. Separate orchestration gear on top of a queue gear.** Move waits, dependencies and children into a second gear that uses the queue gear's public API.
- Good: the cleanest separation (as Temporal separates its matching service from its history service).
- Bad: one task's state would be owned by two gears with no shared transaction. "Unblock on predecessor completion" and "resume on event" would become cross-service sagas. That is a large loss of the atomicity the design relies on.

**1d. Move workflow features to Durable Execution.** Not a fit. Durable Execution is now an embeddable library for **local** Rust activities; its PRD §4.2 excludes remote handlers, arbitrary DAGs and nested workflows (`libs/toolkit-durable-execution/docs/PRD.md`). Task Engine workers are remote processes, and dependencies form DAGs.

### Recommendation

1b. Write the hook contracts into DESIGN §3.2 before Phase 1, and make p1 = core + the modules p1 needs. Keep 1a's matrix as the test plan for the hooks.

### Decisions needed

- Which modules are p1 (the closure branch's ARCH-04 decision already tags many features p2/p3; reuse it).
- Whether the boundary with Durable Execution should be restated in PRD §1.1 by feature, not only by where code runs.

---

## 2. The claim protocol is home-grown

**Verdict: holds. Easier to change than first stated: the PRD does not require the strict ordering that blocks the standard alternative.**

### What the design does

ADR-0008 (status `proposed`) chooses Option 5, the **ToolKit secure claim primitive with counter-first guarded updates** (`toolkit_db::secure::claim`, ToolKit issues #5477, #5478, #5483). In one `READ COMMITTED` transaction:

1. **Candidates**: plain keyset reads per queue in claim order, no locks; k-way merge across queues by `(urgency, created_at, task_id)`.
2. **Reserve**: `UPDATE counter SET used = used + k WHERE used + k <= limit` on every applicable counter row, sorted; locks held to commit.
3. **Win**: per pick, `UPDATE task SET state = claimed … WHERE id = ? AND state = queued`; 0 rows → next candidate.
4. **Settle**: return unused reservations; outbox; renew the worker session; commit.

Plus, in the gear: batch claims, node-local coalescing of waiters, a node saturation cache. Gear-level `SELECT … FOR UPDATE SKIP LOCKED` (Option 1) is rejected because it locks task rows before counters and puts backend behaviour in the gear.

**The documents disagree about `SKIP LOCKED` inside the ToolKit primitive:**
- ADR-0008:108 says the Task Engine asks `toolkit-db` (#5478) to select candidates with `FOR UPDATE SKIP LOCKED` on PostgreSQL/MySQL **by default**, as the outbox already does, with SQLite keeping plain guarded updates.
- ADR-0008:129 says "the benchmark decides whether the ToolKit enables `SKIP LOCKED` internally", and ADR-0008:98 describes the convoy that happens "without `SKIP LOCKED`".
- DESIGN §4.6 (DESIGN.md:2397, 2508, 2525) describes plain candidate reads, claims that "wait for the winner's commit", and `SKIP LOCKED` as an optional internal ToolKit strategy.

So the ADR's own direction (line 108) is already close to alternative 2b below; the gear-level pseudo-code and the rest of the text were not updated to match.

### Why it is a problem

- **Head-of-queue convoy.** Every claimer reads the same head rows, because a plain read skips nothing. They then try to update the same row. Under `READ COMMITTED`, losers wait for the winner's row lock, then match 0 rows and move on, while still holding the counter rows reserved in step 2. On a queue with a concurrency limit, claims effectively run one at a time through the counter row. Coalescing, batching and counter shards reduce the effect; they do not remove it.
- **No production precedent.** None of the common SQL-backed job systems claim this way, so the protocol's behaviour under load is known only once the ADR-0008 benchmark runs. The implementation phase plan in the review notes (`~/notes/task-engine-open-questions.md`, not in the repository) rates it the top risk (Phase 2 could double).
- **The stated reason is stronger than the requirement.** ADR-0008:46 lists "exact claim order among claimable rows (PRD §5.2)" as a driver. PRD §5.2 (PRD.md:390) only fixes the dispatch order: urgency, then tenant fairness, then priority, then age and task ID. It says nothing about strict order between concurrent claimers. PRD §5.6 goes further and asks for skip semantics explicitly (PRD.md:653): "A task that another claim is taking at that moment is skipped; the claim takes the best task that no other claim holds." The DESIGN pseudo-code instead makes the losing claim wait on the row lock, so it does not meet that sentence; `SKIP LOCKED` does.

### Alternatives

**2a. Gear-level `SKIP LOCKED` (ADR-0008 Option 1).** `SELECT … ORDER BY … LIMIT k FOR UPDATE SKIP LOCKED`, then reserve counters, then update.
- Good: the standard pattern (River, Oban, graphile-worker, pgmq, Solid Queue); claimers spread over the head; supported on PostgreSQL, MySQL 8.0+ and MariaDB 10.6+.
- Bad: inverts the lock order (task rows before counters). A cancel or bulk move of a queued task takes counters first and then the task row. A claim holding that task row and waiting for the same counter row deadlocks with it. Fixable, but it breaks the engine-wide ordering rule that the rest of the design depends on.
- Bad: a full counter is found only after rows are locked (no write, but wasted locking).

**2b. Counter-first plus `SKIP LOCKED` candidate selection (recommended).** Keep Option 5's order: plan with plain reads, reserve counters, then select the picks with `FOR UPDATE SKIP LOCKED` (restricted to the planned scopes) and update them.
- Good: keeps the lock order, the "no task write for a full scope" rule and the portable contract; removes the convoy, because claimers never wait on each other's task rows.
- Good: ADR-0008:108 already requests this as the ToolKit default on PostgreSQL and MySQL, with the plain-read path kept for SQLite (single writer, no contention). What remains is to make ADR-0008:129 and DESIGN §4.6 agree with it, and to specify how a skipped planned pick is replaced within the reserved scopes.
- Bad: dispatch order is exact only among rows not locked by another in-flight claim. A claim that locks a row and then rolls back releases it to the next claim. This matches PRD §5.2 as written but needs ADR-0008:46 reworded.
- Bad: one more backend-specific strategy inside `toolkit-db` (which already uses `SKIP LOCKED` for the outbox).

**2c. Dispatcher per queue shard.** Reuse the sharded ownership of the background loops (#5482): the owner of a queue shard claims batches from the database and hands tasks to parked waiters in memory (as Temporal's matching service does).
- Good: one writer per queue shard, no head contention, exact order; concurrency counters could be checked in memory first, with the database row as the durable guard.
- Bad: failover gap when ownership moves; one node's throughput per queue shard (still far above the 500 claims/s NFR).
- Bad: waiters with different PDP scopes must be matched in memory against each task's tenant, type and queue. That is the hard part for tenant-plane workers, and it moves authorization logic out of the secure ORM.

**2d. Candidate spreading (ADR-0008 Option 4).** Each claimer starts at a random row in the head page. Cheap and portable, but order is relaxed by up to `k − 1` tasks, which does contradict PRD §5.2.

**2e. Narrow ready table (combinable with 2b).** Keep `queued` tasks in a small dispatch table (task ID, queue, tenant, urgency, priority, created_at) and claim from it, as Solid Queue does with its ready-executions table. Claims then lock and delete rows in a narrow table instead of updating the wide, heavily indexed `te_task_state`.
- Good: less index churn and vacuum load on the hottest table (see DESIGN.md:1540 *Live table maintenance*).
- Bad: one more table to keep consistent on every transition into and out of `queued`.

### Recommendation

2b as the default on PostgreSQL and MySQL/MariaDB, as ADR-0008:108 already states. Make ADR-0008:129 and DESIGN §4.6 match it (the pseudo-code should show the locking candidate read after the counter reservation), and reword ADR-0008:46 to "exact among rows not locked by another claim". Keep 2c as the fallback if 2b misses the claim-latency NFR; measure 2e in the same benchmark.

This changes only how a claim picks its candidates. The PRD dispatch order (urgency, tenant round-robin, priority, age), the order across several queues (PRD §5.6, the k-way merge), counters reserved before task rows, batch claims and long-poll stay as designed.

### Decisions needed

- Accept "order among unlocked rows" as the meaning of PRD §5.2 (a clarification, not a PRD change).
- Resolve the ADR-0008:108 vs ADR-0008:129 contradiction in favour of `SKIP LOCKED` by default, and confirm it in the scope of ToolKit #5478.

---

## 3. Too many exact counters

**Verdict: holds, and stronger than first stated: the per-tenant counters are on by default.**

### What the design does

Four counter tables: `te_concurrency_slots`, `te_tenant_concurrency_slots`, `te_tenant_queue_backlog`, `te_tenant_wait_counters`. Around them:

- **Hot-row sharding**: `effective_shards = min(counter_shards, max(1, limit / 32))`, `counter_shards` default 8 (DESIGN.md:1671).
- **Recorded shard columns** on every task (`queue_slot_shard`, `type_slot_shard`, `tenant_queue_slot_shard`, `tenant_type_slot_shard`, `backlog_shard`), so a release decrements exactly the shard it reserved.
- **Draining shards** when the shard count changes.
- **Lock-then-count reconciliation** of every counter row (DESIGN.md:1633).
- **Unlimited = no row**; when a limit changes from 0 to N, maintenance creates rows "from a recount before enforcement starts" (DESIGN.md:1667).

Queue-wide and domain-wide depth limits are already **sampled ceilings** without counter rows (DESIGN.md:1669). Per-tenant depth limits (`queue_tenant`, `tenant_deferred`) stay **exact**, and their defaults are non-zero: `tenant_max_queue_depth: 100000` and `tenant_max_deferred_tasks: 100000` (DESIGN.md:2177–2179). So by default **every create and every claim writes a per-tenant backlog row** (8 shards each), on top of the concurrency counters.

### Why it is a problem

- **Write cost.** Backlog counters are written on create (+1), claim (−k and settle), every transition between `queued` and the deferred set, and every terminal transition. That is part of the 25–40 row writes per task (DESIGN.md:2049).
- **Machinery and bugs.** The shard columns, draining and reconciliation are a lot of code to get right. The 0 → N cutover is unspecified: nodes are not told when "enforcement starts", and tasks claimed while the limit was 0 carry no shard, so a sharded recount filtered by shard misses them and their release decrements nothing (Appendix A4).
- **Exactness buys little for depth limits.** A depth limit protects capacity from one tenant flooding a queue. Overshooting it by a few seconds of admissions does not hurt anyone, especially since per-tenant create rate limits already exist (`per_tenant_create_rps: 100`, DESIGN.md:2238). Concurrency limits are different: they protect downstream systems and must be exact.

### Alternatives

**3a. Sampled per-tenant depth limits (recommended).** Treat `queue_tenant` and `tenant_deferred` like the queue-wide limits: the maintenance leader counts per tenant (index-only counts on `te_task_state`, only for tenants above a threshold), and admission refuses while the latest sample is at or above the limit.
- Good: removes `te_tenant_queue_backlog` writes from create, claim and every deferred-set transition; removes `backlog_shard` and its reconciliation.
- Good: the overshoot is bounded by the tenant's create rate limit × the sampling interval (for example 100/s × 5 s = 500 tasks against a 100,000 limit).
- Bad: per-tenant counting cost moves to the maintenance loop; with 100K tenants it must count only tenants with significant backlog (a `GROUP BY owner_tenant_id … HAVING count > threshold` on the dispatch index, per queue).
- Bad: changes a PRD acceptance criterion if one demands exactness for per-tenant depth (check PRD §5.2 Queue Depth Limits, PRD.md:434).

**3b. Keep exact depth counters, default them to 0 (off).** Only tenants or queues that opt in get rows.
- Good: smallest change; out-of-the-box writes disappear.
- Bad: the protection is off by default; all the machinery stays.

**3c. Unsharded concurrency counters by default.** Keep exact concurrency counters (they are needed), but set `counter_shards = 1` until a benchmark shows a hot row.
- Good: removes the recorded-shard and draining logic from the common path; the counter-first protocol already serializes claims per row only for the length of one short transaction.
- Bad: if a single large global limit does turn out to be hot, sharding has to come back (keep the design, just not the default).

**3d. Semaphore plus blocked set (Solid Queue style).** A semaphore row per concurrency key, and tasks that cannot get a slot move to a "blocked by concurrency" set that is released when a slot frees.
- Good: well-tested model for per-key concurrency.
- Bad: adds a state and a release path; overlaps with the existing counter design without removing it. Not recommended here; listed for comparison.

**Why not count live rows instead of keeping counters?** Counting `claimed`/`running` tasks of a scope inside each claim is race-prone: two claims both count 9 under a limit of 10 and both claim. Fixing that needs a lock per scope, which is a counter row in disguise.

### Fix needed whatever is chosen

Specify the 0 → N cutover: a limit state per scope (`counting` → `enforcing`); rows created and recounted by **scope**, not by shard, while in `counting`; tasks with no recorded shard released against shard 0 or recounted; nodes switch to enforcing only after the recount commits (they learn it on the runtime-config poll).

### Recommendation

3a for per-tenant depth limits, 3c for concurrency counters, plus the cutover fix.

### Decisions needed

- Whether per-tenant depth limits may be soft (bounded overshoot).
- Note: the closure branch's CONC-02 decision (lazy per-tenant concurrency rows on first claim) makes the cutover race worse and should be revisited together with this item.

---

## 4. Three deadline mechanisms, and the timer table used as a work queue

**Verdict: holds. The write savings depend on which timeouts are configured.**

### What the design does

ADR-0003 (Option 4) uses three mechanisms:

| Mechanism | Deadlines | Loop |
|---|---|---|
| `te_timers` rows | `scheduled` (`run_after`), `queue`, `lifetime`, `sla`, `wait`, `cancel_grace` | `tickTimers` claims due rows (#5477), `procTimers` workers act on them |
| Columns on `te_task_state` | `assignment_deadline`, `execution_deadline`, `handoff_until` | `deadlineScan` (DESIGN.md:801) |
| `te_worker_sessions.lease_until` | heartbeat | `sessionScan` (DESIGN.md:800) |

In addition, `te_timers` carries **work items** that are not deadlines: `cascade` timers (continue a large cascade, `cascade_after_task_id` cursor) and `children_check` timers (one per finishing child) (DESIGN.md:1808, 2956). And `ix_te_task_state_run_after` (DESIGN.md:1530) indexes `run_after` although the `scheduled` timer row already drives the transition.

The creation rows of the transition table say "set lifetime timer" and, for `queued`, "set queue timeout timer" (DESIGN.md:523–525). Both apply only when the timeout is configured: the shipped `default` queue sets `queue_timeout_secs` and `lifetime_timeout_secs` to `null` (`schemas/instances/…default.v1.json`), so tasks on it get neither row. Where a queue or type does set them, each row is inserted and later deleted.

ADR-0003 considered **Option 5, due-time columns** (as in River or Oban `scheduled_at`) and rejected it "for now" because it "needs one partial index per deadline type on the hottest table" and "per-wait-condition timeouts do not map to one column per task".

### Why it is a problem

- **Writes.** Up to 4 timer-row writes per task (insert and delete of the queue and lifetime timers) on queues that configure those timeouts, plus `te_timers` index churn, for deadlines that are a pure function of the task's own fields. On the shipped `default` queue the saving is zero for these two, so the case for 4a there rests on the other points.
- **Misuse causes bugs.** `procTimers` skips any timer whose task is terminal (DESIGN.md:2731). Cascade timers always belong to a terminal task, so large cascades never continue (A2). The cascade cursor skips dependents that were deferred (A3). Children checks create up to 1,000 timer rows for one parent.
- **Three loops to reason about**, each with its own sharding, claim and recovery rules.

### Alternatives

**4a. One `next_due_at` column (recommended), answering ADR-0003's objection.** Store each fixed per-task deadline as a plain, **unindexed** column on `te_task_state` (`queue_deadline`, `lifetime_deadline`, `sla_deadline`, `run_after`, `cancel_grace_until`, plus the existing `assignment_deadline`, `execution_deadline`, `handoff_until`). Add one indexed column `next_due_at = min(applicable deadlines)`, recomputed in every transition (which already rewrites the row), and one index `(next_due_at, bg_slot)`. One sharded scan reads due rows, works out which deadline fired from the plain columns, applies it through the normal guarded transition, and recomputes `next_due_at`.
- Good: one index instead of one per deadline type (the objection in ADR-0003); no timer rows for fixed deadlines; `deadlineScan` and the timer wheel for task deadlines merge into one loop.
- Good: an SLA breach (non-terminal) just advances `next_due_at` to the next deadline.
- Bad: `next_due_at` changes on most transitions, so it is one more indexed column updated; but those transitions already change `state_code` and are not HOT updates anyway.
- Bad: a migration of ADR-0003 and the §4.6 Timer Wheel algorithm.

**4b. Keep `te_timers` only for things a task can have many of.** Wait-member timeouts and timer-kind wait conditions (one row per member) stay as timer rows, as ADR-0003 itself notes. Combined with 4a, `te_timers` becomes a small, wait-only table.

**4c. A separate internal work table for cascades and children checks.** `te_work(kind, source_task_id, cursor, …)`, claimed with the same due-row primitive, never subject to the "skip terminal tasks" rule; one row per parent for children checks (upsert), not one per child.
- Variant: run them as engine-owned tasks on the `system` queue. Elegant, but it makes engine correctness depend on the queue it implements; not recommended for p1.

**4d. Status quo with targeted fixes.** Exempt `cascade` and `children_check` from the terminal skip, fix the cascade cursor, deduplicate children checks per parent, drop the redundant `run_after` index.
- Good: smallest change.
- Bad: keeps the write cost and the three mechanisms.

### Recommendation

4a + 4b + 4c. If the ADR-0003 rewrite is too much before Phase 3, do 4d now and 4a later; ADR-0003 already calls Option 5 "rejected for now".

### Decisions needed

- Reopen ADR-0003 Option 5 with the single `next_due_at` index.

---

## 5. Re-enqueue revives a terminal task

**Verdict: holds, with smaller impact than first stated.**

### What the design does

An administrator may re-enqueue a `failed` or `dead_lettered` task (DESIGN.md:545). In one transaction the engine moves the row from `te_task_results` back to `te_task_state`, restores the original queue, resets retry budgets, clears the result, re-takes the dedup identity, checks supersession, runs queue-depth admission, re-arms the lifetime timer from now and the SLA timer if still in the future, and emits `taskQueued { reason: reenqueued }`. This is the only exception to "terminal states are final" (ADR-0002).

The PRD already decides that re-enqueue does **not** revive successors (PRD.md:839).

### Why it is a problem

- **The one exception costs a lot.** Moving a row back out of history touches dedup (`expires_at` reset), the ring fence for old buckets (ADR-0009), lifetime re-arming, and the `te_task_results` partitions.
- **Event consumers** see `taskFailed` / `taskDeadLettered` followed by `taskQueued` for the same task ID. Any consumer that treats terminal events as final (archivers, billing, dashboards) is wrong unless it knows the exception.
- **Children are still open.** A failed child can be re-enqueued after its parent has already completed, so a live child exists under a completed parent; `wait_for_children` and `CHILDREN_PENDING` assumed that cannot happen.
- Successors not being revived is decided behaviour, so it is not a bug, but it is a consequence users will need explained.

### Alternatives

**5a. Minimal rules on the current model.**
- Refuse re-enqueue of a task whose parent is terminal (`failed_precondition`), or define it explicitly.
- Document in PRD §5.10 that `taskQueued { reason: reenqueued }` reopens a terminal task, so consumers must not treat terminal events as final.
- Good: small, no redesign.
- Bad: keeps the exception and its cost.

**5b. Re-run as a new linked task (recommended).** Re-run creates a new task (new ID) copying type, queue, input and context, with `rerun_of = old_id`; the old task stays terminal with `rerun_as = new_id`. The dedup identity moves to the new task in the same transaction.
- Good: terminal is always final; no rows move out of history; no ring-fence or timer re-arming special cases; event consumers need no exception; the old run's history stays intact.
- Bad: the task ID changes; callers that track a task by ID must follow the link (the API can return the new ID; `GET` on the old one shows the link).
- Bad: parent/child and dependency links point at the old ID; re-enqueuing a child or predecessor would need to re-point links or be refused.

**5c. Runs inside a task.** The task ID stays; each re-enqueue opens a new **run** (`run_number`), and terminal states belong to runs (as Temporal does with workflow runs).
- Good: stable ID and final runs.
- Bad: a large model change (every table and event gains a run dimension); not justified by this issue alone.

### Recommendation

5b, added after p1 together with DLQ routing (landscape P7 and MVP scope). p1 has retries and the `failed` outcome, and no way to revive a finished task. Use 5a only if re-enqueue must ship in p1 on the current model.

### Decisions needed

- Re-run of a child under a terminal parent: refuse, or allow and define (the new task would be a live child of a finished parent).
- Whether `bulk/retry` and `admin/tasks/{id}/reenqueue` merge into one bulk re-run (landscape section 7).

---

## 6. Event waits put a broker consumer and a copy of the PDP checks in the core

**Verdict: holds.**

### What the design does

To support `event` wait conditions the engine runs an Event Broker consumer over every operator-listed "waitable" topic, as a root-tenant consuming subject with barriers ignored. On set or arm it repeats the broker's subscribe checks with the caller's context (`consume` on the topic, event type and subject type), keeps the decidable PDP constraints as a stored `consume_filter`, and refuses constraints it cannot evaluate (DESIGN.md:827–831). Each candidate event is matched by indexed probe, then filtered, then evaluated with a budgeted CEL expression (ADR-0007). Exactly-once is achieved with an inbox per task domain and the consumer offset committed last (DESIGN.md:925). Event conditions get a maximum lifetime because the authorization check is only done once.

The design records three open broker dependencies: #5492 (barrier mode on publish), #5493 (tenant-subtree delivery; until then no tenant's event wait can resolve) and #5494 (the broker's own consume checks and a shared filter function).

### Why it is a problem

- **Security-sensitive copy.** The engine reads events with platform rights and must reproduce another service's authorization rules exactly. Any drift between the copy and the broker becomes a disclosure path.
- **Core complexity.** Consumer lifecycle, inbox per domain, offset ordering across databases, CEL budgets, per-tenant wait counters and their reconciliation all sit in the same gear as the claim path.
- **Blocked.** Without #5493 the feature does not work at all for tenant tasks.

### Alternatives

**6a. One resolve path in the core; event waits as an adapter (recommended).** The core keeps only waits it can resolve from its own API: timer, input (`resolve` with a payload), children. Event waits become an adapter module (same binary or a separate gear) that consumes events and calls the same internal `resolve` used by input waits.
- Good: the core has one resolution path; the adapter can be shipped, disabled or replaced without touching the claim or terminal paths; p1 does not depend on #5492–#5494.
- Bad: if the adapter runs as a separate gear, set-wait and resolve are no longer in one transaction with the event offset; the inbox moves to the adapter. In the same binary, this stays as today.
- Note: this moves the authorization problem rather than removing it; the adapter still consumes for all tenants.

**6b. Producer resolves.** For events that a known gear emits in response to a task's own action (the main use case: "call a service, wait for its reply event"), the producing gear, or the worker itself, calls `resolve` with the correlation (task ID or a resource reference).
- Good: no broker consumer, no PDP copy; the caller's own rights apply.
- Bad: only works when the producer knows the task; it does not cover "wait for any event of type X about resource Y".
- Bad: does not fit approvals. The PRD routes approval gates through an event wait on the `approval-service` gear (PRD.md:233), and the approval service should not depend on the Task Engine. So event waits are a firm p2 commitment, not optional (landscape P5).
- Needs two fixes to `resolve` (landscape P5): today it requires the `update` action, which also allows tag and dependency changes (DESIGN.md:986), so give it its own action or a per-wait token; and specify that a repeated `resolve` with the same value returns success, so a replying service can retry safely.

**6c. Broker-provided filter.** Keep the design, but replace the copied rules with the shared SDK function #5494 asks for, which returns a storable, matchable filter.
- Good: no copy of the rules; the design otherwise unchanged.
- Bad: still a platform-rights consumer in the core; still blocked on the broker.

**6d. Per-wait broker subscriptions.** Each wait subscribes with the caller's identity. Rejected by the design for good reason: rebalances on every arm/park and unbounded subscription counts.

### Recommendation

6a + 6b: input and timer waits in p1; document "producer resolves" as the recommended pattern for request/reply; event waits as an adapter after #5493/#5494, using 6c inside the adapter when it exists.

### Decisions needed

- Whether event waits are p1 at all (the closure branch's EVT-05 already gates them on #5493).

---

## 7. Cross-node wake-up polls per waiter group

**Verdict: holds. Cluster pub/sub is already listed as optional future work (DESIGN.md:3043).**

### What the design does

Long-poll claims park without a database connection. Same-node work wakes them through in-process signals. For work on other nodes, each node runs a **queue watcher**: for each queue with parked waiters it runs an existence probe `SELECT … WHERE queue_id = ? AND state_code = queued LIMIT 1` every `claim_poll_interval_ms` (1,000 ms, jittered), and on a hit wakes one claim per waiter group (DESIGN.md:958). A waiter group is (domain, subject, compiled scope, queue set). The design says a queue whose queued tasks are blocked only by capacity "keeps matching the probe so its waiters retry each tick". The node saturation cache lasts `claim_saturation_ttl_ms` = 250 ms (DESIGN.md:2136).

Claims use the execution admission class, the same class as heartbeats (DESIGN.md:2032).

### Why it is a problem

- **The probe is not scoped.** A tenant-plane worker group is woken by any tenant's queued task, then runs a scoped claim that finds nothing for it.
- **Capacity-blocked queues cause retries every tick.** With one group per tenant-plane subject, every group on every node runs a full claim transaction every second while the queue is saturated. The saturation cache expires before the next tick, so it does not help.
- **Feedback loop.** Those claims take execution-class permits. Under pressure, heartbeats compete with them and can be shed; a few shed heartbeats expire a whole worker session (all its tasks, ADR-0017); the mass requeue adds more load.

### Alternatives

**7a. Scoped or negatively cached probe per group (recommended).** Keep polling, but probe per waiter group with the group's scope (tenant and type constraints), or cache a negative result per group until a local signal or a `slotReleased` arrives. Extend the saturation cache to at least one tick.
- Good: removes futile claims; no new infrastructure; works on every backend.
- Bad: more probes when there are many groups (still cheap index probes, and no claim transaction).

**7b. Reserved heartbeat permits (recommended, independent of the rest).** Give heartbeats their own small permit pool (or a class that claims cannot use), so claim storms can never shed heartbeats.

**7c. PostgreSQL `LISTEN/NOTIFY` as an accelerator.** Transitions into `queued` run `NOTIFY te_queue_<queue>` inside their transaction (delivered on commit); each node keeps one listening connection and raises local signals. Polling remains the fallback and the only mechanism on MySQL and SQLite. (River, Oban and graphile-worker use this pattern.)
- Good: near-instant cross-node wake-up; polling can slow down.
- Bad: PostgreSQL only; `NOTIFY` adds a little commit cost; payloads must not carry tenant data.

**7d. Cluster pub/sub.** Already in DESIGN §4.8 as optional future work. Portable across databases, but adds a dependency the design deliberately keeps optional.

**7e. Node dispatcher for platform workers.** One claim loop per (node, queue) fills a small local buffer and hands tasks to parked platform-plane waiters (all share one scope). Tenant-plane waiters keep per-group claims. Pairs with 2c.

### Recommendation

7a and 7b in the p1 design; 7c as a PostgreSQL accelerator once Phase 2 benchmarks show cross-node latency matters.

### Decisions needed

- Admission classes: whether heartbeats get a reserved pool.

---

## 8. Database portability is expensive

**Verdict: weaker than first stated.** The original documents support three backends, not four: PostgreSQL ("recommended for production"), MariaDB/MySQL as one backend, and SQLite for development, tests and small single-node use (PRD.md:175). Treating MySQL and MariaDB as two separate engines, with performance gates on PostgreSQL only, comes from the closure branch's TEST-04 decision.

### What the design does

MySQL/MariaDB is a full production backend. The design therefore specifies, for MySQL:
- the partition ring and `ALTER TABLE … TRUNCATE PARTITION` through ToolKit #5481 (DESIGN.md:845, 858);
- a MySQL form for every partial index (DESIGN.md:1933);
- `READ COMMITTED` set explicitly for claims and reconciliation, because MySQL's default `REPEATABLE READ` snapshot would hide committed rows;
- InnoDB key-length limits, purge-lag monitoring instead of autovacuum tuning;
- no foreign keys (also for PostgreSQL partitioning reasons).

### Why it is a problem

- Every concurrency-sensitive path (claim, counters, reconciliation, locking reads) must be designed, reasoned about and tested twice, with different locking behaviour (InnoDB gap and next-key locks, secondary-index locking).
- The partition ring is the most complex storage feature, and MySQL partitioning has its own constraints (the partition key must be part of every unique key).

### Alternatives

**8a. PostgreSQL first.** p1 production support on PostgreSQL only; SQLite for development; MySQL added in a later phase.
- Good: the largest single reduction in Phase 1–5 work and risk.
- Bad: blocks any deployment that can only run MySQL.

**8b. MySQL without the partition ring.** MySQL is supported functionally, but uses the SQLite retention path (bounded `DELETE` by `ring_slot`) instead of partitions.
- Good: removes the hardest MySQL-specific feature; keeps MySQL available.
- Bad: retention on large MySQL deployments costs more I/O and purge work; fine for small and medium installations.

**8c. Full parity (status quo).** Every feature on every production backend.

### Recommendation

8a if no p1 deployment needs MySQL; otherwise 8b.

### Decisions needed

- Is there a p1 deployment that requires MySQL or MariaDB? (The closure branch decided full support for both; this item would revisit TEST-04.)

---

## 9. Correctness is specified in prose and pseudo-code

**Verdict: holds.**

### What the design does

PRD, DESIGN and ADRs total 7,406 lines. The state machine is a Mermaid diagram plus a transition table; concurrency safety is argued in prose (lock order, counter reconciliation, deadlock freedom); algorithms are pseudo-code. ADR confirmation sections list tests to write later.

### Why it is a problem

The bugs in Appendix A survived several reviews of this text. They are interaction and interleaving bugs, which prose review is bad at finding. The diagram and the table also disagree (A12), so there is no single authoritative transition matrix.

### Alternatives

**9a. Randomized concurrency tests against real databases (recommended, required).** A test harness runs many concurrent actors (claimers, completers, cancellers, releasers, timer and session scans, config changes, process kills) against real PostgreSQL and MySQL, then checks invariants against the tables:
- every counter's `used` equals a recount of the tasks charged to it;
- every task is in exactly one state table, with live-only rows only for live tasks;
- no task claimed twice under one epoch; no lost claims;
- a cancel request never leads to `queued`;
- every `blocked` task is eventually resolved once its predecessors end.

This is the only method that covers isolation levels, locking reads and backend differences. It directly serves the Phase 2 exit criterion (no deadlocks, no lost or duplicate claims).

**9b. A small formal model of the core (recommended).** Model the states, leases and sessions, one counter, cancel modes, cascades, timers and limit changes, with each transaction as one atomic step, at small bounds (2–3 tasks, 2 workers, limit 1).
- **Quint or TLA+** (TLC, Apalache): proper liveness checking with fairness, needed for "every blocked task is eventually resolved" and cascade progress.
- **stateright** (Rust): same language as the team and the SDK enums; good for safety invariants; weak liveness checking (no fairness).
- Effort: about 1–2 engineer-weeks for the core. Most of the value comes from writing it: translating the prose into actions surfaces contradictions such as A1, A2 and A6.
- Limitation: it checks the model, not the SQL; it does not replace 9a.

**9c. One executable transition table.** Make the transition table the single source: a Rust table of (from, to, trigger, guard, side effects) from which the guarded updates are built and tests are generated (every transition succeeds from its source state and is rejected from every other). Generate the Mermaid diagram from it.

**9d. Deterministic simulation for node-level behaviour.** Use a simulation framework (for example `turmoil` or `madsim`) for shard ownership changes, signals, shutdown and long-poll behaviour, where real-time tests are flaky.

### Recommendation

9a (required before Phase 2 exit) + 9b for the core before Phase 2 starts + 9c during Phase 1.

---

## 10. Dependencies and waits can be added after creation

**Verdict: new issue, verified against `main`.**

### What the design does

- `POST /tasks` accepts `predecessors` (and `parent_task_id`); the task starts `blocked` in the create transaction. It does **not** accept an initial wait group (PRD.md:261).
- `POST /tasks/{id}/dependencies` and `DELETE /tasks/{id}/dependencies/{predecessor_id}` change dependencies later, on tasks in `queued`, `scheduled` or `blocked` (PRD §5.9 "Changes and cycles", PRD.md:841; DESIGN.md:2891). Adding takes the per-tenant graph lock and walks the graph for cycles (§4.6 Dependency Graph Validation); it may move `queued`/`scheduled` → `blocked` as a deferred admission (`429` when full, PRD.md:452).
- `POST /tasks/{id}/wait-conditions` is called by the worker under its lease, or by an API consumer with `update` and no token on a `queued`, `claimed` or `running` task, which "revokes any lease" (DESIGN.md:532; PRD.md:550).

The PRD gives no use case for changing dependencies after creation; its rationale (PRD §5.9) only justifies the cycle check. No §8 use case needs it.

### Why it is a problem

**1. Create-then-gate is a race with claim.** A task created without a gate is `queued` and claimable at once; a parked long-poll worker on the same node is woken by the `newTask` signal within milliseconds. The follow-up call then races the claim:

| Second call arrives when the task is | Add dependency | API-consumer set wait (park) |
|---|---|---|
| still `queued` | Works: `queued → blocked` (or `429` if the deferred limit is full, and the task runs ungated) | Works: `queued → waiting` (or `429`, task runs ungated) |
| `claimed` | Rejected `400` (state); the task runs ungated | Accepted: the worker's lease is revoked right after it got the task; it gets `LEASE_LOST` at `start` |
| `running` | Rejected `400`; the task runs ungated | Accepted: the lease is revoked mid-execution; side effects already started may run again after resume (at-least-once, but caused by the API, not by a failure) |
| terminal | Rejected; the gate never applied | Rejected; the gate never applied |

The database stays consistent in every case (guarded updates and locking reads decide one winner); the problem is that the caller's intent ("do not run this before X") is lost or turned into a forced interruption, nondeterministically.

**2. There is no safe workaround for waits today.** For dependencies, a caller can pass `predecessors` at create, or create with a future `run_after` (`scheduled`) and then add the dependency. For waits, parking is allowed only from `queued`, `claimed` or `running`, not from `scheduled` or `blocked`, so there is no state in which a new task can be held while a wait is attached. The only race-free pattern is a separate gate task used as a predecessor.

**3. The add-later path is the most expensive part of the dependency feature.** Create-time predecessors only point at tasks that already exist, so they cannot form a cycle. The add-later path is what requires the per-tenant graph lock (`te_dependency_locks`, first in the lock order), the cycle walk of up to 10,000 tasks, the concurrent-cycle guarantee, the `queued`/`scheduled → blocked` transitions with deferred admission, the fixed-mode rules, and review finding A6.

### How other engines handle it

No engine surveyed lets an outside caller add a dependency edge between two existing tasks. They fall into two groups:

- **Fixed graph, declared at definition or submission**: Hatchet DAGs ("every task, along with the dependencies between them, is declared upfront, before the workflow runs"; "the structure of a DAG is fixed when you define it"), Argo Workflows (`dependencies` / `depends` per DAG task), Airflow (DAG code; dynamic task mapping only expands the instance count of a fixed task), Prefect (`submit(wait_for=[futures])`, futures of tasks already submitted), AWS Step Functions (state machine definition).
- **Growth from inside, by the running parent**: Temporal (start child workflows, then await them; ordering is workflow code), Hatchet durable tasks (spawn children, then wait), BullMQ (the parent's processor adds children with `opts.parent`, then `moveToWaitingChildren` with its lock token), Sidekiq Pro batches (jobs can be added "only by another job within that batch"; calling `batch.jobs` again from outside is "not safe").

The add-from-inside path has a record of bugs in practice: BullMQ issues #3120 (parent never moves to waiting-children), #3122 (a failed runtime child leaves the parent stuck), #4855 (re-adding a failed child leaves the parent waiting forever), #4944 (a re-added flow releases the parent before its new children run).

Waits declared before a task starts are also common: Hatchet `wait_for` (sleep and user-event conditions on the task definition, combinable with parent conditions), Step Functions `.waitForTaskToken` in the state definition, Temporal `signalWithStart` (start and signal in one call).

This comparison is from public documentation and issue trackers, not from running these systems.

### Alternatives

**10a. Predecessors at creation only (recommended).** Remove `POST /tasks/{id}/dependencies`. Dynamic fan-in uses parent/child plus the `children` wait, which the engine already has (the same pattern as Temporal, Hatchet durable tasks, BullMQ and Sidekiq).
- Good: the graph is append-only and acyclic by construction; the graph lock, cycle walk, write-skew argument and `queued`/`scheduled → blocked` transitions go away; A6 disappears.
- Bad: a caller who discovers a dependency after creating the successor must create the successor later, or cancel it and create it again. Changes PRD §5.9.

**10b. Replace `DELETE` with an admin "resolve edge" operation.** Marks one pending edge satisfied (or failed), evaluated by the normal ALL/ANY rules. Removing or resolving an edge cannot create a cycle, so it needs no graph lock.

**10c. Optional initial wait group on `POST /tasks` (recommended).** The task starts in `waiting` in the create transaction. It reuses the set-wait path: validation, the event-wait permission checks before the transaction, per-tenant wait-counter admission, deferred-limit admission.
- Good: removes the create-then-park race and the need for gate tasks; matches Hatchet `wait_for` and Step Functions.
- Bad: one more create-path branch; the first claim after resolution must not bump `attempt` (set `resume_pending`).

**10d. Remove the API-consumer wait path (recommended).** Only the worker that holds the task sets waits after creation (including arm-then-park); a creator who wants a wait declares it on `POST /tasks` (10c). If operators need to hold work during an incident or maintenance, add an admin hold of **queued** work per queue or per tenant later, as its own feature. A leased task is never taken from its worker into a wait; it is interrupted only by cancel. This resolves review finding A8.
- Good: the PRD gives no use case for an API consumer setting a wait (its rationale describes only the worker case); removing the path removes lease revocation mid-run and the `update`-without-token branch.
- Bad: changes PRD §5.4; an operator hold, if needed, is a new feature.
- Earlier version of this item: allow API-consumer park only on unleased tasks. Superseded by landscape P2, because with 10c in place no remaining case needs a per-task park by someone else.

**10e. Keep add-later, but make it safe.** Keep the endpoints and document that the gate must be attached before the task is claimable (create `scheduled` with a future `run_after`, add the gate, then let it run), plus allow parking from `scheduled`.
- Good: no PRD change.
- Bad: keeps all of the cost in point 3 and leaves a footgun in the API.

### Recommendation

10a + 10b + 10c + 10d. Bring back add-later edges only if a first-adopter gear shows a case that create-time predecessors and parent/child cannot cover, and an API-consumer wait only if one shows a case beyond holding queued work.

### Decisions needed

- Drop dependency changes after creation from PRD §5.9 (keep create-time predecessors and an admin resolve).
- Add an initial wait group to the create request (PRD §5.1, §5.4).
- Remove the API-consumer wait path (PRD §5.4, DESIGN §3.1); decide later whether an admin hold of queued work is needed.

### Sources

[Hatchet DAGs](https://docs.hatchet.run/home/conditional-workflows), [Hatchet durable tasks](https://docs.hatchet.run/v1/durable-tasks), [Temporal child workflows](https://docs.temporal.io/design-patterns/child-workflows), [Temporal message passing](https://docs.temporal.io/develop/python/message-passing), [Argo DAG](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/), [Airflow dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html), [Prefect tasks](https://docs.prefect.io/v3/concepts/tasks), [Step Functions integration patterns](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html), [BullMQ flows](https://docs.bullmq.io/guide/flows/), [BullMQ process step jobs](https://docs.bullmq.io/patterns/process-step-jobs), BullMQ issues [#3120](https://github.com/taskforcesh/bullmq/issues/3120), [#3122](https://github.com/taskforcesh/bullmq/issues/3122), [#4855](https://github.com/taskforcesh/bullmq/issues/4855), [#4944](https://github.com/taskforcesh/bullmq/issues/4944), [Sidekiq batches](https://github.com/sidekiq/sidekiq/wiki/Batches).

---

## Recommended order

1. **Issue 2, claim protocol** (2b): shortens the critical path and removes the top risk. Needs a ToolKit decision on #5478.
2. **Issue 3, counters** (3a + 3c + cutover fix): fewer hot-row writes on every create and claim by default.
3. **Issue 4, deadlines** (4a–4c, or 4d first): fewer writes per task and removes the cascade bugs at their source.
4. **Issue 9, verification** (9a, 9b, 9c): start in Phase 1; independent of the other decisions.
5. **Issue 7, wake-up** (7a, 7b): small design changes; do them with issue 2.
6. **Issue 10, create-time gates** (10a–10d): an API decision; settle it before Phase 1 fixes the create request, since it removes the graph lock and cycle walk from the dependency work.
7. **Issue 1, core and modules**: define the hooks before Phase 1 code exists; decide the p1 module set.
8. **Issue 6, event waits**: follows from issue 1; gate on the broker.
9. **Issue 5, re-enqueue** (5b, after p1; no reviving in p1).
10. **Issue 8, portability**: a product question; decide before Phase 5 (partition ring).

Items that would change accepted or closure-branch decisions: ADR-0003 (issue 4), ADR-0008 wording (issue 2), CONC-02 (issue 3), TEST-04 (issue 8), EVT-05 (issue 6), PRD §5.9 dependency changes and §5.4 API-consumer park (issue 10), PRD §5.8 re-enqueue (issue 5).

---

## Appendix A: race and state-machine findings

Found in the same review, verified against `6efb0f856`. Referenced from the issues above.

| ID | Finding | Evidence | Notes |
|---|---|---|---|
| A1 | A pending cancel request is lost on requeue. Release, park, retryable failure and lease-loss requeue all leave `cancel_requested` set and the task back in the queue; the `cancel_grace` timer carries an expected state, so after the requeue it is skipped and the graceful escalation never happens | DESIGN.md:546, 551, 732, 1811 | The closure branch (CNCL-02) covers lease loss, assignment timeout and retryable failure, but not release or park |
| A2 | Cascades beyond `cascade_inline_limit` never continue: `procTimers` skips timers of terminal tasks, and a cascade timer always belongs to the terminal source task | DESIGN.md:2731, 1801 | |
| A3 | Dependents skipped inline ("changed after the plan") are lost: the durable batch resumes after `cascade_after_task_id = last processed id`, and a timer is created only "if more dependents remain" | DESIGN.md:2952, 2977–2979 | |
| A4 | Enabling a concurrency limit at runtime: the cutover to enforcement is unspecified, and tasks claimed while the limit was 0 have no recorded shard, so a sharded recount misses them and their release decrements nothing | DESIGN.md:1633, 1667 | Worse with closure-branch CONC-02 (lazy rows) |
| A5 | An expired `te_dedup` row still blocks creates until retention deletes it; after the holder's partition is truncated the caller gets `403` for its own key | DESIGN.md:2586, 1728–1731 | |
| A6 | Adding a dependency to an ANY-mode task that already has a satisfied predecessor blocks it again (`if any unmet: s → blocked`) | DESIGN.md:2904 | |
| A7 | ADR-0017 promises `LEASE_LOST` right after session expiry, but worker writes are fenced on token, subject and state only; between "mark expired" and "requeue" a completion still succeeds | ADR/0017:80, DESIGN.md:734 | Harmless outcome; fix the test wording |
| A8 | An API consumer can park a `running` or `claimed` task, revoking a live worker's lease mid-execution; `queued` can be parked but `scheduled` and `blocked` cannot | DESIGN.md:532, 813 | Removed with the API-consumer wait path (10d, landscape P2) |
| A9 | A failed child can be re-enqueued under a completed parent | DESIGN.md:545 | Successors not reviving is decided (PRD.md:839); with 5b the same question applies to re-runs |
| A10 | Graceful supersession of a leased task lets the old and new task run at the same time for up to the grace period | PRD.md:328, DESIGN.md:552 | Product decision; supersession is kept, limited to `subject` scope (landscape P6) |
| A11 | Poison-pill tasks: a task that crashes its worker process counts as a liveness failure for every task in the session; innocent tasks burn their liveness budget (queue default 20) and the worker gets a blacklist strike | PRD.md:786, DESIGN.md:2201 | Consider crash attribution or quarantine |
| A12 | The state diagram omits transitions in the table: liveness requeue from `claimed`/`running`, `claimed` → `failed`/`dead_lettered`, release with delay from `claimed`, lifetime timeouts from `scheduled`/`blocked`/`waiting` | DESIGN.md state diagram (39 edges) vs transition table | See 9c |
| A13 | Queue-watcher claim storm (issue 7) | DESIGN.md:958, 2136, 2138 | |
| A14 | `fifo` queues with per-tenant limits can starve: at most `claim_scan_rows_max` = 1024 index rows are read per claim, so if they all belong to saturated tenants, eligible tasks further down are never reached | DESIGN.md:2132, 2443 | |
| A15 | The children check counts `te_task_results` by `parent_task_id`, which that table does not index; the lookup goes through `ix_te_tasks_parent` per ring partition (about 17 probes plus `_retained`); one `children_check` timer per finishing child (up to 1,000 per parent) | DESIGN.md:3003, 1472, 1558 | Moderate cost |
| A16 | `ix_te_task_state_run_after` duplicates the `scheduled` timer on the hottest table | DESIGN.md:1530 | See issue 4 |
| A17 | A worker session's lease never grows back: after one short-lease task, the process heartbeats at the short interval for its lifetime | ADR/0017:56 | Minor |
| A18 | ADR-0017 front matter says `status: accepted`; DESIGN §1.2 lists it as **Proposed** | ADR/0017:2, DESIGN.md:116 | Doc inconsistency |
| A19 | ADR-0008:108 asks the ToolKit for `SKIP LOCKED` candidate selection by default on PostgreSQL/MySQL; ADR-0008:129 says the benchmark decides; DESIGN §4.6 shows plain candidate reads and head-row waits | ADR/0008:98, 108, 129; DESIGN.md:2397, 2508, 2525 | Doc inconsistency (issue 2) |
| A20 | PRD §5.6 requires that a task another claim is taking "is skipped"; the DESIGN claim makes the loser wait on the row lock | PRD.md:653; DESIGN.md:2508 | Supports issue 2 |
| A21 | `finalize()` cascades to children only when the parent ends `cancelled`; a parent that ends failed, dead-lettered or timed out leaves its children live | DESIGN.md:2955 | Broader than A9; the PRD requires propagation only on cancel, but the docs never say this is intended |
| A22 | `on_predecessor_terminal` moves a satisfied successor `blocked → queued`, ignoring `run_after`; the transition table says `scheduled` when `run_after` is in the future | DESIGN.md:2913 vs 527 | |
| A23 | `finalize()` step 5 exempts "a cascade timer created in this transaction", but that timer is inserted later, in step 8 | DESIGN.md:2942 vs 2978 | Ordering wording |
| A24 | `limit_value = 0` means both "unlimited" (admission guard `limit_value = 0 OR used < limit_value`) and "draining shard, skip it", so an admission can land on a draining backlog shard | DESIGN.md:1677 vs 1645 | Use a separate draining flag |
| A25 | tickTimers claims only `Pending` rows; nothing re-claims `Claimed` timers whose `claim_until` passed (crashed processor), although ADR-0003 and `ix_te_timers_claim_until` promise it | DESIGN.md:2714–2723; ADR/0003:56 | Fixed on the closure branch (TIMER-01) |
| A26 | Cause precedence (an execution timeout wins over liveness) is applied only in deadlineScan; sessionScan requeues a session's tasks without that check, so the outcome depends on which scan reaches the task first | DESIGN.md:2755–2759 vs 2770 | |

## Appendix C: further gaps reported while building the companion pages

Found by the page-building passes and listed on the companion pages as G-items. Not yet verified line by line in this review; check before acting on them.

| Page | Reported gaps |
|---|---|
| How a Task Ends | Nested `finalize()` in step 8 would lock counters after task rows (lock-order break); child cancel mode unspecified; re-enqueued predecessor of a blocked ANY-mode successor leaves an ambiguous edge; re-enqueue does not re-derive `bg_slot` and shard columns; `CHILDREN_PENDING` and the children check use different indexes |
| Claim Protocol | Round-robin "most urgent class" probe not marked as scoped; a parked claim's result on node shutdown unspecified; ADR-0011 still describes heartbeats as guarded writes (pre-ADR-0017); minor guard-wording and `counter_shards` formula mismatches |
| Storage and Retention | Retention step numbers differ between DESIGN §3.2 and ADR-0009, and DESIGN cross-references point at the wrong steps; `te_ring_state` never returns to `active`; lock-order list omits `te_dedup` and `te_ring_state`; no per-domain ring setting although DESIGN recommends one; supersede-head lifetime from creation (DESIGN) vs terminal time (PRD); `cleanup_time` daily vs paced tick |
| Timer Wheel | Timer claim lease, batch size and poll interval have no config keys; timer-kind wait conditions have no procTimers case and `ix_te_waits_timeout` is unused; backoff text ("first retry waits base") contradicts full jitter; blacklist coalescing assumes a large session requeues within one lease window; scan interval uses the whole 1 s accuracy budget; ADR-0003 tests 10K timers vs a 100K NFR; `waitTimedOut` published only on the fail path |
| Task Lifecycle Transitions | `cancel_grace` expected state unspecified (a `start` can make it stale); worker complete/fail allowed from `claimed` by the fence but not by the table; `resume_pending` also set by release; events missing for liveness requeue; lease-token table omits API-consumer park of a `claimed` task |

## Appendix B: reference systems

| System | Relevant pattern |
|---|---|
| River (Go, PostgreSQL) | `FOR UPDATE SKIP LOCKED` claims; `scheduled_at` column instead of timer rows; `LISTEN/NOTIFY` wake-up |
| Oban (Elixir, PostgreSQL) | `SKIP LOCKED` claims; `scheduled_at`; PostgreSQL notifications between nodes |
| graphile-worker (Node, PostgreSQL) | `SKIP LOCKED`; `run_at` column; `LISTEN/NOTIFY` |
| pgmq (PostgreSQL extension) | `SKIP LOCKED` with a visibility timeout (SQS-style) |
| Solid Queue (Rails; PostgreSQL, MySQL 8, SQLite) | Narrow ready / scheduled / claimed execution tables; `SKIP LOCKED`; semaphore rows plus a blocked set for concurrency limits |
| Temporal | Matching service (task queues) separate from history service (workflow state); workflow runs inside one workflow ID; signals |
| AWS Step Functions | `waitForTaskToken` callback pattern (the producer resolves) |
