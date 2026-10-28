# SQL-backed job queues: feature comparison (research, 2026-10-28)

What this is: web research comparing River, Oban, Solid Queue, graphile-worker and pg-boss (pgmq briefly) against a planned Rust multi-tenant task engine that uses a relational DB as its only store.

How the research was done: official docs first. Where docs were silent, upstream source on the main/master branch was read (cited as such). "not found" means I could not verify the item in docs or source.

Support levels: Yes / Partial / No / N/A.

**Licensing summary**
| System | Open source | Paid tier |
|---|---|---|
| River | MPL-2.0 (Go; clients in Java, Ruby, Rust, TS). River UI is open source. | **River Pro** (private Go module, subscription): concurrency limits, workflows, batching, sequences, DLQ, durable periodic, encrypted, ephemeral, per-queue retention |
| Oban | Apache-2.0. Oban Web has been OSS (Apache-2.0) since 2025-01-16. | **Oban Pro** (private hex): Smart engine (global/partitioned/rate limits, unique index), Workflow, Batch, Chain, Chunk, Relay, DynamicCron, Pro Lifeline, deadlines, recorded output, signals |
| Solid Queue | MIT (Rails default since 8.0) | none |
| graphile-worker | MIT | **Worker Pro** (source-available, $100/mo): heartbeat-based crashed-worker recovery, live migration |
| pg-boss | MIT (plus dashboard and proxy packages) | none |

---

## 1. River (Go, Postgres) — riverqueue.com

| # | Item | Support | How | Source |
|---|---|---|---|---|
| 1a | Databases | Partial | PostgreSQL. SQLite is experimental. No MySQL. | https://riverqueue.com/docs , https://riverqueue.com/docs/sqlite |
| 1b | Claim mechanism | Yes | `JobGetAvailable`: selects `state='available'` rows for a queue with `scheduled_at <= now`, ordered by `priority, scheduled_at, id`, locked with `FOR UPDATE SKIP LOCKED` and limited to the free slots. The same statement then UPDATEs them to `running`, does `attempt+1`, sets `attempted_at`, and appends the client id to `attempted_by[]`. | source: https://github.com/riverqueue/river/blob/master/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql |
| 1c | Wake-up | Yes | Postgres LISTEN/NOTIFY by default, with polling as a backstop (`FetchPollInterval` default 1s, `FetchCooldown` default 100ms). A poll-only mode exists for PgBouncer transaction pooling, and Ruby always polls. | https://riverqueue.com/docs/pgbouncer ; source client.go (defaults) |
| 1d | Table layout | — | One `river_job` table, with states `available, scheduled, pending, running, retryable, completed, cancelled, discarded` as an enum. Also `river_queue`, `river_leader`, `river_client*`. Pro adds tables per migration line (workflow, sequence, dlq). | river_job.sql (source) |
| 1e | Retention/pruning | Yes | Leader-run **Cleaner** deletes completed/cancelled jobs after 24h and discarded after 7d (configurable). **Reindexer** runs `REINDEX CONCURRENTLY` on river_job indexes daily to fight B-tree bloat. **Queue cleaner** removes idle queues. Pro adds per-queue and workflow-aware retention. | https://riverqueue.com/docs/maintenance-services |
| 1f | Partitioning | No | No table partitioning documented. | not found |
| 2 | Multi-tenancy / fairness | Partial (Pro) | No tenant concept. River Pro concurrency limits partition `ByArgs` (e.g. `tenant_id`) and/or `ByKind`, giving one limit per partition. Fetch still orders by priority and `scheduled_at` and **skips** jobs that would breach a limit. No fairness or starvation policy is documented. | https://riverqueue.com/docs/pro/concurrency-limits |
| 3 | Priorities | Yes | `InsertOpts.Priority` takes 1 to 4 (1 is highest). | source client.go ("priority must be between 1 and 4") |
| 4 | Concurrency limits | Yes / Pro | `MaxWorkers` per queue per client (OSS). Pro `ConcurrencyConfig{GlobalLimit, LocalLimit, Partition{ByArgs, ByKind}}` applies across all clients, per client, and per key. "Strictly enforced during job fetching." | https://riverqueue.com/docs/pro/concurrency-limits |
| 5 | Rate limiting | No | Not shipped. The maintainers said in Apr 2025 that it would be built on concurrency limits, and users were still asking for it in 2026. Workaround: snooze plus an app-level limiter. | https://github.com/riverqueue/river/discussions/133 |
| 6a | Worker model | — | In-process Go client: goroutine per job, any number of clients per DB. "Insert-only clients" exist. Maintenance services run on an elected leader. | https://riverqueue.com/docs/leader-election , /docs/insert-only-clients |
| 6b | Heartbeats / stuck rescue | Partial | No per-job heartbeat. The leader-run **Rescuer** finds `running` jobs with `attempted_at` older than `RescueStuckJobsAfter` (default 1h, or JobTimeout + 1h). It retries them, or discards them if attempts are exhausted. This effectively caps job duration. | https://riverqueue.com/docs/maintenance-services |
| 6c | Fencing stale workers | **No** | Completion (`JobSetStateIfRunningMany`) is guarded only by `state='running'`, with no attempt or token check. A rescued and re-claimed job can therefore be finalized by the stale worker. | source river_job.sql |
| 7 | Retries / dead / manual retry | Yes | Default `MaxAttempts` is 25, backoff `attempt^4 ± 10%` seconds (about 3 weeks total). After that the job is `discarded`. `Client.JobRetry`. `JobSnooze` does not consume an attempt. Pro adds a **dead letter queue**. | https://riverqueue.com/docs/job-retries , /docs/snoozing-jobs , /docs/pro/dead-letter-queue |
| 8 | Execution timeout / queue expiry | Partial | `JobTimeout` default 1 min, overridable per worker with `Timeout()`, enforced by cancelling the context. No queue-wait expiry or deadline was found. | source client.go ; https://riverqueue.com/docs/maintenance-services |
| 9 | Scheduled / cron | Yes | `ScheduledAt` on insert. `PeriodicJobs` (cron) run in-memory on the leader. Pro "durable periodic jobs" persist schedule state. | https://riverqueue.com/docs/periodic-jobs , /docs/pro/durable-periodic-jobs |
| 10a | Workflows / DAG | Yes (Pro) | `workflow.Add(name, args, opts, &WorkflowTaskOpts{Deps: [...]})` then `Prepare` and `InsertMany`. Tasks wait in `pending` until deps succeed. | https://riverqueue.com/docs/pro/workflows |
| 10b | Deps added later? | **Yes** | `Client.WorkflowFromExisting` (in a worker) or `WorkflowFromExistingID` adds tasks to a live workflow. The docs recommend doing it in the same tx as `JobCompleteTx`. | same |
| 10c | Durable waits / signals | Yes (Pro) | `WorkflowTaskOpts.Wait` (`riverworkflow.WaitSpec`) combines **signals** (`WorkflowT.Signals().Emit`, idempotency key), **timers** (`TimerAt`, `TimerAfterWaitStarted`, …) and dependency outputs via a CEL `Expr`. This is effectively durable sleep and wait-for-event (human input). | same |
| 10d | Failure policy | Yes | Dependents of a cancelled, discarded or deleted dep are cancelled unless `IgnoreCancelledDeps`, `IgnoreDiscardedDeps` or `IgnoreDeletedDeps` is set. `WorkflowCancel`, `WorkflowT.Retry`. | same |
| 10e | Batches / sequences | Yes (Pro) | **Batching**: a leader job collects same-key jobs into one `WorkMany`. **Sequences**: strict FIFO per key (`ByArgs`), halting on cancel or discard unless `ContinueOnCancelled` or `ContinueOnDiscarded` is set. | https://riverqueue.com/docs/pro/batching , /docs/pro/sequences |
| 11 | Cancel running job | Yes (cooperative) | `JobCancel` sets `metadata.cancel_attempted_at` and `pg_notify`s the client running the job, which cancels the worker's `context.Context`. If the worker ignores ctx, the job runs to the end. A later retryable or snooze result becomes `cancelled`. | https://riverqueue.com/docs/cancelling-jobs ; river_job.sql |
| 12 | Uniqueness | Yes | `UniqueOpts{ByArgs (subset via tags), ByPeriod, ByQueue, ByState}`, enforced by a partial unique index on `unique_key` (bytea) and `unique_states` (bit(8)). A duplicate returns `UniqueSkippedAsDuplicate` plus the existing row. **No replace / latest-wins.** | https://riverqueue.com/docs/unique-jobs |
| 13 | Progress / output | Yes | `river.RecordOutput` stores up to 32MB in `metadata.output`. Job-persisted logging. **Resumable jobs** (`ResumableStep` / `ResumableStepCursor`, with checkpoints in metadata and a tx variant `ResumableSetStepTx`). | https://riverqueue.com/docs/recorded-output , /docs/resumable-jobs , /docs/job-logging |
| 14 | UI / search | Yes | River UI (OSS, Go/React) for job and queue browsing, retry, cancel and delete. Pro adds workflow views and limit overrides. | https://riverqueue.com/docs/river-ui |
| 15 | Events / telemetry | Yes | `Client.Subscribe` (job completed, failed, cancelled, snoozed, queue paused/resumed). OpenTelemetry middleware (`otelriver`) and metrics. | https://riverqueue.com/docs/subscriptions , /docs/open-telemetry , /docs/metrics |
| 16 | Throughput | — | About 46k jobs/s with `river bench` (1M no-op jobs pre-inserted, 2,000 workers, M2 MacBook Air). The docs say "not gospel". A third-party long-horizon benchmark (by the author of awa) got 501 jobs/s peak. | https://riverqueue.com/docs/benchmarks ; https://github.com/hardbyte/postgresql-job-queue-benchmarking |
| 17 | Philosophy / pitfalls | — | Transactional enqueueing is the core pitch. A single hot table, mitigated by the reindexer, cleaner and partial indexes. Unique jobs are still at-least-once. High-cardinality `ByArgs` partitions make limits ineffective. Rescue is time-based, so long jobs must raise `RescueStuckJobsAfter`. | https://riverqueue.com/docs/transactional-enqueueing , /docs/pro/concurrency-limits |

---

## 2. Oban (Elixir; Postgres / MySQL / SQLite) — oban.hexdocs.pm, oban.pro

| # | Item | Support | How | Source |
|---|---|---|---|---|
| 1a | Databases | Yes | PostgreSQL 14+ (`Oban.Engines.Basic`), MySQL 8.4+ (Dolphin engine, since v2.19), SQLite 3.37+ (`Oban.Engines.Lite`). Pro `Smart` engine runs on Postgres. | https://oban.hexdocs.pm/Oban.html |
| 1b | Claim mechanism | Yes | `fetch_jobs`: a CTE "subset" selects `available` ids for the queue `ORDER BY priority, scheduled_at, id`, `LIMIT` set to free slots, `FOR UPDATE SKIP LOCKED`. It then UPDATEs to `executing`, sets `attempted_at` and `attempted_by=[node, producer_uuid]`, does `attempt+1`, and requires `attempt < max_attempts`. | source: https://github.com/oban-bg/oban/blob/main/lib/oban/engines/basic.ex |
| 1c | Wake-up | Yes | Notifier `Oban.Notifiers.Postgres` (LISTEN/NOTIFY, default) or `Oban.Notifiers.PG` (distributed Erlang). Producers poll at `dispatch_cooldown` (5ms). A stager moves due scheduled and retryable jobs to available. | https://oban.hexdocs.pm/Oban.html |
| 1d | Table layout | — | A single `oban_jobs` table with states `available, scheduled, executing, retryable, completed, cancelled, discarded` (plus `suspended` for Pro holds). Pro adds `oban_producers` and other tables. | https://oban.hexdocs.pm/Oban.html |
| 1e | Retention/pruning | Yes | The `Pruner` (`max_age`) deletes completed, cancelled and discarded jobs. A `Reindexer` plugin exists. Pro `DynamicPruner` sets per-queue, per-state ages. | https://oban.hexdocs.pm/Oban.html |
| 1f | Partitioning | Partial (Pro, deprecated) | Pro `DynamicPartitioner` partitions `oban_jobs` by state, with finished states sub-partitioned by day: 95% less bloat and 1000x faster pruning. It does not support unique indexes and was **deprecated in Pro v1.7** ("many unexpected edge cases"). | https://oban.pro/docs/pro/Oban.Pro.Plugins.DynamicPartitioner.html ; https://oban.pro/articles/optimizing-connections-and-bloat-in-oban-pro |
| 2 | Multi-tenancy / fairness | Partial (Pro) | No tenant concept. Pro Smart engine **partitioned global limits**, e.g. `global_limit: [allowed: 1, partition: [args: :tenant_id]]`, and partitioned rate limits. Partitions with the earliest scheduled jobs are chosen first. `burst: true` lets idle capacity flow to busy partitions. `keys_cache_ttl` defaults to 3s. | https://oban.pro/docs/pro/Oban.Pro.Engines.Smart.html (v1.6.13 / 1.7.x) |
| 3 | Priorities | Yes | `priority` 0 to 9 (0 highest) within a queue. | https://oban.hexdocs.pm/Oban.html |
| 4 | Concurrency limits | Yes / Pro | Per queue per node `limit` (OSS). Pro `local_limit`, `global_limit` (cluster-wide), and partitioned by `:worker`, `args` keys or `meta` keys. The lowest of local, global and rate limit wins. You can partition by global **or** rate, not both. | same |
| 5 | Rate limiting | Yes (Pro) | `rate_limit: [allowed: 60, period: {1, :minute}, partition: ...]`. Sliding window by default, with `:fixed_window` and `:token_bucket` available. Job weights supported. Counted across nodes. | same |
| 6a | Worker model | — | In-process BEAM: one process per job, a producer per queue per node. Oban-py (Python) also exists. | https://oban.pro |
| 6b | Stuck rescue | Partial / Pro | OSS `Oban.Lifeline` rescues jobs `executing` longer than `rescue_after` (default 1h, checked every minute). It is purely time-based and may rerun live jobs. **Pro Lifeline** uses producer records (dead producer means orphan) and repairs stuck workflows. | https://oban.hexdocs.pm/Oban.Lifeline.html ; https://oban.pro/docs/pro/Oban.Pro.Lifeline.html |
| 6c | Fencing stale workers | **Yes (attempt-scoped)** | Every ack (`complete`, `discard`, `error`, `snooze`) uses `WHERE id = ? AND state IN acking_states AND attempted_at = job.attempted_at`, so a stale attempt's ack is a no-op once the job is re-claimed. | source basic.ex `ack_query` |
| 7 | Retries / dead / manual | Yes | `max_attempts` default 20. Default backoff is exponential with 15s padding and jitter, customizable with `backoff/1`. Return values `{:error,_}`, `{:cancel,_}` and `{:snooze,n}` (snooze does not consume an attempt in Pro). Exhausted jobs become `discarded`. `Oban.retry_job/2`, `retry_all_jobs`. | https://oban.hexdocs.pm/Oban.Worker.html |
| 8 | Execution timeout / expiry | Yes | `timeout/1` callback (default `:infinity`) raises `Oban.TimeoutError`, which is then retried. Pro `deadline: {1, :hour}` cancels the job if not done by the deadline after insert. `force: true` also kills a running job. | Oban.Worker ; https://oban.pro/docs/pro/Oban.Pro.Worker.html |
| 9 | Scheduled / cron | Yes | `scheduled_at` / `schedule_in`. `Oban.Plugins.Cron` (leader). Pro `DynamicCron` stores schedules in the DB and edits them at runtime. | https://oban.hexdocs.pm/Oban.html |
| 10a | Workflows / DAG | Yes (Pro) | `Workflow.new() |> Workflow.add(:b, job, deps: [:a])`, plus sub-workflows (`add_workflow`), fan-out (`add_many`), `add_cascade` (context and upstream results passed automatically), and **grafts** (`add_graft` placeholder filled at runtime via `apply_graft`). | https://oban.pro/docs/pro/Oban.Pro.Workflow.html |
| 10b | Deps added later? | **Yes** | `Workflow.append/2` adds jobs to an existing workflow with the same `workflow_id` (deps validated, `check_deps: false` skips validation). Grafts are designed for dynamic expansion. | same |
| 10c | Durable waits / signals | Yes (Pro) | `Oban.Pro.Worker.await_signal/1` (`wait_for`, `wait_timeout` default 5s) blocks briefly, then **snoozes** to free the slot. `Workflow.signal/3,4` sends signals, which are persisted so early signals are kept. Not a general durable sleep, but snooze and deadlines cover it. | same ; Oban.Pro.Worker |
| 10d | Batches / chains | Yes (Pro) | `Batch` callbacks run as their own jobs: `batch_attempted`, `batch_completed`, `batch_exhausted`, `batch_cancelled`, `batch_discarded`, `batch_retryable`. `Batch.append/2` adds jobs, but callbacks fire only once. **Chains** give strict per-partition sequencing. Chunks exist. | https://oban.pro/docs/pro/Oban.Pro.Batch.html ; Oban.Pro.Worker |
| 11 | Cancel running job | Yes | `Oban.cancel_job/2` and `cancel_all_jobs/2`. An executing job's process is **killed** across nodes via the notifier. | https://oban.hexdocs.pm/Oban.html |
| 12 | Uniqueness | Yes | `unique: [period, fields, keys, states, timestamp]`. OSS uses `pg_try_advisory_xact_lock(hash)` plus a lookup, so it is racy in some cases. Pro Smart uses a unique index (`uniq_key`) and supports bulk unique inserts. **`replace:` per state** (e.g. `replace: [scheduled: [:scheduled_at, :args]]`) gives latest-wins. | https://oban.hexdocs.pm/unique_jobs.html ; basic.ex |
| 13 | Progress / output | Yes (Pro) | Pro `recorded: true` stores the `{:ok, value}` return (64MB limit, external storage optional) and reads it back with `fetch_recorded`. `meta` is free-form. Relay awaits a job result. No built-in progress API was found. | Oban.Pro.Worker |
| 14 | UI / search | Yes | **Oban Web** (OSS since Jan 2025): filter and search jobs, retry, cancel, delete, plus crons and workflows pages. | https://elixirforum.com/t/oss-oban-web-oban-v2-19/68776 |
| 15 | Events / telemetry | Yes | `:telemetry` events for the job lifecycle (`[:oban, :job, :start, :stop, :exception]`), producer, plugin and engine. | https://oban.hexdocs.pm/Oban.html |
| 16 | Throughput | — | "One million jobs a minute": about 17.7k jobs/s on a single queue and node (M1 Pro, PG 14). The docs give single-queue throughput ≈ (1000 / cooldown) × limit. Smart engine: 92% fewer queries and 96% fewer transactions through async batched acks (≤5ms lag). | https://oban.pro/articles/one-million-jobs-a-minute-with-oban |
| 17 | Philosophy / pitfalls | — | Jobs are kept after completion for history, which causes bloat, so prune and reindex. OSS uniqueness is lock-based and racy. High-cardinality args partitions hurt. Partitioned tables are deprecated (edge cases). LISTEN/NOTIFY does not work with PgBouncer transaction mode, so use the PG notifier. | Smart engine docs ; DynamicPartitioner |

---

## 3. Solid Queue (Rails / Active Job; PG / MySQL / SQLite) — github.com/rails/solid_queue

| # | Item | Support | How | Source |
|---|---|---|---|---|
| 1a | Databases | Yes | MySQL, PostgreSQL, SQLite. `use_skip_locked: false` for MySQL<8, MariaDB<10.6 and PG<9.5. | https://github.com/rails/solid_queue |
| 1b | Claim mechanism | Yes | Workers `SELECT … FROM solid_queue_ready_executions … ORDER BY priority, job_id FOR UPDATE SKIP LOCKED`, then move the rows to `claimed_executions` with `process_id`. Only two query shapes (all queues, or a single queue) so a covering index can be used. | same |
| 1c | Wake-up | Polling only | `polling_interval` 0.1s for workers and 1s for dispatchers. No LISTEN/NOTIFY. | same |
| 1d | Table layout | **Split tables** | `solid_queue_jobs` (the record), plus execution tables `ready_executions`, `scheduled_executions`, `claimed_executions` (job_id, process_id), `blocked_executions` (concurrency_key, expires_at), `failed_executions`, `recurring_executions`, `recurring_tasks`, `semaphores` (key, value, expires_at), `pauses`, `processes` (heartbeats), `batches`, `batch_executions`. Hot polling tables stay small. | https://github.com/rails/solid_queue/blob/main/lib/generators/solid_queue/install/templates/db/queue_schema.rb |
| 1e | Retention | Yes | `preserve_finished_jobs` (default true) and `clear_finished_jobs_after` (default 1 day). The installer adds an hourly recurring clear task. Failed jobs are kept until retried or discarded. | README |
| 1f | Partitioning | No | — | not found |
| 2 | Multi-tenancy / fairness | Partial | No tenant concept. **`limits_concurrency to: N, key: ->(args){…}, duration:, group:, on_conflict: :block|:discard`** uses a semaphore row per key, claimed **at enqueue**. Blocked jobs go to `blocked_executions` and are released in priority order when the semaphore is signalled. No fairness across keys. | README |
| 3 | Priorities | Yes | Integer `priority` (lower runs first) within a queue. Queue order in the worker config takes precedence over priority. | README |
| 4 | Concurrency limits | Yes | Per worker process `threads`/`fibers`, per-queue worker assignment, and per-key semaphores (global, across all processes). | README |
| 5 | Rate limiting | No | — | not found |
| 6a | Worker model | — | A supervisor forks workers, dispatchers and a scheduler (or async/threads mode). Workers can run anywhere and only need DB access. | README |
| 6b | Heartbeats / rescue | Yes (process-level) | `process_heartbeat_interval` 60s and `process_alive_threshold` 5 min. Pruned processes' claimed jobs are **marked failed** (`ProcessPrunedError`), **not retried**, to avoid crash loops. The `fail_many_claimed.solid_queue` event lets you add custom recovery. Liveness is per process, so a hung job in a live process is never rescued. | README |
| 6c | Fencing | not found | Claims are tied to `process_id`, but no attempt or token fencing is documented. | — |
| 7 | Retries / dead / manual | Partial | No built-in retry; uses Active Job `retry_on` / `discard_on`. Failures land in `failed_executions` and can be re-run with `failed_execution.retry` / `.discard` or from Mission Control. | README |
| 8 | Timeouts | No | No per-job execution timeout ("requires an explicit timeout or watchdog"). The semaphore `duration` is only a failsafe. | README |
| 9 | Scheduled / cron | Yes | `set(wait:)` uses `scheduled_executions`, and dispatchers promote due jobs. Recurring tasks come from `config/recurring.yml` (Fugit cron), deduped by a unique index on (`task_key`, `run_at`). Dynamic tasks via `SolidQueue.schedule_recurring_task`. | README |
| 10a | Workflows / DAG | No | No dependencies. **Batches** (`SolidQueue::Batch.enqueue` with `on_finish`, `on_success`, `on_failure`, and progress counters) are a recent addition. | README |
| 10b | Durable waits | Partial | Rails 8.1 Active Job **Continuations** (`ActiveJob::Continuable`, steps and cursors) allow resumable steps. No wait-for-event. | https://github.com/rails/rails (Active Job continuations, Rails 8.1) |
| 11 | Cancel running | No | Not supported. Mission Control can discard queued or failed jobs only. | README (not mentioned) |
| 12 | Uniqueness | No (Partial) | No unique jobs. `limits_concurrency … on_conflict: :discard` approximates "only one per key in flight". | README |
| 13 | Progress / output | Partial | Batch counters and `progress_percentage` only. No job output. | README |
| 14 | UI | Yes | Mission Control – Jobs: browse queues, retry and discard failed jobs, pause queues. | https://github.com/rails/mission_control-jobs |
| 15 | Events | Partial | ActiveSupport::Notifications (`fail_many_claimed.solid_queue` is documented) and lifecycle hooks (`on_start`, `on_worker_start`, …). | README |
| 16 | Throughput | — | HEY: about 20M jobs/day, 800 workers, 4 dispatchers, 74 VMs, 2 DCs. No synthetic benchmark. | https://dev.37signals.com/solid-queue-v1-0/ |
| 17 | Philosophy / pitfalls | — | "Simplicity and performance": move rows between small execution tables so the polling table stays tiny and index-only. Polling, no NOTIFY. MySQL needs READ COMMITTED to avoid gap-lock deadlocks. Concurrency controls do not mix well with scheduling or bulk enqueue. A failed semaphore release can block a key until `duration` expires. | README |

---

## 4. graphile-worker (Node, Postgres) — worker.graphile.org

| # | Item | Support | How | Source |
|---|---|---|---|---|
| 1a | Databases | Partial | PostgreSQL only. A Rust port exists (`graphile_worker` crate). | https://worker.graphile.org/docs |
| 1b | Claim mechanism | Yes | `get_job` CTE: locks an available row in `_private_job_queues` (named queue) **and** a job row with `FOR UPDATE SKIP LOCKED`, `ORDER BY priority, run_at`, filtered by forbidden `flags`. Sets `locked_by` and `locked_at` on the job and on its queue. Batch fetch is supported via `localQueue`. | source: https://github.com/graphile/worker/blob/main/src/sql/getJobs.ts |
| 1c | Wake-up | Yes | LISTEN/NOTIFY on insert (add→start latency about 3–4ms) with polling fallback. | https://worker.graphile.org/docs |
| 1d | Table layout | — | `_private_jobs`, `_private_job_queues` (a lock row per named queue), `_private_tasks`, `_private_known_crontabs`, and a public `jobs` view. Tables are "not a public interface". | https://worker.graphile.org/docs/schema |
| 1e | Retention | — | **Completed jobs are deleted immediately**. Permanently failed jobs stay (attempts = max_attempts) until removed. For history, use a "shadow table" you maintain yourself. | https://worker.graphile.org/docs/schema , /docs/job-key |
| 1f | Partitioning | No | — | not found |
| 2 | Multi-tenancy / fairness | Partial | No tenant concept. A **named queue (`queue_name`) runs serially**, so it gives concurrency 1 per key, but the queue lock is also held for 4h if a worker crashes. **Forbidden flags** (a dynamic `forbiddenFlags` function) let you build per-key throttles. | https://worker.graphile.org/docs/forbidden-flags |
| 3 | Priorities | Yes | `priority` (lower runs first). | getJobs.ts |
| 4 | Concurrency limits | Partial | `concurrentJobs` per process, serial named queues (=1 per key). No global or per-key N>1 limits. | https://worker.graphile.org/docs/config |
| 5 | Rate limiting | Partial (external) | Not built in. Build it with forbidden flags, or use the community `graphile-worker-rate-limiter`. | /docs/forbidden-flags |
| 6a | Worker model | — | In-process Node worker pools, standalone CLI or library. | /docs/library |
| 6b | Stuck rescue | Partial | No heartbeat in OSS. Every worker sweeps every 8–10 min for jobs or queues with `locked_at < now - 4 hours` (fixed). `force_unlock_workers(ids)` is a manual fix. **Worker Pro** adds heartbeats and a configurable release timeout, with an explicit CAP warning that a net-split can cause double runs. | https://worker.graphile.org/docs/admin-functions ; /docs/pro/recovery ; source resetLockedAt.ts |
| 6c | Fencing | Partial | `fail_job(s)` is guarded by `locked_by = worker_id`. Completion deletes the row by id (one path is unguarded). `locked_by` is the pool id since 0.17. | source completeJobs.ts / failJobs.ts |
| 7 | Retries / dead / manual | Yes | `max_attempts` default 25. Backoff `exp(least(10, attempt))` seconds (max about 6h7m, about 3 days total). Permanent failure leaves the row. `reschedule_jobs`, `permanently_fail_jobs`, `complete_jobs`. No DLQ. | /docs/exponential-backoff ; /docs/admin-functions |
| 8 | Timeouts | No | No built-in execution timeout. The AbortSignal is for graceful shutdown. | not found |
| 9 | Scheduled / cron | Yes | `run_at`. Crontab file or option with **backfill**. `known_crontabs` plus job keys prevent duplicate fires across workers. | https://worker.graphile.org/docs/cron |
| 10 | Workflows / waits | No | No dependencies, DAGs or signals. "Batch jobs" means appending array payloads into one pending job via `job_key`. | /docs/job-key |
| 11 | Cancel running | No | `remove_job` on a locked job only prevents a retry. Admin functions skip locked jobs. | /docs/job-key ; /docs/admin-functions |
| 12 | Uniqueness | Yes (rich) | `job_key` with `job_key_mode`: `replace` (latest wins plus debounce, the default), `preserve_run_at` (throttle), `unsafe_dedupe`. If the matching job is **locked** (running), the old job is retired (key cleared, attempts set to max) and a new one created. Array payloads are **concatenated** (batching or debounce windows). There is a known race returning `null` under heavy contention. | https://worker.graphile.org/docs/job-key |
| 13 | Progress / output | No | No output storage (the row is deleted on success). | not found |
| 14 | UI | No (official) | None official; use the `jobs` view and SQL. | not found |
| 15 | Events | Yes | `WorkerEvents` EventEmitter: `pool:*`, `worker:*`, `job:start`, `job:success`, `job:error`, `job:failed`, `job:complete`, … | https://worker.graphile.org/docs/worker-events |
| 16 | Throughput | — | About 183–196k jobs/s executed with batching (i9-14900K, local DB, 4×24 concurrency, trivial jobs) and about 15.6k/s without batching. About 172–202k jobs/s queued. Add→start latency about 4ms avg. | https://worker.graphile.org/docs/performance |
| 17 | Philosophy / pitfalls | — | Minimal and fast: delete on completion keeps the table small. A **4-hour lock expiry** on crash, which also stalls the whole named queue. Reading the jobs table or view inside a long transaction blocks work. Trigger-based enqueue from SQL is first-class. | /docs/schema ; admin-functions |

---

## 5. pg-boss (Node, Postgres) — pgboss.io (v12.x)

| # | Item | Support | How | Source |
|---|---|---|---|---|
| 1a | Databases | Yes | PostgreSQL 13+, plus CockroachDB, Citus and PGlite. | https://pgboss.io/ |
| 1b | Claim mechanism | Yes | `SKIP LOCKED` fetch (`fetch()` / `work()` polling). | https://pgboss.io/ |
| 1c | Wake-up | Yes | Polling by default (`pollingIntervalSeconds` 2s, min 0.5). Optional LISTEN/NOTIFY (`useListenNotify` on the instance plus `notify: true` on the queue), only for immediately available jobs, with a 30s backstop poll. Does not work with PgBouncer transaction mode. | https://pgboss.io/api/workers , /api/constructor |
| 1d | Table layout | — | `pgboss.job` is `PARTITION BY LIST (name)` (since v10), states `created, retry, active, completed, cancelled, failed`. Archive table removed in v11. Schedule, queue and other tables exist. | https://github.com/timgit/pg-boss/releases/tag/10.0.0 , /tag/11.0.0 |
| 1e | Retention | Yes | `deleteAfterSeconds` (completed, default 7d), `retentionSeconds` (queued, default 14d), and a maintenance job (`maintenanceIntervalSeconds`). Warnings for `index_bloat`, `xmin_horizon` and `autovacuum_disabled`. | https://pgboss.io/api/queues , /api/constructor |
| 1f | Partitioning | Yes | List-partitioned by queue name. `partition: true` gives a queue its own table so its backlog cannot slow others. | /api/queues |
| 2 | Multi-tenancy / fairness | **Partial (best of the OSS set)** | `send(..., { group: { id, tier } })` with `work({ groupConcurrency: { default, tiers: { enterprise: 5, pro: 2 } } })` is a **global DB-enforced per-group limit** with tiering (optimistic, brief overshoot possible). `localGroupConcurrency` is per node. Ungrouped jobs are unlimited. No fairness ordering is documented. `key_strict_fifo` policy gives per-key FIFO. | https://pgboss.io/api/workers , /api/queues |
| 3 | Priorities | Yes | `priority` (higher runs first). `minPriority` / `maxPriority` on fetch. | /api/jobs |
| 4 | Concurrency limits | Yes | `localConcurrency` per node, group limits global and per node, and queue **policies**: `singleton` (1 active), `short` (1 queued), `stately`, `exclusive` (1 queued-or-active), each optionally per `singletonKey`. | /api/queues , /api/workers |
| 5 | Rate limiting | Partial | Throttling and debouncing **at enqueue** (`singletonSeconds`, `singletonNextSlot`, `sendThrottled`, `sendDebounced`). No processing-rate token bucket. | /api/jobs |
| 6a | Worker model | — | In-process Node polling workers. An HTTP **proxy** and SQL functions serve other runtimes. Supervision runs on all instances (`supervise`). | https://pgboss.io/ |
| 6b | Heartbeats / rescue | Yes | `heartbeatSeconds` (opt-in, ≥10s) with auto `touch()` from `work()`. Missed heartbeats retry or fail the job (detected within heartbeatSeconds + `monitorIntervalSeconds` 60s). Separately, `expireInSeconds` (default 900s) caps an attempt. | /api/queues |
| 6c | Fencing | **Yes (attempt-scoped)** | `complete()` / `fail()` given the fetched job "target only that attempt". If the claim lapsed and the job was retried, 0 rows are affected. A heartbeat that finds the job no longer active under this claim **aborts the handler's `signal`**. | /api/jobs , /api/workers |
| 7 | Retries / dead / manual | Yes | `retryLimit` (default 2), `retryDelay`, `retryBackoff` (`delay * 2^n` with jitter), `retryDelayMax`. `deadLetter` queue (payload copied, redrive). `retry()`, `resume()`. | /api/queues , /api/jobs |
| 8 | Timeouts / expiry | Yes | `expireInSeconds` (1..86400) aborts `job.signal`, then retry or fail. `retentionSeconds` expires never-started jobs. `transactionTimeoutSeconds` for transactional handlers. | /api/queues , /api/workers |
| 9 | Scheduled / cron | Yes | `startAfter`. `schedule(name, cron|RRULE, data, {tz})` is checked every 30s, deduped across instances with throttling, and corrected for clock skew against the DB clock. | https://pgboss.io/api/scheduling |
| 10a | Workflows / DAG | Partial | `flow(jobs)` uses `ref` and `dependsOn`, and children start `blocked` until all parents complete. **Deps only at creation time**: `send()` / `insert()` do not accept deps ("separate calls can race with job completion"). A failed or cancelled parent leaves children blocked until you cancel or fail them. A background resolver, plus `resolveFlow()`, `getDependencies()` and `getDependents()`. | /api/jobs |
| 10b | Durable waits / signals | No | Only `startAfter` deferral. No wait-for-event. | not found |
| 10c | Pub/sub | Yes | `publish` / `subscribe` fan-out to queues. | https://pgboss.io/ |
| 11 | Cancel running | Partial | `cancel()` covers pending and active jobs. A running handler learns of it only through the heartbeat-driven `signal` abort, so it is cooperative and needs heartbeats. | /api/jobs , /api/workers |
| 12 | Uniqueness | Yes | Queue policies plus `singletonKey`, and throttle windows. **No replace / latest-wins**: a duplicate `send()` returns `null` silently. | /api/queues , /api/jobs |
| 13 | Progress / output | Partial | `complete(id, data)` stores `output`, and handler return values are stored. No progress API. | /api/jobs |
| 14 | UI | Yes | `@pg-boss/dashboard`: search and filter (including inside `data` / `output` JSON), cancel, retry, resume, delete, schedules, and a read-only mode. | https://pgboss.io/dashboard |
| 15 | Events / telemetry | Yes | Built-in OpenTelemetry traces and metrics, `error` / `warning` / `wip` events, and persisted warnings (`slow_query`, `queue_backlog`, …). | https://pgboss.io/opentelemetry , /api/constructor |
| 16 | Throughput | — | No official number found. A third-party long-horizon benchmark (by the author of awa) got 2,387 jobs/s peak. | https://github.com/hardbyte/postgresql-job-queue-benchmarking |
| 17 | Philosophy / pitfalls | — | "Queue storage policies". A queue's `policy` and `partition` are immutable. `singletonKey` alone does nothing on standard queues. `send()` returns null on dedup. `schedule()` keyed by (name, key) overwrites. Ungrouped jobs bypass `groupConcurrency`. Major versions break the schema with no auto-migration from v9 or v10. | https://agledger.ai/blog/pg-boss-production-lessons/ |

---

## 6. pgmq (brief)

A Postgres extension, or plain SQL, that gives SQS-like queues: a table per queue `pgmq.q_<name>` and an archive `pgmq.a_<name>`. `read(queue, vt, qty)` hides messages for a **visibility timeout** (a lease). If they are not `delete`d or `archive`d before it expires, they reappear (`read_ct` increments). FIFO queues with message group keys are available.

It has no workers, retries/backoff policy, cron, priorities, concurrency limits or workflows, so it is a primitive rather than a job engine. PostgreSQL license.

Source: https://github.com/pgmq/pgmq

---

## Cross-cutting observations for the in-house engine

1. **Claim**: all five use `SELECT … FOR UPDATE SKIP LOCKED` plus an UPDATE to running.
   - River and Oban do it in one CTE statement on a single jobs table.
   - Solid Queue moves rows between small `ready_executions` and `claimed_executions` tables.
   - graphile-worker also locks a per-named-queue row.
   - Oban's OSS uniqueness is the only notable use of advisory locks.
2. **Wake-up**: River, Oban and graphile-worker use LISTEN/NOTIFY with a poll fallback. pg-boss makes NOTIFY opt-in and polls at 2s by default. Solid Queue polls only (100ms). Every NOTIFY user must document a PgBouncer-transaction-mode fallback.
3. **Stale-worker fencing**:
   - Oban and pg-boss fence acks per attempt: Oban on `attempted_at`, pg-boss on the attempt held by the fetched job.
   - River guards only on `state='running'`.
   - graphile-worker guards fail on `locked_by` (the pool id).
   - Solid Queue documents none.
   - Rescue is time-based: River 1h, Oban 1h, graphile 4h. The exceptions are pg-boss heartbeats, Solid Queue process heartbeats (which fail jobs rather than retry them), and Pro heartbeats in Oban Pro and Worker Pro.
4. **Per-tenant concurrency**: no system has a first-class tenant. The paid tiers (River Pro `ByArgs`, Oban Pro `partition: [args: :tenant_id]`) and pg-boss OSS (`group` + `groupConcurrency` with tiers) give global per-key limits; Solid Queue gives per-key semaphores.
   - None documents weighted or round-robin fairness. Oban Pro picks partitions by earliest scheduled job and has `burst`.
   - Only Oban Pro has true rate limiting (sliding window, fixed window, token bucket, partitionable).
5. **Workflow deps**:
   - River Pro: deps at insert time, plus `WorkflowFromExisting` to add tasks later.
   - Oban Pro: deps at insert time, plus `append/2` and runtime **grafts**.
   - pg-boss `flow()`: insert time **only**, deliberately ("races with completion").
   - graphile-worker and Solid Queue: no deps.
   - Durable wait-for-signal or timer: River Pro (signals, timers, CEL conditions) and Oban Pro (`await_signal`, which snoozes).
6. **Bloat**: the mitigations are periodic REINDEX (River, Oban), delete-on-complete (graphile-worker), tiny execution tables (Solid Queue), and list partitioning by queue (pg-boss). State/day partitioning (Oban Pro) was tried and then deprecated as too edge-case-prone.
