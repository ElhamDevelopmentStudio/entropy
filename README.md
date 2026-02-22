# HDCF MVP (Go + SQLite)

This repository contains an implementation of the feasibility plan from `mvp.md`:

- Pull-based control plane (`cmd/control`)
- Worker daemon (`cmd/worker`)
- Simple CLI to enqueue jobs (`cmd/hdcfctl`)
- Durable state in SQLite (`jobs.db`)
- Heartbeat + stale-worker recovery

## Quick start

### 1) Start control plane on Mac

```bash
go run ./cmd/control -addr :8080 -db ./jobs.db -admin-token dev-token -worker-token dev-token
```

Environment variables available:

- `HDCF_ADDR` (default `:8080`)
- `HDCF_DB_PATH` (default `jobs.db`)
- `HDCF_API_TOKEN` (legacy shared token, default `dev-token`)
- `HDCF_ADMIN_TOKEN` (admin token; overrides legacy token)
- `HDCF_ADMIN_TOKEN_PREV` (previous admin token)
- `HDCF_WORKER_TOKEN` (worker token; overrides legacy token)
- `HDCF_WORKER_TOKEN_PREV` (previous worker token)
- `HDCF_WORKER_TOKEN_SECRET` (optional signing secret for worker tokens)
- `HDCF_WORKER_TOKEN_TTL_SECONDS` (default `3600`)
- `HDCF_HEARTBEAT_TIMEOUT_SECONDS` (default `60`)
- `HDCF_RECONCILE_INTERVAL_SECONDS` (default `10`)
- `HDCF_CLEANUP_INTERVAL_SECONDS` (default `300`)
- `HDCF_QUEUE_AGING_WINDOW_SECONDS` (default `0`)
- `HDCF_MAX_RETRY_CONCURRENCY_PER_WORKER` (default `0`)
- `HDCF_PREEMPT_HIGH_PRIORITY_BACKLOG_THRESHOLD` (default `0`)
- `HDCF_PREEMPT_HIGH_PRIORITY_FLOOR` (default `0`)
- `HDCF_JOBS_RETENTION_COMPLETED_DAYS` (default `30`)
- `HDCF_ARTIFACTS_RETENTION_DAYS` (default `14`)
- `HDCF_EVENTS_RETENTION_DAYS` (default `30`)
- `HDCF_TLS_CERT` (optional path for HTTPS cert)
- `HDCF_TLS_KEY` (optional path for HTTPS key)
- `HDCF_TLS_CLIENT_CA` (optional CA cert for worker client auth)

Control-plane cleanup controls:
- `-cleanup-interval-seconds` (how often retention sweeps run)
- `-jobs-retention-completed-days` (how long terminal jobs stay in sqlite)
- `-artifacts-retention-days` (how long terminal artifact paths are eligible for on-disk cleanup)
- `-events-retention-days` (how long audit events are retained)
- `-tls-cert`, `-tls-key` (start HTTPS control plane)
- `-tls-client-ca` (optional CA for optional client cert validation)
- `-tls-require-client-cert` (require and verify worker client certs)

Scheduling controls:
- `-queue-aging-window-seconds` (fairness age bonus window in seconds)
- `-max-retry-concurrency-per-worker` (max concurrent retry jobs per worker; `0` means unlimited)
- `-preempt-high-priority-backlog-threshold` (gates lower-priority jobs when high-priority backlog is high)
- `-preempt-high-priority-floor` (priority threshold used during preemption mode)

### 2) Start worker on ASUS

```bash
go run ./cmd/worker -control-url https://<mac-ip>:8080 -worker-token dev-token -log-dir ./worker-logs
```

Worker options:

- `-control-url` (default `http://localhost:8080`)
- `-worker-id` (default `hostname-<generated>`)
- `-token` (legacy fallback token, default `dev-token`)
- `-worker-token` (defaults to `-token`)
- `-worker-token-secret` (optional HMAC secret for signed token mode)
- `-worker-token-ttl-seconds` (default `3600`)
- `-tls-ca` (ca bundle for control-plane cert)
- `-tls-client-cert` (client cert for mTLS)
- `-tls-client-key` (client key for mTLS)
- `-capabilities` (default ``, comma-separated worker capabilities, e.g. `gpu,ssd`)
- `-poll-interval-seconds` (default `3`)
- `-heartbeat-interval-seconds` (default `5`)
- `-request-timeout-seconds` (default `10`)
- `-log-dir` (default `worker-logs`)
- `-log-retention-days` (default `30`)
- `-log-cleanup-interval-seconds` (default `300`)
- `-heartbeat-metrics` (default `false`) — include optional resource metrics in heartbeat payload
- `-artifact-storage-backend` (default `local`) — artifact destination strategy (`local`, `nfs`, `s3`)
- `-artifact-storage-location` (required for `nfs` backend) — storage path for non-local artifact backends
- `-command-allowlist` (default `false`) — enable command allowlist enforcement
- `-allowed-commands` (default ``) — comma-separated command allowlist when `-command-allowlist` is on
- `-allowed-working-dirs` (default ``) — comma-separated list of allowed working directories
- `-require-non-root` (default `false`) — reject jobs while running as root
- `-dry-run` (default `false`) — validate policy and write simulated artifacts without executing jobs

Environment variables:

- `HDCF_WORKER_CAPABILITIES` (comma-separated capabilities)
- `HDCF_WORKER_NONCE`
- `HDCF_CONTROL_URL`
- `HDCF_WORKER_ID`
- `HDCF_WORKER_LOG_RETENTION_DAYS` (default `30`)
- `HDCF_WORKER_LOG_CLEANUP_INTERVAL_SECONDS` (default `300`)
- `HDCF_WORKER_COMMAND_ALLOWLIST` (`true` to enforce allowlist mode)
- `HDCF_WORKER_ALLOWED_COMMANDS` (comma-separated allowed command binaries)
- `HDCF_WORKER_ALLOWED_WORKING_DIRS` (comma-separated working directory allowlist)
- `HDCF_WORKER_REQUIRE_NON_ROOT` (`true` to block root execution)
- `HDCF_WORKER_DRY_RUN` (`true` for simulated execution)
- `HDCF_ARTIFACT_STORAGE_BACKEND` (default `local`; values: `local`, `nfs`, `s3`)
- `HDCF_ARTIFACT_STORAGE_LOCATION` (required for `nfs` backend)
- `HDCF_TLS_CA` (ca bundle for control-plane cert)
- `HDCF_TLS_CLIENT_CERT` (client cert for mTLS)
- `HDCF_TLS_CLIENT_KEY` (client key for mTLS)
- `HDCF_WORKER_TOKEN` (worker token; defaults to `HDCF_API_TOKEN`)
- `HDCF_WORKER_TOKEN_SECRET` (HMAC secret for signed token mode)
- `HDCF_WORKER_TOKEN_TTL_SECONDS` (default `3600`)

Security note:

- When `-command-allowlist` is enabled, the worker only executes commands whose binary name or full command path matches one of `-allowed-commands` (or `HDCF_WORKER_ALLOWED_COMMANDS`).
- When `-allowed-working-dirs` is set, `working_dir` must exist and be under one of the allowed directories.
- If `-dry-run` is enabled, no external processes are spawned; jobs are validated and completed as zero exit jobs with recorded dry-run artifacts.

## Security model

- Admin endpoints (`/jobs`, `/jobs/{id}`, `/abort`, `/workers`, `/events`) require admin credentials.
- Worker endpoints (`/register`, `/next-job`, `/ack`, `/heartbeat`, `/reconnect`, `/complete`, `/fail`) require worker credentials.
- If `-worker-token-secret` is set on both sides, workers use short-lived signed token format `v1.<payload>.<sig>`.
- Use TLS (`-tls-cert`/`-tls-key`) for HTTPS and consider mTLS with `-tls-client-ca` + `-tls-require-client-cert`.

Retention and cleanup behavior:
- Control plane runs periodic retention sweeps on `-cleanup-interval-seconds`.
- Control plane sweeps can:
  - delete terminal jobs (`COMPLETED`, `FAILED`, `ABORTED`) older than `-jobs-retention-completed-days`;
  - delete terminal job artifact file paths older than `-artifacts-retention-days`;
  - delete audit events older than `-events-retention-days`;
  - emit cleanup audit events (`control.cleanup*`) with counts and deletion outcomes.
- Worker retains log/artifact files locally and periodically removes old artifacts via `log-retention-days`.

### 3) Submit and inspect jobs with `hdcfctl`

```bash
go run ./cmd/hdcfctl submit \
  --url http://<mac-ip>:8080 \
  --token dev-token \
  --command sleep \
  --args "2" \
  --priority 10
```

The CLI sends `POST /jobs` and returns the new `job_id`.

Optional scheduling fields:
- `--priority` (higher value = higher queue priority)
- `--scheduled-at` (unix seconds timestamp when job becomes eligible; `0` means now)

List jobs:

```bash
go run ./cmd/hdcfctl jobs list --status=PENDING
```

Show one job:

```bash
go run ./cmd/hdcfctl jobs describe <job_id>
```

List workers:

```bash
go run ./cmd/hdcfctl workers list
```

Abort a job:

```bash
go run ./cmd/hdcfctl abort --job-id <job_id> --reason "no longer needed"
```

Invoke reconnect replay manually:

```bash
go run ./cmd/hdcfctl replay --worker-id worker-1
go run ./cmd/hdcfctl replay --worker-id worker-1 --completed-jobs-file /tmp/reconnect.json
```

Example `reconnect.json` payload:

```json
[
  {
    "job_id": "00000000-0000-0000-0000-000000000000",
    "assignment_id": "assign-123",
    "completion_seq": 1,
    "artifact_id": "a1",
    "status": "COMPLETED",
    "exit_code": 0,
    "stdout_path": "/tmp/stdout.txt",
    "stderr_path": "/tmp/stderr.txt",
    "result_summary": "manual replay"
  }
]
```

Advanced scheduling fields are accepted directly by the API as part of `POST /jobs`:
- `needs_gpu` (`true`/`false`)
- `requirements` (string array)
- `worker_capabilities` (on `POST /register`, via worker startup registration)

## Implemented endpoints

- `POST /jobs`
- `POST /register`
- `GET /next-job?worker_id=...`
- `POST /ack`
- `POST /heartbeat`
- `POST /reconnect`
- `POST /abort`
- `GET /jobs` (optional `status` and `worker_id` query filters)
- `GET /jobs/{job_id}`
- `GET /workers`
- `POST /complete`
- `POST /fail`
- `GET /events` (optional filters: `component`, `event`, `worker_id`, `job_id`, `since_id`, `limit`)  
  `since_id` returns only events with `id > since_id` for incremental polling.
- `POST /reconnect` supports optional `current_job_id` and completed-job replay payload, and can now be manually invoked via `hdcfctl replay`.

Queue ordering behavior:
- `GET /next-job` claims from `PENDING` jobs by descending `priority`, then ascending `created_at`, then ascending `job_id`.
- Optional scheduling controls:
  - `-queue-aging-window-seconds` boosts older low-priority work over time to improve fairness.
  - `-preempt-high-priority-backlog-threshold` and `-preempt-high-priority-floor` can temporarily prioritize higher-priority backlog.
  - `-max-retry-concurrency-per-worker` limits retry job assignment pressure per worker.
- Jobs with `scheduled_at` in the future are not claimed until their scheduled time arrives.

Read/observability behavior:

- `GET /jobs` returns job list entries with state, timestamps, attempt counters, worker assignment, and heartbeat age for jobs with active workers.
- `GET /jobs/{job_id}` returns a single job detail payload with the same fields.
- `GET /workers` returns worker rows with heartbeat age in seconds and optional latest `heartbeat_metrics`.
- `GET /events` returns durable control-plane structured events (filtered by component/event/worker/job, optional `since_id` cursor, with a default descending order and configurable limit).  
  Use this as the primary recovery audit trail before touching the database directly.

Abort behavior:

- `POST /abort` accepts `{ "job_id": "<id>" }`, `{ "worker_id": "<id>" }`, or both.
- Optional `reason` field can be supplied and is persisted in the job `last_error` field.
- Accepted transitions move a job to `ABORTED`, clear active assignment metadata, and clear the worker's current assignment.

ACK flow behavior:

- `/next-job` transitions a claimed job to `ASSIGNED` and returns `assignment_id` plus `assignment_expires_at` (Unix epoch seconds).
- Worker must call `/ack` before executing with `job_id`, `worker_id`, and `assignment_id`.
- `/ack` transitions job from `ASSIGNED` to `RUNNING`.
- Reconciler returns stale assignments (`ASSIGNED` with expired `assignment_expires_at`) to `PENDING`.

Completion safety behavior:

- `POST /complete` and `POST /fail` require `assignment_id` and only apply when it matches the job's current lease.
- `POST /complete` requires artifact contract fields (`artifact_id`, `stdout_path`, `stderr_path`, `stdout_tmp_path`, `stderr_tmp_path`).
- `POST /complete` stores artifact metadata in SQLite and rejects completion when artifact fields are incomplete or violate the temp/final naming contract.
- `POST /complete` also stores artifact abstraction metadata (`artifact_backend`, `artifact_location`, `artifact_upload_state`, `artifact_upload_error`).
- On success, worker computes and reports SHA-256 checksums for stdout/stderr artifacts; the control plane persists them in job records.
- `/complete` and `/fail` are idempotent for terminal states (`COMPLETED`/`FAILED`) and return success without state changes.
- To avoid out-of-order mutation, worker heartbeats and completions now include monotonic sequence numbers:
  - `heartbeat` payload includes `seq`.
  - `complete` payload includes `completion_seq`.
  - Control plane ignores stale heartbeats and stale completions when these fields indicate older messages.

Reconnection behavior:

- `POST /reconnect` is sent by workers on startup and accepts:
  - `worker_id`
  - optional `current_job_id`
  - optional list of recently completed jobs (`job_id`, `assignment_id`, status and completion details)
- Control plane reconciles `current_job_id` and applies reconnection completion replay.
- Worker removes replayed completed jobs from its local recovery state after an accepted action.

Additional reconnection robustness:

- Workers queue completion/failure replay payloads locally and flush them on successful `heartbeat`/`next-job` control-plane calls as well as startup reconnect.
- If the control plane is temporarily unreachable while sending completions, finished artifacts remain locally queued and are retried automatically on the next successful control call.

Worker startup options:

- `-state-file` (default `<log-dir>/worker-state.json`) for persisted reconnect replay data.
- `-worker-nonce` (optional) optional registration nonce that must match CP for the same `worker_id`.

Worker startup flow now includes:
- `POST /register` with `worker_id` (and optional `nonce`)
- `POST /reconnect`
- normal heartbeat/ack/complete/fail flow

On startup, the control plane now performs one reconciliation sweep immediately (in addition to the periodic reconciler) to recover stale assignments and worker state after a restart.

## Data model

Tables:

- `jobs`:
  - `id`, `status`, `command`, `args`, `working_dir`, `timeout_ms`, `priority`, `scheduled_at`, `needs_gpu`, `requirements`,
    `created_at`, `updated_at`,
    `attempt_count`, `max_attempts`, `worker_id`, `assignment_id`, `assignment_expires_at`, `last_error`, `result_path`,
    `artifact_id`, `artifact_stdout_tmp_path`, `artifact_stdout_path`,
    `artifact_stdout_sha256`, `artifact_stderr_tmp_path`, `artifact_stderr_path`,
    `artifact_stderr_sha256`, `artifact_storage_backend`, `artifact_storage_location`,
    `artifact_upload_state`, `artifact_upload_error`, `updated_by`
- `workers`:
  - `worker_id`, `last_seen`, `current_job_id`, `status`, `registered_at`, `registration_nonce`, `worker_capabilities`
- `audit_events`:
  - `id`, `ts`, `component`, `level`, `event`, `request_id`, `worker_id`, `job_id`, `details`

Current states in this MVP: `PENDING`, `ASSIGNED`, `RUNNING`, `COMPLETED`, `FAILED`, `LOST`, `RETRYING`, `ABORTED` (SRS-complete core state machine is in place; advanced lifecycle transitions are being implemented by checklist).

## SRS checklist

- See `SRS_IMPLEMENTATION_CHECKLIST.md` for the full ordered implementation plan and progress tracking.
- See `EXPANSION_CHECKLIST.md` for the post-SRS roadmap (UI, security, retention, and hardening).

## SRS recovery test scenarios

- Section 7 scenarios from `SRS.md` are documented in `SRS_TEST_SCENARIOS.md`.
- A helper script exists at `scripts/reliability-scenarios.sh` for scenario status checks and duplicate-completion replay.
- Current automated coverage includes `internal/store/store_test.go` (completion idempotency/noop behavior).

## UI

- Open `http://<control-plane>/ui` for a lightweight dashboard.
- The dashboard is unauthenticated for page load, but you must provide the `X-API-Token` value in the page input to call APIs.

## Notes

- Jobs are claimed by polling, never pushed.
- Recovery on stale heartbeat marks workers `OFFLINE`, moves `RUNNING` jobs to `LOST`, then recovers:
  - `LOST` -> `RETRYING` after a timeout window
  - `RETRYING` -> `PENDING` when retries remain
  - `RETRYING` -> `FAILED` when retries are exhausted.
- `timeout_ms` is enforced by worker execution context and reported as a failure when exceeded.
