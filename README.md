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
go run ./cmd/control -addr :8080 -db ./jobs.db -token dev-token
```

Environment variables available:

- `HDCF_ADDR` (default `:8080`)
- `HDCF_DB_PATH` (default `jobs.db`)
- `HDCF_API_TOKEN` (default `dev-token`)
- `HDCF_HEARTBEAT_TIMEOUT_SECONDS` (default `60`)
- `HDCF_RECONCILE_INTERVAL_SECONDS` (default `10`)

### 2) Start worker on ASUS

```bash
go run ./cmd/worker -control-url http://<mac-ip>:8080 -token dev-token -log-dir ./worker-logs
```

Worker options:

- `-control-url` (default `http://localhost:8080`)
- `-worker-id` (default `hostname-<generated>`)
- `-token` (default `dev-token`)
- `-capabilities` (default ``, comma-separated worker capabilities, e.g. `gpu,ssd`)
- `-poll-interval-seconds` (default `3`)
- `-heartbeat-interval-seconds` (default `5`)
- `-request-timeout-seconds` (default `10`)
- `-log-dir` (default `worker-logs`)
- `-heartbeat-metrics` (default `false`) — include optional resource metrics in heartbeat payload

Environment variables:

- `HDCF_WORKER_CAPABILITIES` (comma-separated capabilities)
- `HDCF_WORKER_NONCE`
- `HDCF_CONTROL_URL`
- `HDCF_WORKER_ID`

### 3) Submit a job

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
- `GET /events` (optional filters: `component`, `event`, `worker_id`, `job_id`, `limit`)

Queue ordering behavior:
- `GET /next-job` claims from `PENDING` jobs by descending `priority`, then ascending `created_at`, then ascending `job_id`.
- Jobs with `scheduled_at` in the future are not claimed until their scheduled time arrives.

Read/observability behavior:

- `GET /jobs` returns job list entries with state, timestamps, attempt counters, worker assignment, and heartbeat age for jobs with active workers.
- `GET /jobs/{job_id}` returns a single job detail payload with the same fields.
- `GET /workers` returns worker rows with heartbeat age in seconds and optional latest `heartbeat_metrics`.
- `GET /events` returns durable control-plane structured events (filtered by component/event/worker/job with a default descending order and configurable limit).  
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
    `artifact_stderr_sha256`, `updated_by`
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
