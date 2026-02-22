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
- `-poll-interval-seconds` (default `3`)
- `-heartbeat-interval-seconds` (default `5`)
- `-request-timeout-seconds` (default `10`)
- `-log-dir` (default `worker-logs`)

### 3) Submit a job

```bash
go run ./cmd/hdcfctl submit \
  --url http://<mac-ip>:8080 \
  --token dev-token \
  --command sleep \
  --args "2"
```

The CLI sends `POST /jobs` and returns the new `job_id`.

## Implemented endpoints

- `POST /jobs`
- `GET /next-job?worker_id=...`
- `POST /ack`
- `POST /heartbeat`
- `POST /complete`
- `POST /fail`

ACK flow behavior:

- `/next-job` transitions a claimed job to `ASSIGNED` and returns `assignment_id` plus `assignment_expires_at` (Unix epoch seconds).
- Worker must call `/ack` before executing with `job_id`, `worker_id`, and `assignment_id`.
- `/ack` transitions job from `ASSIGNED` to `RUNNING`.
- Reconciler returns stale assignments (`ASSIGNED` with expired `assignment_expires_at`) to `PENDING`.

## Data model

Tables:

- `jobs`:
  - `id`, `status`, `command`, `args`, `working_dir`, `timeout_ms`, `created_at`, `updated_at`,
    `attempt_count`, `max_attempts`, `worker_id`, `assignment_id`, `assignment_expires_at`, `last_error`, `result_path`, `updated_by`
- `workers`:
  - `worker_id`, `last_seen`, `current_job_id`, `status`

Current states in this MVP: `PENDING`, `ASSIGNED`, `RUNNING`, `COMPLETED`, `FAILED`, `LOST`, `RETRYING`, `ABORTED` (SRS-complete core state machine is in place; advanced lifecycle transitions are being implemented by checklist).

## SRS checklist

- See `SRS_IMPLEMENTATION_CHECKLIST.md` for the full ordered implementation plan and progress tracking.

## Notes

- Jobs are claimed by polling, never pushed.
- Recovery on stale heartbeat marks `RUNNING` jobs assigned to offline workers back to `PENDING`.
- `timeout_ms` is enforced by worker execution context and reported as a failure when exceeded.
