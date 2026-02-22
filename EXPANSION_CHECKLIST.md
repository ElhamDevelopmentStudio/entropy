# HDCF Expansion Checklist (Post-SRS and Hardening Roadmap)

This document tracks follow-on work outside the current SRS minimum.  
Treat this as the next phase once `SRS_IMPLEMENTATION_CHECKLIST.md` items `P4-19`+ are implemented.

## Scope

Source references:
- `SRS.md`
- `mvp.md`
- `SRS_IMPLEMENTATION_CHECKLIST.md`
- current code in `cmd/control`, `cmd/worker`, `internal/store`, `internal/hdcf`, `README.md`

## Legend
- `[ ]` not started
- `[~]` partial / in-progress
- `[x]` complete

## Expansion priority list

1. `[ ]` P5-01 — Minimal dashboard UI for queue/worker observability
Requirement source: `SRS.md` operational visibility and optional MVP dashboard
Target: new `web/` or `cmd/dashboard` (static UI + backend read endpoints)
Implementation tasks:
- Show live job list, filters (`status`, `worker_id`, `attempts`), and state transitions.
- Show worker roster with heartbeat age, status, in-flight job, and optional metrics.
- Add simple controls: manual `/abort`, manual `/next-job` dry-run view, and reconnect/replay diagnostics.
Acceptance criteria:
- Operators can inspect all jobs and workers without shelling into DB/curl.
- UI reflects state changes within 1–3 seconds.
Dependency: `GET /jobs`, `GET /jobs/{id}`, `GET /workers`, `GET /events`.

2. `[ ]` P5-02 — Policy/retention engine for logs and artifacts
Current status: no background lifecycle management yet
Target: scheduled cleaner job + policy config
Implementation tasks:
- Add config for retention windows:
  - `jobs.retention_completed_days`
  - `artifacts.retention_days`
  - `events.retention_days`
  - `worker.log.retention_days`
- Add scheduled cleanup path in control plane:
  - remove old DB rows based on policies
  - remove archived log/artifact files
- Emit cleanup audit events.
Acceptance criteria:
- Database and disk do not grow unbounded across long runs.
- Cleanup does not delete non-terminal or active work unexpectedly.

3. `[ ]` P5-03 — Full transport security (TLS) and stronger auth
Current status: static `X-API-Token` only
Target: `cmd/control` + `cmd/worker`
Implementation tasks:
- Add HTTPS listen mode with cert/key config flags (or mTLS optional mode).
- Move to scoped credentials:
  - signed per-node tokens (short-lived if feasible),
  - rotating token support,
  - optional per-endpoint token roles for worker/admin calls.
- Add optional mutual TLS validation for worker registration.
Acceptance criteria:
- Insecure mode remains possible for local lab use, but secure mode prevents traffic sniffing and token replay.
- Token and TLS credentials are never logged.

4. `[ ]` P5-04 — Dashboard-ready audit + event indexing
Current status: event endpoint exists but not indexed for UI search speed
Target: `internal/store/store.go`, control read paths
Implementation tasks:
- Add DB indexes on `audit_events` for frequent filters (`event`, `worker_id`, `job_id`, `component`).
- Add optional event cursor/`since_id` for incremental polling.
- Add UI-friendly summaries (`error_rate`, top transitions).
Acceptance criteria:
- Dashboard and alerting can query large event history without scan overhead.

5. `[ ]` P5-05 — Worker-side security hardening for command execution
Current status: arbitrary shell command string execution
Target: `cmd/worker/main.go`
Implementation tasks:
- Add allowlist mode for command binaries.
- Optional non-root user enforcement and strict working directory checks.
- Optional command dry mode (simulate) for validation before execution.
Acceptance criteria:
- Dangerous ad hoc commands are blocked when allowlist mode is enabled.
- Same execution API still supports existing jobs by default (`compat` off by default in hardening profiles).

6. `[ ]` P5-06 — Better reconnection model and idempotency across all endpoints
Current status: completion/fail heartbeat sequencing already implemented
Target: store + control endpoints
Implementation tasks:
- Extend idempotency/sequence guards to `/ack`, `/reconnect`, and optional `/abort`.
- Add server-side dedupe keys for replayed payloads when retrying.
Acceptance criteria:
- Repeated retriable HTTP calls never fork duplicate side effects.

7. `[ ]` P5-07 — Priority and preemption policy upgrades
Current status: fixed single-dimensional priority ordering exists
Target: scheduling layer in `internal/store/store.go`
Implementation tasks:
- Add per-worker queue eligibility by capability/role and policy:
  - max concurrent retries per worker
  - queue aging/fairness to avoid starvation
- Add explicit preemption settings (e.g. max high-priority backlog threshold).
Acceptance criteria:
- Mixed workloads remain fair and predictable across multiple workers.

8. `[ ]` P5-08 — Artifact storage abstraction
Current status: filesystem temp/final paths on worker host only
Target: worker + control plane API
Implementation tasks:
- Define artifact storage strategy enum: local, NFS share, or S3-compatible object store.
- Add upload progress/error metadata in completion payload.
Acceptance criteria:
- Retry semantics remain consistent regardless of storage backend.

9. `[ ]` P5-09 — Operational CLI enhancements
Current status: only `submit` exists
Target: `cmd/hdcfctl`
Implementation tasks:
- `jobs` list/filter CLI
- `jobs describe <id>`
- `workers` list
- `abort` command
- `replay` command for manually invoking reconnect flow.
Acceptance criteria:
- Common admin operations available without raw `curl`.

10. `[ ]` P5-10 — Integration test suite
Current status: no automated CI test harness yet
Target: new `tests/` directory
Implementation tasks:
- Add unit tests for store transitions and duplicate sequencing.
- Add integration script suite for `P1`–`P4` critical paths.
- Add one scripted SRS Section 7 recovery workflow.
Acceptance criteria:
- Future refactors cannot silently regress recovery semantics.

11. `[ ]` P5-11 — Observability and alerting hooks
Current status: logs + events only
Target: control plane + worker
Implementation tasks:
- Export metrics endpoint (Prometheus-style or JSON) with:
  - queue depth by state
  - worker offline count
  - recovery and retry counters
  - completion/failure rates and completion lag
- Add optional webhook/notification hook on hard events (`offlined worker`, `retry exhaustion`).
Acceptance criteria:
- SRE visibility supports early warnings.

12. `[ ]` P5-12 — Multi-worker operational readiness
Current status: single-worker assumptions still present in behavior
Target: all control-plane scheduling paths
Implementation tasks:
- Validate assignment claims under concurrent workers.
- Harden `claim` transaction and stale-worker cleanup for high contention.
- Stress test with N workers and long-running jobs.
Acceptance criteria:
- No duplicate assignment across concurrent workers.
- No lost progress under contention.

13. `[ ]` P5-13 — Config and deployment hardening
Current status: env/flags only
Target: process startup files
Implementation tasks:
- Add example config file support for all major values.
- Provide sample systemd/supervisor startup for control plane and worker.
- Add migration diagnostics and health endpoint (`/healthz`) with DB and fs checks.
Acceptance criteria:
- Reliable boot-time run in unattended environments.

## Continuation note

Execute these after the existing `P4-*` roadmap, then rotate in:
1) UI/observability items,
2) security and auth hardening,
3) retention/compliance and operational reliability polish.

