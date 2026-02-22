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

1. `[x]` P5-01 — Minimal dashboard UI for queue/worker observability
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
Progress:
- Added `/ui` minimal dashboard to control plane, served directly by `cmd/control/main.go`.
- Dashboard renders live job list, worker list, and recent events with polling and optional filtering.
- Added manual abort action in the UI and API-token persistence in browser local storage.

2. `[x]` P5-02 — Policy/retention engine for logs and artifacts
Current status: active scheduled cleanup on control plane and worker
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
Progress:
- Added periodic control-plane cleanup with configurable retention windows for terminal jobs, terminal artifacts, and events.
- Added worker-side local artifact log cleanup policy with retention interval and skip for currently running job.
- Added audit logging around cleanup passes and outcomes.

3. `[x]` P5-03 — Full transport security (TLS) and stronger auth
Current status: implemented scoped admin/worker auth, signed tokens, and TLS flags
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
Progress:
- Added role-scoped auth:
  - admin token + optional previous token via `-admin-token` / `-admin-token-prev`
  - worker token + optional previous token via `-worker-token` / `-worker-token-prev`
  - optional signed per-worker tokens via `-worker-token-secret` and `-worker-token-ttl-seconds`
- Control-plane HTTPS support:
  - `-tls-cert`, `-tls-key`
  - optional mTLS verification via `-tls-client-ca` and `-tls-require-client-cert`
- Worker TLS client support:
  - `-tls-ca` for control-plane CA trust
  - optional `-tls-client-cert` + `-tls-client-key` for mTLS
- Worker now emits signed tokens when secret mode is configured.

4. `[x]` P5-04 — Dashboard-ready audit + event indexing
Current status: complete
Target: `internal/store/store.go`, control read paths
Implementation tasks:
- Add DB indexes on `audit_events` for frequent filters (`event`, `worker_id`, `job_id`, `component`).
- Add optional event cursor/`since_id` for incremental polling.
- Add UI-friendly summaries (`error_rate`, top transitions).
Acceptance criteria:
- Dashboard and alerting can query large event history without scan overhead.
Progress:
- Added incremental event cursor support via `GET /events?since_id`.
- Added audit event indexes for `component`, `event`, and `ts/id` scan patterns with schema migration-safe creation.

5. `[x]` P5-05 — Worker-side security hardening for command execution
Current status: implemented in `cmd/worker/main.go` with runtime policy controls
Target: `cmd/worker/main.go`
Implementation tasks:
- Add allowlist mode for command binaries.
- Optional non-root user enforcement and strict working directory checks.
- Optional command dry mode (simulate) for validation before execution.
Acceptance criteria:
- Dangerous ad hoc commands are blocked when allowlist mode is enabled.
- Same execution API still supports existing jobs by default (`compat` off by default in hardening profiles).
- Controls now include:
  - `-command-allowlist` + `-allowed-commands` (`HDCF_WORKER_COMMAND_ALLOWLIST`/`HDCF_WORKER_ALLOWED_COMMANDS`)
  - `-allowed-working-dirs` (`HDCF_WORKER_ALLOWED_WORKING_DIRS`)
  - `-require-non-root` (`HDCF_WORKER_REQUIRE_NON_ROOT`)
  - `-dry-run` (`HDCF_WORKER_DRY_RUN`)

6. `[x]` P5-06 — Better reconnection model and idempotency across all endpoints
Current status: completion/fail heartbeat sequencing already implemented
Target: store + control endpoints
Implementation tasks:
- Extend idempotency/sequence guards to `/ack`, `/reconnect`, and optional `/abort`.
- Add server-side dedupe keys for replayed payloads when retrying.
Acceptance criteria:
- Repeated retriable HTTP calls never fork duplicate side effects.

7. `[x]` P5-07 — Priority and preemption policy upgrades
Current status: fixed single-dimensional priority ordering exists
Target: scheduling layer in `internal/store/store.go`
Implementation tasks:
- Add per-worker queue eligibility by capability/role and policy:
  - max concurrent retries per worker
  - queue aging/fairness to avoid starvation
- Add explicit preemption settings (e.g. max high-priority backlog threshold).
Acceptance criteria:
- Mixed workloads remain fair and predictable across multiple workers.
Progress:
- Added scheduling policy knobs for queue aging, preemptive high-priority backlog gating, and worker retry concurrency in control-plane scheduling path.

8. `[x]` P5-08 — Artifact storage abstraction
Current status: worker uploads artifacts using selectable backend
Target: worker + control plane API
Implementation tasks:
- Define artifact storage strategy enum: local, NFS share, or S3-compatible object store.
- Add upload progress/error metadata in completion payload.
Acceptance criteria:
- Retry semantics remain consistent regardless of storage backend.
Progress:
- Implemented local-file and NFS artifact upload strategy via worker + request payload metadata.
- Added artifact storage backend/type metadata propagation (`artifact_storage_backend`, `artifact_storage_location`, upload state/error) through `/complete` and reconnect paths.
- Added API/docs/config updates for worker backend selection (`-artifact-storage-backend`, `-artifact-storage-location`, env vars).
- S3 is not yet implemented at runtime but schema and protocol fields are already in place for future extension.

9. `[x]` P5-09 — Operational CLI enhancements
Current status: complete operational CLI surface implemented
Target: `cmd/hdcfctl`
Implementation tasks:
- `jobs` list/filter CLI
- `jobs describe <id>`
- `workers` list
- `abort` command
- `replay` command for manually invoking reconnect flow.
Acceptance criteria:
- Common admin operations available without raw `curl`.
Progress:
- Added CLI subcommands: `jobs list`, `jobs describe`, `workers list`, `abort`, and `replay`.
- Added `--url`/`--token` flags across commands and JSON output for list/describe/abort/reconnect responses.
- Added reconnect replay payload support via `--completed-jobs-file` in `replay`.

10. `[~]` P5-10 — Integration test suite
Current status: unit coverage started; harness framework still pending
Target: new `tests/` directory
Implementation tasks:
- Add unit tests for store transitions and duplicate sequencing.
- Add integration script suite for `P1`–`P4` critical paths.
- Add one scripted SRS Section 7 recovery workflow.
Acceptance criteria:
- Future refactors cannot silently regress recovery semantics.
Progress:
- Added `internal/store/store_test.go` with recovery-focused unit tests for completion idempotency and terminal-noop behavior.
- Existing `scripts/reliability-scenarios.sh` remains the operational recovery script entrypoint; next step is to add a dedicated automated `tests/` harness and scripted Scenario A-D wrappers.

11. `[~]` P5-11 — Observability and alerting hooks
Current status: metrics endpoint implemented; webhook/notification hooks pending.
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
Progress:
- Added `/metrics` admin endpoint and `MetricsSnapshot` response payload (`GET /metrics`).
- Added control-plane aggregation for queue depth by state, worker totals, retry/loss counts, completion/failure rates, and completion lag.
- Optional alert/webhook hooks still to add for hard events.

12. `[x]` P5-12 — Multi-worker operational readiness
Current status: complete
Target: all control-plane scheduling paths
Implementation tasks:
- Validate assignment claims under concurrent workers.
- Harden `claim` transaction and stale-worker cleanup for high contention.
- Stress test with N workers and long-running jobs.
Acceptance criteria:
- No duplicate assignment across concurrent workers.
- No lost progress under contention.
Progress:
- Added `TestClaimNextJobConcurrentWorkersAreMutualExclusive` to validate claim behavior under concurrent workers.
- Added `TestRecoverStaleRunningJobIsReclaimedAndReassigned` to validate stale-worker handoff and retry-window re-queueing.
- Added `TestMultiWorkerStressAndRecoveryUnderContention` to validate end-to-end claim/processing under concurrent workers and stale-assignment recovery under contention.
- Added parallel `RecoverStaleWorkers` call coverage for contention.
- Added end-to-end completion verification for all jobs after recovery and reprocessing.

13. `[x]` P5-13 — Config and deployment hardening
Current status: complete
Target: process startup files
Implementation tasks:
- Added example config files for major control and worker settings:
  - `deploy/control-config.example.json`
  - `deploy/worker-config.example.json`
- Added sample startup templates for unattended boot:
  - `deploy/hdcf-control.service`
  - `deploy/hdcf-worker.service`
  - `deploy/hdcf-control.supervisor.conf`
  - `deploy/hdcf-worker.supervisor.conf`
- Added migration diagnostics and health endpoint (`/healthz`) with:
  - database connectivity/schema check
  - migration completeness check
  - DB directory filesystem writeability check
- Wired config file support via `-config` and env `HDCF_CONTROL_CONFIG` / `HDCF_WORKER_CONFIG`.
Acceptance criteria:
- Reliable boot-time run in unattended environments.

## Continuation note

Execute these after the existing `P4-*` roadmap, then rotate in:
1) UI/observability items,
2) security and auth hardening,
3) retention/compliance and operational reliability polish.
