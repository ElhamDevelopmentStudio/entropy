# HDCF SRS Implementation Checklist (Session Handoff Document)

## Purpose

This document captures the full feature checklist derived from `SRS.md` and mapped to the current Go MVP implementation. It is the starting point for the next chat session so work can continue without re-deriving requirements.

## Scope Reference

Primary requirements source: `SRS.md`.
Current MVP implementation files:
`cmd/control/main.go`
`cmd/worker/main.go`
`cmd/hdcfctl/main.go`
`internal/store/store.go`
`internal/hdcf/types.go`
`README.md`

## Current Snapshot (what already exists)

Implemented from `mvp.md`:
`POST /jobs`
`GET /next-job`
`POST /heartbeat`
`POST /complete`
`POST /fail`
SQLite-backed durability for jobs/workers
Worker heartbeat loop
Stale worker recovery that requeues running jobs
Command job execution with local log files
Basic CLI enqueue helper
Static token auth via `X-API-Token`

Not yet implemented from SRS:
Duplicate completion safety and reconciliation protocol
Worker registration/reconnect semantics
Artifact integrity + atomic completion guarantee
Control-plane restart reconciliation hardening

## How to read this checklist

Each item is ordered by implementation priority.
Format:
1. Priority ID
Feature
Requirement mapping from SRS
Implementation target
Acceptance criteria
Dependency

Status legend:
`[ ]` not started
`[~]` partial / in-progress
`[x]` completed

## Implementation Checklist (High → Low priority)

1. `[x]` P1-01 — Extend persisted state machine to all SRS states
Requirement source: `SRS.md` Section 4.1
Target: `internal/store/store.go`, `internal/hdcf/types.go`
Implementation details:
Define canonical states `PENDING`, `ASSIGNED`, `RUNNING`, `COMPLETED`, `FAILED`, `LOST`, `RETRYING`, `ABORTED`.
Migrate schema/transition logic to include `ASSIGNED`, `LOST`, `RETRYING`, `ABORTED`.
Add helper transition table or function to enforce legal moves.
Acceptance criteria:
Any job state change persists atomically in SQLite.
No SQL path writes invalid terminal state transitions.
Dependency: none.

2. `[x]` P1-02 — Implement explicit assignment handshake with ACK timeout
Requirement source: `SRS.md` Section 4.1.2
Target: `cmd/control/main.go`, `cmd/worker/main.go`, `internal/store/store.go`
Implementation details:
Change `/next-job` flow to set `ASSIGNED` and return job claim token/lease id.
Add `/ack` (or extend completion path) so worker explicitly ACKs receipt.
If ACK not received within timeout, transition back to `PENDING`.
Acceptance criteria:
If worker crashes before ACK, job never remains stuck in `ASSIGNED`.
Worker cannot move `ASSIGNED` to `RUNNING` without ACK.
Dependency: P1-01.

3. `[ ]` P1-03 — Add heartbeat-based `OFFLINE` and `LOST` transitions
Requirement source: `SRS.md` Sections 4.2, 4.4.1
Target: `internal/store/store.go`, `cmd/control/main.go`
Implementation details:
On stale heartbeat, mark worker `OFFLINE`.
Mark related `RUNNING` jobs as `LOST`.
Add policy to move `LOST` to `RETRYING` or `PENDING` after threshold.
Acceptance criteria:
Worker disappearance does not leave active jobs orphaned.
Lost jobs are recoverable and do not duplicate unexpectedly.
Dependency: P1-01, P1-02.

4. `[ ]` P1-04 — Make completion/failure idempotent and conflict-safe
Requirement source: `SRS.md` Section 4.6, 6 failure matrix
Target: `internal/store/store.go`, `cmd/control/main.go`
Implementation details:
Include monotonic transition checks in `/complete` and `/fail`.
Ignore duplicate `/complete` or `/fail` for same terminal state.
Use request id or completion attempt check to prevent overwriting completed jobs with stale retries.
Acceptance criteria:
Duplicate completion messages from reconnects are safe and deterministic.
A completed job never reverts to non-terminal state.
Dependency: P1-01.

5. `[ ]` P1-05 — Add worker reconnection protocol and startup reporting
Requirement source: `SRS.md` Section 4.6, 4.4.1
Target: `cmd/worker/main.go`, `cmd/control/main.go`, `internal/store/store.go`
Implementation details:
On worker boot, send registration/reconnect payload including:
`worker_id`, `current_job_id` (if any), completed jobs since disconnect.
Control plane reconciles and emits action plan.
Acceptance criteria:
Worker restart after crash leads to deterministic recovery path.
In-flight jobs re-enter valid state without manual intervention.
Dependency: P1-03.

6. `[ ]` P1-06 — Implement artifact-safe completion contract
Requirement source: `SRS.md` Section 4.5
Target: `cmd/worker/main.go`, `cmd/control/main.go`, `internal/store/store.go`, `internal/hdcf/types.go`
Implementation details:
Worker writes outputs as temp files and atomically renames only on success.
Control plane stores artifact metadata and validates temp rename completion.
Reject completion if output is missing or incomplete.
Acceptance criteria:
Partial artifacts are never marked `COMPLETED`.
Restart after partial write does not corrupt final state.
Dependency: P1-04.

7. `[ ]` P1-07 — Add optional checksum/integrity metadata for artifact sets
Requirement source: `SRS.md` Section 4.5
Target: `cmd/worker/main.go`, `cmd/control/main.go`, `internal/store/store.go`
Implementation details:
Compute hash (sha256) during artifact finalization.
Store hash in job record and return it in completion response.
Acceptance criteria:
Integrity field exists and can be used for manual/auto verification.
Dependency: P1-06.

8. `[ ]` P1-08 — Implement control-plane restart recovery sweep
Requirement source: `SRS.md` Section 4.4.2
Target: `cmd/control/main.go`, `internal/store/store.go`
Implementation details:
On startup run reconciliation pass for inconsistent `ASSIGNED`/`RUNNING` jobs and unregistered workers.
Reset stale non-terminal jobs according to policy.
Acceptance criteria:
After CP restart, no job is stranded due to unflushed memory state.
Dependency: P1-01, P1-03.

9. `[ ]` P2-09 — Register worker identity and tighten LAN auth
Requirement source: `SRS.md` Section 5.4
Target: `cmd/control/main.go`, `cmd/worker/main.go`
Implementation details:
Add worker register endpoint with identity + optional nonce.
Bind heartbeat and job operations to known `worker_id`.
Keep current token auth and remove unauthenticated mutation paths.
Acceptance criteria:
Only registered/known workers can heartbeat or claim jobs.
Dependency: P1-05.

10. `[ ]` P2-10 — Add explicit abort workflow
Requirement source: `SRS.md` Section 4.1.1
Target: `cmd/control/main.go`, `internal/store/store.go`
Implementation details:
Add `POST /abort` (job_id or worker_id context).
Transition to `ABORTED` and stop/revoke future retries.
Acceptance criteria:
Aborted jobs stop retrying and stay terminal unless intentionally restarted.
Dependency: P1-01.

11. `[ ]` P2-11 — Add job read APIs for observability
Requirement source: future operations from dashboard optional MVP
Target: `cmd/control/main.go`, `internal/store/store.go`
Implementation details:
Add `GET /jobs`, `GET /jobs/{id}`, `GET /workers`.
Return state, timestamps, attempts, worker id, heartbeat age.
Acceptance criteria:
Operators can inspect state of every job and worker after recovery.
Dependency: P1-08.

12. `[ ]` P2-12 — Add deterministic queue ordering + optional priority
Requirement source: `SRS.md` Section 5.5
Target: `internal/store/store.go`
Implementation details:
Schema add `priority` integer and `scheduled_at`.
`/next-job` selects by priority then created time.
Acceptance criteria:
Higher priority jobs are scheduled first when defined.
Dependency: P1-01.

13. `[ ]` P2-13 — Add worker capability metadata placeholders
Requirement source: `SRS.md` Section 5.5
Target: `internal/store/store.go`, `cmd/control/main.go`, `cmd/worker/main.go`
Implementation details:
Add fields `worker_capabilities`, `needs_gpu`, job requirements.
Filter candidate workers when selecting job.
Acceptance criteria:
Capability-aware assignment works without breaking default single-worker path.
Dependency: P2-12.

14. `[ ]` P2-14 — Buffer and batch log upload on reconnection
Requirement source: `SRS.md` Section 4.3.1
Target: `cmd/worker/main.go`, `cmd/control/main.go`
Implementation details:
Keep log segments in local queue if upload fails.
Upload pending logs when heartbeat/next control succeeds.
Acceptance criteria:
No log loss due to short network disconnections.
Dependency: P1-06, P1-05.

15. `[ ]` P2-15 — Add structured event logging and recovery audit trail
Requirement source: reliability and eventual consistency requirements
Target: `cmd/control/main.go`, `cmd/worker/main.go`
Implementation details:
Log every state transition and reconciliation decision as structured JSON.
Persist enough context for post-mortem.
Acceptance criteria:
Operations are explainable from logs without manual DB introspection.
Dependency: P1-01.

16. `[ ]` P2-16 — Add duplicate message reorder handling in `/heartbeat` and `/complete`
Requirement source: `SRS.md` Reliability section
Target: `internal/store/store.go`, `cmd/control/main.go`
Implementation details:
Track last-seen heartbeat seq or request timestamp per worker.
Ignore stale older messages when ordering is uncertain.
Acceptance criteria:
Late packets do not rollback newer state.
Dependency: P1-04.

17. `[ ]` P3-17 — Add resource-metric heartbeat payload support (optional MVP field)
Requirement source: `SRS.md` Section 4.2
Target: `internal/hdcf/types.go`, `cmd/worker/main.go`, `cmd/control/main.go`
Implementation details:
Extend heartbeat schema with optional CPU/GPU/memory fields.
Store metrics only when provided.
Acceptance criteria:
Metrics ingestion does not break current protocol.
Dependency: P9-10.

18. `[ ]` P3-18 — Add test scenarios from SRS Section 7
Requirement source: `SRS.md` Section 7
Target: repository test plan + future scripts
Implementation details:
Create reproducible scenarios:
router unplug mid-job
worker kill mid-job
ASUS power cycle during execution
Mac restart during active job
duplicate completion replay
Acceptance criteria:
All listed recovery conditions pass and state remains consistent.
Dependency: all high-priority protocol and recovery work.

## Future extension items (after SRS minimum)

`[ ]` P4-19 — Dashboard UI for queue and worker status
`[ ]` P4-20 — Priority/retention policies for log/artifact cleanup
`[ ]` P4-21 — Full TLS and stronger authentication model

## Session-to-session continuation note

When a new session starts, treat this file as the canonical roadmap and begin from the highest `P1` item not marked complete.
If a session asks for “implement next”, execute one checklist item in order and mark it done upon completion.

## References

`SRS.md` is the contract.
`mvp.md` is the minimal baseline target.
`README.md` documents current run commands.
`internal/store/store.go` defines persistence and transition behavior.
`cmd/control/main.go` and `cmd/worker/main.go` define protocol and orchestration.
