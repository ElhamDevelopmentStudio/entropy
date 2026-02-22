# SRS Section 7 — Recovery Scenario Test Plan

This document captures the concrete recovery scenarios required by `SRS.md` Section 7 and maps each scenario to repeatable validation steps against the current control-plane/worker protocol.

## Scope and assumptions

1. Control plane: `cmd/control` running on default `http://localhost:8080` with token `dev-token` unless overridden.
2. One worker is registered and connected before each scenario.
3. Helpers use:
   - `HDCF_TEST_URL` (default `http://localhost:8080`)
   - `HDCF_TEST_TOKEN` (default `dev-token`)
4. `jq` is optional. The commands still work without it using `sed` where possible.
5. Recovered paths:
   - Single-job queue, pull loop, heartbeat and stale worker recovery are already implemented.
6. Expected recovery timing defaults:
   - Worker marked `OFFLINE` after heartbeat timeout window.
   - `RUNNING` jobs move to `LOST`.
   - `LOST` jobs move to `RETRYING` after stale threshold.
   - `RETRYING` returns to `PENDING` when retries remain, otherwise `FAILED`.

Set shared test variables:

```bash
export HDCF_TEST_URL=http://localhost:8080
export HDCF_TEST_TOKEN=dev-token
```

### Shared checks

1. Read a single job:
   `curl -sS -H "X-API-Token: ${HDCF_TEST_TOKEN}" "${HDCF_TEST_URL}/jobs/<JOB_ID>"`
2. Read all workers:
   `curl -sS -H "X-API-Token: ${HDCF_TEST_TOKEN}" "${HDCF_TEST_URL}/workers"`
3. Read audit events:
   `curl -sS -H "X-API-Token: ${HDCF_TEST_TOKEN}" "${HDCF_TEST_URL}/events?limit=25"`

## Scenario A — Router/network drop mid-job

1. Start one long job:
   `go run ./cmd/hdcfctl submit --command sleep --args "120" --url "${HDCF_TEST_URL}" --token "${HDCF_TEST_TOKEN}"`
2. Capture `job_id`.
3. Confirm `GET /jobs/<job_id>` shows `RUNNING` with a worker assignment.
4. Simulate uplink failure from worker host.
5. Confirm worker heartbeats stop (`GET /workers`) and control-plane logs show `reconciler.offline_workers`.
6. Confirm the in-flight job moves to `LOST`, then `RETRYING` (or `PENDING` when retries remain) per timeout policy.
7. Restore network on worker host.
8. Start/restart worker daemon and confirm it reconnects.
9. Verify job either finishes as `COMPLETED` (if rerunnable) or remains in a valid recovery state.

Acceptance:
1. Job does not disappear.
2. No duplicate completion of a single assignment is observed.
3. Event stream contains consistent recovery transitions.

## Scenario B — Worker process kill mid-job

1. Start one long job as in Scenario A.
2. On worker host, identify worker PID.
3. `kill -9 <worker_pid>` (or equivalent).
4. Wait for at least heartbeat timeout + reconcile interval.
5. Confirm worker row `OFFLINE` and job transitions to `LOST`.
6. Restart worker process.
7. Confirm worker re-registers/reconnects and claim behavior is valid.

Acceptance:
1. In-flight job is reclaimed through `LOST`/`RETRYING`/`PENDING`.
2. Restarted worker does not create invalid duplicate terminal states.

## Scenario C — ASUS power-cycle during execution

1. Start one long job.
2. Power cycle the ASUS worker machine.
3. Confirm worker does not heartbeat while offline.
4. Confirm running job moves through `LOST` and then retry path.
5. Power the worker back on and restart worker daemon.
6. Confirm worker can register and continue recover/restart flow.

Acceptance:
1. `ROUTING` and stale assignment semantics remain deterministic.
2. No terminal corruption from abrupt node power loss.

## Scenario D — Mac mini/control-plane restart during active job

1. Start a job while worker is running.
2. Stop control-plane process (or host reboot).
3. Confirm worker continues without panicking (log buffer/reconnect queue may fill if CP unreachable).
4. Restart control-plane.
5. Confirm startup recovery runs (`control.recovery_startup`) and stale work is reclaimed.
6. Confirm worker reconnect flow applies and job state returns to a valid in-memory + persisted state.

Acceptance:
1. Queue and job metadata survive restart from SQLite.
2. No orphaned RUNNING jobs with absent worker context.
3. Recovery replay does not violate idempotency.

## Scenario E — Duplicate completion replay

1. Submit a short but not instant job:
   `go run ./cmd/hdcfctl submit --command bash --args "-c sleep 30" --url "${HDCF_TEST_URL}" --token "${HDCF_TEST_TOKEN}"`
2. Poll `GET /jobs/<job_id>` until `status` becomes `RUNNING` and capture `worker_id` and `assignment_id`.
3. Send a completion payload manually:
   `curl -X POST .../complete` with valid `artifact_id`, `stdout_path`, `stderr_path`, tmp/final paths, and `assignment_id`.
4. Immediately send the exact same completion payload again.
5. Check `GET /jobs/<job_id>` is `COMPLETED`.

Acceptance:
1. Both requests return success.
2. Final state remains `COMPLETED`.
3. Audit log shows second completion treated as terminal no-op or duplicate-safe replay.

## Scenario command reference (optional)

1. Duplicate completion sample (replace placeholders):

```bash
curl -X POST "${HDCF_TEST_URL}/complete" \
  -H "X-API-Token: ${HDCF_TEST_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id":"<JOB_ID>",
    "worker_id":"<WORKER_ID>",
    "assignment_id":"<ASSIGNMENT_ID>",
    "artifact_id":"scenario_e_test",
    "exit_code":0,
    "stdout_tmp_path":"/tmp/stdout.tmp.txt",
    "stdout_path":"/tmp/stdout.txt",
    "stderr_tmp_path":"/tmp/stderr.tmp.txt",
    "stderr_path":"/tmp/stderr.txt",
    "stdout_sha256":"",
    "stderr_sha256":"",
    "result_summary":"duplicate replay check",
    "completion_seq":0
  }'
```

2. To send once more, rerun the same command without changing payload.

## Automation helper

1. `scripts/reliability-scenarios.sh` contains helper functions for these scenarios and reusable API calls.
