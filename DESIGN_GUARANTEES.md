# Design Guarantees

## Execution Model

Entropy implements an at-least-once execution model with strong idempotency guarantees.

A job may execute more than once if:

- Worker crashes mid-execution
- Lease expires before completion
- Network partition delays ACK

However:

Duplicate completions cannot corrupt state.

---

## Safety Invariants

1. A job cannot be COMPLETED without a valid `assignment_id`.
2. A stale `assignment_id` cannot complete a job.
3. `completion_seq` ensures monotonic completion ordering.
4. Duplicate completion requests are safe.
5. Fail after complete is a no-op.
6. Concurrent claims cannot assign same job twice.
7. State transitions are atomic.

---

## Partition Tolerance

During network partitions:

- Worker continues execution.
- Completion buffered locally.
- On reconnect:
  - Worker replays completion.
  - Control plane reconciles deterministically.

---

## Crash Recovery Bound

Recovery bound is defined by:

- `heartbeat_timeout`
- `reconcile_interval`

Worst-case delay before requeue:
≈ heartbeat_timeout + reconcile_interval

---

## Deterministic Final State

Entropy guarantees:

Regardless of:

- Duplicate completions
- Out-of-order heartbeats
- Replay attempts
- Worker restarts
- Control-plane restarts

The final job state converges to a deterministic terminal state.
That’s It.
