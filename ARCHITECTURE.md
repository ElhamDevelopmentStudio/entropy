# Architecture

## High-Level Model


+-------------------+

Control Plane
SQLite Store
Scheduler
Reconciler
Lease Manager
Audit Log
Metrics
+-------------------+
     ↑
     ↓

+-------------------+

Worker Node
Poll Loop
ACK Handshake
Executor
Artifact Writer
Replay Buffer
Heartbeat Loop
+-------------------+

---

## Lease-Based Assignment

1. Worker polls `/next-job`
2. Control plane:
   - Selects PENDING job
   - Sets status → ASSIGNED
   - Generates `assignment_id`
   - Sets `assignment_expires_at`
3. Worker must ACK before RUNNING
4. Lease expires if:
   - Worker stops heartbeating
   - ACK never arrives

Expired leases return job to PENDING.

---

## State Machine


PENDING
↓ (claim)
ASSIGNED
↓ (ack)
RUNNING
↓ (complete)
COMPLETED
↓ (fail)
FAILED

Failure path:
RUNNING → LOST → RETRYING → PENDING


---

## Recovery Engine

Periodic reconciliation:

- Detect offline workers
- Transition RUNNING → LOST
- Transition LOST → RETRYING
- Transition RETRYING → PENDING (if attempts remain)
- Transition RETRYING → FAILED (if attempts exhausted)

---

## Scheduling Model

Job selection priority:

1. Highest `priority`
2. Oldest `created_at`
3. Stable job ID ordering

Optional:
- Queue aging fairness boost
- Retry concurrency caps
- High-priority backlog gating
- Capability-based matching
- `needs_gpu` constraints

---

## Determinism

Entropy ensures:

- Single valid assignment per job
- Idempotent completion
- Monotonic sequence enforcement
- Durable transitions
- Replay-safe reconciliation
