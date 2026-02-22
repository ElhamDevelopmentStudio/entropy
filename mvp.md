
# 🎯 MVP Goal

From your Mac mini:

You can enqueue a job.

Your ASUS:

-   Picks it up
    
-   Executes it
    
-   Reports status
    
-   Survives disconnects
    
-   Doesn’t corrupt state
    

And everything is durable.

----------

# 🧱 Minimal MVP Architecture

## Node Roles

### 🧠 Mac Mini (Control Plane)

Runs:

1.  Simple HTTP API server
    
2.  SQLite (or similar local durable DB)
    
3.  Job state machine
    
4.  In-memory + DB-backed queue
    

No Redis.  
No message brokers.  
No distributed logs.

Just:

-   DB + API + logic.
    

----------

### 🔥 ASUS Worker

Runs:

1.  Worker daemon process
    
2.  Heartbeat loop
    
3.  Job polling loop
    
4.  Local log spool file
    
5.  Retry reconnect logic
    

----------

# 🏗 Architecture Overview

[ Mac Mini ]  
 |  
 |  HTTP (LAN)  
 |  
[ ASUS Worker ]

Communication pattern:

Worker pulls tasks.  
Never push from Mac.

Why?

Because pull model survives:

-   IP changes
    
-   Router reboots
    
-   Temporary unreachability
    

Worker keeps retrying.

----------

# 🧠 Core Design Decisions (Critical)

## 1️⃣ Pull-Based Job Model

Worker does:

Every X seconds:  
→ Ask Mac: “Do you have a job for me?”

Mac responds:

-   200 + Job
    
-   204 No job
    

This avoids:

-   Broken push calls
    
-   Stale connections
    
-   Router buffer issues
    

----------

## 2️⃣ Durable Job Table (SQLite on Mac)

Single table:

Jobs:

-   id (UUID)
    
-   status
    
-   payload (JSON)
    
-   created_at
    
-   updated_at
    
-   attempt_count
    
-   max_attempts
    

Status values (MVP):

-   PENDING
    
-   RUNNING
    
-   COMPLETED
    
-   FAILED
    

We skip LOST for MVP.  
We infer from heartbeat loss.

----------

## 3️⃣ Worker Heartbeat

Worker every 5–10 seconds:

POST /heartbeat  
{  
worker_id,  
current_job_id,  
timestamp  
}

Mac stores:

-   last_seen timestamp
    
-   current job
    

If last_seen > threshold:  
→ mark worker offline  
→ mark RUNNING job back to PENDING (if needed)

Simple.  
Effective.

----------

## 4️⃣ Safe Assignment Protocol (Minimal Version)

Worker polls:

GET /next-job?worker_id=xyz

Mac:

-   Find first PENDING job
    
-   Atomically set to RUNNING
    
-   Return job
    

No two workers issue for now.  
Single worker = simpler atomicity.

If worker crashes mid-job:

-   Job remains RUNNING
    
-   On heartbeat timeout → Mac resets to PENDING
    

----------

## 5️⃣ Log Handling (Minimal Version)

Worker:

-   Writes logs locally to file
    
-   On completion:
    
    -   Upload log file to Mac
        
    -   Then mark job COMPLETED
        

If network drops:

-   Logs stay local
    
-   On reconnect → upload
    

No live streaming in MVP.

----------

## 6️⃣ Artifact Handling (Minimal Version)

Worker:

-   Write output to temp file
    
-   Rename to final
    
-   Upload to Mac
    

Mac:

-   Accept file
    
-   Mark job COMPLETED
    

Atomic rename avoids partial corruption.

----------

# 🔄 Failure Behavior (MVP Simplified)

### Wi-Fi drops mid-job

-   Worker continues
    
-   Heartbeat stops
    
-   Mac waits
    
-   When worker reconnects → heartbeat resumes
    
-   Job continues
    

If worker died:

-   Heartbeat never resumes
    
-   After timeout → Mac resets job to PENDING
    

----------

### ASUS power loss

-   Mac marks job PENDING after timeout
    
-   Worker reboots
    
-   Worker resumes polling
    
-   Gets job again
    

Idempotency requirement:  
Job execution must either:

-   Overwrite safely  
    OR
    
-   Detect partial output
    

----------

### Router reboot

-   Worker retries polling
    
-   Mac keeps DB intact
    
-   System recovers automatically
    

----------

# 📦 What You Actually Build (MVP Scope)

### On Mac Mini

-   HTTP server with endpoints:
    
    -   POST /jobs
        
    -   GET /next-job
        
    -   POST /heartbeat
        
    -   POST /complete
        
    -   POST /fail
        
-   SQLite DB
    
-   Basic job recovery logic
    

That’s it.

----------

### On ASUS

-   Worker daemon:
    
    -   Infinite loop:
        
        -   Poll
            
        -   If job → execute
            
        -   Upload result
            
    -   Heartbeat loop
        
    -   Local retry logic
        
    -   Local log file
        

That’s it.

----------

# ⏳ Realistic Time Estimate

With AI assistance:

Day 1:

-   Mac API + DB
    
-   Basic worker polling + execution
    

Day 2:

-   Heartbeat
    
-   Timeout reset logic
    
-   Log upload
    
-   Failure handling
    

Day 3 (optional):

-   Clean error states
    
-   Retry policy
    
-   Small CLI tool to enqueue jobs
    

You could technically brute-force a version in 1 long focused day.

But stable MVP?  
2–3 days realistic.

----------

# 🚫 What We Are NOT Building Yet

-   Multi-worker scheduling
    
-   Resource-aware allocation
    
-   Priority queues
    
-   Dashboard UI
    
-   Metrics system
    
-   Chunk-level checkpointing
    
-   gRPC
    
-   Message brokers
    
-   Distributed tracing
    

All of that is Phase 2+.

----------

# 🧠 Important: Why This Is Feasible

Because:

You are not building a distributed database.

You are building:

A persistent queue + pull worker + timeout recovery.

That’s manageable.