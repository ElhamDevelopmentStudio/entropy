# 📘 Software Requirements Specification (SRS)

## Project Title

**Home Distributed Compute Fabric (HDCF)**  
Version 1.0  
Environment: Intermittent Infrastructure / Power-Unstable Regions

----------

# 1. Introduction

## 1.1 Purpose

This document defines the functional and non-functional requirements for a two-node distributed compute system consisting of:

-   **Control Plane Node**: Mac mini M2 (always-on, UPS-backed)
    
-   **Worker Node**: ASUS ROG Scar 15 (GPU compute node)
    

The system is designed to operate reliably in environments with:

-   Frequent power outages
    
-   Wi-Fi interruptions
    
-   Router reboots
    
-   DHCP/IP instability
    
-   Intermittent internet connectivity
    

The primary objective is to provide durable, resumable, and eventually consistent task execution across unreliable local infrastructure.

----------

## 1.2 Scope

HDCF provides:

-   Durable job scheduling
    
-   Worker execution coordination
    
-   Heartbeat-based liveness detection
    
-   Crash recovery
    
-   Eventual consistency after disconnection
    
-   Local-only, LAN-first architecture
    
-   Artifact management
    
-   Retry and resumption logic
    

The system does NOT include:

-   Cloud integration
    
-   Kubernetes or container orchestration platforms
    
-   Internet-dependent authentication
    
-   Multi-region clustering
    

----------

# 2. Overall System Description

## 2.1 System Architecture

The system consists of two logical roles:

### 2.1.1 Control Plane (Mac mini)

Responsibilities:

-   Persist job queue and job state
    
-   Assign tasks to workers
    
-   Monitor worker liveness via heartbeat
    
-   Store job metadata and logs
    
-   Manage retries and recovery
    
-   Provide dashboard interface (optional MVP)
    

The Control Plane must remain operational during:

-   Internet outage
    
-   Worker disconnection
    
-   Router reboot
    

----------

### 2.1.2 Worker Node (ASUS GPU)

Responsibilities:

-   Accept tasks
    
-   Execute compute workloads
    
-   Emit progress and logs
    
-   Send heartbeats
    
-   Buffer logs during network loss
    
-   Resume or report crashed tasks on restart
    

The Worker must operate correctly under:

-   Temporary Wi-Fi loss
    
-   Sudden process crash
    
-   Power interruption
    
-   Router reboot
    

----------

# 3. Environmental Assumptions

The system SHALL assume:

-   Power outages occur unpredictably
    
-   Router/AP may reboot unexpectedly
    
-   DHCP leases may change IP addresses
    
-   Wi-Fi may drop for 1–10 minutes
    
-   Internet may be unavailable
    
-   Time synchronization may drift during outages
    

The system SHALL NOT assume:

-   Stable persistent connections
    
-   Guaranteed message delivery
    
-   Exactly-once network communication
    
-   Continuous LAN availability
    

----------

# 4. Functional Requirements

----------

## 4.1 Job Management

### 4.1.1 Job States

Each job SHALL have a persistent state machine:

-   PENDING
    
-   ASSIGNED
    
-   RUNNING
    
-   COMPLETED
    
-   FAILED
    
-   LOST
    
-   RETRYING
    
-   ABORTED
    

State transitions SHALL be durable and atomic.

----------

### 4.1.2 Job Assignment

When assigning a job:

1.  Control Plane sets state to ASSIGNED.
    
2.  Worker must ACK receipt.
    
3.  Only after ACK SHALL job transition to RUNNING.
    

If ACK is not received within timeout:

-   Job SHALL revert to PENDING.
    

----------

### 4.1.3 Idempotency

Each job SHALL have:

-   Globally unique TaskID
    
-   Deterministic output path
    
-   Ability to detect already-completed execution
    

Duplicate execution SHALL NOT corrupt output.

----------

## 4.2 Worker Heartbeat Protocol

Worker SHALL:

-   Send heartbeat every X seconds (configurable)
    
-   Include:
    
    -   Worker ID
        
    -   Current TaskID
        
    -   Resource metrics (optional MVP)
        

Control Plane SHALL:

-   Mark worker OFFLINE if heartbeat not received within threshold
    
-   Mark RUNNING jobs as LOST if worker offline beyond threshold
    

----------

## 4.3 Network Interruption Handling

### 4.3.1 During Job Execution

If network disconnects:

Worker SHALL:

-   Continue execution
    
-   Buffer logs locally
    
-   Retry connection periodically
    
-   Upon reconnect:
    
    -   Flush buffered logs
        
    -   Reconfirm job status
        

Control Plane SHALL:

-   Mark worker as DISCONNECTED
    
-   Not cancel job immediately
    

----------

### 4.3.2 During Assignment

If network fails mid-assignment:

-   Job SHALL either:
    
    -   Remain unassigned, or
        
    -   Be safely re-assignable
        

No job SHALL enter inconsistent partial state.

----------

## 4.4 Crash Recovery

### 4.4.1 Worker Crash

Upon worker restart:

Worker SHALL:

-   Report last running TaskID
    
-   Report completion state if finished
    
-   Allow Control Plane to reconcile
    

Control Plane SHALL:

-   Detect stale job
    
-   Reassign if necessary
    

----------

### 4.4.2 Control Plane Restart

Upon Control Plane restart:

-   All job states SHALL persist
    
-   Worker re-registration SHALL occur automatically
    

----------

## 4.5 Artifact Management

Artifacts SHALL:

-   Be written atomically (temp file → rename)
    
-   Include integrity check (checksum optional in MVP)
    
-   Be stored in durable local storage
    

Partial artifacts SHALL NOT be treated as completed.

----------

## 4.6 Reconnection Reconciliation Protocol

Upon worker reconnection:

Worker SHALL send:

-   Worker ID
    
-   Current running task (if any)
    
-   List of completed tasks since disconnect
    
-   Buffered logs
    

Control Plane SHALL:

-   Reconcile state
    
-   Accept late completion safely
    
-   Resolve conflicts deterministically
    

----------

# 5. Non-Functional Requirements

----------

## 5.1 Reliability

System SHALL tolerate:

-   Wi-Fi drop ≤ 10 minutes
    
-   Router reboot
    
-   Worker power loss
    
-   Duplicate network messages
    
-   Message reordering
    

----------

## 5.2 Durability

Job queue SHALL survive:

-   Control Plane reboot
    
-   Power outage
    
-   Process crash
    

----------

## 5.3 Eventual Consistency

System SHALL guarantee:

-   Correct final job state
    
-   No permanent inconsistent state
    
-   Deterministic reconciliation
    

----------

## 5.4 Security

System SHALL:

-   Operate LAN-only
    
-   Use authenticated worker registration
    
-   Avoid external identity providers
    

----------

## 5.5 Scalability (Future-Ready)

Architecture SHALL allow:

-   Multiple worker nodes
    
-   Worker capability metadata
    
-   Priority scheduling
    

----------

# 6. Failure Modes & Recovery Matrix

Failure Event

Expected Behavior

Wi-Fi drop mid-job

Worker continues, logs buffered

Router reboot

Automatic reconnection

Worker power loss

Job marked LOST, retryable

Duplicate completion message

Ignored safely

Control Plane restart

State restored

Partial artifact write

Not accepted

----------

# 7. Testing Requirements

System SHALL be tested by:

-   Manually unplugging router mid-job
    
-   Killing worker process mid-run
    
-   Power cycling ASUS during execution
    
-   Restarting Mac mini during active job
    
-   Forcing duplicate completion messages