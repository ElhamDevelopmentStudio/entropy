package hdcf

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	StatusPending   = "PENDING"
	StatusAssigned  = "ASSIGNED"
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
	StatusLost      = "LOST"
	StatusRetrying  = "RETRYING"
	StatusAborted   = "ABORTED"

	ReconnectActionKeepCurrentJob  = "KEEP_CURRENT_JOB"
	ReconnectActionClearCurrentJob = "CLEAR_CURRENT_JOB"
	ReconnectActionReplayCompleted = "REPLAY_COMPLETED"
	ReconnectActionReplayFailed    = "REPLAY_FAILED"
	ReconnectResultAccepted       = "ACCEPTED"
	ReconnectResultRejected       = "REJECTED"

	ArtifactStorageBackendLocal = "local"
	ArtifactStorageBackendNFS   = "nfs"
	ArtifactStorageBackendS3    = "s3"

	ArtifactUploadStateOK       = "ok"
	ArtifactUploadStatePending  = "pending"
	ArtifactUploadStateFailed   = "failed"
	ArtifactUploadStateSkipped  = "skipped"
	ArtifactUploadStateNoop     = "noop"
)

var validTransitions = map[string]map[string]bool{
	StatusPending: {
		StatusAssigned:  true,
		StatusRunning:   true,
		StatusFailed:    true,
		StatusAborted:   true,
		StatusRetrying:  true,
	},
	StatusAssigned: {
		StatusRunning:  true,
		StatusPending:  true,
		StatusFailed:   true,
		StatusAborted:  true,
	},
	StatusRunning: {
		StatusCompleted: true,
		StatusFailed:    true,
		StatusLost:      true,
		StatusAborted:   true,
		StatusRetrying:  true,
	},
	StatusCompleted: {
		StatusCompleted: true,
	},
	StatusFailed: {
		StatusFailed:   true,
		StatusPending:  true,
		StatusRetrying: true,
		StatusAborted:  true,
	},
	StatusLost: {
		StatusPending:  true,
		StatusRetrying: true,
		StatusFailed:   true,
		StatusAborted:  true,
	},
	StatusRetrying: {
		StatusPending:   true,
		StatusAssigned:  true,
		StatusRunning:   true,
		StatusAborted:   true,
		StatusFailed:    true,
	},
	StatusAborted: {
		StatusAborted: true,
	},
}

type CreateJobRequest struct {
	Command     string   `json:"command"`
	Args        []string `json:"args"`
	WorkingDir  string   `json:"working_dir"`
	MaxAttempts int      `json:"max_attempts"`
	TimeoutMs   int64    `json:"timeout_ms"`
	Priority    int      `json:"priority"`
	ScheduledAt int64    `json:"scheduled_at"`
	NeedsGPU    bool     `json:"needs_gpu"`
	Requirements []string `json:"requirements"`
}

type CreateJobResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type Job struct {
	JobID       string `json:"job_id"`
	Status      string `json:"status"`
	Command     string `json:"command"`
	Args        string `json:"-"`
	WorkingDir  string `json:"working_dir"`
	TimeoutMs   int64  `json:"timeout_ms"`
	AttemptCount int   `json:"attempt_count"`
	MaxAttempts int    `json:"max_attempts"`
	WorkerID    string `json:"worker_id"`
	LastError   string `json:"last_error"`
	ResultPath  string `json:"result_path"`
}

type JobRead struct {
	JobID              string   `json:"job_id"`
	Status             string   `json:"status"`
	Command            string   `json:"command"`
	Args               []string `json:"args"`
	WorkingDir         string   `json:"working_dir"`
	TimeoutMs          int64    `json:"timeout_ms"`
	Priority           int      `json:"priority"`
	ScheduledAt        int64    `json:"scheduled_at"`
	NeedsGPU           bool     `json:"needs_gpu"`
	Requirements       []string `json:"requirements"`
	CreatedAt          int64    `json:"created_at"`
	UpdatedAt          int64    `json:"updated_at"`
	AttemptCount       int      `json:"attempt_count"`
	MaxAttempts        int      `json:"max_attempts"`
	WorkerID           string   `json:"worker_id"`
	AssignmentID       string   `json:"assignment_id,omitempty"`
	AssignmentExpiresAt int64    `json:"assignment_expires_at"`
	LastError          string   `json:"last_error"`
	ResultPath         string   `json:"result_path"`
	UpdatedBy          string   `json:"updated_by"`
	ArtifactStorageBackend  string `json:"artifact_storage_backend"`
	ArtifactLocation        string `json:"artifact_location"`
	ArtifactUploadState     string `json:"artifact_upload_state"`
	ArtifactUploadError     string `json:"artifact_upload_error"`
	HeartbeatAgeSec    *int64   `json:"heartbeat_age_sec,omitempty"`
}

type AssignedJob struct {
	JobID       string   `json:"job_id"`
	Command     string   `json:"command"`
	Args        []string `json:"args"`
	WorkingDir  string   `json:"working_dir"`
	TimeoutMs   int64    `json:"timeout_ms"`
	AttemptCount int     `json:"attempt_count"`
	MaxAttempts int      `json:"max_attempts"`
	AssignmentID string  `json:"assignment_id"`
	// Unix timestamp in seconds. If empty/zero, caller should treat as immediate expiry.
	AssignmentExpiresAt int64 `json:"assignment_expires_at"`
}

type WorkerRead struct {
	WorkerID          string `json:"worker_id"`
	Status            string `json:"status"`
	CurrentJobID      string `json:"current_job_id"`
	Capabilities      []string `json:"capabilities"`
	HeartbeatMetrics  map[string]interface{} `json:"heartbeat_metrics,omitempty"`
	LastSeen          int64  `json:"last_seen"`
	RegisteredAt      int64  `json:"registered_at"`
	HeartbeatAgeSec   int64  `json:"heartbeat_age_sec"`
	RegistrationNonce string `json:"registration_nonce"`
}

type HeartbeatRequest struct {
	WorkerID     string  `json:"worker_id"`
	CurrentJobID *string `json:"current_job_id"`
	Timestamp    string  `json:"ts"`
	Sequence     int64   `json:"seq"`
	Metrics      *HeartbeatMetrics `json:"metrics,omitempty"`
}

type HeartbeatMetrics struct {
	CPUUsagePercent   *float64 `json:"cpu_usage_percent,omitempty"`
	MemoryUsageMB     *float64 `json:"memory_usage_mb,omitempty"`
	GPUUsagePercent   *float64 `json:"gpu_usage_percent,omitempty"`
	GPUMemoryUsageMB  *float64 `json:"gpu_memory_usage_mb,omitempty"`
}

type RegisterWorkerRequest struct {
	WorkerID     string   `json:"worker_id"`
	Nonce        string   `json:"nonce"`
	Capabilities []string `json:"capabilities"`
}

type AckJobRequest struct {
	JobID        string `json:"job_id"`
	WorkerID     string `json:"worker_id"`
	AssignmentID string `json:"assignment_id"`
}

type CompleteRequest struct {
	JobID         string `json:"job_id"`
	WorkerID      string `json:"worker_id"`
	AssignmentID  string `json:"assignment_id"`
	ArtifactID    string `json:"artifact_id"`
	ExitCode      int    `json:"exit_code"`
	StdoutPath    string `json:"stdout_path"`
	StderrPath    string `json:"stderr_path"`
	StdoutTmpPath string `json:"stdout_tmp_path"`
	StderrTmpPath string `json:"stderr_tmp_path"`
	StdoutSHA256  string `json:"stdout_sha256"`
	StderrSHA256  string `json:"stderr_sha256"`
	ResultSummary string `json:"result_summary"`
	CompletionSeq int64  `json:"completion_seq"`
	ArtifactBackend string `json:"artifact_backend"`
	ArtifactLocation string `json:"artifact_location"`
	ArtifactUploadState string `json:"artifact_upload_state"`
	ArtifactUploadError string `json:"artifact_upload_error"`
}

type FailRequest struct {
	JobID       string `json:"job_id"`
	WorkerID    string `json:"worker_id"`
	AssignmentID string `json:"assignment_id"`
	ExitCode    int    `json:"exit_code"`
	Error       string `json:"error"`
}

type AbortRequest struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	Reason   string `json:"reason"`
}

type ReconnectCompletedJob struct {
	JobID         string `json:"job_id"`
	AssignmentID  string `json:"assignment_id"`
	CompletionSeq int64  `json:"completion_seq"`
	ArtifactID    string `json:"artifact_id"`
	Status        string `json:"status"`
	ExitCode      int    `json:"exit_code"`
	StdoutPath    string `json:"stdout_path"`
	StderrPath    string `json:"stderr_path"`
	StdoutTmpPath string `json:"stdout_tmp_path"`
	StderrTmpPath string `json:"stderr_tmp_path"`
	StdoutSHA256  string `json:"stdout_sha256"`
	StderrSHA256  string `json:"stderr_sha256"`
	ArtifactBackend string `json:"artifact_backend"`
	ArtifactLocation string `json:"artifact_location"`
	ArtifactUploadState string `json:"artifact_upload_state"`
	ArtifactUploadError string `json:"artifact_upload_error"`
	ResultSummary string `json:"result_summary"`
	Error         string `json:"error"`
}

type WorkerReconnectRequest struct {
	WorkerID      string                  `json:"worker_id"`
	CurrentJobID  *string                 `json:"current_job_id"`
	CompletedJobs []ReconnectCompletedJob  `json:"completed_jobs"`
}

type ReconnectAction struct {
	JobID        string `json:"job_id"`
	AssignmentID string `json:"assignment_id"`
	Action       string `json:"action"`
	Result       string `json:"result"`
	Error        string `json:"error,omitempty"`
}

type WorkerReconnectResponse struct {
	Status  string            `json:"status"`
	Actions []ReconnectAction `json:"actions"`
}

type AuditEvent struct {
	ID        int64                  `json:"id"`
	Timestamp int64                  `json:"ts"`
	Component string                 `json:"component"`
	Level     string                 `json:"level"`
	Event     string                 `json:"event"`
	RequestID string                 `json:"request_id,omitempty"`
	WorkerID  string                 `json:"worker_id,omitempty"`
	JobID     string                 `json:"job_id,omitempty"`
	Details   map[string]interface{} `json:"details"`
}

func IsValidStatus(status string) bool {
	switch status {
	case StatusPending, StatusAssigned, StatusRunning, StatusCompleted, StatusFailed, StatusLost, StatusRetrying, StatusAborted:
		return true
	default:
		return false
	}
}

func NormalizeArtifactStorageBackend(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ArtifactStorageBackendLocal
	}
	switch raw {
	case "nfs-share", "nfs_share":
		return ArtifactStorageBackendNFS
	case "s3", "s3-compatible", "s3_compatible":
		return ArtifactStorageBackendS3
	case ArtifactStorageBackendLocal, ArtifactStorageBackendNFS, ArtifactStorageBackendS3:
		return raw
	default:
		return raw
	}
}

func IsValidArtifactStorageBackend(raw string) bool {
	switch NormalizeArtifactStorageBackend(raw) {
	case ArtifactStorageBackendLocal, ArtifactStorageBackendNFS, ArtifactStorageBackendS3:
		return true
	default:
		return false
	}
}

func NormalizeArtifactUploadState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ArtifactUploadStatePending
	case ArtifactUploadStateFailed, ArtifactUploadStateOK, ArtifactUploadStateSkipped, ArtifactUploadStateNoop:
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func IsValidTransition(from, to string) bool {
	nexts, ok := validTransitions[from]
	if !ok {
		return false
	}
	return nexts[to]
}

func NewJobID() string {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%04x%08x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		binary.BigEndian.Uint16(b[10:12]),
		binary.BigEndian.Uint32(b[12:16]))
}
