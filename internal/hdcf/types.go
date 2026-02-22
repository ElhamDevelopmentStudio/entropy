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

type HeartbeatRequest struct {
	WorkerID     string  `json:"worker_id"`
	CurrentJobID *string `json:"current_job_id"`
	Timestamp    string  `json:"ts"`
}

type AckJobRequest struct {
	JobID        string `json:"job_id"`
	WorkerID     string `json:"worker_id"`
	AssignmentID string `json:"assignment_id"`
}

type CompleteRequest struct {
	JobID         string `json:"job_id"`
	WorkerID      string `json:"worker_id"`
	ExitCode      int    `json:"exit_code"`
	StdoutPath    string `json:"stdout_path"`
	StderrPath    string `json:"stderr_path"`
	ResultSummary string `json:"result_summary"`
}

type FailRequest struct {
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error"`
}

func IsValidStatus(status string) bool {
	switch status {
	case StatusPending, StatusAssigned, StatusRunning, StatusCompleted, StatusFailed, StatusLost, StatusRetrying, StatusAborted:
		return true
	default:
		return false
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
