package hdcf

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	StatusPending   = "PENDING"
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

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
}

type HeartbeatRequest struct {
	WorkerID     string  `json:"worker_id"`
	CurrentJobID *string `json:"current_job_id"`
	Timestamp    string  `json:"ts"`
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
