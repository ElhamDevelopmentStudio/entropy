package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/hex"
	"errors"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"hdcf/internal/hdcf"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	cfg := parseWorkerConfig()

	runner := &workerRunner{
		controlURL:       strings.TrimRight(cfg.controlURL, "/"),
		workerID:         cfg.workerID,
		nonce:            cfg.nonce,
		token:            cfg.token,
		pollInterval:     cfg.pollInterval,
		heartbeatInterval: cfg.heartbeatInterval,
		requestTimeout:    cfg.requestTimeout,
		logDir:           cfg.logDir,
		stateFile:        cfg.stateFile,
	}
	if runner.logDir == "" {
		runner.logDir = "worker-logs"
	}
	if err := os.MkdirAll(runner.logDir, 0o755); err != nil {
		log.Fatalf("log directory: %v", err)
	}
	client := &http.Client{Timeout: cfg.requestTimeout}
	runner.httpClient = client

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.register(ctx); err != nil {
		log.Printf("worker registration failed: %v", err)
	}
	if err := runner.reconnect(ctx); err != nil {
		log.Printf("startup reconnect failed: %v", err)
	}
	go runner.heartbeatLoop(ctx)

	runner.loop(ctx)
}

type workerConfig struct {
	controlURL        string
	workerID          string
	nonce             string
	token             string
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
	stateFile         string
}

func parseWorkerConfig() workerConfig {
	var cfg workerConfig
	flag.StringVar(&cfg.controlURL, "control-url", getenv("HDCF_CONTROL_URL", "http://localhost:8080"), "control plane url")
	flag.StringVar(&cfg.workerID, "worker-id", getenv("HDCF_WORKER_ID", ""), "worker id")
	flag.StringVar(&cfg.nonce, "worker-nonce", getenv("HDCF_WORKER_NONCE", ""), "optional registration nonce")
	flag.StringVar(&cfg.token, "token", getenv("HDCF_API_TOKEN", "dev-token"), "api token")
	var pollSec int
	var heartbeatSec int
	var timeoutSec int
	flag.IntVar(&pollSec, "poll-interval-seconds", 3, "poll interval seconds")
	flag.IntVar(&heartbeatSec, "heartbeat-interval-seconds", 5, "heartbeat interval seconds")
	flag.IntVar(&timeoutSec, "request-timeout-seconds", 10, "request timeout seconds")
	flag.StringVar(&cfg.logDir, "log-dir", "worker-logs", "path for local job logs")
	flag.StringVar(&cfg.stateFile, "state-file", "", "path to reconnect state file (default worker-logs/worker-state.json)")
	flag.Parse()

	cfg.pollInterval = time.Duration(pollSec) * time.Second
	cfg.heartbeatInterval = time.Duration(heartbeatSec) * time.Second
	cfg.requestTimeout = time.Duration(timeoutSec) * time.Second
	if strings.TrimSpace(cfg.workerID) == "" {
		host, _ := os.Hostname()
		cfg.workerID = fmt.Sprintf("%s-%s", host, hdcf.NewJobID())
	}
	if strings.TrimSpace(cfg.stateFile) == "" {
		cfg.stateFile = filepath.Join(cfg.logDir, "worker-state.json")
	}
	return cfg
}

func getenv(name, fallback string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	return v
}

type workerRunner struct {
	controlURL        string
	workerID          string
	nonce             string
	token             string
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
	stateFile         string
	currentJobID      atomic.Value
	httpClient        *http.Client
}

type workerReconnectState struct {
	CurrentJobID      string                     `json:"current_job_id"`
	CurrentAssignmentID string                    `json:"current_assignment_id"`
	CompletedJobs     []hdcf.ReconnectCompletedJob `json:"completed_jobs"`
}

func (r *workerRunner) loop(ctx context.Context) {
	backoff := r.pollInterval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := r.register(ctx); err != nil {
			log.Printf("worker registration failed: %v", err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}

		job, err := r.nextJob(ctx)
		if err != nil {
			log.Printf("next-job: %v", err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		if job == nil {
			time.Sleep(jitterDuration(r.pollInterval))
			backoff = r.pollInterval
			continue
		}
		if err := r.ackJob(ctx, job); err != nil {
			log.Printf("ack-job %s: %v", job.JobID, err)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}

		if err := r.setCurrentReconnectState(job.JobID, job.AssignmentID); err != nil {
			log.Printf("save current job %s: %v", job.JobID, err)
		}
		r.currentJobID.Store(job.JobID)
		r.executeJob(ctx, job)
		r.currentJobID.Store("")
		if err := r.clearCurrentReconnectState(); err != nil {
			log.Printf("clear current job: %v", err)
		}
		backoff = r.pollInterval
	}
}

func (r *workerRunner) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(jitterDuration(r.heartbeatInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.register(ctx); err != nil {
				log.Printf("worker registration failed: %v", err)
				continue
			}
			current := r.getCurrentJobID()
			if err := r.sendHeartbeat(ctx, current); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
			ticker.Reset(jitterDuration(r.heartbeatInterval))
		}
	}
}

func (r *workerRunner) reconnect(ctx context.Context) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}

	req := hdcf.WorkerReconnectRequest{
		WorkerID:      r.workerID,
		CompletedJobs: state.CompletedJobs,
	}
	if strings.TrimSpace(state.CurrentJobID) != "" {
		currentJobID := state.CurrentJobID
		req.CurrentJobID = &currentJobID
	}
	if len(req.CompletedJobs) == 0 && req.CurrentJobID == nil {
		req.CurrentJobID = nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	resp, err := r.postJSONWithResponse(reqCtx, r.controlURL+"/reconnect", req)
	if err != nil {
		return err
	}

	for _, action := range resp.Actions {
		switch action.Action {
		case hdcf.ReconnectActionClearCurrentJob:
			if action.JobID == "" || action.JobID == state.CurrentJobID {
				_ = r.clearCurrentReconnectState()
			}
		case hdcf.ReconnectActionReplayCompleted, hdcf.ReconnectActionReplayFailed:
			if action.Result == hdcf.ReconnectResultAccepted {
				_ = r.removeCompletedReconnectResult(action.JobID, action.AssignmentID)
			}
		}
	}
	return nil
}

func (r *workerRunner) getCurrentJobID() *string {
	v := r.currentJobID.Load()
	if v == nil {
		return nil
	}
	id, ok := v.(string)
	if !ok || strings.TrimSpace(id) == "" {
		return nil
	}
	return &id
}

func (r *workerRunner) register(ctx context.Context) error {
	req := hdcf.WorkerRegisterRequest{
		WorkerID: r.workerID,
		Nonce:    r.nonce,
	}
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/register", r.controlURL)
	return retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.postJSON(ctx, endpoint, payload)
	}, 4)
}

func (r *workerRunner) nextJob(ctx context.Context) (*hdcf.AssignedJob, error) {
	ctx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()

	endpoint := fmt.Sprintf("%s/next-job?worker_id=%s", r.controlURL, r.workerID)
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	values := u.Query()
	values.Set("worker_id", r.workerID)
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(authHeader, r.token)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		return nil, fmt.Errorf("next-job status=%s body=%s", resp.Status, body)
	}
	var job hdcf.AssignedJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *workerRunner) ackJob(ctx context.Context, job *hdcf.AssignedJob) error {
	req := hdcf.AckJobRequest{
		JobID:        job.JobID,
		WorkerID:     r.workerID,
		AssignmentID: job.AssignmentID,
	}
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/ack", r.controlURL)
	return retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.postJSON(ctx, endpoint, payload)
	}, 8)
}

func (r *workerRunner) sendHeartbeat(ctx context.Context, currentJobID *string) error {
	body := hdcf.HeartbeatRequest{
		WorkerID:     r.workerID,
		CurrentJobID: currentJobID,
		Timestamp:    time.Now().Format(time.RFC3339),
	}
	payload, _ := json.Marshal(body)
	endpoint := fmt.Sprintf("%s/heartbeat", r.controlURL)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.token)
	ctx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		return fmt.Errorf("heartbeat status=%s body=%s", resp.Status, body)
	}
	return nil
}

func (r *workerRunner) reportComplete(ctx context.Context, req hdcf.CompleteRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/complete", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		return err
	}
	return nil
}

func (r *workerRunner) reportFail(ctx context.Context, req hdcf.FailRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/fail", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		return err
	}
	return nil
}

func (r *workerRunner) postJSON(ctx context.Context, endpoint string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.token)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		return fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}
	return nil
}

func (r *workerRunner) postJSONWithResponse(ctx context.Context, endpoint string, payload interface{}) (hdcf.WorkerReconnectResponse, error) {
	var zero hdcf.WorkerReconnectResponse
	body, err := json.Marshal(payload)
	if err != nil {
		return zero, err
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(body))
	if err != nil {
		return zero, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.token)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		return zero, fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}

	var parsed hdcf.WorkerReconnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return zero, err
	}
	return parsed, nil
}

func (r *workerRunner) executeJob(ctx context.Context, job *hdcf.AssignedJob) {
	start := time.Now()
	stdoutTmpPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stdout.log.tmp", job.JobID))
	stderrTmpPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stderr.log.tmp", job.JobID))
	stdoutPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stdout.log", job.JobID))
	stderrPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stderr.log", job.JobID))

	stdout, err := os.Create(stdoutTmpPath)
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	defer stdout.Close()

	stderr, err := os.Create(stderrTmpPath)
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	defer stderr.Close()

	timeout := time.Duration(job.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, job.Command, job.Args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if strings.TrimSpace(job.WorkingDir) != "" {
		cmd.Dir = job.WorkingDir
	}

	log.Printf("worker %s running job %s: %s", r.workerID, job.JobID, job.Command)
	err = cmd.Start()
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}

	err = cmd.Wait()
	exitCode := 0
	msg := ""
	if err != nil {
		exitCode = extractExitCode(err)
		msg = err.Error()
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			msg = "execution timeout"
			exitCode = -1
		}
		failReq := hdcf.FailRequest{
			JobID:        job.JobID,
			WorkerID:     r.workerID,
			AssignmentID: job.AssignmentID,
			ExitCode:    exitCode,
			Error:       msg,
		}
		reconnectEntry := hdcf.ReconnectCompletedJob{
			JobID:        job.JobID,
			AssignmentID:  job.AssignmentID,
			Status:       hdcf.StatusFailed,
			ExitCode:     failReq.ExitCode,
			StderrPath:   stderrTmpPath,
			StdoutPath:   stdoutTmpPath,
			Error:        msg,
		}
		if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
			log.Printf("record failed completion for job %s: %v", job.JobID, err)
		}
		if retryWithBackoff(ctx, r.requestTimeout, func() error {
			return r.reportFail(ctx, failReq)
		}, 8) == nil {
			log.Printf("reported fail for job %s", job.JobID)
			if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
				log.Printf("clear failed completion for job %s: %v", job.JobID, err)
			}
		}
		return
	}

	if err := r.finalizeArtifact(stdoutTmpPath, stdoutPath); err != nil {
		failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}
	if err := r.finalizeArtifact(stderrTmpPath, stderrPath); err != nil {
		_ = os.Rename(stdoutPath, stdoutTmpPath)
		failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}

	stdoutSHA256, err := r.hashArtifact(stdoutPath)
	if err != nil {
		failMsg := fmt.Sprintf("artifact hash failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}
	stderrSHA256, err := r.hashArtifact(stderrPath)
	if err != nil {
		failMsg := fmt.Sprintf("artifact hash failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}

	duration := time.Since(start).Milliseconds()
	summary := fmt.Sprintf("exit_code=0 duration_ms=%d", duration)
	artifactID := hdcf.NewJobID()
	compReq := hdcf.CompleteRequest{
		JobID:        job.JobID,
		WorkerID:     r.workerID,
		AssignmentID: job.AssignmentID,
		ExitCode:     exitCode,
		ArtifactID:   artifactID,
		StdoutPath:   stdoutPath,
		StderrPath:   stderrPath,
		StdoutTmpPath: stdoutTmpPath,
		StderrTmpPath: stderrTmpPath,
		StdoutSHA256: stdoutSHA256,
		StderrSHA256: stderrSHA256,
		ResultSummary: summary,
	}
	reconnectEntry := hdcf.ReconnectCompletedJob{
		JobID:         job.JobID,
		AssignmentID:   job.AssignmentID,
		Status:        hdcf.StatusCompleted,
		ExitCode:      exitCode,
		StdoutPath:    stdoutPath,
		StderrPath:    stderrPath,
		ArtifactID:    artifactID,
		StdoutTmpPath: stdoutTmpPath,
		StderrTmpPath: stderrTmpPath,
		StdoutSHA256:  stdoutSHA256,
		StderrSHA256:  stderrSHA256,
		ResultSummary: summary,
	}
	if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
		log.Printf("record completion for job %s: %v", job.JobID, err)
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportComplete(ctx, compReq)
	}, 8) == nil {
		log.Printf("job %s completed", job.JobID)
		if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
			log.Printf("clear completion for job %s: %v", job.JobID, err)
		}
	}
}

func (r *workerRunner) finalizeArtifact(tmpPath, finalPath string) error {
	_, err := os.Stat(tmpPath)
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

func (r *workerRunner) hashArtifact(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (r *workerRunner) loadReconnectState() (workerReconnectState, error) {
	state := workerReconnectState{}
	b, err := os.ReadFile(r.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, err
	}
	if err := json.Unmarshal(b, &state); err != nil {
		return state, err
	}
	return state, nil
}

func (r *workerRunner) persistReconnectState(state workerReconnectState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.stateFile), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(r.stateFile), ".hdcf-worker-state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	_, err = tmp.Write(raw)
	if err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, r.stateFile); err != nil {
		return err
	}
	return nil
}

func (r *workerRunner) setCurrentReconnectState(jobID, assignmentID string) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.CurrentJobID = strings.TrimSpace(jobID)
	state.CurrentAssignmentID = strings.TrimSpace(assignmentID)
	if state.CurrentJobID == "" {
		state.CurrentAssignmentID = ""
	}
	return r.persistReconnectState(state)
}

func (r *workerRunner) clearCurrentReconnectState() error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.CurrentJobID = ""
	state.CurrentAssignmentID = ""
	return r.persistReconnectState(state)
}

func (r *workerRunner) enqueueCompletedReconnectResult(entry hdcf.ReconnectCompletedJob) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}

	next := make([]hdcf.ReconnectCompletedJob, 0, len(state.CompletedJobs)+1)
	for _, existing := range state.CompletedJobs {
		if existing.JobID == entry.JobID && existing.AssignmentID == entry.AssignmentID && strings.TrimSpace(entry.JobID) != "" {
			continue
		}
		next = append(next, existing)
	}
	next = append(next, entry)
	state.CompletedJobs = next
	return r.persistReconnectState(state)
}

func (r *workerRunner) removeCompletedReconnectResult(jobID, assignmentID string) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	trimmedJob := strings.TrimSpace(jobID)
	trimmedAssignment := strings.TrimSpace(assignmentID)

	next := state.CompletedJobs[:0]
	for _, existing := range state.CompletedJobs {
		if existing.JobID == trimmedJob && existing.AssignmentID == trimmedAssignment {
			continue
		}
		next = append(next, existing)
	}
	state.CompletedJobs = next
	return r.persistReconnectState(state)
}

func (r *workerRunner) handleJobFailure(ctx context.Context, job *hdcf.AssignedJob, err error) {
	failReq := hdcf.FailRequest{
		JobID:       job.JobID,
		WorkerID:    r.workerID,
		AssignmentID: job.AssignmentID,
		ExitCode:    -1,
		Error:       err.Error(),
	}
	reconnectEntry := hdcf.ReconnectCompletedJob{
		JobID:       failReq.JobID,
		AssignmentID: failReq.AssignmentID,
		Status:      hdcf.StatusFailed,
		ExitCode:    failReq.ExitCode,
		Error:       failReq.Error,
	}
	if queueErr := r.enqueueCompletedReconnectResult(reconnectEntry); queueErr != nil {
		log.Printf("record startup failure for job %s: %v", job.JobID, queueErr)
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportFail(ctx, failReq)
	}, 8) == nil {
		log.Printf("reported startup failure for job %s", job.JobID)
		if queueErr := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); queueErr != nil {
			log.Printf("clear startup failure for job %s: %v", job.JobID, queueErr)
		}
	}
}

func retryWithBackoff(ctx context.Context, base time.Duration, fn func() error, attempts int) error {
	delay := base
	maxDelay := 30 * time.Second
	for i := 0; i < attempts; i++ {
		if err := fn(); err == nil {
			return nil
		} else if i == attempts-1 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitterDuration(delay)):
			delay = nextBackoff(delay, maxDelay)
		}
	}
	return nil
}

func extractExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(d/5) + 1))
	return d + jitter
}

func readLimitedBody(r io.Reader) string {
	const limit = 512
	buf := make([]byte, limit)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return ""
	}
	return strings.TrimSpace(string(buf[:n]))
}

const authHeader = "X-API-Token"
