package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
		token:            cfg.token,
		pollInterval:     cfg.pollInterval,
		heartbeatInterval: cfg.heartbeatInterval,
		requestTimeout:    cfg.requestTimeout,
		logDir:           cfg.logDir,
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
	go runner.heartbeatLoop(ctx)

	runner.loop(ctx)
}

type workerConfig struct {
	controlURL        string
	workerID          string
	token             string
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
}

func parseWorkerConfig() workerConfig {
	var cfg workerConfig
	flag.StringVar(&cfg.controlURL, "control-url", getenv("HDCF_CONTROL_URL", "http://localhost:8080"), "control plane url")
	flag.StringVar(&cfg.workerID, "worker-id", getenv("HDCF_WORKER_ID", ""), "worker id")
	flag.StringVar(&cfg.token, "token", getenv("HDCF_API_TOKEN", "dev-token"), "api token")
	var pollSec int
	var heartbeatSec int
	var timeoutSec int
	flag.IntVar(&pollSec, "poll-interval-seconds", 3, "poll interval seconds")
	flag.IntVar(&heartbeatSec, "heartbeat-interval-seconds", 5, "heartbeat interval seconds")
	flag.IntVar(&timeoutSec, "request-timeout-seconds", 10, "request timeout seconds")
	flag.StringVar(&cfg.logDir, "log-dir", "worker-logs", "path for local job logs")
	flag.Parse()

	cfg.pollInterval = time.Duration(pollSec) * time.Second
	cfg.heartbeatInterval = time.Duration(heartbeatSec) * time.Second
	cfg.requestTimeout = time.Duration(timeoutSec) * time.Second
	if strings.TrimSpace(cfg.workerID) == "" {
		host, _ := os.Hostname()
		cfg.workerID = fmt.Sprintf("%s-%s", host, hdcf.NewJobID())
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
	token             string
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
	currentJobID      atomic.Value
	httpClient        *http.Client
}

func (r *workerRunner) loop(ctx context.Context) {
	backoff := r.pollInterval
	for {
		select {
		case <-ctx.Done():
			return
		default:
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

		r.currentJobID.Store(job.JobID)
		r.executeJob(ctx, job)
		r.currentJobID.Store("")
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
			current := r.getCurrentJobID()
			if err := r.sendHeartbeat(ctx, current); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
			ticker.Reset(jitterDuration(r.heartbeatInterval))
		}
	}
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

func (r *workerRunner) executeJob(ctx context.Context, job *hdcf.AssignedJob) {
	start := time.Now()
	stdoutPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stdout.log", job.JobID))
	stderrPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stderr.log", job.JobID))

	stdout, err := os.Create(stdoutPath)
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	defer stdout.Close()

	stderr, err := os.Create(stderrPath)
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
			JobID:    job.JobID,
			WorkerID: r.workerID,
			ExitCode: exitCode,
			Error:    msg,
		}
		if retryWithBackoff(ctx, r.requestTimeout, func() error {
			return r.reportFail(ctx, failReq)
		}, 8) == nil {
			log.Printf("reported fail for job %s", job.JobID)
		}
		return
	}

	duration := time.Since(start).Milliseconds()
	summary := fmt.Sprintf("exit_code=0 duration_ms=%d", duration)
	compReq := hdcf.CompleteRequest{
		JobID:         job.JobID,
		WorkerID:      r.workerID,
		ExitCode:      exitCode,
		StdoutPath:    stdoutPath,
		StderrPath:    stderrPath,
		ResultSummary:  summary,
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportComplete(ctx, compReq)
	}, 8) == nil {
		log.Printf("job %s completed", job.JobID)
	}
}

func (r *workerRunner) handleJobFailure(ctx context.Context, job *hdcf.AssignedJob, err error) {
	failReq := hdcf.FailRequest{
		JobID:    job.JobID,
		WorkerID: r.workerID,
		ExitCode: -1,
		Error:    err.Error(),
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportFail(ctx, failReq)
	}, 8) == nil {
		log.Printf("reported startup failure for job %s", job.JobID)
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
