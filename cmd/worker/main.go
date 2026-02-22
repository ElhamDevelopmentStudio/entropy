package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/base64"
	"encoding/hex"
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
	"runtime"
	"strconv"
	"sync"
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
		capabilities:     cfg.capabilities,
		token:            cfg.token,
		workerTokenSecret: cfg.workerTokenSecret,
		workerTokenTTL:    cfg.workerTokenTTL,
		pollInterval:     cfg.pollInterval,
		heartbeatInterval: cfg.heartbeatInterval,
		requestTimeout:    cfg.requestTimeout,
		logDir:           cfg.logDir,
		stateFile:        cfg.stateFile,
		logRetentionDays:  cfg.logRetentionDays,
		logCleanupInterval: cfg.logCleanupInterval,
		reportMetrics:     cfg.reportMetrics,
	}
	if runner.logDir == "" {
		runner.logDir = "worker-logs"
	}
	if err := os.MkdirAll(runner.logDir, 0o755); err != nil {
		log.Fatalf("log directory: %v", err)
	}
	client, err := buildWorkerHTTPClient(cfg)
	if err != nil {
		log.Fatalf("http client: %v", err)
	}
	runner.httpClient = client

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.register(ctx); err != nil {
		workerEvent("warn", "worker.register", map[string]any{
			"worker_id": cfg.workerID,
			"action":    "initial",
			"error":     err.Error(),
		})
	} else {
		workerEvent("info", "worker.register", map[string]any{
			"worker_id": cfg.workerID,
			"action":    "initial",
			"status":    "ok",
		})
	}
	if err := runner.reconnect(ctx); err != nil {
		workerEvent("warn", "worker.reconnect", map[string]any{
			"worker_id": cfg.workerID,
			"scope":     "startup",
			"error":     err.Error(),
		})
	} else {
		workerEvent("info", "worker.reconnect", map[string]any{
			"worker_id": cfg.workerID,
			"scope":     "startup",
			"status":    "ok",
		})
	}
	workerEvent("info", "worker.loop_start", map[string]any{
		"worker_id":        cfg.workerID,
		"control_endpoint": strings.TrimRight(cfg.controlURL, "/"),
	})
	go runner.heartbeatLoop(ctx)
	go runner.logCleanupLoop(ctx)

	runner.loop(ctx)
}

type workerConfig struct {
	controlURL        string
	workerID          string
	nonce             string
	capabilities      []string
	token             string
	workerTokenSecret string
	workerTokenTTL    time.Duration
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
	stateFile         string
	logRetentionDays  int
	logCleanupInterval time.Duration
	reportMetrics     bool
	tlsCA            string
	tlsClientCert    string
	tlsClientKey     string
}

func parseWorkerConfig() workerConfig {
	var cfg workerConfig
	var token string
	flag.StringVar(&cfg.controlURL, "control-url", getenv("HDCF_CONTROL_URL", "http://localhost:8080"), "control plane url")
	flag.StringVar(&cfg.workerID, "worker-id", getenv("HDCF_WORKER_ID", ""), "worker id")
	flag.StringVar(&cfg.nonce, "worker-nonce", getenv("HDCF_WORKER_NONCE", ""), "optional registration nonce")
	capabilities := flag.String("capabilities", getenv("HDCF_WORKER_CAPABILITIES", ""), "comma-separated worker capabilities")
	flag.StringVar(&token, "token", getenv("HDCF_API_TOKEN", "dev-token"), "legacy api token")
	var workerToken string
	flag.StringVar(&workerToken, "worker-token", getenv("HDCF_WORKER_TOKEN", ""), "worker token")
	flag.StringVar(&cfg.workerTokenSecret, "worker-token-secret", getenv("HDCF_WORKER_TOKEN_SECRET", ""), "secret for signed worker tokens")
	var tokenTTLSeconds int
	flag.IntVar(&tokenTTLSeconds, "worker-token-ttl-seconds", int(getenvInt("HDCF_WORKER_TOKEN_TTL_SECONDS", 3600)), "signed worker token ttl seconds")
	var pollSec int
	var heartbeatSec int
	var timeoutSec int
	var logRetentionDays int
	var logCleanupIntervalSec int
	flag.IntVar(&pollSec, "poll-interval-seconds", 3, "poll interval seconds")
	flag.IntVar(&heartbeatSec, "heartbeat-interval-seconds", 5, "heartbeat interval seconds")
	flag.BoolVar(&cfg.reportMetrics, "heartbeat-metrics", false, "include optional resource metrics in heartbeat payload")
	flag.IntVar(&timeoutSec, "request-timeout-seconds", 10, "request timeout seconds")
	flag.IntVar(&logRetentionDays, "log-retention-days", int(getenvInt("HDCF_WORKER_LOG_RETENTION_DAYS", 30)), "days to retain worker local log/artifact files")
	flag.IntVar(&logCleanupIntervalSec, "log-cleanup-interval-seconds", int(getenvInt("HDCF_WORKER_LOG_CLEANUP_INTERVAL_SECONDS", 300)), "log cleanup interval seconds")
	flag.StringVar(&cfg.logDir, "log-dir", "worker-logs", "path for local job logs")
	flag.StringVar(&cfg.stateFile, "state-file", "", "path to reconnect state file (default worker-logs/worker-state.json)")
	flag.StringVar(&cfg.tlsCA, "tls-ca", getenv("HDCF_TLS_CA", ""), "trusted control plane CA certificate")
	flag.StringVar(&cfg.tlsClientCert, "tls-client-cert", getenv("HDCF_TLS_CLIENT_CERT", ""), "client certificate for mTLS")
	flag.StringVar(&cfg.tlsClientKey, "tls-client-key", getenv("HDCF_TLS_CLIENT_KEY", ""), "client private key for mTLS")
	flag.Parse()

	cfg.token = strings.TrimSpace(workerToken)
	if cfg.token == "" {
		cfg.token = strings.TrimSpace(token)
	}
	if tokenTTLSeconds < 1 {
		tokenTTLSeconds = 1
	}
	cfg.workerTokenTTL = time.Duration(tokenTTLSeconds) * time.Second
	cfg.pollInterval = time.Duration(pollSec) * time.Second
	cfg.heartbeatInterval = time.Duration(heartbeatSec) * time.Second
	cfg.requestTimeout = time.Duration(timeoutSec) * time.Second
	cfg.logRetentionDays = logRetentionDays
	cfg.logCleanupInterval = time.Duration(logCleanupIntervalSec) * time.Second
	if strings.TrimSpace(cfg.workerID) == "" {
		host, _ := os.Hostname()
		cfg.workerID = fmt.Sprintf("%s-%s", host, hdcf.NewJobID())
	}
	if strings.TrimSpace(cfg.stateFile) == "" {
		cfg.stateFile = filepath.Join(cfg.logDir, "worker-state.json")
	}
	cfg.capabilities = splitCapabilities(*capabilities)
	return cfg
}

func buildWorkerHTTPClient(cfg workerConfig) (*http.Client, error) {
	transport := &http.Transport{}
	if !strings.HasPrefix(cfg.controlURL, "https://") {
		return &http.Client{Timeout: cfg.requestTimeout, Transport: transport}, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if cfg.tlsCA != "" {
		caBundle, err := os.ReadFile(cfg.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("read tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBundle) {
			return nil, fmt.Errorf("parse tls ca: no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if (cfg.tlsClientCert != "" && cfg.tlsClientKey == "") || (cfg.tlsClientCert == "" && cfg.tlsClientKey != "") {
		return nil, fmt.Errorf("both -tls-client-cert and -tls-client-key are required when using client certificate auth")
	}
	if cfg.tlsClientCert != "" && cfg.tlsClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.tlsClientCert, cfg.tlsClientKey)
		if err != nil {
			return nil, fmt.Errorf("load tls client keypair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Timeout: cfg.requestTimeout, Transport: transport}, nil
}

func getenv(name, fallback string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	return v
}

func getenvInt(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCapabilities(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

type workerRunner struct {
	controlURL        string
	workerID          string
	nonce             string
	capabilities      []string
	token             string
	workerTokenSecret string
	workerTokenTTL    time.Duration
	pollInterval      time.Duration
	heartbeatInterval time.Duration
	requestTimeout    time.Duration
	logDir            string
	stateFile         string
	logRetentionDays   int
	logCleanupInterval time.Duration
	currentJobID      atomic.Value
	httpClient        *http.Client
	stateMu           sync.Mutex
	heartbeatSeq      int64
	completionSeq     int64
	reportMetrics     bool
}

type workerReconnectState struct {
	CurrentJobID      string                     `json:"current_job_id"`
	CurrentAssignmentID string                    `json:"current_assignment_id"`
	CompletedJobs     []hdcf.ReconnectCompletedJob `json:"completed_jobs"`
	HeartbeatSeq      int64                      `json:"heartbeat_seq"`
	LastCompletionSeq int64                      `json:"last_completion_seq"`
}

func (r *workerRunner) loop(ctx context.Context) {
	backoff := r.pollInterval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		workerEvent("debug", "worker.loop_cycle", map[string]any{
			"worker_id": r.workerID,
			"backoff_ms": backoff.Milliseconds(),
		})
		if err := r.register(ctx); err != nil {
			workerEvent("warn", "worker.register", map[string]any{
				"worker_id": r.workerID,
				"action":    "loop",
				"error":     err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		workerEvent("debug", "worker.register", map[string]any{
			"worker_id": r.workerID,
			"action":    "loop",
			"status":    "ok",
		})

		job, err := r.nextJob(ctx)
		if err != nil {
			workerEvent("warn", "worker.next_job", map[string]any{
				"worker_id": r.workerID,
				"error":     err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		if job == nil {
			workerEvent("debug", "worker.next_job", map[string]any{
				"worker_id": r.workerID,
				"status":    "no_job",
			})
			time.Sleep(jitterDuration(r.pollInterval))
			backoff = r.pollInterval
			continue
		}
		workerEvent("info", "worker.next_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"attempt_count": job.AttemptCount,
			"command":       job.Command,
		})
		if err := r.ackJob(ctx, job); err != nil {
			workerEvent("warn", "worker.ack_job", map[string]any{
				"worker_id":     r.workerID,
				"job_id":        job.JobID,
				"assignment_id": job.AssignmentID,
				"error":         err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		workerEvent("info", "worker.ack_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
		})

		if err := r.setCurrentReconnectState(job.JobID, job.AssignmentID); err != nil {
			workerEvent("warn", "worker.state_persist", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
			})
		}
		r.currentJobID.Store(job.JobID)
		r.executeJob(ctx, job)
		r.currentJobID.Store("")
		if err := r.clearCurrentReconnectState(); err != nil {
			workerEvent("warn", "worker.state_persist", map[string]any{
				"worker_id": r.workerID,
				"current_job_id": "",
				"error":     err.Error(),
			})
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
			workerEvent("debug", "worker.heartbeat_cycle", map[string]any{
				"worker_id": r.workerID,
			})
			if err := r.register(ctx); err != nil {
				workerEvent("warn", "worker.register", map[string]any{
					"worker_id": r.workerID,
					"action":    "heartbeat",
					"error":     err.Error(),
				})
				continue
			}
			current := r.getCurrentJobID()
			if err := r.sendHeartbeat(ctx, current); err != nil {
				workerEvent("warn", "worker.heartbeat", map[string]any{
					"worker_id": r.workerID,
					"error":     err.Error(),
				})
				continue
			}
			workerEvent("info", "worker.heartbeat", map[string]any{
				"worker_id":  r.workerID,
				"current_job_id": current,
			})
			if err := r.flushPendingReconnectResults(ctx); err != nil {
				workerEvent("warn", "worker.reconnect_flush", map[string]any{
					"worker_id": r.workerID,
					"error":     err.Error(),
				})
			} else {
				workerEvent("debug", "worker.reconnect_flush", map[string]any{
					"worker_id": r.workerID,
				})
			}
			ticker.Reset(jitterDuration(r.heartbeatInterval))
		}
	}
}

func (r *workerRunner) logCleanupLoop(ctx context.Context) {
	if r.logRetentionDays <= 0 || r.logCleanupInterval <= 0 {
		workerEvent("info", "worker.log_cleanup", map[string]any{
			"worker_id": r.workerID,
			"retention_days": r.logRetentionDays,
			"interval_sec":  int(r.logCleanupInterval.Seconds()),
			"status":        "disabled",
		})
		return
	}

	cleanupTicker := time.NewTicker(r.logCleanupInterval)
	defer cleanupTicker.Stop()
	_ = r.cleanupLogArtifacts()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			if err := r.cleanupLogArtifacts(); err != nil {
				workerEvent("warn", "worker.log_cleanup", map[string]any{
					"worker_id": r.workerID,
					"status":    "failed",
					"error":     err.Error(),
				})
			}
		}
	}
}

func (r *workerRunner) reconnect(ctx context.Context) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	r.heartbeatSeq = state.HeartbeatSeq
	r.completionSeq = state.LastCompletionSeq
	workerEvent("info", "worker.reconnect", map[string]any{
		"worker_id":      r.workerID,
		"scope":          "startup",
		"has_current_job": strings.TrimSpace(state.CurrentJobID) != "",
		"pending_replays": len(state.CompletedJobs),
	})

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
	workerEvent("info", "worker.reconnect_response", map[string]any{
		"worker_id":    r.workerID,
		"status":       resp.Status,
		"action_count": len(resp.Actions),
	})

	for _, action := range resp.Actions {
		actionLog := map[string]any{
			"worker_id":     r.workerID,
			"action":        action.Action,
			"job_id":        action.JobID,
			"assignment_id": action.AssignmentID,
			"result":        action.Result,
		}
		switch action.Action {
		case hdcf.ReconnectActionClearCurrentJob:
			actionLog["state_change"] = "current_job_clear"
			if action.JobID == "" || action.JobID == state.CurrentJobID {
				_ = r.clearCurrentReconnectState()
			}
		case hdcf.ReconnectActionReplayCompleted, hdcf.ReconnectActionReplayFailed:
			actionLog["state_change"] = action.Action
			if action.Result == hdcf.ReconnectResultAccepted {
				_ = r.removeCompletedReconnectResult(action.JobID, action.AssignmentID)
				actionLog["result_state"] = "removed_local_replay"
			} else {
				actionLog["result_state"] = "kept_local_replay"
			}
			if errMsg := action.Error; errMsg != "" {
				actionLog["error"] = errMsg
			}
		default:
			actionLog["state_change"] = "unknown"
		}
		workerEvent("debug", "worker.reconnect_action", actionLog)
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
		WorkerID:     r.workerID,
		Nonce:        r.nonce,
		Capabilities: r.capabilities,
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
		if err := r.flushPendingReconnectResults(ctx); err != nil {
			workerEvent("warn", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"scope":     "no_content_after_next_job",
				"error":     err.Error(),
			})
			return nil, err
		}
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.next_job", map[string]any{
			"worker_id": r.workerID,
			"status":    resp.Status,
			"body":      body,
		})
		return nil, fmt.Errorf("next-job status=%s body=%s", resp.Status, body)
	}
	var job hdcf.AssignedJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		workerEvent("warn", "worker.next_job", map[string]any{
			"worker_id": r.workerID,
			"error":     err.Error(),
		})
		return nil, err
	}
	if err := r.flushPendingReconnectResults(ctx); err != nil {
		workerEvent("warn", "worker.reconnect_flush", map[string]any{
			"worker_id": r.workerID,
			"scope":     "post_next_job",
			"error":     err.Error(),
		})
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
	err := retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.postJSON(ctx, endpoint, payload)
	}, 8)
	if err != nil {
		workerEvent("warn", "worker.ack_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"error":         err.Error(),
		})
		return err
	}
	return nil
}

func (r *workerRunner) sendHeartbeat(ctx context.Context, currentJobID *string) error {
	seq := r.nextHeartbeatSeq()
	body := hdcf.HeartbeatRequest{
		WorkerID:     r.workerID,
		CurrentJobID: currentJobID,
		Timestamp:    time.Now().Format(time.RFC3339),
		Sequence:     seq,
	}
	if r.reportMetrics {
		body.Metrics = r.collectHeartbeatMetrics()
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
		workerEvent("warn", "worker.heartbeat", map[string]any{
			"worker_id": r.workerID,
			"status":    resp.Status,
			"body":      body,
		})
		return fmt.Errorf("heartbeat status=%s body=%s", resp.Status, body)
	}
	return nil
}

func (r *workerRunner) collectHeartbeatMetrics() *hdcf.HeartbeatMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	memoryUsageMB := float64(memStats.Alloc) / (1024 * 1024)
	return &hdcf.HeartbeatMetrics{
		MemoryUsageMB: &memoryUsageMB,
	}
}

func (r *workerRunner) nextHeartbeatSeq() int64 {
	next := atomic.AddInt64(&r.heartbeatSeq, 1)
	if next < 1 {
		next = 1
		atomic.StoreInt64(&r.heartbeatSeq, next)
	}
	if err := r.persistSequenceState(next, atomic.LoadInt64(&r.completionSeq)); err != nil {
		workerEvent("warn", "worker.sequence_persist", map[string]any{
			"worker_id": r.workerID,
			"scope":     "heartbeat_seq",
			"error":     err.Error(),
		})
	}
	return next
}

func (r *workerRunner) nextCompletionSeq() int64 {
	next := atomic.AddInt64(&r.completionSeq, 1)
	if next < 1 {
		next = 1
		atomic.StoreInt64(&r.completionSeq, next)
	}
	if err := r.persistSequenceState(atomic.LoadInt64(&r.heartbeatSeq), next); err != nil {
		workerEvent("warn", "worker.sequence_persist", map[string]any{
			"worker_id": r.workerID,
			"scope":     "completion_seq",
			"error":     err.Error(),
		})
	}
	return next
}

func (r *workerRunner) persistSequenceState(heartbeatSeq, completionSeq int64) error {
	if heartbeatSeq < 0 {
		heartbeatSeq = 0
	}
	if completionSeq < 0 {
		completionSeq = 0
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.HeartbeatSeq = heartbeatSeq
	state.LastCompletionSeq = completionSeq
	return r.persistReconnectState(state)
}

func (r *workerRunner) reportComplete(ctx context.Context, req hdcf.CompleteRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/complete", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		workerEvent("warn", "worker.report_complete", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"artifact_id":   req.ArtifactID,
			"exit_code":     req.ExitCode,
			"error":         err.Error(),
		})
		return err
	}
	return nil
}

func (r *workerRunner) reportFail(ctx context.Context, req hdcf.FailRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/fail", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		workerEvent("warn", "worker.report_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"exit_code":     req.ExitCode,
			"error":         err.Error(),
		})
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
	req.Header.Set(authHeader, r.workerAuthToken())
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"body":      body,
		})
		return fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}
	workerEvent("debug", "worker.http_request", map[string]any{
		"worker_id": r.workerID,
		"endpoint":  endpoint,
		"status":    resp.Status,
	})
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
	req.Header.Set(authHeader, r.workerAuthToken())
	resp, err := r.httpClient.Do(req)
	if err != nil {
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"error":     err.Error(),
		})
		return zero, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"body":      body,
		})
		return zero, fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}

	var parsed hdcf.WorkerReconnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"error":     err.Error(),
		})
		return zero, err
	}
	workerEvent("debug", "worker.http_request", map[string]any{
		"worker_id": r.workerID,
		"endpoint":  endpoint,
		"status":    resp.Status,
		"actions":   len(parsed.Actions),
	})
	return parsed, nil
}

func (r *workerRunner) workerAuthToken() string {
	if strings.TrimSpace(r.workerTokenSecret) == "" {
		return r.token
	}
	signed, err := makeSignedWorkerToken(r.workerID, r.workerTokenTTL, r.workerTokenSecret)
	if err != nil {
		workerEvent("warn", "worker.token", map[string]any{
			"worker_id": r.workerID,
			"error":     err.Error(),
		})
		return r.token
	}
	return signed
}

func makeSignedWorkerToken(workerID string, ttl time.Duration, secret string) (string, error) {
	if strings.TrimSpace(secret) == "" || strings.TrimSpace(workerID) == "" {
		return "", fmt.Errorf("invalid token inputs")
	}
	expiresAt := time.Now().Add(ttl).Unix()
	nonce := hdcf.NewJobID()
	payload := fmt.Sprintf("%s|%d|%s", workerID, expiresAt, nonce)
	encodedPayload := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(encodedPayload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("v1.%s.%s", encodedPayload, sig), nil
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

	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"command":       job.Command,
		"attempt_count": job.AttemptCount,
		"timeout_ms":    job.TimeoutMs,
		"working_dir":   strings.TrimSpace(job.WorkingDir),
		"state":         "starting",
	})
	err = cmd.Start()
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"state":         "running",
		"pid":           cmd.Process.Pid,
	})

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
		completionSeq := r.nextCompletionSeq()
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
			CompletionSeq: completionSeq,
			Status:       hdcf.StatusFailed,
			ExitCode:     failReq.ExitCode,
			StderrPath:   stderrTmpPath,
			StdoutPath:   stdoutTmpPath,
			Error:        msg,
		}
		if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
			workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
				"action":    "enqueue_failure",
			})
		}
		if retryWithBackoff(ctx, r.requestTimeout, func() error {
			return r.reportFail(ctx, failReq)
		}, 8) == nil {
			workerEvent("info", "worker.job_reported_fail", map[string]any{
				"worker_id":     r.workerID,
				"job_id":        job.JobID,
				"assignment_id": job.AssignmentID,
				"exit_code":     failReq.ExitCode,
				"error":         msg,
			})
			if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
				workerEvent("warn", "worker.job_reported_fail", map[string]any{
					"worker_id": r.workerID,
					"job_id":    job.JobID,
					"error":     err.Error(),
				})
			}
		}
		workerEvent("info", "worker.job_execute", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"state":         "failed",
			"exit_code":     exitCode,
			"message":       msg,
		})
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
	completionSeq := r.nextCompletionSeq()
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
		CompletionSeq: completionSeq,
		ResultSummary: summary,
	}
	reconnectEntry := hdcf.ReconnectCompletedJob{
		JobID:         job.JobID,
		AssignmentID:   job.AssignmentID,
		CompletionSeq: completionSeq,
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
	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":   r.workerID,
		"job_id":      job.JobID,
		"assignment_id": job.AssignmentID,
		"state":       "ready_for_completion",
		"duration_ms": duration,
		"artifact_id": artifactID,
		"stdout_sha256": stdoutSHA256,
		"stderr_sha256": stderrSHA256,
	})
	if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
		workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
			"worker_id": r.workerID,
			"job_id":    job.JobID,
			"error":     err.Error(),
			"action":    "enqueue_completion",
		})
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportComplete(ctx, compReq)
	}, 8) == nil {
		workerEvent("info", "worker.job_completed", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"artifact_id":   artifactID,
			"duration_ms":   duration,
			"stdout_path":   stdoutPath,
			"stderr_path":   stderrPath,
			"exit_code":     exitCode,
		})
		if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
			workerEvent("warn", "worker.job_completed", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
			})
		}
	} else {
		workerEvent("warn", "worker.job_completed", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"artifact_id":   artifactID,
			"error":         "report_complete_failed_after_retries",
		})
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
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

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
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.CurrentJobID = ""
	state.CurrentAssignmentID = ""
	return r.persistReconnectState(state)
}

func (r *workerRunner) enqueueCompletedReconnectResult(entry hdcf.ReconnectCompletedJob) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

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
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

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

func (r *workerRunner) flushPendingReconnectResults(ctx context.Context) error {
	const batchSize = 24

	for {
		r.stateMu.Lock()
		state, err := r.loadReconnectState()
		r.stateMu.Unlock()
		if err != nil {
			workerEvent("warn", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"error":     err.Error(),
			})
			return err
		}
		if len(state.CompletedJobs) == 0 {
			workerEvent("debug", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"status":    "empty_queue",
			})
			return nil
		}

		batch := state.CompletedJobs
		if len(batch) > batchSize {
			batch = batch[:batchSize]
		}
		req := hdcf.WorkerReconnectRequest{
			WorkerID:      r.workerID,
			CompletedJobs: batch,
		}
		workerEvent("info", "worker.reconnect_flush", map[string]any{
			"worker_id": r.workerID,
			"batch":     len(batch),
			"queued":    len(state.CompletedJobs),
		})
		resp, err := r.postJSONWithResponse(ctx, r.controlURL+"/reconnect", req)
		if err != nil {
			return err
		}
		removedAny := false
		for _, action := range resp.Actions {
			actionLog := map[string]any{
				"worker_id":     r.workerID,
				"action":        action.Action,
				"job_id":        action.JobID,
				"assignment_id": action.AssignmentID,
				"result":        action.Result,
			}
			switch action.Action {
			case hdcf.ReconnectActionReplayCompleted, hdcf.ReconnectActionReplayFailed:
				if action.Result == hdcf.ReconnectResultAccepted {
					if err := r.removeCompletedReconnectResult(action.JobID, action.AssignmentID); err != nil {
						return err
					}
					actionLog["removed_local_queue"] = true
					removedAny = true
				} else {
					actionLog["removed_local_queue"] = false
				}
			case hdcf.ReconnectActionClearCurrentJob:
				if action.JobID == "" || action.JobID == strings.TrimSpace(state.CurrentJobID) {
					if err := r.clearCurrentReconnectState(); err != nil {
						return err
					}
					actionLog["current_job_cleared"] = true
				}
			}
			if action.Error != "" {
				actionLog["error"] = action.Error
			}
			workerEvent("debug", "worker.reconnect_flush_action", actionLog)
		}
		if !removedAny {
			workerEvent("info", "worker.reconnect_flush", map[string]any{
				"worker_id":       r.workerID,
				"result":          "pending_replays_rejected",
				"action_count":    len(resp.Actions),
				"remaining_count":  len(state.CompletedJobs),
			})
			return nil
		}
		workerEvent("info", "worker.reconnect_flush", map[string]any{
			"worker_id":    r.workerID,
			"result":       "batch_accepted",
			"removed_any":  true,
			"action_count": len(resp.Actions),
		})
	}
}

func (r *workerRunner) handleJobFailure(ctx context.Context, job *hdcf.AssignedJob, err error) {
	workerEvent("warn", "worker.job_local_failure", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"error":         err.Error(),
	})
	completionSeq := r.nextCompletionSeq()
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
		CompletionSeq: completionSeq,
		Status:      hdcf.StatusFailed,
		ExitCode:    failReq.ExitCode,
		Error:       failReq.Error,
	}
	if queueErr := r.enqueueCompletedReconnectResult(reconnectEntry); queueErr != nil {
		workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"action":        "enqueue_startup_failure",
			"error":         queueErr.Error(),
		})
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportFail(ctx, failReq)
	}, 8) == nil {
		workerEvent("info", "worker.job_reported_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"exit_code":     failReq.ExitCode,
		})
		if queueErr := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); queueErr != nil {
			workerEvent("warn", "worker.job_reported_fail", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     queueErr.Error(),
			})
		}
	} else {
		workerEvent("warn", "worker.job_reported_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"error":         "report_fail_retries_exhausted",
		})
	}
}

func (r *workerRunner) cleanupLogArtifacts() error {
	if r.logRetentionDays <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-time.Duration(r.logRetentionDays) * 24 * time.Hour).Unix()
	entries, err := os.ReadDir(r.logDir)
	if err != nil {
		return err
	}
	currentJob := ""
	if v := r.getCurrentJobID(); v != nil {
		if val, ok := v.(string); ok {
			currentJob = strings.TrimSpace(val)
		}
	}

	var deleted, failed int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isWorkerLogArtifact(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			failed++
			continue
		}
		if info.ModTime().Unix() >= cutoff {
			continue
		}
		if currentJob != "" && strings.HasPrefix(name, currentJob+".") {
			continue
		}
		if err := os.Remove(filepath.Join(r.logDir, name)); err != nil {
			failed++
			continue
		}
		deleted++
	}
	if deleted > 0 || failed > 0 {
		workerEvent("info", "worker.log_cleanup", map[string]any{
			"worker_id":      r.workerID,
			"retention_days": r.logRetentionDays,
			"deleted":        deleted,
			"failed":         failed,
			"candidate_cutoff": cutoff,
		})
	}
	return nil
}

func isWorkerLogArtifact(name string) bool {
	return strings.HasSuffix(name, ".stdout.log") ||
		strings.HasSuffix(name, ".stderr.log") ||
		strings.HasSuffix(name, ".stdout.log.tmp") ||
		strings.HasSuffix(name, ".stderr.log.tmp")
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

const (
	authHeader    = "X-API-Token"
	auditComponent = "worker_daemon"
)

func workerEvent(level, event string, fields map[string]any) {
	payload := map[string]any{
		"ts":         time.Now().Format(time.RFC3339Nano),
		"component":  auditComponent,
		"level":      level,
		"event":      event,
	}
	for k, v := range fields {
		payload[k] = v
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to marshal worker event: %v", err)
		return
	}
	log.Printf("%s", string(raw))
}
