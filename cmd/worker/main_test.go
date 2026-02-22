package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hdcf/internal/hdcf"
)

func TestWorkerSplitCSV(t *testing.T) {
	t.Parallel()
	got := splitCSV("echo,  sleep,, echo ")
	want := []string{"echo", "sleep"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if got := splitCSV("   "); got != nil {
		t.Fatalf("expected nil for empty input, got %v", got)
	}
}

func TestWorkerNormalizeAllowedWorkingDirs(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	nested := filepath.Join(tmp, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	got := normalizeAllowedWorkingDirs(tmp + ", " + nested + ", " + tmp)
	if len(got) != 2 {
		t.Fatalf("expected 2 unique dirs, got %v", got)
	}
	for _, dir := range got {
		if dir == "" {
			t.Fatalf("expected non-empty allowed dir")
		}
	}
}

func TestWorkerBuildArtifactUploader(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	local, err := newArtifactUploader(hdcf.ArtifactStorageBackendLocal, "")
	if err != nil {
		t.Fatalf("local uploader: %v", err)
	}
	src1 := filepath.Join(tmp, "stdout.txt")
	src2 := filepath.Join(tmp, "stderr.txt")
	if err := os.WriteFile(src1, []byte("out"), 0o644); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	if err := os.WriteFile(src2, []byte("err"), 0o644); err != nil {
		t.Fatalf("write stderr: %v", err)
	}
	state, err := local.UploadArtifacts(context.Background(), "job-1", "artifact-1", src1, src2)
	if err != nil {
		t.Fatalf("upload artifacts: %v", err)
	}
	if state.state != hdcf.ArtifactUploadStateOK {
		t.Fatalf("expected state ok, got %q", state.state)
	}
	if state.backend != hdcf.ArtifactStorageBackendLocal {
		t.Fatalf("expected backend local, got %q", state.backend)
	}

	_, err = local.UploadArtifacts(context.Background(), "job-1", "artifact-1", "", src2)
	if err == nil {
		t.Fatalf("expected missing path failure")
	}

	nfsRoot := filepath.Join(tmp, "nfs")
	nfs, err := newArtifactUploader(hdcf.ArtifactStorageBackendNFS, nfsRoot)
	if err != nil {
		t.Fatalf("nfs uploader: %v", err)
	}
	_, err = nfs.UploadArtifacts(context.Background(), "", "artifact-1", src1, src2)
	if err == nil {
		t.Fatalf("expected missing job id failure")
	}
	state, err = nfs.UploadArtifacts(context.Background(), "job-1", "artifact-1", src1, src2)
	if err != nil {
		t.Fatalf("nfs upload: %v", err)
	}
	if state.state != hdcf.ArtifactUploadStateOK {
		t.Fatalf("expected nfs upload ok, got %q error=%q", state.state, state.errorMsg)
	}
	for _, p := range []string{"stdout.txt", "stderr.txt"} {
		if _, err := os.Stat(filepath.Join(nfsRoot, "job-1", p)); err != nil {
			t.Fatalf("expected copied artifact %s, got %v", p, err)
		}
	}
	if _, err := newArtifactUploader("unsupported", ""); err == nil {
		t.Fatalf("expected unsupported backend to fail")
	}
}

func TestWorkerRetryAndBackoffHelpers(t *testing.T) {
	t.Parallel()
	ttl := 2 * time.Millisecond
	var attempts atomic.Int32
	err := retryWithBackoff(context.Background(), ttl, func() error {
		attempts.Add(1)
		if attempts.Load() < 3 {
			return errors.New("temporary")
		}
		return nil
	}, 4)
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}

	if nextBackoff(500*time.Millisecond, 2*time.Second) != time.Second {
		t.Fatalf("expected backoff to double")
	}
	if nextBackoff(2*time.Second, time.Second) != time.Second {
		t.Fatalf("expected backoff capped by max")
	}
	if jitter := jitterDuration(0); jitter != 0 {
		t.Fatalf("expected zero jitter for non-positive duration")
	}
	jitter := jitterDuration(100 * time.Millisecond)
	if jitter < 100*time.Millisecond || jitter > 120*time.Millisecond {
		t.Fatalf("unexpected jitter value %s", jitter)
	}
}

func TestWorkerEnsureRegisteredThrottles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	calls := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/register" {
			http.NotFound(w, req)
			return
		}
		calls++
		if req.Header.Get(authHeader) != "unit-token" {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	r := &workerRunner{
		workerID:         "worker-throttle",
		token:            "unit-token",
		requestTimeout:   2 * time.Second,
		registerInterval: 75 * time.Millisecond,
		controlURL:       ts.URL,
	}
	client, err := buildWorkerHTTPClient(workerConfig{controlURL: ts.URL})
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	r.httpClient = client

	if err := r.ensureRegistered(ctx, "initial"); err != nil {
		t.Fatalf("ensureRegistered initial: %v", err)
	}
	if err := r.ensureRegistered(ctx, "initial"); err != nil {
		t.Fatalf("ensureRegistered throttled: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only one registration due throttle, got %d", calls)
	}

	time.Sleep(100 * time.Millisecond)
	if err := r.ensureRegistered(ctx, "initial"); err != nil {
		t.Fatalf("ensureRegistered after cooldown: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected second registration after throttle window, got %d", calls)
	}
}

func TestWorkerReadLimitedBody(t *testing.T) {
	t.Parallel()
	short := readLimitedBody(strings.NewReader("ok"))
	if short != "ok" {
		t.Fatalf("unexpected short body: %q", short)
	}
	longInput := strings.Repeat("x", 700)
	body := readLimitedBody(strings.NewReader(longInput))
	if len(body) != 512 {
		t.Fatalf("expected read limit 512, got %d", len(body))
	}
}

func TestWorkerConfigPathFromArgs(t *testing.T) {
	t.Parallel()
	oldArgs := append([]string(nil), os.Args...)
	oldEnv := os.Getenv("HDCF_WORKER_CONFIG")
	t.Cleanup(func() {
		os.Args = oldArgs
		if oldEnv == "" {
			_ = os.Unsetenv("HDCF_WORKER_CONFIG")
		} else {
			_ = os.Setenv("HDCF_WORKER_CONFIG", oldEnv)
		}
	})

	_ = os.Setenv("HDCF_WORKER_CONFIG", "/env/worker-config.json")
	os.Args = []string{"cmd", "-config", "/flag/path.json"}
	if got := workerConfigPathFromArgs(); got != "/flag/path.json" {
		t.Fatalf("expected -config flag to win, got %q", got)
	}
	os.Args = []string{"cmd", "--config=/flag/eq.json"}
	if got := workerConfigPathFromArgs(); got != "/flag/eq.json" {
		t.Fatalf("expected --config= flag to win, got %q", got)
	}
	os.Args = []string{"cmd"}
	if got := workerConfigPathFromArgs(); got != "/env/worker-config.json" {
		t.Fatalf("expected env path fallback, got %q", got)
	}
}

func TestWorkerResolveConfigHelpers(t *testing.T) {
	t.Parallel()
	oldEnv := os.Getenv("HDCF_TEST_BOOL")
	oldList := os.Getenv("HDCF_TEST_LIST")
	oldString := os.Getenv("HDCF_TEST_STRING")
	t.Cleanup(func() {
		if oldEnv == "" {
			_ = os.Unsetenv("HDCF_TEST_BOOL")
		} else {
			_ = os.Setenv("HDCF_TEST_BOOL", oldEnv)
		}
		if oldList == "" {
			_ = os.Unsetenv("HDCF_TEST_LIST")
		} else {
			_ = os.Setenv("HDCF_TEST_LIST", oldList)
		}
		if oldString == "" {
			_ = os.Unsetenv("HDCF_TEST_STRING")
		} else {
			_ = os.Setenv("HDCF_TEST_STRING", oldString)
		}
	})

	raw := "from-file"
	if got := resolveWorkerConfigString(&raw, "HDCF_TEST_STRING", "fallback"); got != "from-file" {
		t.Fatalf("expected file value, got %q", got)
	}
	_ = os.Setenv("HDCF_TEST_STRING", "")
	if got := resolveWorkerConfigString(nil, "HDCF_TEST_STRING", "fallback"); got != "fallback" {
		t.Fatalf("expected fallback when env empty, got %q", got)
	}
	if got := resolveWorkerConfigStringList([]string{" alpha ", "beta", "alpha"}, "HDCF_TEST_LIST", "fallback"); got != "alpha,beta" {
		t.Fatalf("expected normalized list, got %q", got)
	}
	if got := resolveWorkerConfigInt64(nil, "MISSING_INT", 77); got != 77 {
		t.Fatalf("expected int fallback, got %d", got)
	}
	maxAttempts := int64(99)
	if got := resolveWorkerConfigInt64(&maxAttempts, "MISSING_INT", 77); got != 99 {
		t.Fatalf("expected override int, got %d", got)
	}
	_ = os.Setenv("HDCF_TEST_BOOL", "true")
	if got := resolveWorkerConfigBool(nil, "HDCF_TEST_BOOL", false); got != true {
		t.Fatalf("expected bool env value, got %v", got)
	}
	if got := resolveWorkerConfigBool(nil, "MISSING_BOOL", false); got != false {
		t.Fatalf("expected bool fallback, got %v", got)
	}
}

func TestWorkerBuildHTTPClientTLSValidation(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	cfg := workerConfig{
		controlURL: "https://localhost:8443",
		tlsCA:      filepath.Join(tmp, "missing.pem"),
	}
	if _, err := buildWorkerHTTPClient(cfg); err == nil {
		t.Fatalf("expected missing TLS CA file to fail for https")
	}

	cfg = workerConfig{
		controlURL:    "https://localhost:8443",
		tlsClientCert: filepath.Join(tmp, "client.crt"),
		tlsClientKey:  "",
	}
	if _, err := buildWorkerHTTPClient(cfg); err == nil {
		t.Fatalf("expected TLS cert/key mismatch to fail")
	}

	cfg = workerConfig{
		controlURL: "http://localhost:8080",
	}
	client, err := buildWorkerHTTPClient(cfg)
	if err != nil {
		t.Fatalf("non-https control URL should succeed: %v", err)
	}
	if client == nil {
		t.Fatalf("expected non-nil http client")
	}
}

func TestWorkerExtractionAndHashing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	out := filepath.Join(dir, "sum.txt")
	if err := os.WriteFile(out, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	sum := sha256.Sum256([]byte("payload"))
	want := hex.EncodeToString(sum[:])
	r := &workerRunner{}
	got, err := r.hashArtifact(out)
	if err != nil {
		t.Fatalf("hash artifact: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected hash, got %s want %s", got, want)
	}
}

func TestWorkerValidateExecutionPolicy(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	allowed := filepath.Join(base, "allowed")
	if err := os.MkdirAll(allowed, 0o755); err != nil {
		t.Fatalf("mkdir allowed: %v", err)
	}
	disallowed := filepath.Join(base, "disallowed")
	if err := os.MkdirAll(disallowed, 0o755); err != nil {
		t.Fatalf("mkdir disallowed: %v", err)
	}

	r := &workerRunner{
		commandAllowlistEnabled: true,
		commandAllowlist:        []string{"echo", "/usr/bin/printf"},
		allowedWorkingDirs:      []string{allowed},
	}
	if err := r.validateExecutionPolicy(&hdcf.AssignedJob{Command: "echo", WorkingDir: allowed}); err != nil {
		t.Fatalf("expected allowlisted command allowed: %v", err)
	}
	if err := r.validateExecutionPolicy(&hdcf.AssignedJob{Command: "/usr/bin/printf", WorkingDir: allowed}); err != nil {
		t.Fatalf("expected base-allowed command allowed: %v", err)
	}
	if err := r.validateExecutionPolicy(&hdcf.AssignedJob{Command: "forbidden", WorkingDir: allowed}); err == nil {
		t.Fatalf("expected blocked command under allowlist")
	}
	if err := r.validateExecutionPolicy(&hdcf.AssignedJob{Command: "echo", WorkingDir: disallowed}); err == nil {
		t.Fatalf("expected blocked working directory")
	}
	r.allowedWorkingDirs = nil
	if err := r.validateExecutionPolicy(&hdcf.AssignedJob{Command: "echo", WorkingDir: disallowed}); err != nil {
		t.Fatalf("expected working directory allowlist bypass, got %v", err)
	}
}

func TestWorkerTokenGeneration(t *testing.T) {
	t.Parallel()
	r := &workerRunner{
		workerID:          "worker-1",
		workerTokenSecret: "secret",
		token:             "legacy-token",
	}
	signed := r.workerAuthToken()
	if signed == r.token {
		t.Fatalf("expected signed token when secret configured")
	}
	payload, sig, err := parseWorkerSignedToken(signed)
	if err != nil {
		t.Fatalf("parse signed token: %v", err)
	}
	if !strings.HasPrefix(payload, "worker-1|") {
		t.Fatalf("unexpected payload: %q", payload)
	}
	parts := strings.Split(payload, "|")
	if len(parts) != 3 {
		t.Fatalf("expected 3 payload parts, got %d", len(parts))
	}
	if got := sig; got == "" {
		t.Fatalf("expected signature")
	}
}

func TestWorkerReconnectStatePersistence(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{stateFile: statePath}

	if err := r.setCurrentReconnectState("job-1", "assign-1"); err != nil {
		t.Fatalf("set current: %v", err)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.CurrentJobID != "job-1" || state.CurrentAssignmentID != "assign-1" {
		t.Fatalf("unexpected current state %+v", state)
	}

	if err := r.enqueueCompletedReconnectResult(hdcf.ReconnectCompletedJob{
		JobID:        "job-1",
		AssignmentID: "assign-1",
		Status:       hdcf.StatusCompleted,
	}); err != nil {
		t.Fatalf("enqueue completion: %v", err)
	}
	state, err = r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 1 {
		t.Fatalf("expected one queued replay, got %d", len(state.CompletedJobs))
	}
	if err := r.removeCompletedReconnectResult("job-1", "assign-1"); err != nil {
		t.Fatalf("remove replay: %v", err)
	}
	state, err = r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 0 {
		t.Fatalf("expected no queued replay, got %d", len(state.CompletedJobs))
	}

	if err := r.clearCurrentReconnectState(); err != nil {
		t.Fatalf("clear current: %v", err)
	}
	state, err = r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.CurrentJobID != "" || state.CurrentAssignmentID != "" {
		t.Fatalf("expected cleared current state, got %+v", state)
	}
}

func TestWorkerFullDryRunFlowThroughMockControl(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logDir := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{
		workerID:                "worker-e2e",
		token:                   "unit-token",
		logDir:                  logDir,
		stateFile:               stateFile,
		dryRun:                  true,
		artifactStorageBackend:  hdcf.ArtifactStorageBackendLocal,
		artifactStorageLocation: "",
		requestTimeout:          2 * time.Second,
	}
	artifactUploader, err := newArtifactUploader(r.artifactStorageBackend, r.artifactStorageLocation)
	if err != nil {
		t.Fatalf("artifact uploader: %v", err)
	}
	r.artifactStorage = artifactUploader

	assigned := hdcf.AssignedJob{
		JobID:        "job-dry",
		Command:      "echo",
		Args:         []string{"hello"},
		WorkingDir:   "",
		TimeoutMs:    1000,
		AttemptCount: 1,
		MaxAttempts:  3,
		AssignmentID: "assign-dry",
	}
	var nextCalled, ackCalled, completeCalled, reconnectCalled int
	var completeReq hdcf.CompleteRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/register":
			if req.Header.Get(authHeader) != "unit-token" {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		case "/reconnect":
			reconnectCalled++
			var payload hdcf.WorkerReconnectRequest
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if payload.WorkerID != "worker-e2e" {
				http.Error(w, "worker id mismatch", http.StatusBadRequest)
				return
			}
			raw, _ := json.Marshal(hdcf.WorkerReconnectResponse{
				Status:  "ok",
				Actions: []hdcf.ReconnectAction{},
			})
			_, _ = w.Write(raw)
		case "/next-job":
			nextCalled++
			raw, _ := json.Marshal(assigned)
			_, _ = w.Write(raw)
		case "/ack":
			ackCalled++
			var reqPayload hdcf.AckJobRequest
			if err := json.NewDecoder(req.Body).Decode(&reqPayload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if reqPayload.JobID != assigned.JobID || reqPayload.AssignmentID != assigned.AssignmentID {
				http.Error(w, "assignment mismatch", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		case "/complete":
			completeCalled++
			if err := json.NewDecoder(req.Body).Decode(&completeReq); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			raw, _ := json.Marshal(map[string]any{
				"status": hdcf.StatusCompleted,
				"job_id": completeReq.JobID,
			})
			_, _ = w.Write(raw)
		default:
			http.NotFound(w, req)
		}
	}))
	defer ts.Close()

	r.controlURL = ts.URL
	r.httpClient, err = buildWorkerHTTPClient(workerConfig{controlURL: ts.URL})
	if err != nil {
		t.Fatalf("http client: %v", err)
	}

	if err := r.register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.reconnect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	job, err := r.nextJob(ctx)
	if err != nil {
		t.Fatalf("next-job: %v", err)
	}
	if job == nil {
		t.Fatalf("expected assigned job")
	}
	if err := r.ackJob(ctx, job); err != nil {
		t.Fatalf("ack: %v", err)
	}
	r.executeJob(ctx, job)

	if nextCalled != 1 {
		t.Fatalf("expected one next-job request, got %d", nextCalled)
	}
	if ackCalled != 1 {
		t.Fatalf("expected one ack request, got %d", ackCalled)
	}
	if completeCalled != 1 {
		t.Fatalf("expected one complete request, got %d", completeCalled)
	}
	if reconnectCalled < 1 {
		t.Fatalf("expected reconnect calls (startup + flush), got %d", reconnectCalled)
	}
	if completeReq.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", completeReq.ExitCode)
	}
	if completeReq.ArtifactBackend != hdcf.ArtifactStorageBackendLocal {
		t.Fatalf("expected artifact backend local, got %q", completeReq.ArtifactBackend)
	}
	if _, err := os.Stat(filepath.Join(logDir, assigned.JobID+".stdout.log")); err != nil {
		t.Fatalf("stdout log should exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logDir, assigned.JobID+".stderr.log")); err != nil {
		t.Fatalf("stderr log should exist: %v", err)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 0 {
		t.Fatalf("expected reconnect queue to be empty, got %d", len(state.CompletedJobs))
	}
}

func TestWorkerExecuteJobCommandFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logDir := t.TempDir()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{
		workerID:                "worker-fail",
		token:                   "unit-token",
		logDir:                  logDir,
		stateFile:               stateFile,
		artifactStorageBackend:  hdcf.ArtifactStorageBackendLocal,
		artifactStorageLocation: "",
		requestTimeout:          2 * time.Second,
	}
	artifactUploader, err := newArtifactUploader(r.artifactStorageBackend, r.artifactStorageLocation)
	if err != nil {
		t.Fatalf("artifact uploader: %v", err)
	}
	r.artifactStorage = artifactUploader

	assigned := hdcf.AssignedJob{
		JobID:        "job-fail",
		Command:      "sh",
		Args:         []string{"-c", "exit 9"},
		WorkingDir:   "",
		TimeoutMs:    1000,
		AttemptCount: 1,
		MaxAttempts:  3,
		AssignmentID: "assign-fail",
	}
	failCalls := 0
	var failReq hdcf.FailRequest
	completeCalls := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/fail":
			failCalls++
			if err := json.NewDecoder(req.Body).Decode(&failReq); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		case "/complete":
			completeCalls++
			http.Error(w, "unexpected complete call", http.StatusBadRequest)
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer ts.Close()
	r.controlURL = ts.URL
	r.httpClient = &http.Client{
		Timeout: r.requestTimeout,
	}

	r.executeJob(ctx, &assigned)
	if failCalls != 1 {
		t.Fatalf("expected one fail request, got %d", failCalls)
	}
	if completeCalls != 0 {
		t.Fatalf("did not expect complete request, got %d", completeCalls)
	}
	if failReq.ExitCode != 9 {
		t.Fatalf("expected exit code 9, got %d", failReq.ExitCode)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 0 {
		t.Fatalf("expected completed queue cleared after fail report, got %d", len(state.CompletedJobs))
	}
}

func TestWorkerNextJobNoContentFlushesReconnectQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{
		workerID:       "worker-nc",
		stateFile:      stateFile,
		token:          "unit-token",
		requestTimeout: 2 * time.Second,
	}
	if err := r.enqueueCompletedReconnectResult(hdcf.ReconnectCompletedJob{
		JobID:        "queued",
		AssignmentID: "assign-q",
		Status:       hdcf.StatusCompleted,
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	nextCalled := false
	reconnectCalled := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/next-job":
			nextCalled = true
			w.WriteHeader(http.StatusNoContent)
		case "/reconnect":
			reconnectCalled++
			var payload hdcf.WorkerReconnectRequest
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(payload.CompletedJobs) != 1 {
				http.Error(w, "expected replay payload", http.StatusBadRequest)
				return
			}
			raw, _ := json.Marshal(hdcf.WorkerReconnectResponse{
				Status:  "ok",
				Actions: []hdcf.ReconnectAction{},
			})
			_, _ = w.Write(raw)
		default:
			http.NotFound(w, req)
		}
	}))
	defer ts.Close()
	r.controlURL = ts.URL
	client, err := buildWorkerHTTPClient(workerConfig{controlURL: ts.URL})
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	r.httpClient = client

	got, err := r.nextJob(ctx)
	if err != nil {
		t.Fatalf("next-job: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil job on 204 response")
	}
	if !nextCalled {
		t.Fatalf("expected next-job request")
	}
	if reconnectCalled != 1 {
		t.Fatalf("expected reconnect flush request, got %d", reconnectCalled)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 1 {
		t.Fatalf("expected queue retained when no replay actions accepted")
	}
}

func TestWorkerReconnectClearsCurrentJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{
		workerID:       "worker-clear",
		stateFile:      stateFile,
		token:          "unit-token",
		requestTimeout: 2 * time.Second,
	}
	if err := r.setCurrentReconnectState("running-job", "assign-clear"); err != nil {
		t.Fatalf("seed current state: %v", err)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var payload hdcf.WorkerReconnectRequest
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if payload.CurrentJobID == nil || strings.TrimSpace(*payload.CurrentJobID) != "running-job" {
			http.Error(w, "missing current job", http.StatusBadRequest)
			return
		}
		raw, _ := json.Marshal(hdcf.WorkerReconnectResponse{
			Status: "ok",
			Actions: []hdcf.ReconnectAction{
				{
					Action:       hdcf.ReconnectActionClearCurrentJob,
					JobID:        "running-job",
					AssignmentID: "assign-clear",
					Result:       hdcf.ReconnectResultAccepted,
				},
			},
		})
		_, _ = w.Write(raw)
	}))
	defer ts.Close()
	r.controlURL = ts.URL
	client, err := buildWorkerHTTPClient(workerConfig{controlURL: ts.URL})
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	r.httpClient = client

	if err := r.reconnect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.CurrentJobID != "" || state.CurrentAssignmentID != "" {
		t.Fatalf("expected current job cleared, got %+v", state)
	}
}

func TestWorkerCleanupLogArtifacts(t *testing.T) {
	t.Parallel()
	logDir := t.TempDir()
	r := &workerRunner{
		logDir:           logDir,
		logRetentionDays: 1,
		currentJobID:     atomic.Value{},
	}
	r.currentJobID.Store("job-current")
	oldLog := filepath.Join(logDir, "job-old.stdout.log")
	oldTmp := filepath.Join(logDir, "job-old.stderr.log.tmp")
	currentTmp := filepath.Join(logDir, "job-current.stdout.log.tmp")
	recentFile := filepath.Join(logDir, "job-recent.stderr.log")
	for _, file := range []string{oldLog, oldTmp, currentTmp, recentFile} {
		if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
			t.Fatalf("write log file: %v", err)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldLog, old, old); err != nil {
		t.Fatalf("set mtime old log: %v", err)
	}
	if err := os.Chtimes(oldTmp, old, old); err != nil {
		t.Fatalf("set mtime old tmp: %v", err)
	}

	if err := r.cleanupLogArtifacts(); err != nil {
		t.Fatalf("cleanup logs: %v", err)
	}
	if _, err := os.Stat(oldLog); err == nil {
		t.Fatalf("expected old log file removed: %s", oldLog)
	}
	if _, err := os.Stat(oldTmp); err == nil {
		t.Fatalf("expected old tmp file removed: %s", oldTmp)
	}
	if _, err := os.Stat(currentTmp); err != nil {
		t.Fatalf("expected active current job temp file retained: %v", err)
	}
	if _, err := os.Stat(recentFile); err != nil {
		t.Fatalf("expected recent file retained: %v", err)
	}
}

func TestWorkerFlushReconnectResultsHonorsControlResponse(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	r := &workerRunner{
		workerID:  "worker-1",
		stateFile: statePath,
	}
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests++
		var payload hdcf.WorkerReconnectRequest
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if payload.WorkerID != "worker-1" {
			http.Error(w, "unexpected worker id", http.StatusBadRequest)
			return
		}
		if len(payload.CompletedJobs) == 0 {
			http.Error(w, "expected replay payload", http.StatusBadRequest)
			return
		}
		accepted := payload.CompletedJobs[0].JobID == "accept"
		res := hdcf.WorkerReconnectResponse{
			Status: "ok",
			Actions: []hdcf.ReconnectAction{
				{
					JobID:        payload.CompletedJobs[0].JobID,
					AssignmentID: payload.CompletedJobs[0].AssignmentID,
					Action:       hdcf.ReconnectActionReplayCompleted,
					Result:       hdcf.ReconnectResultAccepted,
				},
			},
		}
		if !accepted {
			res.Actions[0].Result = hdcf.ReconnectResultRejected
			res.Actions[0].Error = "rejected-for-test"
		}
		raw, _ := json.Marshal(res)
		_, _ = w.Write(raw)
	}))
	defer ts.Close()
	r.controlURL = ts.URL
	r.httpClient = &http.Client{Timeout: time.Second}
	ctx := context.Background()

	if err := r.enqueueCompletedReconnectResult(hdcf.ReconnectCompletedJob{
		JobID:         "accept",
		AssignmentID:  "assignment-1",
		Status:        hdcf.StatusCompleted,
		CompletionSeq: 1,
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := r.flushPendingReconnectResults(ctx); err != nil {
		t.Fatalf("flush accepted: %v", err)
	}
	state, err := r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 0 {
		t.Fatalf("expected accepted replay removed, got %d", len(state.CompletedJobs))
	}

	state, err = r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if err := r.persistReconnectState(state); err != nil {
		t.Fatalf("persist state: %v", err)
	}
	if err := r.enqueueCompletedReconnectResult(hdcf.ReconnectCompletedJob{
		JobID:         "reject",
		AssignmentID:  "assignment-2",
		Status:        hdcf.StatusCompleted,
		CompletionSeq: 2,
	}); err != nil {
		t.Fatalf("seed reject state: %v", err)
	}
	if err := r.flushPendingReconnectResults(ctx); err != nil {
		t.Fatalf("flush rejected: %v", err)
	}
	state, err = r.loadReconnectState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.CompletedJobs) != 1 || state.CompletedJobs[0].JobID != "reject" {
		t.Fatalf("expected rejected replay to remain queued, got %+v", state.CompletedJobs)
	}
	if requests == 0 {
		t.Fatalf("expected reconnect requests")
	}
}

func parseWorkerSignedToken(token string) (string, string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || strings.TrimSpace(parts[0]) != "v1" {
		return "", "", errors.New("invalid worker token format")
	}
	payload, err := decodeBase64(token)
	if err != nil {
		return "", "", err
	}
	return payload, strings.TrimSpace(parts[2]), nil
}

func decodeBase64(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid worker token format")
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("invalid worker token payload")
	}
	return strings.TrimSpace(string(payloadRaw)), nil
}
