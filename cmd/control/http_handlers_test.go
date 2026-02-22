package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hdcf/internal/hdcf"
	"hdcf/internal/store"
)

const (
	controlAdminToken  = "admin-token"
	controlWorkerToken = "worker-token"
)

type controlHarness struct {
	server *httptest.Server
	store  *store.Store
	cfg    *controlConfig
}

func newControlHarness(t *testing.T) *controlHarness {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "control-harness.db")
	s, err := store.Open(dbPath, time.Second, store.StoreOptions{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	cfg := &controlConfig{
		adminToken:        controlAdminToken,
		adminTokenPrev:    "",
		workerToken:       controlWorkerToken,
		workerTokenPrev:   "",
		workerTokenSecret: "",
		heartbeatTimeout:  time.Second,
		reconcileInterval: 10 * time.Second,
		cleanupInterval:   10 * time.Second,
		dbPath:            dbPath,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ui", withUIAuth(cfg, dashboardUI()))
	mux.HandleFunc("/ui/", withUIAuth(cfg, dashboardUI()))
	mux.HandleFunc("/healthz", healthzHandler(s, dbPath))
	mux.HandleFunc("/jobs", withAdminAuth(cfg, jobsHandler(s)))
	mux.HandleFunc("/jobs/", withAdminAuth(cfg, getJobHandler(s)))
	mux.HandleFunc("/metrics", withAdminAuth(cfg, metricsHandler(s)))
	mux.HandleFunc("/register", withWorkerAuth(cfg, registerWorker(cfg, s)))
	mux.HandleFunc("/next-job", withWorkerAuth(cfg, nextJob(cfg, s)))
	mux.HandleFunc("/ack", withWorkerAuth(cfg, ackJob(cfg, s)))
	mux.HandleFunc("/heartbeat", withWorkerAuth(cfg, heartbeat(cfg, s)))
	mux.HandleFunc("/reconnect", withWorkerAuth(cfg, reconnectWorker(cfg, s)))
	mux.HandleFunc("/abort", withAdminAuth(cfg, abortJob(s)))
	mux.HandleFunc("/workers", withAdminAuth(cfg, listWorkers(s)))
	mux.HandleFunc("/events", withAdminAuth(cfg, listEvents(s)))
	mux.HandleFunc("/complete", withWorkerAuth(cfg, completeJob(cfg, s)))
	mux.HandleFunc("/fail", withWorkerAuth(cfg, failJob(cfg, s)))

	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
		_ = s.Close()
	})

	return &controlHarness{
		server: server,
		store:  s,
		cfg:    cfg,
	}
}

func (h *controlHarness) doJSON(t *testing.T, method, path string, token string, payload any) (*http.Response, []byte) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		body = bytes.NewBuffer(raw)
	}
	req, err := http.NewRequest(method, h.server.URL+path, body)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set(authHeaderName, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	raw, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, raw
}

func TestControlJobsEndpointRequiresAdminAuthAndSupportsFlow(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)
	_, raw := h.doJSON(t, http.MethodPost, "/jobs", "", map[string]any{
		"command": "echo",
		"args":    []string{"hello"},
	})
	if string(raw) == "" || strings.TrimSpace(string(raw)) == "" {
		// just ensuring body is read in response path
	}
	if resp, _ := h.doJSON(t, http.MethodPost, "/jobs", "", map[string]any{}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized without token, got %d", resp.StatusCode)
	}
	resp, raw := h.doJSON(t, http.MethodPost, "/jobs", controlAdminToken, map[string]any{
		"command": "echo",
		"args":    []string{"hello"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating job, got %d body=%s", resp.StatusCode, string(raw))
	}
	var created hdcf.CreateJobResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if created.Status != hdcf.StatusPending {
		t.Fatalf("expected created status pending, got %q", created.Status)
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/jobs", controlAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected list 200, got %d", resp.StatusCode)
	}
	var all []hdcf.JobRead
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatalf("unmarshal jobs list: %v", err)
	}
	if got := len(all); got != 1 {
		t.Fatalf("expected one job, got %d", got)
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/jobs/"+created.JobID, controlAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected get job 200, got %d", resp.StatusCode)
	}
	var job hdcf.JobRead
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.JobID != created.JobID {
		t.Fatalf("unexpected job id %q != %q", job.JobID, created.JobID)
	}
}

func TestControlWorkerLifecycleCompleteAndDuplicateCompletion(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, raw := h.doJSON(t, http.MethodPost, "/register", controlWorkerToken, map[string]any{
		"worker_id":    "worker-a",
		"capabilities": []string{"linux"},
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("worker register failed: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/jobs", controlAdminToken, map[string]any{
		"command":      "echo",
		"args":         []string{"done"},
		"max_attempts": 2,
		"requirements": []string{"linux"},
		"needs_gpu":    false,
		"priority":     10,
		"scheduled_at": 0,
		"timeout_ms":   1000,
		"working_dir":  "",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create job failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var created hdcf.CreateJobResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/next-job?worker_id=worker-a", controlWorkerToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("next job failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var assigned hdcf.AssignedJob
	if err := json.Unmarshal(raw, &assigned); err != nil {
		t.Fatalf("unmarshal assigned job: %v", err)
	}
	if assigned.JobID != created.JobID {
		t.Fatalf("assigned wrong job: %s != %s", assigned.JobID, created.JobID)
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/ack", "", map[string]any{
		"job_id":        assigned.JobID,
		"worker_id":     "worker-a",
		"assignment_id": assigned.AssignmentID,
	})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("ack should require worker token, got %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/ack", controlWorkerToken, map[string]any{
		"job_id":        assigned.JobID,
		"worker_id":     "worker-a",
		"assignment_id": assigned.AssignmentID,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ack failed: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/complete", controlWorkerToken, hdcf.CompleteRequest{
		JobID:               assigned.JobID,
		WorkerID:            "worker-a",
		AssignmentID:        assigned.AssignmentID,
		ArtifactID:          "artifact-1",
		ExitCode:            0,
		StdoutPath:          "/tmp/worker.out",
		StderrPath:          "/tmp/worker.err",
		StdoutTmpPath:       "/tmp/worker.out.tmp",
		StderrTmpPath:       "/tmp/worker.err.tmp",
		ArtifactBackend:     hdcf.ArtifactStorageBackendLocal,
		ArtifactUploadState: hdcf.ArtifactUploadStateOK,
		CompletionSeq:       1,
		ResultSummary:       "unit complete",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var completeResp map[string]any
	if err := json.Unmarshal(raw, &completeResp); err != nil {
		t.Fatalf("decode complete response: %v", err)
	}
	if status := completeResp["status"]; status != hdcf.StatusCompleted {
		t.Fatalf("expected complete status %q got %v", hdcf.StatusCompleted, status)
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/complete", controlWorkerToken, hdcf.CompleteRequest{
		JobID:               assigned.JobID,
		WorkerID:            "worker-a",
		AssignmentID:        assigned.AssignmentID,
		ArtifactID:          "artifact-1",
		ExitCode:            0,
		StdoutPath:          "/tmp/worker.out",
		StderrPath:          "/tmp/worker.err",
		StdoutTmpPath:       "/tmp/worker.out.tmp",
		StderrTmpPath:       "/tmp/worker.err.tmp",
		ArtifactBackend:     hdcf.ArtifactStorageBackendLocal,
		ArtifactUploadState: hdcf.ArtifactUploadStateOK,
		CompletionSeq:       0,
		ResultSummary:       "duplicate",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate complete should be idempotent: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/jobs/"+assigned.JobID, controlAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get job: %d body=%s", resp.StatusCode, string(raw))
	}
	var final hdcf.JobRead
	if err := json.Unmarshal(raw, &final); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if final.Status != hdcf.StatusCompleted {
		t.Fatalf("expected job status completed, got %q", final.Status)
	}
}

func TestControlWorkerFailThenRequeue(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)
	resp, raw := h.doJSON(t, http.MethodPost, "/register", controlWorkerToken, map[string]any{
		"worker_id": "worker-b",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register failed: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/jobs", controlAdminToken, map[string]any{
		"command":      "echo",
		"max_attempts": 2,
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create job failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var created hdcf.CreateJobResponse
	_ = json.Unmarshal(raw, &created)

	resp, raw = h.doJSON(t, http.MethodGet, "/next-job?worker_id=worker-b", controlWorkerToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("next job failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var assigned hdcf.AssignedJob
	if err := json.Unmarshal(raw, &assigned); err != nil {
		t.Fatalf("decode assigned job: %v", err)
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/ack", controlWorkerToken, map[string]any{
		"job_id":        assigned.JobID,
		"worker_id":     "worker-b",
		"assignment_id": assigned.AssignmentID,
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ack failed: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/fail", controlWorkerToken, hdcf.FailRequest{
		JobID:        assigned.JobID,
		WorkerID:     "worker-b",
		AssignmentID: assigned.AssignmentID,
		ExitCode:     123,
		Error:        "simulated test failure",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fail failed: %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/jobs/"+assigned.JobID, controlAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("job query failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var job hdcf.JobRead
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	if job.Status != hdcf.StatusPending {
		t.Fatalf("expected pending after first failure, got %q", job.Status)
	}
}

func TestControlAbortEndpoint(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, raw := h.doJSON(t, http.MethodPost, "/jobs", controlAdminToken, map[string]any{
		"command": "echo",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create job failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var created hdcf.CreateJobResponse
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	resp, raw = h.doJSON(t, http.MethodPost, "/abort", controlAdminToken, hdcf.AbortRequest{
		JobID:  created.JobID,
		Reason: "test abort",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("abort failed: %d body=%s", resp.StatusCode, string(raw))
	}
	var aborted struct {
		Status  string `json:"status"`
		Aborted int64  `json:"aborted_jobs"`
	}
	if err := json.Unmarshal(raw, &aborted); err != nil {
		t.Fatalf("decode abort response: %v", err)
	}
	if aborted.Status != hdcf.StatusAborted {
		t.Fatalf("expected status aborted response, got %q", aborted.Status)
	}
	if aborted.Aborted != 1 {
		t.Fatalf("expected aborted_jobs=1, got %d", aborted.Aborted)
	}
}

func TestControlHealthzDegradedOnClosedDB(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, _ := h.doJSON(t, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected healthy endpoint 200, got %d", resp.StatusCode)
	}
	if err := h.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	resp, raw := h.doJSON(t, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected degraded health for closed db, got %d body=%s", resp.StatusCode, string(raw))
	}
}

func TestControlEventsAndInvalidQueryHandling(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, raw := h.doJSON(t, http.MethodGet, "/events?since_id=abc", controlAdminToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad request for invalid since_id, got %d body=%s", resp.StatusCode, string(raw))
	}

	resp, raw = h.doJSON(t, http.MethodGet, "/events?limit=0", controlAdminToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected bad request for invalid limit, got %d body=%s", resp.StatusCode, string(raw))
	}
}

func TestControlUIRequiresAdminToken(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, _ := h.doJSON(t, http.MethodGet, "/ui", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected /ui to require admin token, got %d", resp.StatusCode)
	}

	resp, raw := h.doJSON(t, http.MethodGet, "/ui", controlAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /ui with admin token to return 200, got %d body=%s", resp.StatusCode, string(raw))
	}
}

func TestControlUISupportsQueryToken(t *testing.T) {
	t.Parallel()
	h := newControlHarness(t)

	resp, raw := h.doJSON(t, http.MethodGet, "/ui?token="+controlAdminToken, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /ui with query token to return 200, got %d body=%s", resp.StatusCode, string(raw))
	}
	if !strings.Contains(string(raw), "<title>HDCF Dashboard</title>") {
		t.Fatalf("expected dashboard HTML body, got: %s", string(raw))
	}
}
