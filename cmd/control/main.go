package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"hdcf/internal/hdcf"
	"hdcf/internal/store"
)

const (
	authHeaderName = "X-API-Token"
)

func main() {
	cfg := parseConfig()
	ctx := context.Background()

	s, err := store.Open(cfg.dbPath, cfg.heartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	if err := s.RecoverStaleWorkers(ctx); err != nil {
		log.Printf("startup reconciliation error: %v", err)
	}

	go runReconciler(ctx, s, cfg.reconcileInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", withAuth(cfg.apiToken, createJob(s)))
	mux.HandleFunc("/register", withAuth(cfg.apiToken, registerWorker(s)))
	mux.HandleFunc("/next-job", withAuth(cfg.apiToken, nextJob(s)))
	mux.HandleFunc("/ack", withAuth(cfg.apiToken, ackJob(s)))
	mux.HandleFunc("/heartbeat", withAuth(cfg.apiToken, heartbeat(s)))
	mux.HandleFunc("/reconnect", withAuth(cfg.apiToken, reconnectWorker(s)))
	mux.HandleFunc("/abort", withAuth(cfg.apiToken, abortJob(s)))
	mux.HandleFunc("/complete", withAuth(cfg.apiToken, completeJob(s)))
	mux.HandleFunc("/fail", withAuth(cfg.apiToken, failJob(s)))

	log.Printf("control plane listening on %s (db=%s)", cfg.addr, cfg.dbPath)
	if err := http.ListenAndServe(cfg.addr, mux); err != nil {
		log.Fatalf("http server exited: %v", err)
	}
}

type controlConfig struct {
	addr               string
	dbPath             string
	apiToken           string
	heartbeatTimeout   time.Duration
	reconcileInterval  time.Duration
}

func parseConfig() controlConfig {
	var cfg controlConfig
	flag.StringVar(&cfg.addr, "addr", getenvDefault("HDCF_ADDR", ":8080"), "control plane listen addr")
	flag.StringVar(&cfg.dbPath, "db", getenvDefault("HDCF_DB_PATH", "jobs.db"), "sqlite db path")
	flag.StringVar(&cfg.apiToken, "token", getenvDefault("HDCF_API_TOKEN", "dev-token"), "api token")
	var heartbeatSec int64
	var reconcileSec int64
	flag.Int64Var(&heartbeatSec, "heartbeat-timeout-seconds", int64(getenvInt("HDCF_HEARTBEAT_TIMEOUT_SECONDS", 60)), "heartbeat timeout seconds")
	flag.Int64Var(&reconcileSec, "reconcile-interval-seconds", int64(getenvInt("HDCF_RECONCILE_INTERVAL_SECONDS", 10)), "reconcile interval seconds")
	flag.Parse()
	cfg.heartbeatTimeout = time.Duration(heartbeatSec) * time.Second
	cfg.reconcileInterval = time.Duration(reconcileSec) * time.Second
	return cfg
}

func getenvDefault(name, fallback string) string {
	if v := os.Getenv(name); strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func getenvInt(name string, fallback int64) int64 {
	raw := os.Getenv(name)
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func withAuth(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(token) != "" && r.Header.Get(authHeaderName) != token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func createJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.CreateJobRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		res, err := s.CreateJob(r.Context(), req)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, res)
	}
}

func registerWorker(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.RegisterWorkerRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		req.WorkerID = workerID
		if err := s.RegisterWorker(r.Context(), req); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "worker_id": workerID})
	}
}

func nextJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		workerID := strings.TrimSpace(r.URL.Query().Get("worker_id"))
		if workerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		job, ok, err := s.ClaimNextJob(r.Context(), workerID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !ok || job == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func ackJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.AckJobRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.AcknowledgeJob(r.Context(), req); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "job_id": req.JobID})
	}
}

func heartbeat(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.HeartbeatRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.RecordHeartbeat(r.Context(), req); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func reconnectWorker(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.WorkerReconnectRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		actions, err := s.ReconnectWorker(r.Context(), req)
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, hdcf.WorkerReconnectResponse{
			Status:  "ok",
			Actions: actions,
		})
	}
}

func abortJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.AbortRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		req.JobID = strings.TrimSpace(req.JobID)
		req.WorkerID = strings.TrimSpace(req.WorkerID)
		req.Reason = strings.TrimSpace(req.Reason)
		if req.JobID == "" && req.WorkerID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id or worker_id required"})
			return
		}
		aborted, err := s.AbortJobs(r.Context(), req)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrAbortNoTarget):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
				return
			case errors.Is(err, store.ErrAbortWorkerMismatch), errors.Is(err, store.ErrAbortCompleted):
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			case err.Error() == "job not found":
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case err.Error() == "job state changed during abort request":
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			}
			return
		}
		reason := req.Reason
		if reason == "" {
			reason = "aborted"
		}
		resp := map[string]interface{}{
			"status":       hdcf.StatusAborted,
			"aborted_jobs": aborted,
			"reason":       reason,
		}
		if req.JobID != "" {
			resp["job_id"] = req.JobID
		}
		if req.WorkerID != "" {
			resp["worker_id"] = req.WorkerID
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func completeJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.CompleteRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.CompleteJob(r.Context(), req); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": hdcf.StatusCompleted, "job_id": req.JobID})
	}
}

func failJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.FailRequest
		if err := decodeJSON(w, r, &req); err != nil {
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(r.Context(), s, workerID); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.FailJob(r.Context(), req); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": hdcf.StatusFailed, "job_id": req.JobID})
	}
}

func runReconciler(ctx context.Context, s *store.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RecoverStaleWorkers(context.Background()); err != nil {
				log.Printf("reconcile error: %v", err)
			}
		}
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.Encode(payload)
}

func requireRegisteredWorker(ctx context.Context, s *store.Store, workerID string) error {
	registered, err := s.IsWorkerRegistered(ctx, workerID)
	if err != nil {
		return err
	}
	if !registered {
		return fmt.Errorf("worker_id not registered")
	}
	return nil
}
