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
	auditComponent = "control_plane"
)

func main() {
	cfg := parseConfig()
	ctx := context.Background()

	s, err := store.Open(cfg.dbPath, cfg.heartbeatTimeout)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	auditEvent("info", "control.startup", "", map[string]any{
		"addr":              cfg.addr,
		"db_path":           cfg.dbPath,
		"heartbeat_timeout":  int(cfg.heartbeatTimeout.Seconds()),
		"reconcile_interval": int(cfg.reconcileInterval.Seconds()),
	})
	recoverCtx := store.WithRequestID(ctx, hdcf.NewJobID())
	if err := s.RecoverStaleWorkers(recoverCtx); err != nil {
		auditEvent("error", "control.recovery_startup", "", map[string]any{
			"status": "failed",
			"error":  err.Error(),
		})
	}
	auditEvent("info", "control.recovery_startup", "", map[string]any{
		"status": "ok",
	})

	go runReconciler(ctx, s, cfg.reconcileInterval)

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", withAuth(cfg.apiToken, jobsHandler(s)))
	mux.HandleFunc("/jobs/", withAuth(cfg.apiToken, getJobHandler(s)))
	mux.HandleFunc("/register", withAuth(cfg.apiToken, registerWorker(s)))
	mux.HandleFunc("/next-job", withAuth(cfg.apiToken, nextJob(s)))
	mux.HandleFunc("/ack", withAuth(cfg.apiToken, ackJob(s)))
	mux.HandleFunc("/heartbeat", withAuth(cfg.apiToken, heartbeat(s)))
	mux.HandleFunc("/reconnect", withAuth(cfg.apiToken, reconnectWorker(s)))
	mux.HandleFunc("/abort", withAuth(cfg.apiToken, abortJob(s)))
	mux.HandleFunc("/workers", withAuth(cfg.apiToken, listWorkers(s)))
	mux.HandleFunc("/events", withAuth(cfg.apiToken, listEvents(s)))
	mux.HandleFunc("/complete", withAuth(cfg.apiToken, completeJob(s)))
	mux.HandleFunc("/fail", withAuth(cfg.apiToken, failJob(s)))

	auditEvent("info", "control.listen", "", map[string]any{
		"addr":  cfg.addr,
		"db":    cfg.dbPath,
		"state": "starting",
	})
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
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.jobs_create", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.CreateJobRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.jobs_create", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		auditEvent("info", "control.jobs_create", requestID, map[string]any{
			"command": req.Command,
			"attempts": req.MaxAttempts,
		})
		res, err := s.CreateJob(ctx, req)
		if err != nil {
			auditEvent("warn", "control.jobs_create", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.jobs_create", requestID, map[string]any{
			"job_id": res.JobID,
			"status": res.Status,
		})
		writeJSON(w, http.StatusCreated, res)
	}
}

func jobsHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		switch r.Method {
		case http.MethodGet:
			status := strings.TrimSpace(r.URL.Query().Get("status"))
			workerID := strings.TrimSpace(r.URL.Query().Get("worker_id"))
			jobs, err := s.ListJobs(ctx, status, workerID)
			if err != nil {
				auditEvent("warn", "control.jobs_list", requestID, map[string]any{
					"status_code": http.StatusInternalServerError,
					"status_filter": status,
					"worker_filter": workerID,
					"error":       err.Error(),
				})
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			auditEvent("info", "control.jobs_list", requestID, map[string]any{
				"status_filter": status,
				"worker_filter": workerID,
				"count":         len(jobs),
				"status_code":   http.StatusOK,
			})
			writeJSON(w, http.StatusOK, jobs)
		case http.MethodPost:
			createJob(s)(w, r)
		default:
			auditEvent("warn", "control.jobs_list", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	}
}

func getJobHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			auditEvent("warn", "control.job_get", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		jobID := strings.TrimPrefix(r.URL.Path, "/jobs/")
		jobID = strings.TrimSpace(jobID)
		if jobID == "" || strings.Contains(jobID, "/") {
			auditEvent("warn", "control.job_get", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "invalid job_id",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid job_id"})
			return
		}
		job, ok, err := s.GetJob(ctx, jobID)
		if err != nil {
			auditEvent("warn", "control.job_get", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"job_id":      jobID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !ok {
			auditEvent("warn", "control.job_get", requestID, map[string]any{
				"status_code": http.StatusNotFound,
				"job_id":      jobID,
				"error":       "job not found",
			})
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
			return
		}
		auditEvent("info", "control.job_get", requestID, map[string]any{
			"job_id": job.JobID,
			"status": job.Status,
			"status_code": http.StatusOK,
		})
		writeJSON(w, http.StatusOK, job)
	}
}

func listWorkers(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			auditEvent("warn", "control.workers_list", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		workers, err := s.ListWorkers(ctx)
		if err != nil {
			auditEvent("warn", "control.workers_list", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.workers_list", requestID, map[string]any{
			"count":       len(workers),
			"status_code": http.StatusOK,
		})
		writeJSON(w, http.StatusOK, workers)
	}
}

func listEvents(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			auditEvent("warn", "control.events_list", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		query := r.URL.Query()
		component := strings.TrimSpace(query.Get("component"))
		eventName := strings.TrimSpace(query.Get("event"))
		workerID := strings.TrimSpace(query.Get("worker_id"))
		jobID := strings.TrimSpace(query.Get("job_id"))

		limit := 200
		if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 {
				auditEvent("warn", "control.events_list", requestID, map[string]any{
					"status_code": http.StatusBadRequest,
					"error":       "limit must be a positive integer",
				})
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
				return
			}
			limit = value
		}
		if limit > 5000 {
			limit = 5000
		}

		events, err := s.ListAuditEvents(ctx, component, eventName, workerID, jobID, limit)
		if err != nil {
			auditEvent("warn", "control.events_list", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"component":   component,
				"event":       eventName,
				"worker_id":   workerID,
				"job_id":      jobID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.events_list", requestID, map[string]any{
			"status_code": http.StatusOK,
			"component":   component,
			"event":       eventName,
			"worker_id":   workerID,
			"job_id":      jobID,
			"count":       len(events),
			"limit":       limit,
		})
		writeJSON(w, http.StatusOK, events)
	}
}

func registerWorker(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.worker_register", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.RegisterWorkerRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.worker_register", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			auditEvent("warn", "control.worker_register", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "worker_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		req.WorkerID = workerID
		if err := s.RegisterWorker(ctx, req); err != nil {
			auditEvent("warn", "control.worker_register", requestID, map[string]any{
				"status_code": http.StatusConflict,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.worker_register", requestID, map[string]any{
			"worker_id": workerID,
			"status":    "ok",
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "worker_id": workerID})
	}
}

func nextJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			auditEvent("warn", "control.next_job", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		workerID := strings.TrimSpace(r.URL.Query().Get("worker_id"))
		if workerID == "" {
			auditEvent("warn", "control.next_job", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "worker_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.next_job", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		job, ok, err := s.ClaimNextJob(ctx, workerID)
		if err != nil {
			auditEvent("warn", "control.next_job", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !ok || job == nil {
			auditEvent("info", "control.next_job", requestID, map[string]any{
				"worker_id":   workerID,
				"status_code": http.StatusNoContent,
				"result":      "no_job_available",
			})
			w.WriteHeader(http.StatusNoContent)
			return
		}
		auditEvent("info", "control.next_job", requestID, map[string]any{
			"worker_id":        workerID,
			"job_id":           job.JobID,
			"assignment_id":    job.AssignmentID,
			"attempt_count":    job.AttemptCount,
			"status_code":      http.StatusOK,
			"transition_to":    hdcf.StatusAssigned,
			"assignment_expires_at": job.AssignmentExpiresAt,
		})
		writeJSON(w, http.StatusOK, job)
	}
}

func ackJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.AckJobRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "job_id, worker_id, and assignment_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"job_id":      strings.TrimSpace(req.JobID),
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.AcknowledgeJob(ctx, req); err != nil {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusConflict,
				"job_id":      req.JobID,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.ack", requestID, map[string]any{
			"job_id":        req.JobID,
			"worker_id":     workerID,
			"assignment_id": req.AssignmentID,
			"status_code":   http.StatusOK,
			"from_status":   hdcf.StatusAssigned,
			"to_status":     hdcf.StatusRunning,
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "job_id": req.JobID})
	}
}

func heartbeat(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.HeartbeatRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "worker_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.RecordHeartbeat(ctx, req); err != nil {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.heartbeat", requestID, map[string]any{
			"status_code":      http.StatusOK,
			"worker_id":        workerID,
			"current_job_id":   req.CurrentJobID,
			"timestamp":        req.Timestamp,
			"heartbeat_seq":    req.Sequence,
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func reconnectWorker(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.WorkerReconnectRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if workerID == "" {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "worker_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "worker_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		actions, err := s.ReconnectWorker(ctx, req)
		if err != nil {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code":   http.StatusConflict,
				"worker_id":     workerID,
				"pending_count": len(req.CompletedJobs),
				"error":         err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		actionSummary := make([]map[string]string, 0, len(actions))
		for _, action := range actions {
			actionSummary = append(actionSummary, map[string]string{
				"job_id":        action.JobID,
				"assignment_id": action.AssignmentID,
				"action":        action.Action,
				"result":        action.Result,
			})
		}
		auditEvent("info", "control.reconnect", requestID, map[string]any{
			"status_code":   http.StatusOK,
			"worker_id":     workerID,
			"current_job":   req.CurrentJobID,
			"pending_count": len(req.CompletedJobs),
			"action_count":  len(actions),
			"actions":       actionSummary,
		})
		writeJSON(w, http.StatusOK, hdcf.WorkerReconnectResponse{
			Status:  "ok",
			Actions: actions,
		})
	}
}

func abortJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.abort", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.AbortRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.abort", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		req.JobID = strings.TrimSpace(req.JobID)
		req.WorkerID = strings.TrimSpace(req.WorkerID)
		req.Reason = strings.TrimSpace(req.Reason)
		if req.JobID == "" && req.WorkerID == "" {
			auditEvent("warn", "control.abort", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "job_id or worker_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id or worker_id required"})
			return
		}
		aborted, err := s.AbortJobs(ctx, req)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrAbortNoTarget):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
				return
			case errors.Is(err, store.ErrAbortWorkerMismatch), errors.Is(err, store.ErrAbortCompleted):
				auditEvent("warn", "control.abort", requestID, map[string]any{
					"status_code": http.StatusConflict,
					"worker_id":   req.WorkerID,
					"job_id":      req.JobID,
					"error":       err.Error(),
				})
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			case err.Error() == "job not found":
				auditEvent("warn", "control.abort", requestID, map[string]any{
					"status_code": http.StatusNotFound,
					"worker_id":   req.WorkerID,
					"job_id":      req.JobID,
					"error":       err.Error(),
				})
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case err.Error() == "job state changed during abort request":
				auditEvent("warn", "control.abort", requestID, map[string]any{
					"status_code": http.StatusConflict,
					"worker_id":   req.WorkerID,
					"job_id":      req.JobID,
					"error":       err.Error(),
				})
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			default:
				auditEvent("warn", "control.abort", requestID, map[string]any{
					"status_code": http.StatusConflict,
					"worker_id":   req.WorkerID,
					"job_id":      req.JobID,
					"error":       err.Error(),
				})
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
		auditEvent("info", "control.abort", requestID, map[string]any{
			"status_code":  http.StatusOK,
			"job_id":       req.JobID,
			"worker_id":    req.WorkerID,
			"reason":       req.Reason,
			"aborted_jobs": aborted,
		})
		writeJSON(w, http.StatusOK, resp)
	}
}

func completeJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.CompleteRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "job_id, worker_id, and assignment_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"job_id":      req.JobID,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.CompleteJob(ctx, req); err != nil {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusConflict,
				"job_id":      req.JobID,
				"worker_id":   workerID,
				"assignment_id": req.AssignmentID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.complete", requestID, map[string]any{
			"status_code":   http.StatusOK,
			"job_id":        req.JobID,
			"worker_id":     workerID,
			"assignment_id": req.AssignmentID,
			"artifact_id":   req.ArtifactID,
			"exit_code":     req.ExitCode,
			"completion_seq": req.CompletionSeq,
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": hdcf.StatusCompleted, "job_id": req.JobID})
	}
}

func failJob(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodPost {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		var req hdcf.FailRequest
		if err := decodeJSON(w, r, &req, requestID); err != nil {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       err.Error(),
			})
			return
		}
		workerID := strings.TrimSpace(req.WorkerID)
		if strings.TrimSpace(req.JobID) == "" || workerID == "" || strings.TrimSpace(req.AssignmentID) == "" {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code": http.StatusBadRequest,
				"error":       "job_id, worker_id, and assignment_id required",
			})
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, worker_id, and assignment_id required"})
			return
		}
		if err := requireRegisteredWorker(ctx, s, workerID); err != nil {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code": http.StatusForbidden,
				"job_id":      req.JobID,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			return
		}
		req.WorkerID = workerID
		if err := s.FailJob(ctx, req); err != nil {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code":   http.StatusConflict,
				"job_id":        req.JobID,
				"worker_id":     workerID,
				"assignment_id": req.AssignmentID,
				"exit_code":     req.ExitCode,
				"error":         err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.fail", requestID, map[string]any{
			"status_code":   http.StatusOK,
			"job_id":        req.JobID,
			"worker_id":     workerID,
			"assignment_id": req.AssignmentID,
			"exit_code":     req.ExitCode,
		})
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
			runID := hdcf.NewJobID()
			auditEvent("info", "control.reconcile", runID, map[string]any{
				"status_code": http.StatusOK,
				"action":      "tick",
			})
			reconcileCtx := store.WithRequestID(context.Background(), runID)
			if err := s.RecoverStaleWorkers(reconcileCtx); err != nil {
				auditEvent("error", "control.reconcile", runID, map[string]any{
					"status_code": http.StatusInternalServerError,
					"error":       err.Error(),
				})
			}
		}
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst interface{}, requestID string) error {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		auditEvent("warn", "control.request_decode_error", requestID, map[string]any{
			"error": err.Error(),
		})
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

func requestIDFromHTTP(r *http.Request) string {
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID != "" {
		return requestID
	}
	return hdcf.NewJobID()
}

func auditEvent(level, eventName, requestID string, fields map[string]any) {
	payload := map[string]any{
		"ts":         time.Now().Format(time.RFC3339Nano),
		"component":  auditComponent,
		"level":      level,
		"event":      eventName,
		"request_id": requestID,
	}
	for k, v := range fields {
		payload[k] = v
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to marshal audit event: %v", err)
		return
	}
	log.Printf("%s", string(raw))
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
