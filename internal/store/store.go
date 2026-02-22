package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"hdcf/internal/hdcf"
)

type Store struct {
	db               *sql.DB
	heartbeatTimeout time.Duration
}

type auditRequestIDKey struct{}

var (
	ErrAbortNoTarget       = errors.New("job_id or worker_id required")
	ErrAbortWorkerMismatch = errors.New("worker_id mismatch")
	ErrAbortCompleted      = errors.New("cannot abort completed job")
)

func WithRequestID(ctx context.Context, requestID string) context.Context {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return ctx
	}
	return context.WithValue(ctx, auditRequestIDKey{}, requestID)
}

func requestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v := ctx.Value(auditRequestIDKey{})
	requestID, _ := v.(string)
	return strings.TrimSpace(requestID)
}

func Open(path string, heartbeatTimeout time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		return nil, err
	}

	s := &Store{db: db, heartbeatTimeout: heartbeatTimeout}
	if err := s.initSchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		status TEXT NOT NULL CHECK (status IN ('PENDING','ASSIGNED','RUNNING','COMPLETED','FAILED','LOST','RETRYING','ABORTED')),
		command TEXT NOT NULL,
		args TEXT NOT NULL,
		working_dir TEXT,
		timeout_ms INTEGER NOT NULL DEFAULT 0,
		priority INTEGER NOT NULL DEFAULT 0,
		scheduled_at INTEGER NOT NULL DEFAULT 0,
		needs_gpu INTEGER NOT NULL DEFAULT 0,
		requirements TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		attempt_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		worker_id TEXT,
		assignment_id TEXT,
		assignment_expires_at INTEGER,
		last_error TEXT,
		completion_seq INTEGER NOT NULL DEFAULT 0,
		result_path TEXT,
		artifact_id TEXT,
		artifact_stdout_tmp_path TEXT,
		artifact_stdout_path TEXT,
		artifact_stdout_sha256 TEXT,
		artifact_stderr_tmp_path TEXT,
		artifact_stderr_path TEXT,
		artifact_stderr_sha256 TEXT,
		updated_by TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at);
	CREATE TABLE IF NOT EXISTS workers (
		worker_id TEXT PRIMARY KEY,
		last_seen INTEGER NOT NULL,
		current_job_id TEXT,
		worker_capabilities TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		registered_at INTEGER,
		registration_nonce TEXT,
		heartbeat_seq INTEGER NOT NULL DEFAULT 0,
		heartbeat_metrics TEXT
	);
	CREATE TABLE IF NOT EXISTS audit_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ts INTEGER NOT NULL,
		component TEXT NOT NULL,
		level TEXT NOT NULL,
		event TEXT NOT NULL,
		request_id TEXT,
		worker_id TEXT,
		job_id TEXT,
		details TEXT NOT NULL DEFAULT '{}'
	);
	CREATE INDEX IF NOT EXISTS idx_audit_events_ts ON audit_events(ts);
	CREATE INDEX IF NOT EXISTS idx_audit_events_component_event ON audit_events(component, event);
	CREATE INDEX IF NOT EXISTS idx_audit_events_component ON audit_events(component);
	CREATE INDEX IF NOT EXISTS idx_audit_events_event ON audit_events(event);
	CREATE INDEX IF NOT EXISTS idx_audit_events_ts_id ON audit_events(ts, id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_job ON audit_events(job_id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_worker ON audit_events(worker_id);
	`
	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return err
	}
	if err := s.ensureJobColumns(ctx); err != nil {
		return err
	}
	if err := s.ensureAuditEventColumns(ctx); err != nil {
		return err
	}
	if err := s.ensureAuditEventIndexes(ctx); err != nil {
		return err
	}
	if err := s.ensureJobIndexes(ctx); err != nil {
		return err
	}
	return s.ensureWorkerColumns(ctx)
}

func (s *Store) ensureAuditEventColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(audit_events)")
	if err != nil {
		return err
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	type columnDef struct {
		name string
		ddl  string
	}
	need := []columnDef{
		{name: "request_id", ddl: "ALTER TABLE audit_events ADD COLUMN request_id TEXT"},
		{name: "worker_id", ddl: "ALTER TABLE audit_events ADD COLUMN worker_id TEXT"},
		{name: "job_id", ddl: "ALTER TABLE audit_events ADD COLUMN job_id TEXT"},
		{name: "details", ddl: "ALTER TABLE audit_events ADD COLUMN details TEXT NOT NULL DEFAULT '{}'"},
	}
	for _, col := range need {
		if _, ok := columns[col.name]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, col.ddl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureAuditEventIndexes(ctx context.Context) error {
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_audit_events_component_event_ts_id ON audit_events(component, event, ts, id)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_events_event_ts_id ON audit_events(event, ts, id)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_events_worker_ts_id ON audit_events(worker_id, ts, id)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_events_job_ts_id ON audit_events(job_id, ts, id)`,
	}
	for _, ddl := range indexes {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) logEvent(ctx context.Context, tx *sql.Tx, component, level, eventName, workerID, jobID string, details map[string]any) error {
	requestID := requestIDFromContext(ctx)
	now := time.Now().Unix()
	if strings.TrimSpace(level) == "" {
		level = "info"
	}
	if strings.TrimSpace(eventName) == "" {
		return nil
	}
	if details == nil {
		details = map[string]any{}
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	stmt := `INSERT INTO audit_events (ts, component, level, event, request_id, worker_id, job_id, details) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if tx != nil {
		_, err = tx.ExecContext(ctx, stmt,
			now,
			"control_plane",
			level,
			eventName,
			requestID,
			workerID,
			jobID,
			string(raw),
		)
		return err
	}
	_, err = s.db.ExecContext(ctx, stmt,
		now,
		"control_plane",
		level,
		eventName,
		requestID,
		workerID,
		jobID,
		string(raw),
	)
	return err
}

func (s *Store) heartbeatMetricsFromRequest(req hdcf.HeartbeatRequest) (string, bool, error) {
	if req.Metrics == nil {
		return "", false, nil
	}
	metrics := map[string]any{}
	if req.Metrics.CPUUsagePercent != nil {
		metrics["cpu_usage_percent"] = *req.Metrics.CPUUsagePercent
	}
	if req.Metrics.MemoryUsageMB != nil {
		metrics["memory_usage_mb"] = *req.Metrics.MemoryUsageMB
	}
	if req.Metrics.GPUUsagePercent != nil {
		metrics["gpu_usage_percent"] = *req.Metrics.GPUUsagePercent
	}
	if req.Metrics.GPUMemoryUsageMB != nil {
		metrics["gpu_memory_usage_mb"] = *req.Metrics.GPUMemoryUsageMB
	}
	if len(metrics) == 0 {
		return "", false, nil
	}
	raw, err := json.Marshal(metrics)
	if err != nil {
		return "", true, err
	}
	return string(raw), true, nil
}

func (s *Store) ensureJobColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(jobs)")
	if err != nil {
		return err
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	type columnDef struct {
		name string
		ddl  string
	}
	need := []columnDef{
		{name: "assignment_id", ddl: "ALTER TABLE jobs ADD COLUMN assignment_id TEXT"},
		{name: "assignment_expires_at", ddl: "ALTER TABLE jobs ADD COLUMN assignment_expires_at INTEGER"},
		{name: "priority", ddl: "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"},
		{name: "scheduled_at", ddl: "ALTER TABLE jobs ADD COLUMN scheduled_at INTEGER NOT NULL DEFAULT 0"},
		{name: "needs_gpu", ddl: "ALTER TABLE jobs ADD COLUMN needs_gpu INTEGER NOT NULL DEFAULT 0"},
		{name: "requirements", ddl: "ALTER TABLE jobs ADD COLUMN requirements TEXT NOT NULL DEFAULT ''"},
		{name: "artifact_id", ddl: "ALTER TABLE jobs ADD COLUMN artifact_id TEXT"},
		{name: "artifact_stdout_tmp_path", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stdout_tmp_path TEXT"},
		{name: "artifact_stdout_path", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stdout_path TEXT"},
		{name: "artifact_stdout_sha256", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stdout_sha256 TEXT"},
		{name: "artifact_stderr_tmp_path", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stderr_tmp_path TEXT"},
		{name: "artifact_stderr_path", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stderr_path TEXT"},
		{name: "artifact_stderr_sha256", ddl: "ALTER TABLE jobs ADD COLUMN artifact_stderr_sha256 TEXT"},
		{name: "completion_seq", ddl: "ALTER TABLE jobs ADD COLUMN completion_seq INTEGER NOT NULL DEFAULT 0"},
	}
	for _, col := range need {
		if _, ok := columns[col.name]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, col.ddl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureJobIndexes(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_jobs_status_queue_order ON jobs(status, priority DESC, scheduled_at ASC, created_at ASC)")
	return err
}

func (s *Store) ensureWorkerColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(workers)")
	if err != nil {
		return err
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	type columnDef struct {
		name string
		ddl  string
	}
	need := []columnDef{
		{name: "registered_at", ddl: "ALTER TABLE workers ADD COLUMN registered_at INTEGER"},
		{name: "registration_nonce", ddl: "ALTER TABLE workers ADD COLUMN registration_nonce TEXT"},
		{name: "worker_capabilities", ddl: "ALTER TABLE workers ADD COLUMN worker_capabilities TEXT NOT NULL DEFAULT ''"},
		{name: "heartbeat_seq", ddl: "ALTER TABLE workers ADD COLUMN heartbeat_seq INTEGER NOT NULL DEFAULT 0"},
		{name: "heartbeat_metrics", ddl: "ALTER TABLE workers ADD COLUMN heartbeat_metrics TEXT"},
	}
	for _, col := range need {
		if _, ok := columns[col.name]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, col.ddl); err != nil {
			return err
		}
	}
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func normalizeCapabilities(input []string) []string {
	out := make([]string, 0, len(input))
	seen := map[string]struct{}{}
	for _, capab := range input {
		c := strings.TrimSpace(strings.ToLower(capab))
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

func hasCapability(caps []string, target string) bool {
	target = strings.TrimSpace(strings.ToLower(target))
	if target == "" {
		return false
	}
	for _, c := range caps {
		if c == target {
			return true
		}
	}
	return false
}

func hasAllCapabilities(workerCaps []string, required []string) bool {
	for _, req := range required {
		if !hasCapability(workerCaps, req) {
			return false
		}
	}
	return true
}

func workerCapabilitiesMatch(workerCaps []string, needsGPU bool, requirementsJSON string) bool {
	var required []string
	if strings.TrimSpace(requirementsJSON) != "" {
		if err := json.Unmarshal([]byte(requirementsJSON), &required); err != nil {
			return false
		}
	}
	required = normalizeCapabilities(required)

	if needsGPU {
		if !hasCapability(workerCaps, "gpu") {
			return false
		}
	}
	return hasAllCapabilities(workerCaps, required)
}

func (s *Store) getWorkerCapabilitiesTx(ctx context.Context, tx *sql.Tx, workerID string) ([]string, error) {
	var capabilitiesJSON string
	if err := tx.QueryRowContext(ctx, `SELECT worker_capabilities FROM workers WHERE worker_id = ?`, workerID).Scan(&capabilitiesJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("worker not registered")
		}
		return nil, err
	}
	if strings.TrimSpace(capabilitiesJSON) == "" {
		return []string{}, nil
	}
	var caps []string
	if err := json.Unmarshal([]byte(capabilitiesJSON), &caps); err != nil {
		return nil, err
	}
	return normalizeCapabilities(caps), nil
}

func (s *Store) CreateJob(ctx context.Context, req hdcf.CreateJobRequest) (hdcf.CreateJobResponse, error) {
	if strings.TrimSpace(req.Command) == "" {
		return hdcf.CreateJobResponse{}, errors.New("command required")
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	now := time.Now().Unix()
	priority := req.Priority
	if priority < 0 {
		priority = 0
	}
	scheduledAt := req.ScheduledAt
	if scheduledAt <= 0 {
		scheduledAt = now
	}
	requirementsJSON, err := json.Marshal(normalizeCapabilities(req.Requirements))
	if err != nil {
		return hdcf.CreateJobResponse{}, err
	}

	argsJSON, err := json.Marshal(req.Args)
	if err != nil {
		return hdcf.CreateJobResponse{}, err
	}

	id := hdcf.NewJobID()
	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO jobs (id, status, command, args, working_dir, timeout_ms, priority, scheduled_at, needs_gpu, requirements, created_at, updated_at, attempt_count, max_attempts)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
		id,
		hdcf.StatusPending,
		req.Command,
		string(argsJSON),
		req.WorkingDir,
		req.TimeoutMs,
		priority,
		scheduledAt,
		boolToInt(req.NeedsGPU),
		string(requirementsJSON),
		now,
		now,
		req.MaxAttempts,
	)
	if err != nil {
		return hdcf.CreateJobResponse{}, err
	}

	_ = s.logEvent(ctx, nil, "info", "job.create", "", id, map[string]any{
		"command":      req.Command,
		"status":       hdcf.StatusPending,
		"args_count":   len(req.Args),
		"attempts":     req.MaxAttempts,
		"priority":     priority,
		"scheduled_at": scheduledAt,
		"needs_gpu":    req.NeedsGPU,
	})

	return hdcf.CreateJobResponse{
		JobID:  id,
		Status: hdcf.StatusPending,
	}, nil
}

func (s *Store) ListJobs(ctx context.Context, statusFilter, workerIDFilter string) ([]hdcf.JobRead, error) {
	now := time.Now().Unix()
	statusFilter = strings.TrimSpace(statusFilter)
	workerIDFilter = strings.TrimSpace(workerIDFilter)

	query := `
		SELECT j.id, j.status, j.command, j.args, j.working_dir, j.timeout_ms, j.priority, j.scheduled_at, j.needs_gpu, j.requirements, j.created_at, j.updated_at,
		       j.attempt_count, j.max_attempts, j.worker_id, j.assignment_id, j.assignment_expires_at,
		       j.last_error, j.result_path, j.updated_by, w.last_seen
		FROM jobs j
		LEFT JOIN workers w ON w.worker_id = j.worker_id
	`
	args := []any{}
	filters := []string{}
	if statusFilter != "" {
		filters = append(filters, "j.status = ?")
		args = append(args, statusFilter)
	}
	if workerIDFilter != "" {
		filters = append(filters, "j.worker_id = ?")
		args = append(args, workerIDFilter)
	}
	if len(filters) > 0 {
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY j.created_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]hdcf.JobRead, 0, 32)
	for rows.Next() {
		var job hdcf.JobRead
		var argsJSON string
		var requirementsJSON string
		var needsGPU int
		var workerID sql.NullString
		var assignmentID sql.NullString
		var assignmentExpires sql.NullInt64
		var workerLastSeen sql.NullInt64
		if err := rows.Scan(
			&job.JobID,
			&job.Status,
			&job.Command,
			&argsJSON,
			&job.WorkingDir,
			&job.TimeoutMs,
			&job.Priority,
			&job.ScheduledAt,
			&needsGPU,
			&requirementsJSON,
			&job.CreatedAt,
			&job.UpdatedAt,
			&job.AttemptCount,
			&job.MaxAttempts,
			&workerID,
			&assignmentID,
			&assignmentExpires,
			&job.LastError,
			&job.ResultPath,
			&job.UpdatedBy,
			&workerLastSeen,
		); err != nil {
			return nil, err
		}
		if workerID.Valid {
			job.WorkerID = strings.TrimSpace(workerID.String)
		}
		if assignmentID.Valid {
			job.AssignmentID = assignmentID.String
		}
		if assignmentExpires.Valid {
			job.AssignmentExpiresAt = assignmentExpires.Int64
		}
		job.NeedsGPU = needsGPU == 1
		if strings.TrimSpace(requirementsJSON) != "" {
			if err := json.Unmarshal([]byte(requirementsJSON), &job.Requirements); err != nil {
				return nil, err
			}
		}
		if job.WorkerID != "" && workerLastSeen.Valid {
			age := now - workerLastSeen.Int64
			if age < 0 {
				age = 0
			}
			job.HeartbeatAgeSec = &age
		}
		if strings.TrimSpace(argsJSON) != "" {
			if err := json.Unmarshal([]byte(argsJSON), &job.Args); err != nil {
				return nil, err
			}
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (hdcf.JobRead, bool, error) {
	now := time.Now().Unix()
	id := strings.TrimSpace(jobID)
	if id == "" {
		return hdcf.JobRead{}, false, nil
	}

	query := `
		SELECT j.id, j.status, j.command, j.args, j.working_dir, j.timeout_ms, j.priority, j.scheduled_at, j.needs_gpu, j.requirements, j.created_at, j.updated_at,
		       j.attempt_count, j.max_attempts, j.worker_id, j.assignment_id, j.assignment_expires_at,
		       j.last_error, j.result_path, j.updated_by, w.last_seen
		FROM jobs j
		LEFT JOIN workers w ON w.worker_id = j.worker_id
		WHERE j.id = ?
	`
	var job hdcf.JobRead
	var argsJSON string
	var requirementsJSON string
	var needsGPU int
	var workerID sql.NullString
	var assignmentID sql.NullString
	var assignmentExpires sql.NullInt64
	var workerLastSeen sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query, id).Scan(
		&job.JobID,
		&job.Status,
		&job.Command,
		&argsJSON,
		&job.WorkingDir,
		&job.TimeoutMs,
		&job.Priority,
		&job.ScheduledAt,
		&needsGPU,
		&requirementsJSON,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.AttemptCount,
		&job.MaxAttempts,
		&workerID,
		&assignmentID,
		&assignmentExpires,
		&job.LastError,
		&job.ResultPath,
		&job.UpdatedBy,
		&workerLastSeen,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hdcf.JobRead{}, false, nil
		}
		return hdcf.JobRead{}, false, err
	}
	if workerID.Valid {
		job.WorkerID = strings.TrimSpace(workerID.String)
	}
	if assignmentID.Valid {
		job.AssignmentID = assignmentID.String
	}
	if assignmentExpires.Valid {
		job.AssignmentExpiresAt = assignmentExpires.Int64
	}
	job.NeedsGPU = needsGPU == 1
	if strings.TrimSpace(requirementsJSON) != "" {
		if err := json.Unmarshal([]byte(requirementsJSON), &job.Requirements); err != nil {
			return hdcf.JobRead{}, false, err
		}
	}
	if job.WorkerID != "" && workerLastSeen.Valid {
		age := now - workerLastSeen.Int64
		if age < 0 {
			age = 0
		}
		job.HeartbeatAgeSec = &age
	}
	if strings.TrimSpace(argsJSON) != "" {
		if err := json.Unmarshal([]byte(argsJSON), &job.Args); err != nil {
			return hdcf.JobRead{}, false, err
		}
	}
	return job, true, nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]hdcf.WorkerRead, error) {
	now := time.Now().Unix()
	query := `
		SELECT worker_id, last_seen, current_job_id, status, registered_at, registration_nonce, worker_capabilities, heartbeat_metrics
		FROM workers
		ORDER BY worker_id
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]hdcf.WorkerRead, 0, 16)
	for rows.Next() {
		var w hdcf.WorkerRead
		var currentJobID sql.NullString
		var capabilitiesJSON string
		var heartbeatMetricsJSON sql.NullString
		if err := rows.Scan(
			&w.WorkerID,
			&w.LastSeen,
			&currentJobID,
			&w.Status,
			&w.RegisteredAt,
			&w.RegistrationNonce,
			&capabilitiesJSON,
			&heartbeatMetricsJSON,
		); err != nil {
			return nil, err
		}
		if currentJobID.Valid {
			w.CurrentJobID = strings.TrimSpace(currentJobID.String)
		}
		if strings.TrimSpace(capabilitiesJSON) != "" {
			if err := json.Unmarshal([]byte(capabilitiesJSON), &w.Capabilities); err != nil {
				return nil, err
			}
		}
		if heartbeatMetricsJSON.Valid && strings.TrimSpace(heartbeatMetricsJSON.String) != "" {
			var metrics map[string]interface{}
			if err := json.Unmarshal([]byte(heartbeatMetricsJSON.String), &metrics); err != nil {
				return nil, err
			}
			w.HeartbeatMetrics = metrics
		}
		age := now - w.LastSeen
		if age < 0 {
			age = 0
		}
		w.HeartbeatAgeSec = age
		result = append(result, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) ListAuditEvents(ctx context.Context, component, eventName, workerID, jobID string, sinceID int64, limit int) ([]hdcf.AuditEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 5000 {
		limit = 5000
	}

	component = strings.TrimSpace(component)
	eventName = strings.TrimSpace(eventName)
	workerID = strings.TrimSpace(workerID)
	jobID = strings.TrimSpace(jobID)

	query := `
		SELECT id, ts, component, level, event, request_id, worker_id, job_id, details
		FROM audit_events
	`
	args := []any{}
	filters := []string{}
	if component != "" {
		filters = append(filters, "component = ?")
		args = append(args, component)
	}
	if eventName != "" {
		filters = append(filters, "event = ?")
		args = append(args, eventName)
	}
	if workerID != "" {
		filters = append(filters, "worker_id = ?")
		args = append(args, workerID)
	}
	if jobID != "" {
		filters = append(filters, "job_id = ?")
		args = append(args, jobID)
	}
	if sinceID > 0 {
		filters = append(filters, "id > ?")
		args = append(args, sinceID)
	}
	if len(filters) > 0 {
		query += " WHERE " + strings.Join(filters, " AND ")
	}
	query += " ORDER BY ts DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]hdcf.AuditEvent, 0, 32)
	for rows.Next() {
		var row hdcf.AuditEvent
		var detailsJSON string
		var requestID sql.NullString
		var rowWorkerID sql.NullString
		var rowJobID sql.NullString
		if err := rows.Scan(
			&row.ID,
			&row.Timestamp,
			&row.Component,
			&row.Level,
			&row.Event,
			&requestID,
			&rowWorkerID,
			&rowJobID,
			&detailsJSON,
		); err != nil {
			return nil, err
		}
		row.RequestID = strings.TrimSpace(requestID.String)
		if rowWorkerID.Valid {
			row.WorkerID = strings.TrimSpace(rowWorkerID.String)
		}
		if rowJobID.Valid {
			row.JobID = strings.TrimSpace(rowJobID.String)
		}
		if strings.TrimSpace(detailsJSON) == "" {
			row.Details = map[string]interface{}{}
		} else if err := json.Unmarshal([]byte(detailsJSON), &row.Details); err != nil {
			return nil, err
		}
		if row.Details == nil {
			row.Details = map[string]interface{}{}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) IsWorkerRegistered(ctx context.Context, workerID string) (bool, error) {
	id := strings.TrimSpace(workerID)
	if id == "" {
		return false, nil
	}

	var found string
	if err := s.db.QueryRowContext(ctx, `SELECT worker_id FROM workers WHERE worker_id = ?`, id).Scan(&found); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) RegisterWorker(ctx context.Context, req hdcf.RegisterWorkerRequest) error {
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		return fmt.Errorf("worker_id required")
	}
	nonce := strings.TrimSpace(req.Nonce)
	capabilitiesJSON, err := json.Marshal(normalizeCapabilities(req.Capabilities))
	if err != nil {
		return err
	}
	now := time.Now().Unix()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existingNonce sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT registration_nonce FROM workers WHERE worker_id = ?`, workerID).Scan(&existingNonce); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	} else if nonce != "" && existingNonce.Valid && strings.TrimSpace(existingNonce.String) != "" && existingNonce.String != nonce {
		return fmt.Errorf("registration nonce mismatch")
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO workers (worker_id, last_seen, current_job_id, status, registered_at, registration_nonce, worker_capabilities, heartbeat_seq)
		 VALUES (?, ?, NULL, 'ONLINE', ?, ?, ?, 0)
		 ON CONFLICT(worker_id) DO UPDATE SET
		   last_seen = excluded.last_seen,
		   status = 'ONLINE',
		   registered_at = excluded.registered_at,
		   worker_capabilities = CASE
		      WHEN NULLIF(excluded.worker_capabilities, '') IS NULL THEN workers.worker_capabilities
		      ELSE excluded.worker_capabilities
		   END,
		   registration_nonce = CASE
		      WHEN NULLIF(excluded.registration_nonce, '') IS NULL THEN workers.registration_nonce
		      ELSE excluded.registration_nonce
		   END`,
		workerID,
		now,
		now,
		nonce,
		string(capabilitiesJSON),
	)
	if err != nil {
		return err
	}
	_ = s.logEvent(ctx, tx, "info", "worker.register", workerID, "", map[string]any{
		"registered_at": now,
		"nonce":         nonce != "",
		"capabilities":  len(req.Capabilities),
	})
	return tx.Commit()
}

func (s *Store) ClaimNextJob(ctx context.Context, workerID string) (*hdcf.AssignedJob, bool, error) {
	if !hdcf.IsValidTransition(hdcf.StatusPending, hdcf.StatusAssigned) {
		return nil, false, errors.New("invalid transition PENDING -> ASSIGNED")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	cutoff := time.Now().Unix() - int64(s.heartbeatTimeout.Seconds())
	now := time.Now()
	nowSec := now.Unix()
	workerCaps, err := s.getWorkerCapabilitiesTx(ctx, tx, workerID)
	if err != nil {
		return nil, false, err
	}
	assignmentID := hdcf.NewJobID()
	assignmentExpiresAt := now.Add(s.heartbeatTimeout).Unix()

	candidates, err := tx.QueryContext(
		ctx,
		`SELECT id, command, args, working_dir, timeout_ms, attempt_count, max_attempts, needs_gpu, requirements
		FROM jobs
		WHERE status = ?
		  AND attempt_count < max_attempts
		  AND scheduled_at <= ?
		  AND (
			worker_id IS NULL
			OR worker_id = ''
			OR NOT EXISTS (
				SELECT 1 FROM workers w
				WHERE w.worker_id = jobs.worker_id
					AND w.status = 'ONLINE'
		      AND w.last_seen >= ?
			)
		  )
		ORDER BY priority DESC, created_at ASC, id ASC
		LIMIT 64`,
		hdcf.StatusPending,
		nowSec,
		cutoff,
	)
	if err != nil {
		return nil, false, err
	}
	defer candidates.Close()

	for candidates.Next() {
		var job hdcf.AssignedJob
		var argsJSON string
		var requirementsJSON string
		var needsGPU int
		if err := candidates.Scan(
			&job.JobID,
			&job.Command,
			&argsJSON,
			&job.WorkingDir,
			&job.TimeoutMs,
			&job.AttemptCount,
			&job.MaxAttempts,
			&needsGPU,
			&requirementsJSON,
		); err != nil {
			return nil, false, err
		}
		if err := json.Unmarshal([]byte(argsJSON), &job.Args); err != nil {
			return nil, false, err
		}

		if !workerCapabilitiesMatch(workerCaps, needsGPU == 1, requirementsJSON) {
			continue
		}

		res, err := tx.ExecContext(
			ctx,
			`UPDATE jobs SET status = ?, worker_id = ?, assignment_id = ?, assignment_expires_at = ?, attempt_count = attempt_count + 1, updated_at = ?, updated_by = ? WHERE id = ? AND status = ?`,
			hdcf.StatusAssigned,
			workerID,
			assignmentID,
			assignmentExpiresAt,
			time.Now().Unix(),
			workerID,
			job.JobID,
			hdcf.StatusPending,
		)
		if err != nil {
			return nil, false, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, false, err
		}
		if affected == 0 {
			continue
		}

		job.AttemptCount++
		job.AssignmentID = assignmentID
		job.AssignmentExpiresAt = assignmentExpiresAt
		_ = s.logEvent(ctx, tx, "info", "job.claim", workerID, job.JobID, map[string]any{
			"from_status":         hdcf.StatusPending,
			"to_status":           hdcf.StatusAssigned,
			"assignment_id":       assignmentID,
			"attempt_count":       job.AttemptCount,
			"assignment_expires_at": assignmentExpiresAt,
		})
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		return &job, true, nil
	}
	if err := candidates.Err(); err != nil {
		return nil, false, err
	}

	return nil, false, nil
}

func (s *Store) AcknowledgeJob(ctx context.Context, req hdcf.AckJobRequest) error {
	now := time.Now().Unix()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status, workerID, assignmentID sql.NullString
	var assignmentExpiresAt sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT status, worker_id, assignment_id, assignment_expires_at FROM jobs WHERE id = ?`, req.JobID).
		Scan(&status, &workerID, &assignmentID, &assignmentExpiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("job not found")
		}
		return err
	}
	currentStatus := strings.TrimSpace(status.String)

	if currentStatus == hdcf.StatusCompleted {
		_ = s.logEvent(ctx, tx, "info", "job.ack", req.WorkerID, req.JobID, map[string]any{
			"from_status": currentStatus,
			"to_status":   currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "noop_terminal",
		})
		return tx.Commit()
	}
	if currentStatus == hdcf.StatusRunning {
		if strings.TrimSpace(req.WorkerID) == "" {
			return fmt.Errorf("worker_id required")
		}
		if workerID.Valid && workerID.String != req.WorkerID {
			return fmt.Errorf("job worker mismatch for ack")
		}
		if !assignmentID.Valid || assignmentID.String == "" {
			return fmt.Errorf("assignment not expected")
		}
		if strings.TrimSpace(req.AssignmentID) == "" {
			return fmt.Errorf("assignment_id required")
		}
		if assignmentID.String != req.AssignmentID {
			return fmt.Errorf("assignment_id mismatch")
		}
		_ = s.logEvent(ctx, tx, "info", "job.ack", req.WorkerID, req.JobID, map[string]any{
			"from_status":  currentStatus,
			"to_status":    currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "already_running",
		})
		return tx.Commit()
	}
	if currentStatus != hdcf.StatusAssigned {
		return fmt.Errorf("job state not ackable: %s", currentStatus)
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		return fmt.Errorf("worker_id required")
	}
	if strings.TrimSpace(req.AssignmentID) == "" {
		return fmt.Errorf("assignment_id required")
	}
	if workerID.Valid && workerID.String != req.WorkerID {
		return fmt.Errorf("job worker mismatch for ack")
	}
	if !assignmentID.Valid || assignmentID.String == "" {
		return fmt.Errorf("assignment_id mismatch")
	}
	if assignmentID.String != req.AssignmentID {
		return fmt.Errorf("assignment_id mismatch")
	}
	if assignmentExpiresAt.Valid && assignmentExpiresAt.Int64 > 0 && assignmentExpiresAt.Int64 < now {
		return fmt.Errorf("assignment expired")
	}
	if !hdcf.IsValidTransition(hdcf.StatusAssigned, hdcf.StatusRunning) {
		return fmt.Errorf("invalid transition %s -> %s", hdcf.StatusAssigned, hdcf.StatusRunning)
	}

	_, err = tx.ExecContext(
		ctx,
		`UPDATE jobs SET status = ?, updated_at = ?, updated_by = ? WHERE id = ? AND status = ?`,
		hdcf.StatusRunning,
		now,
		req.WorkerID,
		req.JobID,
		hdcf.StatusAssigned,
	)
	if err != nil {
		return err
	}
	_ = s.logEvent(ctx, tx, "info", "job.ack", req.WorkerID, req.JobID, map[string]any{
		"from_status":  currentStatus,
		"to_status":    hdcf.StatusRunning,
		"assignment_id": req.AssignmentID,
	})
	return tx.Commit()
}

func (s *Store) ReconnectWorker(ctx context.Context, req hdcf.WorkerReconnectRequest) ([]hdcf.ReconnectAction, error) {
	if strings.TrimSpace(req.WorkerID) == "" {
		return nil, fmt.Errorf("worker_id required")
	}
	workerID := strings.TrimSpace(req.WorkerID)

	actions := make([]hdcf.ReconnectAction, 0, 4+len(req.CompletedJobs))

	now := time.Now().Unix()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var currentJob interface{}
	currentJobID := ""
	if req.CurrentJobID != nil {
		currentJobID = strings.TrimSpace(*req.CurrentJobID)
	}
	if currentJobID != "" {
		currentJob = currentJobID
	}

	var existingWorker sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT worker_id FROM workers WHERE worker_id = ?`, workerID).Scan(&existingWorker); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("worker not registered")
		}
		return nil, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE workers
		 SET last_seen = ?, current_job_id = ?, status = 'ONLINE'
		 WHERE worker_id = ?`,
		now,
		currentJob,
		workerID,
	); err != nil {
		return nil, err
	}

	if currentJobID != "" {
		var status, owner sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT status, worker_id FROM jobs WHERE id = ?`, currentJobID).Scan(&status, &owner)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if errors.Is(err, sql.ErrNoRows) {
			actions = append(actions, hdcf.ReconnectAction{
				JobID:   currentJobID,
				Action:  hdcf.ReconnectActionClearCurrentJob,
				Result:  hdcf.ReconnectResultAccepted,
				Error:   "job not found",
			})
			if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, req.WorkerID); err != nil {
				return nil, err
			}
		} else {
			ownerID := strings.TrimSpace(owner.String)
			switch status.String {
			case hdcf.StatusRunning:
				if ownerID == req.WorkerID {
					actions = append(actions, hdcf.ReconnectAction{
						JobID:  currentJobID,
						Action: hdcf.ReconnectActionKeepCurrentJob,
						Result: hdcf.ReconnectResultAccepted,
					})
					_ = s.logEvent(ctx, tx, "info", "worker.reconnect_current_job", req.WorkerID, currentJobID, map[string]any{
						"action":   "keep_current_job",
						"status":   status.String,
						"worker_id": req.WorkerID,
					})
				} else {
					if _, err := tx.ExecContext(
						ctx,
						`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL, updated_by = 'reconnect', updated_at = ? WHERE id = ? AND status = ?`,
						hdcf.StatusLost,
						now,
						currentJobID,
						hdcf.StatusRunning,
					); err != nil {
						return nil, err
					}
					_ = s.logEvent(ctx, tx, "info", "worker.reconnect_current_job", req.WorkerID, currentJobID, map[string]any{
						"action":   "reassign_to_lost",
						"from_status": hdcf.StatusRunning,
						"to_status":   hdcf.StatusLost,
						"owner_id":     ownerID,
					})
					actions = append(actions, hdcf.ReconnectAction{
						JobID:  currentJobID,
						Action: hdcf.ReconnectActionClearCurrentJob,
						Result: hdcf.ReconnectResultAccepted,
						Error:   "ownership mismatch",
					})
					if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, req.WorkerID); err != nil {
						return nil, err
					}
				}
			case hdcf.StatusAssigned:
				if ownerID == req.WorkerID {
					actions = append(actions, hdcf.ReconnectAction{
						JobID:  currentJobID,
						Action: hdcf.ReconnectActionKeepCurrentJob,
						Result: hdcf.ReconnectResultAccepted,
					})
					_ = s.logEvent(ctx, tx, "info", "worker.reconnect_current_job", req.WorkerID, currentJobID, map[string]any{
						"action":   "keep_current_job",
						"status":   status.String,
						"worker_id": req.WorkerID,
					})
				} else {
					if _, err := tx.ExecContext(
						ctx,
						`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL, updated_by = 'reconnect', updated_at = ? WHERE id = ? AND status = ?`,
						hdcf.StatusPending,
						now,
						currentJobID,
						hdcf.StatusAssigned,
					); err != nil {
						return nil, err
					}
					_ = s.logEvent(ctx, tx, "info", "worker.reconnect_current_job", req.WorkerID, currentJobID, map[string]any{
						"action":    "reassign_to_pending",
						"from_status": hdcf.StatusAssigned,
						"to_status":   hdcf.StatusPending,
						"owner_id":    ownerID,
					})
					actions = append(actions, hdcf.ReconnectAction{
						JobID:  currentJobID,
						Action: hdcf.ReconnectActionClearCurrentJob,
						Result: hdcf.ReconnectResultAccepted,
						Error:   "ownership mismatch",
					})
					if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, req.WorkerID); err != nil {
						return nil, err
					}
				}
			default:
				actions = append(actions, hdcf.ReconnectAction{
					JobID:  currentJobID,
					Action: hdcf.ReconnectActionClearCurrentJob,
					Result: hdcf.ReconnectResultAccepted,
				})
				_ = s.logEvent(ctx, tx, "info", "worker.reconnect_current_job", req.WorkerID, currentJobID, map[string]any{
					"action": "clear_current_job",
					"status": status.String,
				})
				if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, req.WorkerID); err != nil {
					return nil, err
				}
			}
		}
	}

	_ = s.logEvent(ctx, tx, "info", "worker.reconnect", req.WorkerID, "", map[string]any{
		"current_job_id": currentJobID,
		"replay_count":  len(req.CompletedJobs),
	})

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for _, completed := range req.CompletedJobs {
		jobID := strings.TrimSpace(completed.JobID)
		assignmentID := strings.TrimSpace(completed.AssignmentID)
		replay := hdcf.ReconnectAction{
			JobID:        jobID,
			AssignmentID: assignmentID,
		}
		if jobID == "" {
			replay.Result = hdcf.ReconnectResultRejected
			replay.Error = "job_id required"
			actions = append(actions, replay)
			continue
		}

		switch strings.ToUpper(strings.TrimSpace(completed.Status)) {
		case hdcf.StatusCompleted:
			replay.Action = hdcf.ReconnectActionReplayCompleted
			err := s.CompleteJob(ctx, hdcf.CompleteRequest{
				JobID:        jobID,
				WorkerID:     req.WorkerID,
				AssignmentID: assignmentID,
				ExitCode:     completed.ExitCode,
				ArtifactID:   completed.ArtifactID,
				StdoutPath:   completed.StdoutPath,
				StderrPath:   completed.StderrPath,
				StdoutTmpPath: completed.StdoutTmpPath,
				StderrTmpPath: completed.StderrTmpPath,
				StdoutSHA256: completed.StdoutSHA256,
				StderrSHA256: completed.StderrSHA256,
				ResultSummary: completed.ResultSummary,
			})
			if err != nil {
				replay.Result = hdcf.ReconnectResultRejected
				replay.Error = err.Error()
			} else {
				replay.Result = hdcf.ReconnectResultAccepted
			}
		case hdcf.StatusFailed:
			replay.Action = hdcf.ReconnectActionReplayFailed
			err := s.FailJob(ctx, hdcf.FailRequest{
				JobID:       jobID,
				WorkerID:    req.WorkerID,
				AssignmentID: assignmentID,
				ExitCode:    completed.ExitCode,
				Error:       completed.Error,
			})
			if err != nil {
				replay.Result = hdcf.ReconnectResultRejected
				replay.Error = err.Error()
			} else {
				replay.Result = hdcf.ReconnectResultAccepted
			}
		default:
			replay.Action = hdcf.ReconnectActionReplayFailed
			replay.Result = hdcf.ReconnectResultRejected
			replay.Error = "unsupported completion status"
		}
		actions = append(actions, replay)
	}

	return actions, nil
}

func (s *Store) RecordHeartbeat(ctx context.Context, req hdcf.HeartbeatRequest) error {
	now := time.Now().Unix()
	workerID := strings.TrimSpace(req.WorkerID)
	if workerID == "" {
		return fmt.Errorf("worker_id required")
	}
	var lastSeq sql.NullInt64
	var status sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT heartbeat_seq, status FROM workers WHERE worker_id = ?`, workerID).Scan(&lastSeq, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("worker not registered")
		}
		return err
	}
	prevStatus := strings.TrimSpace(status.String)
	if req.Sequence > 0 && lastSeq.Valid && req.Sequence <= lastSeq.Int64 {
		_ = s.logEvent(ctx, nil, "debug", "worker.heartbeat_stale", workerID, "", map[string]any{
			"seq": req.Sequence,
		})
		return nil
	}

	metricsJSON, hasMetrics, metricsErr := s.heartbeatMetricsFromRequest(req)
	if metricsErr != nil {
		return metricsErr
	}

	var currentJob interface{}
	if req.CurrentJobID == nil || strings.TrimSpace(*req.CurrentJobID) == "" {
		currentJob = nil
	} else {
		currentJob = *req.CurrentJobID
	}

	var res sql.Result
	var err error
	if req.Sequence > 0 {
		if hasMetrics {
			res, err = s.db.ExecContext(
				ctx,
				`UPDATE workers
				 SET last_seen = ?, current_job_id = ?, status = 'ONLINE', heartbeat_seq = ?, heartbeat_metrics = ?
				 WHERE worker_id = ? AND heartbeat_seq < ?`,
				now,
				currentJob,
				req.Sequence,
				metricsJSON,
				workerID,
				req.Sequence,
			)
		} else {
			res, err = s.db.ExecContext(
				ctx,
				`UPDATE workers
				 SET last_seen = ?, current_job_id = ?, status = 'ONLINE', heartbeat_seq = ?
				 WHERE worker_id = ? AND heartbeat_seq < ?`,
				now,
				currentJob,
				req.Sequence,
				workerID,
				req.Sequence,
			)
		}
	} else {
		if hasMetrics {
			res, err = s.db.ExecContext(
				ctx,
				`UPDATE workers
				 SET last_seen = ?, current_job_id = ?, status = 'ONLINE', heartbeat_metrics = ?
				 WHERE worker_id = ?`,
				now,
				currentJob,
				metricsJSON,
				workerID,
			)
		} else {
			res, err = s.db.ExecContext(
				ctx,
				`UPDATE workers
				 SET last_seen = ?, current_job_id = ?, status = 'ONLINE'
				 WHERE worker_id = ?`,
				now,
				currentJob,
				workerID,
			)
		}
	}
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		if req.Sequence > 0 {
			_ = s.logEvent(ctx, nil, "debug", "worker.heartbeat_noop", workerID, "", map[string]any{
				"seq": req.Sequence,
			})
			return nil
		}
		return fmt.Errorf("worker not registered")
	}
	if prevStatus != "ONLINE" {
		_ = s.logEvent(ctx, nil, "info", "worker.heartbeat", workerID, "", map[string]any{
			"from_status": prevStatus,
			"to_status":   "ONLINE",
			"seq":         req.Sequence,
		})
	}
	return nil
}

func (s *Store) CompleteJob(ctx context.Context, req hdcf.CompleteRequest) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status, workerID, assignmentID sql.NullString
	var completionSeq sql.NullInt64
	errScan := tx.QueryRowContext(ctx, `SELECT status, worker_id, assignment_id, completion_seq FROM jobs WHERE id = ?`, req.JobID).
		Scan(&status, &workerID, &assignmentID, &completionSeq)
	if errScan != nil {
		if errors.Is(errScan, sql.ErrNoRows) {
			return fmt.Errorf("job not found")
		}
		return errScan
	}
	currentStatus := strings.TrimSpace(status.String)
	if currentStatus == hdcf.StatusCompleted {
		_ = s.logEvent(ctx, tx, "info", "job.complete", req.WorkerID, req.JobID, map[string]any{
			"from_status": currentStatus,
			"to_status":   currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "noop_terminal",
		})
		return tx.Commit()
	}
	if currentStatus == hdcf.StatusFailed {
		_ = s.logEvent(ctx, tx, "info", "job.complete", req.WorkerID, req.JobID, map[string]any{
			"from_status": currentStatus,
			"to_status":   currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "noop_terminal",
		})
		return tx.Commit()
	}
	if req.CompletionSeq > 0 && completionSeq.Valid && req.CompletionSeq <= completionSeq.Int64 {
		_ = s.logEvent(ctx, tx, "info", "job.complete", req.WorkerID, req.JobID, map[string]any{
			"from_status":       currentStatus,
			"to_status":         currentStatus,
			"assignment_id":     req.AssignmentID,
			"completion_seq":    req.CompletionSeq,
			"last_completion_seq": completionSeq.Int64,
			"state":             "stale_completion",
		})
		return tx.Commit()
	}
	if currentStatus != hdcf.StatusRunning {
		return fmt.Errorf("job state not completable: %s", currentStatus)
	}
	if !hdcf.IsValidTransition(currentStatus, hdcf.StatusCompleted) {
		return fmt.Errorf("invalid transition %s -> %s", currentStatus, hdcf.StatusCompleted)
	}
	if strings.TrimSpace(req.ArtifactID) == "" {
		return fmt.Errorf("artifact_id required")
	}
	if strings.TrimSpace(req.StdoutPath) == "" {
		return fmt.Errorf("stdout_path required")
	}
	if strings.TrimSpace(req.StderrPath) == "" {
		return fmt.Errorf("stderr_path required")
	}
	if strings.TrimSpace(req.StdoutTmpPath) == "" {
		return fmt.Errorf("stdout_tmp_path required")
	}
	if strings.TrimSpace(req.StderrTmpPath) == "" {
		return fmt.Errorf("stderr_tmp_path required")
	}
	if err := validateCompletedArtifact(req.StdoutPath, req.StdoutTmpPath, "stdout"); err != nil {
		return err
	}
	if err := validateCompletedArtifact(req.StderrPath, req.StderrTmpPath, "stderr"); err != nil {
		return err
	}
	if workerID.Valid && workerID.String != req.WorkerID {
		return fmt.Errorf("job worker mismatch for completion")
	}
	if strings.TrimSpace(req.AssignmentID) == "" {
		return fmt.Errorf("assignment_id required")
	}
	if !assignmentID.Valid || assignmentID.String == "" {
		return fmt.Errorf("assignment not found")
	}
	if strings.TrimSpace(req.AssignmentID) != assignmentID.String {
		return fmt.Errorf("assignment_id mismatch")
	}

	if req.CompletionSeq > 0 {
		_, err = tx.ExecContext(
			ctx,
			`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL,
			 result_path = ?, artifact_id = ?, artifact_stdout_tmp_path = ?, artifact_stdout_path = ?, artifact_stdout_sha256 = ?, artifact_stderr_tmp_path = ?, artifact_stderr_path = ?, artifact_stderr_sha256 = ?, completion_seq = ?, last_error = ?, updated_at = ?, updated_by = ? WHERE id = ?`,
			hdcf.StatusCompleted,
			req.StdoutPath,
			req.ArtifactID,
			req.StdoutTmpPath,
			req.StdoutPath,
			req.StdoutSHA256,
			req.StderrTmpPath,
			req.StderrPath,
			req.StderrSHA256,
			req.CompletionSeq,
			req.ResultSummary,
			time.Now().Unix(),
			req.WorkerID,
			req.JobID,
		)
	} else {
		_, err = tx.ExecContext(
			ctx,
			`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL,
			 result_path = ?, artifact_id = ?, artifact_stdout_tmp_path = ?, artifact_stdout_path = ?, artifact_stdout_sha256 = ?, artifact_stderr_tmp_path = ?, artifact_stderr_path = ?, artifact_stderr_sha256 = ?, last_error = ?, updated_at = ?, updated_by = ? WHERE id = ?`,
			hdcf.StatusCompleted,
			req.StdoutPath,
			req.ArtifactID,
			req.StdoutTmpPath,
			req.StdoutPath,
			req.StdoutSHA256,
			req.StderrTmpPath,
			req.StderrPath,
			req.StderrSHA256,
			req.ResultSummary,
			time.Now().Unix(),
			req.WorkerID,
			req.JobID,
		)
	}
	if err != nil {
		return err
	}
	_ = s.logEvent(ctx, tx, "info", "job.complete", req.WorkerID, req.JobID, map[string]any{
		"from_status":         currentStatus,
		"to_status":           hdcf.StatusCompleted,
		"assignment_id":       req.AssignmentID,
		"artifact_id":         req.ArtifactID,
		"completion_seq":      req.CompletionSeq,
		"stdout":              req.StdoutPath,
		"stderr":              req.StderrPath,
		"stdout_sha256":       req.StdoutSHA256,
		"stderr_sha256":       req.StderrSHA256,
	})
	return tx.Commit()
}

func validateCompletedArtifact(finalPath, tmpPath, label string) error {
	if strings.TrimSpace(finalPath) == "" {
		return fmt.Errorf("%s_path required", label)
	}
	if strings.TrimSpace(tmpPath) == "" {
		return fmt.Errorf("%s_tmp_path required", label)
	}
	if strings.TrimSpace(finalPath) == strings.TrimSpace(tmpPath) {
		return fmt.Errorf("%s tmp path must differ from final path", label)
	}
	if strings.HasSuffix(finalPath, ".tmp") {
		return fmt.Errorf("%s final path should not be a temp path: %s", label, finalPath)
	}
	if !strings.HasSuffix(tmpPath, ".tmp") {
		return fmt.Errorf("%s temp path should be a temp file path: %s", label, tmpPath)
	}
	return nil
}

func (s *Store) FailJob(ctx context.Context, req hdcf.FailRequest) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status, workerID, assignmentID sql.NullString
	var attemptCount, maxAttempts int
	if err := tx.QueryRowContext(ctx, `SELECT status, worker_id, assignment_id, attempt_count, max_attempts FROM jobs WHERE id = ?`, req.JobID).
		Scan(&status, &workerID, &assignmentID, &attemptCount, &maxAttempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("job not found")
		}
		return err
	}
	currentStatus := strings.TrimSpace(status.String)
	if currentStatus == hdcf.StatusCompleted {
		_ = s.logEvent(ctx, tx, "info", "job.fail", req.WorkerID, req.JobID, map[string]any{
			"from_status": currentStatus,
			"to_status":   currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "noop_terminal",
		})
		return tx.Commit()
	}
	if currentStatus == hdcf.StatusFailed {
		_ = s.logEvent(ctx, tx, "info", "job.fail", req.WorkerID, req.JobID, map[string]any{
			"from_status": currentStatus,
			"to_status":   currentStatus,
			"assignment_id": req.AssignmentID,
			"state":        "noop_terminal",
		})
		return tx.Commit()
	}
	if currentStatus != hdcf.StatusRunning {
		return fmt.Errorf("job state not fail-safe: %s", currentStatus)
	}
	if !hdcf.IsValidTransition(currentStatus, hdcf.StatusFailed) {
		return fmt.Errorf("invalid transition %s -> %s", currentStatus, hdcf.StatusFailed)
	}
	if strings.TrimSpace(req.WorkerID) != "" && workerID.Valid && workerID.String != req.WorkerID {
		return fmt.Errorf("job worker mismatch for failure")
	}
	if strings.TrimSpace(req.AssignmentID) == "" {
		return fmt.Errorf("assignment_id required")
	}
	if !assignmentID.Valid || assignmentID.String == "" {
		return fmt.Errorf("assignment not found")
	}
	if strings.TrimSpace(req.AssignmentID) != assignmentID.String {
		return fmt.Errorf("assignment_id mismatch")
	}

	nextStatus := hdcf.StatusFailed
	if attemptCount < maxAttempts {
		nextStatus = hdcf.StatusPending
	}

	_, err = tx.ExecContext(
		ctx,
		`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL, last_error = ?, updated_at = ?, updated_by = ? WHERE id = ?`,
		nextStatus,
		req.Error,
		time.Now().Unix(),
		req.WorkerID,
		req.JobID,
	)
	if err != nil {
		return err
	}
	_ = s.logEvent(ctx, tx, "info", "job.fail", req.WorkerID, req.JobID, map[string]any{
		"from_status":   currentStatus,
		"to_status":     nextStatus,
		"assignment_id": req.AssignmentID,
		"exit_code":     req.ExitCode,
		"error":         req.Error,
	})
	return tx.Commit()
}

func (s *Store) AbortJobs(ctx context.Context, req hdcf.AbortRequest) (int64, error) {
	jobID := strings.TrimSpace(req.JobID)
	workerID := strings.TrimSpace(req.WorkerID)
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "aborted"
	}
	if jobID == "" && workerID == "" {
		return 0, ErrAbortNoTarget
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	if jobID != "" {
		var status, owner sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT status, worker_id FROM jobs WHERE id = ?`, jobID).Scan(&status, &owner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, fmt.Errorf("job not found")
			}
			return 0, err
		}
		ownerID := strings.TrimSpace(owner.String)
		if workerID != "" && ownerID != "" && ownerID != workerID {
			return 0, ErrAbortWorkerMismatch
		}
		if !status.Valid {
			return 0, fmt.Errorf("job status missing")
		}
		if status.String == hdcf.StatusCompleted {
			_ = s.logEvent(ctx, tx, "warn", "job.abort", "", jobID, map[string]any{
				"from_status": status.String,
				"to_status":   status.String,
				"state":       "reject_completed",
				"reason":      reason,
			})
			return 0, ErrAbortCompleted
		}
		if status.String == hdcf.StatusAborted {
			_ = s.logEvent(ctx, tx, "info", "job.abort", "", jobID, map[string]any{
				"from_status": status.String,
				"to_status":   status.String,
				"state":       "noop",
			})
			return 1, tx.Commit()
		}
		if !hdcf.IsValidTransition(status.String, hdcf.StatusAborted) {
			return 0, fmt.Errorf("invalid transition %s -> %s", status.String, hdcf.StatusAborted)
		}
		res, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL,
			     last_error = ?, updated_by = 'abort', updated_at = ?
			 WHERE id = ? AND status = ?`,
			hdcf.StatusAborted,
			reason,
			now,
			jobID,
			status.String,
		)
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if affected == 0 {
			return 0, fmt.Errorf("job state changed during abort request")
		}
		if ownerID != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ? AND current_job_id = ?`, ownerID, jobID); err != nil {
				return 0, err
			}
		}
		_ = s.logEvent(ctx, tx, "info", "job.abort", "", jobID, map[string]any{
			"from_status": status.String,
			"to_status":   hdcf.StatusAborted,
			"reason":      reason,
		})
		return affected, tx.Commit()
	}

	res, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL,
		     last_error = ?, updated_by = 'abort', updated_at = ?
		 WHERE worker_id = ? AND status NOT IN (?, ?)`,
		hdcf.StatusAborted,
		reason,
		now,
		workerID,
		hdcf.StatusCompleted,
		hdcf.StatusAborted,
	)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		return 0, ErrAbortNoTarget
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, workerID); err != nil {
		return 0, err
	}
	_ = s.logEvent(ctx, tx, "info", "job.abort", "", "", map[string]any{
		"worker_id":   workerID,
		"to_status":   hdcf.StatusAborted,
		"count":       affected,
		"reason":      reason,
	})

	return affected, tx.Commit()
}

func (s *Store) RecoverStaleWorkers(ctx context.Context) error {
	now := time.Now().Unix()
	cutoff := now - int64(s.heartbeatTimeout.Seconds())
	lostRetryCutoff := now - int64(2*s.heartbeatTimeout.Seconds())
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(
		ctx,
		`UPDATE workers SET status = 'OFFLINE' WHERE status = 'ONLINE' AND last_seen < ?`,
		cutoff,
	); err != nil {
		return err
	}
	offlineWorkers, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if offlineWorkers > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.offline_workers", "", "", map[string]any{
			"count": offlineWorkers,
			"cutoff": cutoff,
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'LOST',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'RUNNING'
		   AND (
		     worker_id IS NULL
		     OR worker_id IN (
		     SELECT worker_id FROM workers WHERE status = 'OFFLINE'
		     )
		   )`,
		now,
	); err != nil {
		return err
	}
	lostFromRunningOffline, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if lostFromRunningOffline > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.running_to_lost", "", "", map[string]any{
			"count": lostFromRunningOffline,
			"cutoff": cutoff,
			"from": hdcf.StatusRunning,
			"to": hdcf.StatusLost,
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'LOST',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'RUNNING'
		   AND worker_id NOT IN (SELECT worker_id FROM workers)`,
		now,
	); err != nil {
		return err
	}
	lostFromRunningMissing, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if lostFromRunningMissing > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.running_to_lost", "", "", map[string]any{
			"count": lostFromRunningMissing,
			"cutoff": cutoff,
			"from": hdcf.StatusRunning,
			"to": hdcf.StatusLost,
			"reason": "missing_worker_record",
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'ASSIGNED'
		   AND (
		     worker_id IS NULL
		     OR worker_id NOT IN (SELECT worker_id FROM workers)
		   )`,
		now,
	); err != nil {
		return err
	}
	pendingFromAssignedMissing, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if pendingFromAssignedMissing > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.assigned_to_pending", "", "", map[string]any{
			"count": pendingFromAssignedMissing,
			"from": hdcf.StatusAssigned,
			"to": hdcf.StatusPending,
			"reason": "missing_worker_record",
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'RETRYING',
		     updated_at = ?,
		     updated_by = 'reconciler'
		 WHERE status = 'LOST'
		   AND updated_at < ?`,
		now,
		lostRetryCutoff,
	); err != nil {
		return err
	}
	lostToRetrying, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if lostToRetrying > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.lost_to_retrying", "", "", map[string]any{
			"count": lostToRetrying,
			"from": hdcf.StatusLost,
			"to": hdcf.StatusRetrying,
			"retry_cutoff": lostRetryCutoff,
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'ASSIGNED'
		   AND assignment_expires_at IS NOT NULL
		   AND assignment_expires_at < ?`,
		now,
		cutoff,
	); err != nil {
		return err
	}
	assignedExpiredToPending, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if assignedExpiredToPending > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.assigned_to_pending", "", "", map[string]any{
			"count": assignedExpiredToPending,
			"from": hdcf.StatusAssigned,
			"to": hdcf.StatusPending,
			"reason": "assignment_expired",
			"cutoff": cutoff,
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'RETRYING'
		   AND attempt_count < max_attempts
		   AND updated_at < ?`,
		now,
		lostRetryCutoff,
	); err != nil {
		return err
	}
	retryingToPending, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if retryingToPending > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.retrying_to_pending", "", "", map[string]any{
			"count": retryingToPending,
			"from": hdcf.StatusRetrying,
			"to": hdcf.StatusPending,
			"retry_cutoff": lostRetryCutoff,
		})
	}

	res, err = tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'FAILED',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?,
		     last_error = 'worker offline'
		 WHERE status = 'RETRYING'
		   AND attempt_count >= max_attempts
		   AND updated_at < ?`,
		now,
		lostRetryCutoff,
	); err != nil {
		return err
	}
	retryingToFailed, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if retryingToFailed > 0 {
		_ = s.logEvent(ctx, tx, "warn", "reconciler.retrying_to_failed", "", "", map[string]any{
			"count": retryingToFailed,
			"from": hdcf.StatusRetrying,
			"to": hdcf.StatusFailed,
			"reason": "attempts_exhausted",
			"retry_cutoff": lostRetryCutoff,
		})
	}

	return tx.Commit()
}

func (s *Store) ListTerminalArtifactPaths(ctx context.Context, retentionDays int) ([]string, error) {
	if retentionDays <= 0 {
		return nil, nil
	}
	cutoff := time.Now().Unix() - int64(retentionDays)*86400
	query := `
		SELECT artifact_stdout_path, artifact_stdout_tmp_path, artifact_stderr_path, artifact_stderr_tmp_path
		FROM jobs
		WHERE status IN ('COMPLETED', 'FAILED', 'ABORTED')
		  AND updated_at < ?
	`
	return s.collectTerminalArtifactPaths(ctx, nil, query, cutoff)
}

func (s *Store) PruneTerminalJobs(ctx context.Context, retentionDays int) ([]string, int64, error) {
	if retentionDays <= 0 {
		return nil, 0, nil
	}
	cutoff := time.Now().Unix() - int64(retentionDays)*86400

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	query := `
		SELECT artifact_stdout_path, artifact_stdout_tmp_path, artifact_stderr_path, artifact_stderr_tmp_path
		FROM jobs
		WHERE status IN ('COMPLETED', 'FAILED', 'ABORTED')
		  AND updated_at < ?
	`
	paths, err := s.collectTerminalArtifactPaths(ctx, tx, query, cutoff)
	if err != nil {
		return nil, 0, err
	}

	res, err := tx.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE status IN ('COMPLETED', 'FAILED', 'ABORTED')
		  AND updated_at < ?
	`, cutoff)
	if err != nil {
		return nil, 0, err
	}
	deletedJobs, err := res.RowsAffected()
	if err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return paths, deletedJobs, nil
}

func (s *Store) CleanupOldEvents(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Unix() - int64(retentionDays)*86400
	res, err := s.db.ExecContext(ctx, `DELETE FROM audit_events WHERE ts < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	deletedEvents, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return deletedEvents, nil
}

func (s *Store) collectTerminalArtifactPaths(ctx context.Context, tx *sql.Tx, query string, cutoff int64) ([]string, error) {
	executeQuery := func(q string, args ...any) (*sql.Rows, error) {
		if tx != nil {
			return tx.QueryContext(ctx, q, args...)
		}
		return s.db.QueryContext(ctx, q, args...)
	}

	rows, err := executeQuery(query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pathSet := map[string]struct{}{}
	for rows.Next() {
		var stdoutPath, stdoutTmpPath, stderrPath, stderrTmpPath string
		if err := rows.Scan(&stdoutPath, &stdoutTmpPath, &stderrPath, &stderrTmpPath); err != nil {
			return nil, err
		}
		for _, p := range []string{stdoutPath, stdoutTmpPath, stderrPath, stderrTmpPath} {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			pathSet[p] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(pathSet))
	for p := range pathSet {
		paths = append(paths, p)
	}
	return paths, nil
}
