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
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		attempt_count INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		worker_id TEXT,
		assignment_id TEXT,
		assignment_expires_at INTEGER,
		last_error TEXT,
		result_path TEXT,
		updated_by TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at);
	CREATE TABLE IF NOT EXISTS workers (
		worker_id TEXT PRIMARY KEY,
		last_seen INTEGER NOT NULL,
		current_job_id TEXT,
		status TEXT NOT NULL
	);
	`
	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return err
	}
	return s.ensureJobColumns(ctx)
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

func (s *Store) CreateJob(ctx context.Context, req hdcf.CreateJobRequest) (hdcf.CreateJobResponse, error) {
	if strings.TrimSpace(req.Command) == "" {
		return hdcf.CreateJobResponse{}, errors.New("command required")
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}

	argsJSON, err := json.Marshal(req.Args)
	if err != nil {
		return hdcf.CreateJobResponse{}, err
	}

	now := time.Now().Unix()
	id := hdcf.NewJobID()
	_, err = s.db.ExecContext(
		ctx,
		`INSERT INTO jobs (id, status, command, args, working_dir, timeout_ms, created_at, updated_at, attempt_count, max_attempts)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
		id,
		hdcf.StatusPending,
		req.Command,
		string(argsJSON),
		req.WorkingDir,
		req.TimeoutMs,
		now,
		now,
		req.MaxAttempts,
	)
	if err != nil {
		return hdcf.CreateJobResponse{}, err
	}

	return hdcf.CreateJobResponse{
		JobID:  id,
		Status: hdcf.StatusPending,
	}, nil
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
	assignmentID := hdcf.NewJobID()
	assignmentExpiresAt := now.Add(s.heartbeatTimeout).Unix()

	row := tx.QueryRowContext(
		ctx,
		`SELECT id, command, args, working_dir, timeout_ms, attempt_count, max_attempts
		FROM jobs
		WHERE status = ?
		  AND attempt_count < max_attempts
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
		ORDER BY created_at ASC
		LIMIT 1`,
		hdcf.StatusPending,
		cutoff,
	)

	var job hdcf.AssignedJob
	var argsJSON string
	if err := row.Scan(&job.JobID, &job.Command, &argsJSON, &job.WorkingDir, &job.TimeoutMs, &job.AttemptCount, &job.MaxAttempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := json.Unmarshal([]byte(argsJSON), &job.Args); err != nil {
		return nil, false, err
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
		return nil, false, nil
	}
	job.AttemptCount++
	job.AssignmentID = assignmentID
	job.AssignmentExpiresAt = assignmentExpiresAt
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	return &job, true, nil
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

	if status.String == hdcf.StatusCompleted {
		return tx.Commit()
	}
	if status.String == hdcf.StatusRunning {
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
		return tx.Commit()
	}
	if status.String != hdcf.StatusAssigned {
		return fmt.Errorf("job state not ackable: %s", status.String)
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
	return tx.Commit()
}

func (s *Store) ReconnectWorker(ctx context.Context, req hdcf.WorkerReconnectRequest) ([]hdcf.ReconnectAction, error) {
	if strings.TrimSpace(req.WorkerID) == "" {
		return nil, fmt.Errorf("worker_id required")
	}

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

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workers (worker_id, last_seen, current_job_id, status)
		 VALUES (?, ?, ?, 'ONLINE')
		 ON CONFLICT(worker_id) DO UPDATE SET
		   last_seen = excluded.last_seen,
		   current_job_id = excluded.current_job_id,
		   status = 'ONLINE'`,
		req.WorkerID,
		now,
		currentJob,
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
				if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_job_id = NULL WHERE worker_id = ?`, req.WorkerID); err != nil {
					return nil, err
				}
			}
		}
	}

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
				StdoutPath:   completed.StdoutPath,
				StderrPath:   completed.StderrPath,
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
	var currentJob interface{}
	if req.CurrentJobID == nil || strings.TrimSpace(*req.CurrentJobID) == "" {
		currentJob = nil
	} else {
		currentJob = *req.CurrentJobID
	}
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO workers (worker_id, last_seen, current_job_id, status)
		 VALUES (?, ?, ?, 'ONLINE')
		 ON CONFLICT(worker_id) DO UPDATE SET
		   last_seen = excluded.last_seen,
		   current_job_id = excluded.current_job_id,
		   status = 'ONLINE'`,
		req.WorkerID,
		now,
		currentJob,
	)
	return err
}

func (s *Store) CompleteJob(ctx context.Context, req hdcf.CompleteRequest) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status, workerID, assignmentID, lastError, resultPath sql.NullString
	errScan := tx.QueryRowContext(ctx, `SELECT status, worker_id, assignment_id, last_error, result_path FROM jobs WHERE id = ?`, req.JobID).
		Scan(&status, &workerID, &assignmentID, &lastError, &resultPath)
	if errScan != nil {
		if errors.Is(errScan, sql.ErrNoRows) {
			return fmt.Errorf("job not found")
		}
		return errScan
	}
	if status.String == hdcf.StatusCompleted {
		return tx.Commit()
	}
	if status.String == hdcf.StatusFailed {
		return tx.Commit()
	}
	if status.String != hdcf.StatusRunning {
		return fmt.Errorf("job state not completable: %s", status.String)
	}
	if !hdcf.IsValidTransition(status.String, hdcf.StatusCompleted) {
		return fmt.Errorf("invalid transition %s -> %s", status.String, hdcf.StatusCompleted)
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

	_, err = tx.ExecContext(
		ctx,
		`UPDATE jobs SET status = ?, worker_id = NULL, assignment_id = NULL, assignment_expires_at = NULL, result_path = ?, last_error = ?, updated_at = ?, updated_by = ? WHERE id = ?`,
		hdcf.StatusCompleted,
		req.StdoutPath,
		req.ResultSummary,
		time.Now().Unix(),
		req.WorkerID,
		req.JobID,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
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
	if status.String == hdcf.StatusCompleted {
		return tx.Commit()
	}
	if status.String == hdcf.StatusFailed {
		return tx.Commit()
	}
	if status.String != hdcf.StatusRunning {
		return fmt.Errorf("job state not fail-safe: %s", status.String)
	}
	if !hdcf.IsValidTransition(status.String, hdcf.StatusFailed) {
		return fmt.Errorf("invalid transition %s -> %s", status.String, hdcf.StatusFailed)
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
	return tx.Commit()
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

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE workers SET status = 'OFFLINE' WHERE status = 'ONLINE' AND last_seen < ?`,
		cutoff,
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET status = 'LOST',
		     worker_id = NULL,
		     assignment_id = NULL,
		     assignment_expires_at = NULL,
		     updated_by = 'reconciler',
		     updated_at = ?
		 WHERE status = 'RUNNING'
		   AND worker_id IN (
		     SELECT worker_id FROM workers WHERE status = 'OFFLINE'
		   )`,
		now,
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
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

	if _, err := tx.ExecContext(
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

	if _, err := tx.ExecContext(
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

	if _, err := tx.ExecContext(
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

	return tx.Commit()
}
