package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestMigrationDiagnosticsHealthyForFreshDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)

	diag, err := s.MigrationDiagnostics(ctx)
	if err != nil {
		t.Fatalf("migration diagnostics failed: %v", err)
	}
	if !diag.Healthy {
		t.Fatalf("expected healthy diagnostics, got %#v", diag)
	}
	for _, table := range []string{"jobs", "workers", "audit_events"} {
		if _, ok := diag.TableCounts[table]; !ok {
			t.Fatalf("expected table_counts to include %s", table)
		}
	}
}

func TestMigrationDiagnosticsDetectsMissingTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "missing_table.sqlite")
	s, err := Open(storePath, time.Second, StoreOptions{})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})

	if _, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS audit_events`); err != nil {
		t.Fatalf("drop audit_events: %v", err)
	}

	diag, err := s.MigrationDiagnostics(ctx)
	if err != nil {
		t.Fatalf("diagnostics should not fail on missing table: %v", err)
	}
	if diag.Healthy {
		t.Fatal("expected diagnostics unhealthy when table is missing")
	}
	if len(diag.MissingTables) != 1 {
		t.Fatalf("expected exactly one missing table, got %d", len(diag.MissingTables))
	}
	if diag.MissingTables[0] != "audit_events" {
		t.Fatalf("expected missing table audit_events, got %q", diag.MissingTables[0])
	}
	if _, ok := diag.TableCounts["jobs"]; !ok {
		t.Fatal("expected table_counts to include existing table jobs")
	}
}

func TestMigrationDiagnosticsDetectsMissingColumnsAndIndexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "partial.sqlite")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite file: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		t.Fatalf("set wal: %v", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		t.Fatalf("set busy timeout: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE jobs (
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
		artifact_storage_backend TEXT NOT NULL DEFAULT 'local',
		artifact_storage_location TEXT,
		artifact_upload_state TEXT,
		updated_by TEXT
	);
	CREATE TABLE workers (
		worker_id TEXT PRIMARY KEY,
		last_seen INTEGER NOT NULL,
		current_job_id TEXT,
		status TEXT NOT NULL,
		worker_capabilities TEXT NOT NULL DEFAULT '',
		registered_at INTEGER,
		registration_nonce TEXT,
		heartbeat_seq INTEGER NOT NULL DEFAULT 0,
		heartbeat_metrics TEXT
	);
	CREATE TABLE audit_events (
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
	CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs(status, created_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_queue_order ON jobs(status, priority DESC, scheduled_at ASC, created_at ASC);
	CREATE INDEX IF NOT EXISTS idx_audit_events_ts ON audit_events(ts);
	CREATE INDEX IF NOT EXISTS idx_audit_events_component_event ON audit_events(component, event);
	CREATE INDEX IF NOT EXISTS idx_audit_events_component ON audit_events(component);
	CREATE INDEX IF NOT EXISTS idx_audit_events_event ON audit_events(event);
	CREATE INDEX IF NOT EXISTS idx_audit_events_ts_id ON audit_events(ts, id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_job ON audit_events(job_id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_worker ON audit_events(worker_id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_component_event_ts_id ON audit_events(component, event, ts, id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_event_ts_id ON audit_events(event, ts, id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_worker_ts_id ON audit_events(worker_id, ts, id);
	CREATE INDEX IF NOT EXISTS idx_audit_events_job_ts_id ON audit_events(job_id, ts, id);
	`); err != nil {
		t.Fatalf("create partial schema: %v", err)
	}

	s := &Store{
		db: db,
	}

	diag, err := s.MigrationDiagnostics(ctx)
	if err != nil {
		t.Fatalf("migration diagnostics failed: %v", err)
	}
	if diag.Healthy {
		t.Fatal("expected diagnostics unhealthy for partial schema")
	}

	missingJobs := diag.MissingColumns["jobs"]
	sort.Strings(missingJobs)
	if len(missingJobs) == 0 {
		t.Fatalf("expected missing jobs columns, got %#v", diag.MissingColumns)
	}
	if missingJobs[0] != "artifact_upload_error" {
		t.Fatalf("expected missing artifact_upload_error, got %#v", missingJobs)
	}

	if idxs, ok := diag.MissingIndexes["jobs"]; ok {
		if len(idxs) == 0 {
			t.Fatal("expected missing job indexes for partial schema")
		}
	}
}
