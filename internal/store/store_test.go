package store

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"hdcf/internal/hdcf"
)

func TestCompleteJobIsIdempotentAndMonotonic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)

	jobID, assignmentID, workerID := enqueueAndClaimJob(t, s, ctx)
	ackJob(t, s, ctx, jobID, assignmentID, workerID)

	completeReq := hdcf.CompleteRequest{
		JobID:         jobID,
		WorkerID:      workerID,
		AssignmentID:   assignmentID,
		ArtifactID:     "artifact-1",
		ExitCode:       0,
		StdoutPath:     "/tmp/hdcf-stdout.txt",
		StderrPath:     "/tmp/hdcf-stderr.txt",
		StdoutTmpPath:  "/tmp/hdcf-stdout.tmp",
		StderrTmpPath:  "/tmp/hdcf-stderr.tmp",
		ArtifactBackend: hdcf.ArtifactStorageBackendLocal,
		ArtifactUploadState: hdcf.ArtifactUploadStateOK,
		CompletionSeq:   7,
		ResultSummary:   "first completion",
	}
	if err := s.CompleteJob(ctx, completeReq); err != nil {
		t.Fatalf("first completion should succeed: %v", err)
	}

	first := mustGetJob(t, s, ctx, jobID)
	if got := first.Status; got != hdcf.StatusCompleted {
		t.Fatalf("first completion should complete job, got status %s", got)
	}
	if first.AttemptCount != 1 {
		t.Fatalf("expected attempt_count 1 after first completion, got %d", first.AttemptCount)
	}
	if got, want := mustGetCompletionSeq(t, s, ctx, jobID), int64(7); got != want {
		t.Fatalf("expected completion_seq %d, got %d", want, got)
	}

	// Duplicate completion should succeed without changing terminal state.
	completeReq.ResultSummary = "duplicate completion"
	if err := s.CompleteJob(ctx, completeReq); err != nil {
		t.Fatalf("duplicate completion should succeed: %v", err)
	}
	if err := s.CompleteJob(ctx, hdcf.CompleteRequest{
		JobID:         jobID,
		WorkerID:      workerID,
		AssignmentID:   assignmentID,
		ArtifactID:     "artifact-2",
		ExitCode:       0,
		StdoutPath:     "/tmp/retry-stdout.txt",
		StderrPath:     "/tmp/retry-stderr.txt",
		StdoutTmpPath:  "/tmp/retry-stdout.tmp",
		StderrTmpPath:  "/tmp/retry-stderr.tmp",
		ArtifactBackend: hdcf.ArtifactStorageBackendLocal,
		ArtifactUploadState: hdcf.ArtifactUploadStateOK,
		CompletionSeq:   2,
		ResultSummary:   "stale completion",
	}); err != nil {
		t.Fatalf("stale completion should not fail: %v", err)
	}

	final := mustGetJob(t, s, ctx, jobID)
	if final.Status != hdcf.StatusCompleted {
		t.Fatalf("job should remain completed, got %s", final.Status)
	}
	if final.ResultPath != "/tmp/hdcf-stdout.txt" {
		t.Fatalf("stale completion should preserve terminal artifact path, got %s", final.ResultPath)
	}
	if got, want := mustGetCompletionSeq(t, s, ctx, jobID), int64(7); got != want {
		t.Fatalf("completion_seq should remain monotonic, expected %d got %d", want, got)
	}
}

func TestFailAfterCompletionIsNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)

	jobID, assignmentID, workerID := enqueueAndClaimJob(t, s, ctx)
	ackJob(t, s, ctx, jobID, assignmentID, workerID)

	completeReq := hdcf.CompleteRequest{
		JobID:         jobID,
		WorkerID:      workerID,
		AssignmentID:   assignmentID,
		ArtifactID:     "artifact-3",
		ExitCode:       0,
		StdoutPath:     "/tmp/cli-stdout.txt",
		StderrPath:     "/tmp/cli-stderr.txt",
		StdoutTmpPath:  "/tmp/cli-stdout.tmp",
		StderrTmpPath:  "/tmp/cli-stderr.tmp",
		ArtifactBackend: hdcf.ArtifactStorageBackendLocal,
		ArtifactUploadState: hdcf.ArtifactUploadStateOK,
		ResultSummary:   "complete then noop fail",
	}
	if err := s.CompleteJob(ctx, completeReq); err != nil {
		t.Fatalf("initial completion should succeed: %v", err)
	}

	if err := s.FailJob(ctx, hdcf.FailRequest{
		JobID:       jobID,
		WorkerID:    workerID,
		AssignmentID: assignmentID,
		ExitCode:    1,
		Error:       "late failure should noop",
	}); err != nil {
		t.Fatalf("fail after completion should be idempotent: %v", err)
	}

	job, ok, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !ok {
		t.Fatal("job should exist")
	}
	if job.Status != hdcf.StatusCompleted {
		t.Fatalf("job should remain completed, got %s", job.Status)
	}
}

func TestClaimNextJobConcurrentWorkersAreMutualExclusive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)

	const (
		jobCount    = 40
		workerCount = 8
	)

	for i := 0; i < jobCount; i++ {
		if _, err := s.CreateJob(ctx, hdcf.CreateJobRequest{
			Command:     "sleep",
			Args:        []string{"1"},
			MaxAttempts: 1,
		}); err != nil {
			t.Fatalf("create job: %v", err)
		}
	}
	workers := make([]string, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		workerID := "worker-" + strconv.Itoa(i)
		if err := s.RegisterWorker(ctx, hdcf.RegisterWorkerRequest{WorkerID: workerID}); err != nil {
			t.Fatalf("register worker %s: %v", workerID, err)
		}
		workers = append(workers, workerID)
	}

	assignmentByJob := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr atomicError

	for _, workerID := range workers {
		workerID := workerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				assigned, ok, err := s.ClaimNextJob(ctx, workerID)
				if err != nil {
					firstErr.set(err)
					return
				}
				if !ok || assigned == nil {
					return
				}
				mu.Lock()
				if previous, exists := assignmentByJob[assigned.JobID]; exists {
					firstErr.setf("job %s assigned to multiple workers: %s and %s", assigned.JobID, previous, workerID)
					mu.Unlock()
					return
				}
				assignmentByJob[assigned.JobID] = workerID
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if err := firstErr.get(); err != nil {
		t.Fatalf("claim loop error: %v", err)
	}
	if len(assignmentByJob) != jobCount {
		t.Fatalf("expected to claim all jobs, got %d of %d", len(assignmentByJob), jobCount)
	}
}

func enqueueAndClaimJob(t *testing.T, s *Store, ctx context.Context) (string, string, string) {
	t.Helper()
	resp, err := s.CreateJob(ctx, hdcf.CreateJobRequest{
		Command:     "sleep",
		Args:        []string{"1"},
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	workerID := "test-worker"
	if err := s.RegisterWorker(ctx, hdcf.RegisterWorkerRequest{WorkerID: workerID}); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	assigned, ok, err := s.ClaimNextJob(ctx, workerID)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if !ok || assigned == nil {
		t.Fatal("no job assigned")
	}
	return resp.JobID, assigned.AssignmentID, workerID
}

type atomicError struct {
	err error
	mu  sync.Mutex
}

func (e *atomicError) set(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err == nil {
		e.err = err
	}
}

func (e *atomicError) setf(format string, args ...interface{}) {
	e.set(fmt.Errorf(format, args...))
}

func (e *atomicError) get() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func ackJob(t *testing.T, s *Store, ctx context.Context, jobID, assignmentID, workerID string) {
	t.Helper()
	if err := s.AcknowledgeJob(ctx, hdcf.AckJobRequest{
		JobID:        jobID,
		WorkerID:     workerID,
		AssignmentID: assignmentID,
	}); err != nil {
		t.Fatalf("ack job: %v", err)
	}
}

func mustGetJob(t *testing.T, s *Store, ctx context.Context, jobID string) hdcf.JobRead {
	t.Helper()
	job, ok, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if !ok {
		t.Fatal("job not found")
	}
	return job
}

func mustGetCompletionSeq(t *testing.T, s *Store, ctx context.Context, jobID string) int64 {
	t.Helper()
	var seq int64
	if err := s.db.QueryRowContext(ctx, `SELECT completion_seq FROM jobs WHERE id = ?`, jobID).Scan(&seq); err != nil {
		t.Fatalf("read completion_seq: %v", err)
	}
	return seq
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:", time.Second, StoreOptions{})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}
