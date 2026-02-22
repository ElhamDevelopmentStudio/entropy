package hdcf

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeArtifactStorageBackend(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw      string
		expected string
	}{
		{"", ArtifactStorageBackendLocal},
		{"local", ArtifactStorageBackendLocal},
		{"nfs-share", ArtifactStorageBackendNFS},
		{"nfs_share", ArtifactStorageBackendNFS},
		{"s3", ArtifactStorageBackendS3},
		{"s3-compatible", ArtifactStorageBackendS3},
		{"S3_Compatible", ArtifactStorageBackendS3},
		{"  NFS  ", ArtifactStorageBackendNFS},
		{"custom", "custom"},
	}
	for _, tc := range cases {
		got := NormalizeArtifactStorageBackend(tc.raw)
		if got != tc.expected {
			t.Fatalf("NormalizeArtifactStorageBackend(%q) = %q, want %q", tc.raw, got, tc.expected)
		}
	}
}

func TestArtifactStorageBackendValidation(t *testing.T) {
	t.Parallel()

	if !IsValidArtifactStorageBackend("nfs") {
		t.Fatalf("expected nfs to be valid")
	}
	if !IsValidArtifactStorageBackend(" s3_compatible ") {
		t.Fatalf("expected s3_compatible alias to be valid")
	}
	if IsValidArtifactStorageBackend("not-a-backend") {
		t.Fatalf("expected invalid backend to be rejected")
	}
}

func TestNormalizeArtifactUploadState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw      string
		expected string
	}{
		{"", ArtifactUploadStatePending},
		{ArtifactUploadStateOK, ArtifactUploadStateOK},
		{ArtifactUploadStateFailed, ArtifactUploadStateFailed},
		{ArtifactUploadStateNoop, ArtifactUploadStateNoop},
		{ArtifactUploadStateSkipped, ArtifactUploadStateSkipped},
		{"Unknown", "unknown"},
	}
	for _, tc := range cases {
		got := NormalizeArtifactUploadState(tc.raw)
		if got != tc.expected {
			t.Fatalf("NormalizeArtifactUploadState(%q) = %q, want %q", tc.raw, got, tc.expected)
		}
	}
}

func TestIsValidTransition(t *testing.T) {
	t.Parallel()
	if !IsValidTransition(StatusPending, StatusAssigned) {
		t.Fatal("expected PENDING->ASSIGNED to be valid")
	}
	if !IsValidTransition(StatusRunning, StatusCompleted) {
		t.Fatal("expected RUNNING->COMPLETED to be valid")
	}
	if IsValidTransition(StatusCompleted, StatusFailed) {
		t.Fatal("expected COMPLETED->FAILED to be invalid")
	}
	if IsValidTransition("UNKNOWN", StatusPending) {
		t.Fatal("expected unknown status transition to be invalid")
	}
}

func TestNewJobIDFormatAndUniqueness(t *testing.T) {
	t.Parallel()
	idOne := NewJobID()
	if !strings.Contains(idOne, "-") {
		t.Fatalf("expected generated id to include separators, got %q", idOne)
	}
	ids := map[string]struct{}{idOne: {}}
	for i := 0; i < 64; i++ {
		got := NewJobID()
		if _, exists := ids[got]; exists {
			t.Fatalf("generated duplicate job id %q", got)
		}
		if got == "" {
			t.Fatal("generated empty job id")
		}
		ids[got] = struct{}{}
	}
}

func TestNewJobIDMonotonicHint(t *testing.T) {
	t.Parallel()
	t0 := NewJobID()
	time.Sleep(time.Millisecond)
	t1 := NewJobID()
	if t0 == t1 {
		t.Fatal("expected two generated job IDs to differ")
	}
}
