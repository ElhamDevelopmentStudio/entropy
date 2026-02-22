package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"hdcf/internal/hdcf"
)

func TestControlConfigPathFromArgs(t *testing.T) {
	t.Parallel()
	oldArgs := append([]string(nil), os.Args...)
	oldEnv := os.Getenv("HDCF_CONTROL_CONFIG")
	defer func() {
		os.Args = oldArgs
		if oldEnv == "" {
			_ = os.Unsetenv("HDCF_CONTROL_CONFIG")
		} else {
			_ = os.Setenv("HDCF_CONTROL_CONFIG", oldEnv)
		}
	}()

	_ = os.Setenv("HDCF_CONTROL_CONFIG", "/env/path.json")
	os.Args = []string{"cmd", "-config", "/flag/path.json"}
	if got := controlConfigPathFromArgs(); got != "/flag/path.json" {
		t.Fatalf("expected -config flag to win, got %q", got)
	}

	os.Args = []string{"cmd", "--config=/long/flag/path.json"}
	if got := controlConfigPathFromArgs(); got != "/long/flag/path.json" {
		t.Fatalf("expected --config to win, got %q", got)
	}

	os.Args = []string{"cmd"}
	if got := controlConfigPathFromArgs(); got != "/env/path.json" {
		t.Fatalf("expected env value, got %q", got)
	}
}

func TestControlResolveConfigHelpers(t *testing.T) {
	t.Parallel()
	raw := "from-file"
	if got := resolveConfigString(&raw, "HDCF_ADDR", "fallback"); got != "from-file" {
		t.Fatalf("expected file value, got %q", got)
	}

	_ = os.Setenv("HDCF_ADDR", "from-env")
	if got := resolveConfigString(nil, "HDCF_ADDR", "fallback"); got != "from-env" {
		t.Fatalf("expected env value, got %q", got)
	}

	if got := resolveConfigInt64(nil, "MISSING", 99); got != 99 {
		t.Fatalf("expected fallback, got %d", got)
	}
	override := int64(42)
	if got := resolveConfigInt64(&override, "MISSING", 99); got != 42 {
		t.Fatalf("expected raw value, got %d", got)
	}

	if got := resolveConfigBool(nil, "HDCF_BOOL", true); got != true {
		t.Fatalf("expected fallback when env unset, got %v", got)
	}
	_ = os.Setenv("HDCF_BOOL", "false")
	if got := resolveConfigBool(nil, "HDCF_BOOL", true); got != false {
		t.Fatalf("expected env bool value, got %v", got)
	}
}

func TestControlAuthHelpers(t *testing.T) {
	t.Parallel()
	cfg := &controlConfig{
		adminToken:      "admin",
		adminTokenPrev:  "admin-prev",
		workerToken:     "worker",
		workerTokenPrev: "worker-prev",
	}
	if err := cfg.authorizeAdminRequest("admin-prev"); err != nil {
		t.Fatalf("admin previous token should be accepted: %v", err)
	}
	if err := cfg.authorizeWorkerRequest("worker-prev", "worker-id"); err != nil {
		t.Fatalf("worker previous token should be accepted: %v", err)
	}
	if cfg.authorizeWorkerRequest("bad-token", "worker-id") == nil {
		t.Fatal("expected invalid worker token to be rejected")
	}
}

func TestControlWorkerSignedTokenValidation(t *testing.T) {
	t.Parallel()
	cfg := &controlConfig{
		workerTokenSecret: "unit-secret",
	}
	now := time.Now().Unix()
	rawPayload := fmt.Sprintf("%s|%d|%s", "worker-1", now+30, hdcf.NewJobID())
	encodedPayload := base64.RawURLEncoding.EncodeToString([]byte(rawPayload))
	validSig := generateWorkerSignedTokenSig(cfg.workerTokenSecret, rawPayload)
	validToken := "v1." + encodedPayload + "." + validSig
	if err := cfg.authorizeWorkerRequest(validToken, "worker-1"); err != nil {
		t.Fatalf("expected signed token to be accepted: %v", err)
	}

	if err := cfg.authorizeWorkerRequest("not-a-token", "worker-1"); err == nil {
		t.Fatal("expected invalid token string to fail")
	}

	expiredPayload := fmt.Sprintf("%s|%d|%s", "worker-1", now-10, hdcf.NewJobID())
	expiredToken := "v1." + base64.RawURLEncoding.EncodeToString([]byte(expiredPayload)) + "." + generateWorkerSignedTokenSig(cfg.workerTokenSecret, expiredPayload)
	if err := cfg.authorizeWorkerRequest(expiredToken, "worker-1"); err == nil {
		t.Fatal("expected expired token to fail")
	}
}

func TestParseWorkerSignedTokenValidation(t *testing.T) {
	t.Parallel()
	payload, sig, err := parseWorkerSignedToken("v1." + base64.RawURLEncoding.EncodeToString([]byte("w|123|n")) + ".sig")
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if payload != "w|123|n" {
		t.Fatalf("unexpected payload %q", payload)
	}
	if sig != "sig" {
		t.Fatalf("unexpected signature %q", sig)
	}
	if _, _, err := parseWorkerSignedToken("bad-format"); err == nil {
		t.Fatal("expected parse failure")
	}
}

func TestBuildControlTLSConfigValidation(t *testing.T) {
	t.Parallel()
	_, err := buildControlTLSConfig(controlConfig{
		tlsClientCA:         "",
		tlsRequireClientCert: true,
	})
	if err == nil {
		t.Fatal("expected error when tlsRequireClientCert without CA")
	}

	if _, err := buildControlTLSConfig(controlConfig{}); err != nil {
		t.Fatal("expected tls config without CA or client cert to succeed")
	}

	if _, err := buildControlTLSConfig(controlConfig{
		tlsClientCA: "does-not-exist.pem",
	}); err == nil {
		t.Fatal("expected error when tls CA file does not exist")
	}
}

func TestResolveTokenFallbacks(t *testing.T) {
	t.Parallel()
	cfg := &controlConfig{
		workerToken: "worker",
		adminToken:  "admin",
	}
	if got := cfg.authorizeWorkerRequest("", "w"); got == nil {
		t.Fatal("expected empty token to be rejected")
	}
	cfg.workerToken = ""
	if err := cfg.authorizeWorkerRequest("admin", "w"); err != nil {
		t.Fatalf("expected admin token fallback for worker auth: %v", err)
	}

	if err := cfg.authorizeAdminRequest("admin"); err != nil {
		t.Fatalf("expected admin token: %v", err)
	}
}

func TestTokenMatchingRequiresEqualLengths(t *testing.T) {
	t.Parallel()
	if !tokenMatches("abc", "abc") {
		t.Fatal("expected equal tokens to match")
	}
	if tokenMatches("abc", "abcd") {
		t.Fatal("expected token lengths to mismatch")
	}
}
