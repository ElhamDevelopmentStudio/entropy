package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hdcf/internal/hdcf"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	cfg := parseWorkerConfig()

	runner := &workerRunner{
		controlURL:              strings.TrimRight(cfg.controlURL, "/"),
		workerID:                cfg.workerID,
		nonce:                   cfg.nonce,
		capabilities:            cfg.capabilities,
		token:                   cfg.token,
		workerTokenSecret:       cfg.workerTokenSecret,
		workerTokenTTL:          cfg.workerTokenTTL,
		pollInterval:            cfg.pollInterval,
		heartbeatInterval:       cfg.heartbeatInterval,
		requestTimeout:          cfg.requestTimeout,
		logDir:                  cfg.logDir,
		stateFile:               cfg.stateFile,
		logRetentionDays:        cfg.logRetentionDays,
		logCleanupInterval:      cfg.logCleanupInterval,
		reportMetrics:           cfg.reportMetrics,
		commandAllowlistEnabled: cfg.commandAllowlistEnabled,
		commandAllowlist:        cfg.commandAllowlist,
		requireNonRoot:          cfg.requireNonRoot,
		dryRun:                  cfg.dryRun,
		allowedWorkingDirs:      cfg.allowedWorkingDirs,
		registerInterval:        30 * time.Second,
	}
	artifactUploader, err := newArtifactUploader(cfg.artifactStorageBackend, cfg.artifactStorageLocation)
	if err != nil {
		log.Fatalf("artifact storage backend: %v", err)
	}
	runner.artifactStorage = artifactUploader
	runner.artifactStorageBackend = hdcf.NormalizeArtifactStorageBackend(cfg.artifactStorageBackend)
	runner.artifactStorageLocation = strings.TrimSpace(cfg.artifactStorageLocation)
	if runner.logDir == "" {
		runner.logDir = "worker-logs"
	}
	if err := os.MkdirAll(runner.logDir, 0o755); err != nil {
		log.Fatalf("log directory: %v", err)
	}
	client, err := buildWorkerHTTPClient(cfg)
	if err != nil {
		log.Fatalf("http client: %v", err)
	}
	runner.httpClient = client

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.ensureRegistered(ctx, "initial"); err != nil {
		workerEvent("warn", "worker.register", map[string]any{
			"worker_id": cfg.workerID,
			"action":    "initial",
			"error":     err.Error(),
		})
	} else {
		workerEvent("info", "worker.register", map[string]any{
			"worker_id": cfg.workerID,
			"action":    "initial",
			"status":    "ok",
		})
	}
	if err := runner.reconnect(ctx); err != nil {
		workerEvent("warn", "worker.reconnect", map[string]any{
			"worker_id": cfg.workerID,
			"scope":     "startup",
			"error":     err.Error(),
		})
	} else {
		workerEvent("info", "worker.reconnect", map[string]any{
			"worker_id": cfg.workerID,
			"scope":     "startup",
			"status":    "ok",
		})
	}
	workerEvent("info", "worker.loop_start", map[string]any{
		"worker_id":        cfg.workerID,
		"control_endpoint": strings.TrimRight(cfg.controlURL, "/"),
	})
	go runner.heartbeatLoop(ctx)
	go runner.logCleanupLoop(ctx)

	runner.loop(ctx)
}

type workerConfig struct {
	controlURL              string
	workerID                string
	nonce                   string
	capabilities            []string
	token                   string
	workerTokenSecret       string
	workerTokenTTL          time.Duration
	pollInterval            time.Duration
	heartbeatInterval       time.Duration
	requestTimeout          time.Duration
	logDir                  string
	stateFile               string
	logRetentionDays        int
	logCleanupInterval      time.Duration
	reportMetrics           bool
	commandAllowlistEnabled bool
	commandAllowlist        []string
	requireNonRoot          bool
	dryRun                  bool
	allowedWorkingDirs      []string
	artifactStorageBackend  string
	artifactStorageLocation string
	tlsCA                   string
	tlsClientCert           string
	tlsClientKey            string
}

type workerConfigFile struct {
	ControlURL               *string  `json:"control_url"`
	WorkerID                 *string  `json:"worker_id"`
	WorkerNonce              *string  `json:"worker_nonce"`
	Capabilities             []string `json:"capabilities"`
	Token                    *string  `json:"token"`
	WorkerToken              *string  `json:"worker_token"`
	WorkerTokenSecret        *string  `json:"worker_token_secret"`
	WorkerTokenTTLSeconds    *int64   `json:"worker_token_ttl_seconds"`
	PollIntervalSeconds      *int64   `json:"poll_interval_seconds"`
	HeartbeatIntervalSeconds *int64   `json:"heartbeat_interval_seconds"`
	RequestTimeoutSeconds    *int64   `json:"request_timeout_seconds"`
	LogDir                   *string  `json:"log_dir"`
	StateFile                *string  `json:"state_file"`
	LogRetentionDays         *int64   `json:"log_retention_days"`
	LogCleanupIntervalSec    *int64   `json:"log_cleanup_interval_seconds"`
	ReportMetrics            *bool    `json:"heartbeat_metrics"`
	CommandAllowlist         *bool    `json:"command_allowlist"`
	AllowedCommands          *string  `json:"allowed_commands"`
	AllowedWorkingDirs       *string  `json:"allowed_working_dirs"`
	ArtifactStorageBackend   *string  `json:"artifact_storage_backend"`
	ArtifactStorageLocation  *string  `json:"artifact_storage_location"`
	RequireNonRoot           *bool    `json:"require_non_root"`
	DryRun                   *bool    `json:"dry_run"`
	TLSCA                    *string  `json:"tls_ca"`
	TLSClientCert            *string  `json:"tls_client_cert"`
	TLSClientKey             *string  `json:"tls_client_key"`
}

func parseWorkerConfig() workerConfig {
	cfgFile := loadWorkerConfig(workerConfigPathFromArgs())
	var cfg workerConfig
	var token string
	flag.StringVar(&cfg.controlURL, "control-url", resolveWorkerConfigString(cfgFile.ControlURL, "HDCF_CONTROL_URL", "http://localhost:8080"), "control plane url")
	flag.StringVar(&cfg.workerID, "worker-id", resolveWorkerConfigString(cfgFile.WorkerID, "HDCF_WORKER_ID", ""), "worker id")
	flag.StringVar(&cfg.nonce, "worker-nonce", resolveWorkerConfigString(cfgFile.WorkerNonce, "HDCF_WORKER_NONCE", ""), "optional registration nonce")
	capabilitiesDefault := resolveWorkerConfigStringList(cfgFile.Capabilities, "HDCF_WORKER_CAPABILITIES", "")
	capabilities := flag.String("capabilities", capabilitiesDefault, "comma-separated worker capabilities")
	flag.StringVar(&token, "token", resolveWorkerConfigString(cfgFile.Token, "HDCF_API_TOKEN", "dev-token"), "legacy api token")
	var workerToken string
	var workerTokenDefault string
	if cfgFile.WorkerToken != nil && strings.TrimSpace(*cfgFile.WorkerToken) != "" {
		workerTokenDefault = strings.TrimSpace(*cfgFile.WorkerToken)
	} else if envToken := strings.TrimSpace(os.Getenv("HDCF_WORKER_TOKEN")); envToken != "" {
		workerTokenDefault = envToken
	}
	flag.StringVar(&workerToken, "worker-token", workerTokenDefault, "worker token")
	flag.StringVar(&cfg.workerTokenSecret, "worker-token-secret", resolveWorkerConfigString(cfgFile.WorkerTokenSecret, "HDCF_WORKER_TOKEN_SECRET", ""), "secret for signed worker tokens")
	var tokenTTLSeconds int64
	flag.Int64Var(&tokenTTLSeconds, "worker-token-ttl-seconds", resolveWorkerConfigInt64(cfgFile.WorkerTokenTTLSeconds, "HDCF_WORKER_TOKEN_TTL_SECONDS", 3600), "signed worker token ttl seconds")
	var pollSec int
	var heartbeatSec int
	var timeoutSec int
	var logRetentionDays int
	var logCleanupIntervalSec int
	allowedCommands := flag.String("allowed-commands", resolveWorkerConfigString(cfgFile.AllowedCommands, "HDCF_WORKER_ALLOWED_COMMANDS", ""), "comma-separated allowed command binaries when -command-allowlist is enabled")
	allowedWorkingDirs := flag.String("allowed-working-dirs", resolveWorkerConfigString(cfgFile.AllowedWorkingDirs, "HDCF_WORKER_ALLOWED_WORKING_DIRS", ""), "comma-separated allowed working directories for strict job execution checks")
	artifactStorageBackend := flag.String("artifact-storage-backend", resolveWorkerConfigString(cfgFile.ArtifactStorageBackend, "HDCF_ARTIFACT_STORAGE_BACKEND", hdcf.ArtifactStorageBackendLocal), "artifact storage backend: local|nfs|s3")
	artifactStorageLocation := flag.String("artifact-storage-location", resolveWorkerConfigString(cfgFile.ArtifactStorageLocation, "HDCF_ARTIFACT_STORAGE_LOCATION", ""), "storage location for non-local artifact backends")
	commandAllowlist := flag.Bool("command-allowlist", resolveWorkerConfigBool(cfgFile.CommandAllowlist, "HDCF_WORKER_COMMAND_ALLOWLIST", false), "enforce command allowlist before execution")
	requireNonRoot := flag.Bool("require-non-root", resolveWorkerConfigBool(cfgFile.RequireNonRoot, "HDCF_WORKER_REQUIRE_NON_ROOT", false), "reject running jobs when this process is running as root")
	dryRun := flag.Bool("dry-run", resolveWorkerConfigBool(cfgFile.DryRun, "HDCF_WORKER_DRY_RUN", false), "validate and simulate execution without running commands")
	flag.IntVar(&pollSec, "poll-interval-seconds", int(resolveWorkerConfigInt64(cfgFile.PollIntervalSeconds, "HDCF_WORKER_POLL_INTERVAL_SECONDS", 3)), "poll interval seconds")
	flag.IntVar(&heartbeatSec, "heartbeat-interval-seconds", int(resolveWorkerConfigInt64(cfgFile.HeartbeatIntervalSeconds, "HDCF_WORKER_HEARTBEAT_INTERVAL_SECONDS", 5)), "heartbeat interval seconds")
	flag.BoolVar(&cfg.reportMetrics, "heartbeat-metrics", resolveWorkerConfigBool(cfgFile.ReportMetrics, "HDCF_WORKER_HEARTBEAT_METRICS", false), "include optional resource metrics in heartbeat payload")
	flag.IntVar(&timeoutSec, "request-timeout-seconds", int(resolveWorkerConfigInt64(cfgFile.RequestTimeoutSeconds, "HDCF_WORKER_REQUEST_TIMEOUT_SECONDS", 10)), "request timeout seconds")
	flag.IntVar(&logRetentionDays, "log-retention-days", int(resolveWorkerConfigInt64(cfgFile.LogRetentionDays, "HDCF_WORKER_LOG_RETENTION_DAYS", 30)), "days to retain worker local log/artifact files")
	flag.IntVar(&logCleanupIntervalSec, "log-cleanup-interval-seconds", int(resolveWorkerConfigInt64(cfgFile.LogCleanupIntervalSec, "HDCF_WORKER_LOG_CLEANUP_INTERVAL_SECONDS", 300)), "log cleanup interval seconds")
	flag.StringVar(&cfg.logDir, "log-dir", resolveWorkerConfigString(cfgFile.LogDir, "HDCF_WORKER_LOG_DIR", "worker-logs"), "path for local job logs")
	flag.StringVar(&cfg.stateFile, "state-file", resolveWorkerConfigString(cfgFile.StateFile, "HDCF_WORKER_STATE_FILE", ""), "path to reconnect state file (default worker-logs/worker-state.json)")
	flag.StringVar(&cfg.tlsCA, "tls-ca", resolveWorkerConfigString(cfgFile.TLSCA, "HDCF_TLS_CA", ""), "trusted control plane CA certificate")
	flag.StringVar(&cfg.tlsClientCert, "tls-client-cert", resolveWorkerConfigString(cfgFile.TLSClientCert, "HDCF_TLS_CLIENT_CERT", ""), "client certificate for mTLS")
	flag.StringVar(&cfg.tlsClientKey, "tls-client-key", resolveWorkerConfigString(cfgFile.TLSClientKey, "HDCF_TLS_CLIENT_KEY", ""), "client private key for mTLS")
	flag.String("config", workerConfigPathFromArgs(), "path to worker JSON config file (defaults to HDCF_WORKER_CONFIG)")
	flag.Parse()

	cfg.token = strings.TrimSpace(workerToken)
	if cfg.token == "" {
		cfg.token = strings.TrimSpace(token)
	}
	cfg.commandAllowlistEnabled = *commandAllowlist
	cfg.commandAllowlist = normalizeCommandAllowlist(*allowedCommands)
	cfg.requireNonRoot = *requireNonRoot
	cfg.dryRun = *dryRun
	cfg.allowedWorkingDirs = normalizeAllowedWorkingDirs(*allowedWorkingDirs)
	if cfg.commandAllowlistEnabled && len(cfg.commandAllowlist) == 0 {
		log.Fatalf("command allowlist is enabled but no allowed commands are configured")
	}
	if tokenTTLSeconds < 1 {
		tokenTTLSeconds = 1
	}
	cfg.workerTokenTTL = time.Duration(tokenTTLSeconds) * time.Second
	cfg.pollInterval = time.Duration(pollSec) * time.Second
	cfg.heartbeatInterval = time.Duration(heartbeatSec) * time.Second
	cfg.requestTimeout = time.Duration(timeoutSec) * time.Second
	cfg.logRetentionDays = logRetentionDays
	cfg.logCleanupInterval = time.Duration(logCleanupIntervalSec) * time.Second
	cfg.artifactStorageBackend = hdcf.NormalizeArtifactStorageBackend(*artifactStorageBackend)
	cfg.artifactStorageLocation = strings.TrimSpace(*artifactStorageLocation)
	if !hdcf.IsValidArtifactStorageBackend(cfg.artifactStorageBackend) {
		log.Fatalf("unsupported artifact storage backend: %s", cfg.artifactStorageBackend)
	}
	if cfg.artifactStorageBackend == hdcf.ArtifactStorageBackendS3 {
		log.Fatalf("artifact storage backend %s is not yet supported", hdcf.ArtifactStorageBackendS3)
	}
	if cfg.artifactStorageBackend == hdcf.ArtifactStorageBackendNFS && strings.TrimSpace(cfg.artifactStorageLocation) == "" {
		log.Fatalf("artifact-storage-location is required for nfs backend")
	}
	if strings.TrimSpace(cfg.workerID) == "" {
		host, _ := os.Hostname()
		cfg.workerID = fmt.Sprintf("%s-%s", host, hdcf.NewJobID())
	}
	if strings.TrimSpace(cfg.stateFile) == "" {
		cfg.stateFile = filepath.Join(cfg.logDir, "worker-state.json")
	}
	cfg.capabilities = splitCapabilities(*capabilities)
	return cfg
}

func workerConfigPathFromArgs() string {
	path := strings.TrimSpace(os.Getenv("HDCF_WORKER_CONFIG"))
	for i := 1; i < len(os.Args); i++ {
		arg := strings.TrimSpace(os.Args[i])
		switch {
		case arg == "-config":
			if i+1 < len(os.Args) {
				path = strings.TrimSpace(os.Args[i+1])
				i++
			}
		case strings.HasPrefix(arg, "-config="):
			path = strings.TrimSpace(strings.TrimPrefix(arg, "-config="))
		case arg == "--config":
			if i+1 < len(os.Args) {
				path = strings.TrimSpace(os.Args[i+1])
				i++
			}
		case strings.HasPrefix(arg, "--config="):
			path = strings.TrimSpace(strings.TrimPrefix(arg, "--config="))
		}
	}
	return path
}

func loadWorkerConfig(path string) workerConfigFile {
	if strings.TrimSpace(path) == "" {
		return workerConfigFile{}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read worker config: %v", err)
	}
	var cfg workerConfigFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse worker config: %v", err)
	}
	return cfg
}

func resolveWorkerConfigString(raw *string, envName, fallback string) string {
	if raw != nil && strings.TrimSpace(*raw) != "" {
		return strings.TrimSpace(*raw)
	}
	if envName != "" {
		if envVal := strings.TrimSpace(os.Getenv(envName)); envVal != "" {
			return envVal
		}
	}
	return fallback
}

func resolveWorkerConfigStringList(raw []string, envName, fallback string) string {
	if len(raw) > 0 {
		clean := splitCSV(strings.Join(raw, ","))
		if len(clean) > 0 {
			return strings.Join(clean, ",")
		}
	}
	return resolveWorkerConfigString(nil, envName, fallback)
}

func resolveWorkerConfigInt64(raw *int64, envName string, fallback int64) int64 {
	if raw != nil {
		return *raw
	}
	if envName == "" {
		return fallback
	}
	return getenvInt(envName, fallback)
}

func resolveWorkerConfigBool(raw *bool, envName string, fallback bool) bool {
	if raw != nil {
		return *raw
	}
	return getenvBool(envName, fallback)
}

func buildWorkerHTTPClient(cfg workerConfig) (*http.Client, error) {
	transport := &http.Transport{}
	if !strings.HasPrefix(cfg.controlURL, "https://") {
		return &http.Client{Timeout: cfg.requestTimeout, Transport: transport}, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if cfg.tlsCA != "" {
		caBundle, err := os.ReadFile(cfg.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("read tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBundle) {
			return nil, fmt.Errorf("parse tls ca: no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if (cfg.tlsClientCert != "" && cfg.tlsClientKey == "") || (cfg.tlsClientCert == "" && cfg.tlsClientKey != "") {
		return nil, fmt.Errorf("both -tls-client-cert and -tls-client-key are required when using client certificate auth")
	}
	if cfg.tlsClientCert != "" && cfg.tlsClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.tlsClientCert, cfg.tlsClientKey)
		if err != nil {
			return nil, fmt.Errorf("load tls client keypair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Timeout: cfg.requestTimeout, Transport: transport}, nil
}

func getenv(name, fallback string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	return v
}

func getenvInt(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvBool(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	raw = strings.ToLower(strings.TrimSpace(raw))
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func splitCapabilities(raw string) []string {
	return splitCSV(raw)
}

func normalizeCommandAllowlist(raw string) []string {
	return splitCSV(raw)
}

func normalizeAllowedWorkingDirs(raw string) []string {
	parsed := splitCSV(raw)
	out := make([]string, 0, len(parsed))
	seen := map[string]struct{}{}
	for _, p := range parsed {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		abs, err := filepath.Abs(trimmed)
		if err != nil {
			abs = filepath.Clean(trimmed)
		} else {
			abs = filepath.Clean(abs)
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		out = append(out, abs)
	}
	return out
}

type workerRunner struct {
	controlURL              string
	workerID                string
	nonce                   string
	capabilities            []string
	token                   string
	workerTokenSecret       string
	workerTokenTTL          time.Duration
	pollInterval            time.Duration
	heartbeatInterval       time.Duration
	requestTimeout          time.Duration
	logDir                  string
	stateFile               string
	logRetentionDays        int
	logCleanupInterval      time.Duration
	currentJobID            atomic.Value
	httpClient              *http.Client
	stateMu                 sync.Mutex
	heartbeatSeq            int64
	completionSeq           int64
	lastRegisterAt          int64
	registerInterval        time.Duration
	reportMetrics           bool
	commandAllowlistEnabled bool
	commandAllowlist        []string
	requireNonRoot          bool
	dryRun                  bool
	allowedWorkingDirs      []string
	artifactStorageBackend  string
	artifactStorageLocation string
	artifactStorage         artifactUploader
}

type workerReconnectState struct {
	CurrentJobID        string                       `json:"current_job_id"`
	CurrentAssignmentID string                       `json:"current_assignment_id"`
	CompletedJobs       []hdcf.ReconnectCompletedJob `json:"completed_jobs"`
	HeartbeatSeq        int64                        `json:"heartbeat_seq"`
	LastCompletionSeq   int64                        `json:"last_completion_seq"`
}

type artifactUploadState struct {
	backend  string
	location string
	state    string
	errorMsg string
}

type artifactUploader interface {
	UploadArtifacts(ctx context.Context, jobID string, artifactID string, stdoutPath, stderrPath string) (artifactUploadState, error)
}

type localArtifactUploader struct{}

type nfsArtifactUploader struct {
	rootPath string
}

func newArtifactUploader(backend, location string) (artifactUploader, error) {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case hdcf.ArtifactStorageBackendNFS:
		cleanLocation := strings.TrimSpace(location)
		if cleanLocation == "" {
			return nil, fmt.Errorf("artifact-storage-location is required for nfs")
		}
		return nfsArtifactUploader{rootPath: cleanLocation}, nil
	case hdcf.ArtifactStorageBackendLocal:
		return localArtifactUploader{}, nil
	default:
		return nil, fmt.Errorf("unsupported artifact backend: %s", backend)
	}
}

func (localArtifactUploader) UploadArtifacts(ctx context.Context, jobID, artifactID, stdoutPath, stderrPath string) (artifactUploadState, error) {
	if strings.TrimSpace(stdoutPath) == "" || strings.TrimSpace(stderrPath) == "" {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendLocal,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: "missing artifact paths",
		}, fmt.Errorf("artifact path required")
	}
	return artifactUploadState{
		backend:  hdcf.ArtifactStorageBackendLocal,
		location: filepath.Dir(stdoutPath),
		state:    hdcf.ArtifactUploadStateOK,
	}, nil
}

func (n nfsArtifactUploader) UploadArtifacts(ctx context.Context, jobID, artifactID, stdoutPath, stderrPath string) (artifactUploadState, error) {
	_ = ctx
	if strings.TrimSpace(jobID) == "" {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: "missing job id",
		}, fmt.Errorf("job id required")
	}
	if strings.TrimSpace(stdoutPath) == "" || strings.TrimSpace(stderrPath) == "" {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: "missing artifact paths",
		}, fmt.Errorf("artifact path required")
	}
	targetDir := filepath.Clean(n.rootPath)
	if targetDir == "" {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: "invalid artifact location",
		}, fmt.Errorf("invalid artifact location")
	}
	jobDir := filepath.Join(targetDir, jobID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: err.Error(),
		}, err
	}
	stdoutDest := filepath.Join(jobDir, filepath.Base(stdoutPath))
	stderrDest := filepath.Join(jobDir, filepath.Base(stderrPath))
	if err := copyArtifactFile(stdoutPath, stdoutDest); err != nil {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: err.Error(),
		}, err
	}
	if err := copyArtifactFile(stderrPath, stderrDest); err != nil {
		return artifactUploadState{
			backend:  hdcf.ArtifactStorageBackendNFS,
			state:    hdcf.ArtifactUploadStateFailed,
			errorMsg: err.Error(),
		}, err
	}
	return artifactUploadState{
		backend:  hdcf.ArtifactStorageBackendNFS,
		location: jobDir,
		state:    hdcf.ArtifactUploadStateOK,
	}, nil
}

func copyArtifactFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	dstTmp := dst + ".tmp"
	target, err := os.Create(dstTmp)
	if err != nil {
		return err
	}
	defer target.Close()

	if _, err := io.Copy(target, source); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}
	if err := target.Sync(); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}
	if err := target.Close(); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}
	return os.Rename(dstTmp, dst)
}

func (r *workerRunner) loop(ctx context.Context) {
	backoff := r.pollInterval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		workerEvent("debug", "worker.loop_cycle", map[string]any{
			"worker_id":  r.workerID,
			"backoff_ms": backoff.Milliseconds(),
		})
		if err := r.ensureRegistered(ctx, "loop"); err != nil {
			workerEvent("warn", "worker.register", map[string]any{
				"worker_id": r.workerID,
				"action":    "loop",
				"error":     err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		workerEvent("debug", "worker.register", map[string]any{
			"worker_id":        r.workerID,
			"action":           "loop",
			"status":           "ok",
			"last_register_at": time.Unix(0, atomic.LoadInt64(&r.lastRegisterAt)).Format(time.RFC3339Nano),
		})

		job, err := r.nextJob(ctx)
		if err != nil {
			workerEvent("warn", "worker.next_job", map[string]any{
				"worker_id": r.workerID,
				"error":     err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		if job == nil {
			workerEvent("debug", "worker.next_job", map[string]any{
				"worker_id": r.workerID,
				"status":    "no_job",
			})
			time.Sleep(jitterDuration(r.pollInterval))
			backoff = r.pollInterval
			continue
		}
		workerEvent("info", "worker.next_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"attempt_count": job.AttemptCount,
			"command":       job.Command,
		})
		if err := r.ackJob(ctx, job); err != nil {
			workerEvent("warn", "worker.ack_job", map[string]any{
				"worker_id":     r.workerID,
				"job_id":        job.JobID,
				"assignment_id": job.AssignmentID,
				"error":         err.Error(),
			})
			time.Sleep(backoff)
			backoff = nextBackoff(backoff, 30*time.Second)
			continue
		}
		workerEvent("info", "worker.ack_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
		})

		if err := r.setCurrentReconnectState(job.JobID, job.AssignmentID); err != nil {
			workerEvent("warn", "worker.state_persist", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
			})
		}
		r.currentJobID.Store(job.JobID)
		r.executeJob(ctx, job)
		r.currentJobID.Store("")
		if err := r.clearCurrentReconnectState(); err != nil {
			workerEvent("warn", "worker.state_persist", map[string]any{
				"worker_id":      r.workerID,
				"current_job_id": "",
				"error":          err.Error(),
			})
		}
		backoff = r.pollInterval
	}
}

func (r *workerRunner) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(jitterDuration(r.heartbeatInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			workerEvent("debug", "worker.heartbeat_cycle", map[string]any{
				"worker_id": r.workerID,
			})
			if err := r.ensureRegistered(ctx, "heartbeat"); err != nil {
				workerEvent("warn", "worker.register", map[string]any{
					"worker_id": r.workerID,
					"action":    "heartbeat",
					"error":     err.Error(),
				})
				continue
			}
			current := r.getCurrentJobID()
			if err := r.sendHeartbeat(ctx, current); err != nil {
				workerEvent("warn", "worker.heartbeat", map[string]any{
					"worker_id": r.workerID,
					"error":     err.Error(),
				})
				continue
			}
			workerEvent("info", "worker.heartbeat", map[string]any{
				"worker_id":      r.workerID,
				"current_job_id": current,
			})
			if err := r.flushPendingReconnectResults(ctx); err != nil {
				workerEvent("warn", "worker.reconnect_flush", map[string]any{
					"worker_id": r.workerID,
					"error":     err.Error(),
				})
			} else {
				workerEvent("debug", "worker.reconnect_flush", map[string]any{
					"worker_id": r.workerID,
				})
			}
			ticker.Reset(jitterDuration(r.heartbeatInterval))
		}
	}
}

func (r *workerRunner) logCleanupLoop(ctx context.Context) {
	if r.logRetentionDays <= 0 || r.logCleanupInterval <= 0 {
		workerEvent("info", "worker.log_cleanup", map[string]any{
			"worker_id":      r.workerID,
			"retention_days": r.logRetentionDays,
			"interval_sec":   int(r.logCleanupInterval.Seconds()),
			"status":         "disabled",
		})
		return
	}

	cleanupTicker := time.NewTicker(r.logCleanupInterval)
	defer cleanupTicker.Stop()
	_ = r.cleanupLogArtifacts()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			if err := r.cleanupLogArtifacts(); err != nil {
				workerEvent("warn", "worker.log_cleanup", map[string]any{
					"worker_id": r.workerID,
					"status":    "failed",
					"error":     err.Error(),
				})
			}
		}
	}
}

func (r *workerRunner) reconnect(ctx context.Context) error {
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	r.heartbeatSeq = state.HeartbeatSeq
	r.completionSeq = state.LastCompletionSeq
	workerEvent("info", "worker.reconnect", map[string]any{
		"worker_id":       r.workerID,
		"scope":           "startup",
		"has_current_job": strings.TrimSpace(state.CurrentJobID) != "",
		"pending_replays": len(state.CompletedJobs),
	})

	req := hdcf.WorkerReconnectRequest{
		WorkerID:      r.workerID,
		CompletedJobs: state.CompletedJobs,
	}
	if strings.TrimSpace(state.CurrentJobID) != "" {
		currentJobID := state.CurrentJobID
		req.CurrentJobID = &currentJobID
	}
	if len(req.CompletedJobs) == 0 && req.CurrentJobID == nil {
		req.CurrentJobID = nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	resp, err := r.postJSONWithResponse(reqCtx, r.controlURL+"/reconnect", req)
	if err != nil {
		return err
	}
	workerEvent("info", "worker.reconnect_response", map[string]any{
		"worker_id":    r.workerID,
		"status":       resp.Status,
		"action_count": len(resp.Actions),
	})

	for _, action := range resp.Actions {
		actionLog := map[string]any{
			"worker_id":     r.workerID,
			"action":        action.Action,
			"job_id":        action.JobID,
			"assignment_id": action.AssignmentID,
			"result":        action.Result,
		}
		switch action.Action {
		case hdcf.ReconnectActionClearCurrentJob:
			actionLog["state_change"] = "current_job_clear"
			if action.JobID == "" || action.JobID == state.CurrentJobID {
				_ = r.clearCurrentReconnectState()
			}
		case hdcf.ReconnectActionReplayCompleted, hdcf.ReconnectActionReplayFailed:
			actionLog["state_change"] = action.Action
			if action.Result == hdcf.ReconnectResultAccepted {
				_ = r.removeCompletedReconnectResult(action.JobID, action.AssignmentID)
				actionLog["result_state"] = "removed_local_replay"
			} else {
				actionLog["result_state"] = "kept_local_replay"
			}
			if errMsg := action.Error; errMsg != "" {
				actionLog["error"] = errMsg
			}
		default:
			actionLog["state_change"] = "unknown"
		}
		workerEvent("debug", "worker.reconnect_action", actionLog)
	}
	return nil
}

func (r *workerRunner) getCurrentJobID() *string {
	v := r.currentJobID.Load()
	if v == nil {
		return nil
	}
	id, ok := v.(string)
	if !ok || strings.TrimSpace(id) == "" {
		return nil
	}
	return &id
}

func (r *workerRunner) validateExecutionPolicy(job *hdcf.AssignedJob) error {
	if err := r.validateNotRoot(); err != nil {
		return err
	}
	if err := r.validateAllowedCommand(job.Command); err != nil {
		return err
	}
	if err := r.validateWorkingDir(job.WorkingDir); err != nil {
		return err
	}
	return nil
}

func (r *workerRunner) validateNotRoot() error {
	if !r.requireNonRoot {
		return nil
	}
	current, err := user.Current()
	if err != nil {
		return fmt.Errorf("resolve worker process user: %w", err)
	}
	if current.Username == "root" || current.Uid == "0" {
		return fmt.Errorf("execution denied by worker policy: running as root")
	}
	return nil
}

func (r *workerRunner) validateAllowedCommand(command string) error {
	if !r.commandAllowlistEnabled {
		return nil
	}
	command = strings.TrimSpace(command)
	if command == "" {
		return fmt.Errorf("command is empty")
	}
	requestedCommand := filepath.Clean(command)
	requestedCommandBase := filepath.Base(requestedCommand)
	for _, allowed := range r.commandAllowlist {
		normalizedAllowed := filepath.Clean(strings.TrimSpace(allowed))
		if normalizedAllowed == "" {
			continue
		}
		if normalizedAllowed == requestedCommand || normalizedAllowed == requestedCommandBase {
			return nil
		}
		allowedBase := filepath.Base(normalizedAllowed)
		if allowedBase == requestedCommandBase {
			return nil
		}
	}
	return fmt.Errorf("command blocked by allowlist: %s", command)
}

func (r *workerRunner) validateWorkingDir(raw string) error {
	if len(r.allowedWorkingDirs) == 0 {
		return nil
	}
	workingDir := strings.TrimSpace(raw)
	if workingDir == "" {
		return fmt.Errorf("working_dir is required when working directory allowlist is configured")
	}
	abs, err := filepath.Abs(workingDir)
	if err != nil {
		return fmt.Errorf("resolve working_dir: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("working_dir does not exist: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("working_dir is not a directory: %s", abs)
	}
	abs = filepath.Clean(abs)
	for _, allowed := range r.allowedWorkingDirs {
		allowedClean := filepath.Clean(strings.TrimSpace(allowed))
		if allowedClean == "" {
			continue
		}
		relative, err := filepath.Rel(allowedClean, abs)
		if err != nil {
			continue
		}
		if relative == "." || (relative != ".." && !strings.HasPrefix(relative, fmt.Sprintf("..%s", string(filepath.Separator)))) {
			return nil
		}
	}
	return fmt.Errorf("working_dir blocked by allowlist: %s", abs)
}

func (r *workerRunner) register(ctx context.Context) error {
	req := hdcf.RegisterWorkerRequest{
		WorkerID:     r.workerID,
		Nonce:        r.nonce,
		Capabilities: r.capabilities,
	}
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/register", r.controlURL)
	return retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.postJSON(ctx, endpoint, payload)
	}, 4)
}

func (r *workerRunner) ensureRegistered(ctx context.Context, scope string) error {
	if r.registerInterval <= 0 {
		return r.register(ctx)
	}

	last := atomic.LoadInt64(&r.lastRegisterAt)
	if last > 0 {
		lastRegister := time.Unix(0, last)
		if time.Since(lastRegister) < r.registerInterval {
			workerEvent("debug", "worker.register", map[string]any{
				"worker_id":      r.workerID,
				"action":         scope,
				"status":         "throttled",
				"throttle_until": lastRegister.Add(r.registerInterval).Format(time.RFC3339Nano),
			})
			return nil
		}
	}

	if err := r.register(ctx); err != nil {
		return err
	}
	atomic.StoreInt64(&r.lastRegisterAt, time.Now().UnixNano())
	return nil
}

func (r *workerRunner) nextJob(ctx context.Context) (*hdcf.AssignedJob, error) {
	ctx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()

	endpoint := fmt.Sprintf("%s/next-job?worker_id=%s", r.controlURL, r.workerID)
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	values := u.Query()
	values.Set("worker_id", r.workerID)
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(authHeader, r.token)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		if err := r.flushPendingReconnectResults(ctx); err != nil {
			workerEvent("warn", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"scope":     "no_content_after_next_job",
				"error":     err.Error(),
			})
			return nil, err
		}
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.next_job", map[string]any{
			"worker_id": r.workerID,
			"status":    resp.Status,
			"body":      body,
		})
		return nil, fmt.Errorf("next-job status=%s body=%s", resp.Status, body)
	}
	var job hdcf.AssignedJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		workerEvent("warn", "worker.next_job", map[string]any{
			"worker_id": r.workerID,
			"error":     err.Error(),
		})
		return nil, err
	}
	if err := r.flushPendingReconnectResults(ctx); err != nil {
		workerEvent("warn", "worker.reconnect_flush", map[string]any{
			"worker_id": r.workerID,
			"scope":     "post_next_job",
			"error":     err.Error(),
		})
	}
	return &job, nil
}

func (r *workerRunner) ackJob(ctx context.Context, job *hdcf.AssignedJob) error {
	req := hdcf.AckJobRequest{
		JobID:        job.JobID,
		WorkerID:     r.workerID,
		AssignmentID: job.AssignmentID,
	}
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/ack", r.controlURL)
	err := retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.postJSON(ctx, endpoint, payload)
	}, 8)
	if err != nil {
		workerEvent("warn", "worker.ack_job", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"error":         err.Error(),
		})
		return err
	}
	return nil
}

func (r *workerRunner) sendHeartbeat(ctx context.Context, currentJobID *string) error {
	seq := r.nextHeartbeatSeq()
	body := hdcf.HeartbeatRequest{
		WorkerID:     r.workerID,
		CurrentJobID: currentJobID,
		Timestamp:    time.Now().Format(time.RFC3339),
		Sequence:     seq,
	}
	if r.reportMetrics {
		body.Metrics = r.collectHeartbeatMetrics()
	}
	payload, _ := json.Marshal(body)
	endpoint := fmt.Sprintf("%s/heartbeat", r.controlURL)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.token)
	ctx, cancel := context.WithTimeout(ctx, r.requestTimeout)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.heartbeat", map[string]any{
			"worker_id": r.workerID,
			"status":    resp.Status,
			"body":      body,
		})
		return fmt.Errorf("heartbeat status=%s body=%s", resp.Status, body)
	}
	return nil
}

func (r *workerRunner) collectHeartbeatMetrics() *hdcf.HeartbeatMetrics {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	memoryUsageMB := float64(memStats.Alloc) / (1024 * 1024)
	return &hdcf.HeartbeatMetrics{
		MemoryUsageMB: &memoryUsageMB,
	}
}

func (r *workerRunner) nextHeartbeatSeq() int64 {
	next := atomic.AddInt64(&r.heartbeatSeq, 1)
	if next < 1 {
		next = 1
		atomic.StoreInt64(&r.heartbeatSeq, next)
	}
	if err := r.persistSequenceState(next, atomic.LoadInt64(&r.completionSeq)); err != nil {
		workerEvent("warn", "worker.sequence_persist", map[string]any{
			"worker_id": r.workerID,
			"scope":     "heartbeat_seq",
			"error":     err.Error(),
		})
	}
	return next
}

func (r *workerRunner) nextCompletionSeq() int64 {
	next := atomic.AddInt64(&r.completionSeq, 1)
	if next < 1 {
		next = 1
		atomic.StoreInt64(&r.completionSeq, next)
	}
	if err := r.persistSequenceState(atomic.LoadInt64(&r.heartbeatSeq), next); err != nil {
		workerEvent("warn", "worker.sequence_persist", map[string]any{
			"worker_id": r.workerID,
			"scope":     "completion_seq",
			"error":     err.Error(),
		})
	}
	return next
}

func (r *workerRunner) persistSequenceState(heartbeatSeq, completionSeq int64) error {
	if heartbeatSeq < 0 {
		heartbeatSeq = 0
	}
	if completionSeq < 0 {
		completionSeq = 0
	}
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.HeartbeatSeq = heartbeatSeq
	state.LastCompletionSeq = completionSeq
	return r.persistReconnectState(state)
}

func (r *workerRunner) reportComplete(ctx context.Context, req hdcf.CompleteRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/complete", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		workerEvent("warn", "worker.report_complete", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"artifact_id":   req.ArtifactID,
			"exit_code":     req.ExitCode,
			"error":         err.Error(),
		})
		return err
	}
	return nil
}

func (r *workerRunner) reportFail(ctx context.Context, req hdcf.FailRequest) error {
	payload, _ := json.Marshal(req)
	endpoint := fmt.Sprintf("%s/fail", r.controlURL)
	if err := r.postJSON(ctx, endpoint, payload); err != nil {
		workerEvent("warn", "worker.report_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        req.JobID,
			"assignment_id": req.AssignmentID,
			"exit_code":     req.ExitCode,
			"error":         err.Error(),
		})
		return err
	}
	return nil
}

func (r *workerRunner) postJSON(ctx context.Context, endpoint string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.workerAuthToken())
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"body":      body,
		})
		return fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}
	workerEvent("debug", "worker.http_request", map[string]any{
		"worker_id": r.workerID,
		"endpoint":  endpoint,
		"status":    resp.Status,
	})
	return nil
}

func (r *workerRunner) postJSONWithResponse(ctx context.Context, endpoint string, payload interface{}) (hdcf.WorkerReconnectResponse, error) {
	var zero hdcf.WorkerReconnectResponse
	body, err := json.Marshal(payload)
	if err != nil {
		return zero, err
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(body))
	if err != nil {
		return zero, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, r.workerAuthToken())
	resp, err := r.httpClient.Do(req)
	if err != nil {
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"error":     err.Error(),
		})
		return zero, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readLimitedBody(resp.Body)
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"body":      body,
		})
		return zero, fmt.Errorf("request status=%s body=%s", resp.Status, body)
	}

	var parsed hdcf.WorkerReconnectResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		workerEvent("warn", "worker.http_request", map[string]any{
			"worker_id": r.workerID,
			"endpoint":  endpoint,
			"status":    resp.Status,
			"error":     err.Error(),
		})
		return zero, err
	}
	workerEvent("debug", "worker.http_request", map[string]any{
		"worker_id": r.workerID,
		"endpoint":  endpoint,
		"status":    resp.Status,
		"actions":   len(parsed.Actions),
	})
	return parsed, nil
}

func (r *workerRunner) workerAuthToken() string {
	if strings.TrimSpace(r.workerTokenSecret) == "" {
		return r.token
	}
	signed, err := makeSignedWorkerToken(r.workerID, r.workerTokenTTL, r.workerTokenSecret)
	if err != nil {
		workerEvent("warn", "worker.token", map[string]any{
			"worker_id": r.workerID,
			"error":     err.Error(),
		})
		return r.token
	}
	return signed
}

func makeSignedWorkerToken(workerID string, ttl time.Duration, secret string) (string, error) {
	if strings.TrimSpace(secret) == "" || strings.TrimSpace(workerID) == "" {
		return "", fmt.Errorf("invalid token inputs")
	}
	expiresAt := time.Now().Add(ttl).Unix()
	nonce := hdcf.NewJobID()
	payload := fmt.Sprintf("%s|%d|%s", workerID, expiresAt, nonce)
	encodedPayload := base64.RawURLEncoding.EncodeToString([]byte(payload))
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(encodedPayload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("v1.%s.%s", encodedPayload, sig), nil
}

func (r *workerRunner) executeJob(ctx context.Context, job *hdcf.AssignedJob) {
	start := time.Now()
	stdoutTmpPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stdout.log.tmp", job.JobID))
	stderrTmpPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stderr.log.tmp", job.JobID))
	stdoutPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stdout.log", job.JobID))
	stderrPath := filepath.Join(r.logDir, fmt.Sprintf("%s.stderr.log", job.JobID))

	if err := r.validateExecutionPolicy(job); err != nil {
		r.handleJobFailure(ctx, job, fmt.Errorf("execution policy denied: %w", err))
		return
	}

	stdout, err := os.Create(stdoutTmpPath)
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	defer stdout.Close()

	stderr, err := os.Create(stderrTmpPath)
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	defer stderr.Close()

	timeout := time.Duration(job.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}

	if r.dryRun {
		if _, err := fmt.Fprintf(stdout, "dry-run execution: command=%q args=%v timeout_ms=%d working_dir=%q\n", job.Command, job.Args, job.TimeoutMs, strings.TrimSpace(job.WorkingDir)); err != nil {
			r.handleJobFailure(ctx, job, err)
			return
		}
		if _, err := fmt.Fprintf(stderr, "dry-run mode enabled; no process execution performed\n"); err != nil {
			r.handleJobFailure(ctx, job, err)
			return
		}
		workerEvent("info", "worker.job_execute", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"state":         "dry_run_validated",
			"command":       job.Command,
			"attempt_count": job.AttemptCount,
		})
		if err := r.finalizeArtifact(stdoutTmpPath, stdoutPath); err != nil {
			failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
			r.handleJobFailure(ctx, job, errors.New(failMsg))
			return
		}
		if err := r.finalizeArtifact(stderrTmpPath, stderrPath); err != nil {
			_ = os.Rename(stdoutPath, stdoutTmpPath)
			failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
			r.handleJobFailure(ctx, job, errors.New(failMsg))
			return
		}

		stdoutSHA256, err := r.hashArtifact(stdoutPath)
		if err != nil {
			failMsg := fmt.Sprintf("artifact hash failed: %v", err)
			r.handleJobFailure(ctx, job, errors.New(failMsg))
			return
		}
		stderrSHA256, err := r.hashArtifact(stderrPath)
		if err != nil {
			failMsg := fmt.Sprintf("artifact hash failed: %v", err)
			r.handleJobFailure(ctx, job, errors.New(failMsg))
			return
		}
		summary := fmt.Sprintf("dry_run validation completed duration_ms=%d", time.Since(start).Milliseconds())
		r.reportCompletedJob(ctx, job, start, summary, stdoutPath, stderrPath, stdoutTmpPath, stderrTmpPath, stdoutSHA256, stderrSHA256, 0)
		return
	}

	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, job.Command, job.Args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if strings.TrimSpace(job.WorkingDir) != "" {
		cmd.Dir = job.WorkingDir
	}

	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"command":       job.Command,
		"attempt_count": job.AttemptCount,
		"timeout_ms":    job.TimeoutMs,
		"working_dir":   strings.TrimSpace(job.WorkingDir),
		"state":         "starting",
	})
	err = cmd.Start()
	if err != nil {
		r.handleJobFailure(ctx, job, err)
		return
	}
	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"state":         "running",
		"pid":           cmd.Process.Pid,
	})

	err = cmd.Wait()
	exitCode := 0
	msg := ""
	if err != nil {
		exitCode = extractExitCode(err)
		msg = err.Error()
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			msg = "execution timeout"
			exitCode = -1
		}
		completionSeq := r.nextCompletionSeq()
		failReq := hdcf.FailRequest{
			JobID:        job.JobID,
			WorkerID:     r.workerID,
			AssignmentID: job.AssignmentID,
			ExitCode:     exitCode,
			Error:        msg,
		}
		reconnectEntry := hdcf.ReconnectCompletedJob{
			JobID:         job.JobID,
			AssignmentID:  job.AssignmentID,
			CompletionSeq: completionSeq,
			Status:        hdcf.StatusFailed,
			ExitCode:      failReq.ExitCode,
			StderrPath:    stderrTmpPath,
			StdoutPath:    stdoutTmpPath,
			Error:         msg,
		}
		if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
			workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
				"action":    "enqueue_failure",
			})
		}
		if retryWithBackoff(ctx, r.requestTimeout, func() error {
			return r.reportFail(ctx, failReq)
		}, 8) == nil {
			workerEvent("info", "worker.job_reported_fail", map[string]any{
				"worker_id":     r.workerID,
				"job_id":        job.JobID,
				"assignment_id": job.AssignmentID,
				"exit_code":     failReq.ExitCode,
				"error":         msg,
			})
			if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
				workerEvent("warn", "worker.job_reported_fail", map[string]any{
					"worker_id": r.workerID,
					"job_id":    job.JobID,
					"error":     err.Error(),
				})
			}
		}
		workerEvent("info", "worker.job_execute", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"state":         "failed",
			"exit_code":     exitCode,
			"message":       msg,
		})
		return
	}
	if err := r.finalizeArtifact(stdoutTmpPath, stdoutPath); err != nil {
		failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}
	if err := r.finalizeArtifact(stderrTmpPath, stderrPath); err != nil {
		_ = os.Rename(stdoutPath, stdoutTmpPath)
		failMsg := fmt.Sprintf("artifact finalization failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}

	stdoutSHA256, err := r.hashArtifact(stdoutPath)
	if err != nil {
		failMsg := fmt.Sprintf("artifact hash failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}
	stderrSHA256, err := r.hashArtifact(stderrPath)
	if err != nil {
		failMsg := fmt.Sprintf("artifact hash failed: %v", err)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}

	summary := fmt.Sprintf("exit_code=0 duration_ms=%d", time.Since(start).Milliseconds())
	r.reportCompletedJob(ctx, job, start, summary, stdoutPath, stderrPath, stdoutTmpPath, stderrTmpPath, stdoutSHA256, stderrSHA256, exitCode)
}

func (r *workerRunner) finalizeArtifact(tmpPath, finalPath string) error {
	_, err := os.Stat(tmpPath)
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

func (r *workerRunner) hashArtifact(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (r *workerRunner) reportCompletedJob(ctx context.Context, job *hdcf.AssignedJob, start time.Time, summary, stdoutPath, stderrPath, stdoutTmpPath, stderrTmpPath, stdoutSHA256, stderrSHA256 string, exitCode int) {
	duration := time.Since(start).Milliseconds()
	artifactID := hdcf.NewJobID()
	uploadState, uploadErr := r.artifactStorage.UploadArtifacts(ctx, job.JobID, artifactID, stdoutPath, stderrPath)
	if uploadErr != nil {
		failMsg := fmt.Sprintf("artifact storage failed: %v", uploadErr)
		r.handleJobFailure(ctx, job, errors.New(failMsg))
		return
	}
	completionSeq := r.nextCompletionSeq()
	compReq := hdcf.CompleteRequest{
		JobID:               job.JobID,
		WorkerID:            r.workerID,
		AssignmentID:        job.AssignmentID,
		ExitCode:            exitCode,
		ArtifactID:          artifactID,
		StdoutPath:          stdoutPath,
		StderrPath:          stderrPath,
		StdoutTmpPath:       stdoutTmpPath,
		StderrTmpPath:       stderrTmpPath,
		StdoutSHA256:        stdoutSHA256,
		StderrSHA256:        stderrSHA256,
		ArtifactBackend:     uploadState.backend,
		ArtifactLocation:    uploadState.location,
		ArtifactUploadState: uploadState.state,
		ArtifactUploadError: uploadState.errorMsg,
		CompletionSeq:       completionSeq,
		ResultSummary:       summary,
	}
	reconnectEntry := hdcf.ReconnectCompletedJob{
		JobID:               job.JobID,
		AssignmentID:        job.AssignmentID,
		CompletionSeq:       completionSeq,
		Status:              hdcf.StatusCompleted,
		ExitCode:            exitCode,
		StdoutPath:          stdoutPath,
		StderrPath:          stderrPath,
		ArtifactID:          artifactID,
		StdoutTmpPath:       stdoutTmpPath,
		StderrTmpPath:       stderrTmpPath,
		StdoutSHA256:        stdoutSHA256,
		StderrSHA256:        stderrSHA256,
		ArtifactBackend:     uploadState.backend,
		ArtifactLocation:    uploadState.location,
		ArtifactUploadState: uploadState.state,
		ArtifactUploadError: uploadState.errorMsg,
		ResultSummary:       summary,
	}
	workerEvent("info", "worker.job_execute", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"state":         "ready_for_completion",
		"duration_ms":   duration,
		"artifact_id":   artifactID,
		"stdout_sha256": stdoutSHA256,
		"stderr_sha256": stderrSHA256,
	})
	if err := r.enqueueCompletedReconnectResult(reconnectEntry); err != nil {
		workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
			"worker_id": r.workerID,
			"job_id":    job.JobID,
			"error":     err.Error(),
			"action":    "enqueue_completion",
		})
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportComplete(ctx, compReq)
	}, 8) == nil {
		workerEvent("info", "worker.job_completed", map[string]any{
			"worker_id":      r.workerID,
			"job_id":         job.JobID,
			"assignment_id":  job.AssignmentID,
			"artifact_id":    artifactID,
			"duration_ms":    duration,
			"stdout_path":    stdoutPath,
			"stderr_path":    stderrPath,
			"exit_code":      exitCode,
			"result_summary": summary,
		})
		if err := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); err != nil {
			workerEvent("warn", "worker.job_completed", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     err.Error(),
			})
		}
	} else {
		workerEvent("warn", "worker.job_completed", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"artifact_id":   artifactID,
			"error":         "report_complete_failed_after_retries",
		})
	}
}

func (r *workerRunner) loadReconnectState() (workerReconnectState, error) {
	state := workerReconnectState{}
	b, err := os.ReadFile(r.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, err
	}
	if err := json.Unmarshal(b, &state); err != nil {
		return state, err
	}
	return state, nil
}

func (r *workerRunner) persistReconnectState(state workerReconnectState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.stateFile), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(r.stateFile), ".hdcf-worker-state-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	_, err = tmp.Write(raw)
	if err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, r.stateFile); err != nil {
		return err
	}
	return nil
}

func (r *workerRunner) setCurrentReconnectState(jobID, assignmentID string) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.CurrentJobID = strings.TrimSpace(jobID)
	state.CurrentAssignmentID = strings.TrimSpace(assignmentID)
	if state.CurrentJobID == "" {
		state.CurrentAssignmentID = ""
	}
	return r.persistReconnectState(state)
}

func (r *workerRunner) clearCurrentReconnectState() error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	state.CurrentJobID = ""
	state.CurrentAssignmentID = ""
	return r.persistReconnectState(state)
}

func (r *workerRunner) enqueueCompletedReconnectResult(entry hdcf.ReconnectCompletedJob) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}

	next := make([]hdcf.ReconnectCompletedJob, 0, len(state.CompletedJobs)+1)
	for _, existing := range state.CompletedJobs {
		if existing.JobID == entry.JobID && existing.AssignmentID == entry.AssignmentID && strings.TrimSpace(entry.JobID) != "" {
			continue
		}
		next = append(next, existing)
	}
	next = append(next, entry)
	state.CompletedJobs = next
	return r.persistReconnectState(state)
}

func (r *workerRunner) removeCompletedReconnectResult(jobID, assignmentID string) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()

	state, err := r.loadReconnectState()
	if err != nil {
		return err
	}
	trimmedJob := strings.TrimSpace(jobID)
	trimmedAssignment := strings.TrimSpace(assignmentID)

	next := state.CompletedJobs[:0]
	for _, existing := range state.CompletedJobs {
		if existing.JobID == trimmedJob && existing.AssignmentID == trimmedAssignment {
			continue
		}
		next = append(next, existing)
	}
	state.CompletedJobs = next
	return r.persistReconnectState(state)
}

func (r *workerRunner) flushPendingReconnectResults(ctx context.Context) error {
	const batchSize = 24

	for {
		r.stateMu.Lock()
		state, err := r.loadReconnectState()
		r.stateMu.Unlock()
		if err != nil {
			workerEvent("warn", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"error":     err.Error(),
			})
			return err
		}
		if len(state.CompletedJobs) == 0 {
			workerEvent("debug", "worker.reconnect_flush", map[string]any{
				"worker_id": r.workerID,
				"status":    "empty_queue",
			})
			return nil
		}

		batch := state.CompletedJobs
		if len(batch) > batchSize {
			batch = batch[:batchSize]
		}
		req := hdcf.WorkerReconnectRequest{
			WorkerID:      r.workerID,
			CompletedJobs: batch,
		}
		workerEvent("info", "worker.reconnect_flush", map[string]any{
			"worker_id": r.workerID,
			"batch":     len(batch),
			"queued":    len(state.CompletedJobs),
		})
		resp, err := r.postJSONWithResponse(ctx, r.controlURL+"/reconnect", req)
		if err != nil {
			return err
		}
		removedAny := false
		for _, action := range resp.Actions {
			actionLog := map[string]any{
				"worker_id":     r.workerID,
				"action":        action.Action,
				"job_id":        action.JobID,
				"assignment_id": action.AssignmentID,
				"result":        action.Result,
			}
			switch action.Action {
			case hdcf.ReconnectActionReplayCompleted, hdcf.ReconnectActionReplayFailed:
				if action.Result == hdcf.ReconnectResultAccepted {
					if err := r.removeCompletedReconnectResult(action.JobID, action.AssignmentID); err != nil {
						return err
					}
					actionLog["removed_local_queue"] = true
					removedAny = true
				} else {
					actionLog["removed_local_queue"] = false
				}
			case hdcf.ReconnectActionClearCurrentJob:
				if action.JobID == "" || action.JobID == strings.TrimSpace(state.CurrentJobID) {
					if err := r.clearCurrentReconnectState(); err != nil {
						return err
					}
					actionLog["current_job_cleared"] = true
				}
			}
			if action.Error != "" {
				actionLog["error"] = action.Error
			}
			workerEvent("debug", "worker.reconnect_flush_action", actionLog)
		}
		if !removedAny {
			workerEvent("info", "worker.reconnect_flush", map[string]any{
				"worker_id":       r.workerID,
				"result":          "pending_replays_rejected",
				"action_count":    len(resp.Actions),
				"remaining_count": len(state.CompletedJobs),
			})
			return nil
		}
		workerEvent("info", "worker.reconnect_flush", map[string]any{
			"worker_id":    r.workerID,
			"result":       "batch_accepted",
			"removed_any":  true,
			"action_count": len(resp.Actions),
		})
	}
}

func (r *workerRunner) handleJobFailure(ctx context.Context, job *hdcf.AssignedJob, err error) {
	workerEvent("warn", "worker.job_local_failure", map[string]any{
		"worker_id":     r.workerID,
		"job_id":        job.JobID,
		"assignment_id": job.AssignmentID,
		"error":         err.Error(),
	})
	completionSeq := r.nextCompletionSeq()
	failReq := hdcf.FailRequest{
		JobID:        job.JobID,
		WorkerID:     r.workerID,
		AssignmentID: job.AssignmentID,
		ExitCode:     -1,
		Error:        err.Error(),
	}
	reconnectEntry := hdcf.ReconnectCompletedJob{
		JobID:         failReq.JobID,
		AssignmentID:  failReq.AssignmentID,
		CompletionSeq: completionSeq,
		Status:        hdcf.StatusFailed,
		ExitCode:      failReq.ExitCode,
		Error:         failReq.Error,
	}
	if queueErr := r.enqueueCompletedReconnectResult(reconnectEntry); queueErr != nil {
		workerEvent("warn", "worker.job_reconnect_queue", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"action":        "enqueue_startup_failure",
			"error":         queueErr.Error(),
		})
	}
	if retryWithBackoff(ctx, r.requestTimeout, func() error {
		return r.reportFail(ctx, failReq)
	}, 8) == nil {
		workerEvent("info", "worker.job_reported_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"exit_code":     failReq.ExitCode,
		})
		if queueErr := r.removeCompletedReconnectResult(job.JobID, job.AssignmentID); queueErr != nil {
			workerEvent("warn", "worker.job_reported_fail", map[string]any{
				"worker_id": r.workerID,
				"job_id":    job.JobID,
				"error":     queueErr.Error(),
			})
		}
	} else {
		workerEvent("warn", "worker.job_reported_fail", map[string]any{
			"worker_id":     r.workerID,
			"job_id":        job.JobID,
			"assignment_id": job.AssignmentID,
			"error":         "report_fail_retries_exhausted",
		})
	}
}

func (r *workerRunner) cleanupLogArtifacts() error {
	if r.logRetentionDays <= 0 {
		return nil
	}
	cutoff := time.Now().Add(-time.Duration(r.logRetentionDays) * 24 * time.Hour).Unix()
	entries, err := os.ReadDir(r.logDir)
	if err != nil {
		return err
	}
	currentJob := ""
	if v := r.getCurrentJobID(); v != nil {
		currentJob = strings.TrimSpace(*v)
	}

	var deleted, failed int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isWorkerLogArtifact(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			failed++
			continue
		}
		if info.ModTime().Unix() >= cutoff {
			continue
		}
		if currentJob != "" && strings.HasPrefix(name, currentJob+".") {
			continue
		}
		if err := os.Remove(filepath.Join(r.logDir, name)); err != nil {
			failed++
			continue
		}
		deleted++
	}
	if deleted > 0 || failed > 0 {
		workerEvent("info", "worker.log_cleanup", map[string]any{
			"worker_id":        r.workerID,
			"retention_days":   r.logRetentionDays,
			"deleted":          deleted,
			"failed":           failed,
			"candidate_cutoff": cutoff,
		})
	}
	return nil
}

func isWorkerLogArtifact(name string) bool {
	return strings.HasSuffix(name, ".stdout.log") ||
		strings.HasSuffix(name, ".stderr.log") ||
		strings.HasSuffix(name, ".stdout.log.tmp") ||
		strings.HasSuffix(name, ".stderr.log.tmp")
}

func retryWithBackoff(ctx context.Context, base time.Duration, fn func() error, attempts int) error {
	delay := base
	maxDelay := 30 * time.Second
	for i := 0; i < attempts; i++ {
		if err := fn(); err == nil {
			return nil
		} else if i == attempts-1 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jitterDuration(delay)):
			delay = nextBackoff(delay, maxDelay)
		}
	}
	return nil
}

func extractExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int63n(int64(d/5) + 1))
	return d + jitter
}

func readLimitedBody(r io.Reader) string {
	const limit = 512
	buf := make([]byte, limit)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return ""
	}
	return strings.TrimSpace(string(buf[:n]))
}

const (
	authHeader     = "X-API-Token"
	auditComponent = "worker_daemon"
)

func workerEvent(level, event string, fields map[string]any) {
	payload := map[string]any{
		"ts":        time.Now().Format(time.RFC3339Nano),
		"component": auditComponent,
		"level":     level,
		"event":     event,
	}
	for k, v := range fields {
		payload[k] = v
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to marshal worker event: %v", err)
		return
	}
	log.Printf("%s", string(raw))
}
