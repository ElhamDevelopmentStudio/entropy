package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
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

	s, err := store.Open(cfg.dbPath, cfg.heartbeatTimeout, store.StoreOptions{
		QueueAgingWindowSeconds:            cfg.queueAgingWindowSeconds,
		MaxConcurrentRetriesPerWorker:      cfg.maxConcurrentRetriesPerWorker,
		PreemptBacklogThreshold:            cfg.preemptBacklogThreshold,
		PreemptHighPriorityMinimumPriority: cfg.preemptHighPriorityMinimumPriority,
	})
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	auditEvent("info", "control.startup", "", map[string]any{
		"addr":                          cfg.addr,
		"db_path":                       cfg.dbPath,
		"heartbeat_timeout":             int(cfg.heartbeatTimeout.Seconds()),
		"reconcile_interval":            int(cfg.reconcileInterval.Seconds()),
		"cleanup_interval":              int(cfg.cleanupInterval.Seconds()),
		"queue_aging_window_seconds":    cfg.queueAgingWindowSeconds,
		"max_retry_per_worker":          cfg.maxConcurrentRetriesPerWorker,
		"preempt_backlog_threshold":     cfg.preemptBacklogThreshold,
		"preempt_priority_floor":        cfg.preemptHighPriorityMinimumPriority,
		"jobs_retention_completed_days": cfg.jobsRetentionCompletedDays,
		"artifacts_retention_days":      cfg.artifactsRetentionDays,
		"events_retention_days":         cfg.eventsRetentionDays,
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
	go runCleanup(ctx, s, cfg.cleanupInterval, cfg.jobsRetentionCompletedDays, cfg.artifactsRetentionDays, cfg.eventsRetentionDays)

	mux := http.NewServeMux()
	mux.HandleFunc("/ui", withUIAuth(&cfg, dashboardUI()))
	mux.HandleFunc("/ui/", withUIAuth(&cfg, dashboardUI()))
	mux.HandleFunc("/healthz", healthzHandler(s, cfg.dbPath))
	mux.HandleFunc("/jobs", withAdminAuth(&cfg, jobsHandler(s)))
	mux.HandleFunc("/jobs/", withAdminAuth(&cfg, getJobHandler(s)))
	mux.HandleFunc("/metrics", withAdminAuth(&cfg, metricsHandler(s)))
	mux.HandleFunc("/register", withWorkerAuth(&cfg, registerWorker(&cfg, s)))
	mux.HandleFunc("/next-job", withWorkerAuth(&cfg, nextJob(&cfg, s)))
	mux.HandleFunc("/ack", withWorkerAuth(&cfg, ackJob(&cfg, s)))
	mux.HandleFunc("/heartbeat", withWorkerAuth(&cfg, heartbeat(&cfg, s)))
	mux.HandleFunc("/reconnect", withWorkerAuth(&cfg, reconnectWorker(&cfg, s)))
	mux.HandleFunc("/abort", withAdminAuth(&cfg, abortJob(s)))
	mux.HandleFunc("/workers", withAdminAuth(&cfg, listWorkers(s)))
	mux.HandleFunc("/events", withAdminAuth(&cfg, listEvents(s)))
	mux.HandleFunc("/complete", withWorkerAuth(&cfg, completeJob(&cfg, s)))
	mux.HandleFunc("/fail", withWorkerAuth(&cfg, failJob(&cfg, s)))

	auditEvent("info", "control.listen", "", map[string]any{
		"addr":  cfg.addr,
		"db":    cfg.dbPath,
		"state": "starting",
		"tls":   cfg.tlsCert != "" && cfg.tlsKey != "",
	})
	server := &http.Server{
		Addr:    cfg.addr,
		Handler: mux,
	}
	if cfg.tlsCert != "" || cfg.tlsKey != "" {
		if cfg.tlsCert == "" || cfg.tlsKey == "" {
			log.Fatalf("both -tls-cert and -tls-key are required for TLS mode")
		}
		tlsConfig, err := buildControlTLSConfig(cfg)
		if err != nil {
			log.Fatalf("failed to configure TLS: %v", err)
		}
		server.TLSConfig = tlsConfig
		auditEvent("info", "control.listen", "", map[string]any{
			"addr":     cfg.addr,
			"protocol": "https",
		})
		if err := server.ListenAndServeTLS(cfg.tlsCert, cfg.tlsKey); err != nil {
			log.Fatalf("https server exited: %v", err)
		}
		return
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("http server exited: %v", err)
	}
}

func dashboardUI() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ui" && r.URL.Path != "/ui/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(dashboardHTML))
	}
}

const dashboardHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>HDCF Dashboard</title>
    <style>
      :root {
        --bg: #0e1424;
        --panel: #1a2340;
        --panel-2: #243059;
        --text: #edf2ff;
        --muted: #96a0bf;
        --ok: #38c172;
        --warn: #f59f00;
        --bad: #f06548;
      }
      body {
        margin: 0;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        color: var(--text);
        background: linear-gradient(145deg, #0a0f1f, #101f3d);
      }
      .page {
        max-width: 1100px;
        margin: 0 auto;
        padding: 20px;
      }
      .grid {
        display: grid;
        gap: 14px;
        grid-template-columns: 1fr;
      }
      .card {
        background: linear-gradient(180deg, var(--panel), var(--panel-2));
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 14px;
        box-shadow: 0 12px 26px rgba(0,0,0,0.2);
      }
      h1, h2 {
        margin: 0 0 10px 0;
      }
      .row {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }
      label {
        display: inline-flex;
        align-items: center;
        gap: 6px;
      }
      input, select, button {
        border: 1px solid rgba(255,255,255,0.15);
        border-radius: 8px;
        padding: 8px 10px;
        background: #141c35;
        color: var(--text);
      }
      button {
        cursor: pointer;
      }
      button.warn { background: #6b1f1a; border-color: #9f2e1c; }
      button:disabled { opacity: .5; cursor: not-allowed; }
      table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 10px;
      }
      th, td {
        border-bottom: 1px solid rgba(255,255,255,0.09);
        text-align: left;
        font-size: 13px;
        padding: 8px;
        vertical-align: top;
      }
      th {
        color: #cbd4f0;
        font-weight: 600;
      }
      .pill {
        display: inline-block;
        border-radius: 999px;
        padding: 2px 8px;
        border: 1px solid rgba(255,255,255,0.2);
        font-size: 12px;
      }
      .pill.ok { background: rgba(56,193,114,0.18); color: #a8f7c4; }
      .pill.warn { background: rgba(245,159,0,0.18); color: #ffe6b3; }
      .pill.bad { background: rgba(240,101,72,0.22); color: #ffd3c8; }
      .mono {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      }
      .help {
        color: var(--muted);
        font-size: 12px;
      }
      .flex {
        display: flex;
      }
      .flex-grow { flex: 1; }
      #status {
        margin-left: 10px;
        font-size: 12px;
        color: var(--muted);
      }
      .small {
        font-size: 12px;
        color: var(--muted);
      }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="grid">
        <section class="card">
          <h1>HDCF Dashboard</h1>
          <div class="row">
            <label>
              API Token
              <input id="token" type="password" placeholder="X-API-Token" value="" />
            </label>
            <label>
              Job status
              <select id="jobStatus">
                <option value="">All</option>
                <option value="PENDING">PENDING</option>
                <option value="ASSIGNED">ASSIGNED</option>
                <option value="RUNNING">RUNNING</option>
                <option value="COMPLETED">COMPLETED</option>
                <option value="FAILED">FAILED</option>
                <option value="LOST">LOST</option>
                <option value="RETRYING">RETRYING</option>
                <option value="ABORTED">ABORTED</option>
              </select>
            </label>
            <label>
              Refresh (ms)
              <select id="refreshMs">
                <option value="1000">1000</option>
                <option value="2000">2000</option>
                <option value="3000" selected>3000</option>
                <option value="5000">5000</option>
              </select>
            </label>
            <button id="saveToken">Save</button>
            <button id="refreshNow">Refresh now</button>
          </div>
          <div class="row">
            <div id="status" class="help">waiting for auth...</div>
            <div id="nextRefresh" class="help"></div>
          </div>
        </section>

        <section class="card">
          <div class="row">
            <h2>Jobs</h2>
          </div>
          <div id="jobsError" class="small help"></div>
          <table>
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Status</th>
                <th>Command</th>
                <th>Worker / Assoc</th>
                <th>Attempts</th>
                <th>Updated</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody id="jobsRows"></tbody>
          </table>
        </section>

        <section class="card">
          <h2>Workers</h2>
          <div id="workersError" class="small help"></div>
          <table>
            <thead>
              <tr>
                <th>Worker ID</th>
                <th>Status</th>
                <th>Current Job</th>
                <th>Heartbeat Age</th>
                <th>Capabilities</th>
                <th>Metrics</th>
              </tr>
            </thead>
            <tbody id="workersRows"></tbody>
          </table>
        </section>

        <section class="card">
          <h2>Recent Events</h2>
          <div id="eventsError" class="small help"></div>
          <table>
            <thead>
              <tr>
                <th>When</th>
                <th>Component</th>
                <th>Event</th>
                <th>Level</th>
                <th>Job / Worker</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody id="eventsRows"></tbody>
          </table>
        </section>
      </div>
    </div>
    <script>
      const STORAGE_KEY = 'hdcf-ui-token';
      const tokenInput = document.getElementById('token');
      const jobStatus = document.getElementById('jobStatus');
      const refreshMs = document.getElementById('refreshMs');
      const statusEl = document.getElementById('status');
      const nextRefreshEl = document.getElementById('nextRefresh');
      const jobsRows = document.getElementById('jobsRows');
      const workersRows = document.getElementById('workersRows');
      const eventsRows = document.getElementById('eventsRows');
      const jobsError = document.getElementById('jobsError');
      const workersError = document.getElementById('workersError');
      const eventsError = document.getElementById('eventsError');
      let timer = null;

      function state() {
        return {
          token: tokenInput.value.trim() || localStorage.getItem(STORAGE_KEY) || '',
          statusFilter: jobStatus.value
        };
      }

      function apiHeaders() {
        const token = state().token;
        if (!token) return null;
        return {
          'Content-Type': 'application/json',
          'X-API-Token': token
        };
      }

      async function callApi(path, init = {}) {
        const headers = apiHeaders() || {'Content-Type': 'application/json'};
        const res = await fetch(path, {
          ...init,
          headers: {
            ...headers,
            ...(init.headers || {}),
          },
        });
        const text = await res.text();
        let payload = null;
        try { payload = text ? JSON.parse(text) : null; } catch (_) {}
        return { ok: res.ok, status: res.status, payload, raw: text };
      }

      function formatEpoch(sec) {
        if (!sec && sec !== 0) return '';
        const d = new Date((Number(sec) || 0) * 1000);
        return d.toLocaleString();
      }

      function formatAge(sec) {
        if (sec == null) return 'n/a';
        return sec + 's';
      }

      function escapeHtml(value) {
        return String(value == null ? '' : value)
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;')
          .replaceAll('"', '&quot;')
          .replaceAll("'", '&#39;')
          .replaceAll(String.fromCharCode(96), '&#96;');
      }

      function shortId(id) {
        if (!id) return '';
        return id.length > 10 ? id.slice(0, 8) + '…' : id;
      }

      function pillClass(status) {
        switch (status) {
          case 'COMPLETED': return 'ok';
          case 'RUNNING':
          case 'ASSIGNED': return 'warn';
          case 'FAILED':
          case 'ABORTED':
          case 'LOST': return 'bad';
          default: return '';
        }
      }

      function statusText(j) {
        if (!j.status) return '';
        const age = j.heartbeat_age_sec == null ? '' : ', heartbeat ' + formatAge(j.heartbeat_age_sec);
        return '' + j.status + age;
      }

      function metricsText(metrics) {
        const parts = [];
        if (metrics.cpu_usage_percent != null) {
          parts.push('CPU: ' + Number(metrics.cpu_usage_percent).toFixed(1) + '%');
        }
        if (metrics.memory_usage_mb != null) {
          parts.push('RAM: ' + Number(metrics.memory_usage_mb).toFixed(1) + 'MB');
        }
        if (metrics.gpu_usage_percent != null) {
          parts.push('GPU: ' + Number(metrics.gpu_usage_percent).toFixed(1) + '%');
        }
        if (metrics.gpu_memory_usage_mb != null) {
          parts.push('VRAM: ' + Number(metrics.gpu_memory_usage_mb).toFixed(1) + 'MB');
        }
        return parts.length === 0 ? '-' : parts.join(' • ');
      }

      function renderJobs(jobs) {
        jobsRows.innerHTML = jobs.map(j => {
          const terminal = ['COMPLETED', 'FAILED', 'ABORTED'].includes(j.status);
          const action = terminal ? '' : '<button class="warn" data-job-id="' + escapeHtml(j.job_id) + '" onclick="abortJob(this.dataset.jobId)">Abort</button>';
          const assignment = j.assignment_id ? shortId(j.assignment_id) : '';
          const worker = j.worker_id || '-';
          const workerCell = escapeHtml(worker) + (assignment ? ' <span>/ ' + escapeHtml(assignment) + '</span>' : '');
          return '<tr>' +
            '<td class="mono">' + escapeHtml(shortId(j.job_id)) + '</td>' +
            '<td><span class="pill ' + pillClass(j.status) + '">' + escapeHtml(statusText(j)) + '</span></td>' +
            '<td><div>' + escapeHtml(j.command || '') + '</div><div class="small mono">' + escapeHtml((j.args || []).join(' ')) + '</div></td>' +
            '<td class="small mono">' + workerCell + '</td>' +
            '<td>' + escapeHtml(j.attempt_count) + '/' + escapeHtml(j.max_attempts) + '</td>' +
            '<td><div>' + formatEpoch(j.updated_at) + '</div><div class="small">' + escapeHtml(j.last_error ? ('err: ' + j.last_error) : '') + '</div></td>' +
            '<td>' + action + '</td>' +
          '</tr>';
        }).join('');
      }

      function renderWorkers(workers) {
        workersRows.innerHTML = workers.map(w => {
          const metrics = w.heartbeat_metrics || {};
          return '<tr>' +
            '<td class="mono">' + escapeHtml(w.worker_id) + '</td>' +
            '<td class="pill ' + (w.status === 'ONLINE' ? 'ok' : 'bad') + '">' + escapeHtml(w.status) + '</td>' +
            '<td class="mono">' + escapeHtml(w.current_job_id || '-') + '</td>' +
            '<td>' + (w.heartbeat_age_sec == null ? 'n/a' : formatAge(w.heartbeat_age_sec)) + '</td>' +
            '<td class="small mono">' + escapeHtml((w.capabilities || []).join(', ') || '-') + '</td>' +
            '<td class="small mono">' + escapeHtml(metricsText(metrics)) + '</td>' +
          '</tr>';
        }).join('');
      }

      function renderEvents(events) {
        eventsRows.innerHTML = events.map(ev => {
          const details = ev.details ? JSON.stringify(ev.details) : '';
          return '<tr>' +
            '<td>' + formatEpoch(ev.ts) + '</td>' +
            '<td>' + escapeHtml(ev.component || '') + '</td>' +
            '<td>' + escapeHtml(ev.event || '') + '</td>' +
            '<td>' + escapeHtml(ev.level || '') + '</td>' +
            '<td class="small mono">' + escapeHtml((ev.job_id ? ev.job_id : '') + (ev.worker_id ? ' / ' + ev.worker_id : '')) + '</td>' +
            '<td class="small mono">' + escapeHtml(details) + '</td>' +
          '</tr>';
        }).join('');
      }

      async function abortJob(jobId) {
        if (!jobId) return;
        const token = state().token;
        if (!token) {
          setStatus('save token first');
          return;
        }
        const resp = await callApi('/abort', {
          method: 'POST',
          body: JSON.stringify({ job_id: jobId })
        });
        if (!resp.ok) {
          alert('Abort failed: ' + (resp.raw || 'error'));
          return;
        }
        await refreshAll();
      }

      function setStatus(msg) {
        statusEl.textContent = msg;
      }

      async function refreshAll() {
        if (!state().token) {
          setStatus('set token to query API');
          return;
        }
        const filters = state().statusFilter ? '?status=' + encodeURIComponent(state().statusFilter) : '';
        const [jobsResp, workersResp, eventsResp] = await Promise.all([
          callApi('/jobs' + filters),
          callApi('/workers'),
          callApi('/events?limit=20')
        ]);

        jobsError.textContent = workersError.textContent = eventsError.textContent = '';
        let allOk = true;

        if (!jobsResp.ok) {
          allOk = false;
          jobsError.textContent = 'jobs: ' + jobsResp.status + ' ' + (jobsResp.raw || '');
        } else {
          renderJobs(jobsResp.payload || []);
        }

        if (!workersResp.ok) {
          allOk = false;
          workersError.textContent = 'workers: ' + workersResp.status + ' ' + (workersResp.raw || '');
        } else {
          renderWorkers(workersResp.payload || []);
        }

        if (!eventsResp.ok) {
          allOk = false;
          eventsError.textContent = 'events: ' + eventsResp.status + ' ' + (eventsResp.raw || '');
        } else {
          renderEvents(eventsResp.payload || []);
        }

        setStatus(allOk ? 'last refresh ' + new Date().toLocaleTimeString() : 'refresh errors');
      }

      function startTicker() {
        if (timer) clearInterval(timer);
        const interval = Number(refreshMs.value || 3000);
        timer = setInterval(async () => {
          const next = Number(interval) / 1000;
          nextRefreshEl.textContent = 'next in ' + next.toFixed(1) + 's';
          await refreshAll();
        }, interval);
      }

      async function onSaveToken() {
        localStorage.setItem(STORAGE_KEY, tokenInput.value.trim());
        setStatus('token saved');
        await refreshAll();
      }

      async function init() {
        const queryToken = new URLSearchParams(window.location.search).get('token');
        if (queryToken) {
          tokenInput.value = queryToken;
          localStorage.setItem(STORAGE_KEY, queryToken);
          const current = new URL(window.location.href);
          current.searchParams.delete('token');
          window.history.replaceState({}, '', current.toString());
        }
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) tokenInput.value = saved;
        setStatus('ready');
        document.getElementById('saveToken').addEventListener('click', onSaveToken);
        document.getElementById('refreshNow').addEventListener('click', refreshAll);
        refreshMs.addEventListener('change', startTicker);
        jobStatus.addEventListener('change', refreshAll);
        setInterval(() => {
          const next = Number(refreshMs.value || 3000) / 1000;
          nextRefreshEl.textContent = 'next in ' + next.toFixed(1) + 's';
        }, 1000);
        startTicker();
        await refreshAll();
      }

      init();
    </script>
  </body>
</html>`

type controlConfig struct {
	addr                               string
	dbPath                             string
	adminToken                         string
	adminTokenPrev                     string
	workerToken                        string
	workerTokenPrev                    string
	workerTokenSecret                  string
	workerTokenTTL                     time.Duration
	tlsCert                            string
	tlsKey                             string
	tlsClientCA                        string
	tlsRequireClientCert               bool
	heartbeatTimeout                   time.Duration
	reconcileInterval                  time.Duration
	cleanupInterval                    time.Duration
	queueAgingWindowSeconds            int64
	maxConcurrentRetriesPerWorker      int
	preemptBacklogThreshold            int
	preemptHighPriorityMinimumPriority int
	jobsRetentionCompletedDays         int
	artifactsRetentionDays             int
	eventsRetentionDays                int
}

type controlConfigFile struct {
	Addr                         *string `json:"addr"`
	DBPath                       *string `json:"db_path"`
	Token                        *string `json:"token"`
	AdminToken                   *string `json:"admin_token"`
	AdminTokenPrev               *string `json:"admin_token_prev"`
	WorkerToken                  *string `json:"worker_token"`
	WorkerTokenPrev              *string `json:"worker_token_prev"`
	WorkerTokenSecret            *string `json:"worker_token_secret"`
	WorkerTokenTTLSeconds        *int64  `json:"worker_token_ttl_seconds"`
	TLSCert                      *string `json:"tls_cert"`
	TLSKey                       *string `json:"tls_key"`
	TLSClientCA                  *string `json:"tls_client_ca"`
	TLSRequireClientCert         *bool   `json:"tls_require_client_cert"`
	HeartbeatTimeoutSeconds      *int64  `json:"heartbeat_timeout_seconds"`
	ReconcileIntervalSeconds     *int64  `json:"reconcile_interval_seconds"`
	CleanupIntervalSeconds       *int64  `json:"cleanup_interval_seconds"`
	QueueAgingWindowSeconds      *int64  `json:"queue_aging_window_seconds"`
	MaxRetryConcurrencyPerWorker *int64  `json:"max_retry_concurrency_per_worker"`
	PreemptBacklogThreshold      *int64  `json:"preempt_high_priority_backlog_threshold"`
	PreemptPriorityFloor         *int64  `json:"preempt_high_priority_floor"`
	JobsRetentionCompletedDays   *int64  `json:"jobs_retention_completed_days"`
	ArtifactsRetentionDays       *int64  `json:"artifacts_retention_days"`
	EventsRetentionDays          *int64  `json:"events_retention_days"`
}

type controlHealthCheck struct {
	Status  string                    `json:"status"`
	Checked string                    `json:"checked_at"`
	Checks  map[string]map[string]any `json:"checks"`
}

func parseConfig() controlConfig {
	var cfg controlConfig
	cfgFile := controlLoadConfig(controlConfigPathFromArgs())
	defaultAddr := resolveConfigString(cfgFile.Addr, "HDCF_ADDR", ":8080")
	defaultDBPath := resolveConfigString(cfgFile.DBPath, "HDCF_DB_PATH", "jobs.db")
	defaultToken := resolveConfigString(cfgFile.Token, "HDCF_API_TOKEN", "dev-token")
	defaultAdminToken := resolveConfigString(cfgFile.AdminToken, "HDCF_ADMIN_TOKEN", "")
	defaultAdminTokenPrev := resolveConfigString(cfgFile.AdminTokenPrev, "HDCF_ADMIN_TOKEN_PREV", "")
	defaultWorkerToken := resolveConfigString(cfgFile.WorkerToken, "HDCF_WORKER_TOKEN", "")
	defaultWorkerTokenPrev := resolveConfigString(cfgFile.WorkerTokenPrev, "HDCF_WORKER_TOKEN_PREV", "")
	defaultWorkerTokenSecret := resolveConfigString(cfgFile.WorkerTokenSecret, "HDCF_WORKER_TOKEN_SECRET", "")
	defaultTLSCert := resolveConfigString(cfgFile.TLSCert, "HDCF_TLS_CERT", "")
	defaultTLSKey := resolveConfigString(cfgFile.TLSKey, "HDCF_TLS_KEY", "")
	defaultTLSClientCA := resolveConfigString(cfgFile.TLSClientCA, "HDCF_TLS_CLIENT_CA", "")
	defaultTLSRequireClientCert := resolveConfigBool(cfgFile.TLSRequireClientCert, "HDCF_TLS_REQUIRE_CLIENT_CERT", false)
	var legacyToken string
	flag.StringVar(&cfg.addr, "addr", defaultAddr, "control plane listen addr")
	flag.StringVar(&cfg.dbPath, "db", defaultDBPath, "sqlite db path")
	flag.StringVar(&legacyToken, "token", defaultToken, "legacy shared token for admin and worker endpoints")
	flag.StringVar(&cfg.adminToken, "admin-token", defaultAdminToken, "admin token (overrides -token)")
	flag.StringVar(&cfg.adminTokenPrev, "admin-token-prev", defaultAdminTokenPrev, "previous admin token (for rotation)")
	flag.StringVar(&cfg.workerToken, "worker-token", defaultWorkerToken, "worker token (overrides -token)")
	flag.StringVar(&cfg.workerTokenPrev, "worker-token-prev", defaultWorkerTokenPrev, "previous worker token (for rotation)")
	flag.StringVar(&cfg.workerTokenSecret, "worker-token-secret", defaultWorkerTokenSecret, "secret for short-lived signed worker tokens")
	var workerTokenTTLSeconds int64
	flag.Int64Var(&workerTokenTTLSeconds, "worker-token-ttl-seconds", resolveConfigInt64(cfgFile.WorkerTokenTTLSeconds, "HDCF_WORKER_TOKEN_TTL_SECONDS", 3600), "signed worker token TTL in seconds")
	flag.StringVar(&cfg.tlsCert, "tls-cert", defaultTLSCert, "TLS certificate PEM for https control API")
	flag.StringVar(&cfg.tlsKey, "tls-key", defaultTLSKey, "TLS private key PEM for https control API")
	flag.StringVar(&cfg.tlsClientCA, "tls-client-ca", defaultTLSClientCA, "CA certificate PEM to verify worker client certs when mTLS is enabled")
	flag.BoolVar(&cfg.tlsRequireClientCert, "tls-require-client-cert", defaultTLSRequireClientCert, "require and verify client certificates")
	var heartbeatSec int64
	var reconcileSec int64
	var cleanupIntervalSec int64
	var queueAgingWindowSeconds int64
	var maxConcurrentRetriesPerWorker int64
	var preemptBacklogThreshold int64
	var preemptHighPriorityMinimumPriority int64
	var jobsRetentionCompletedDays int64
	var artifactsRetentionDays int64
	var eventsRetentionDays int64
	flag.Int64Var(&heartbeatSec, "heartbeat-timeout-seconds", resolveConfigInt64(cfgFile.HeartbeatTimeoutSeconds, "HDCF_HEARTBEAT_TIMEOUT_SECONDS", 60), "heartbeat timeout seconds")
	flag.Int64Var(&reconcileSec, "reconcile-interval-seconds", resolveConfigInt64(cfgFile.ReconcileIntervalSeconds, "HDCF_RECONCILE_INTERVAL_SECONDS", 10), "reconcile interval seconds")
	flag.Int64Var(&cleanupIntervalSec, "cleanup-interval-seconds", resolveConfigInt64(cfgFile.CleanupIntervalSeconds, "HDCF_CLEANUP_INTERVAL_SECONDS", 300), "cleanup interval seconds")
	flag.Int64Var(&queueAgingWindowSeconds, "queue-aging-window-seconds", resolveConfigInt64(cfgFile.QueueAgingWindowSeconds, "HDCF_QUEUE_AGING_WINDOW_SECONDS", 0), "aging window seconds for priority-based fairness")
	flag.Int64Var(&maxConcurrentRetriesPerWorker, "max-retry-concurrency-per-worker", resolveConfigInt64(cfgFile.MaxRetryConcurrencyPerWorker, "HDCF_MAX_RETRY_CONCURRENCY_PER_WORKER", 0), "max concurrent retry jobs per worker (0=unlimited)")
	flag.Int64Var(&preemptBacklogThreshold, "preempt-high-priority-backlog-threshold", resolveConfigInt64(cfgFile.PreemptBacklogThreshold, "HDCF_PREEMPT_HIGH_PRIORITY_BACKLOG_THRESHOLD", 0), "if high-priority backlog exceeds threshold, temporarily gate lower priority jobs")
	flag.Int64Var(&preemptHighPriorityMinimumPriority, "preempt-high-priority-floor", resolveConfigInt64(cfgFile.PreemptPriorityFloor, "HDCF_PREEMPT_HIGH_PRIORITY_FLOOR", 0), "minimum priority during preemption mode")
	flag.Int64Var(&jobsRetentionCompletedDays, "jobs-retention-completed-days", resolveConfigInt64(cfgFile.JobsRetentionCompletedDays, "HDCF_JOBS_RETENTION_COMPLETED_DAYS", 30), "days to retain terminal jobs in sqlite")
	flag.Int64Var(&artifactsRetentionDays, "artifacts-retention-days", resolveConfigInt64(cfgFile.ArtifactsRetentionDays, "HDCF_ARTIFACTS_RETENTION_DAYS", 14), "days to retain terminal artifact/log files (cleanup does not block scheduling)")
	flag.Int64Var(&eventsRetentionDays, "events-retention-days", resolveConfigInt64(cfgFile.EventsRetentionDays, "HDCF_EVENTS_RETENTION_DAYS", 30), "days to retain audit events")
	flag.String("config", controlConfigPathFromArgs(), "path to control-plane JSON config file (defaults to HDCF_CONTROL_CONFIG)")
	flag.Parse()
	if strings.TrimSpace(cfg.adminToken) == "" {
		cfg.adminToken = legacyToken
	}
	if strings.TrimSpace(cfg.workerToken) == "" && strings.TrimSpace(cfg.workerTokenSecret) == "" {
		log.Fatalf("worker authentication token required: set -worker-token or -worker-token-secret (or config equivalents)")
	}
	if workerTokenTTLSeconds < 1 {
		workerTokenTTLSeconds = 1
	}
	cfg.heartbeatTimeout = time.Duration(heartbeatSec) * time.Second
	cfg.reconcileInterval = time.Duration(reconcileSec) * time.Second
	cfg.cleanupInterval = time.Duration(cleanupIntervalSec) * time.Second
	cfg.queueAgingWindowSeconds = queueAgingWindowSeconds
	cfg.maxConcurrentRetriesPerWorker = int(maxConcurrentRetriesPerWorker)
	cfg.preemptBacklogThreshold = int(preemptBacklogThreshold)
	cfg.preemptHighPriorityMinimumPriority = int(preemptHighPriorityMinimumPriority)
	cfg.jobsRetentionCompletedDays = int(jobsRetentionCompletedDays)
	cfg.artifactsRetentionDays = int(artifactsRetentionDays)
	cfg.eventsRetentionDays = int(eventsRetentionDays)
	cfg.workerTokenTTL = time.Duration(workerTokenTTLSeconds) * time.Second
	if cfg.tlsRequireClientCert && strings.TrimSpace(cfg.tlsClientCA) == "" {
		log.Fatalf("tls-client-ca is required when tls-require-client-cert is enabled")
	}
	return cfg
}

func controlConfigPathFromArgs() string {
	path := strings.TrimSpace(os.Getenv("HDCF_CONTROL_CONFIG"))
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

func controlLoadConfig(path string) controlConfigFile {
	if strings.TrimSpace(path) == "" {
		return controlConfigFile{}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read control config: %v", err)
	}
	var cfg controlConfigFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse control config: %v", err)
	}
	return cfg
}

func resolveConfigString(raw *string, envName, fallback string) string {
	if raw != nil && strings.TrimSpace(*raw) != "" {
		return strings.TrimSpace(*raw)
	}
	return getenvDefault(envName, fallback)
}

func resolveConfigInt64(raw *int64, envName string, fallback int64) int64 {
	if raw != nil {
		return *raw
	}
	return getenvInt(envName, fallback)
}

func resolveConfigBool(raw *bool, envName string, fallback bool) bool {
	if raw != nil {
		return *raw
	}
	envRaw := strings.TrimSpace(os.Getenv(envName))
	if envRaw == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(envRaw)
	if err != nil {
		return fallback
	}
	return parsed
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

func withAdminAuth(cfg *controlConfig, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := cfg.authorizeAdminRequest(getAdminTokenFromRequest(r)); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func withUIAuth(cfg *controlConfig, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := getUIAdminTokenFromRequest(r)
		if strings.TrimSpace(token) != "" && cfg.authorizeAdminRequest(token) == nil {
			http.SetCookie(w, &http.Cookie{
				Name:     "hdcf_admin_token",
				Value:    token,
				Path:     "/",
				HttpOnly: true,
				MaxAge:   86400,
				SameSite: http.SameSiteLaxMode,
				Secure:   r.TLS != nil,
			})
		}
		if err := cfg.authorizeAdminRequest(token); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func getAdminTokenFromRequest(r *http.Request) string {
	if token := strings.TrimSpace(r.Header.Get(authHeaderName)); token != "" {
		return token
	}
	if cookie, err := r.Cookie("hdcf_admin_token"); err == nil {
		return strings.TrimSpace(cookie.Value)
	}
	return ""
}

func getUIAdminTokenFromRequest(r *http.Request) string {
	if token := getAdminTokenFromRequest(r); token != "" {
		return token
	}
	return strings.TrimSpace(r.URL.Query().Get("token"))
}

func withWorkerAuth(cfg *controlConfig, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), ""); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (cfg *controlConfig) authorizeAdminRequest(token string) error {
	if cfg.adminToken == "" {
		return nil
	}
	if tokenMatches(token, cfg.adminToken) {
		return nil
	}
	if cfg.adminTokenPrev != "" && tokenMatches(token, cfg.adminTokenPrev) {
		return nil
	}
	return fmt.Errorf("invalid admin token")
}

func (cfg *controlConfig) authorizeWorkerRequest(token string, workerID string) error {
	if tokenMatches(token, cfg.workerToken) {
		return nil
	}
	if cfg.workerTokenPrev != "" && tokenMatches(token, cfg.workerTokenPrev) {
		return nil
	}
	if cfg.workerTokenSecret != "" {
		return cfg.validateWorkerSignedToken(token, workerID)
	}
	return fmt.Errorf("invalid worker token")
}

func (cfg *controlConfig) validateWorkerSignedToken(token, workerID string) error {
	payload, sig, err := parseWorkerSignedToken(token)
	if err != nil {
		return err
	}
	if tokenMatches(sig, generateWorkerSignedTokenSig(cfg.workerTokenSecret, payload)) {
		parts := strings.SplitN(payload, "|", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid worker token payload")
		}
		expiresUnix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid worker token expiry")
		}
		if time.Now().Unix() > expiresUnix {
			return fmt.Errorf("expired worker token")
		}
		if workerID != "" && strings.TrimSpace(parts[0]) != workerID {
			return fmt.Errorf("worker token mismatch")
		}
		return nil
	}
	return fmt.Errorf("invalid worker token signature")
}

func parseWorkerSignedToken(token string) (string, string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return "", "", fmt.Errorf("invalid worker token format")
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid worker token payload")
	}
	return strings.TrimSpace(string(payloadRaw)), strings.TrimSpace(parts[2]), nil
}

func generateWorkerSignedTokenSig(secret string, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func buildControlTLSConfig(cfg controlConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if cfg.tlsClientCA == "" {
		if cfg.tlsRequireClientCert {
			return nil, fmt.Errorf("tls-client-ca required when tls-require-client-cert is enabled")
		}
		return tlsConfig, nil
	}
	caPem, err := os.ReadFile(cfg.tlsClientCA)
	if err != nil {
		return nil, fmt.Errorf("read tls client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("failed to parse tls client ca")
	}
	tlsConfig.ClientCAs = pool
	if cfg.tlsRequireClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return tlsConfig, nil
}

func tokenMatches(provided, expected string) bool {
	if strings.TrimSpace(expected) == "" || strings.TrimSpace(provided) == "" {
		return false
	}
	if len(provided) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
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
			"command":  req.Command,
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
					"status_code":   http.StatusInternalServerError,
					"status_filter": status,
					"worker_filter": workerID,
					"error":         err.Error(),
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

func metricsHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			auditEvent("warn", "control.metrics", requestID, map[string]any{
				"status_code": http.StatusMethodNotAllowed,
				"error":       "method not allowed",
			})
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		metrics, err := s.GetMetrics(ctx)
		if err != nil {
			auditEvent("warn", "control.metrics", requestID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.metrics", requestID, map[string]any{
			"status_code":        http.StatusOK,
			"workers_total":      metrics.WorkersTotal,
			"workers_online":     metrics.WorkersOnline,
			"workers_offline":    metrics.WorkersOffline,
			"retrying_jobs":      metrics.RetryingJobs,
			"lost_jobs":          metrics.LostJobs,
			"completed_last_5m":  metrics.CompletedLast5m,
			"failed_last_5m":     metrics.FailedLast5m,
			"avg_completion_sec": metrics.AvgCompletionSec,
		})
		writeJSON(w, http.StatusOK, metrics)
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
			"job_id":      job.JobID,
			"status":      job.Status,
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
		sinceID := int64(0)
		if raw := strings.TrimSpace(query.Get("since_id")); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 1 {
				auditEvent("warn", "control.events_list", requestID, map[string]any{
					"status_code": http.StatusBadRequest,
					"error":       "since_id must be a positive integer",
				})
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "since_id must be a positive integer"})
				return
			}
			sinceID = value
		}

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

		events, err := s.ListAuditEvents(ctx, component, eventName, workerID, jobID, sinceID, limit)
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

func healthzHandler(s *store.Store, dbPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDFromHTTP(r)
		ctx := store.WithRequestID(r.Context(), requestID)
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		checkedAt := time.Now().Format(time.RFC3339Nano)
		checks := map[string]map[string]any{}
		overall := "ok"

		diag, err := s.MigrationDiagnostics(ctx)
		dbCheck := map[string]any{
			"status":      "ok",
			"diagnostics": diag,
		}
		if err != nil {
			dbCheck["status"] = "unhealthy"
			dbCheck["error"] = err.Error()
			overall = "degraded"
		} else if !diag.Healthy {
			dbCheck["status"] = "degraded"
			overall = "degraded"
		}
		checks["db"] = dbCheck

		fsCheck := map[string]any{
			"path": dbPath,
		}
		if strings.TrimSpace(dbPath) == "" || strings.TrimSpace(dbPath) == ":memory:" {
			fsCheck["status"] = "ok"
			fsCheck["type"] = "memory"
		} else {
			dbDir := filepath.Dir(dbPath)
			if strings.TrimSpace(dbDir) == "" {
				dbDir = "."
			}
			tmp, err := os.CreateTemp(dbDir, ".hdcf-healthcheck-*")
			if err != nil {
				fsCheck["status"] = "unhealthy"
				fsCheck["error"] = err.Error()
				overall = "degraded"
			} else {
				path := tmp.Name()
				if err := tmp.Close(); err != nil {
					fsCheck["status"] = "unhealthy"
					fsCheck["error"] = err.Error()
					_ = os.Remove(path)
					overall = "degraded"
				} else if err := os.Remove(path); err != nil {
					fsCheck["status"] = "degraded"
					fsCheck["error"] = err.Error()
					overall = "degraded"
				} else {
					fsCheck["status"] = "ok"
					fsCheck["directory"] = dbDir
				}
			}
		}
		checks["filesystem"] = fsCheck

		resp := controlHealthCheck{
			Status:  overall,
			Checked: checkedAt,
			Checks: map[string]map[string]any{
				"db":         checks["db"],
				"filesystem": checks["filesystem"],
			},
		}
		if overall == "ok" {
			writeJSON(w, http.StatusOK, resp)
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, resp)
	}
}

func registerWorker(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.worker_register", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
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

func nextJob(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.next_job", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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
			"worker_id":             workerID,
			"job_id":                job.JobID,
			"assignment_id":         job.AssignmentID,
			"attempt_count":         job.AttemptCount,
			"status_code":           http.StatusOK,
			"transition_to":         hdcf.StatusAssigned,
			"assignment_expires_at": job.AssignmentExpiresAt,
		})
		writeJSON(w, http.StatusOK, job)
	}
}

func ackJob(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.ack", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"job_id":      strings.TrimSpace(req.JobID),
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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

func heartbeat(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.heartbeat", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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
			"status_code":    http.StatusOK,
			"worker_id":      workerID,
			"current_job_id": req.CurrentJobID,
			"timestamp":      req.Timestamp,
			"heartbeat_seq":  req.Sequence,
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

func reconnectWorker(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.reconnect", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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

func completeJob(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.complete", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"job_id":      strings.TrimSpace(req.JobID),
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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
				"status_code":   http.StatusConflict,
				"job_id":        req.JobID,
				"worker_id":     workerID,
				"assignment_id": req.AssignmentID,
				"error":         err.Error(),
			})
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		auditEvent("info", "control.complete", requestID, map[string]any{
			"status_code":    http.StatusOK,
			"job_id":         req.JobID,
			"worker_id":      workerID,
			"assignment_id":  req.AssignmentID,
			"artifact_id":    req.ArtifactID,
			"exit_code":      req.ExitCode,
			"completion_seq": req.CompletionSeq,
		})
		writeJSON(w, http.StatusOK, map[string]string{"status": hdcf.StatusCompleted, "job_id": req.JobID})
	}
}

func failJob(cfg *controlConfig, s *store.Store) http.HandlerFunc {
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
		if err := cfg.authorizeWorkerRequest(strings.TrimSpace(r.Header.Get(authHeaderName)), workerID); err != nil {
			auditEvent("warn", "control.fail", requestID, map[string]any{
				"status_code": http.StatusUnauthorized,
				"job_id":      strings.TrimSpace(req.JobID),
				"worker_id":   workerID,
				"error":       err.Error(),
			})
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
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

func runCleanup(ctx context.Context, s *store.Store, interval time.Duration, jobsRetentionDays, artifactsRetentionDays, eventsRetentionDays int) {
	if interval <= 0 {
		auditEvent("warn", "control.cleanup", "", map[string]any{
			"status":   "disabled",
			"reason":   "cleanup interval must be greater than zero",
			"interval": interval.Seconds(),
		})
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		runCleanupOnce(s, jobsRetentionDays, artifactsRetentionDays, eventsRetentionDays)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func runCleanupOnce(s *store.Store, jobsRetentionDays, artifactsRetentionDays, eventsRetentionDays int) {
	runID := hdcf.NewJobID()
	runCtx := store.WithRequestID(context.Background(), runID)
	var jobsDeleted, eventsDeleted int64
	var pathsCandidate, pathsDeleted, pathsMissing, pathsFailed int64

	if jobsRetentionDays <= 0 && artifactsRetentionDays <= 0 && eventsRetentionDays <= 0 {
		auditEvent("info", "control.cleanup", runID, map[string]any{
			"status": "skipped",
			"reason": "retention policies disabled",
		})
		return
	}

	auditEvent("info", "control.cleanup", runID, map[string]any{
		"status":                   "start",
		"jobs_retention_days":      jobsRetentionDays,
		"artifacts_retention_days": artifactsRetentionDays,
		"events_retention_days":    eventsRetentionDays,
	})

	pathSet := map[string]struct{}{}
	if jobsRetentionDays > 0 {
		artifactPaths, deletedJobs, err := s.PruneTerminalJobs(runCtx, jobsRetentionDays)
		if err != nil {
			auditEvent("error", "control.cleanup_jobs", runID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"error":       err.Error(),
			})
		} else {
			jobsDeleted = deletedJobs
			for _, path := range artifactPaths {
				path = strings.TrimSpace(path)
				if path == "" {
					continue
				}
				pathSet[path] = struct{}{}
			}
		}
	}

	if artifactsRetentionDays > 0 {
		artifactPaths, err := s.ListTerminalArtifactPaths(runCtx, artifactsRetentionDays)
		if err != nil {
			auditEvent("error", "control.cleanup_artifacts", runID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"error":       err.Error(),
			})
		} else {
			for _, path := range artifactPaths {
				path = strings.TrimSpace(path)
				if path == "" {
					continue
				}
				pathSet[path] = struct{}{}
			}
		}
	}

	deleted, missing, failed := cleanupArtifactPaths(pathSet)
	pathsCandidate = int64(len(pathSet))
	pathsDeleted = deleted
	pathsMissing = missing
	pathsFailed = failed

	if eventsRetentionDays > 0 {
		deletedEvents, err := s.CleanupOldEvents(runCtx, eventsRetentionDays)
		if err != nil {
			auditEvent("error", "control.cleanup_events", runID, map[string]any{
				"status_code": http.StatusInternalServerError,
				"error":       err.Error(),
			})
		} else {
			eventsDeleted = deletedEvents
		}
	}

	auditEvent("info", "control.cleanup", runID, map[string]any{
		"status":          "complete",
		"jobs_deleted":    jobsDeleted,
		"events_deleted":  eventsDeleted,
		"paths_candidate": pathsCandidate,
		"paths_deleted":   pathsDeleted,
		"paths_missing":   pathsMissing,
		"paths_failed":    pathsFailed,
	})

	if jobsDeleted == 0 && eventsDeleted == 0 && pathsCandidate == 0 && pathsFailed == 0 {
		return
	}
}

func cleanupArtifactPaths(paths map[string]struct{}) (int64, int64, int64) {
	var deleted, missing, failed int64
	for path := range paths {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				missing++
				continue
			}
			failed++
			continue
		}
		deleted++
	}
	return deleted, missing, failed
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
