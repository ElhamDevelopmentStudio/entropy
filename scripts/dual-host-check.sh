#!/usr/bin/env bash
set -euo pipefail

SIM_DIR=/tmp/hdcf-e2e
BASE="http://127.0.0.1:8091"
TOKEN="dev-token"
DB_PATH="${SIM_DIR}/jobs-$(date +%s%N).db"
CTRL_BIN="${SIM_DIR}/control.bin"
WORKER_BIN="${SIM_DIR}/worker.bin"
HDCFCTL_BIN="${SIM_DIR}/hdcfctl.bin"
CONTROL_PID=""
MAC_PID=""
LINUX_PID=""

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
cleanup() {
  for pid in "$CONTROL_PID" "$MAC_PID" "$LINUX_PID"; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

wait_http() {
  local url=$1
  for _ in $(seq 1 120); do
    if curl -fsS "$url" >/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

submit_job() {
  local cmd="$1" args="$2"
  "$HDCFCTL_BIN" submit --url "$BASE" --token "$TOKEN" --command "$cmd" --args "$args" --max-attempts 3 | sed -n 's/job_id=\([^ ]*\).*/\1/p'
}

job_json() {
  "$HDCFCTL_BIN" jobs describe --url "$BASE" --token "$TOKEN" "$1"
}
job_field() {
  job_json "$1" | jq -r ".${2}"
}

wait_until_status() {
  local job="$1" want="$2" timeout="${3:-120}"
  for _ in $(seq 1 "$timeout"); do
    st=$(job_field "$job" status)
    if [[ "$st" == "$want" ]]; then
      echo "$st"
      return 0
    fi
    sleep 1
  done
  echo "TIMEOUT"
  return 1
}

wait_terminal() {
  local job="$1" timeout="${2:-120}"
  for _ in $(seq 1 "$timeout"); do
    st=$(job_field "$job" status)
    case "$st" in
      COMPLETED|FAILED|ABORTED)
        echo "$st"
        return 0
        ;;
    esac
    sleep 1
  done
  echo "TIMEOUT"
  return 1
}

wait_claimed() {
  local job="$1" timeout="${2:-120}"
  for _ in $(seq 1 "$timeout"); do
    st=$(job_field "$job" status)
    wid=$(job_field "$job" worker_id)
    aid=$(job_field "$job" assignment_id)
    if { [[ "$st" == "ASSIGNED" || "$st" == "RUNNING" ]]; } && [[ -n "$wid" && "$wid" != "null" ]] && [[ -n "$aid" && "$aid" != "null" ]]; then
      echo "$st $wid $aid"
      return 0
    fi
    if [[ "$st" == "FAILED" || "$st" == "COMPLETED" || "$st" == "ABORTED" ]]; then
      echo "$st"
      return 2
    fi
    sleep 1
  done
  echo "TIMEOUT"
  return 1
}

wait_workers_online() {
  for _ in $(seq 1 60); do
    online=$(curl -fsS -H "X-API-Token: ${TOKEN}" "$BASE/workers" | jq -r 'map(select(.status=="ONLINE")) | length')
    if [[ "$online" == "2" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

kill_worker() {
  local label="$1"
  local var_name="$2"
  local pid
  if [[ -n "$var_name" ]]; then
    pid="${!var_name}"
  else
    pid=""
  fi
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    log "killed $label pid=$pid"
    if declare -p "$var_name" >/dev/null 2>&1; then
      printf -v "$var_name" ''
    fi
  fi
}

mkdir -p "$SIM_DIR/logs"

log "start control db=$DB_PATH"
$CTRL_BIN -addr :8091 -db "$DB_PATH" -admin-token "$TOKEN" -worker-token "$TOKEN" -heartbeat-timeout-seconds 8 -reconcile-interval-seconds 2 > "$SIM_DIR/logs/control.log" 2>&1 &
CONTROL_PID=$!
if ! wait_http "$BASE/healthz"; then
  echo "control failed to come up" >&2
  exit 1
fi

log "start mac-mini and asus-linux workers"
$WORKER_BIN -control-url "$BASE" -worker-id mac-mini -token "$TOKEN" -poll-interval-seconds 1 -heartbeat-interval-seconds 2 -log-dir "$SIM_DIR/mac-logs" > "$SIM_DIR/logs/worker-mac.log" 2>&1 &
MAC_PID=$!
$WORKER_BIN -control-url "$BASE" -worker-id asus-linux -token "$TOKEN" -poll-interval-seconds 1 -heartbeat-interval-seconds 2 -log-dir "$SIM_DIR/linux-logs" > "$SIM_DIR/logs/worker-linux.log" 2>&1 &
LINUX_PID=$!

wait_workers_online || { echo "workers not online" >&2; exit 1; }
log "workers are online"

# baseline
log "baseline queue"
B1=$(submit_job echo "hello")
B2=$(submit_job sleep 1)
B3=$(submit_job sleep 1)
B4=$(submit_job echo "done")
for id in "$B1" "$B2" "$B3" "$B4"; do
  status=$(wait_terminal "$id" 120)
  echo "baseline $id -> $status"
done

# recovery scenario
log "recovery scenario"
kill_worker "mac-mini" MAC_PID
J5=$(submit_job sleep 30)
log "submitted J5 $J5"
if ! claim=$(wait_claimed "$J5" 120); then
  echo "failed to get J5 claim" >&2
  exit 1
fi
if [[ "$claim" != *"asus-linux"* ]]; then
  echo "expected J5 to be on asus-linux, got '$claim'" >&2
  exit 1
fi
kill_worker "asus-linux" LINUX_PID
if [[ "$(wait_until_status "$J5" LOST 30)" != "LOST" ]]; then
  echo "J5 not LOST (got $(job_field "$J5" status))" >&2
  exit 1
fi

log "restarting asus-linux"
$WORKER_BIN -control-url "$BASE" -worker-id asus-linux -token "$TOKEN" -poll-interval-seconds 1 -heartbeat-interval-seconds 2 -log-dir "$SIM_DIR/linux-logs" > "$SIM_DIR/logs/worker-linux.log" 2>&1 &
LINUX_PID=$!
if ! wait_claimed "$J5" 120 >/tmp/hdcf-e2e/j5_claim2.txt; then
  echo "J5 not claimed after restart" >&2
  cat /tmp/hdcf-e2e/j5_claim2.txt
  exit 1
fi
if ! wait_terminal "$J5" 160 >/tmp/hdcf-e2e/j5_wait.txt; then
  echo "J5 did not finish" >&2
  cat /tmp/hdcf-e2e/j5_wait.txt
  exit 1
fi
log "J5 final: $(job_json "$J5" | jq -r '"\(.status) attempts=\(.attempt_count)/\(.max_attempts) error=\(.last_error)"')"

# duplicate completion replay
log "duplicate completion"
J6=$(submit_job bash "-c,sleep 30")
claim=$(wait_claimed "$J6" 120)
if [[ "$claim" == TIMEOUT || "$claim" == FAILED || "$claim" == COMPLETED || "$claim" == ABORTED ]]; then
  echo "J6 not claimable: $claim" >&2
  exit 1
fi
read -r _ J6_WORKER J6_ASSIGNMENT <<< "$claim"
STD_TMP="$SIM_DIR/j6.stdout.tmp"
STD_FINAL="$SIM_DIR/j6.stdout"
ERR_TMP="$SIM_DIR/j6.stderr.tmp"
ERR_FINAL="$SIM_DIR/j6.stderr"
printf 'out' > "$STD_TMP"
printf 'err' > "$ERR_TMP"

payload=$(cat <<JSON
{
  "job_id":"$J6",
  "worker_id":"$J6_WORKER",
  "assignment_id":"$J6_ASSIGNMENT",
  "artifact_id":"dup-check-6",
  "exit_code":0,
  "stdout_path":"$STD_FINAL",
  "stderr_path":"$ERR_FINAL",
  "stdout_tmp_path":"$STD_TMP",
  "stderr_tmp_path":"$ERR_TMP",
  "stdout_sha256":"",
  "stderr_sha256":"",
  "result_summary":"dup replay check",
  "completion_seq":1
}
JSON
)

rc1=$(curl -sS -w '%{http_code}' -o /tmp/hdcf-e2e/dup1.json -X POST -H "X-API-Token: ${TOKEN}" -H "Content-Type: application/json" -d "$payload" "$BASE/complete")
rc2=$(curl -sS -w '%{http_code}' -o /tmp/hdcf-e2e/dup2.json -X POST -H "X-API-Token: ${TOKEN}" -H "Content-Type: application/json" -d "$payload" "$BASE/complete")
echo "duplicate response: $rc1/$rc2"
echo "dup1 $(cat /tmp/hdcf-e2e/dup1.json)"
echo "dup2 $(cat /tmp/hdcf-e2e/dup2.json)"

if ! wait_terminal "$J6" 120; then
  echo "J6 not terminal" >&2
  exit 1
fi
log "J6 final: $(job_json "$J6" | jq -r '"\(.status) attempts=\(.attempt_count)/\(.max_attempts) error=\(.last_error)"')"

# control restart smoke
log "control restart smoke"
kill "$CONTROL_PID"
$CTRL_BIN -addr :8091 -db "$DB_PATH" -admin-token "$TOKEN" -worker-token "$TOKEN" -heartbeat-timeout-seconds 8 -reconcile-interval-seconds 2 > "$SIM_DIR/logs/control.log" 2>&1 &
CONTROL_PID=$!
wait_http "$BASE/healthz" || { echo "control restart failed" >&2; exit 1; }
log "post-restart jobs count $($HDCFCTL_BIN jobs list --url "$BASE" --token "$TOKEN" | jq 'length')"

$HDCFCTL_BIN workers list --url "$BASE" --token "$TOKEN" | jq -c '.[] | "\(.worker_id) \(.status) \(.current_job_id|tostring)"'
$HDCFCTL_BIN jobs list --url "$BASE" --token "$TOKEN" | jq -c '.[] | "\(.job_id) \(.status) \(.worker_id) \(.attempt_count)/\(.max_attempts)"'

log "dual-host simulation done"
