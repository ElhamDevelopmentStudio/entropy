#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${HDCF_TEST_URL:-http://localhost:8080}"
TOKEN="${HDCF_TEST_TOKEN:-dev-token}"

usage() {
  cat <<'USAGE'
Usage:
  reliability-scenarios.sh duplicate-completion
  reliability-scenarios.sh scenario-info
  reliability-scenarios.sh status <job_id>

Requires:
  Control plane running at HDCF_TEST_URL
  X-API-Token sent via HDCF_TEST_TOKEN
USAGE
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

api_get() {
  curl -sS -H "X-API-Token: ${TOKEN}" "$@"
}

api_post() {
  curl -sS -X POST \
    -H "X-API-Token: ${TOKEN}" \
    -H "Content-Type: application/json" \
    "$@"
}

extract() {
  local key="$1"
  local json="$2"
  printf '%s' "${json}" | sed -E 's/.*"'${key}'"\s*:\s*"([^"]*)".*/\1/p'
}

submit_sleep_job() {
  local command="$1"
  local args="$2"
  local output
  output=$(go run ./cmd/hdcfctl submit \
    --url "${BASE_URL}" \
    --token "${TOKEN}" \
    --command "${command}" \
    --args "${args}" \
    --max-attempts 2)
  printf '%s\n' "${output}"
  printf '%s\n' "${output}" | sed -n 's/job_id=\([^ ]*\) status=.*/\1/p'
}

wait_for_running_state() {
  local job_id="$1"
  local attempts=0
  while (( attempts < 20 )); do
    local job_body
    job_body="$(api_get "${BASE_URL}/jobs/${job_id}")"
    local status
    status="$(extract status "${job_body}")"
    if [[ "${status}" == "RUNNING" ]]; then
      echo "${job_body}"
      return 0
    fi
    if [[ "${status}" == "COMPLETED" || "${status}" == "FAILED" || "${status}" == "ABORTED" ]]; then
      echo "${job_body}"
      return 1
    fi
    sleep 2
    ((attempts += 1))
  done
  return 2
}

scenario_duplicate_completion() {
  local job_id
  job_id="$(submit_sleep_job "bash" '-c sleep 30' | tail -n 1)"
  echo "Started job ${job_id}"
  local job_state
  if ! job_state="$(wait_for_running_state "${job_id}")"; then
    echo "Job not in RUNNING state in time" >&2
    exit 1
  fi

  local assignment_id
  local worker_id
  assignment_id="$(extract assignment_id "${job_state}")"
  worker_id="$(extract worker_id "${job_state}")"

  if [[ -z "${assignment_id}" || -z "${worker_id}" ]]; then
    echo "Missing RUNNING assignment context" >&2
    exit 1
  fi

  local payload='{'
  payload+=' "job_id":"'"${job_id}"'",'
  payload+=' "worker_id":"'"${worker_id}"'",'
  payload+=' "assignment_id":"'"${assignment_id}"'",'
  payload+=' "artifact_id":"scenario-e-replay",'
  payload+=' "exit_code":0,'
  payload+=' "stdout_tmp_path":"/tmp/hdcf_stdout.tmp",'
  payload+=' "stdout_path":"/tmp/hdcf_stdout.txt",'
  payload+=' "stderr_tmp_path":"/tmp/hdcf_stderr.tmp",'
  payload+=' "stderr_path":"/tmp/hdcf_stderr.txt",'
  payload+=' "stdout_sha256":"",'
  payload+=' "stderr_sha256":"",'
  payload+=' "result_summary":"duplicate completion replay",'
  payload+=' "completion_seq":0'
  payload+='}'

  echo "Replaying completion twice for ${job_id}..."
  api_post "${BASE_URL}/complete" -d "${payload}"
  api_post "${BASE_URL}/complete" -d "${payload}"

  echo "Final job state:"
  api_get "${BASE_URL}/jobs/${job_id}"
}

scenario_info() {
  cat <<'INFO'
Scenarios not fully automated:
  scenario-router-drop
  scenario-worker-kill
  scenario-power-cycle-worker
  scenario-cp-restart

Use SRS_TEST_SCENARIOS.md for manual execution instructions and expected state transitions.
INFO
}

case "${1}" in
  duplicate-completion)
    scenario_duplicate_completion
    ;;
  scenario-info)
    scenario_info
    ;;
  status)
    if [[ $# -lt 2 ]]; then
      echo "status requires job_id" >&2
      exit 1
    fi
    api_get "${BASE_URL}/jobs/${2}"
    ;;
  *)
    usage
    exit 1
    ;;
esac
