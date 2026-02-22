#!/usr/bin/env bash

set -euo pipefail

ACTION="${1:-}"
if [[ -n "${ACTION}" ]]; then
  shift
fi
ROLE="${HDCF_LAUNCHD_ROLE:-all}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${HDCF_PROJECT_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
LAUNCHD_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${HDCF_LAUNCHD_LOG_DIR:-${HOME}/Library/Logs/hdcf}"
DOMAIN="gui/$(id -u)"
CONTROL_LABEL="com.hdcf.control"
WORKER_LABEL="com.hdcf.worker"
CONTROL_PLIST="${LAUNCHD_DIR}/${CONTROL_LABEL}.plist"
WORKER_PLIST="${LAUNCHD_DIR}/${WORKER_LABEL}.plist"
CONTROL_BINARY="${HDCF_CONTROL_BINARY:-${PROJECT_ROOT}/bin/hdcf-control}"
WORKER_BINARY="${HDCF_WORKER_BINARY:-${PROJECT_ROOT}/bin/hdcf-worker}"
CONTROL_CONFIG="${HDCF_CONTROL_CONFIG:-${PROJECT_ROOT}/deploy/control-config.json}"
WORKER_CONFIG="${HDCF_WORKER_CONFIG:-${PROJECT_ROOT}/deploy/worker-config.json}"

usage() {
  cat <<'USAGE'
Usage:
  launchd-entropy.sh install [--role control|worker|all]
  launchd-entropy.sh uninstall [--role control|worker|all]
  launchd-entropy.sh status

Environment:
  HDCF_PROJECT_ROOT         path to entropy repo (default repo root)
  HDCF_CONTROL_BINARY       absolute path to compiled control binary
  HDCF_WORKER_BINARY        absolute path to compiled worker binary
  HDCF_CONTROL_CONFIG       absolute path to control config file
  HDCF_WORKER_CONFIG        absolute path to worker config file
  HDCF_LAUNCHD_LOG_DIR     launch log directory (default ~/Library/Logs/hdcf)
  HDCF_LAUNCHD_ROLE         default role for install/uninstall (default all)

Examples:
  # mac-mini (control + worker):
  HDCF_CONTROL_BINARY=/usr/local/bin/hdcf-control \
  HDCF_WORKER_BINARY=/usr/local/bin/hdcf-worker \
  HDCF_CONTROL_CONFIG=/opt/hdcf/control-config.json \
  HDCF_WORKER_CONFIG=/opt/hdcf/worker-on-mini-config.json \
  ./launchd-entropy.sh install --role all

  # ASUS (worker only):
  HDCF_WORKER_BINARY=/usr/local/bin/hdcf-worker \
  HDCF_WORKER_CONFIG=/opt/hdcf/worker-on-asus-config.json \
  ./launchd-entropy.sh install --role worker
USAGE
}

require() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "missing required tool: $name" >&2
    exit 1
  fi
}

require_file() {
  local target="$1"
  if [[ ! -f "$target" ]]; then
    echo "required file missing: $target" >&2
    exit 1
  fi
}

ensure_context() {
  require_file "$PROJECT_ROOT/go.mod"
  mkdir -p "$LAUNCHD_DIR" "$LOG_DIR"
}

write_control_plist() {
  cat >"$CONTROL_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${CONTROL_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${CONTROL_BINARY}</string>
    <string>-config</string>
    <string>${CONTROL_CONFIG}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${PROJECT_ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/hdcf-control.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/hdcf-control.err.log</string>
</dict>
</plist>
PLIST
}

write_worker_plist() {
  cat >"$WORKER_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${WORKER_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${WORKER_BINARY}</string>
    <string>-config</string>
    <string>${WORKER_CONFIG}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${PROJECT_ROOT}</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/hdcf-worker.out.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/hdcf-worker.err.log</string>
</dict>
</plist>
PLIST
}

bootstrap_plist() {
  local plist="$1"
  local label
  label="$(/usr/libexec/PlistBuddy -c "Print :Label" "$plist")"
  launchctl print "${DOMAIN}/${label}" >/dev/null 2>&1 && launchctl bootout "${DOMAIN}/${label}" >/dev/null 2>&1 || true
  if ! launchctl bootstrap "$DOMAIN" "$plist" 2>/dev/null; then
    launchctl load "$plist"
  fi
  echo "started ${label}"
}

bootout_plist() {
  local plist="$1"
  local label
  label="$(/usr/libexec/PlistBuddy -c "Print :Label" "$plist")"
  if launchctl print "${DOMAIN}/${label}" >/dev/null 2>&1; then
    launchctl bootout "${DOMAIN}/${label}" || launchctl unload "$plist"
    echo "stopped ${label}"
  else
    echo "${label} was not running"
  fi
}

status_label() {
  local label="$1"
  if launchctl print "${DOMAIN}/${label}" >/dev/null 2>&1; then
    launchctl print "${DOMAIN}/${label}"
  else
    echo "${label}: not loaded"
  fi
}

case "${ACTION}" in
  install)
    while [[ $# -gt 0 ]]; do
      case "${1}" in
        --role)
          ROLE="${2:?--role requires all/control/worker}"
          shift 2
          ;;
        --help|-h)
          usage
          exit 0
          ;;
        *)
          usage
          exit 1
          ;;
      esac
    done

    require "launchctl"
    require "/usr/libexec/PlistBuddy"
    ensure_context

    case "${ROLE}" in
      control|worker|all)
        ;;
      *)
        echo "invalid role: ${ROLE}" >&2
        exit 1
        ;;
    esac

    if [[ "$ROLE" == "control" || "$ROLE" == "all" ]]; then
      require_file "$CONTROL_BINARY"
      require_file "$CONTROL_CONFIG"
      write_control_plist
      bootstrap_plist "$CONTROL_PLIST"
    fi

    if [[ "$ROLE" == "worker" || "$ROLE" == "all" ]]; then
      require_file "$WORKER_BINARY"
      require_file "$WORKER_CONFIG"
      write_worker_plist
      bootstrap_plist "$WORKER_PLIST"
    fi

    echo "launchd services configured."
    echo "edit logs in ${LOG_DIR}"
    ;;
  uninstall)
    while [[ $# -gt 0 ]]; do
      case "${1}" in
        --role)
          ROLE="${2:?--role requires all/control/worker}"
          shift 2
          ;;
        --help|-h)
          usage
          exit 0
          ;;
        *)
          usage
          exit 1
          ;;
      esac
    done

    case "${ROLE}" in
      control|worker|all)
        ;;
      *)
        echo "invalid role: ${ROLE}" >&2
        exit 1
        ;;
    esac

    if [[ "$ROLE" == "control" || "$ROLE" == "all" ]]; then
      [[ -f "$CONTROL_PLIST" ]] && bootout_plist "$CONTROL_PLIST"
      rm -f "$CONTROL_PLIST"
    fi

    if [[ "$ROLE" == "worker" || "$ROLE" == "all" ]]; then
      [[ -f "$WORKER_PLIST" ]] && bootout_plist "$WORKER_PLIST"
      rm -f "$WORKER_PLIST"
    fi

    echo "launchd services removed."
    ;;
  status)
    status_label "$CONTROL_LABEL"
    status_label "$WORKER_LABEL"
    ;;
  ""|help|-h|--help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
