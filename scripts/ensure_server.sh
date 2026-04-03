#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -P "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="${CREATURE_OS_WORKSPACE_ROOT:-$PROJECT_ROOT}"
HEALTH_URL="${CREATURE_OS_URL:-http://localhost:404/healthz}"
PROCESS_PATTERN="creatureos.cli serve"
PYTHON_BIN="${CREATURE_OS_PYTHON_BIN:-${PYTHON_BIN:-python3}}"
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
if [[ -n "${CREATURE_OS_DATA_DIR:-}" ]]; then
  DATA_DIR="$CREATURE_OS_DATA_DIR"
else
  DATA_DIR="$("$PYTHON_BIN" - <<'PY'
from creatureos import config
print(config.data_dir())
PY
)"
  export CREATURE_OS_DATA_DIR="$DATA_DIR"
fi
if [[ -n "${CREATURE_OS_DB_PATH:-}" ]]; then
  DB_PATH="$CREATURE_OS_DB_PATH"
else
  DB_PATH="$("$PYTHON_BIN" - <<'PY'
from creatureos import config
print(config.db_path())
PY
)"
  export CREATURE_OS_DB_PATH="$DB_PATH"
fi
LOCK_FILE="$DATA_DIR/server_watchdog.lock"
PID_FILE="$DATA_DIR/server.pid"
mkdir -p "$DATA_DIR"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  exit 0
fi

log() {
  printf '%s %s\n' "$(date -Is)" "$*" >>"$DATA_DIR/watchdog.log"
}

current_source_revision() {
  "$PYTHON_BIN" - <<'PY'
from creatureos import config
print(config.server_source_revision())
PY
}

healthy() {
  local body current_revision
  body="$(curl -fsS --max-time 15 "$HEALTH_URL" 2>/dev/null || true)"
  [[ "$body" == *'"status":"ok"'* || "$body" == *'"status": "ok"'* ]] || return 1
  current_revision="$(current_source_revision)"
  PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}" "$PYTHON_BIN" - <<'PY' "$body" "$current_revision"
import json
import sys

body = sys.argv[1]
current_revision = sys.argv[2]
try:
    payload = json.loads(body)
except Exception:
    raise SystemExit(1)
server_revision = str(payload.get("source_revision") or "").strip()
if str(payload.get("status") or "").strip().lower() != "ok":
    raise SystemExit(1)
if not server_revision or server_revision != current_revision:
    raise SystemExit(1)
raise SystemExit(0)
PY
}

stop_server() {
  local pid=""
  if [[ -f "$PID_FILE" ]]; then
    pid="$("$PYTHON_BIN" - <<'PY' "$PID_FILE"
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
print(int(payload.get("pid") or 0))
PY
)"
  fi

  if [[ "$pid" =~ ^[0-9]+$ ]] && ((pid > 0)) && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    return 0
  fi

  mapfile -t pids < <(pgrep -f "$PROCESS_PATTERN" || true)
  if ((${#pids[@]} == 0)); then
    return 0
  fi
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1
  mapfile -t survivors < <(pgrep -f "$PROCESS_PATTERN" || true)
  if ((${#survivors[@]} > 0)); then
    kill -9 "${survivors[@]}" 2>/dev/null || true
  fi
}

start_server() {
  (
    export CREATURE_OS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
    export CREATURE_OS_DATA_DIR="$DATA_DIR"
    export CREATURE_OS_DB_PATH="$DB_PATH"
    export CREATURE_OS_SERVE_TAILSCALE="${CREATURE_OS_SERVE_TAILSCALE:-1}"
    exec 9>&-
    cmd=(
      "$PYTHON_BIN"
      -m
      creatureos.cli
      --workspace
      "$WORKSPACE_ROOT"
      --data-dir
      "$DATA_DIR"
      --db-path
      "$DB_PATH"
      serve
    )
    if [[ "${CREATURE_OS_SERVE_TAILSCALE:-1}" == "1" ]]; then
      cmd+=("--tailscale")
    fi
    if command -v setsid >/dev/null 2>&1; then
      setsid "${cmd[@]}" >>"$DATA_DIR/server.log" 2>&1 < /dev/null &
    else
      nohup "${cmd[@]}" >>"$DATA_DIR/server.log" 2>&1 < /dev/null &
    fi
  )
}

if healthy; then
  exit 0
fi

log "health check failed or live source revision drifted; restarting creatureos"
stop_server
start_server

for _ in 1 2 3 4 5 6 7 8 9 10; do
  sleep 1
  if healthy; then
    log "restart succeeded"
    exit 0
  fi
done

log "restart failed"
exit 1
