#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -P "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="${CREATURE_OS_WORKSPACE_ROOT:-$PROJECT_ROOT}"
PYTHON_BIN="${CREATURE_OS_PYTHON_BIN:-${PYTHON_BIN:-python3}}"

cd "$PROJECT_ROOT"
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
export CREATURE_OS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
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
mkdir -p "$DATA_DIR"
exec "$PYTHON_BIN" -m creatureos.cli run-due >>"$DATA_DIR/runner.log" 2>&1
