#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
COMMAND=""
SKIP_NEXT=false
for ARG in "$@"; do
  if [[ "$SKIP_NEXT" == "true" ]]; then
    CONFIG_PATH="$ARG"
    SKIP_NEXT=false
    continue
  fi
  if [[ "$ARG" == "--config" ]]; then
    SKIP_NEXT=true
    continue
  fi
  if [[ -z "$COMMAND" ]]; then
    COMMAND="$ARG"
  fi
done

VENV_DIR="$(awk '
  $1 == "paths:" {in_paths = 1; next}
  /^[^[:space:]]/ {in_paths = 0}
  in_paths && $1 == "venv_dir:" {print $2; exit}
' "$CONFIG_PATH")"

if [[ -z "$VENV_DIR" ]]; then
  echo "failed to read paths.venv_dir from $CONFIG_PATH" >&2
  exit 1
fi

if [[ -f "$VENV_DIR/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
fi
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/drkg_bench_mplconfig}"
mkdir -p "$MPLCONFIGDIR"

case "$COMMAND" in
  load-postgres|load-neo4j|run-postgres-baseline|run-neo4j-baseline|run-join-order|compute-theory)
    bash "$ROOT/scripts/_maybe_start_services.sh" --config "$CONFIG_PATH"
    ;;
esac

case "$COMMAND" in
  load-postgres|run-postgres-baseline|run-join-order|compute-theory)
    python - <<'PY' "$CONFIG_PATH"
from drkg_bench.common import load_context
from drkg_bench.postgres import wait_for_postgres

ctx = load_context(__import__("sys").argv[1])
wait_for_postgres(ctx)
PY
    ;;
  load-neo4j|run-neo4j-baseline)
    python - <<'PY' "$CONFIG_PATH"
from drkg_bench.common import load_context
from drkg_bench.neo4j_db import wait_for_neo4j

ctx = load_context(__import__("sys").argv[1])
wait_for_neo4j(ctx)
PY
    ;;
esac

python -m drkg_bench.cli "$@"
