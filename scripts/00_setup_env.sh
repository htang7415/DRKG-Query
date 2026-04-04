#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_PATH="$2"
fi

VENV_DIR="$(awk '
  $1 == "paths:" {in_paths = 1; next}
  /^[^[:space:]]/ {in_paths = 0}
  in_paths && $1 == "venv_dir:" {print $2; exit}
' "$CONFIG_PATH")"
ENV_DIR="$(awk '
  $1 == "paths:" {in_paths = 1; next}
  /^[^[:space:]]/ {in_paths = 0}
  in_paths && $1 == "env_dir:" {print $2; exit}
' "$CONFIG_PATH")"

if [[ -z "$VENV_DIR" || -z "$ENV_DIR" ]]; then
  echo "failed to read paths.venv_dir or paths.env_dir from $CONFIG_PATH" >&2
  exit 1
fi

if [[ ! -f "$VENV_DIR/bin/python" ]]; then
  python3 -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/drkg_bench_mplconfig}"
mkdir -p "$MPLCONFIGDIR"

if ! python - <<'PY' >/dev/null 2>&1
import yaml
import psycopg
import neo4j
import matplotlib
import networkx
import numpy
import pulp
PY
then
  python -m pip install -r "$ROOT/requirements.txt"
fi

mkdir -p "$ROOT/$ENV_DIR"
python -m pip freeze > "$ROOT/$ENV_DIR/requirements_lock.txt"
{
  echo "config=$CONFIG_PATH"
  echo "venv=$VENV_DIR"
  echo "python=$(python --version 2>&1)"
  echo "pip=$(python -m pip --version)"
} > "$ROOT/$ENV_DIR/setup.log"
