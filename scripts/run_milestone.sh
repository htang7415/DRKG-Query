#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$ROOT_DIR/00_setup_env.sh" "$@"
bash "$ROOT_DIR/_run_cli.sh" preprocess "$@"
bash "$ROOT_DIR/_run_cli.sh" load-postgres "$@"
bash "$ROOT_DIR/_run_cli.sh" mine-milestone-templates "$@"
bash "$ROOT_DIR/_run_cli.sh" sample-bindings "$@"
bash "$ROOT_DIR/_run_cli.sh" run-postgres-baseline "$@"
bash "$ROOT_DIR/_run_cli.sh" write-milestone-report "$@"
