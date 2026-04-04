#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$ROOT_DIR/_run_cli.sh" preprocess "$@"
bash "$ROOT_DIR/_run_cli.sh" load-postgres "$@"
bash "$ROOT_DIR/_run_cli.sh" mine-templates "$@"
bash "$ROOT_DIR/_run_cli.sh" sample-bindings "$@"
bash "$ROOT_DIR/_run_cli.sh" load-neo4j "$@"
