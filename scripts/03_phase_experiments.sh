#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$ROOT_DIR/_run_cli.sh" run-postgres-baseline "$@"
bash "$ROOT_DIR/_run_cli.sh" run-neo4j-baseline "$@"
bash "$ROOT_DIR/_run_cli.sh" compare-engines "$@"
bash "$ROOT_DIR/_run_cli.sh" run-join-order "$@"
