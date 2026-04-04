#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$ROOT_DIR/_run_cli.sh" build-final-package "$@"
bash "$ROOT_DIR/_run_cli.sh" verify-results "$@"
