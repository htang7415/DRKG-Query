#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$ROOT_DIR/_run_cli.sh" compute-theory "$@"
bash "$ROOT_DIR/_run_cli.sh" postprocess "$@"
bash "$ROOT_DIR/_run_cli.sh" make-figures "$@"
