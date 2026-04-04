#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PHASES=(
  "01_phase_setup.sh:setup"
  "02_phase_prepare.sh:prepare"
  "03_phase_experiments.sh:experiments"
  "04_phase_analysis.sh:analysis"
  "05_phase_finalize.sh:finalize"
)

for item in "${PHASES[@]}"; do
  phase_script="${item%%:*}"
  phase_name="${item##*:}"
  printf '[drkg-bench] phase: %s\n' "$phase_name" >&2
  bash "$ROOT_DIR/$phase_script" "$@"
done
