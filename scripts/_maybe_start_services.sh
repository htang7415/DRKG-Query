#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_PATH="$2"
fi

SERVICE_MODE="$(awk '
  $1 == "services:" {in_services = 1; next}
  /^[^[:space:]]/ {in_services = 0}
  in_services && $1 == "mode:" {print $2; exit}
' "$CONFIG_PATH")"
AUTO_START="$(awk '
  $1 == "services:" {in_services = 1; next}
  /^[^[:space:]]/ {in_services = 0}
  in_services && $1 == "auto_start:" {print $2; exit}
' "$CONFIG_PATH")"

SERVICE_MODE="${SERVICE_MODE:-manual}"
AUTO_START="${AUTO_START:-false}"

if [[ "$SERVICE_MODE" == "docker" && "$AUTO_START" == "true" ]]; then
  bash "$ROOT/scripts/dev_start_services_docker.sh" --config "$CONFIG_PATH"
fi
