#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_PATH="$2"
fi

PG_CONTAINER="$(awk '
  $1 == "services:" {in_services = 1; next}
  /^[^[:space:]]/ {in_services = 0}
  in_services && $1 == "docker:" {in_docker = 1; next}
  in_services && in_docker && $1 == "postgres_container:" {print $2; exit}
' "$CONFIG_PATH")"

if docker inspect -f '{{json .HostConfig.Tmpfs}}' "$PG_CONTAINER" | grep -q '/var/lib/postgresql/data'; then
  echo "Refusing to restart tmpfs-backed PostgreSQL container $PG_CONTAINER; recreate it without tmpfs for benchmark runs" >&2
  exit 1
fi

docker restart "$PG_CONTAINER" >/dev/null
echo "Restarted $PG_CONTAINER"
