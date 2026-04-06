#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/_config_arg.sh"
CONFIG_PATH="$(resolve_config_path "$ROOT" "$@")"

NEO_CONTAINER="$(awk '
  $1 == "services:" {in_services = 1; next}
  /^[^[:space:]]/ {in_services = 0}
  in_services && $1 == "docker:" {in_docker = 1; next}
  in_services && in_docker && $1 == "neo4j_container:" {print $2; exit}
' "$CONFIG_PATH")"

HAS_TMPFS=false
if docker inspect -f '{{json .HostConfig.Tmpfs}}' "$NEO_CONTAINER" | grep -q '"/data"'; then
  HAS_TMPFS=true
fi

if docker exec "$NEO_CONTAINER" bash -lc 'command -v /var/lib/neo4j/bin/neo4j >/dev/null 2>&1 && /var/lib/neo4j/bin/neo4j restart >/dev/null 2>&1'; then
  echo "Restarted Neo4j inside $NEO_CONTAINER via neo4j restart"
  exit 0
fi

if [[ "$HAS_TMPFS" == "true" ]]; then
  echo "Refusing to docker-restart tmpfs-backed Neo4j container $NEO_CONTAINER; recreate it without tmpfs for benchmark runs" >&2
  exit 1
fi

docker restart "$NEO_CONTAINER" >/dev/null
echo "Restarted $NEO_CONTAINER"
