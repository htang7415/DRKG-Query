#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_PATH="$2"
fi

eval "$(python - <<'PY' "$CONFIG_PATH"
import sys
from pathlib import Path
import yaml

config_path = Path(sys.argv[1]).resolve()
with config_path.open("r", encoding="utf-8") as handle:
    cfg = yaml.safe_load(handle)

svc = cfg["services"]["docker"]
print(f"PG_CONTAINER={svc['postgres_container']!r}")
print(f"NEO_CONTAINER={svc['neo4j_container']!r}")
PY
)"

docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
docker stop "$NEO_CONTAINER" >/dev/null 2>&1 || true

echo "Stopped Docker services:"
echo "  $PG_CONTAINER"
echo "  $NEO_CONTAINER"
