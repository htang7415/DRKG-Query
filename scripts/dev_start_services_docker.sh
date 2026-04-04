#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="$ROOT/config.yaml"
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_PATH="$2"
fi

eval "$(python - <<'PY' "$CONFIG_PATH"
import os
import shlex
import sys
from pathlib import Path
import yaml

config_path = Path(sys.argv[1]).resolve()
with config_path.open("r", encoding="utf-8") as handle:
    cfg = yaml.safe_load(handle)

svc = cfg["services"]["docker"]
pg = cfg["postgres"]
neo = cfg["neo4j"]
enable_postgres = svc.get("enable_postgres", True)
enable_neo4j = svc.get("enable_neo4j", True)

def shell_value(value):
    return shlex.quote(str(value))

pg_password = ""
if enable_postgres:
    pg_pass_env = pg.get("password_env")
    pg_password = os.environ.get(pg_pass_env) if pg_pass_env else None
    pg_password = pg_password or pg.get("password_value")
    if not pg_password:
        raise SystemExit("Missing PostgreSQL password; set the configured env var or postgres.password_value")

neo_password = ""
if enable_neo4j:
    neo_pass_env = neo.get("password_env")
    neo_password = os.environ.get(neo_pass_env) if neo_pass_env else None
    neo_password = neo_password or neo.get("password_value")
    if not neo_password:
        raise SystemExit("Missing Neo4j password; set the configured env var or neo4j.password_value")

for key, value in {
    "ENABLE_POSTGRES": enable_postgres,
    "ENABLE_NEO4J": enable_neo4j,
    "PG_CONTAINER": svc["postgres_container"],
    "PG_IMAGE": svc["postgres_image"],
    "PG_PORT": svc["postgres_port"],
    "PG_DB": pg["database"],
    "PG_USER": pg["user"],
    "PG_PASSWORD": pg_password,
    "PG_SHARED_BUFFERS": pg.get("shared_buffers", "default"),
    "PG_WORK_MEM": pg.get("work_mem", "default"),
    "PG_MAINTENANCE_WORK_MEM": pg.get("maintenance_work_mem", "default"),
    "PG_EFFECTIVE_CACHE_SIZE": pg.get("effective_cache_size", "default"),
    "PG_MAX_PARALLEL_WORKERS_PER_GATHER": pg.get("max_parallel_workers_per_gather", 0),
    "PG_DATA_TMPFS": svc.get("postgres_data_tmpfs", False),
    "PG_DATA_TMPFS_SIZE": svc.get("postgres_data_tmpfs_size", "16g"),
    "NEO_CONTAINER": svc["neo4j_container"],
    "NEO_IMAGE": svc["neo4j_image"],
    "NEO_HTTP_PORT": svc["neo4j_http_port"],
    "NEO_BOLT_PORT": svc["neo4j_bolt_port"],
    "NEO_USER": neo["user"],
    "NEO_PASSWORD": neo_password,
    "NEO_LOAD_BATCH_SIZE": neo.get("load_batch_size", 10000),
    "NEO_HEAP_SIZE": neo.get("heap_size", "default"),
    "NEO_PAGE_CACHE_SIZE": neo.get("page_cache_size", "default"),
    "NEO_DATA_TMPFS": svc.get("neo4j_data_tmpfs", False),
    "NEO_DATA_TMPFS_SIZE": svc.get("neo4j_data_tmpfs_size", "16g"),
    "NEO_LOGS_TMPFS": svc.get("neo4j_logs_tmpfs", False),
    "NEO_LOGS_TMPFS_SIZE": svc.get("neo4j_logs_tmpfs_size", "1g"),
    "READINESS_POLL_INTERVAL_SEC": cfg.get("services", {}).get("readiness_poll_interval_sec", 1.0),
}.items():
    print(f"{key}={shell_value(value)}")
PY
)"

PG_DOCKER_ARGS=()
PG_SERVER_ARGS=()
if [[ "$PG_DATA_TMPFS" == "True" || "$PG_DATA_TMPFS" == "true" ]]; then
  PG_DOCKER_ARGS+=("--tmpfs" "/var/lib/postgresql/data:size=${PG_DATA_TMPFS_SIZE}")
fi
if [[ "$PG_SHARED_BUFFERS" != "default" ]]; then
  PG_SERVER_ARGS+=("-c" "shared_buffers=${PG_SHARED_BUFFERS}")
fi
if [[ "$PG_WORK_MEM" != "default" ]]; then
  PG_SERVER_ARGS+=("-c" "work_mem=${PG_WORK_MEM}")
fi
if [[ "$PG_MAINTENANCE_WORK_MEM" != "default" ]]; then
  PG_SERVER_ARGS+=("-c" "maintenance_work_mem=${PG_MAINTENANCE_WORK_MEM}")
fi
if [[ "$PG_EFFECTIVE_CACHE_SIZE" != "default" ]]; then
  PG_SERVER_ARGS+=("-c" "effective_cache_size=${PG_EFFECTIVE_CACHE_SIZE}")
fi
PG_SERVER_ARGS+=("-c" "max_parallel_workers_per_gather=${PG_MAX_PARALLEL_WORKERS_PER_GATHER}")

if [[ "$ENABLE_POSTGRES" == "True" || "$ENABLE_POSTGRES" == "true" ]]; then
  if ! docker inspect "$PG_CONTAINER" >/dev/null 2>&1; then
    docker run -d \
      --name "$PG_CONTAINER" \
      -e POSTGRES_DB="$PG_DB" \
      -e POSTGRES_USER="$PG_USER" \
      -e POSTGRES_PASSWORD="$PG_PASSWORD" \
      "${PG_DOCKER_ARGS[@]}" \
      -p "${PG_PORT}:5432" \
      "$PG_IMAGE" \
      "${PG_SERVER_ARGS[@]}"
  else
    docker start "$PG_CONTAINER" >/dev/null
    echo "Using existing PostgreSQL container $PG_CONTAINER; tmpfs and server memory settings only apply on container creation"
  fi
fi

NEO_DOCKER_ARGS=()
if [[ "$NEO_HEAP_SIZE" != "default" ]]; then
  NEO_DOCKER_ARGS+=("-e" "NEO4J_server_memory_heap_initial__size=${NEO_HEAP_SIZE}")
  NEO_DOCKER_ARGS+=("-e" "NEO4J_server_memory_heap_max__size=${NEO_HEAP_SIZE}")
fi
if [[ "$NEO_PAGE_CACHE_SIZE" != "default" ]]; then
  NEO_DOCKER_ARGS+=("-e" "NEO4J_server_memory_pagecache_size=${NEO_PAGE_CACHE_SIZE}")
fi
if [[ "$NEO_DATA_TMPFS" == "True" || "$NEO_DATA_TMPFS" == "true" ]]; then
  NEO_DOCKER_ARGS+=("--tmpfs" "/data:size=${NEO_DATA_TMPFS_SIZE}")
fi
if [[ "$NEO_LOGS_TMPFS" == "True" || "$NEO_LOGS_TMPFS" == "true" ]]; then
  NEO_DOCKER_ARGS+=("--tmpfs" "/logs:size=${NEO_LOGS_TMPFS_SIZE}")
fi

if [[ "$ENABLE_NEO4J" == "True" || "$ENABLE_NEO4J" == "true" ]]; then
  if ! docker inspect "$NEO_CONTAINER" >/dev/null 2>&1; then
    docker run -d \
      --name "$NEO_CONTAINER" \
      -e NEO4J_AUTH="${NEO_USER}/${NEO_PASSWORD}" \
      -e NEO4J_server_default__listen__address=0.0.0.0 \
      "${NEO_DOCKER_ARGS[@]}" \
      -p "${NEO_HTTP_PORT}:7474" \
      -p "${NEO_BOLT_PORT}:7687" \
      "$NEO_IMAGE"
  else
    docker start "$NEO_CONTAINER" >/dev/null
    echo "Using existing Neo4j container $NEO_CONTAINER; tmpfs and memory settings only apply on container creation"
  fi
fi

python - <<'PY' "$ENABLE_POSTGRES" "$PG_PORT" "$ENABLE_NEO4J" "$NEO_BOLT_PORT" "$READINESS_POLL_INTERVAL_SEC"
import socket
import sys
import time

ports = []
if sys.argv[1].lower() == "true":
    ports.append(int(sys.argv[2]))
if sys.argv[3].lower() == "true":
    ports.append(int(sys.argv[4]))
poll_interval = float(sys.argv[5])
deadline = time.time() + 120.0
remaining = set(ports)

while remaining and time.time() < deadline:
    for port in list(remaining):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1.0):
                remaining.remove(port)
        except OSError:
            pass
    if remaining:
        time.sleep(poll_interval)

if remaining:
    raise SystemExit(f"Timed out waiting for localhost ports: {sorted(remaining)}")
PY

echo "Started Docker services:"
if [[ "$ENABLE_POSTGRES" == "True" || "$ENABLE_POSTGRES" == "true" ]]; then
  echo "  PostgreSQL: $PG_CONTAINER on localhost:$PG_PORT"
fi
if [[ "$ENABLE_NEO4J" == "True" || "$ENABLE_NEO4J" == "true" ]]; then
  echo "  Neo4j: $NEO_CONTAINER on localhost:$NEO_BOLT_PORT (bolt), localhost:$NEO_HTTP_PORT (http)"
fi
