from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

import psycopg

from .common import AppContext, BenchmarkError, print_status, resolve_secret, run_command


def connect_postgres(ctx: AppContext):
    cfg = ctx.config["postgres"]
    password, _ = resolve_secret(
        cfg,
        env_key="password_env",
        value_key="password_value",
        label="PostgreSQL password",
    )
    conn = psycopg.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=password,
        autocommit=False,
    )
    return conn


def ensure_postgres_schema(ctx: AppContext, conn) -> None:
    cfg = ctx.config["postgres"]
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS anchor_degrees")
        cur.execute("DROP TABLE IF EXISTS typed_edges")
        cur.execute("DROP TABLE IF EXISTS edges")
        cur.execute("DROP TABLE IF EXISTS nodes")
        cur.execute(
            """
            CREATE TABLE nodes (
                node_id TEXT PRIMARY KEY,
                node_type TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE edges (
                src_id TEXT NOT NULL,
                rel_type TEXT NOT NULL,
                dst_id TEXT NOT NULL,
                PRIMARY KEY (src_id, rel_type, dst_id)
            )
            """
        )
        cur.execute("CREATE INDEX idx_edges_rel_src ON edges (rel_type, src_id)")
        cur.execute("CREATE INDEX idx_edges_rel_dst ON edges (rel_type, dst_id)")
        cur.execute("CREATE INDEX idx_edges_src_rel ON edges (src_id, rel_type)")
        cur.execute(f"SET max_parallel_workers_per_gather = {int(cfg.get('max_parallel_workers_per_gather', 0))}")
    conn.commit()


def _copy_csv(cur, table_name: str, columns: list[str], csv_path: Path) -> None:
    with csv_path.open("r", encoding="utf-8") as handle:
        next(handle)
        with cur.copy(f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV)") as copy:
            for line in handle:
                copy.write(line)


def load_postgres(ctx: AppContext) -> None:
    paths = ctx.config["paths"]
    conn = connect_postgres(ctx)
    try:
        print_status("PostgreSQL load: resetting schema")
        ensure_postgres_schema(ctx, conn)
        with conn.cursor() as cur:
            print_status("PostgreSQL load: COPY nodes.csv")
            _copy_csv(cur, "nodes", ["node_id", "node_type"], ctx.path(paths["preprocess_dir"]) / "nodes.csv")
            print_status("PostgreSQL load: COPY edges.csv")
            _copy_csv(cur, "edges", ["src_id", "rel_type", "dst_id"], ctx.path(paths["preprocess_dir"]) / "edges.csv")
            print_status("PostgreSQL load: building typed_edges and anchor_degrees")
            cur.execute(
                """
                CREATE TABLE typed_edges AS
                SELECT
                    e.src_id,
                    src.node_type AS src_type,
                    e.rel_type,
                    e.dst_id,
                    dst.node_type AS dst_type
                FROM edges e
                JOIN nodes src ON src.node_id = e.src_id
                JOIN nodes dst ON dst.node_id = e.dst_id
                """
            )
            cur.execute(
                """
                CREATE TABLE anchor_degrees AS
                SELECT
                    rel_type,
                    src_id AS anchor_id,
                    COUNT(*) AS first_edge_degree
                FROM typed_edges
                GROUP BY rel_type, src_id
                """
            )
            print_status("PostgreSQL load: indexing and ANALYZE")
            cur.execute("CREATE INDEX idx_typed_edges_rel_src ON typed_edges (rel_type, src_id)")
            cur.execute("CREATE INDEX idx_typed_edges_rel_dst ON typed_edges (rel_type, dst_id)")
            cur.execute("CREATE INDEX idx_typed_edges_src_rel ON typed_edges (src_id, rel_type)")
            cur.execute("CREATE INDEX idx_anchor_degrees_rel_anchor ON anchor_degrees (rel_type, anchor_id)")
            cur.execute("ANALYZE nodes")
            cur.execute("ANALYZE edges")
            cur.execute("ANALYZE typed_edges")
            cur.execute("ANALYZE anchor_degrees")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM nodes")
            node_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM edges")
            edge_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM typed_edges")
            typed_edge_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM anchor_degrees")
            anchor_degree_count = cur.fetchone()[0]
        settings = collect_postgres_settings(conn)

        payload = {
            "node_count": node_count,
            "edge_count": edge_count,
            "typed_edge_count": typed_edge_count,
            "anchor_degree_count": anchor_degree_count,
            "parallel_disabled": bool(ctx.config["postgres"]["disable_parallel_query"]),
            "max_parallel_workers_per_gather": int(ctx.config["postgres"].get("max_parallel_workers_per_gather", 0)),
            "settings": settings,
        }
        print_status("PostgreSQL load: writing load_summary.json")
        ctx.write_json(ctx.config["paths"]["load_postgres_dir"] + "/load_summary.json", payload)
    finally:
        conn.close()


def restart_postgres(ctx: AppContext) -> None:
    restart_command = ctx.config["postgres"].get("restart_command")
    if restart_command:
        run_command(restart_command)
        return

    if ctx.config.get("services", {}).get("mode") == "docker":
        container = ctx.config["services"]["docker"]["postgres_container"]
        run_command(["docker", "restart", container])
        return

    raise BenchmarkError("config.yaml is missing postgres.restart_command")


def wait_for_postgres(ctx: AppContext, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    poll_interval = float(ctx.config.get("services", {}).get("readiness_poll_interval_sec", 0.1))
    while time.time() < deadline:
        try:
            conn = connect_postgres(ctx)
            conn.close()
            return
        except Exception as exc:  # pragma: no cover - external service path
            last_error = exc
            time.sleep(poll_interval)
    raise BenchmarkError(f"PostgreSQL did not become ready in time: {last_error}")


def explain_json(conn, sql: str, params: Iterable[Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", list(params))
        raw = cur.fetchone()[0]
        if isinstance(raw, str):
            data = json.loads(raw)
        else:
            data = raw
    return data[0]


def collect_postgres_settings(conn) -> dict[str, str]:
    keys = [
        "server_version",
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "effective_cache_size",
        "max_parallel_workers_per_gather",
    ]
    payload: dict[str, str] = {}
    with conn.cursor() as cur:
        for key in keys:
            cur.execute(f"SHOW {key}")
            payload[key] = str(cur.fetchone()[0])
    return payload


def plan_metrics(plan: dict[str, Any]) -> dict[str, float]:
    def walk(node: dict[str, Any]) -> tuple[float, float]:
        rows = float(node.get("Actual Rows", 0.0)) * float(node.get("Actual Loops", 0.0))
        shared_hits = float(node.get("Shared Hit Blocks", 0.0))
        total_rows = rows
        total_hits = shared_hits
        for child in node.get("Plans", []) or []:
            child_rows, child_hits = walk(child)
            total_rows += child_rows
            total_hits += child_hits
        return total_rows, total_hits

    total_rows, total_hits = walk(plan["Plan"])
    return {
        "intermediate_work_rows": total_rows,
        "shared_hit_blocks": total_hits,
        "execution_time_ms": float(plan.get("Execution Time", 0.0)),
    }
