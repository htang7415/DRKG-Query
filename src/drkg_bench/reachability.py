from __future__ import annotations

import random
import statistics
import time
from pathlib import Path
from typing import Any

from neo4j import Query

from .common import AppContext, print_status
from .duckdb_db import connect_duckdb
from .neo4j_db import connect_neo4j
from .postgres import connect_postgres
from .reporting import fmt_int, fmt_num


REACHABILITY_FIELDS = [
    "eng",
    "reg",
    "bid",
    "anchor_id",
    "anchor_out_degree",
    "depth",
    "status",
    "fail_type",
    "ms",
    "out",
]


CHECK_FIELDS = [
    "reg",
    "bid",
    "anchor_id",
    "depth",
    "pg_out",
    "duck_out",
    "neo_out",
    "pg_duck_match",
    "pg_neo_match",
]


def run_reachability(ctx: AppContext) -> None:
    print_status("Reachability: selecting shared anchors from DuckDB")
    anchors = _sample_reachability_anchors(ctx)
    depths = [int(depth) for depth in ctx.config["reachability"]["depths"]]
    rows: list[dict[str, Any]] = []

    print_status("Reachability: running PostgreSQL recursive CTEs")
    rows.extend(_run_postgres_reachability(ctx, anchors, depths))
    print_status("Reachability: running DuckDB recursive CTEs")
    rows.extend(_run_duckdb_reachability(ctx, anchors, depths))
    print_status("Reachability: running Neo4j variable-length paths")
    rows.extend(_run_neo4j_reachability(ctx, anchors, depths))

    output_dir = Path(ctx.config["paths"]["reachability_dir"])
    ctx.write_csv(output_dir / "reachability_runtime.csv", REACHABILITY_FIELDS, rows)
    ctx.write_csv(output_dir / "reachability_correctness.csv", CHECK_FIELDS, _correctness_rows(anchors, depths, rows))
    ctx.write_json(
        output_dir / "reachability_summary.json",
        {
            "anchors_per_regime": int(ctx.config["reachability"]["anchors_per_regime"]),
            "depths": depths,
            "timeout_sec": int(ctx.config["reachability"]["timeout_sec"]),
            "rows": len(rows),
            "status_counts": _status_counts(rows),
            "note": "Small directional bounded-reachability sample; per-anchor rows are the primary evidence.",
        },
    )


def _sample_reachability_anchors(ctx: AppContext) -> list[dict[str, Any]]:
    sample_size = int(ctx.config["reachability"]["anchors_per_regime"])
    seed = int(ctx.config["project"]["random_seed"])
    conn = connect_duckdb(ctx)
    try:
        degree_rows = [
            {"anchor_id": row[0], "anchor_out_degree": int(row[1])}
            for row in conn.execute(
                """
                SELECT src_id, COUNT(*) AS out_degree
                FROM edges
                GROUP BY src_id
                ORDER BY src_id
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    rng = random.Random(f"{seed}:reachability")
    uniform_candidates = list(degree_rows)
    uniform = uniform_candidates if len(uniform_candidates) <= sample_size else rng.sample(uniform_candidates, sample_size)
    uniform = sorted(uniform, key=lambda row: row["anchor_id"])
    hub = sorted(degree_rows, key=lambda row: (-row["anchor_out_degree"], row["anchor_id"]))[:sample_size]

    rows = []
    for regime, selected in [("uniform", uniform), ("hub", hub)]:
        for index, item in enumerate(selected, start=1):
            rows.append(
                {
                    "reg": regime,
                    "bid": index,
                    "anchor_id": item["anchor_id"],
                    "anchor_out_degree": item["anchor_out_degree"],
                }
            )
    return rows


def _run_postgres_reachability(ctx: AppContext, anchors: list[dict[str, Any]], depths: list[int]) -> list[dict[str, Any]]:
    sql = """
        WITH RECURSIVE r(node_id, depth) AS (
            SELECT %s::text AS node_id, 0 AS depth
            UNION
            SELECT e.dst_id, r.depth + 1
            FROM edges e
            JOIN r ON e.src_id = r.node_id
            WHERE r.depth < %s
        )
        SELECT COUNT(DISTINCT node_id)
        FROM r
        WHERE depth > 0
    """
    timeout_ms = int(ctx.config["reachability"]["timeout_sec"]) * 1000
    rows = []
    conn = connect_postgres(ctx)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {timeout_ms}")
        for anchor in anchors:
            for depth in depths:
                rows.append(_timed_sql_row("pg", anchor, depth, lambda: _postgres_count(conn, sql, [anchor["anchor_id"], depth])))
    finally:
        conn.close()
    return rows


def _run_duckdb_reachability(ctx: AppContext, anchors: list[dict[str, Any]], depths: list[int]) -> list[dict[str, Any]]:
    sql = """
        WITH RECURSIVE r(node_id, depth) AS (
            SELECT ?::VARCHAR AS node_id, 0 AS depth
            UNION
            SELECT e.dst_id, r.depth + 1
            FROM edges e
            JOIN r ON e.src_id = r.node_id
            WHERE r.depth < ?
        )
        SELECT COUNT(DISTINCT node_id)
        FROM r
        WHERE depth > 0
    """
    rows = []
    conn = connect_duckdb(ctx)
    try:
        for anchor in anchors:
            for depth in depths:
                rows.append(_timed_sql_row("duck", anchor, depth, lambda a=anchor, d=depth: _duckdb_count(conn, sql, [a["anchor_id"], d])))
    finally:
        conn.close()
    return rows


def _run_neo4j_reachability(ctx: AppContext, anchors: list[dict[str, Any]], depths: list[int]) -> list[dict[str, Any]]:
    timeout_sec = int(ctx.config["reachability"]["timeout_sec"])
    rows = []
    driver = connect_neo4j(ctx)
    try:
        with driver.session() as session:
            for anchor in anchors:
                for depth in depths:
                    query = (
                        f"MATCH (start:Entity {{node_id: $anchor_id}}) "
                        f"MATCH (start)-[*1..{depth}]->(n:Entity) "
                        "RETURN count(DISTINCT n.node_id) AS output_cardinality"
                    )
                    rows.append(
                        _timed_sql_row(
                            "neo",
                            anchor,
                            depth,
                            lambda q=query, a=anchor: _neo4j_count(session, q, a["anchor_id"], timeout_sec),
                        )
                    )
    finally:
        driver.close()
    return rows


def _timed_sql_row(engine: str, anchor: dict[str, Any], depth: int, callback) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        output = callback()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "eng": engine,
            "reg": anchor["reg"],
            "bid": fmt_int(anchor["bid"]),
            "anchor_id": anchor["anchor_id"],
            "anchor_out_degree": fmt_int(anchor["anchor_out_degree"]),
            "depth": fmt_int(depth),
            "status": "ok",
            "fail_type": "",
            "ms": fmt_num(elapsed_ms),
            "out": fmt_int(output),
        }
    except Exception as exc:
        return {
            "eng": engine,
            "reg": anchor["reg"],
            "bid": fmt_int(anchor["bid"]),
            "anchor_id": anchor["anchor_id"],
            "anchor_out_degree": fmt_int(anchor["anchor_out_degree"]),
            "depth": fmt_int(depth),
            "status": "fail",
            "fail_type": type(exc).__name__,
            "ms": "",
            "out": "",
        }


def _postgres_count(conn, sql: str, params: list[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def _duckdb_count(conn, sql: str, params: list[Any]) -> int:
    return int(conn.execute(sql, params).fetchone()[0])


def _neo4j_count(session, query: str, anchor_id: str, timeout_sec: int) -> int:
    result = session.run(Query(query, timeout=timeout_sec), {"anchor_id": anchor_id})
    record = result.single()
    return int(record["output_cardinality"])


def _correctness_rows(anchors: list[dict[str, Any]], depths: list[int], rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    index = {
        (row["eng"], row["reg"], row["bid"], row["depth"]): row
        for row in rows
    }
    checks = []
    selected_anchors = []
    for regime in ["uniform", "hub"]:
        selected_anchors.extend([anchor for anchor in anchors if anchor["reg"] == regime][:3])
    for anchor in selected_anchors:
        for depth in depths:
            bid = fmt_int(anchor["bid"])
            depth_s = fmt_int(depth)
            pg = index.get(("pg", anchor["reg"], bid, depth_s), {})
            duck = index.get(("duck", anchor["reg"], bid, depth_s), {})
            neo = index.get(("neo", anchor["reg"], bid, depth_s), {})
            pg_out = pg.get("out", "")
            duck_out = duck.get("out", "")
            neo_out = neo.get("out", "")
            checks.append(
                {
                    "reg": anchor["reg"],
                    "bid": bid,
                    "anchor_id": anchor["anchor_id"],
                    "depth": depth_s,
                    "pg_out": pg_out,
                    "duck_out": duck_out,
                    "neo_out": neo_out,
                    "pg_duck_match": str(bool(pg_out) and pg_out == duck_out).lower(),
                    "pg_neo_match": str(bool(pg_out) and pg_out == neo_out).lower(),
                }
            )
    return checks


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = f"{row['eng']}:{row['status']}"
        counts[key] = counts.get(key, 0) + 1
    return counts
