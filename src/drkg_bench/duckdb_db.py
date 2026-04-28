from __future__ import annotations

from pathlib import Path

import duckdb

from .common import AppContext, BenchmarkError, print_status


def duckdb_path(ctx: AppContext) -> Path:
    return ctx.path(ctx.config["duckdb"]["database_path"])


def connect_duckdb(ctx: AppContext):
    path = duckdb_path(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    configure_duckdb(ctx, conn)
    return conn


def configure_duckdb(ctx: AppContext, conn) -> None:
    cfg = ctx.config.get("duckdb", {})
    memory_limit = cfg.get("memory_limit")
    threads = cfg.get("threads")
    if memory_limit:
        conn.execute(f"SET memory_limit='{memory_limit}'")
    if threads:
        conn.execute(f"SET threads={int(threads)}")


def load_duckdb(ctx: AppContext) -> None:
    paths = ctx.config["paths"]
    nodes_csv = ctx.path(paths["preprocess_dir"]) / "nodes.csv"
    edges_csv = ctx.path(paths["preprocess_dir"]) / "edges.csv"
    if not nodes_csv.exists() or not edges_csv.exists():
        raise BenchmarkError(
            "DuckDB load requires preprocess CSVs. Run `preprocess` first; "
            f"missing nodes={not nodes_csv.exists()} edges={not edges_csv.exists()}."
        )

    db_path = duckdb_path(ctx)
    if db_path.exists():
        db_path.unlink()

    conn = connect_duckdb(ctx)
    try:
        print_status("DuckDB load: importing nodes.csv and edges.csv")
        conn.execute("DROP TABLE IF EXISTS anchor_degrees")
        conn.execute("DROP TABLE IF EXISTS typed_edges")
        conn.execute("DROP TABLE IF EXISTS edges")
        conn.execute("DROP TABLE IF EXISTS nodes")
        conn.execute(
            """
            CREATE TABLE nodes AS
            SELECT
                node_id::VARCHAR AS node_id,
                node_type::VARCHAR AS node_type
            FROM read_csv(?, header=true, all_varchar=true)
            """,
            [str(nodes_csv)],
        )
        conn.execute(
            """
            CREATE TABLE edges AS
            SELECT
                src_id::VARCHAR AS src_id,
                rel_type::VARCHAR AS rel_type,
                dst_id::VARCHAR AS dst_id
            FROM read_csv(?, header=true, all_varchar=true)
            """,
            [str(edges_csv)],
        )
        print_status("DuckDB load: building typed_edges and anchor_degrees")
        conn.execute(
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
        conn.execute(
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
        print_status("DuckDB load: indexing and ANALYZE")
        for statement in [
            "CREATE INDEX idx_edges_src ON edges (src_id)",
            "CREATE INDEX idx_edges_dst ON edges (dst_id)",
            "CREATE INDEX idx_edges_rel_src ON edges (rel_type, src_id)",
            "CREATE INDEX idx_typed_edges_rel_src ON typed_edges (rel_type, src_id)",
            "CREATE INDEX idx_typed_edges_rel_dst ON typed_edges (rel_type, dst_id)",
            "CREATE INDEX idx_typed_edges_src_rel ON typed_edges (src_id, rel_type)",
            "CREATE INDEX idx_anchor_degrees_rel_anchor ON anchor_degrees (rel_type, anchor_id)",
        ]:
            conn.execute(statement)
        conn.execute("ANALYZE")

        payload = {
            "database_path": str(db_path.relative_to(ctx.root) if db_path.is_relative_to(ctx.root) else db_path),
            "node_count": conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
            "edge_count": conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0],
            "typed_edge_count": conn.execute("SELECT COUNT(*) FROM typed_edges").fetchone()[0],
            "anchor_degree_count": conn.execute("SELECT COUNT(*) FROM anchor_degrees").fetchone()[0],
            "memory_limit": str(ctx.config.get("duckdb", {}).get("memory_limit", "")),
            "threads": int(ctx.config.get("duckdb", {}).get("threads", 0) or 0),
            "node_id_type": "VARCHAR",
        }
        print_status("DuckDB load: writing load_summary.json")
        ctx.write_json(Path(paths["load_duckdb_dir"]) / "load_summary.json", payload)
    finally:
        conn.close()


def duckdb_sql(sql: str) -> str:
    return sql.replace("%s", "?")
