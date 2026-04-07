from __future__ import annotations

import csv
import time
from collections import defaultdict

from neo4j import GraphDatabase

from .common import AppContext, BenchmarkError, print_status, resolve_secret, run_command


def connect_neo4j(ctx: AppContext):
    cfg = ctx.config["neo4j"]
    password, _ = resolve_secret(
        cfg,
        env_key="password_env",
        value_key="password_value",
        label="Neo4j password",
    )
    return GraphDatabase.driver(cfg["uri"], auth=(cfg["user"], password))


def load_relation_mapping(ctx: AppContext) -> dict[str, str]:
    path = ctx.path(ctx.config["paths"]["preprocess_dir"]) / "relation_type_map.csv"
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            mapping[row["raw_rel_type"]] = row["neo4j_rel_type"]
    return mapping


def load_neo4j(ctx: AppContext, batch_size: int | None = None) -> None:
    paths = ctx.config["paths"]
    if batch_size is None:
        batch_size = int(ctx.config["neo4j"].get("load_batch_size", 10000))
    clear_batch_size = int(ctx.config["neo4j"].get("clear_batch_size", min(batch_size, 1000)))
    driver = connect_neo4j(ctx)
    relation_mapping = load_relation_mapping(ctx)
    nodes_csv = ctx.path(paths["preprocess_dir"]) / "nodes.csv"
    edges_csv = ctx.path(paths["preprocess_dir"]) / "edges.csv"
    node_count = 0
    edge_count = 0
    settings: dict[str, str] = {}
    try:
        with driver.session() as session:
            print_status("Neo4j load: clearing existing graph")
            _clear_graph_in_batches(session, clear_batch_size)
            session.run("CREATE CONSTRAINT entity_node_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.node_id IS UNIQUE").consume()

            print_status("Neo4j load: loading nodes")
            batch = []
            next_node_log = 50000
            with nodes_csv.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    batch.append({"node_id": row["node_id"], "node_type": row["node_type"]})
                    if len(batch) >= batch_size:
                        session.run(
                            """
                            UNWIND $rows AS row
                            MERGE (n:Entity {node_id: row.node_id})
                            SET n.node_type = row.node_type
                            """,
                            rows=batch,
                        ).consume()
                        node_count += len(batch)
                        if node_count >= next_node_log:
                            print_status(f"Neo4j load: loaded {node_count:,} nodes")
                            next_node_log += 50000
                        batch = []
                if batch:
                    session.run(
                        """
                        UNWIND $rows AS row
                        MERGE (n:Entity {node_id: row.node_id})
                        SET n.node_type = row.node_type
                        """,
                        rows=batch,
                    ).consume()
                    node_count += len(batch)
                    if node_count >= next_node_log:
                        print_status(f"Neo4j load: loaded {node_count:,} nodes")

            print_status("Neo4j load: loading relationships")
            rel_batches: dict[str, list[dict[str, str]]] = defaultdict(list)
            next_edge_log = 250000
            with edges_csv.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    rel_type = relation_mapping[row["rel_type"]]
                    rel_batches[rel_type].append({"src_id": row["src_id"], "dst_id": row["dst_id"]})
                    if len(rel_batches[rel_type]) >= batch_size:
                        _flush_rel_batch(session, rel_type, rel_batches[rel_type])
                        edge_count += len(rel_batches[rel_type])
                        if edge_count >= next_edge_log:
                            print_status(f"Neo4j load: loaded {edge_count:,} relationships")
                            next_edge_log += 250000
                        rel_batches[rel_type].clear()
                for rel_type, batch_rows in rel_batches.items():
                    if batch_rows:
                        _flush_rel_batch(session, rel_type, batch_rows)
                        edge_count += len(batch_rows)
                        if edge_count >= next_edge_log:
                            print_status(f"Neo4j load: loaded {edge_count:,} relationships")
                            next_edge_log += 250000
            print_status("Neo4j load: collecting settings")
            settings = collect_neo4j_settings(session, ctx)

        payload = {
            "node_count": node_count,
            "edge_count": edge_count,
            "runtime": ctx.config["neo4j"]["runtime"],
            "settings": settings,
        }
        print_status("Neo4j load: writing load_summary.json")
        ctx.write_json(paths["load_neo4j_dir"] + "/load_summary.json", payload)
    finally:
        driver.close()


def _flush_rel_batch(session, rel_type: str, rows: list[dict[str, str]]) -> None:
    query = f"""
    UNWIND $rows AS row
    MATCH (src:Entity {{node_id: row.src_id}})
    MATCH (dst:Entity {{node_id: row.dst_id}})
    CREATE (src)-[:`{rel_type}`]->(dst)
    """
    session.run(query, rows=rows).consume()


def _clear_graph_in_batches(session, batch_size: int) -> None:
    if batch_size <= 0:
        raise BenchmarkError("neo4j.clear_batch_size must be positive")
    session.run(
        f"""
        MATCH (n)
        CALL (n) {{
            DETACH DELETE n
        }} IN TRANSACTIONS OF {int(batch_size)} ROWS
        """
    ).consume()


def restart_neo4j(ctx: AppContext) -> None:
    restart_command = ctx.config["neo4j"].get("restart_command")
    if restart_command:
        run_command(restart_command)
        return

    if ctx.config.get("services", {}).get("mode") == "docker":
        container = ctx.config["services"]["docker"]["neo4j_container"]
        run_command(["docker", "restart", container])
        return

    raise BenchmarkError("config.yaml is missing neo4j.restart_command")


def wait_for_neo4j(ctx: AppContext, timeout_seconds: int = 60) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    poll_interval = float(ctx.config.get("services", {}).get("readiness_poll_interval_sec", 0.1))
    while time.time() < deadline:
        try:
            driver = connect_neo4j(ctx)
            driver.verify_connectivity()
            driver.close()
            return
        except Exception as exc:  # pragma: no cover - external service path
            last_error = exc
            time.sleep(poll_interval)
    raise BenchmarkError(f"Neo4j did not become ready in time: {last_error}")


def collect_neo4j_settings(session, ctx: AppContext) -> dict[str, str]:
    payload = {
        "configured_runtime": str(ctx.config["neo4j"]["runtime"]),
        "configured_heap_size": str(ctx.config["neo4j"]["heap_size"]),
        "configured_page_cache_size": str(ctx.config["neo4j"]["page_cache_size"]),
    }
    try:
        record = session.run(
            "CALL dbms.components() YIELD name, versions, edition "
            "RETURN name AS name, versions[0] AS version, edition AS edition"
        ).single()
        if record:
            payload["component_name"] = str(record["name"])
            payload["component_version"] = str(record["version"])
            payload["component_edition"] = str(record["edition"])
    except Exception:
        payload["component_name"] = "unavailable"
    return payload
