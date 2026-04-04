from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .common import AppContext, BenchmarkError, print_status
from .plotting import NATURE_PALETTE, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest
from .relation_mapping import sanitize_relation_type


def _node_type(node_id: str) -> str:
    return node_id.split("::", 1)[0]


def _has_empty_local_identifier(node_id: str) -> bool:
    return "::" in node_id and node_id.split("::", 1)[1] == ""


def run_preprocess(ctx: AppContext) -> None:
    paths = ctx.config["paths"]
    preprocess_cfg = ctx.config["preprocess"]
    mapping_cfg = ctx.config["relation_mapping"]

    raw_path = ctx.path(paths["raw_drkg"])
    out_dir = ctx.path(paths["preprocess_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir_value = paths.get("preprocess_work_dir", paths["preprocess_dir"])
    work_dir = ctx.path(work_dir_value)
    work_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = work_dir / "preprocess.sqlite"

    if sqlite_path.exists():
        sqlite_path.unlink()

    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(
        """
        CREATE TABLE nodes (
            node_id TEXT PRIMARY KEY,
            node_type TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE edges (
            src_id TEXT NOT NULL,
            rel_type TEXT NOT NULL,
            dst_id TEXT NOT NULL,
            PRIMARY KEY (src_id, rel_type, dst_id)
        )
        """
    )
    conn.commit()

    stats = {
        "raw_rows": 0,
        "kept_rows": 0,
        "duplicate_rows_dropped": 0,
        "rows_with_empty_endpoint_dropped": 0,
        "self_loops_kept": 0,
    }
    relation_types: set[str] = set()
    raw_row_limit = preprocess_cfg.get("raw_row_limit")
    raw_row_limit = int(raw_row_limit) if raw_row_limit not in {"", None} else None

    print_status("Preprocessing DRKG into deduplicated sqlite staging tables")
    with raw_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if raw_row_limit is not None and stats["raw_rows"] >= raw_row_limit:
                break
            stats["raw_rows"] += 1
            if len(row) != 3:
                raise BenchmarkError(f"Unexpected DRKG row width at row {stats['raw_rows']}: {row}")
            src_id, rel_type, dst_id = row
            if preprocess_cfg.get("drop_empty_local_identifier", True) and (
                _has_empty_local_identifier(src_id) or _has_empty_local_identifier(dst_id)
            ):
                stats["rows_with_empty_endpoint_dropped"] += 1
                continue

            relation_types.add(rel_type)
            if src_id == dst_id:
                stats["self_loops_kept"] += 1

            inserted = conn.execute(
                "INSERT OR IGNORE INTO edges (src_id, rel_type, dst_id) VALUES (?, ?, ?)",
                (src_id, rel_type, dst_id),
            )
            if inserted.rowcount == 0:
                stats["duplicate_rows_dropped"] += 1
                continue

            conn.execute(
                "INSERT OR IGNORE INTO nodes (node_id, node_type) VALUES (?, ?)",
                (src_id, _node_type(src_id)),
            )
            conn.execute(
                "INSERT OR IGNORE INTO nodes (node_id, node_type) VALUES (?, ?)",
                (dst_id, _node_type(dst_id)),
            )
            stats["kept_rows"] += 1

            if stats["raw_rows"] % 100000 == 0:
                conn.commit()
                print_status(f"Processed {stats['raw_rows']:,} rows")

    conn.commit()

    print_status("Writing deduplicated nodes.csv and edges.csv")
    nodes_csv = ctx.path(paths["preprocess_dir"]) / "nodes.csv"
    edges_csv = ctx.path(paths["preprocess_dir"]) / "edges.csv"
    relation_map_csv = ctx.path(paths["preprocess_dir"]) / "relation_type_map.csv"

    with nodes_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["node_id", "node_type"])
        for row in conn.execute("SELECT node_id, node_type FROM nodes ORDER BY node_id"):
            writer.writerow(row)

    with edges_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["src_id", "rel_type", "dst_id"])
        for row in conn.execute("SELECT src_id, rel_type, dst_id FROM edges ORDER BY rel_type, src_id, dst_id"):
            writer.writerow(row)

    relation_rows = []
    for raw_relation in sorted(relation_types):
        relation_rows.append(
            {
                "raw_rel_type": raw_relation,
                "neo4j_rel_type": sanitize_relation_type(
                    raw_relation,
                    prefix=mapping_cfg.get("prefix", "REL"),
                    hash_chars=int(mapping_cfg.get("append_sha1_hex_chars", 8)),
                ),
            }
        )

    with relation_map_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["raw_rel_type", "neo4j_rel_type"])
        writer.writeheader()
        writer.writerows(relation_rows)

    node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    node_type_counts = [
        {"node_type": node_type, "node_count": count}
        for node_type, count in conn.execute(
            "SELECT node_type, COUNT(*) AS node_count FROM nodes GROUP BY node_type ORDER BY node_count DESC, node_type"
        )
    ]
    relation_edge_counts = [
        {"rel_type": rel_type, "edge_count": count}
        for rel_type, count in conn.execute(
            "SELECT rel_type, COUNT(*) AS edge_count FROM edges GROUP BY rel_type ORDER BY edge_count DESC, rel_type"
        )
    ]
    out_degrees = np.asarray(
        [row[0] for row in conn.execute("SELECT COUNT(*) AS out_degree FROM edges GROUP BY src_id")],
        dtype=float,
    )
    in_degrees = np.asarray(
        [row[0] for row in conn.execute("SELECT COUNT(*) AS in_degree FROM edges GROUP BY dst_id")],
        dtype=float,
    )
    total_degrees = np.asarray(
        [
            row[0]
            for row in conn.execute(
                """
                WITH out_deg AS (
                    SELECT src_id AS node_id, COUNT(*) AS out_degree
                    FROM edges
                    GROUP BY src_id
                ),
                in_deg AS (
                    SELECT dst_id AS node_id, COUNT(*) AS in_degree
                    FROM edges
                    GROUP BY dst_id
                )
                SELECT
                    COALESCE(out_deg.out_degree, 0) + COALESCE(in_deg.in_degree, 0) AS total_degree
                FROM nodes
                LEFT JOIN out_deg ON out_deg.node_id = nodes.node_id
                LEFT JOIN in_deg ON in_deg.node_id = nodes.node_id
                """
            )
        ],
        dtype=float,
    )
    dataset_analysis = {
        "node_type_count": len(node_type_counts),
        "relation_type_count": len(relation_edge_counts),
        "top_node_types": node_type_counts[:10],
        "top_relation_types": relation_edge_counts[:10],
        "out_degree_summary": _degree_summary(out_degrees),
        "in_degree_summary": _degree_summary(in_degrees),
        "total_degree_summary": _degree_summary(total_degrees),
    }

    ctx.write_csv(Path(paths["preprocess_dir"]) / "node_type_counts.csv", ["node_type", "node_count"], node_type_counts)
    ctx.write_csv(Path(paths["preprocess_dir"]) / "relation_edge_counts.csv", ["rel_type", "edge_count"], relation_edge_counts)
    ctx.write_json(Path(paths["preprocess_dir"]) / "dataset_analysis.json", dataset_analysis)
    _write_dataset_figures(
        ctx,
        node_type_counts=node_type_counts,
        relation_edge_counts=relation_edge_counts,
        total_degrees=total_degrees,
    )

    summary = {
        **stats,
        "unique_nodes": node_count,
        "unique_edges": edge_count,
        "unique_relations": len(relation_types),
        "raw_row_limit": raw_row_limit,
        "sqlite_path": str(sqlite_path if sqlite_path.is_absolute() else sqlite_path.relative_to(ctx.root)),
        "nodes_csv": str(nodes_csv.relative_to(ctx.root)),
        "edges_csv": str(edges_csv.relative_to(ctx.root)),
        "relation_type_map_csv": str(relation_map_csv.relative_to(ctx.root)),
        "node_type_counts_csv": str((ctx.path(paths["preprocess_dir"]) / "node_type_counts.csv").relative_to(ctx.root)),
        "relation_edge_counts_csv": str((ctx.path(paths["preprocess_dir"]) / "relation_edge_counts.csv").relative_to(ctx.root)),
        "dataset_analysis_json": str((ctx.path(paths["preprocess_dir"]) / "dataset_analysis.json").relative_to(ctx.root)),
    }
    ctx.write_json(Path(paths["preprocess_dir"]) / "preprocess_summary.json", summary)
    conn.close()


def _degree_summary(values: np.ndarray) -> dict[str, float]:
    if values.size == 0:
        return {"count": 0, "min": 0.0, "median": 0.0, "p95": 0.0, "max": 0.0}
    return {
        "count": int(values.size),
        "min": float(np.min(values)),
        "median": float(np.median(values)),
        "p95": float(np.percentile(values, 95)),
        "max": float(np.max(values)),
    }


def _write_dataset_figures(
    ctx: AppContext,
    *,
    node_type_counts: list[dict[str, object]],
    relation_edge_counts: list[dict[str, object]],
    total_degrees: np.ndarray,
) -> None:
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["prepare_figures_dir"])
    figure_dir.mkdir(parents=True, exist_ok=True)
    remove_existing_figures(
        figure_dir,
        [
        "dataset_node_type_counts.png",
        "dataset_relation_edge_counts_top20.png",
        "dataset_total_degree_histogram.png",
        ],
    )

    if node_type_counts:
        fig, ax = plt.subplots(figsize=(10, 6))
        labels = [row["node_type"] for row in node_type_counts]
        values = [int(row["node_count"]) for row in node_type_counts]
        ax.bar(range(len(labels)), values, color=NATURE_PALETTE["dataset"])
        ax.set_ylabel("Nodes")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        style_axes(ax)
        fig.tight_layout()
        fig.savefig(figure_dir / "dataset_node_type_counts.png", dpi=ctx.config["plotting"]["dpi"])
        plt.close(fig)

    top_relations = relation_edge_counts[:20]
    if top_relations:
        fig, ax = plt.subplots(figsize=(14, 8))
        labels = [row["rel_type"] for row in top_relations]
        values = [int(row["edge_count"]) for row in top_relations]
        ax.bar(range(len(labels)), values, color=NATURE_PALETTE["neutral"])
        ax.set_ylabel("Edges")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=90)
        style_axes(ax)
        fig.tight_layout()
        fig.savefig(figure_dir / "dataset_relation_edge_counts_top20.png", dpi=ctx.config["plotting"]["dpi"])
        plt.close(fig)

    positive_degrees = total_degrees[total_degrees > 0]
    if positive_degrees.size > 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        max_degree = float(np.max(positive_degrees))
        if max_degree > 1:
            bins = np.logspace(0, np.log10(max_degree), num=30)
            ax.set_xscale("log")
        else:
            bins = 20
        ax.hist(positive_degrees, bins=bins, color=NATURE_PALETTE["template"], alpha=0.9)
        ax.set_yscale("log")
        ax.set_xlabel("Total degree")
        ax.set_ylabel("Nodes")
        style_axes(ax)
        fig.tight_layout()
        fig.savefig(figure_dir / "dataset_total_degree_histogram.png", dpi=ctx.config["plotting"]["dpi"])
        plt.close(fig)

    write_figure_manifest(ctx, figure_dir)
