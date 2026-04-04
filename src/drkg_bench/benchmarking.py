from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
from neo4j import Query

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, print_status
from .neo4j_db import connect_neo4j
from .plotting import NATURE_PALETTE, apply_plot_style, style_axes, write_figure_manifest
from .postgres import connect_postgres, explain_json, plan_metrics
from .system_ops import restart_engine_for_instance
from .templates import (
    Template,
    all_left_deep_orders,
    cypher_count_query,
    cypher_params,
    default_count_params,
    default_count_sql,
    forced_order_params,
    forced_order_sql,
)


def run_postgres_baseline(ctx: AppContext) -> None:
    binding_rows = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    print_status(f"PostgreSQL baseline: benchmarking {len(binding_rows)} query instances")
    rows = _run_postgres_instances(
        ctx,
        binding_rows=binding_rows,
        mode="baseline",
    )
    if rows:
        output_dir = Path(ctx.config["paths"]["postgres_baseline_dir"])
        print_status("PostgreSQL baseline: writing benchmark outputs")
        ctx.write_csv(output_dir / "postgres_baseline.csv", list(rows[0].keys()), rows)
        _write_logs(ctx, output_dir / "logs", rows)


def run_join_order(ctx: AppContext) -> None:
    binding_rows = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv")
    print_status(f"PostgreSQL join-order study: benchmarking {len(binding_rows)} binding seeds")
    rows = _run_postgres_instances(
        ctx,
        binding_rows=binding_rows,
        mode="join_order",
    )
    if rows:
        output_dir = Path(ctx.config["paths"]["join_order_dir"])
        print_status("PostgreSQL join-order study: writing benchmark outputs")
        ctx.write_csv(output_dir / "postgres_join_order.csv", list(rows[0].keys()), rows)
        join_classes = [
            {
                "template_id": row["template_id"],
                "join_order": row["join_order"],
                "join_order_class": row["join_order_class"],
            }
            for row in rows
            if row["join_order"]
        ]
        if join_classes:
            ctx.write_csv(output_dir / "join_order_classes.csv", list(join_classes[0].keys()), join_classes)
        _write_logs(ctx, output_dir / "logs", rows)
        _write_join_order_figure(ctx, rows)


def _run_postgres_instances(
    ctx: AppContext,
    *,
    binding_rows: list[dict[str, str]],
    mode: str,
) -> list[dict[str, Any]]:
    templates = {template.template_id: template for template in load_selected_templates(ctx)}
    rows: list[dict[str, Any]] = []
    total_bindings = len(binding_rows)
    progress_every = _progress_interval(total_bindings)
    last_group: tuple[str, str] | None = None

    for binding_index, binding in enumerate(binding_rows, start=1):
        template = templates[binding["template_id"]]
        group = (template.template_id, binding["regime"])
        if group != last_group:
            print_status(f"PostgreSQL {mode}: {template.template_id} / {binding['regime']}")
            last_group = group
        if binding_index == 1 or binding_index == total_bindings or binding_index % progress_every == 0:
            print_status(f"PostgreSQL {mode}: binding {binding_index}/{total_bindings}")
        if mode == "baseline":
            rows.append(_benchmark_postgres_instance(ctx, template, binding, None))
            continue

        if template.template_id.startswith("path_2_"):
            continue

        rows.append(_benchmark_postgres_instance(ctx, template, binding, None))
        for order in all_left_deep_orders(template):
            rows.append(_benchmark_postgres_instance(ctx, template, binding, order))
    return rows


def _benchmark_postgres_instance(
    ctx: AppContext,
    template: Template,
    binding: dict[str, str],
    order: tuple[str, ...] | None,
) -> dict[str, Any]:
    instance_info = restart_engine_for_instance(ctx, "postgres")
    conn = connect_postgres(ctx)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SET max_parallel_workers_per_gather = {int(ctx.config['postgres']['max_parallel_workers_per_gather'])}"
            )
            if order:
                cur.execute(f"SET join_collapse_limit = {int(ctx.config['join_order']['join_collapse_limit'])}")
                cur.execute(f"SET from_collapse_limit = {int(ctx.config['join_order']['from_collapse_limit'])}")
        conn.commit()

        if order:
            sql, join_order_class = forced_order_sql(template, order)
            params = forced_order_params(template, order, binding["anchor_id"])
            join_order = ",".join(order)
        else:
            sql = default_count_sql(template)
            params = default_count_params(template, binding["anchor_id"])
            join_order_class = "default_plan"
            join_order = ""

        return _run_postgres_query_instance(
            ctx,
            conn=conn,
            sql=sql,
            params=params,
            template=template,
            binding=binding,
            join_order=join_order,
            join_order_class=join_order_class,
            instance_info=instance_info,
        )
    finally:
        conn.close()


def _run_postgres_query_instance(
    ctx: AppContext,
    *,
    conn,
    sql: str,
    params: list[Any],
    template: Template,
    binding: dict[str, str],
    join_order: str,
    join_order_class: str,
    instance_info: dict[str, Any],
) -> dict[str, Any]:
    warmup_runs = int(ctx.config["benchmark"]["warmup_runs"])
    measured_runs = int(ctx.config["benchmark"]["measured_runs"])
    instrumented_runs = int(ctx.config["benchmark"]["instrumented_runs"])
    plain_timeout = int(ctx.config["benchmark"]["plain_timeout_sec"]) * 1000
    instrumented_timeout = int(ctx.config["benchmark"]["instrumented_timeout_sec"]) * 1000
    if instrumented_runs != 1:
        raise ValueError("This harness expects benchmark.instrumented_runs = 1")

    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {plain_timeout}")

    result_row = _base_result_row("postgres", template, binding, join_order, join_order_class, instance_info)
    timings = []
    output_cardinality = None

    try:
        for _ in range(warmup_runs):
            output_cardinality = _execute_count_sql(conn, sql, params)
    except Exception as exc:
        result_row.update(
            {
                "status": "failed",
                "failure_stage": "warmup",
                "failure_type": type(exc).__name__,
                "timeout_value_sec": ctx.config["benchmark"]["plain_timeout_sec"],
                "median_ms": "",
                "iqr_ms": "",
                "output_cardinality": "",
                "shared_hit_blocks": "",
                "intermediate_work_rows": "",
            }
        )
        return result_row

    try:
        for index in range(measured_runs):
            started = time.perf_counter()
            output_cardinality = _execute_count_sql(conn, sql, params)
            timings.append((time.perf_counter() - started) * 1000.0)
            result_row["completed_measured_runs"] = index + 1
    except Exception as exc:
        result_row.update(
            {
                "status": "failed",
                "failure_stage": "measured",
                "failure_type": type(exc).__name__,
                "timeout_value_sec": ctx.config["benchmark"]["plain_timeout_sec"],
                "median_ms": "",
                "iqr_ms": "",
                "output_cardinality": "",
                "shared_hit_blocks": "",
                "intermediate_work_rows": "",
            }
        )
        return result_row

    result_row["median_ms"] = round(statistics.median(timings), 6)
    result_row["iqr_ms"] = round(float(np.percentile(timings, 75) - np.percentile(timings, 25)), 6)
    result_row["output_cardinality"] = output_cardinality

    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {instrumented_timeout}")
        plan = explain_json(conn, sql, params)
        metrics = plan_metrics(plan)
        result_row["shared_hit_blocks"] = metrics["shared_hit_blocks"]
        result_row["intermediate_work_rows"] = metrics["intermediate_work_rows"]
        result_row["status"] = "ok"
    except Exception as exc:
        result_row.update(
            {
                "status": "instrumented_failed",
                "failure_stage": "instrumented",
                "failure_type": type(exc).__name__,
                "timeout_value_sec": ctx.config["benchmark"]["instrumented_timeout_sec"],
                "shared_hit_blocks": "",
                "intermediate_work_rows": "",
            }
        )
    return result_row


def _execute_count_sql(conn, sql: str, params: list[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def run_neo4j_baseline(ctx: AppContext) -> None:
    templates = {template.template_id: template for template in load_selected_templates(ctx)}
    bindings = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    print_status(f"Neo4j baseline: benchmarking {len(bindings)} query instances")
    rows = []
    total_bindings = len(bindings)
    progress_every = _progress_interval(total_bindings)
    last_group: tuple[str, str] | None = None
    for binding_index, binding in enumerate(bindings, start=1):
        template = templates[binding["template_id"]]
        group = (template.template_id, binding["regime"])
        if group != last_group:
            print_status(f"Neo4j baseline: {template.template_id} / {binding['regime']}")
            last_group = group
        if binding_index == 1 or binding_index == total_bindings or binding_index % progress_every == 0:
            print_status(f"Neo4j baseline: binding {binding_index}/{total_bindings}")
        rows.append(_benchmark_neo4j_instance(ctx, template, binding))
    if rows:
        output_dir = Path(ctx.config["paths"]["neo4j_baseline_dir"])
        print_status("Neo4j baseline: writing benchmark outputs")
        ctx.write_csv(output_dir / "neo4j_baseline.csv", list(rows[0].keys()), rows)
        _write_logs(ctx, output_dir / "logs", rows)


def _benchmark_neo4j_instance(ctx: AppContext, template: Template, binding: dict[str, str]) -> dict[str, Any]:
    instance_info = restart_engine_for_instance(ctx, "neo4j")
    driver = connect_neo4j(ctx)
    warmup_runs = int(ctx.config["benchmark"]["warmup_runs"])
    measured_runs = int(ctx.config["benchmark"]["measured_runs"])
    instrumented_runs = int(ctx.config["benchmark"]["instrumented_runs"])
    plain_timeout = int(ctx.config["benchmark"]["plain_timeout_sec"])
    if instrumented_runs != 1:
        raise ValueError("This harness expects benchmark.instrumented_runs = 1")
    result_row = _base_result_row("neo4j", template, binding, "", "default_plan", instance_info)
    timings = []
    output_cardinality = None

    try:
        with driver.session() as session:
            plain_query = cypher_count_query(template, profile=False)
            profile_query = cypher_count_query(template, profile=True)
            params = cypher_params(template, binding["anchor_id"])
            try:
                for _ in range(warmup_runs):
                    output_cardinality = _run_neo4j_count(session, plain_query, params, plain_timeout)
            except Exception as exc:
                result_row.update(
                    {
                        "status": "failed",
                        "failure_stage": "warmup",
                        "failure_type": type(exc).__name__,
                        "timeout_value_sec": ctx.config["benchmark"]["plain_timeout_sec"],
                        "median_ms": "",
                        "iqr_ms": "",
                        "output_cardinality": "",
                        "db_hits": "",
                        "intermediate_work_rows": "",
                        "runtime_verified": False,
                    }
                )
                return result_row

            try:
                for index in range(measured_runs):
                    started = time.perf_counter()
                    output_cardinality = _run_neo4j_count(session, plain_query, params, plain_timeout)
                    timings.append((time.perf_counter() - started) * 1000.0)
                    result_row["completed_measured_runs"] = index + 1
            except Exception as exc:
                result_row.update(
                    {
                        "status": "failed",
                        "failure_stage": "measured",
                        "failure_type": type(exc).__name__,
                        "timeout_value_sec": ctx.config["benchmark"]["plain_timeout_sec"],
                        "median_ms": "",
                        "iqr_ms": "",
                        "output_cardinality": "",
                        "db_hits": "",
                        "intermediate_work_rows": "",
                        "runtime_verified": False,
                    }
                )
                return result_row

            result_row["median_ms"] = round(statistics.median(timings), 6)
            result_row["iqr_ms"] = round(float(np.percentile(timings, 75) - np.percentile(timings, 25)), 6)
            result_row["output_cardinality"] = output_cardinality

            try:
                records, summary = _run_neo4j_profile(session, profile_query, params, int(ctx.config["benchmark"]["instrumented_timeout_sec"]))
                result_row["output_cardinality"] = int(records[0]["output_cardinality"])
                runtime_ok = _verify_neo4j_runtime(summary)
                result_row["runtime_verified"] = runtime_ok
                if runtime_ok:
                    profile_metrics = _neo4j_profile_metrics(summary.profile)
                    result_row["db_hits"] = profile_metrics["db_hits"]
                    result_row["intermediate_work_rows"] = profile_metrics["rows"]
                    result_row["status"] = "ok"
                else:
                    result_row["status"] = "instrumented_failed"
                    result_row["failure_stage"] = "instrumented"
                    result_row["failure_type"] = "RuntimeVerificationFailed"
                    result_row["timeout_value_sec"] = ctx.config["benchmark"]["instrumented_timeout_sec"]
                    result_row["db_hits"] = ""
                    result_row["intermediate_work_rows"] = ""
            except Exception as exc:
                result_row.update(
                    {
                        "status": "instrumented_failed",
                        "failure_stage": "instrumented",
                        "failure_type": type(exc).__name__,
                        "timeout_value_sec": ctx.config["benchmark"]["instrumented_timeout_sec"],
                        "db_hits": "",
                        "intermediate_work_rows": "",
                        "runtime_verified": False,
                    }
                )
    except Exception as exc:
        result_row.update(
            {
                "status": "failed",
                "failure_stage": "session",
                "failure_type": type(exc).__name__,
                "timeout_value_sec": ctx.config["benchmark"]["plain_timeout_sec"],
                "median_ms": "",
                "iqr_ms": "",
                "output_cardinality": "",
                "db_hits": "",
                "intermediate_work_rows": "",
                "runtime_verified": False,
            }
        )
    finally:
        driver.close()
    return result_row


def _run_neo4j_count(session, query: str, params: dict[str, Any], timeout_sec: int) -> int:
    result = session.run(Query(query, timeout=timeout_sec), params)
    record = result.single()
    return int(record["output_cardinality"])


def _run_neo4j_profile(session, query: str, params: dict[str, Any], timeout_sec: int):
    result = session.run(Query(query, timeout=timeout_sec), params)
    records = list(result)
    summary = result.consume()
    return records, summary


def _verify_neo4j_runtime(summary) -> bool:
    stack = []
    for root in [getattr(summary, "plan", None), getattr(summary, "profile", None)]:
        if root is not None:
            stack.append(root)
    while stack:
        node = stack.pop()
        arguments = getattr(node, "arguments", {}) or {}
        for key, value in arguments.items():
            if "runtime" not in str(key).lower():
                continue
            if isinstance(value, str) and value.upper() == "SLOTTED":
                return True
        stack.extend(getattr(node, "children", []) or [])
    return False


def _neo4j_profile_metrics(profile) -> dict[str, float]:
    def walk(node) -> tuple[float, float]:
        db_hits = float(getattr(node, "db_hits", 0.0) or 0.0)
        rows = float(getattr(node, "rows", 0.0) or 0.0)
        total_db_hits = db_hits
        total_rows = rows
        for child in getattr(node, "children", []) or []:
            child_db_hits, child_rows = walk(child)
            total_db_hits += child_db_hits
            total_rows += child_rows
        return total_db_hits, total_rows

    db_hits, rows = walk(profile)
    return {"db_hits": db_hits, "rows": rows}


def _base_result_row(
    engine: str,
    template: Template,
    binding: dict[str, str],
    join_order: str,
    join_order_class: str,
    instance_info: dict[str, Any],
) -> dict[str, Any]:
    cache_flush = instance_info["cache_flush"]
    return {
        "engine": engine,
        "template_id": template.template_id,
        "family": template.family,
        "regime": binding["regime"],
        "binding_group": binding["binding_group"],
        "binding_index": binding["binding_index"],
        "anchor_id": binding["anchor_id"],
        "join_order": join_order,
        "join_order_class": join_order_class,
        "status": "ok",
        "failure_stage": "",
        "failure_type": "",
        "timeout_value_sec": "",
        "completed_measured_runs": 0,
        "median_ms": "",
        "iqr_ms": "",
        "output_cardinality": "",
        "shared_hit_blocks": "",
        "db_hits": "",
        "intermediate_work_rows": "",
        "runtime_verified": "",
        "cache_flush_attempted": cache_flush["attempted"],
        "cache_flush_success": cache_flush["success"],
        "cache_flush_detail": cache_flush["detail"],
    }


def _write_logs(ctx: AppContext, log_dir: Path, rows: list[dict[str, Any]]) -> None:
    destination = ctx.path(log_dir)
    destination.mkdir(parents=True, exist_ok=True)
    with (destination / "instances.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    failures = [row for row in rows if row["status"] != "ok"]
    ctx.write_json(
        destination / "summary.json",
        {
            "instance_count": len(rows),
            "ok_count": sum(1 for row in rows if row["status"] == "ok"),
            "failure_count": len(failures),
            "failures": failures[:100],
        },
    )


def _progress_interval(total: int) -> int:
    return max(1, min(25, max(1, total // 10)))


def _write_join_order_figure(ctx: AppContext, rows: list[dict[str, Any]]) -> None:
    figure_dir = ctx.path(ctx.config["paths"]["experiments_figures_dir"])
    figure_dir.mkdir(parents=True, exist_ok=True)
    apply_plot_style(ctx)

    for filename in ["postgres_join_order_per_template.png", "postgres_join_order_runtime.png"]:
        path = figure_dir / filename
        if path.exists():
            path.unlink()
    output_path = figure_dir / "postgres_join_order_per_template.png"

    template_ids = sorted({str(row["template_id"]) for row in rows if row.get("median_ms", "") not in {"", None}})
    if not template_ids:
        write_figure_manifest(ctx, figure_dir)
        return

    fig, ax = plt.subplots(figsize=(max(12, len(template_ids) * 1.6), 7))
    offsets = {
        "default_plan": -0.18,
        "connected_prefix": 0.0,
        "cross_product_inducing": 0.18,
    }
    markers = {
        "default_plan": "D",
        "connected_prefix": "o",
        "cross_product_inducing": "^",
    }
    for join_class in ["default_plan", "connected_prefix", "cross_product_inducing"]:
        class_rows = [row for row in rows if row.get("join_order_class") == join_class and row.get("median_ms", "") not in {"", None}]
        if not class_rows:
            continue
        x_values = []
        y_values = []
        for row in class_rows:
            template_index = template_ids.index(str(row["template_id"]))
            x_values.append(template_index + offsets[join_class])
            y_values.append(float(row["median_ms"]))
        ax.scatter(
            x_values,
            y_values,
            s=42,
            alpha=0.8,
            marker=markers[join_class],
            color=NATURE_PALETTE[join_class],
            label=join_class,
        )
    ax.set_yscale("log")
    ax.set_ylabel("Median runtime (ms)")
    ax.set_xticks(range(len(template_ids)))
    ax.set_xticklabels(template_ids, rotation=45, ha="right")
    style_axes(ax)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(output_path, dpi=ctx.config["plotting"]["dpi"])
    plt.close(fig)
    write_figure_manifest(ctx, figure_dir)
