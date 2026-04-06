from __future__ import annotations

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
from .reporting import engine_label, fmt_int, fmt_num, join_class_label, template_label_map
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

BASELINE_RESULT_FIELDS = [
    "eng",
    "tid",
    "fam",
    "reg",
    "grp",
    "bid",
    "ord_idx",
    "join_cls",
    "status",
    "fail_stage",
    "fail_type",
    "med_ms",
    "iqr_ms",
    "out",
    "buf_hit",
    "db_hits",
    "work",
    "run_ok",
    "flush_ok",
]

JOIN_ORDER_RESULT_FIELDS = [
    "eng",
    "tid",
    "fam",
    "reg",
    "grp",
    "bid",
    "ord_idx",
    "join_cls",
    "status",
    "fail_stage",
    "fail_type",
    "med_ms",
    "iqr_ms",
    "out",
    "buf_hit",
    "db_hits",
    "work",
    "run_ok",
    "flush_ok",
]


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
        ctx.write_csv(
            output_dir / "postgres_baseline.csv",
            BASELINE_RESULT_FIELDS,
            rows,
        )
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
        ctx.write_csv(
            output_dir / "postgres_join_order.csv",
            JOIN_ORDER_RESULT_FIELDS,
            rows,
        )
        _write_logs(ctx, output_dir / "logs", rows)
        _write_join_order_figure(ctx, rows)


def _run_postgres_instances(
    ctx: AppContext,
    *,
    binding_rows: list[dict[str, str]],
    mode: str,
) -> list[dict[str, Any]]:
    selected_templates = load_selected_templates(ctx)
    label_map = template_label_map(selected_templates)
    templates = {label_map[template.template_id]: template for template in selected_templates}
    rows: list[dict[str, Any]] = []
    total_bindings = len(binding_rows)
    progress_every = _progress_interval(total_bindings)
    last_group: tuple[str, str] | None = None

    for binding_index, binding in enumerate(binding_rows, start=1):
        template = templates[binding["tid"]]
        group = (binding["tid"], binding["reg"])
        if group != last_group:
            print_status(f"PostgreSQL {mode}: {binding['tid']} / {binding['reg']}")
            last_group = group
        if binding_index == 1 or binding_index == total_bindings or binding_index % progress_every == 0:
            print_status(f"PostgreSQL {mode}: binding {binding_index}/{total_bindings}")
        if mode == "baseline":
            rows.append(_benchmark_postgres_instance(ctx, template, binding, None, 0))
            continue

        if binding["tid"] == "P2":
            continue

        rows.append(_benchmark_postgres_instance(ctx, template, binding, None, 0))
        for order_index, order in enumerate(all_left_deep_orders(template), start=1):
            rows.append(_benchmark_postgres_instance(ctx, template, binding, order, order_index))
    return rows


def _benchmark_postgres_instance(
    ctx: AppContext,
    template: Template,
    binding: dict[str, str],
    order: tuple[str, ...] | None,
    order_index: int,
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
        else:
            sql = default_count_sql(template)
            params = default_count_params(template, binding["anchor_id"])
            join_order_class = "default_plan"

        return _run_postgres_query_instance(
            ctx,
            conn=conn,
            sql=sql,
            params=params,
            template=template,
            binding=binding,
            order_index=order_index,
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
    order_index: int,
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

    result_row = _base_result_row("postgres", template, binding, order_index, join_order_class, instance_info)
    timings = []
    output_cardinality = None

    try:
        for _ in range(warmup_runs):
            output_cardinality = _execute_count_sql(conn, sql, params)
    except Exception as exc:
        result_row.update(
            {
                "status": "fail",
                "fail_stage": "warmup",
                "fail_type": type(exc).__name__,
                "med_ms": "",
                "iqr_ms": "",
                "out": "",
                "buf_hit": "",
                "work": "",
            }
        )
        return result_row

    try:
        for index in range(measured_runs):
            started = time.perf_counter()
            output_cardinality = _execute_count_sql(conn, sql, params)
            timings.append((time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        result_row.update(
            {
                "status": "fail",
                "fail_stage": "measured",
                "fail_type": type(exc).__name__,
                "med_ms": "",
                "iqr_ms": "",
                "out": "",
                "buf_hit": "",
                "work": "",
            }
        )
        return result_row

    result_row["med_ms"] = fmt_num(statistics.median(timings))
    result_row["iqr_ms"] = fmt_num(float(np.percentile(timings, 75) - np.percentile(timings, 25)))
    result_row["out"] = fmt_int(output_cardinality)

    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {instrumented_timeout}")
        plan = explain_json(conn, sql, params)
        metrics = plan_metrics(plan)
        result_row["buf_hit"] = fmt_num(metrics["shared_hit_blocks"])
        result_row["work"] = fmt_num(metrics["intermediate_work_rows"])
        result_row["status"] = "ok"
    except Exception as exc:
        result_row.update(
            {
                "status": "inst_fail",
                "fail_stage": "inst",
                "fail_type": type(exc).__name__,
                "buf_hit": "",
                "work": "",
            }
        )
    return result_row


def _execute_count_sql(conn, sql: str, params: list[Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.fetchone()[0])


def run_neo4j_baseline(ctx: AppContext) -> None:
    selected_templates = load_selected_templates(ctx)
    label_map = template_label_map(selected_templates)
    templates = {label_map[template.template_id]: template for template in selected_templates}
    bindings = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    print_status(f"Neo4j baseline: benchmarking {len(bindings)} query instances")
    rows = []
    total_bindings = len(bindings)
    progress_every = _progress_interval(total_bindings)
    last_group: tuple[str, str] | None = None
    for binding_index, binding in enumerate(bindings, start=1):
        template = templates[binding["tid"]]
        group = (binding["tid"], binding["reg"])
        if group != last_group:
            print_status(f"Neo4j baseline: {binding['tid']} / {binding['reg']}")
            last_group = group
        if binding_index == 1 or binding_index == total_bindings or binding_index % progress_every == 0:
            print_status(f"Neo4j baseline: binding {binding_index}/{total_bindings}")
        rows.append(_benchmark_neo4j_instance(ctx, template, binding))
    if rows:
        output_dir = Path(ctx.config["paths"]["neo4j_baseline_dir"])
        print_status("Neo4j baseline: writing benchmark outputs")
        ctx.write_csv(
            output_dir / "neo4j_baseline.csv",
            BASELINE_RESULT_FIELDS,
            rows,
        )
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
    result_row = _base_result_row("neo4j", template, binding, 0, "default_plan", instance_info)
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
                        "status": "fail",
                        "fail_stage": "warmup",
                        "fail_type": type(exc).__name__,
                        "med_ms": "",
                        "iqr_ms": "",
                        "out": "",
                        "db_hits": "",
                        "work": "",
                        "run_ok": "false",
                    }
                )
                return result_row

            try:
                for index in range(measured_runs):
                    started = time.perf_counter()
                    output_cardinality = _run_neo4j_count(session, plain_query, params, plain_timeout)
                    timings.append((time.perf_counter() - started) * 1000.0)
            except Exception as exc:
                result_row.update(
                    {
                        "status": "fail",
                        "fail_stage": "measured",
                        "fail_type": type(exc).__name__,
                        "med_ms": "",
                        "iqr_ms": "",
                        "out": "",
                        "db_hits": "",
                        "work": "",
                        "run_ok": "false",
                    }
                )
                return result_row

            result_row["med_ms"] = fmt_num(statistics.median(timings))
            result_row["iqr_ms"] = fmt_num(float(np.percentile(timings, 75) - np.percentile(timings, 25)))
            result_row["out"] = fmt_int(output_cardinality)

            try:
                records, summary = _run_neo4j_profile(session, profile_query, params, int(ctx.config["benchmark"]["instrumented_timeout_sec"]))
                result_row["out"] = fmt_int(records[0]["output_cardinality"])
                runtime_ok = _verify_neo4j_runtime(summary)
                result_row["run_ok"] = str(runtime_ok).lower()
                if runtime_ok:
                    profile_metrics = _neo4j_profile_metrics(summary.profile)
                    result_row["db_hits"] = fmt_num(profile_metrics["db_hits"])
                    result_row["work"] = fmt_num(profile_metrics["rows"])
                    result_row["status"] = "ok"
                else:
                    result_row["status"] = "inst_fail"
                    result_row["fail_stage"] = "inst"
                    result_row["fail_type"] = "RuntimeCheck"
                    result_row["db_hits"] = ""
                    result_row["work"] = ""
            except Exception as exc:
                result_row.update(
                    {
                        "status": "inst_fail",
                        "fail_stage": "inst",
                        "fail_type": type(exc).__name__,
                        "db_hits": "",
                        "work": "",
                        "run_ok": "false",
                    }
                )
    except Exception as exc:
        result_row.update(
            {
                "status": "fail",
                "fail_stage": "session",
                "fail_type": type(exc).__name__,
                "med_ms": "",
                "iqr_ms": "",
                "out": "",
                "db_hits": "",
                "work": "",
                "run_ok": "false",
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
    order_index: int,
    join_order_class: str,
    instance_info: dict[str, Any],
) -> dict[str, Any]:
    cache_flush = instance_info["cache_flush"]
    return {
        "eng": engine_label(engine),
        "tid": binding["tid"],
        "fam": binding["fam"],
        "reg": binding["reg"],
        "grp": binding["grp"],
        "bid": binding["bid"],
        "ord_idx": fmt_int(order_index),
        "join_cls": join_class_label(join_order_class),
        "status": "ok",
        "fail_stage": "",
        "fail_type": "",
        "med_ms": "",
        "iqr_ms": "",
        "out": "",
        "buf_hit": "",
        "db_hits": "",
        "work": "",
        "run_ok": "",
        "flush_ok": str(bool(cache_flush["success"])).lower(),
    }


def _write_logs(ctx: AppContext, log_dir: Path, rows: list[dict[str, Any]]) -> None:
    destination = ctx.path(log_dir)
    destination.mkdir(parents=True, exist_ok=True)
    failures = [row for row in rows if row["status"] != "ok"]
    ctx.write_json(
        destination / "summary.json",
        {
            "instance_count": len(rows),
            "ok_count": sum(1 for row in rows if row["status"] == "ok"),
            "failure_count": len(failures),
            "failures": failures[:20],
        },
    )


def _progress_interval(total: int) -> int:
    return max(1, min(25, max(1, total // 10)))


def _write_join_order_figure(ctx: AppContext, rows: list[dict[str, Any]]) -> None:
    figure_dir = ctx.path(ctx.config["paths"]["experiments_figures_dir"])
    figure_dir.mkdir(parents=True, exist_ok=True)
    apply_plot_style(ctx)

    for filename in ["join_order_effect.png", "postgres_join_order_per_template.png", "postgres_join_order_runtime.png"]:
        path = figure_dir / filename
        if path.exists():
            path.unlink()
    output_path = figure_dir / "join_order_effect.png"

    template_ids = sorted({str(row["tid"]) for row in rows if row.get("med_ms", "") not in {"", None}})
    if not template_ids:
        write_figure_manifest(ctx, figure_dir)
        return

    fig, ax = plt.subplots(figsize=(max(12, len(template_ids) * 1.6), 7))
    offsets = {
        "default": -0.18,
        "connected": 0.0,
        "cross": 0.18,
    }
    markers = {
        "default": "D",
        "connected": "o",
        "cross": "^",
    }
    for join_class in ["default", "connected", "cross"]:
        class_rows = [row for row in rows if row.get("join_cls") == join_class and row.get("med_ms", "") not in {"", None}]
        if not class_rows:
            continue
        x_values = []
        y_values = []
        for row in class_rows:
            template_index = template_ids.index(str(row["tid"]))
            x_values.append(template_index + offsets[join_class])
            y_values.append(float(row["med_ms"]))
        ax.scatter(
            x_values,
            y_values,
            s=42,
            alpha=0.8,
            marker=markers[join_class],
            color=NATURE_PALETTE[
                {
                    "default": "default_plan",
                    "connected": "connected_prefix",
                    "cross": "cross_product_inducing",
                }[join_class]
            ],
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
