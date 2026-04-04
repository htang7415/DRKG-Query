from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, print_status


def run_postprocess(ctx: AppContext) -> None:
    print_status("Analysis: reading baseline, theory, comparison, and join-order tables")
    templates = {template.template_id: template for template in load_selected_templates(ctx)}
    baseline_pg = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    baseline_neo = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    theory_rows = read_csv_rows(ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv")
    comparison_rows = read_csv_rows(ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_tables.csv")
    join_order_path = ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv"
    join_rows = read_csv_rows(join_order_path) if join_order_path.exists() else []

    theory_index = {
        (row["template_id"], row["regime"], row["anchor_id"]): row
        for row in theory_rows
    }
    acyclic_by_template = {}
    for row in theory_rows:
        acyclic_by_template.setdefault(row["template_id"], _parse_bool(row.get("acyclic", "")))

    instance_rows = []
    for row in baseline_pg + baseline_neo:
        theory = theory_index.get((row["template_id"], row["regime"], row["anchor_id"]), {})
        template = templates[row["template_id"]]
        runtime = _safe_float(row["median_ms"])
        work = _safe_float(row["intermediate_work_rows"])
        output_cardinality = _safe_float(row["output_cardinality"])
        agm_bound = _safe_float(theory.get("agm_bound", ""))
        acyclic = _parse_bool(theory.get("acyclic", template.family == "path"))
        instance_rows.append(
            {
                "engine": row["engine"],
                "template_id": row["template_id"],
                "family": template.family,
                "edge_count": template.edge_count,
                "regime": row["regime"],
                "anchor_id": row["anchor_id"],
                "status": row["status"],
                "median_ms": row["median_ms"],
                "iqr_ms": row["iqr_ms"],
                "output_cardinality": row["output_cardinality"],
                "intermediate_work_rows": row["intermediate_work_rows"],
                "agm_bound": theory.get("agm_bound", ""),
                "lp_status": theory.get("lp_status", ""),
                "fractional_cover": theory.get("fractional_cover", ""),
                "acyclic": acyclic,
                "runtime_over_agm": _ratio(runtime, agm_bound),
                "work_over_agm": _ratio(work, agm_bound),
                "agm_over_output_cardinality": _ratio(agm_bound, output_cardinality),
            }
        )

    summary_dir = Path(ctx.config["paths"]["analysis_summary_dir"])
    print_status("Analysis: writing instance-level theory/runtime table")
    ctx.write_csv(summary_dir / "instance_theory_runtime.csv", list(instance_rows[0].keys()) if instance_rows else ["engine"], instance_rows)

    template_summary_rows = _aggregate_rows(
        instance_rows,
        group_keys=["engine", "template_id", "family", "edge_count", "regime", "acyclic"],
    )
    print_status("Analysis: writing template and family summaries")
    ctx.write_csv(summary_dir / "summary_tables.csv", list(template_summary_rows[0].keys()) if template_summary_rows else ["engine"], template_summary_rows)
    ctx.write_csv(summary_dir / "template_engine_summary.csv", list(template_summary_rows[0].keys()) if template_summary_rows else ["engine"], template_summary_rows)

    family_summary_rows = _aggregate_rows(
        instance_rows,
        group_keys=["engine", "family", "edge_count", "regime", "acyclic"],
    )
    ctx.write_csv(summary_dir / "family_regime_summary.csv", list(family_summary_rows[0].keys()) if family_summary_rows else ["engine"], family_summary_rows)

    cyclicity_summary_rows = _aggregate_rows(
        [row for row in instance_rows if int(row["edge_count"]) in {3, 4}],
        group_keys=["engine", "edge_count", "family", "acyclic", "regime"],
    )
    ctx.write_csv(summary_dir / "cyclicity_contrast_summary.csv", list(cyclicity_summary_rows[0].keys()) if cyclicity_summary_rows else ["engine"], cyclicity_summary_rows)

    join_summary_rows = _aggregate_rows(
        [
            {
                "engine": row["engine"],
                "template_id": row["template_id"],
                "family": row["family"],
                "edge_count": _edge_count_from_template_id(row["template_id"]),
                "regime": row["regime"],
                "join_order_class": row["join_order_class"],
                "join_order": row["join_order"],
                "acyclic": acyclic_by_template.get(row["template_id"], False),
                "status": row["status"],
                "median_ms": row["median_ms"],
                "iqr_ms": row["iqr_ms"],
                "output_cardinality": row["output_cardinality"],
                "intermediate_work_rows": row["intermediate_work_rows"],
                "agm_bound": "",
                "lp_status": "",
                "fractional_cover": "",
                "runtime_over_agm": "",
                "work_over_agm": "",
                "agm_over_output_cardinality": "",
            }
            for row in join_rows
        ],
        group_keys=["template_id", "join_order_class", "join_order"],
    )
    ctx.write_csv(summary_dir / "join_order_summary.csv", list(join_summary_rows[0].keys()) if join_summary_rows else ["template_id"], join_summary_rows)

    comparison_summary_rows = _aggregate_comparison_rows(comparison_rows)
    print_status("Analysis: writing comparison and join-order summaries")
    ctx.write_csv(
        summary_dir / "comparison_summary.csv",
        list(comparison_summary_rows[0].keys()) if comparison_summary_rows else ["template_id"],
        comparison_summary_rows,
    )

    print_status("Analysis: writing summary_metrics.json")
    ctx.write_json(
        summary_dir / "summary_metrics.json",
        {
            "postgres_baseline_instances": len(baseline_pg),
            "neo4j_baseline_instances": len(baseline_neo),
            "theory_instances": len(theory_rows),
            "comparison_instances": len(comparison_rows),
            "join_order_instances": len(join_rows),
            "instance_theory_runtime_rows": len(instance_rows),
            "template_engine_summary_rows": len(template_summary_rows),
            "family_regime_summary_rows": len(family_summary_rows),
            "cyclicity_contrast_summary_rows": len(cyclicity_summary_rows),
            "join_order_summary_rows": len(join_summary_rows),
        },
    )


def _aggregate_rows(
    rows: list[dict[str, object]],
    *,
    group_keys: list[str],
) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = tuple(row[key_name] for key_name in group_keys)
        grouped[key].append(row)

    summaries = []
    for key, group_rows in grouped.items():
        summary = {group_keys[idx]: key[idx] for idx in range(len(group_keys))}
        runtime_values = _float_series(group_rows, "median_ms")
        work_values = _float_series(group_rows, "intermediate_work_rows")
        output_values = _float_series(group_rows, "output_cardinality")
        agm_values = _float_series(group_rows, "agm_bound")
        runtime_over_agm = _float_series(group_rows, "runtime_over_agm")
        work_over_agm = _float_series(group_rows, "work_over_agm")
        agm_over_output = _float_series(group_rows, "agm_over_output_cardinality")
        summary.update(
            {
                "instance_count": len(group_rows),
                "ok_count": sum(1 for row in group_rows if row.get("status") == "ok"),
                "runtime_q1_ms": _stat_or_blank(runtime_values, 25),
                "median_runtime_ms": _stat_or_blank(runtime_values, 50),
                "runtime_q3_ms": _stat_or_blank(runtime_values, 75),
                "work_q1": _stat_or_blank(work_values, 25),
                "median_work": _stat_or_blank(work_values, 50),
                "work_q3": _stat_or_blank(work_values, 75),
                "median_output_cardinality": _stat_or_blank(output_values, 50),
                "median_agm_bound": _stat_or_blank(agm_values, 50),
                "median_runtime_over_agm": _stat_or_blank(runtime_over_agm, 50),
                "median_work_over_agm": _stat_or_blank(work_over_agm, 50),
                "median_agm_over_output_cardinality": _stat_or_blank(agm_over_output, 50),
            }
        )
        summaries.append(summary)
    return sorted(summaries, key=lambda row: tuple(str(row[key]) for key in group_keys))


def _aggregate_comparison_rows(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["template_id"], row["family"], row["regime"])].append(row)

    summaries = []
    for key, group_rows in grouped.items():
        speedups = _float_series(group_rows, "neo4j_over_postgres_speedup")
        summaries.append(
            {
                "template_id": key[0],
                "family": key[1],
                "regime": key[2],
                "instance_count": len(group_rows),
                "matching_output_cardinality_instances": sum(1 for row in group_rows if str(row["output_cardinality_match"]).lower() == "true"),
                "speedup_q1": _stat_or_blank(speedups, 25),
                "median_speedup": _stat_or_blank(speedups, 50),
                "speedup_q3": _stat_or_blank(speedups, 75),
            }
        )
    return sorted(summaries, key=lambda row: (row["template_id"], row["regime"]))


def _float_series(rows: list[dict[str, object]], field: str) -> list[float]:
    values = []
    for row in rows:
        value = _safe_float(row.get(field, ""))
        if value is not None:
            values.append(value)
    return values


def _safe_float(value: object) -> float | None:
    if value in {"", None}:
        return None
    return float(value)


def _ratio(numerator: float | None, denominator: float | None) -> str:
    if numerator is None or denominator is None or denominator <= 0:
        return ""
    return str(numerator / denominator)


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def _stat_or_blank(values: list[float], percentile: int) -> str:
    if not values:
        return ""
    array = np.asarray(values, dtype=float)
    if percentile == 50:
        return str(float(np.median(array)))
    return str(float(np.percentile(array, percentile)))


def _edge_count_from_template_id(template_id: str) -> int:
    if template_id.startswith("path_"):
        return int(template_id.split("_", 2)[1])
    if template_id.startswith("triangle_"):
        return 3
    if template_id.startswith("cycle_4_"):
        return 4
    raise ValueError(f"Unrecognized template_id: {template_id}")
