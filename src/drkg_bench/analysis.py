from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .reporting import fmt_int, fmt_num


def run_postprocess(ctx: AppContext) -> None:
    print_status("Analysis: reading baseline, theory, comparison, and join-order tables")
    baseline_pg = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    baseline_neo = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    theory_rows = read_csv_rows(ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv")
    comparison_rows = read_csv_rows(ctx.path(ctx.config["paths"]["comparison_dir"]) / "engine_summary.csv")
    join_rows = read_csv_rows(ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv")

    theory_index = {
        (row["tid"], row["reg"], row["bid"]): row
        for row in theory_rows
    }

    instance_rows = []
    for row in baseline_pg + baseline_neo:
        theory = theory_index.get((row["tid"], row["reg"], row["bid"]), {})
        med_ms = _safe_float(row.get("med_ms", ""))
        work = _safe_float(row.get("work", ""))
        out = _safe_float(row.get("out", ""))
        agm = _safe_float(theory.get("agm", ""))
        shape = "acyclic" if theory.get("acyclic", "") == "true" else "cyclic"
        instance_rows.append(
            {
                "eng": row["eng"],
                "tid": row["tid"],
                "fam": row["fam"],
                "reg": row["reg"],
                "bid": row["bid"],
                "shape": shape,
                "med_ms": row.get("med_ms", ""),
                "iqr_ms": row.get("iqr_ms", ""),
                "out": row.get("out", ""),
                "work": row.get("work", ""),
                "agm": theory.get("agm", ""),
                "rt_agm": fmt_num(_ratio(med_ms, agm)),
                "work_agm": fmt_num(_ratio(work, agm)),
                "agm_out": fmt_num(_ratio(agm, out)),
                "status": row.get("status", ""),
            }
        )

    summary_dir = Path(ctx.config["paths"]["analysis_summary_dir"])
    ctx.write_csv(
        summary_dir / "instance_summary.csv",
        ["eng", "tid", "fam", "reg", "bid", "shape", "med_ms", "iqr_ms", "out", "work", "agm", "rt_agm", "work_agm", "agm_out", "status"],
        instance_rows,
    )

    template_summary_rows = _aggregate_rows(
        instance_rows,
        group_keys=["eng", "tid", "fam", "reg", "shape"],
    )
    ctx.write_csv(
        summary_dir / "template_summary.csv",
        ["eng", "tid", "fam", "reg", "shape", "n", "ok_n", "q1_ms", "med_ms", "q3_ms", "q1_work", "med_work", "q3_work", "med_out", "med_agm"],
        template_summary_rows,
    )

    structure_rows = _aggregate_rows(
        [row for row in instance_rows if row["tid"] in {"P3", "P4", "T1", "T2", "C4"}],
        group_keys=["eng", "reg", "shape"],
    )
    ctx.write_csv(
        summary_dir / "structure_summary.csv",
        ["eng", "reg", "shape", "n", "ok_n", "q1_ms", "med_ms", "q3_ms", "q1_work", "med_work", "q3_work", "med_out", "med_agm"],
        structure_rows,
    )

    join_summary_rows = _aggregate_rows(
        join_rows,
        group_keys=["tid", "join_cls"],
        metric_map={"med_ms": "med_ms", "work": "work", "out": "out"},
    )
    ctx.write_csv(
        summary_dir / "join_order_summary.csv",
        ["tid", "join_cls", "n", "ok_n", "q1_ms", "med_ms", "q3_ms", "q1_work", "med_work", "q3_work", "med_out", "med_agm"],
        join_summary_rows,
    )

    ctx.write_json(
        summary_dir / "summary_metrics.json",
        {
            "postgres_baseline_instances": len(baseline_pg),
            "neo4j_baseline_instances": len(baseline_neo),
            "theory_instances": len(theory_rows),
            "comparison_rows": len(comparison_rows),
            "join_order_instances": len(join_rows),
            "instance_rows": len(instance_rows),
            "template_summary_rows": len(template_summary_rows),
            "structure_summary_rows": len(structure_rows),
            "join_order_summary_rows": len(join_summary_rows),
        },
    )


def _aggregate_rows(
    rows: list[dict[str, object]],
    *,
    group_keys: list[str],
    metric_map: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    grouped: dict[tuple[object, ...], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row[key] for key in group_keys)].append(row)

    metric_map = metric_map or {"med_ms": "med_ms", "work": "work", "out": "out", "agm": "agm"}
    summaries = []
    for key, group_rows in grouped.items():
        summary = {group_keys[idx]: key[idx] for idx in range(len(group_keys))}
        runtime_values = _float_series(group_rows, metric_map["med_ms"])
        work_values = _float_series(group_rows, metric_map["work"])
        out_values = _float_series(group_rows, metric_map["out"])
        agm_values = _float_series(group_rows, metric_map.get("agm", "agm"))
        summary.update(
            {
                "n": fmt_int(len(group_rows)),
                "ok_n": fmt_int(sum(1 for row in group_rows if row.get("status", "ok") == "ok")),
                "q1_ms": fmt_num(_percentile(runtime_values, 25)),
                "med_ms": fmt_num(_percentile(runtime_values, 50)),
                "q3_ms": fmt_num(_percentile(runtime_values, 75)),
                "q1_work": fmt_num(_percentile(work_values, 25)),
                "med_work": fmt_num(_percentile(work_values, 50)),
                "q3_work": fmt_num(_percentile(work_values, 75)),
                "med_out": fmt_num(_percentile(out_values, 50)),
                "med_agm": fmt_num(_percentile(agm_values, 50)),
            }
        )
        summaries.append(summary)
    return sorted(summaries, key=lambda row: tuple(str(row[key]) for key in group_keys))


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


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    array = np.asarray(values, dtype=float)
    if percentile == 50:
        return float(np.median(array))
    return float(np.percentile(array, percentile))
