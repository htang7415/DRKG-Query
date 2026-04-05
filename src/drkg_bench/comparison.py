from __future__ import annotations

import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .plotting import NATURE_PALETTE, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest
from .reporting import fmt_int, fmt_num


def run_engine_comparison(ctx: AppContext) -> None:
    print_status("Comparison: reading PostgreSQL and Neo4j baseline tables")
    pg_rows = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    neo_rows = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    neo_index = {
        (row["tid"], row["reg"], row["bid"]): row
        for row in neo_rows
    }

    matched_rows = []
    for pg in pg_rows:
        key = (pg["tid"], pg["reg"], pg["bid"])
        neo = neo_index.get(key)
        if neo is None:
            continue
        pg_ms = _safe_float(pg.get("med_ms", ""))
        neo_ms = _safe_float(neo.get("med_ms", ""))
        matched_rows.append(
            {
                "tid": pg["tid"],
                "reg": pg["reg"],
                "bid": pg["bid"],
                "pg_ms": pg_ms,
                "neo_ms": neo_ms,
                "pg_out": pg.get("out", ""),
                "neo_out": neo.get("out", ""),
                "out_match": bool(pg.get("out")) and pg.get("out") == neo.get("out"),
                "spd": (pg_ms / neo_ms) if pg_ms and neo_ms and neo_ms > 0 else None,
            }
        )

    summary_rows = _summarize_matches(matched_rows)
    comparison_dir = Path(ctx.config["paths"]["comparison_dir"])
    ctx.write_csv(
        comparison_dir / "engine_summary.csv",
        [
            "tid",
            "reg",
            "n",
            "match_n",
            "pg_q1",
            "pg_ms",
            "pg_q3",
            "neo_q1",
            "neo_ms",
            "neo_q3",
            "spd_q1",
            "spd",
            "spd_q3",
        ],
        summary_rows,
    )

    speedups = [row["spd"] for row in matched_rows if row["spd"] is not None]
    ctx.write_json(
        comparison_dir / "comparison_metrics.json",
        {
            "matched_instances": len(matched_rows),
            "matching_output_cardinality_instances": sum(1 for row in matched_rows if row["out_match"]),
            "mean_neo_over_pg_speedup": statistics.mean(speedups) if speedups else None,
        },
    )
    print_status(f"Comparison: matched {len(matched_rows)} baseline instances; writing figures")
    _write_experiment_figures(ctx, summary_rows)


def _summarize_matches(rows: list[dict[str, object]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["tid"]), str(row["reg"]))].append(row)

    summary_rows = []
    for (tid, reg), group_rows in sorted(grouped.items()):
        pg_values = [float(row["pg_ms"]) for row in group_rows if row["pg_ms"] is not None]
        neo_values = [float(row["neo_ms"]) for row in group_rows if row["neo_ms"] is not None]
        spd_values = [float(row["spd"]) for row in group_rows if row["spd"] is not None]
        summary_rows.append(
            {
                "tid": tid,
                "reg": reg,
                "n": fmt_int(len(group_rows)),
                "match_n": fmt_int(sum(1 for row in group_rows if row["out_match"])),
                "pg_q1": fmt_num(_percentile(pg_values, 25)),
                "pg_ms": fmt_num(_percentile(pg_values, 50)),
                "pg_q3": fmt_num(_percentile(pg_values, 75)),
                "neo_q1": fmt_num(_percentile(neo_values, 25)),
                "neo_ms": fmt_num(_percentile(neo_values, 50)),
                "neo_q3": fmt_num(_percentile(neo_values, 75)),
                "spd_q1": fmt_num(_percentile(spd_values, 25)),
                "spd": fmt_num(_percentile(spd_values, 50)),
                "spd_q3": fmt_num(_percentile(spd_values, 75)),
            }
        )
    return summary_rows


def _write_experiment_figures(ctx: AppContext, summary_rows: list[dict[str, str]]) -> None:
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["experiments_figures_dir"])
    remove_existing_figures(
        figure_dir,
        [
            "engine_runtime.png",
            "speedup.png",
            "engine_runtime_grouped.png",
            "engine_work_grouped.png",
            "engine_speedup_by_template.png",
            "regime_runtime_by_engine.png",
            "postgres_baseline_runtime.png",
            "neo4j_baseline_runtime.png",
            "engine_runtime_ratio.png",
        ],
    )
    _engine_runtime_figure(figure_dir / "engine_runtime.png", summary_rows, dpi=int(ctx.config["plotting"]["dpi"]))
    _speedup_figure(figure_dir / "speedup.png", summary_rows, dpi=int(ctx.config["plotting"]["dpi"]))
    write_figure_manifest(ctx, figure_dir)


def _engine_runtime_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    if not rows:
        return
    regimes = [reg for reg in ["uniform", "hub"] if any(row["reg"] == reg for row in rows)]
    tids = sorted({row["tid"] for row in rows})
    fig, axes = plt.subplots(1, len(regimes), figsize=(max(12, len(tids) * 1.5), 5.6), squeeze=False)

    for axis, reg in zip(axes[0], regimes, strict=True):
        reg_rows = {row["tid"]: row for row in rows if row["reg"] == reg}
        x_values = np.arange(len(tids))
        width = 0.34
        pg_values = [float(reg_rows[tid]["pg_ms"]) if tid in reg_rows and reg_rows[tid]["pg_ms"] else np.nan for tid in tids]
        neo_values = [float(reg_rows[tid]["neo_ms"]) if tid in reg_rows and reg_rows[tid]["neo_ms"] else np.nan for tid in tids]
        axis.bar(x_values - width / 2, pg_values, width=width, color=NATURE_PALETTE["postgres"], label="pg")
        axis.bar(x_values + width / 2, neo_values, width=width, color=NATURE_PALETTE["neo4j"], label="neo")
        axis.set_yscale("log")
        axis.set_ylabel("Median ms")
        axis.set_xticks(x_values)
        axis.set_xticklabels(tids)
        axis.text(0.02, 0.96, reg, transform=axis.transAxes, va="top", ha="left", fontsize=14)
        style_axes(axis)
        axis.legend()

    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _speedup_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    if not rows:
        return
    regimes = [reg for reg in ["uniform", "hub"] if any(row["reg"] == reg for row in rows)]
    tids = sorted({row["tid"] for row in rows})
    fig, axes = plt.subplots(1, len(regimes), figsize=(max(12, len(tids) * 1.5), 5.2), squeeze=False)

    for axis, reg in zip(axes[0], regimes, strict=True):
        reg_rows = {row["tid"]: row for row in rows if row["reg"] == reg}
        values = [float(reg_rows[tid]["spd"]) if tid in reg_rows and reg_rows[tid]["spd"] else np.nan for tid in tids]
        q1_values = [float(reg_rows[tid]["spd_q1"]) if tid in reg_rows and reg_rows[tid]["spd_q1"] else np.nan for tid in tids]
        q3_values = [float(reg_rows[tid]["spd_q3"]) if tid in reg_rows and reg_rows[tid]["spd_q3"] else np.nan for tid in tids]
        errors_low = [max(0.0, val - q1) if not np.isnan(val) and not np.isnan(q1) else np.nan for val, q1 in zip(values, q1_values, strict=True)]
        errors_high = [max(0.0, q3 - val) if not np.isnan(val) and not np.isnan(q3) else np.nan for val, q3 in zip(values, q3_values, strict=True)]
        axis.bar(
            np.arange(len(tids)),
            values,
            yerr=np.asarray([errors_low, errors_high]),
            capsize=3,
            color=NATURE_PALETTE["neutral"],
            width=0.64,
        )
        axis.axhline(1.0, color=NATURE_PALETTE["frame"], linewidth=1.0, linestyle="--")
        axis.set_ylabel("pg / neo")
        axis.set_xticks(np.arange(len(tids)))
        axis.set_xticklabels(tids)
        axis.text(0.02, 0.96, reg, transform=axis.transAxes, va="top", ha="left", fontsize=14)
        style_axes(axis)

    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    array = np.asarray(values, dtype=float)
    if percentile == 50:
        return float(np.median(array))
    return float(np.percentile(array, percentile))


def _safe_float(raw_value: object) -> float | None:
    if raw_value in {"", None}:
        return None
    return float(raw_value)
