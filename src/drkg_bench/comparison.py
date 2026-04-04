from __future__ import annotations

import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .plotting import NATURE_PALETTE, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest


def run_engine_comparison(ctx: AppContext) -> None:
    print_status("Comparison: reading PostgreSQL and Neo4j baseline tables")
    pg_rows = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    neo4j_rows = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    neo4j_index = {
        (row["template_id"], row["regime"], row["anchor_id"]): row
        for row in neo4j_rows
    }
    comparison_rows = []
    for pg in pg_rows:
        key = (pg["template_id"], pg["regime"], pg["anchor_id"])
        if key not in neo4j_index:
            continue
        neo = neo4j_index[key]
        pg_median = _safe_float(pg["median_ms"])
        neo_median = _safe_float(neo["median_ms"])
        output_match = bool(pg["output_cardinality"]) and pg["output_cardinality"] == neo["output_cardinality"]
        comparison_rows.append(
            {
                "template_id": pg["template_id"],
                "family": pg["family"],
                "regime": pg["regime"],
                "anchor_id": pg["anchor_id"],
                "postgres_status": pg["status"],
                "neo4j_status": neo["status"],
                "postgres_median_ms": pg["median_ms"],
                "neo4j_median_ms": neo["median_ms"],
                "postgres_iqr_ms": pg["iqr_ms"],
                "neo4j_iqr_ms": neo["iqr_ms"],
                "postgres_output_cardinality": pg["output_cardinality"],
                "neo4j_output_cardinality": neo["output_cardinality"],
                "output_cardinality_match": output_match,
                "neo4j_over_postgres_speedup": (pg_median / neo_median) if pg_median and neo_median and neo_median > 0 else "",
            }
        )

    comparison_dir = Path(ctx.config["paths"]["comparison_dir"])
    fieldnames = [
        "template_id",
        "family",
        "regime",
        "anchor_id",
        "postgres_status",
        "neo4j_status",
        "postgres_median_ms",
        "neo4j_median_ms",
        "postgres_iqr_ms",
        "neo4j_iqr_ms",
        "postgres_output_cardinality",
        "neo4j_output_cardinality",
        "output_cardinality_match",
        "neo4j_over_postgres_speedup",
    ]
    ctx.write_csv(comparison_dir / "comparison_tables.csv", fieldnames, comparison_rows)
    avg_speedup = [
        float(row["neo4j_over_postgres_speedup"])
        for row in comparison_rows
        if row["neo4j_over_postgres_speedup"] != ""
    ]
    ctx.write_json(
        comparison_dir / "comparison_metrics.json",
        {
            "matched_instances": len(comparison_rows),
            "matching_output_cardinality_instances": sum(1 for row in comparison_rows if row["output_cardinality_match"]),
            "mean_neo4j_over_postgres_speedup": statistics.mean(avg_speedup) if avg_speedup else None,
        },
    )
    print_status(f"Comparison: matched {len(comparison_rows)} baseline instances; writing figures")
    _write_experiment_figures(ctx, pg_rows, neo4j_rows, comparison_rows)


def _write_experiment_figures(
    ctx: AppContext,
    pg_rows: list[dict[str, str]],
    neo4j_rows: list[dict[str, str]],
    comparison_rows: list[dict[str, object]],
) -> None:
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["experiments_figures_dir"])
    remove_existing_figures(
        figure_dir,
        [
            "engine_runtime_grouped.png",
            "engine_work_grouped.png",
            "engine_speedup_by_template.png",
            "regime_runtime_by_engine.png",
            "postgres_baseline_runtime.png",
            "neo4j_baseline_runtime.png",
            "engine_runtime_ratio.png",
        ],
    )

    runtime_summary = _aggregate_metric(
        pg_rows + neo4j_rows,
        group_keys=["engine", "template_id", "regime"],
        metric_key="median_ms",
    )
    work_summary = _aggregate_metric(
        pg_rows + neo4j_rows,
        group_keys=["engine", "template_id", "regime"],
        metric_key="intermediate_work_rows",
    )
    speedup_summary = _aggregate_metric(
        comparison_rows,
        group_keys=["template_id", "regime"],
        metric_key="neo4j_over_postgres_speedup",
    )

    _grouped_engine_metric_figure(
        figure_dir / "engine_runtime_grouped.png",
        runtime_summary,
        metric_label="Median runtime (ms)",
        metric_field="median",
        y_log=True,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _grouped_engine_metric_figure(
        figure_dir / "engine_work_grouped.png",
        work_summary,
        metric_label="Intermediate work",
        metric_field="median",
        y_log=True,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _speedup_figure(
        figure_dir / "engine_speedup_by_template.png",
        speedup_summary,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _regime_runtime_figure(
        figure_dir / "regime_runtime_by_engine.png",
        runtime_summary,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )

    write_figure_manifest(ctx, figure_dir)


def _aggregate_metric(
    rows: list[dict[str, object]],
    *,
    group_keys: list[str],
    metric_key: str,
) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], list[float]] = defaultdict(list)
    for row in rows:
        raw_value = row.get(metric_key, "")
        value = _safe_float(raw_value)
        if value is None:
            continue
        key = tuple(row[key_name] for key_name in group_keys)
        grouped[key].append(value)

    summaries = []
    for key, values in grouped.items():
        q1, median, q3 = _quartiles(values)
        entry = {group_keys[idx]: key[idx] for idx in range(len(group_keys))}
        entry.update(
            {
                "count": len(values),
                "q1": q1,
                "median": median,
                "q3": q3,
            }
        )
        summaries.append(entry)
    return sorted(summaries, key=lambda row: tuple(str(row[key]) for key in group_keys))


def _grouped_engine_metric_figure(
    path: Path,
    rows: list[dict[str, object]],
    *,
    metric_label: str,
    metric_field: str,
    y_log: bool,
    dpi: int,
) -> None:
    if not rows:
        return

    labels = sorted({(str(row["template_id"]), str(row["regime"])) for row in rows})
    label_positions = {label: idx for idx, label in enumerate(labels)}
    fig, ax = plt.subplots(figsize=(max(12, len(labels) * 1.5), 7))
    width = 0.38
    for engine, offset in [("postgres", -width / 2), ("neo4j", width / 2)]:
        engine_rows = [row for row in rows if row["engine"] == engine]
        if not engine_rows:
            continue
        x_values = []
        heights = []
        errors_low = []
        errors_high = []
        for row in engine_rows:
            label = (str(row["template_id"]), str(row["regime"]))
            x_values.append(label_positions[label] + offset)
            heights.append(float(row[metric_field]))
            errors_low.append(max(0.0, float(row["median"]) - float(row["q1"])))
            errors_high.append(max(0.0, float(row["q3"]) - float(row["median"])))
        ax.bar(
            x_values,
            heights,
            width=width,
            color=NATURE_PALETTE[engine],
            label=engine,
            yerr=np.asarray([errors_low, errors_high]),
            capsize=3,
            linewidth=0,
        )
    if y_log:
        ax.set_yscale("log")
    ax.set_ylabel(metric_label)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels([f"{template}\n{regime}" for template, regime in labels], rotation=45, ha="right")
    style_axes(ax)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _speedup_figure(path: Path, rows: list[dict[str, object]], *, dpi: int) -> None:
    if not rows:
        return

    labels = [f"{row['template_id']}\n{row['regime']}" for row in rows]
    values = [float(row["median"]) for row in rows]
    errors_low = [max(0.0, float(row["median"]) - float(row["q1"])) for row in rows]
    errors_high = [max(0.0, float(row["q3"]) - float(row["median"])) for row in rows]
    fig, ax = plt.subplots(figsize=(max(12, len(labels) * 1.3), 6))
    ax.bar(
        range(len(labels)),
        values,
        color=NATURE_PALETTE["neutral"],
        yerr=np.asarray([errors_low, errors_high]),
        capsize=3,
        linewidth=0,
    )
    ax.axhline(1.0, color="#666666", linewidth=1.0, linestyle="--")
    ax.set_ylabel("Neo4j over PostgreSQL speedup")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    style_axes(ax)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _regime_runtime_figure(path: Path, rows: list[dict[str, object]], *, dpi: int) -> None:
    if not rows:
        return
    engines = [engine for engine in ["postgres", "neo4j"] if any(row["engine"] == engine for row in rows)]
    if not engines:
        return

    fig, axes = plt.subplots(1, len(engines), figsize=(max(12, len(rows) * 0.8), 6), squeeze=False)
    for axis, engine in zip(axes[0], engines, strict=True):
        engine_rows = [row for row in rows if row["engine"] == engine]
        templates = sorted({str(row["template_id"]) for row in engine_rows})
        template_positions = {template: idx for idx, template in enumerate(templates)}
        width = 0.38
        for regime, offset in [("uniform_random", -width / 2), ("hub_anchored", width / 2)]:
            regime_rows = [row for row in engine_rows if row["regime"] == regime]
            x_values = []
            heights = []
            errors_low = []
            errors_high = []
            for row in regime_rows:
                x_values.append(template_positions[str(row["template_id"])] + offset)
                heights.append(float(row["median"]))
                errors_low.append(max(0.0, float(row["median"]) - float(row["q1"])))
                errors_high.append(max(0.0, float(row["q3"]) - float(row["median"])))
            axis.bar(
                x_values,
                heights,
                width=width,
                color=NATURE_PALETTE[regime],
                label=regime,
                yerr=np.asarray([errors_low, errors_high]),
                capsize=3,
                linewidth=0,
            )
        axis.set_yscale("log")
        axis.set_ylabel("Median runtime (ms)")
        axis.set_xticks(range(len(templates)))
        axis.set_xticklabels(templates, rotation=45, ha="right")
        style_axes(axis)
        axis.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _quartiles(values: list[float]) -> tuple[float, float, float]:
    data = np.asarray(values, dtype=float)
    return (
        float(np.percentile(data, 25)),
        float(np.median(data)),
        float(np.percentile(data, 75)),
    )


def _safe_float(raw_value: object) -> float | None:
    if raw_value in {"", None}:
        return None
    return float(raw_value)
