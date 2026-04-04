from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .plotting import (
    FAMILY_MARKERS,
    NATURE_PALETTE,
    REGIME_MARKERS,
    apply_plot_style,
    remove_existing_figures,
    style_axes,
    write_figure_manifest,
)


def run_figures(ctx: AppContext) -> None:
    print_status("Analysis figures: building summary plots")
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["analysis_figures_dir"])
    remove_existing_figures(
        figure_dir,
        [
            "acyclic_vs_cyclic_runtime.png",
            "acyclic_vs_cyclic_work.png",
            "agm_vs_output_cardinality.png",
            "runtime_vs_agm_colored.png",
            "work_vs_runtime_by_engine.png",
            "postgres_runtime_vs_agm.png",
            "neo4j_runtime_vs_agm.png",
            "postgres_work_vs_runtime.png",
            "neo4j_work_vs_runtime.png",
        ],
    )

    instance_rows = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_theory_runtime.csv")
    cyclicity_rows = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "cyclicity_contrast_summary.csv")

    print_status("Analysis figures: writing acyclic vs cyclic plots")
    _acyclic_cyclic_figure(
        figure_dir / "acyclic_vs_cyclic_runtime.png",
        cyclicity_rows,
        median_key="median_runtime_ms",
        q1_key="runtime_q1_ms",
        q3_key="runtime_q3_ms",
        ylabel="Median runtime (ms)",
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _acyclic_cyclic_figure(
        figure_dir / "acyclic_vs_cyclic_work.png",
        cyclicity_rows,
        median_key="median_work",
        q1_key="work_q1",
        q3_key="work_q3",
        ylabel="Intermediate work",
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    print_status("Analysis figures: writing AGM and runtime scatter plots")
    _agm_vs_output_figure(
        figure_dir / "agm_vs_output_cardinality.png",
        instance_rows,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _runtime_vs_agm_colored_figure(
        figure_dir / "runtime_vs_agm_colored.png",
        instance_rows,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    _work_vs_runtime_by_engine_figure(
        figure_dir / "work_vs_runtime_by_engine.png",
        instance_rows,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )

    print_status("Analysis figures: writing figure_manifest.json")
    write_figure_manifest(ctx, figure_dir)


def _acyclic_cyclic_figure(
    path: Path,
    rows: list[dict[str, str]],
    *,
    median_key: str,
    q1_key: str,
    q3_key: str,
    ylabel: str,
    dpi: int,
) -> None:
    if not rows:
        return
    labels = []
    values = []
    errors_low = []
    errors_high = []
    colors = []
    for row in rows:
        median = _safe_float(row.get(median_key, ""))
        q1 = _safe_float(row.get(q1_key, ""))
        q3 = _safe_float(row.get(q3_key, ""))
        if median is None or q1 is None or q3 is None:
            continue
        labels.append(f"{row['engine']}\n{_display_family(row)}\n{row['regime']}")
        values.append(median)
        errors_low.append(max(0.0, median - q1))
        errors_high.append(max(0.0, q3 - median))
        colors.append(NATURE_PALETTE["acyclic"] if str(row["acyclic"]).lower() == "true" else NATURE_PALETTE["cyclic"])
    if not values:
        return

    fig, ax = plt.subplots(figsize=(max(12, len(labels) * 0.9), 7))
    ax.bar(
        range(len(labels)),
        values,
        color=colors,
        yerr=np.asarray([errors_low, errors_high]),
        capsize=3,
        linewidth=0,
    )
    ax.set_yscale("log")
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    style_axes(ax)
    ax.legend(
        handles=[
            Patch(facecolor=NATURE_PALETTE["acyclic"], label="acyclic"),
            Patch(facecolor=NATURE_PALETTE["cyclic"], label="cyclic"),
        ],
        frameon=False,
    )
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _agm_vs_output_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    points = []
    for row in rows:
        agm = _safe_float(row.get("agm_bound", ""))
        output = _safe_float(row.get("output_cardinality", ""))
        if agm is None or output is None or agm <= 0 or output <= 0:
            continue
        points.append((agm, output, row["family"], row["regime"]))
    if not points:
        return

    fig, ax = plt.subplots(figsize=(8, 6))
    for family in ["path", "triangle", "cycle"]:
        family_points = [point for point in points if point[2] == family]
        if not family_points:
            continue
        ax.scatter(
            [point[0] for point in family_points],
            [point[1] for point in family_points],
            color=NATURE_PALETTE[family],
            marker=FAMILY_MARKERS[family],
            s=42,
            alpha=0.75,
            label=family,
        )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("AGM bound")
    ax.set_ylabel("Output cardinality")
    style_axes(ax, grid_axis="both")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _runtime_vs_agm_colored_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(8, 6))
    plotted = set()
    for row in rows:
        agm = _safe_float(row.get("agm_bound", ""))
        runtime = _safe_float(row.get("median_ms", ""))
        if agm is None or runtime is None or agm <= 0 or runtime <= 0:
            continue
        family = row["family"]
        regime = row["regime"]
        label = f"{family}:{regime}"
        ax.scatter(
            agm,
            runtime,
            color=NATURE_PALETTE[family],
            marker=REGIME_MARKERS[regime],
            s=42,
            alpha=0.75,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("AGM bound")
    ax.set_ylabel("Median runtime (ms)")
    style_axes(ax, grid_axis="both")
    ax.legend(frameon=False, ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _work_vs_runtime_by_engine_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(8, 6))
    plotted = set()
    for row in rows:
        runtime = _safe_float(row.get("median_ms", ""))
        work = _safe_float(row.get("intermediate_work_rows", ""))
        if runtime is None or work is None or runtime <= 0 or work <= 0:
            continue
        engine = row["engine"]
        family = row["family"]
        label = f"{engine}:{family}"
        ax.scatter(
            work,
            runtime,
            color=NATURE_PALETTE[engine],
            marker=FAMILY_MARKERS[family],
            s=42,
            alpha=0.75,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Intermediate work")
    ax.set_ylabel("Median runtime (ms)")
    style_axes(ax, grid_axis="both")
    ax.legend(frameon=False, ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _safe_float(raw_value: object) -> float | None:
    if raw_value in {"", None}:
        return None
    return float(raw_value)


def _display_family(row: dict[str, str]) -> str:
    family = row["family"]
    edge_count = int(row["edge_count"])
    if family == "path":
        return f"{edge_count}-edge path"
    if family == "triangle":
        return "triangle"
    if family == "cycle":
        return "4-cycle"
    return family
