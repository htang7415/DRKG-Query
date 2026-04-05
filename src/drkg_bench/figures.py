from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .plotting import FAMILY_MARKERS, NATURE_PALETTE, REGIME_MARKERS, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest


def run_figures(ctx: AppContext) -> None:
    print_status("Analysis figures: building summary plots")
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["analysis_figures_dir"])
    remove_existing_figures(
        figure_dir,
        [
            "structure_runtime.png",
            "agm_runtime.png",
            "work_runtime.png",
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

    instance_rows = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_summary.csv")
    structure_rows = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "structure_summary.csv")

    _structure_runtime_figure(figure_dir / "structure_runtime.png", structure_rows, dpi=int(ctx.config["plotting"]["dpi"]))
    _agm_runtime_figure(figure_dir / "agm_runtime.png", instance_rows, dpi=int(ctx.config["plotting"]["dpi"]))
    _work_runtime_figure(figure_dir / "work_runtime.png", instance_rows, dpi=int(ctx.config["plotting"]["dpi"]))
    write_figure_manifest(ctx, figure_dir)


def _structure_runtime_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    if not rows:
        return
    engines = [eng for eng in ["pg", "neo"] if any(row["eng"] == eng for row in rows)]
    regimes = [reg for reg in ["uniform", "hub"] if any(row["reg"] == reg for row in rows)]
    fig, axes = plt.subplots(1, len(engines), figsize=(max(12, len(regimes) * 4.8), 5.4), squeeze=False)

    for axis, eng in zip(axes[0], engines, strict=True):
        eng_rows = [row for row in rows if row["eng"] == eng]
        x_values = np.arange(len(regimes))
        width = 0.34
        for index, shape in enumerate(["acyclic", "cyclic"]):
            values = []
            for reg in regimes:
                matched = next((row for row in eng_rows if row["reg"] == reg and row["shape"] == shape), None)
                values.append(float(matched["med_ms"]) if matched and matched["med_ms"] else np.nan)
            axis.bar(
                x_values + (-width / 2 if index == 0 else width / 2),
                values,
                width=width,
                color=NATURE_PALETTE["acyclic" if shape == "acyclic" else "cyclic"],
                label=shape,
            )
        axis.set_yscale("log")
        axis.set_ylabel("Median ms")
        axis.set_xticks(x_values)
        axis.set_xticklabels(regimes)
        axis.text(0.02, 0.96, eng, transform=axis.transAxes, va="top", ha="left", fontsize=14)
        style_axes(axis)
        axis.legend()

    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _agm_runtime_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    points = []
    for row in rows:
        agm = _safe_float(row.get("agm", ""))
        runtime = _safe_float(row.get("med_ms", ""))
        if agm is None or runtime is None or agm <= 0 or runtime <= 0:
            continue
        points.append((agm, runtime, row["eng"], row["fam"]))
    if not points:
        return

    fig, ax = plt.subplots(figsize=(7.4, 5.8))
    plotted = set()
    for agm, runtime, eng, fam in points:
        label = f"{eng}-{fam}"
        ax.scatter(
            agm,
            runtime,
            color=NATURE_PALETTE["postgres"] if eng == "pg" else NATURE_PALETTE["neo4j"],
            marker=FAMILY_MARKERS["triangle" if fam == "tri" else fam],
            s=54,
            alpha=0.82,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("AGM")
    ax.set_ylabel("Median ms")
    style_axes(ax, grid_axis="both")
    ax.legend(ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _work_runtime_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    points = []
    for row in rows:
        runtime = _safe_float(row.get("med_ms", ""))
        work = _safe_float(row.get("work", ""))
        if runtime is None or work is None or runtime <= 0 or work <= 0:
            continue
        points.append((work, runtime, row["eng"], row["reg"]))
    if not points:
        return

    fig, ax = plt.subplots(figsize=(7.4, 5.8))
    plotted = set()
    for work, runtime, eng, reg in points:
        label = f"{eng}-{reg}"
        ax.scatter(
            work,
            runtime,
            color=NATURE_PALETTE["postgres"] if eng == "pg" else NATURE_PALETTE["neo4j"],
            marker=REGIME_MARKERS["uniform_random" if reg == "uniform" else "hub_anchored"],
            s=52,
            alpha=0.82,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Work")
    ax.set_ylabel("Median ms")
    style_axes(ax, grid_axis="both")
    ax.legend(ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _safe_float(raw_value: object) -> float | None:
    if raw_value in {"", None}:
        return None
    return float(raw_value)
