from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .artifacts import read_csv_rows
from .common import AppContext, print_status
from .plotting import FAMILY_MARKERS, NATURE_PALETTE, REGIME_MARKERS, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest


def _font_size() -> int:
    return int(plt.rcParams["font.size"])


def run_figures(ctx: AppContext) -> None:
    print_status("Analysis figures: building summary plots")
    apply_plot_style(ctx)
    _refresh_template_profile(ctx)
    _refresh_reachability_figure(ctx)
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
    engines = [eng for eng in ["pg", "duck", "neo"] if any(row["eng"] == eng for row in rows)]
    regimes = [reg for reg in ["uniform", "hub"] if any(row["reg"] == reg for row in rows)]
    fig, axes = plt.subplots(1, len(engines), figsize=(max(12, len(engines) * 4.2), 5.4), squeeze=False)

    legend_handles = None
    for axis, eng in zip(axes[0], engines, strict=True):
        eng_rows = [row for row in rows if row["eng"] == eng]
        x_values = np.arange(len(regimes))
        width = 0.34
        handles = []
        for index, shape in enumerate(["acyclic", "cyclic"]):
            values = []
            for reg in regimes:
                matched = next((row for row in eng_rows if row["reg"] == reg and row["shape"] == shape), None)
                values.append(float(matched["med_ms"]) if matched and matched["med_ms"] else np.nan)
            bar_container = axis.bar(
                x_values + (-width / 2 if index == 0 else width / 2),
                values,
                width=width,
                color=NATURE_PALETTE["acyclic" if shape == "acyclic" else "cyclic"],
                label=shape,
            )
            handles.append(bar_container)
        legend_handles = handles
        axis.set_yscale("log")
        axis.set_ylabel("Runtime (ms, log scale)")
        axis.set_xticks(x_values)
        axis.set_xticklabels(regimes)
        axis.set_title(eng, loc="left", fontsize=_font_size())
        style_axes(axis)

    if legend_handles:
        fig.legend(
            legend_handles,
            ["acyclic", "cyclic"],
            loc="upper center",
            ncol=2,
            bbox_to_anchor=(0.5, 1.00),
        )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
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
            color=_engine_color(eng),
            marker=FAMILY_MARKERS["triangle" if fam == "tri" else fam],
            s=54,
            alpha=0.82,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("AGM")
    ax.set_ylabel("Runtime (ms, log scale)")
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
            color=_engine_color(eng),
            marker=REGIME_MARKERS["uniform_random" if reg == "uniform" else "hub_anchored"],
            s=52,
            alpha=0.82,
            label=label if label not in plotted else None,
        )
        plotted.add(label)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Work")
    ax.set_ylabel("Runtime (ms, log scale)")
    style_axes(ax, grid_axis="both")
    ax.legend(ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _refresh_template_profile(ctx: AppContext) -> None:
    selected_path = ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv"
    if not selected_path.exists():
        return
    candidate_path = ctx.path(ctx.config["paths"]["template_mining_dir"]) / "candidate_summary.csv"
    selected_rows = read_csv_rows(selected_path)
    candidate_rows = read_csv_rows(candidate_path) if candidate_path.exists() else []
    figure_dir = ctx.path(ctx.config["paths"]["prepare_figures_dir"])
    _template_profile_figure(
        figure_dir / "template_profile.png",
        selected_rows,
        candidate_rows,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    write_figure_manifest(ctx, figure_dir)


def _template_profile_figure(
    path: Path,
    selected_rows: list[dict[str, str]],
    candidate_rows: list[dict[str, str]],
    *,
    dpi: int,
) -> None:
    if not selected_rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 3, figsize=(14.2, 5.2))

    candidate_items = [
        (f"{row['fam']}-{row['edges']}", _safe_float(row.get("cand_n", "")), _safe_float(row.get("sel_n", "")))
        for row in candidate_rows
    ]
    candidate_items = [(label, cand, sel) for label, cand, sel in candidate_items if cand is not None and cand > 0]
    if candidate_items:
        labels = [item[0] for item in candidate_items]
        values = [item[1] for item in candidate_items]
        colors = [NATURE_PALETTE["template"] if item[2] and item[2] > 0 else NATURE_PALETTE["candidate"] for item in candidate_items]
        axes[0].bar(np.arange(len(labels)), values, color=colors, width=0.68)
        axes[0].set_yscale("log")
        axes[0].set_xticks(np.arange(len(labels)))
        axes[0].set_xticklabels(labels, rotation=25, ha="right")
        axes[0].set_ylabel("Candidate templates")
        axes[0].set_title("Mined space", loc="left", fontsize=_font_size())
        style_axes(axes[0])
    else:
        axes[0].axis("off")

    tids = [row["tid"] for row in selected_rows]
    families = [row["fam"] for row in selected_rows]
    grounded = [_safe_float(row.get("grounded", "")) or np.nan for row in selected_rows]
    anchors = [_safe_float(row.get("anchors", "")) or np.nan for row in selected_rows]
    colors = [_family_color(fam) for fam in families]

    x_values = np.arange(len(tids))
    axes[1].bar(x_values, grounded, color=colors, width=0.68)
    axes[1].set_yscale("log")
    axes[1].set_xticks(x_values)
    axes[1].set_xticklabels(tids)
    axes[1].set_ylabel("Grounded matches")
    axes[1].set_title("Selected queries", loc="left", fontsize=_font_size())
    style_axes(axes[1])

    axes[2].bar(x_values, anchors, color=colors, width=0.68)
    axes[2].set_yscale("log")
    axes[2].set_xticks(x_values)
    axes[2].set_xticklabels(tids)
    axes[2].set_ylabel("Valid anchor nodes")
    axes[2].set_title("Binding support", loc="left", fontsize=_font_size())
    style_axes(axes[2])

    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _refresh_reachability_figure(ctx: AppContext) -> None:
    reachability_dir = ctx.path(ctx.config["paths"].get("reachability_dir", ""))
    runtime_path = reachability_dir / "reachability_runtime.csv"
    if not runtime_path.exists():
        return
    rows = read_csv_rows(runtime_path)
    figure_dir = ctx.path(ctx.config["paths"]["experiments_figures_dir"])
    _reachability_runtime_figure(
        figure_dir / "reachability_runtime.png",
        rows,
        dpi=int(ctx.config["plotting"]["dpi"]),
    )
    write_figure_manifest(ctx, figure_dir)


def _reachability_runtime_figure(path: Path, rows: list[dict[str, str]], *, dpi: int) -> None:
    ok_rows = [row for row in rows if row.get("status") == "ok" and _safe_float(row.get("ms", "")) is not None]
    if not ok_rows:
        return
    regimes = [reg for reg in ["uniform", "hub"] if any(row["reg"] == reg for row in ok_rows)]
    depths = [depth for depth in ["2", "3"] if any(row["depth"] == depth for row in ok_rows)]
    engines = [eng for eng in ["pg", "duck", "neo"] if any(row["eng"] == eng for row in ok_rows)]
    fig, axes = plt.subplots(len(depths), len(regimes), figsize=(max(9.6, len(regimes) * 4.8), max(6.8, len(depths) * 3.4)), squeeze=False)

    for row_index, depth in enumerate(depths):
        for col_index, reg in enumerate(regimes):
            axis = axes[row_index][col_index]
            panel_rows = [row for row in ok_rows if row["depth"] == depth and row["reg"] == reg]
            for eng_index, eng in enumerate(engines):
                eng_rows = [row for row in panel_rows if row["eng"] == eng]
                if not eng_rows:
                    continue
                y_values = [_safe_float(row["ms"]) for row in eng_rows]
                x_values = np.linspace(eng_index - 0.14, eng_index + 0.14, num=len(eng_rows))
                axis.scatter(
                    x_values,
                    y_values,
                    color=_engine_color(eng),
                    s=54,
                    alpha=0.86,
                    edgecolor="white",
                    linewidth=0.6,
                )
            axis.set_yscale("log")
            axis.set_xticks(np.arange(len(engines)))
            axis.set_xticklabels(engines)
            axis.set_ylabel("Runtime (ms, log scale)")
            axis.set_title(f"{reg}, depth {depth}", loc="left", fontsize=_font_size())
            style_axes(axis)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi)
    plt.close(fig)


def _engine_color(eng: str) -> str:
    return {
        "pg": NATURE_PALETTE["postgres"],
        "duck": NATURE_PALETTE["duckdb"],
        "neo": NATURE_PALETTE["neo4j"],
    }.get(eng, NATURE_PALETTE["neutral"])


def _family_color(fam: str) -> str:
    return {
        "path": NATURE_PALETTE["path"],
        "tri": NATURE_PALETTE["triangle"],
        "triangle": NATURE_PALETTE["triangle"],
        "cycle": NATURE_PALETTE["cycle"],
    }.get(fam, NATURE_PALETTE["template"])


def _safe_float(raw_value: object) -> float | None:
    if raw_value in {"", None}:
        return None
    return float(raw_value)
