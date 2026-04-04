from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from .common import AppContext


NATURE_PALETTE = {
    "postgres": "#4DBBD5",
    "neo4j": "#E64B35",
    "uniform_random": "#00A087",
    "hub_anchored": "#3C5488",
    "path": "#4DBBD5",
    "triangle": "#E64B35",
    "cycle": "#00A087",
    "dataset": "#3C5488",
    "template": "#F39B7F",
    "candidate": "#91D1C2",
    "default_plan": "#3C5488",
    "connected_prefix": "#00A087",
    "cross_product_inducing": "#E64B35",
    "acyclic": "#4DBBD5",
    "cyclic": "#E64B35",
    "neutral": "#7E6148",
}

FAMILY_MARKERS = {
    "path": "o",
    "triangle": "^",
    "cycle": "s",
}

REGIME_MARKERS = {
    "uniform_random": "o",
    "hub_anchored": "D",
}


def apply_plot_style(ctx: AppContext) -> None:
    plt.rcParams.update(
        {
            "font.size": ctx.config["plotting"]["font_size"],
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
            "axes.linewidth": 1.1,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": False,
            "grid.alpha": 0.25,
            "grid.linewidth": 0.6,
            "figure.dpi": ctx.config["plotting"]["dpi"],
            "savefig.dpi": ctx.config["plotting"]["dpi"],
        }
    )


def style_axes(ax, *, grid_axis: str = "y") -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis=grid_axis, color="#d0d0d0", linewidth=0.6, alpha=0.4)
    ax.set_axisbelow(True)


def style_axes_box(ax, *, grid_axis: str = "y") -> None:
    for side in ["top", "right", "bottom", "left"]:
        ax.spines[side].set_visible(True)
        ax.spines[side].set_linewidth(1.1)
        ax.spines[side].set_color("#404040")
    ax.grid(axis=grid_axis, color="#d0d0d0", linewidth=0.6, alpha=0.35)
    ax.set_axisbelow(True)


def remove_existing_figures(figure_dir: Path, filenames: list[str]) -> None:
    figure_dir.mkdir(parents=True, exist_ok=True)
    for filename in filenames:
        path = figure_dir / filename
        if path.exists():
            path.unlink()


def write_figure_manifest(ctx: AppContext, figure_dir: Path) -> None:
    ctx.write_json(
        figure_dir / "figure_manifest.json",
        {
            "font_size": ctx.config["plotting"]["font_size"],
            "dpi": ctx.config["plotting"]["dpi"],
            "files": sorted(path.name for path in figure_dir.glob("*.png")),
        },
    )
