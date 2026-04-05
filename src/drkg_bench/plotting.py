from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from .common import AppContext


NATURE_PALETTE = {
    "postgres": "#2F6C8F",
    "neo4j": "#D65D31",
    "uniform_random": "#2D9D78",
    "hub_anchored": "#274C77",
    "path": "#3E7CB1",
    "triangle": "#C96E12",
    "cycle": "#2C8C69",
    "dataset": "#4B657D",
    "template": "#B85C38",
    "candidate": "#7AA6A1",
    "default_plan": "#5F6B7A",
    "connected_prefix": "#2D9D78",
    "cross_product_inducing": "#D65D31",
    "acyclic": "#3E7CB1",
    "cyclic": "#C96E12",
    "neutral": "#7A6F63",
    "grid": "#C9D1D9",
    "frame": "#31363F",
    "text": "#1F2328",
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
            "font.size": int(ctx.config["plotting"]["font_size"]),
            "font.family": "sans-serif",
            "font.sans-serif": ["DejaVu Sans", "Arial", "Helvetica"],
            "axes.labelcolor": NATURE_PALETTE["text"],
            "axes.edgecolor": NATURE_PALETTE["frame"],
            "axes.linewidth": 1.2,
            "axes.spines.top": True,
            "axes.spines.right": True,
            "axes.grid": False,
            "grid.alpha": 0.25,
            "grid.linewidth": 0.6,
            "xtick.color": NATURE_PALETTE["text"],
            "ytick.color": NATURE_PALETTE["text"],
            "xtick.direction": "out",
            "ytick.direction": "out",
            "xtick.major.size": 4.5,
            "ytick.major.size": 4.5,
            "legend.frameon": False,
            "figure.dpi": ctx.config["plotting"]["dpi"],
            "savefig.dpi": ctx.config["plotting"]["dpi"],
            "savefig.facecolor": "white",
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )


def style_axes(ax, *, grid_axis: str = "y") -> None:
    for side in ["top", "right", "bottom", "left"]:
        ax.spines[side].set_visible(True)
        ax.spines[side].set_linewidth(1.2)
        ax.spines[side].set_color(NATURE_PALETTE["frame"])
    ax.grid(axis=grid_axis, color=NATURE_PALETTE["grid"], linewidth=0.7, alpha=0.5)
    ax.set_axisbelow(True)
    ax.tick_params(top=False, right=False)


def style_axes_box(ax, *, grid_axis: str = "y") -> None:
    style_axes(ax, grid_axis=grid_axis)


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
