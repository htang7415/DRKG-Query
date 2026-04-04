from __future__ import annotations

import shutil
import time
from collections import defaultdict
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, print_status
from .plotting import NATURE_PALETTE, apply_plot_style, style_axes_box
from .postgres import connect_postgres
from .template_mining import (
    _load_graph_index,
    _load_relation_mapping,
    _mine_path2_counts,
    _mine_path3_and_triangle_counts,
    _seed_candidate_rows,
    _select_from_exact_counts,
    _write_candidate_csv,
)
from .templates import Template


def run_milestone_template_mining(ctx: AppContext) -> None:
    started = time.perf_counter()
    graph = _load_graph_index(ctx)
    relation_mapping = _load_relation_mapping(ctx)
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]] = {}
    selected_templates: list[Template] = []
    family_summaries: list[dict[str, object]] = []

    min_grounded = int(ctx.config["templates"]["min_grounded_matches"])
    min_anchors = int(ctx.config["templates"]["min_valid_anchors"])
    milestone_cfg = ctx.config.get("milestone", {})
    requested_path2 = int(milestone_cfg.get("select_path2", 1))
    requested_path3 = int(milestone_cfg.get("select_path3", 0))
    requested_triangle = int(milestone_cfg.get("select_triangle", 0))

    path2_counts = {}
    path3_counts = {}
    triangle_counts = {}

    if requested_path2 > 0:
        print_status("Milestone mining: local exact path_2 counts")
        path2_counts = _mine_path2_counts(graph)
        _seed_candidate_rows(graph, candidate_rows, "path", path2_counts, min_grounded)

    if requested_path3 > 0 or requested_triangle > 0:
        print_status("Milestone mining: local exact path_3 and triangle counts")
        path3_counts, triangle_counts = _mine_path3_and_triangle_counts(graph)
        if requested_path3 > 0:
            _seed_candidate_rows(graph, candidate_rows, "path", path3_counts, min_grounded)
        if requested_triangle > 0:
            _seed_candidate_rows(graph, candidate_rows, "triangle", triangle_counts, min_grounded)

    conn = connect_postgres(ctx)
    try:
        selections = [
            ("path", path2_counts, requested_path2, 2),
            ("path", path3_counts, requested_path3, 3),
            ("triangle", triangle_counts, requested_triangle, 3),
        ]
        for family, counts, required, edge_count in selections:
            if required <= 0 or not counts:
                continue
            selected, groups = _select_from_exact_counts(
                ctx,
                conn,
                graph,
                relation_mapping,
                family=family,
                exact_counts=counts,
                required=required,
                candidate_rows=candidate_rows,
            )
            selected_templates.extend(selected)
            family_summaries.append(
                {
                    "family": family,
                    "edge_count": edge_count,
                    "candidate_count": len(counts),
                    "selected_count": len(selected),
                    "evaluated_grounded_groups": groups,
                    "mode": "milestone_exact_local_only",
                }
            )
    finally:
        conn.close()

    flat_candidate_rows = list(candidate_rows.values())
    flat_candidate_rows.sort(
        key=lambda row: (
            row["family"],
            int(row["edge_count"]),
            -int(row["grounded_match_count"]),
            tuple(str(row["relation_type_pattern"]).split("|")),
        )
    )
    _write_candidate_csv(ctx, flat_candidate_rows)
    ctx.write_yaml(
        Path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        {"templates": [template.to_dict() for template in selected_templates]},
    )
    ctx.write_json(
        Path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
        {
            "mode": "milestone",
            "candidate_count": len(flat_candidate_rows),
            "selected_count": len(selected_templates),
            "min_grounded_matches": min_grounded,
            "min_valid_anchors": min_anchors,
            "selected_template_ids": [template.template_id for template in selected_templates],
            "families": family_summaries,
            "elapsed_sec": round(time.perf_counter() - started, 3),
        },
    )
    # Milestone deliverables are consolidated in write_milestone_report().


def write_milestone_report(ctx: AppContext) -> None:
    results_dir = ctx.path(ctx.config["paths"]["results_dir"])
    figure_dir = ctx.path(ctx.config["paths"]["milestone_figures_dir"])
    results_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    preprocess_summary = ctx.path(ctx.config["paths"]["preprocess_dir"]) / "preprocess_summary.json"
    dataset_analysis = ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_analysis.json"
    preprocess_payload = _read_json(preprocess_summary)
    templates = load_selected_templates(ctx)
    bindings = _read_optional_csv(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    benchmark_rows = _read_optional_csv(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    template_label_map = {template.template_id: f"T{index}" for index, template in enumerate(templates, start=1)}

    runtime_summary_rows = _runtime_summary_rows(benchmark_rows, template_label_map)
    if runtime_summary_rows:
        ctx.write_csv(
            results_dir / "postgres_runtime.csv",
            list(runtime_summary_rows[0].keys()),
            runtime_summary_rows,
        )
    template_rows = [
        {
            "template": f"T{index}",
            "family": template.family,
            "edge_count": template.edge_count,
            "pattern": _short_relation_pattern(template.relation_type_pattern),
            "types": _short_endpoint_pattern(template.endpoint_type_pattern),
            "grounded": template.grounded_match_count,
            "anchors": template.valid_anchor_count,
        }
        for index, template in enumerate(templates, start=1)
    ]
    if template_rows:
        ctx.write_csv(results_dir / "selected_templates.csv", list(template_rows[0].keys()), template_rows)

    milestone_metrics = {
        "mode": "lightweight_postgres_only",
        "raw_row_limit": ctx.config.get("preprocess", {}).get("raw_row_limit"),
        "selected_families": _requested_family_labels(ctx.config.get("milestone", {})),
        "template_count": len(templates),
        "binding_count": len(bindings),
        "benchmark_instance_count": len(benchmark_rows),
        "benchmark_ok_count": sum(1 for row in benchmark_rows if row["status"] == "ok"),
        "warmup_runs": int(ctx.config["benchmark"]["warmup_runs"]),
        "measured_runs": int(ctx.config["benchmark"]["measured_runs"]),
        "instrumented_runs": int(ctx.config["benchmark"]["instrumented_runs"]),
    }

    apply_plot_style(ctx)
    _clear_figure_dir(figure_dir)
    _write_process_figure(ctx, figure_dir / "milestone_process.png", preprocess_payload, len(templates), len(bindings), len(benchmark_rows))
    _write_template_metrics_figure(ctx, figure_dir / "selected_template_metrics.png", template_rows)
    _write_runtime_figure(ctx, figure_dir / "postgres_runtime.png", runtime_summary_rows, benchmark_rows)
    _write_milestone_markdown(
        ctx,
        milestone_metrics=milestone_metrics,
        template_rows=template_rows,
        runtime_summary_rows=runtime_summary_rows,
        preprocess_payload=preprocess_payload,
        preprocess_summary_path=preprocess_summary,
        dataset_analysis_path=dataset_analysis,
    )
    _cleanup_intermediate_milestone_outputs(ctx)


def _runtime_summary_rows(
    rows: list[dict[str, str]],
    template_label_map: dict[str, str],
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        if row.get("median_ms", "") in {"", None}:
            continue
        grouped[(row["template_id"], row["regime"])].append(float(row["median_ms"]))

    summary_rows = []
    for (template_id, regime), values in sorted(grouped.items()):
        q1 = float(np.percentile(values, 25))
        median = float(np.percentile(values, 50))
        q3 = float(np.percentile(values, 75))
        summary_rows.append(
            {
                "template": template_label_map.get(template_id, template_id),
                "regime": _short_regime_label(regime),
                "n": len(values),
                "q1_ms": round(q1, 6),
                "median_ms": round(median, 6),
                "q3_ms": round(q3, 6),
            }
        )
    return summary_rows


def _write_runtime_figure(
    ctx: AppContext,
    destination: Path,
    rows: list[dict[str, object]],
    benchmark_rows: list[dict[str, str]],
) -> None:
    if not rows:
        return
    font_size = int(ctx.config["plotting"]["font_size"])
    rows = sorted(rows, key=lambda row: str(row["regime"]))
    labels = [str(row["regime"]) for row in rows]
    medians = [float(row["median_ms"]) for row in rows]
    errors_low = [max(0.0, float(row["median_ms"]) - float(row["q1_ms"])) for row in rows]
    errors_high = [max(0.0, float(row["q3_ms"]) - float(row["median_ms"])) for row in rows]
    colors = [NATURE_PALETTE[_full_regime_label(str(row["regime"]))] for row in rows]

    fig, ax = plt.subplots(figsize=(7.4, 5.8))
    positions = np.arange(len(labels), dtype=float)
    ax.bar(
        positions,
        medians,
        color=colors,
        yerr=np.asarray([errors_low, errors_high]),
        capsize=3,
        linewidth=0.8,
        edgecolor="#404040",
        width=0.6,
    )
    grouped_points: dict[str, list[float]] = defaultdict(list)
    for row in benchmark_rows:
        if row.get("median_ms", "") in {"", None}:
            continue
        grouped_points[_short_regime_label(row["regime"])].append(float(row["median_ms"]))
    for index, row in enumerate(rows):
        values = grouped_points.get(str(row["regime"]), [])
        if not values:
            continue
        offsets = np.linspace(-0.08, 0.08, num=len(values)) if len(values) > 1 else np.asarray([0.0])
        ax.scatter(positions[index] + offsets, values, color="#202020", s=20, zorder=3)
        ax.text(
            positions[index],
            medians[index] + max(errors_high[index], 0.05) + max(medians[index] * 0.03, 0.08),
            f"{medians[index]:.2f}",
            ha="center",
            va="bottom",
            fontsize=font_size,
        )
    ax.set_ylabel("Runtime (ms)", fontsize=font_size)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=font_size)
    ax.tick_params(axis="y", labelsize=font_size)
    ax.margins(y=0.14)
    style_axes_box(ax)
    fig.tight_layout()
    fig.savefig(destination, dpi=int(ctx.config["plotting"]["dpi"]))
    plt.close(fig)


def _write_process_figure(
    ctx: AppContext,
    destination: Path,
    preprocess_payload: dict[str, object],
    template_count: int,
    binding_count: int,
    benchmark_count: int,
) -> None:
    font_size = int(ctx.config["plotting"]["font_size"])
    stages = [
        ("raw", int(preprocess_payload.get("raw_rows", 0)), NATURE_PALETTE["dataset"]),
        ("edges", int(preprocess_payload.get("unique_edges", 0)), "#5E86B6"),
        ("nodes", int(preprocess_payload.get("unique_nodes", 0)), "#7BA6D8"),
        ("tpl", int(template_count), NATURE_PALETTE["template"]),
        ("bind", int(binding_count), NATURE_PALETTE["neutral"]),
        ("runs", int(benchmark_count), NATURE_PALETTE["postgres"]),
    ]
    fig, ax = plt.subplots(figsize=(8.6, 4.0))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 2)
    ax.axis("off")
    box_w = 1.45
    box_h = 0.82
    xs = np.linspace(0.7, 10.3, num=len(stages))
    y = 0.6
    for index, (label, value, color) in enumerate(stages):
        patch = FancyBboxPatch(
            (xs[index], y),
            box_w,
            box_h,
            boxstyle="round,pad=0.04,rounding_size=0.08",
            linewidth=1.0,
            edgecolor="#404040",
            facecolor=color,
        )
        ax.add_patch(patch)
        ax.text(
            xs[index] + box_w / 2,
            y + 0.54,
            label,
            ha="center",
            va="center",
            color="white",
            fontweight="bold",
            fontsize=font_size,
        )
        ax.text(
            xs[index] + box_w / 2,
            y + 0.23,
            f"{value:,}",
            ha="center",
            va="center",
            color="white",
            fontsize=font_size,
        )
        if index < len(stages) - 1:
            arrow = FancyArrowPatch(
                (xs[index] + box_w, y + box_h / 2),
                (xs[index + 1], y + box_h / 2),
                arrowstyle="-|>",
                mutation_scale=16,
                linewidth=1.2,
                color="#505050",
            )
            ax.add_patch(arrow)
    fig.tight_layout()
    fig.savefig(destination, dpi=int(ctx.config["plotting"]["dpi"]))
    plt.close(fig)


def _write_template_metrics_figure(ctx: AppContext, destination: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    font_size = int(ctx.config["plotting"]["font_size"])
    selected = rows[0]
    labels = ["grounded", "anchors"]
    values = [float(selected["grounded"]), float(selected["anchors"])]
    colors = [NATURE_PALETTE["template"], NATURE_PALETTE["path"]]

    fig, ax = plt.subplots(figsize=(6.8, 5.8))
    ax.bar(
        range(len(labels)),
        values,
        color=colors,
        edgecolor="#404040",
        linewidth=0.8,
        width=0.65,
    )
    ax.set_ylabel("Count", fontsize=font_size)
    ax.set_yscale("log")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=font_size)
    ax.tick_params(axis="y", labelsize=font_size)
    for index, value in enumerate(values):
        ax.text(
            index,
            value * 1.1,
            f"{int(value):,}",
            ha="center",
            va="bottom",
            fontsize=font_size,
        )
    ax.margins(y=0.14)
    style_axes_box(ax)
    fig.tight_layout()
    fig.savefig(destination, dpi=int(ctx.config["plotting"]["dpi"]))
    plt.close(fig)


def _write_milestone_markdown(
    ctx: AppContext,
    *,
    milestone_metrics: dict[str, object],
    template_rows: list[dict[str, object]],
    runtime_summary_rows: list[dict[str, object]],
    preprocess_payload: dict[str, object],
    preprocess_summary_path: Path,
    dataset_analysis_path: Path,
) -> None:
    lines = [
        "# Lightweight Milestone Run",
        "",
        "This is a PostgreSQL-only milestone run intended to produce quick, representative outputs rather than the full final benchmark.",
        "",
        "Scope of this run:",
        f"- DRKG subset: first `{milestone_metrics['raw_row_limit']}` raw rows from `data/drkg.tsv`",
        "- Engine coverage: PostgreSQL only",
        f"- Template mining: {', '.join(f'`{label}`' for label in milestone_metrics['selected_families']) if milestone_metrics['selected_families'] else '`none`'}",
        "- Skipped for milestone: Neo4j baseline, 4-edge path search, 4-cycle search, join-order study, final report package",
        (
            "- Benchmark simplifications: no per-instance DB restart, "
            f"`{milestone_metrics['warmup_runs']}` warmup runs, "
            f"`{milestone_metrics['measured_runs']}` measured run, "
            f"`{milestone_metrics['instrumented_runs']}` instrumented run, "
            f"`{ctx.config['sampling']['baseline_bindings_per_template_regime']}` bindings per template/regime"
        ),
        "",
        "Saved outputs in this folder:",
        "- `selected_templates.csv`",
        "- `postgres_runtime.csv`",
        "- `figures/milestone_process.png`",
        "- `figures/selected_template_metrics.png`",
        "- `figures/postgres_runtime.png`",
        "- `milestone.md`",
        "",
        "Run summary:",
        f"- Selected templates: `{milestone_metrics['template_count']}`",
        f"- Baseline bindings: `{milestone_metrics['binding_count']}`",
        f"- PostgreSQL benchmark instances: `{milestone_metrics['benchmark_instance_count']}`",
        f"- Successful PostgreSQL instances: `{milestone_metrics['benchmark_ok_count']}`",
        "",
        "Milestone process:",
        f"- Raw rows scanned: `{preprocess_payload.get('raw_rows', 0)}`",
        f"- Unique edges kept: `{preprocess_payload.get('unique_edges', 0)}`",
        f"- Unique nodes kept: `{preprocess_payload.get('unique_nodes', 0)}`",
        f"- Duplicate rows dropped: `{preprocess_payload.get('duplicate_rows_dropped', 0)}`",
        "",
        "Selected templates:",
    ]
    if template_rows:
        for row in template_rows:
            lines.append(
                f"- `{row['template']}`: family=`{row['family']}`, pattern=`{row['pattern']}`, types=`{row['types']}`, grounded=`{row['grounded']}`, anchors=`{row['anchors']}`"
            )
    else:
        lines.append("- No templates were selected.")

    lines.extend(["", "PostgreSQL runtime summary:"])
    if runtime_summary_rows:
        for row in runtime_summary_rows:
            lines.append(
                f"- `{row['template']}` / `{row['regime']}`: median=`{row['median_ms']}` ms, q1=`{row['q1_ms']}` ms, q3=`{row['q3_ms']}` ms"
            )
    else:
        lines.append("- No benchmark rows were produced.")

    lines.extend(
        [
            "",
            "File guide:",
            "- `selected_templates.csv`: short template catalog used in the milestone run. `pattern` is a concise relation sequence and `types` shows endpoint types on each edge.",
            "- `postgres_runtime.csv`: runtime summary for the PostgreSQL milestone queries. `n` is the number of benchmark instances per regime.",
            "- `figures/milestone_process.png`: compact workflow diagram from DRKG subset to final PostgreSQL runs.",
            "- `figures/selected_template_metrics.png`: grounded matches and valid anchors for the chosen template.",
            "- `figures/postgres_runtime.png`: PostgreSQL runtime comparison between `uniform` and `hub` anchor regimes, with IQR bars and individual run points.",
            "",
            "Interpretation:",
            "- This milestone run is useful for checking data flow, query generation, PostgreSQL execution, logging, and figure generation.",
            "- It is not a substitute for the final full benchmark because it uses a DRKG subset and omits cross-engine and join-order evaluations.",
            "- Intermediate step folders were removed intentionally so `results_milestone/` stays concise.",
            "",
        ]
    )
    destination = ctx.path(ctx.config["paths"]["milestone_doc"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines), encoding="utf-8")


def _read_optional_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return read_csv_rows(path)


def _requested_family_labels(milestone_cfg: dict[str, object]) -> list[str]:
    labels: list[str] = []
    if int(milestone_cfg.get("select_path2", 0)) > 0:
        labels.append("path_2")
    if int(milestone_cfg.get("select_path3", 0)) > 0:
        labels.append("path_3")
    if int(milestone_cfg.get("select_triangle", 0)) > 0:
        labels.append("triangle")
    return labels


def _short_relation_piece(rel_type: str) -> str:
    tail = rel_type.split("::")[-1]
    return tail.split(":")[0]


def _short_relation_pattern(pattern: tuple[str, ...] | list[str]) -> str:
    return " -> ".join(_short_relation_piece(item) for item in pattern)


def _short_endpoint_pattern(pattern: tuple[str, ...] | list[str]) -> str:
    return " -> ".join(str(item) for item in pattern)


def _short_regime_label(regime: str) -> str:
    return {"uniform_random": "uniform", "hub_anchored": "hub"}.get(regime, regime)


def _full_regime_label(regime: str) -> str:
    return {"uniform": "uniform_random", "hub": "hub_anchored"}.get(regime, regime)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _clear_figure_dir(figure_dir: Path) -> None:
    figure_dir.mkdir(parents=True, exist_ok=True)
    for path in figure_dir.glob("*"):
        if path.is_file():
            path.unlink()


def _cleanup_intermediate_milestone_outputs(ctx: AppContext) -> None:
    for key in ["setup_dir", "prepare_dir", "experiments_dir", "analysis_dir", "final_dir"]:
        path = ctx.path(ctx.config["paths"][key])
        if path.exists():
            shutil.rmtree(path)
