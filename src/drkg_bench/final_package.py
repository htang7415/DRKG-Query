from __future__ import annotations

import shutil
from pathlib import Path

from .common import AppContext, print_status


def build_final_package(ctx: AppContext) -> None:
    print_status("Final package: refreshing final_tables and final_figures")
    final_dir = ctx.path(ctx.config["paths"]["final_dir"])
    final_dir.mkdir(parents=True, exist_ok=True)

    final_tables = final_dir / "final_tables"
    final_figures = final_dir / "final_figures"
    if final_tables.exists():
        shutil.rmtree(final_tables)
    if final_figures.exists():
        shutil.rmtree(final_figures)
    final_tables.mkdir(exist_ok=True)
    final_figures.mkdir(exist_ok=True)

    explicit_tables = [
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "preprocess_summary.json",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_analysis.json",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_overview.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_node_types.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_relations.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "candidate_summary.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "engine_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "template_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "structure_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_metrics.json",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "theory_summary.json",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
    ]
    for table_path in explicit_tables:
        if table_path.exists():
            shutil.copy2(table_path, final_tables / table_path.name)

    print_status("Final package: copying figure PNGs")
    explicit_figures = [
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "dataset_profile.png",
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "template_profile.png",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "engine_runtime.png",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "speedup.png",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "join_order_effect.png",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "structure_runtime.png",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "agm_runtime.png",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "work_runtime.png",
    ]
    for figure_path in explicit_figures:
        if figure_path.exists():
            shutil.copy2(figure_path, final_figures / figure_path.name)

    shutil.copy2(ctx.config_path, final_dir / "config_snapshot.yaml")
    print_status("Final package: writing final_manifest.json")
    ctx.write_json(
        final_dir / "final_manifest.json",
        {
            "final_tables": sorted(path.name for path in final_tables.glob("*")),
            "final_figures": sorted(path.name for path in final_figures.glob("*")),
        },
    )
    print_status("Final package: pruning low-value intermediate artifacts")
    _prune_intermediate_artifacts(ctx)


def _prune_intermediate_artifacts(ctx: AppContext) -> None:
    removable_paths = [
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "nodes.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "edges.csv",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv",
        ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "template_hypergraphs.json",
    ]
    for path in removable_paths:
        _remove_if_exists(path)


def _remove_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink()
