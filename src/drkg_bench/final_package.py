from __future__ import annotations

import shutil

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
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "node_type_counts.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "relation_edge_counts.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_tables.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_theory_runtime.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "template_engine_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "family_regime_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "cyclicity_contrast_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "comparison_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_metrics.json",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_tables.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "template_hypergraphs.json",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "theory_summary.json",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "join_order_classes.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
    ]
    for table_path in explicit_tables:
        if table_path.exists():
            shutil.copy2(table_path, final_tables / table_path.name)

    print_status("Final package: copying figure PNGs")
    for figures_dir in [
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]),
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]),
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]),
    ]:
        if not figures_dir.exists():
            continue
        for file in figures_dir.glob("*.png"):
            shutil.copy2(file, final_figures / file.name)

    shutil.copy2(ctx.config_path, final_dir / "config_snapshot.yaml")
    print_status("Final package: writing final_manifest.json")
    ctx.write_json(
        final_dir / "final_manifest.json",
        {
            "final_tables": sorted(path.name for path in final_tables.glob("*")),
            "final_figures": sorted(path.name for path in final_figures.glob("*")),
        },
    )
