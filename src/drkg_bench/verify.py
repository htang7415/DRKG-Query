from __future__ import annotations

import json
from pathlib import Path

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, BenchmarkError, print_status
from .reporting import template_label_map
from .templates import all_left_deep_orders


def verify_results(ctx: AppContext) -> None:
    print_status("Verify: checking result folder layout and required files")
    results_dir = ctx.path(ctx.config["paths"]["results_dir"])
    actual_top_level = sorted(path.name for path in results_dir.iterdir() if path.is_dir())
    expected_top_level = sorted(
        {
            Path(ctx.config["paths"]["setup_dir"]).name,
            Path(ctx.config["paths"]["prepare_dir"]).name,
            Path(ctx.config["paths"]["experiments_dir"]).name,
            Path(ctx.config["paths"]["analysis_dir"]).name,
            Path(ctx.config["paths"]["final_dir"]).name,
        }
    )
    if actual_top_level != expected_top_level:
        raise BenchmarkError(
            f"results/ top-level folders mismatch: expected {expected_top_level}, found {actual_top_level}"
        )

    required_paths = [
        ctx.path(ctx.config["paths"]["setup_dir"]),
        ctx.path(ctx.config["paths"]["prepare_dir"]),
        ctx.path(ctx.config["paths"]["experiments_dir"]),
        ctx.path(ctx.config["paths"]["analysis_dir"]),
        ctx.path(ctx.config["paths"]["final_dir"]),
        ctx.path(ctx.config["paths"]["env_dir"]) / "environment_report.json",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "preprocess_summary.json",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_analysis.json",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_overview.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_node_types.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_relations.csv",
        ctx.path(ctx.config["paths"]["load_postgres_dir"]) / "load_summary.json",
        ctx.path(ctx.config["paths"]["load_neo4j_dir"]) / "load_summary.json",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "candidate_summary.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
        ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv",
        ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "engine_summary.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "theory_summary.json",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "template_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "structure_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_metrics.json",
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["final_dir"]) / "final_manifest.json",
        ctx.path(ctx.config["paths"]["final_dir"]) / "config_snapshot.yaml",
    ]
    missing = [str(path.relative_to(ctx.root)) for path in required_paths if not path.exists()]
    if missing:
        raise BenchmarkError(f"Missing expected result files: {missing}")

    print_status("Verify: checking benchmark row counts and compact output consistency")
    selected_templates = load_selected_templates(ctx)
    label_map = template_label_map(selected_templates)
    templates_by_tid = {label_map[template.template_id]: template for template in selected_templates}
    postgres_rows = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    neo4j_rows = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    join_rows = read_csv_rows(ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv")

    pg_keys = {(row["tid"], row["reg"], row["bid"]) for row in postgres_rows}
    neo_keys = {(row["tid"], row["reg"], row["bid"]) for row in neo4j_rows}
    if pg_keys != neo_keys:
        raise BenchmarkError("PostgreSQL and Neo4j baseline instance keys do not match")
    if len(pg_keys) != len(postgres_rows) or len(neo_keys) != len(neo4j_rows):
        raise BenchmarkError("Duplicate baseline instance keys detected")

    join_keys_by_tid: dict[str, set[str]] = {}
    for row in join_rows:
        join_keys_by_tid.setdefault(row["tid"], set()).add(row["bid"])
    expected_join_rows = 0
    for tid, template in templates_by_tid.items():
        if tid == "P2":
            continue
        binding_count = len(join_keys_by_tid.get(tid, set()))
        expected_join_rows += binding_count * (1 + len(all_left_deep_orders(template)))
    if len(join_rows) != expected_join_rows:
        raise BenchmarkError(f"Join-order row count mismatch: expected {expected_join_rows}, found {len(join_rows)}")

    with (ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json").open("r", encoding="utf-8") as handle:
        comparison_metrics = json.load(handle)
    if int(comparison_metrics.get("matched_instances", -1)) != len(pg_keys):
        raise BenchmarkError("comparison_metrics.json matched_instances does not match baseline key count")

    print_status("Verify: checking figure manifests and final package manifest")
    for manifest_path in [
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "figure_manifest.json",
    ]:
        with manifest_path.open("r", encoding="utf-8") as handle:
            figure_manifest = json.load(handle)
        if int(figure_manifest.get("font_size", -1)) != int(ctx.config["plotting"]["font_size"]):
            raise BenchmarkError(f"Figure font size mismatch in {manifest_path.name}")
        if int(figure_manifest.get("dpi", -1)) != int(ctx.config["plotting"]["dpi"]):
            raise BenchmarkError(f"Figure DPI mismatch in {manifest_path.name}")

    final_manifest_path = ctx.path(ctx.config["paths"]["final_dir"]) / "final_manifest.json"
    with final_manifest_path.open("r", encoding="utf-8") as handle:
        final_manifest = json.load(handle)
    if "final_tables" not in final_manifest or "final_figures" not in final_manifest:
        raise BenchmarkError("final_manifest.json is missing final_tables or final_figures")
    print_status("Verify: all checks passed")
