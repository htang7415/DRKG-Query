from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, BenchmarkError, print_status
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
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "node_type_counts.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "relation_edge_counts.csv",
        ctx.path(ctx.config["paths"]["load_postgres_dir"]) / "load_summary.json",
        ctx.path(ctx.config["paths"]["load_neo4j_dir"]) / "load_summary.json",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "candidate_templates.csv",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv",
        ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv",
        ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_tables.csv",
        ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_tables.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_theory_runtime.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "family_regime_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "cyclicity_contrast_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "comparison_summary.csv",
        ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv",
        ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "figure_manifest.json",
        ctx.path(ctx.config["paths"]["final_dir"]) / "final_manifest.json",
        ctx.path(ctx.config["paths"]["final_dir"]) / "config_snapshot.yaml",
    ]
    missing = [str(path.relative_to(ctx.root)) for path in required_paths if not path.exists()]
    if missing:
        raise BenchmarkError(f"Missing expected result files: {missing}")

    print_status("Verify: checking benchmark row counts and nominal budgets")
    templates = load_selected_templates(ctx)
    baseline_bindings = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    join_bindings = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv")
    postgres_rows = read_csv_rows(ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv")
    neo4j_rows = read_csv_rows(ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv")
    join_rows = read_csv_rows(ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv")

    expected_baseline = len(baseline_bindings)
    if len(postgres_rows) != expected_baseline:
        raise BenchmarkError(f"PostgreSQL baseline row count mismatch: expected {expected_baseline}, found {len(postgres_rows)}")
    if len(neo4j_rows) != expected_baseline:
        raise BenchmarkError(f"Neo4j baseline row count mismatch: expected {expected_baseline}, found {len(neo4j_rows)}")

    join_binding_counts = Counter(row["template_id"] for row in join_bindings)
    expected_join_rows = 0
    for template in templates:
        if template.template_id.startswith("path_2_"):
            continue
        expected_join_rows += join_binding_counts[template.template_id] * (1 + len(all_left_deep_orders(template)))
    if len(join_rows) != expected_join_rows:
        raise BenchmarkError(f"Join-order row count mismatch: expected {expected_join_rows}, found {len(join_rows)}")

    four_cycle_selected = any(template.family == "cycle" for template in templates)
    if four_cycle_selected:
        nominal = ctx.config.get("validation", {}).get("nominal_counts_if_four_cycle", {})
        expected_nominal_baseline = int(nominal.get("baseline_instances", expected_baseline))
        expected_nominal_join = int(nominal.get("join_order_instances", expected_join_rows))
        if expected_baseline != expected_nominal_baseline:
            raise BenchmarkError(
                f"Baseline instance budget mismatch against config nominal count: expected {expected_nominal_baseline}, computed {expected_baseline}"
            )
        if expected_join_rows != expected_nominal_join:
            raise BenchmarkError(
                f"Join-order instance budget mismatch against config nominal count: expected {expected_nominal_join}, computed {expected_join_rows}"
            )

    with (ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json").open("r", encoding="utf-8") as handle:
        comparison_metrics = json.load(handle)
    if int(comparison_metrics.get("matched_instances", -1)) < 0:
        raise BenchmarkError("comparison_metrics.json is missing matched_instances")

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
