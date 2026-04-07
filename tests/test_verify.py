from __future__ import annotations

import csv
import json
from pathlib import Path

import yaml

from drkg_bench.common import AppContext
from drkg_bench.templates import build_template
from drkg_bench.verify import verify_results


def test_verify_results_counts_join_order_bindings_per_regime(tmp_path: Path) -> None:
    ctx = _make_context(tmp_path)
    template = build_template(
        family="path",
        relation_type_pattern=["r1", "r2", "r3"],
        node_type_pattern=["n1", "n2", "n3", "n4"],
        endpoint_type_pattern=["n1", "n4"],
        grounded_match_count=10,
        valid_anchor_count=2,
        anchor_degree_min=1.0,
        anchor_degree_median=1.0,
        anchor_degree_p95=1.0,
        anchor_degree_max=1.0,
    )

    _write_yaml(
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        {"templates": [template.to_dict()]},
    )
    _write_csv(
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv",
        ["template_id"],
        [{"template_id": template.template_id}],
    )
    _write_csv(
        ctx.path(ctx.config["paths"]["template_mining_dir"]) / "candidate_summary.csv",
        ["template_id"],
        [{"template_id": template.template_id}],
    )
    _write_json(ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json", {"templates": 1})

    baseline_rows = [
        {"tid": "P3", "reg": "uniform", "bid": "1"},
        {"tid": "P3", "reg": "hub", "bid": "1"},
    ]
    _write_csv(
        ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "postgres_baseline.csv",
        ["tid", "reg", "bid"],
        baseline_rows,
    )
    _write_csv(
        ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "neo4j_baseline.csv",
        ["tid", "reg", "bid"],
        baseline_rows,
    )

    join_rows = []
    for regime in ["uniform", "hub"]:
        for order_index in range(7):
            join_rows.append(
                {
                    "tid": "P3",
                    "reg": regime,
                    "bid": "1",
                    "ord_idx": str(order_index),
                }
            )
    _write_csv(
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "postgres_join_order.csv",
        ["tid", "reg", "bid", "ord_idx"],
        join_rows,
    )

    _write_json(ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json", {"matched_instances": 2})
    _write_csv(ctx.path(ctx.config["paths"]["comparison_dir"]) / "engine_summary.csv", ["tid"], [])
    _write_csv(ctx.path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv", ["tid"], [])
    _write_json(ctx.path(ctx.config["paths"]["theory_dir"]) / "theory_summary.json", {"ok": True})

    _write_json(ctx.path(ctx.config["paths"]["env_dir"]) / "environment_report.json", {"ok": True})
    _write_json(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "preprocess_summary.json", {"ok": True})
    _write_json(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_analysis.json", {"ok": True})
    _write_csv(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "dataset_overview.csv", ["name"], [])
    _write_csv(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_node_types.csv", ["name"], [])
    _write_csv(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "top_relations.csv", ["name"], [])
    _write_json(ctx.path(ctx.config["paths"]["load_postgres_dir"]) / "load_summary.json", {"ok": True})
    _write_json(ctx.path(ctx.config["paths"]["load_neo4j_dir"]) / "load_summary.json", {"ok": True})

    _write_csv(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "instance_summary.csv", ["tid"], [])
    _write_csv(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "template_summary.csv", ["tid"], [])
    _write_csv(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "structure_summary.csv", ["tid"], [])
    _write_csv(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv", ["tid"], [])
    _write_json(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_metrics.json", {"ok": True})

    figure_manifest = {
        "font_size": ctx.config["plotting"]["font_size"],
        "dpi": ctx.config["plotting"]["dpi"],
    }
    _write_json(ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "figure_manifest.json", figure_manifest)
    _write_json(ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "figure_manifest.json", figure_manifest)
    _write_json(ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "figure_manifest.json", figure_manifest)
    _write_json(
        ctx.path(ctx.config["paths"]["final_dir"]) / "final_manifest.json",
        {"final_tables": [], "final_figures": []},
    )
    _write_yaml(ctx.path(ctx.config["paths"]["final_dir"]) / "config_snapshot.yaml", {"ok": True})

    verify_results(ctx)


def _make_context(tmp_path: Path) -> AppContext:
    config = {
        "paths": {
            "results_dir": "results",
            "setup_dir": "results/01_setup",
            "prepare_dir": "results/02_prepare",
            "experiments_dir": "results/03_experiments",
            "analysis_dir": "results/04_analysis",
            "final_dir": "results/05_final",
            "env_dir": "results/01_setup",
            "preprocess_dir": "results/02_prepare/preprocess",
            "load_postgres_dir": "results/02_prepare/load_postgres",
            "load_neo4j_dir": "results/02_prepare/load_neo4j",
            "template_mining_dir": "results/02_prepare/template_mining",
            "postgres_baseline_dir": "results/03_experiments/postgres_baseline",
            "neo4j_baseline_dir": "results/03_experiments/neo4j_baseline",
            "comparison_dir": "results/03_experiments/engine_comparison",
            "join_order_dir": "results/03_experiments/postgres_join_order",
            "prepare_figures_dir": "results/02_prepare/figures",
            "experiments_figures_dir": "results/03_experiments/figures",
            "theory_dir": "results/04_analysis/theory",
            "analysis_summary_dir": "results/04_analysis/summary",
            "analysis_figures_dir": "results/04_analysis/figures",
        },
        "plotting": {
            "font_size": 16,
            "dpi": 600,
        },
    }
    config_path = tmp_path / "config.yaml"
    _write_yaml(config_path, config)
    ctx = AppContext(root=tmp_path, config_path=config_path, config=config)
    ctx.ensure_results_dirs()
    return ctx


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _write_yaml(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
