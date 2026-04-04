from __future__ import annotations

import random
from pathlib import Path

from .artifacts import load_selected_templates
from .common import AppContext, print_status
from .postgres import connect_postgres
from .templates import Template, valid_anchors_params, valid_anchors_sql


def run_sampling(ctx: AppContext) -> None:
    templates = load_selected_templates(ctx)
    conn = connect_postgres(ctx)
    try:
        baseline_rows = []
        join_rows = []
        seed = int(ctx.config["project"]["random_seed"])
        print_status(f"Sampling bindings for {len(templates)} selected templates")
        for index, template in enumerate(templates, start=1):
            print_status(f"Sampling template {index}/{len(templates)}: {template.template_id}")
            anchors = fetch_valid_anchors(conn, template)
            baseline_rows.extend(sample_bindings(ctx, template, anchors, "baseline", seed))
            join_rows.extend(sample_bindings(ctx, template, anchors, "join_order", seed))
        print_status("Sampling complete; writing binding CSVs")
        ctx.write_csv(Path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv", list(baseline_rows[0].keys()) if baseline_rows else ["template_id"], baseline_rows)
        ctx.write_csv(Path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv", list(join_rows[0].keys()) if join_rows else ["template_id"], join_rows)
    finally:
        conn.close()


def fetch_valid_anchors(conn, template: Template) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(valid_anchors_sql(template), valid_anchors_params(template))
        columns = [desc.name for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def sample_bindings(ctx: AppContext, template: Template, anchors: list[dict], mode: str, seed: int) -> list[dict]:
    sample_size = (
        int(ctx.config["sampling"]["baseline_bindings_per_template_regime"])
        if mode == "baseline"
        else int(ctx.config["sampling"]["join_order_bindings_per_template_regime"])
    )
    rows = []

    sorted_anchors = sorted(anchors, key=lambda row: row["anchor_id"])
    for regime in ["uniform_random", "hub_anchored"]:
        rng = random.Random(f"{seed}:{template.template_id}:{mode}:{regime}")
        if regime == "uniform_random":
            candidates = list(sorted_anchors)
        else:
            anchors_by_degree = sorted(sorted_anchors, key=lambda row: (-int(row["first_edge_degree"]), row["anchor_id"]))
            top_k = max(1, round(len(anchors_by_degree) * float(ctx.config["sampling"]["hub_top_fraction"])))
            candidates = anchors_by_degree[:top_k]
        selected = candidates if len(candidates) <= sample_size else rng.sample(candidates, sample_size)
        selected = sorted(selected, key=lambda row: row["anchor_id"])
        for index, item in enumerate(selected, start=1):
            rows.append(
                {
                    "template_id": template.template_id,
                    "family": template.family,
                    "regime": regime,
                    "binding_group": mode,
                    "binding_index": index,
                    "anchor_id": item["anchor_id"],
                    "grounded_match_count_for_anchor": item["grounded_match_count"],
                    "first_edge_degree": item["first_edge_degree"],
                    "requested_sample_size": sample_size,
                    "candidate_count": len(candidates),
                    "shortfall": max(0, sample_size - len(candidates)),
                }
            )
    return rows
