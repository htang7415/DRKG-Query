from __future__ import annotations

import random
from pathlib import Path

from .artifacts import load_selected_templates
from .common import AppContext, print_status
from .postgres import connect_postgres
from .reporting import family_label, regime_label, template_label_map
from .templates import Template, valid_anchors_params, valid_anchors_sql


def run_sampling(ctx: AppContext) -> None:
    templates = load_selected_templates(ctx)
    conn = connect_postgres(ctx)
    try:
        baseline_rows = []
        join_rows = []
        seed = int(ctx.config["project"]["random_seed"])
        label_map = template_label_map(templates)
        print_status(f"Sampling bindings for {len(templates)} selected templates")
        for index, template in enumerate(templates, start=1):
            print_status(f"Sampling template {index}/{len(templates)}: {template.template_id}")
            anchors = fetch_valid_anchors(conn, template)
            baseline_rows.extend(sample_bindings(ctx, template, anchors, "baseline", seed, label_map[template.template_id]))
            join_rows.extend(sample_bindings(ctx, template, anchors, "join_order", seed, label_map[template.template_id]))
        print_status("Sampling complete; writing binding CSVs")
        fields = ["tid", "fam", "reg", "grp", "bid", "anchor_id", "support", "deg", "target_n", "cand_n", "shortfall"]
        ctx.write_csv(Path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv", fields, baseline_rows)
        ctx.write_csv(Path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv", fields, join_rows)
    finally:
        conn.close()


def fetch_valid_anchors(conn, template: Template) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(valid_anchors_sql(template), valid_anchors_params(template))
        columns = [desc.name for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def sample_bindings(ctx: AppContext, template: Template, anchors: list[dict], mode: str, seed: int, tid: str) -> list[dict]:
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
                    "tid": tid,
                    "fam": family_label(template.family),
                    "reg": regime_label(regime),
                    "grp": "base" if mode == "baseline" else "join",
                    "bid": index,
                    "anchor_id": item["anchor_id"],
                    "support": item["grounded_match_count"],
                    "deg": item["first_edge_degree"],
                    "target_n": sample_size,
                    "cand_n": len(candidates),
                    "shortfall": max(0, sample_size - len(candidates)),
                }
            )
    return rows
