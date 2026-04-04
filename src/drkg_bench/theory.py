from __future__ import annotations

import math
from pathlib import Path

import networkx as nx
import pulp

from .artifacts import load_selected_templates, read_csv_rows
from .common import AppContext, print_status
from .postgres import connect_postgres
from .templates import Template


def run_theory(ctx: AppContext) -> None:
    templates = {template.template_id: template for template in load_selected_templates(ctx)}
    bindings = read_csv_rows(ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv")
    conn = connect_postgres(ctx)
    try:
        rows = []
        hypergraphs = {}
        print_status(f"Theory: computing hypergraphs and AGM bounds for {len(bindings)} baseline instances")
        progress_every = max(1, min(25, max(1, len(bindings) // 10)))
        for index, binding in enumerate(bindings, start=1):
            template = templates[binding["template_id"]]
            hypergraph = describe_hypergraph(template)
            hypergraphs[template.template_id] = hypergraph
            relation_sizes = relation_sizes_for_binding(conn, template, binding["anchor_id"])
            agm_bound, lp_status, fractional_cover = compute_agm_bound(template, relation_sizes)
            rows.append(
                {
                    "template_id": template.template_id,
                    "family": template.family,
                    "regime": binding["regime"],
                    "anchor_id": binding["anchor_id"],
                    "acyclic": hypergraph["acyclic"],
                    "relation_sizes": "|".join(str(value) for value in relation_sizes),
                    "agm_bound": agm_bound,
                    "lp_status": lp_status,
                    "fractional_cover": "|".join(str(value) for value in fractional_cover),
                }
            )
            if index == 1 or index == len(bindings) or index % progress_every == 0:
                print_status(f"Theory: processed {index}/{len(bindings)} bindings")
        if rows:
            print_status("Theory: writing agm_bounds.csv")
            ctx.write_csv(Path(ctx.config["paths"]["theory_dir"]) / "agm_bounds.csv", list(rows[0].keys()), rows)
        print_status("Theory: writing hypergraph summaries")
        ctx.write_json(Path(ctx.config["paths"]["theory_dir"]) / "template_hypergraphs.json", hypergraphs)
        ctx.write_json(Path(ctx.config["paths"]["theory_dir"]) / "theory_summary.json", {"templates": list(hypergraphs.keys()), "instance_count": len(rows)})
    finally:
        conn.close()


def describe_hypergraph(template: Template) -> dict:
    graph = nx.Graph()
    graph.add_nodes_from(template.node_vars)
    for edge in template.edges:
        graph.add_edge(edge.src_var, edge.dst_var, rel_type=edge.rel_type)
    return {
        "nodes": template.node_vars,
        "edges": [{"src": edge.src_var, "dst": edge.dst_var, "rel_type": edge.rel_type} for edge in template.edges],
        "acyclic": nx.is_forest(graph),
    }


def relation_sizes_for_binding(conn, template: Template, anchor_id: str) -> list[int]:
    node_type_map = {var: template.node_type_pattern[index] for index, var in enumerate(template.node_vars)}
    values = []
    with conn.cursor() as cur:
        for index, edge in enumerate(template.edges, start=1):
            params = [edge.rel_type, node_type_map[edge.src_var], node_type_map[edge.dst_var]]
            anchor_sql = ""
            if index == 1:
                anchor_sql = " AND e.src_id = %s"
                params.append(anchor_id)
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM typed_edges e
                WHERE e.rel_type = %s
                  AND e.src_type = %s
                  AND e.dst_type = %s
                  {anchor_sql}
                """,
                params,
            )
            values.append(int(cur.fetchone()[0]))
    return values


def compute_agm_bound(template: Template, relation_sizes: list[int]) -> tuple[float, str, list[float]]:
    if any(size == 0 for size in relation_sizes):
        return 0.0, "zero_relation", [0.0 for _ in relation_sizes]

    problem = pulp.LpProblem("agm_bound", pulp.LpMinimize)
    variables = [pulp.LpVariable(f"x_{idx}", lowBound=0) for idx in range(len(template.edges))]
    problem += pulp.lpSum(math.log(max(size, 1)) * variables[idx] for idx, size in enumerate(relation_sizes))

    for node_var in template.node_vars:
        incident = [variables[idx] for idx, edge in enumerate(template.edges) if edge.src_var == node_var or edge.dst_var == node_var]
        problem += pulp.lpSum(incident) >= 1

    problem.solve(pulp.PULP_CBC_CMD(msg=False))
    status = pulp.LpStatus.get(problem.status, f"status_{problem.status}")
    if status != "Optimal":
        raise RuntimeError(f"AGM LP did not solve to optimality: {status}")
    weights = [float(variables[idx].value()) if variables[idx].value() is not None else 0.0 for idx in range(len(variables))]
    exponent = sum(math.log(size) * weights[idx] for idx, size in enumerate(relation_sizes))
    return math.exp(exponent), status, weights
