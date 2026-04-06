from __future__ import annotations

import csv
import heapq
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np

from .common import AppContext, BenchmarkError, print_status
from .plotting import NATURE_PALETTE, apply_plot_style, remove_existing_figures, style_axes, write_figure_manifest
from .postgres import connect_postgres
from .reporting import family_label, fmt_int, fmt_num, template_label_map
from .templates import (
    Template,
    build_template,
    grounded_count_params,
    grounded_count_sql,
    valid_anchors_params,
    valid_anchors_sql,
)


@dataclass(frozen=True)
class RelationInfo:
    rel_idx: int
    rel_type: str
    src_type: str
    dst_type: str
    edge_count: int
    max_out_degree: int
    max_in_degree: int


@dataclass
class GraphIndex:
    node_types: list[str]
    out_adj: list[dict[int, set[int]]]
    in_adj: list[dict[int, set[int]]]
    edges_by_rel: dict[int, list[tuple[int, int]]]
    rel_info: dict[int, RelationInfo]
    rel_name_to_idx: dict[str, int]
    rel_idx_to_name: dict[int, str]


def run_template_mining(ctx: AppContext) -> None:
    started = time.perf_counter()
    relation_mapping = _load_relation_mapping(ctx)
    graph = _load_graph_index(ctx)
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]] = {}
    selected_templates: list[Template] = []
    family_summaries: list[dict[str, object]] = []

    min_grounded = int(ctx.config["templates"]["min_grounded_matches"])
    min_anchors = int(ctx.config["templates"]["min_valid_anchors"])

    print_status("Mining exact path_2, path_3, and triangle grounded counts from local adjacency index")
    path2_started = time.perf_counter()
    path2_counts = _mine_path2_counts(graph)
    path2_elapsed = round(time.perf_counter() - path2_started, 3)
    path3_started = time.perf_counter()
    path3_counts, triangle_counts = _mine_path3_and_triangle_counts(graph)
    path3_triangle_elapsed = round(time.perf_counter() - path3_started, 3)
    family_summaries.extend(
        [
            {
                "family": "path",
                "edge_count": 2,
                "candidate_count": len(path2_counts),
                "elapsed_sec": path2_elapsed,
                "mode": "exact_local",
            },
            {
                "family": "path",
                "edge_count": 3,
                "candidate_count": len(path3_counts),
                "elapsed_sec": path3_triangle_elapsed,
                "mode": "exact_local",
            },
            {
                "family": "triangle",
                "edge_count": 3,
                "candidate_count": len(triangle_counts),
                "elapsed_sec": path3_triangle_elapsed,
                "mode": "exact_local",
            },
        ]
    )

    _seed_candidate_rows(graph, candidate_rows, "path", path2_counts, min_grounded)
    _seed_candidate_rows(graph, candidate_rows, "path", path3_counts, min_grounded)
    _seed_candidate_rows(graph, candidate_rows, "triangle", triangle_counts, min_grounded)
    print_status("Completed local path/triangle counts; starting exact SQL template selection")

    conn = connect_postgres(ctx)
    try:
        path2_selected, path2_groups = _select_from_exact_counts(
            ctx,
            conn,
            graph,
            relation_mapping,
            family="path",
            exact_counts=path2_counts,
            required=1,
            candidate_rows=candidate_rows,
        )
        selected_templates.extend(path2_selected)
        family_summaries.append(
            {
                "family": "path",
                "edge_count": 2,
                "selected_count": len(path2_selected),
                "evaluated_grounded_groups": path2_groups,
                "mode": "exact_local_then_exact_anchor",
            }
        )

        path3_selected, path3_groups = _select_from_exact_counts(
            ctx,
            conn,
            graph,
            relation_mapping,
            family="path",
            exact_counts=path3_counts,
            required=1,
            candidate_rows=candidate_rows,
        )
        selected_templates.extend(path3_selected)
        family_summaries.append(
            {
                "family": "path",
                "edge_count": 3,
                "selected_count": len(path3_selected),
                "evaluated_grounded_groups": path3_groups,
                "mode": "exact_local_then_exact_anchor",
            }
        )

        triangle_selected, triangle_groups = _select_from_exact_counts(
            ctx,
            conn,
            graph,
            relation_mapping,
            family="triangle",
            exact_counts=triangle_counts,
            required=int(ctx.config["templates"]["select_num_triangles"]),
            candidate_rows=candidate_rows,
        )
        selected_templates.extend(triangle_selected)
        family_summaries.append(
            {
                "family": "triangle",
                "edge_count": 3,
                "selected_count": len(triangle_selected),
                "evaluated_grounded_groups": triangle_groups,
                "mode": "exact_local_then_exact_anchor",
            }
        )

        path4_selected, path4_summary = _select_length4_with_branch_and_bound(
            ctx,
            conn,
            graph,
            relation_mapping,
            family="path",
            prefix_counts=path3_counts,
            candidate_rows=candidate_rows,
        )
        selected_templates.extend(path4_selected)
        family_summaries.append(path4_summary)

        if ctx.config["templates"]["select_four_cycle_if_available"]:
            cycle_selected, cycle_summary = _select_length4_with_branch_and_bound(
                ctx,
                conn,
                graph,
                relation_mapping,
                family="cycle",
                prefix_counts=path3_counts,
                candidate_rows=candidate_rows,
            )
            selected_templates.extend(cycle_selected)
            family_summaries.append(cycle_summary)
    finally:
        conn.close()

    flat_candidate_rows = list(candidate_rows.values())
    flat_candidate_rows.sort(
        key=lambda row: (
            row["family"],
            int(row["edge_count"]),
            -int(row["grounded_match_count"]),
            tuple(row["relation_type_pattern"].split("|")),
        )
    )
    _write_candidate_summary_csv(ctx, family_summaries)
    ctx.write_yaml(
        Path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml",
        {"templates": [template.to_dict() for template in selected_templates]},
    )
    _write_selected_template_csv(ctx, selected_templates)
    ctx.write_json(
        Path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json",
        {
            "candidate_count": len(flat_candidate_rows),
            "selected_count": len(selected_templates),
            "min_grounded_matches": min_grounded,
            "min_valid_anchors": min_anchors,
            "selected_template_ids": [template.template_id for template in selected_templates],
            "families": family_summaries,
            "elapsed_sec": round(time.perf_counter() - started, 3),
        },
    )
    _write_prepare_figures(ctx, graph, family_summaries, selected_templates)


def _load_relation_mapping(ctx: AppContext) -> dict[str, str]:
    path = Path(ctx.config["paths"]["preprocess_dir"]) / "relation_type_map.csv"
    full_path = ctx.path(path)
    mapping: dict[str, str] = {}
    with full_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            mapping[row["raw_rel_type"]] = row["neo4j_rel_type"]
    return mapping


def _load_graph_index(ctx: AppContext) -> GraphIndex:
    nodes_path = ctx.path(ctx.config["paths"]["preprocess_dir"]) / "nodes.csv"
    edges_path = ctx.path(ctx.config["paths"]["preprocess_dir"]) / "edges.csv"
    if not nodes_path.exists() or not edges_path.exists():
        raise BenchmarkError("Preprocess outputs are missing; run phase 2 preprocessing before mining templates")

    node_name_to_idx: dict[str, int] = {}
    node_types: list[str] = []
    with nodes_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for idx, row in enumerate(reader):
            node_name_to_idx[row["node_id"]] = idx
            node_types.append(row["node_type"])

    out_adj: list[dict[int, set[int]]] = [dict() for _ in range(len(node_types))]
    in_adj: list[dict[int, set[int]]] = [dict() for _ in range(len(node_types))]
    rel_name_to_idx: dict[str, int] = {}
    rel_idx_to_name: dict[int, str] = {}
    rel_edges: dict[int, list[tuple[int, int]]] = defaultdict(list)
    rel_counts: Counter[int] = Counter()
    rel_types: dict[int, tuple[str, str]] = {}

    with edges_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            src_idx = node_name_to_idx[row["src_id"]]
            dst_idx = node_name_to_idx[row["dst_id"]]
            rel_type = row["rel_type"]
            rel_idx = rel_name_to_idx.setdefault(rel_type, len(rel_name_to_idx))
            rel_idx_to_name.setdefault(rel_idx, rel_type)

            rel_edges[rel_idx].append((src_idx, dst_idx))
            rel_counts[rel_idx] += 1
            rel_types.setdefault(rel_idx, (node_types[src_idx], node_types[dst_idx]))

            out_adj[src_idx].setdefault(rel_idx, set()).add(dst_idx)
            in_adj[dst_idx].setdefault(rel_idx, set()).add(src_idx)

    max_out_degree: Counter[int] = Counter()
    max_in_degree: Counter[int] = Counter()
    for mapping in out_adj:
        for rel_idx, neighbors in mapping.items():
            if len(neighbors) > max_out_degree[rel_idx]:
                max_out_degree[rel_idx] = len(neighbors)
    for mapping in in_adj:
        for rel_idx, neighbors in mapping.items():
            if len(neighbors) > max_in_degree[rel_idx]:
                max_in_degree[rel_idx] = len(neighbors)

    rel_info = {
        rel_idx: RelationInfo(
            rel_idx=rel_idx,
            rel_type=rel_idx_to_name[rel_idx],
            src_type=rel_types[rel_idx][0],
            dst_type=rel_types[rel_idx][1],
            edge_count=rel_counts[rel_idx],
            max_out_degree=max_out_degree[rel_idx],
            max_in_degree=max_in_degree[rel_idx],
        )
        for rel_idx in rel_idx_to_name
    }
    return GraphIndex(
        node_types=node_types,
        out_adj=out_adj,
        in_adj=in_adj,
        edges_by_rel=dict(rel_edges),
        rel_info=rel_info,
        rel_name_to_idx=rel_name_to_idx,
        rel_idx_to_name=rel_idx_to_name,
    )


def _mine_path2_counts(graph: GraphIndex) -> Counter[tuple[int, int]]:
    counts: Counter[tuple[int, int]] = Counter()
    total_nodes = len(graph.in_adj)
    for middle, incoming in enumerate(graph.in_adj):
        if middle > 0 and middle % 10000 == 0:
            print_status(f"path_2 scan {middle}/{total_nodes} nodes")
        outgoing = graph.out_adj[middle]
        if not incoming or not outgoing:
            continue
        for rel1, preds in incoming.items():
            pred_count = _count_without(preds, middle)
            if pred_count <= 0:
                continue
            for rel2, succs in outgoing.items():
                succ_count = _count_without(succs, middle)
                if succ_count <= 0:
                    continue
                invalid = _intersection_size(preds, succs, middle)
                grounded = pred_count * succ_count - invalid
                if grounded > 0:
                    counts[(rel1, rel2)] += grounded
    return counts


def _mine_path3_and_triangle_counts(graph: GraphIndex) -> tuple[Counter[tuple[int, int, int]], Counter[tuple[int, int, int]]]:
    path_counts: Counter[tuple[int, int, int]] = Counter()
    triangle_counts: Counter[tuple[int, int, int]] = Counter()
    relation_items = sorted(graph.edges_by_rel.items())
    total_relations = len(relation_items)
    for relation_index, (rel2, edges) in enumerate(relation_items, start=1):
        print_status(f"path_3/triangle scan relation {relation_index}/{total_relations} ({graph.rel_idx_to_name[rel2]})")
        for left, right in edges:
            if left == right:
                continue
            incoming = graph.in_adj[left]
            outgoing = graph.out_adj[right]
            if not incoming or not outgoing:
                continue
            for rel1, preds in incoming.items():
                pred_count = _count_without(preds, left, right)
                if pred_count <= 0:
                    continue
                for rel3, succs in outgoing.items():
                    succ_count = _count_without(succs, left, right)
                    if succ_count <= 0:
                        continue
                    shared = _intersection_size(preds, succs, left, right)
                    grounded = pred_count * succ_count - shared
                    key = (rel1, rel2, rel3)
                    if grounded > 0:
                        path_counts[key] += grounded
                    if shared > 0:
                        triangle_counts[key] += shared
    return path_counts, triangle_counts


def _select_from_exact_counts(
    ctx: AppContext,
    conn,
    graph: GraphIndex,
    relation_mapping: dict[str, str],
    *,
    family: str,
    exact_counts: Counter[tuple[int, ...]],
    required: int,
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]],
) -> tuple[list[Template], int]:
    min_grounded = int(ctx.config["templates"]["min_grounded_matches"])
    min_anchors = int(ctx.config["templates"]["min_valid_anchors"])
    if not exact_counts:
        return [], 0

    sorted_items = sorted(
        exact_counts.items(),
        key=lambda item: (-item[1], _relation_pattern_tuple(graph, item[0])),
    )
    print_status(
        f"Selecting {_family_stage_label(family, len(next(iter(exact_counts))))} from {len(sorted_items)} qualifying counted patterns"
    )

    selected: list[Template] = []
    evaluated_groups = 0
    cursor = 0
    while cursor < len(sorted_items) and len(selected) < required:
        grounded = sorted_items[cursor][1]
        if grounded < min_grounded:
            break
        same_grounded: list[tuple[tuple[int, ...], int]] = []
        while cursor < len(sorted_items) and sorted_items[cursor][1] == grounded:
            same_grounded.append(sorted_items[cursor])
            cursor += 1
        evaluated_groups += 1
        print_status(
            f"{_family_stage_label(family, len(same_grounded[0][0]))}: exact anchor eval group {evaluated_groups}, "
            f"grounded={grounded}, patterns_in_group={len(same_grounded)}, selected_so_far={len(selected)}/{required}"
        )

        evaluated_templates: list[Template] = []
        for group_index, (rel_seq, grounded_count) in enumerate(same_grounded, start=1):
            if group_index == 1 or group_index == len(same_grounded) or group_index % 10 == 0:
                print_status(
                    f"{_family_stage_label(family, len(rel_seq))}: evaluating pattern {group_index}/{len(same_grounded)} in current grounded-count group"
                )
            template = _evaluate_candidate(
                conn,
                graph,
                relation_mapping,
                family=family,
                rel_seq=rel_seq,
                grounded_override=grounded_count,
            )
            _update_candidate_row(candidate_rows, template, evaluation_stage="exact_anchor_eval")
            if template.valid_anchor_count >= min_anchors:
                evaluated_templates.append(template)

        evaluated_templates.sort(key=_ranking_key)
        selected.extend(evaluated_templates[: max(0, required - len(selected))])
    return selected, evaluated_groups


def _select_length4_with_branch_and_bound(
    ctx: AppContext,
    conn,
    graph: GraphIndex,
    relation_mapping: dict[str, str],
    *,
    family: str,
    prefix_counts: Counter[tuple[int, int, int]],
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]],
) -> tuple[list[Template], dict[str, object]]:
    if family == "path" and not bool(ctx.config["templates"].get("select_path4_if_available", True)):
        print_status("Skipping path_4 template selection because templates.select_path4_if_available=false")
        return [], {
            "family": family,
            "edge_count": 4,
            "candidate_count": 0,
            "selected_count": 0,
            "mode": "skipped",
            "exact_evaluations": 0,
            "stop_reason": "disabled_by_config",
        }

    exact_limit_key = "path4_branch_and_bound_max_exact_evals" if family == "path" else "cycle4_branch_and_bound_max_exact_evals"
    exact_limit = int(ctx.config["templates"][exact_limit_key])
    min_grounded = int(ctx.config["templates"]["min_grounded_matches"])
    min_anchors = int(ctx.config["templates"]["min_valid_anchors"])
    if exact_limit <= 0:
        print_status(f"Skipping {_family_stage_label(family, 4)} because templates.{exact_limit_key}={exact_limit}")
        return [], {
            "family": family,
            "edge_count": 4,
            "candidate_count": 0,
            "selected_count": 0,
            "mode": "skipped",
            "exact_evaluations": 0,
            "stop_reason": "exact_eval_disabled",
        }

    candidates, candidate_count = _iter_length4_candidates(
        graph,
        prefix_counts=prefix_counts,
        family=family,
    )
    if candidate_count == 0:
        return [], {
            "family": family,
            "edge_count": 4,
            "candidate_count": 0,
            "selected_count": 0,
            "mode": "branch_and_bound_exact",
            "exact_evaluations": 0,
            "stop_reason": "no_candidates",
        }

    print_status(
        f"Selecting {_family_stage_label(family, 4)} via branch-and-bound over {candidate_count} candidate patterns"
    )
    best_grounded = -1
    best_templates: list[Template] = []
    exact_evaluations = 0
    stop_reason = "bound_resolved"
    timeout_sec = int(ctx.config["templates"].get("length4_exact_eval_timeout_sec", 60))
    _set_statement_timeout(conn, timeout_sec * 1000 if timeout_sec > 0 else 0)

    try:
        for rel_seq, upper_bound in candidates:
            if best_grounded >= min_grounded and upper_bound < best_grounded:
                print_status(
                    f"{_family_stage_label(family, 4)}: stopping after {exact_evaluations} exact evaluations; "
                    f"remaining upper bound {upper_bound} is below best grounded count {best_grounded}"
                )
                break
            if exact_evaluations >= exact_limit:
                stop_reason = "exact_evaluation_limit_reached"
                raise BenchmarkError(
                    f"Length-4 {family} mining exceeded exact evaluation limit {exact_limit}; "
                    f"increase templates.{exact_limit_key}"
                )
            if exact_evaluations == 0 or (exact_evaluations + 1) % 10 == 0:
                print_status(
                    f"{_family_stage_label(family, 4)}: exact candidate {exact_evaluations + 1}/{candidate_count}, "
                    f"current_upper_bound={upper_bound}, best_grounded={best_grounded if best_grounded >= 0 else 'none'}"
                )
            try:
                template = _evaluate_candidate(
                    conn,
                    graph,
                    relation_mapping,
                    family=family,
                    rel_seq=rel_seq,
                    grounded_override=None,
                )
            except Exception as exc:
                exact_evaluations += 1
                conn.rollback()
                _set_statement_timeout(conn, timeout_sec * 1000 if timeout_sec > 0 else 0)
                print_status(
                    f"{_family_stage_label(family, 4)}: skipping candidate after evaluation failure "
                    f"({type(exc).__name__}: {exc})"
                )
                continue
            exact_evaluations += 1
            row = _update_candidate_row(candidate_rows, template, evaluation_stage="branch_and_bound_exact")
            row["upper_bound"] = upper_bound

            if template.grounded_match_count < min_grounded or template.valid_anchor_count < min_anchors:
                continue
            if template.grounded_match_count > best_grounded:
                best_grounded = template.grounded_match_count
                best_templates = [template]
            elif template.grounded_match_count == best_grounded:
                best_templates.append(template)
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        _set_statement_timeout(conn, 0)

    best_templates.sort(key=_ranking_key)
    return best_templates[:1], {
        "family": family,
        "edge_count": 4,
        "candidate_count": candidate_count,
        "selected_count": 1 if best_templates else 0,
        "mode": "branch_and_bound_exact",
        "exact_evaluations": exact_evaluations,
        "stop_reason": stop_reason,
    }


def _iter_length4_candidates(
    graph: GraphIndex,
    *,
    prefix_counts: Counter[tuple[int, int, int]],
    family: str,
) -> tuple[Iterable[tuple[tuple[int, int, int, int], int]], int]:
    prefix_cache: dict[tuple[int, int, int], list[tuple[int, int]]] = {}
    heap: list[tuple[int, tuple[str, ...], tuple[int, int, int], int]] = []
    candidate_count = 0

    for rel_seq in prefix_counts:
        options = _sorted_length4_options(graph, prefix_counts=prefix_counts, family=family, rel_seq=rel_seq)
        if not options:
            continue
        candidate_count += len(options)
        rel4, upper_bound = options[0]
        full_seq = rel_seq + (rel4,)
        heapq.heappush(heap, (-upper_bound, _relation_pattern_tuple(graph, full_seq), rel_seq, 0))

    def _generator() -> Iterable[tuple[tuple[int, int, int, int], int]]:
        while heap:
            neg_upper_bound, _, rel_seq, option_index = heapq.heappop(heap)
            options = prefix_cache.get(rel_seq)
            if options is None:
                options = _sorted_length4_options(graph, prefix_counts=prefix_counts, family=family, rel_seq=rel_seq)
                prefix_cache[rel_seq] = options
            rel4, upper_bound = options[option_index]
            yield rel_seq + (rel4,), int(-neg_upper_bound)
            next_index = option_index + 1
            if next_index < len(options):
                next_rel4, next_upper_bound = options[next_index]
                next_full_seq = rel_seq + (next_rel4,)
                heapq.heappush(heap, (-next_upper_bound, _relation_pattern_tuple(graph, next_full_seq), rel_seq, next_index))
            else:
                prefix_cache.pop(rel_seq, None)

    return _generator(), candidate_count


def _sorted_length4_options(
    graph: GraphIndex,
    *,
    prefix_counts: Counter[tuple[int, int, int]],
    family: str,
    rel_seq: tuple[int, int, int],
) -> list[tuple[int, int]]:
    rel1, rel2, rel3 = rel_seq
    grounded_count = prefix_counts[rel_seq]
    src_type = graph.rel_info[rel1].src_type
    tail_type = graph.rel_info[rel3].dst_type
    options: list[tuple[int, int]] = []
    for rel4, info in graph.rel_info.items():
        if info.src_type != tail_type:
            continue
        if family == "cycle" and info.dst_type != src_type:
            continue
        suffix_count = prefix_counts.get((rel2, rel3, rel4), 0)
        upper_bound = grounded_count * info.max_out_degree
        if suffix_count > 0:
            upper_bound = min(upper_bound, suffix_count * graph.rel_info[rel1].max_in_degree)
        if upper_bound > 0:
            options.append((rel4, upper_bound))
    options.sort(key=lambda item: (-item[1], _relation_pattern_tuple(graph, rel_seq + (item[0],))))
    return options


def _evaluate_candidate(
    conn,
    graph: GraphIndex,
    relation_mapping: dict[str, str],
    *,
    family: str,
    rel_seq: tuple[int, ...],
    grounded_override: int | None,
) -> Template:
    raw_rel_seq = [graph.rel_idx_to_name[idx] for idx in rel_seq]
    node_type_pattern = _node_type_pattern(graph, family, rel_seq)
    endpoint_type_pattern = _endpoint_pattern(family, node_type_pattern)
    template = build_template(
        family=family,
        relation_type_pattern=raw_rel_seq,
        node_type_pattern=node_type_pattern,
        endpoint_type_pattern=endpoint_type_pattern,
        grounded_match_count=0,
        valid_anchor_count=0,
        anchor_degree_min=0.0,
        anchor_degree_median=0.0,
        anchor_degree_p95=0.0,
        anchor_degree_max=0.0,
        relation_mapping=relation_mapping,
    )
    local_stats = _local_exact_anchor_stats(graph, template)
    if local_stats is not None:
        local_grounded, valid_anchor_count, degree_min, degree_median, degree_p95, degree_max = local_stats
        if grounded_override is not None and int(grounded_override) != int(local_grounded):
            raise BenchmarkError(
                f"Local grounded-count mismatch for {template.template_id}: "
                f"override={grounded_override}, local={local_grounded}"
            )
        grounded = int(grounded_override) if grounded_override is not None else int(local_grounded)
    else:
        grounded = grounded_override if grounded_override is not None else _grounded_match_count(conn, template)
        valid_anchor_count, degree_min, degree_median, degree_p95, degree_max = _anchor_stats(conn, template)
    template.grounded_match_count = grounded
    template.valid_anchor_count = valid_anchor_count
    template.anchor_degree_min = degree_min
    template.anchor_degree_median = degree_median
    template.anchor_degree_p95 = degree_p95
    template.anchor_degree_max = degree_max
    return template


def _local_exact_anchor_stats(
    graph: GraphIndex,
    template: Template,
) -> tuple[int, int, float, float, float, float] | None:
    if template.family == "path" and template.edge_count == 2:
        rel_seq = tuple(graph.rel_name_to_idx[rel] for rel in template.relation_type_pattern)
        anchor_counts = _path2_anchor_counts(graph, rel_seq)
    elif template.family == "path" and template.edge_count == 3:
        rel_seq = tuple(graph.rel_name_to_idx[rel] for rel in template.relation_type_pattern)
        anchor_counts = _path3_anchor_counts(graph, rel_seq)
    elif template.family == "triangle" and template.edge_count == 3:
        rel_seq = tuple(graph.rel_name_to_idx[rel] for rel in template.relation_type_pattern)
        anchor_counts = _triangle_anchor_counts(graph, rel_seq)
    else:
        return None

    grounded = int(sum(anchor_counts.values()))
    if not anchor_counts:
        return grounded, 0, 0.0, 0.0, 0.0, 0.0

    rel1 = graph.rel_name_to_idx[template.relation_type_pattern[0]]
    degrees = np.asarray(
        [float(len(graph.out_adj[anchor].get(rel1, set()))) for anchor in anchor_counts],
        dtype=float,
    )
    return (
        grounded,
        len(anchor_counts),
        float(np.min(degrees)),
        float(np.median(degrees)),
        float(np.percentile(degrees, 95)),
        float(np.max(degrees)),
    )


def _path2_anchor_counts(graph: GraphIndex, rel_seq: tuple[int, int]) -> Counter[int]:
    rel1, rel2 = rel_seq
    anchor_counts: Counter[int] = Counter()
    for anchor, middle in graph.edges_by_rel.get(rel1, []):
        if anchor == middle:
            continue
        successors = graph.out_adj[middle].get(rel2)
        if not successors:
            continue
        grounded = _count_without(successors, anchor, middle)
        if grounded > 0:
            anchor_counts[anchor] += grounded
    return anchor_counts


def _path3_anchor_counts(graph: GraphIndex, rel_seq: tuple[int, int, int]) -> Counter[int]:
    rel1, rel2, rel3 = rel_seq
    anchor_counts: Counter[int] = Counter()
    for left, right in graph.edges_by_rel.get(rel2, []):
        if left == right:
            continue
        predecessors = graph.in_adj[left].get(rel1)
        successors = graph.out_adj[right].get(rel3)
        if not predecessors or not successors:
            continue
        successor_count = _count_without(successors, left, right)
        if successor_count <= 0:
            continue
        for anchor in predecessors:
            if anchor == left or anchor == right:
                continue
            grounded = successor_count - (1 if anchor in successors else 0)
            if grounded > 0:
                anchor_counts[anchor] += grounded
    return anchor_counts


def _triangle_anchor_counts(graph: GraphIndex, rel_seq: tuple[int, int, int]) -> Counter[int]:
    rel1, rel2, rel3 = rel_seq
    anchor_counts: Counter[int] = Counter()
    for left, right in graph.edges_by_rel.get(rel2, []):
        if left == right:
            continue
        predecessors = graph.in_adj[left].get(rel1)
        successors = graph.out_adj[right].get(rel3)
        if not predecessors or not successors:
            continue
        if len(predecessors) <= len(successors):
            iter_anchors = predecessors
            other = successors
        else:
            iter_anchors = successors
            other = predecessors
        for anchor in iter_anchors:
            if anchor == left or anchor == right:
                continue
            if anchor in other:
                anchor_counts[anchor] += 1
    return anchor_counts


def _grounded_match_count(conn, template: Template) -> int:
    with conn.cursor() as cur:
        cur.execute(grounded_count_sql(template), grounded_count_params(template))
        return int(cur.fetchone()[0])


def _anchor_stats(conn, template: Template) -> tuple[int, float, float, float, float]:
    with conn.cursor() as cur:
        cur.execute(valid_anchors_sql(template), valid_anchors_params(template))
        rows = cur.fetchall()
    if not rows:
        return 0, 0.0, 0.0, 0.0, 0.0
    degrees = np.asarray([float(row[2]) for row in rows], dtype=float)
    return (
        len(rows),
        float(np.min(degrees)),
        float(np.median(degrees)),
        float(np.percentile(degrees, 95)),
        float(np.max(degrees)),
    )


def _set_statement_timeout(conn, timeout_ms: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {int(timeout_ms)}")


def _seed_candidate_rows(
    graph: GraphIndex,
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]],
    family: str,
    exact_counts: Counter[tuple[int, ...]],
    min_grounded: int,
) -> None:
    for rel_seq, grounded_count in exact_counts.items():
        if grounded_count < min_grounded:
            continue
        key = (family, tuple(graph.rel_idx_to_name[idx] for idx in rel_seq))
        candidate_rows[key] = _candidate_row(
            graph,
            family=family,
            rel_seq=rel_seq,
            grounded_count=grounded_count,
            evaluation_stage="exact_grounded_only",
        )


def _update_candidate_row(
    candidate_rows: dict[tuple[str, tuple[str, ...]], dict[str, object]],
    template: Template,
    *,
    evaluation_stage: str,
) -> dict[str, object]:
    rel_seq = tuple(template.relation_type_pattern)
    key = (template.family, rel_seq)
    row = candidate_rows.setdefault(
        key,
        {
            "template_id": template.template_id,
            "family": template.family,
            "edge_count": template.edge_count,
            "relation_type_pattern": "|".join(template.relation_type_pattern),
            "endpoint_type_pattern": "|".join(template.endpoint_type_pattern),
            "node_type_pattern": "|".join(template.node_type_pattern),
            "grounded_match_count": template.grounded_match_count,
            "valid_anchor_count": "",
            "anchor_degree_min": "",
            "anchor_degree_median": "",
            "anchor_degree_p95": "",
            "anchor_degree_max": "",
            "evaluation_stage": evaluation_stage,
        },
    )
    row.update(
        {
            "template_id": template.template_id,
            "family": template.family,
            "edge_count": template.edge_count,
            "relation_type_pattern": "|".join(template.relation_type_pattern),
            "endpoint_type_pattern": "|".join(template.endpoint_type_pattern),
            "node_type_pattern": "|".join(template.node_type_pattern),
            "grounded_match_count": template.grounded_match_count,
            "valid_anchor_count": template.valid_anchor_count,
            "anchor_degree_min": round(template.anchor_degree_min, 6),
            "anchor_degree_median": round(template.anchor_degree_median, 6),
            "anchor_degree_p95": round(template.anchor_degree_p95, 6),
            "anchor_degree_max": round(template.anchor_degree_max, 6),
            "evaluation_stage": evaluation_stage,
        }
    )
    return row


def _candidate_row(
    graph: GraphIndex,
    *,
    family: str,
    rel_seq: tuple[int, ...],
    grounded_count: int,
    evaluation_stage: str,
) -> dict[str, object]:
    raw_rel_seq = [graph.rel_idx_to_name[idx] for idx in rel_seq]
    node_type_pattern = _node_type_pattern(graph, family, rel_seq)
    endpoint_type_pattern = _endpoint_pattern(family, node_type_pattern)
    return {
        "template_id": _template_id(family, raw_rel_seq),
        "family": family,
        "edge_count": len(rel_seq),
        "relation_type_pattern": "|".join(raw_rel_seq),
        "endpoint_type_pattern": "|".join(endpoint_type_pattern),
        "node_type_pattern": "|".join(node_type_pattern),
        "grounded_match_count": grounded_count,
        "valid_anchor_count": "",
        "anchor_degree_min": "",
        "anchor_degree_median": "",
        "anchor_degree_p95": "",
        "anchor_degree_max": "",
        "evaluation_stage": evaluation_stage,
    }


def _write_candidate_summary_csv(ctx: AppContext, family_summaries: list[dict[str, object]]) -> None:
    rows = []
    for summary in family_summaries:
        if "candidate_count" not in summary:
            continue
        rows.append(
            {
                "fam": family_label(str(summary["family"])),
                "edges": fmt_int(summary["edge_count"]),
                "cand_n": fmt_int(summary["candidate_count"]),
                "sel_n": fmt_int(summary.get("selected_count", "")),
                "sec": fmt_num(summary.get("elapsed_sec", "")),
            }
        )
    ctx.write_csv(
        Path(ctx.config["paths"]["template_mining_dir"]) / "candidate_summary.csv",
        ["fam", "edges", "cand_n", "sel_n", "sec"],
        rows,
    )


def _write_selected_template_csv(ctx: AppContext, templates: list[Template]) -> None:
    label_map = template_label_map(templates)
    rows = [
        {
            "tid": label_map[template.template_id],
            "fam": family_label(template.family),
            "edges": fmt_int(template.edge_count),
            "grounded": fmt_int(template.grounded_match_count),
            "anchors": fmt_int(template.valid_anchor_count),
            "deg_med": fmt_num(template.anchor_degree_median),
            "deg_p95": fmt_num(template.anchor_degree_p95),
        }
        for template in templates
    ]
    ctx.write_csv(
        Path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv",
        ["tid", "fam", "edges", "grounded", "anchors", "deg_med", "deg_p95"],
        rows,
    )


def _write_prepare_figures(
    ctx: AppContext,
    graph: GraphIndex,
    family_summaries: list[dict[str, object]],
    selected_templates: list[Template],
) -> None:
    apply_plot_style(ctx)
    figure_dir = ctx.path(ctx.config["paths"]["prepare_figures_dir"])
    figure_dir.mkdir(parents=True, exist_ok=True)
    remove_existing_figures(
        figure_dir,
        [
        "template_profile.png",
        "relation_edge_counts_top20.png",
        "selected_template_grounded_matches.png",
        "candidate_count_by_family.png",
        ],
    )

    candidate_entries = []
    for summary in family_summaries:
        if "candidate_count" not in summary:
            continue
        candidate_entries.append((f"{family_label(str(summary['family']))}-{summary['edge_count']}", int(summary["candidate_count"])))
    if candidate_entries and selected_templates:
        label_map = template_label_map(selected_templates)
        fig, axes = plt.subplots(1, 2, figsize=(12.8, 5.6))

        left_ax = axes[0]
        ordered = sorted(candidate_entries)
        left_labels = [item[0] for item in ordered]
        left_values = [item[1] for item in ordered]
        left_ax.bar(range(len(left_labels)), left_values, color=NATURE_PALETTE["candidate"], width=0.7)
        left_ax.set_ylabel("Candidates")
        left_ax.set_xticks(range(len(left_labels)))
        left_ax.set_xticklabels(left_labels)
        style_axes(left_ax)

        right_ax = axes[1]
        tids = [label_map[template.template_id] for template in selected_templates]
        grounded = [template.grounded_match_count for template in selected_templates]
        anchors = [template.valid_anchor_count for template in selected_templates]
        x_values = np.arange(len(tids))
        width = 0.36
        right_ax.bar(x_values - width / 2, grounded, width=width, color=NATURE_PALETTE["template"], label="grounded")
        right_ax.bar(x_values + width / 2, anchors, width=width, color=NATURE_PALETTE["dataset"], label="anchors")
        right_ax.set_yscale("log")
        right_ax.set_ylabel("Count")
        right_ax.set_xticks(x_values)
        right_ax.set_xticklabels(tids)
        style_axes(right_ax)
        right_ax.legend()

        fig.tight_layout()
        fig.savefig(figure_dir / "template_profile.png", dpi=ctx.config["plotting"]["dpi"])
        plt.close(fig)

    write_figure_manifest(ctx, figure_dir)


def _template_id(family: str, raw_rel_seq: list[str]) -> str:
    if family == "path":
        return f"path_{len(raw_rel_seq)}_" + "__".join(raw_rel_seq)
    if family == "triangle":
        return "triangle_" + "__".join(raw_rel_seq)
    if family == "cycle":
        return "cycle_4_" + "__".join(raw_rel_seq)
    raise ValueError(family)


def _family_stage_label(family: str, edge_count: int) -> str:
    if family == "path":
        return f"path_{edge_count}"
    if family == "triangle":
        return "triangle"
    if family == "cycle":
        return "cycle_4"
    return f"{family}_{edge_count}"


def _node_type_pattern(graph: GraphIndex, family: str, rel_seq: tuple[int, ...]) -> list[str]:
    if family == "path":
        return [graph.rel_info[rel_seq[0]].src_type, *[graph.rel_info[idx].dst_type for idx in rel_seq]]
    if family == "triangle":
        return [
            graph.rel_info[rel_seq[0]].src_type,
            graph.rel_info[rel_seq[0]].dst_type,
            graph.rel_info[rel_seq[1]].dst_type,
        ]
    if family == "cycle":
        return [
            graph.rel_info[rel_seq[0]].src_type,
            graph.rel_info[rel_seq[0]].dst_type,
            graph.rel_info[rel_seq[1]].dst_type,
            graph.rel_info[rel_seq[2]].dst_type,
        ]
    raise ValueError(family)


def _endpoint_pattern(family: str, node_type_pattern: list[str]) -> list[str]:
    if family == "path":
        return [f"{node_type_pattern[idx]}:{node_type_pattern[idx + 1]}" for idx in range(len(node_type_pattern) - 1)]
    if family in {"triangle", "cycle"}:
        return [f"{node_type_pattern[idx]}:{node_type_pattern[(idx + 1) % len(node_type_pattern)]}" for idx in range(len(node_type_pattern))]
    raise ValueError(family)


def _relation_pattern_tuple(graph: GraphIndex, rel_seq: Iterable[int]) -> tuple[str, ...]:
    return tuple(graph.rel_idx_to_name[idx] for idx in rel_seq)


def _ranking_key(template: Template) -> tuple[object, ...]:
    return (
        -template.grounded_match_count,
        -template.valid_anchor_count,
        tuple(template.relation_type_pattern),
        tuple(template.endpoint_type_pattern),
    )


def _count_without(values: set[int], *excluded: int) -> int:
    count = len(values)
    seen: list[int] = []
    for value in excluded:
        if value in seen:
            continue
        seen.append(value)
        if value in values:
            count -= 1
    return count


def _intersection_size(left: set[int], right: set[int], *excluded: int) -> int:
    unique_excluded: list[int] = []
    for value in excluded:
        if value not in unique_excluded:
            unique_excluded.append(value)
    if len(left) > len(right):
        left, right = right, left
    count = 0
    for value in left:
        if value in unique_excluded:
            continue
        if value in right:
            count += 1
    return count
