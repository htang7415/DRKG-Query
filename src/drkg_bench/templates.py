from __future__ import annotations

import itertools
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class EdgeDef:
    alias: str
    src_var: str
    dst_var: str
    rel_type: str
    neo4j_rel_type: str | None = None


@dataclass
class Template:
    template_id: str
    family: str
    edge_count: int
    node_vars: list[str]
    anchor_var: str
    node_type_pattern: list[str]
    endpoint_type_pattern: list[str]
    relation_type_pattern: list[str]
    edges: list[EdgeDef]
    grounded_match_count: int
    valid_anchor_count: int
    anchor_degree_min: float
    anchor_degree_median: float
    anchor_degree_p95: float
    anchor_degree_max: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["edges"] = [asdict(edge) for edge in self.edges]
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Template":
        payload = dict(payload)
        payload["edges"] = [EdgeDef(**edge) for edge in payload["edges"]]
        return cls(**payload)


def build_template(
    *,
    family: str,
    relation_type_pattern: list[str],
    node_type_pattern: list[str],
    endpoint_type_pattern: list[str],
    grounded_match_count: int,
    valid_anchor_count: int,
    anchor_degree_min: float,
    anchor_degree_median: float,
    anchor_degree_p95: float,
    anchor_degree_max: float,
    relation_mapping: dict[str, str] | None = None,
) -> Template:
    relation_mapping = relation_mapping or {}
    edge_count = len(relation_type_pattern)
    if family == "path":
        node_vars = [f"v{i}" for i in range(1, edge_count + 2)]
        edges = [
            EdgeDef(
                alias=f"e{i}",
                src_var=node_vars[i - 1],
                dst_var=node_vars[i],
                rel_type=rel,
                neo4j_rel_type=relation_mapping.get(rel),
            )
            for i, rel in enumerate(relation_type_pattern, start=1)
        ]
        template_id = f"path_{edge_count}_" + "__".join(relation_type_pattern)
    elif family == "triangle":
        node_vars = ["v1", "v2", "v3"]
        edges = [
            EdgeDef("e1", "v1", "v2", relation_type_pattern[0], relation_mapping.get(relation_type_pattern[0])),
            EdgeDef("e2", "v2", "v3", relation_type_pattern[1], relation_mapping.get(relation_type_pattern[1])),
            EdgeDef("e3", "v3", "v1", relation_type_pattern[2], relation_mapping.get(relation_type_pattern[2])),
        ]
        template_id = "triangle_" + "__".join(relation_type_pattern)
    elif family == "cycle":
        node_vars = ["v1", "v2", "v3", "v4"]
        edges = [
            EdgeDef("e1", "v1", "v2", relation_type_pattern[0], relation_mapping.get(relation_type_pattern[0])),
            EdgeDef("e2", "v2", "v3", relation_type_pattern[1], relation_mapping.get(relation_type_pattern[1])),
            EdgeDef("e3", "v3", "v4", relation_type_pattern[2], relation_mapping.get(relation_type_pattern[2])),
            EdgeDef("e4", "v4", "v1", relation_type_pattern[3], relation_mapping.get(relation_type_pattern[3])),
        ]
        template_id = "cycle_4_" + "__".join(relation_type_pattern)
    else:
        raise ValueError(f"Unsupported family: {family}")

    return Template(
        template_id=template_id,
        family=family,
        edge_count=edge_count,
        node_vars=node_vars,
        anchor_var="v1",
        node_type_pattern=node_type_pattern,
        endpoint_type_pattern=endpoint_type_pattern,
        relation_type_pattern=relation_type_pattern,
        edges=edges,
        grounded_match_count=grounded_match_count,
        valid_anchor_count=valid_anchor_count,
        anchor_degree_min=anchor_degree_min,
        anchor_degree_median=anchor_degree_median,
        anchor_degree_p95=anchor_degree_p95,
        anchor_degree_max=anchor_degree_max,
    )


def node_var_columns(template: Template) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for edge in template.edges:
        mapping.setdefault(edge.src_var, []).append(f"{edge.alias}.src_id")
        mapping.setdefault(edge.dst_var, []).append(f"{edge.alias}.dst_id")
    return mapping


def node_var_type_columns(template: Template) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for edge in template.edges:
        mapping.setdefault(edge.src_var, []).append(f"{edge.alias}.src_type")
        mapping.setdefault(edge.dst_var, []).append(f"{edge.alias}.dst_type")
    return mapping


def equality_predicates(template: Template) -> list[str]:
    predicates: list[str] = []
    for _, columns in node_var_columns(template).items():
        first = columns[0]
        for other in columns[1:]:
            predicates.append(f"{first} = {other}")
    return predicates


def distinct_node_predicates(template: Template) -> list[str]:
    columns = node_var_columns(template)
    canonical = {var: refs[0] for var, refs in columns.items()}
    predicates = []
    for left, right in itertools.combinations(template.node_vars, 2):
        predicates.append(f"{canonical[left]} <> {canonical[right]}")
    return predicates


def non_reuse_predicates(template: Template) -> list[str]:
    predicates = []
    for left, right in itertools.combinations(template.edges, 2):
        if left.rel_type != right.rel_type:
            continue
        predicates.append(
            f"NOT ({left.alias}.src_id = {right.alias}.src_id AND "
            f"{left.alias}.dst_id = {right.alias}.dst_id)"
        )
    return predicates


def anchor_expression(template: Template) -> str:
    return "e1.src_id"


def default_count_sql(template: Template) -> str:
    from_clause = ", ".join(f"typed_edges {edge.alias}" for edge in template.edges)
    where_parts = [f"{edge.alias}.rel_type = %s" for edge in template.edges]
    where_parts.extend(node_type_predicates(template))
    where_parts.extend(equality_predicates(template))
    where_parts.extend(distinct_node_predicates(template))
    where_parts.extend(non_reuse_predicates(template))
    where_parts.append(f"{anchor_expression(template)} = %s")
    return (
        "SELECT COUNT(*) AS output_cardinality "
        f"FROM {from_clause} "
        "WHERE " + " AND ".join(where_parts)
    )


def default_count_params(template: Template, anchor_id: str) -> list[Any]:
    return [*template.relation_type_pattern, *template.node_type_pattern, anchor_id]


def valid_anchors_sql(template: Template) -> str:
    where_parts = [f"{edge.alias}.rel_type = %s" for edge in template.edges]
    where_parts.extend(node_type_predicates(template))
    where_parts.extend(equality_predicates(template))
    where_parts.extend(distinct_node_predicates(template))
    where_parts.extend(non_reuse_predicates(template))
    from_clause = ", ".join(f"typed_edges {edge.alias}" for edge in template.edges)
    return f"""
        WITH matches AS (
            SELECT {anchor_expression(template)} AS anchor_id
            FROM {from_clause}
            WHERE {" AND ".join(where_parts)}
        )
        SELECT
            m.anchor_id,
            COUNT(*) AS grounded_match_count,
            d.first_edge_degree
        FROM matches m
        JOIN anchor_degrees d
          ON d.rel_type = %s
         AND d.anchor_id = m.anchor_id
        GROUP BY m.anchor_id, d.first_edge_degree
        ORDER BY m.anchor_id
    """


def valid_anchors_params(template: Template) -> list[Any]:
    return [*template.relation_type_pattern, *template.node_type_pattern, template.relation_type_pattern[0]]


def grounded_count_sql(template: Template) -> str:
    from_clause = ", ".join(f"typed_edges {edge.alias}" for edge in template.edges)
    where_parts = [f"{edge.alias}.rel_type = %s" for edge in template.edges]
    where_parts.extend(node_type_predicates(template))
    where_parts.extend(equality_predicates(template))
    where_parts.extend(distinct_node_predicates(template))
    where_parts.extend(non_reuse_predicates(template))
    return (
        "SELECT COUNT(*) AS grounded_match_count "
        f"FROM {from_clause} "
        "WHERE " + " AND ".join(where_parts)
    )


def grounded_count_params(template: Template) -> list[Any]:
    return [*template.relation_type_pattern, *template.node_type_pattern]


def forced_order_sql(template: Template, order: tuple[str, ...]) -> tuple[str, str]:
    all_equalities = _atomic_equalities(template)
    consumed: set[int] = set()
    included = {order[0]}
    current_sql = (
        f"(SELECT src_id, src_type, dst_id, dst_type, rel_type FROM typed_edges WHERE rel_type = %s) AS {order[0]}"
    )
    classification = "connected_prefix"

    for alias in order[1:]:
        available_indexes = [
            idx
            for idx, item in enumerate(all_equalities)
            if idx not in consumed and ((item[0] == alias and item[1] in included) or (item[1] == alias and item[0] in included))
        ]
        relation_sql = (
            f"(SELECT src_id, src_type, dst_id, dst_type, rel_type FROM typed_edges WHERE rel_type = %s) AS {alias}"
        )
        if available_indexes:
            join_pred = " AND ".join(all_equalities[idx][2] for idx in available_indexes)
            current_sql = f"({current_sql} JOIN {relation_sql} ON {join_pred})"
            consumed.update(available_indexes)
        else:
            classification = "cross_product_inducing"
            current_sql = f"({current_sql} CROSS JOIN {relation_sql})"
        included.add(alias)

    remaining_predicates = [all_equalities[idx][2] for idx in range(len(all_equalities)) if idx not in consumed]
    remaining_predicates.extend(node_type_predicates(template))
    remaining_predicates.extend(distinct_node_predicates(template))
    remaining_predicates.extend(non_reuse_predicates(template))
    remaining_predicates.append(f"{anchor_expression(template)} = %s")
    sql = (
        "SELECT COUNT(*) AS output_cardinality "
        f"FROM {current_sql} "
        "WHERE " + " AND ".join(remaining_predicates)
    )
    return sql, classification


def forced_order_params(template: Template, order: tuple[str, ...], anchor_id: str) -> list[Any]:
    rel_by_alias = {edge.alias: edge.rel_type for edge in template.edges}
    return [*(rel_by_alias[alias] for alias in order), *template.node_type_pattern, anchor_id]


def all_left_deep_orders(template: Template) -> list[tuple[str, ...]]:
    return list(itertools.permutations([edge.alias for edge in template.edges]))


def cypher_count_query(template: Template, *, profile: bool) -> str:
    prefix = "CYPHER runtime=slotted PROFILE" if profile else "CYPHER runtime=slotted"
    pieces = [f"({template.anchor_var}:Entity {{node_id: $anchor_id}})"]
    for index, edge in enumerate(template.edges, start=1):
        rel = edge.neo4j_rel_type or edge.rel_type
        next_node = edge.dst_var
        pieces.append(f"-[{edge.alias}:`{rel}`]->({next_node}:Entity)")
    pattern = "".join(pieces)
    distinct_predicates = [f"{left} <> {right}" for left, right in itertools.combinations(template.node_vars, 2)]
    same_edge_predicates = [f"id({left.alias}) <> id({right.alias})" for left, right in itertools.combinations(template.edges, 2) if left.rel_type == right.rel_type]
    type_predicates = [f"{var}.node_type = ${var}_type" for var in template.node_vars]
    where_clause = " AND ".join(type_predicates + distinct_predicates + same_edge_predicates)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    return f"{prefix} MATCH {pattern} {where_sql} RETURN count(*) AS output_cardinality"


def cypher_params(template: Template, anchor_id: str) -> dict[str, Any]:
    params: dict[str, Any] = {"anchor_id": anchor_id}
    for var, node_type in zip(template.node_vars, template.node_type_pattern, strict=True):
        params[f"{var}_type"] = node_type
    return params


def _atomic_equalities(template: Template) -> list[tuple[str, str, str]]:
    result: list[tuple[str, str, str]] = []
    for _, columns in node_var_columns(template).items():
        first_alias, first_col = columns[0].split(".")
        for other in columns[1:]:
            other_alias, other_col = other.split(".")
            result.append((first_alias, other_alias, f"{first_alias}.{first_col} = {other_alias}.{other_col}"))
    return result


def node_type_predicates(template: Template) -> list[str]:
    predicates = []
    canonical = {var: refs[0] for var, refs in node_var_type_columns(template).items()}
    for var, node_type in zip(template.node_vars, template.node_type_pattern, strict=True):
        predicates.append(f"{canonical[var]} = %s")
    return predicates
