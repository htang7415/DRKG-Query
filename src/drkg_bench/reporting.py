from __future__ import annotations

from collections import defaultdict

from .templates import Template


def template_label_map(templates: list[Template]) -> dict[str, str]:
    labels: dict[str, str] = {}
    counters: defaultdict[str, int] = defaultdict(int)
    for template in templates:
        if template.family == "path":
            label = f"P{template.edge_count}"
        elif template.family == "triangle":
            counters["triangle"] += 1
            label = f"T{counters['triangle']}"
        elif template.family == "cycle":
            counters["cycle"] += 1
            label = "C4" if counters["cycle"] == 1 else f"C4-{counters['cycle']}"
        else:
            counters[template.family] += 1
            label = f"{template.family[:1].upper()}{counters[template.family]}"
        labels[template.template_id] = label
    return labels


def template_label(template: Template, label_map: dict[str, str]) -> str:
    return label_map.get(template.template_id, template.template_id)


def regime_label(value: str) -> str:
    mapping = {
        "uniform_random": "uniform",
        "hub_anchored": "hub",
    }
    return mapping.get(value, value)


def engine_label(value: str) -> str:
    mapping = {
        "postgres": "pg",
        "neo4j": "neo",
    }
    return mapping.get(value, value)


def family_label(value: str) -> str:
    mapping = {
        "path": "path",
        "triangle": "tri",
        "cycle": "cycle",
    }
    return mapping.get(value, value)


def status_label(value: str) -> str:
    mapping = {
        "ok": "ok",
        "failed": "fail",
        "instrumented_failed": "inst_fail",
    }
    return mapping.get(value, value)


def join_class_label(value: str) -> str:
    mapping = {
        "default_plan": "default",
        "connected_prefix": "connected",
        "cross_product_inducing": "cross",
    }
    return mapping.get(value, value)


def fmt_num(value: object, *, digits: int = 3) -> str:
    if value in {"", None}:
        return ""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.{digits}f}".rstrip("0").rstrip(".")


def fmt_int(value: object) -> str:
    if value in {"", None}:
        return ""
    return str(int(float(value)))
