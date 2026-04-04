from __future__ import annotations

import csv
from pathlib import Path

from .common import AppContext, load_yaml
from .templates import Template


def load_selected_templates(ctx: AppContext) -> list[Template]:
    payload = load_yaml(ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.yaml")
    return [Template.from_dict(item) for item in payload.get("templates", [])]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))
