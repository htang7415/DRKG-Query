from __future__ import annotations

import importlib
import shutil
import sys
from dataclasses import asdict, dataclass

from .common import AppContext, print_status


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str


def _module_check(name: str) -> CheckResult:
    try:
        importlib.import_module(name)
        return CheckResult(name=name, status="ok", detail="imported")
    except Exception as exc:  # pragma: no cover - diagnostic path
        return CheckResult(name=name, status="error", detail=str(exc))


def run_env_check(ctx: AppContext) -> None:
    print_status("Environment check: validating tools, secrets, and config wiring")
    service_mode = ctx.config.get("services", {}).get("mode", "manual")
    postgres_restart = ctx.config["postgres"].get("restart_command") or ("docker-fallback" if service_mode == "docker" else "missing")
    neo4j_restart = ctx.config["neo4j"].get("restart_command") or ("docker-fallback" if service_mode == "docker" else "missing")
    postgres_secret_source = "missing"
    if ctx.config["postgres"].get("password_env"):
        postgres_secret_source = f"env:{ctx.config['postgres']['password_env']}"
    if ctx.config["postgres"].get("password_value"):
        postgres_secret_source = "config:password_value"

    neo4j_secret_source = "missing"
    if ctx.config["neo4j"].get("password_env"):
        neo4j_secret_source = f"env:{ctx.config['neo4j']['password_env']}"
    if ctx.config["neo4j"].get("password_value"):
        neo4j_secret_source = "config:password_value"

    results = [
        CheckResult(name="python", status="ok", detail=sys.version.split()[0]),
        CheckResult(
            name="raw_drkg",
            status="ok" if ctx.path(ctx.config["paths"]["raw_drkg"]).exists() else "error",
            detail=str(ctx.path(ctx.config["paths"]["raw_drkg"])),
        ),
        CheckResult(name="service_mode", status="ok", detail=str(service_mode)),
        CheckResult(
            name="service_auto_start",
            status="ok",
            detail=str(bool(ctx.config.get("services", {}).get("auto_start", False))).lower(),
        ),
        CheckResult(name="psql_cli", status="ok" if shutil.which("psql") else "warn", detail=shutil.which("psql") or "not found"),
        CheckResult(name="neo4j_cli", status="ok" if shutil.which("neo4j") else "warn", detail=shutil.which("neo4j") or "not found"),
        CheckResult(name="docker_cli", status="ok" if shutil.which("docker") else "warn", detail=shutil.which("docker") or "not found"),
        CheckResult(
            name="postgres_password_source",
            status="ok" if postgres_secret_source != "missing" else "error",
            detail=postgres_secret_source,
        ),
        CheckResult(
            name="neo4j_password_source",
            status="ok" if neo4j_secret_source != "missing" else "error",
            detail=neo4j_secret_source,
        ),
        CheckResult(
            name="postgres_restart",
            status="ok" if postgres_restart != "missing" else "error",
            detail=str(postgres_restart),
        ),
        CheckResult(
            name="neo4j_restart",
            status="ok" if neo4j_restart != "missing" else "error",
            detail=str(neo4j_restart),
        ),
    ]
    for module in ["yaml", "psycopg", "neo4j", "matplotlib", "networkx", "numpy", "pulp"]:
        results.append(_module_check(module))

    payload = {
        "checks": [asdict(item) for item in results],
        "config_path": str(ctx.config_path.relative_to(ctx.root)),
    }
    print_status("Environment check: writing environment_report.json")
    ctx.write_json(ctx.config["paths"]["env_dir"] + "/environment_report.json", payload)
