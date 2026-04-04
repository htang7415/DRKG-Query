from __future__ import annotations

from .common import AppContext, BenchmarkError, run_command
from .neo4j_db import restart_neo4j, wait_for_neo4j
from .postgres import restart_postgres, wait_for_postgres


def attempt_page_cache_flush(ctx: AppContext) -> dict[str, object]:
    command = ctx.config.get("system", {}).get("page_cache_flush_command")
    if not command:
        return {"attempted": False, "success": False, "detail": "page cache flush command not configured"}
    try:
        result = run_command(command)
        return {"attempted": True, "success": True, "detail": result.stdout.strip() or "ok"}
    except Exception as exc:  # pragma: no cover - external command path
        return {"attempted": True, "success": False, "detail": str(exc)}


def restart_engine_for_instance(ctx: AppContext, engine: str) -> dict[str, object]:
    cache_flush = attempt_page_cache_flush(ctx)
    if not ctx.config["benchmark"].get("restart_dbms_per_instance", True):
        return {"cache_flush": cache_flush, "restarted": False}

    if engine == "postgres":
        restart_postgres(ctx)
        wait_for_postgres(ctx)
    elif engine == "neo4j":
        restart_neo4j(ctx)
        wait_for_neo4j(ctx)
    else:
        raise BenchmarkError(f"Unsupported engine: {engine}")
    return {"cache_flush": cache_flush, "restarted": True}
