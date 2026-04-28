from __future__ import annotations

import json
import shutil
from pathlib import Path

from .artifacts import read_csv_rows
from .common import AppContext, print_status


def build_final_package(ctx: AppContext) -> None:
    print_status("Final package: refreshing final_tables and final_figures")
    final_dir = ctx.path(ctx.config["paths"]["final_dir"])
    final_dir.mkdir(parents=True, exist_ok=True)

    final_tables = final_dir / "final_tables"
    final_figures = final_dir / "final_figures"
    if final_tables.exists():
        shutil.rmtree(final_tables)
    if final_figures.exists():
        shutil.rmtree(final_figures)
    final_tables.mkdir(exist_ok=True)
    final_figures.mkdir(exist_ok=True)

    story_tables = [
        ("1_selected_templates.csv", ctx.path(ctx.config["paths"]["template_mining_dir"]) / "selected_templates.csv"),
        ("2_engine_summary.csv", ctx.path(ctx.config["paths"]["comparison_dir"]) / "engine_summary.csv"),
        ("3_join_order_summary.csv", ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "join_order_summary.csv"),
    ]
    for target_name, table_path in story_tables:
        if table_path.exists():
            shutil.copy2(table_path, final_tables / target_name)

    raw_reachability = ctx.path(ctx.config["paths"].get("reachability_dir", "")) / "reachability_runtime.csv"
    if raw_reachability.exists():
        _write_reachability_summary(ctx, raw_reachability, final_tables / "4_reachability_runtime.csv")

    print_status("Final package: copying figure PNGs")
    story_figures = [
        ("1_template_profile.png", ctx.path(ctx.config["paths"]["prepare_figures_dir"]) / "template_profile.png"),
        ("2_engine_runtime.png", ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "engine_runtime.png"),
        ("3_structure_runtime.png", ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "structure_runtime.png"),
        ("4_agm_runtime.png", ctx.path(ctx.config["paths"]["analysis_figures_dir"]) / "agm_runtime.png"),
        ("5_join_order_effect.png", ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "join_order_effect.png"),
        ("6_reachability_runtime.png", ctx.path(ctx.config["paths"]["experiments_figures_dir"]) / "reachability_runtime.png"),
    ]
    for target_name, figure_path in story_figures:
        if figure_path.exists():
            shutil.copy2(figure_path, final_figures / target_name)

    shutil.copy2(ctx.config_path, final_dir / "config_snapshot.yaml")
    print_status("Final package: writing results markdown")
    _write_final_markdown(ctx, final_dir)
    print_status("Final package: writing final_manifest.json")
    ctx.write_json(
        final_dir / "final_manifest.json",
        {
            "final_docs": sorted(path.name for path in final_dir.glob("*.md")),
            "final_tables": sorted(path.name for path in final_tables.glob("*")),
            "final_figures": sorted(path.name for path in final_figures.glob("*")),
        },
    )
    print_status("Final package: pruning low-value intermediate artifacts")
    _prune_intermediate_artifacts(ctx)


def _prune_intermediate_artifacts(ctx: AppContext) -> None:
    removable_paths = [
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "nodes.csv",
        ctx.path(ctx.config["paths"]["preprocess_dir"]) / "edges.csv",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "baseline_bindings.csv",
        ctx.path(ctx.config["paths"]["bindings_dir"]) / "join_order_bindings.csv",
        ctx.path(ctx.config["paths"]["postgres_baseline_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["neo4j_baseline_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["join_order_dir"]) / "logs",
        ctx.path(ctx.config["paths"]["theory_dir"]) / "template_hypergraphs.json",
    ]
    for path in removable_paths:
        _remove_if_exists(path)


def _remove_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink()


def _write_final_markdown(ctx: AppContext, final_dir: Path) -> None:
    final_tables = final_dir / "final_tables"
    preprocess_summary = _read_json(ctx.path(ctx.config["paths"]["preprocess_dir"]) / "preprocess_summary.json")
    mining_summary = _read_json(ctx.path(ctx.config["paths"]["template_mining_dir"]) / "mining_summary.json")
    summary_metrics = _read_json(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "summary_metrics.json")
    comparison_metrics = _read_json(ctx.path(ctx.config["paths"]["comparison_dir"]) / "comparison_metrics.json")
    reachability_summary_path = ctx.path(ctx.config["paths"].get("reachability_dir", "")) / "reachability_summary.json"
    reachability_summary = _read_json(reachability_summary_path) if reachability_summary_path.exists() else {}
    selected_templates = read_csv_rows(final_tables / "1_selected_templates.csv")
    template_summary = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "template_summary.csv")
    structure_summary = read_csv_rows(ctx.path(ctx.config["paths"]["analysis_summary_dir"]) / "structure_summary.csv")
    join_order_summary = read_csv_rows(final_tables / "3_join_order_summary.csv")
    reachability_runtime_path = final_tables / "4_reachability_runtime.csv"
    reachability_rows = read_csv_rows(reachability_runtime_path) if reachability_runtime_path.exists() else []
    has_c4 = any(row.get("tid") == "C4" for row in selected_templates)

    engine_lines = []
    for row in template_summary:
        failures = int(row["n"]) - int(row["ok_n"])
        line = (
            f"- `{row['tid']}` / `{row['reg']}` / `{row['eng']}`: "
            f"median=`{row['med_ms']}` ms, IQR=`{row['q1_ms']}`-`{row['q3_ms']}` ms, "
            f"fully_ok=`{row['ok_n']}/{row['n']}`, median output=`{_fmt_number(row['med_out'])}`"
        )
        if failures:
            line += f", non_ok_rows=`{failures}`"
        engine_lines.append(line)

    structure_lines = []
    for row in structure_summary:
        work_piece = f", median work=`{_fmt_number(row['med_work'])}`" if row.get("med_work") else ""
        agm_piece = f", median AGM=`{_fmt_number(row['med_agm'])}`" if row.get("med_agm") else ""
        structure_lines.append(
            f"- `{row['eng']}` / `{row['reg']}` / `{row['shape']}`: "
            f"median=`{row['med_ms']}` ms, fully_ok=`{row['ok_n']}/{row['n']}`, "
            f"median output=`{_fmt_number(row['med_out'])}`{work_piece}{agm_piece}"
        )

    join_lines = []
    for row in join_order_summary:
        line = (
            f"- `{row['tid']}` / `{row['join_cls']}`: "
            f"median=`{row['med_ms'] or 'NA'}` ms, fully_ok=`{row['ok_n']}/{row['n']}`"
        )
        if row.get("med_work"):
            line += f", median work=`{_fmt_number(row['med_work'])}`"
        join_lines.append(line)

    reachability_lines = []
    for row in reachability_rows:
        reachability_lines.append(
            f"- `{row['reg']}` / depth `{row['depth']}` ({row['n_anchors']} anchors): "
            f"anchor_deg_med=`{row['anchor_deg_med']}`, reach_med=`{row['reach_med']}`, "
            f"pg=`{row['pg_ms']}` ms, duck=`{row['duck_ms']}` ms, neo=`{row['neo_ms']}` ms"
        )

    lines = [
        "# Final Results Report",
        "",
        "This report summarizes the packaged benchmark results in `results/05_final/`.",
        "",
        "## Scope",
        f"- Baseline instances: PostgreSQL=`{summary_metrics['postgres_baseline_instances']}`, DuckDB=`{summary_metrics.get('duckdb_baseline_instances', 0)}`, Neo4j=`{summary_metrics['neo4j_baseline_instances']}`",
        f"- Join-order instances: `{summary_metrics['join_order_instances']}`",
        f"- Reachability instances: `{reachability_summary.get('rows', 0)}`",
        f"- Matched engine instances: `{comparison_metrics['matched_instances']}`",
        f"- Matching output-cardinality instances: `{comparison_metrics['matching_output_cardinality_instances']}`",
        f"- DuckDB matching output-cardinality instances: `{comparison_metrics.get('duckdb_matching_output_cardinality_instances', 0)}`",
        "",
        "## Dataset",
        f"- Raw rows: `{preprocess_summary['raw_rows']}`",
        f"- Unique edges: `{preprocess_summary['unique_edges']}`",
        f"- Unique nodes: `{preprocess_summary['unique_nodes']}`",
        f"- Unique relations: `{preprocess_summary['unique_relations']}`",
        f"- Clean-up: dropped `{preprocess_summary['rows_with_empty_endpoint_dropped']}` empty-endpoint rows and `{preprocess_summary['duplicate_rows_dropped']}` duplicate triples",
        "",
        "## Templates",
        f"- Candidates enumerated: `{mining_summary['candidate_count']}`",
        f"- Templates selected: `{mining_summary['selected_count']}`",
        f"- Mining time: `{mining_summary['elapsed_sec']}` seconds",
        "",
        "Selected templates:",
    ]
    for row in selected_templates:
        lines.append(
            f"- `{row['tid']}`: `{row['fam']}` with `{row['edges']}` edges, grounded=`{_fmt_number(row['grounded'])}`, anchors=`{_fmt_number(row['anchors'])}`"
        )

    lines.extend(
        [
            "",
            "## Engine Results",
            f"- Mean PostgreSQL/Neo4j runtime ratio: `{_fmt_optional_float(comparison_metrics.get('mean_neo_over_pg_speedup'))}`",
            f"- Mean PostgreSQL/DuckDB runtime ratio: `{_fmt_optional_float(comparison_metrics.get('mean_pg_over_duckdb_speedup'))}`",
            *(
                [
                    "- C4 was added as a hard-query extension. PostgreSQL and DuckDB C4 rows use the standard baseline harness; Neo4j C4 rows use reduced measured timing without `PROFILE` to avoid the profile pass dominating the extension."
                ]
                if has_c4
                else []
            ),
            "",
            "Per-template summary:",
            *engine_lines,
            "",
            "## Reachability",
            "Bounded reachability uses a small directional sample. Each row aggregates 5 anchors per (regime, depth) by reporting median anchor degree, median reachable count, and median runtime per engine.",
            *reachability_lines,
            "",
            "## Structure Summary",
            *structure_lines,
            "",
            "## Join-Order Summary",
            *join_lines,
            "",
            "## Findings",
            "- The benchmark now compares three execution models: Neo4j graph traversal, PostgreSQL row-store SQL, and DuckDB columnar SQL.",
            "- Hub-anchored queries are consistently slower than uniform-random queries across the fixed-query workloads.",
            *(
                [
                    "- The added 4-cycle `C4` directly tests a harder cyclic query shape beyond triangles."
                ]
                if has_c4
                else []
            ),
            "- The acyclic 3-edge path `P3` is the hardest workload in the package; it is much slower and less stable than the triangle workloads despite being acyclic.",
            "- Bounded reachability is a separate recursive/path-expansion workload: SQL engines expand a recursive frontier, while Neo4j variable-length Cypher enumerates paths before counting distinct endpoints.",
            "- PostgreSQL join-order choice matters materially: connected/default orders for `P3` succeed on some bindings, while the cross-product-inducing class times out on all attempted `P3` bindings.",
            "- Neo4j timing rows are present, but Neo4j profile-derived work metrics are unavailable in the saved package because the separate `PROFILE` pass failed runtime verification.",
            "",
            "## Conclusion",
            "The saved DRKG benchmark results support a practical conclusion rather than a purely structural one. Runtime is dominated by skew, anchor choice, output size, and intermediate expansion, not by the acyclic-versus-cyclic label alone. The expanded package adds DuckDB as a modern SQL engine and bounded reachability as a recursive workload, so the comparison now separates fixed typed joins from path-expansion behavior. The triangle queries remain comparatively stable, while the selected 3-edge path and hub reachability cases are the main stress cases.",
            "",
            "## Files",
            "- `final_tables/`: 4 story tables — selected templates, cross-engine summary, PG join-order summary, per-anchor reachability runtime.",
            "- `final_figures/`: 6 story figures — template profile, engine runtime (with engine/pg ratio labels), structure runtime, AGM vs runtime, join-order effect, reachability runtime.",
            "- `config_snapshot.yaml`: configuration used for the packaged run.",
            "",
        ]
    )
    (final_dir / "results_and_conclusion.md").write_text("\n".join(lines), encoding="utf-8")


def _write_reachability_summary(ctx: AppContext, raw_path: Path, out_path: Path) -> None:
    raw_rows = read_csv_rows(raw_path)
    grouped: dict[tuple[str, str], list[dict[str, str]]] = {}
    for row in raw_rows:
        grouped.setdefault((row["reg"], row["depth"]), []).append(row)

    def _med(values: list[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2.0

    def _per_engine_median(rows: list[dict[str, str]], eng: str) -> float | None:
        ms_values = [
            float(row["ms"])
            for row in rows
            if row.get("eng") == eng and row.get("status") == "ok" and row.get("ms")
        ]
        return _med(ms_values)

    summary_rows: list[dict[str, str]] = []
    for reg in ("hub", "uniform"):
        for depth in ("2", "3"):
            rows = grouped.get((reg, depth), [])
            if not rows:
                continue
            anchor_index: dict[str, dict[str, str]] = {}
            for row in rows:
                anchor_index.setdefault(row["bid"], row)
            anchor_degs = [float(r["anchor_out_degree"]) for r in anchor_index.values() if r.get("anchor_out_degree")]
            reach_values = [float(r["out"]) for r in rows if r.get("eng") == "pg" and r.get("out")]
            summary_rows.append(
                {
                    "reg": reg,
                    "depth": depth,
                    "n_anchors": str(len(anchor_index)),
                    "anchor_deg_med": _fmt_optional_int(_med(anchor_degs)),
                    "reach_med": _fmt_optional_int(_med(reach_values)),
                    "pg_ms": _fmt_optional_float_value(_per_engine_median(rows, "pg")),
                    "duck_ms": _fmt_optional_float_value(_per_engine_median(rows, "duck")),
                    "neo_ms": _fmt_optional_float_value(_per_engine_median(rows, "neo")),
                }
            )
    ctx.write_csv(
        out_path,
        ["reg", "depth", "n_anchors", "anchor_deg_med", "reach_med", "pg_ms", "duck_ms", "neo_ms"],
        summary_rows,
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_number(value: str | int | float) -> str:
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.3f}".rstrip("0").rstrip(".")


def _fmt_optional_float(value: object) -> str:
    if value in {"", None}:
        return "NA"
    return f"{float(value):.3f}"


def _fmt_optional_int(value: float | None) -> str:
    if value is None:
        return "NA"
    return str(int(round(value)))


def _fmt_optional_float_value(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.3f}"
