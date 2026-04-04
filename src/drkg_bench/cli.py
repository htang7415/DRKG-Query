from __future__ import annotations

import argparse

from .common import load_context


def _run_check_env(ctx):
    from .env_check import run_env_check

    run_env_check(ctx)


def _run_preprocess(ctx):
    from .preprocess import run_preprocess

    run_preprocess(ctx)


def _run_load_postgres(ctx):
    from .postgres import load_postgres

    load_postgres(ctx)


def _run_load_neo4j(ctx):
    from .neo4j_db import load_neo4j

    load_neo4j(ctx)


def _run_mine_templates(ctx):
    from .template_mining import run_template_mining

    run_template_mining(ctx)


def _run_mine_milestone_templates(ctx):
    from .milestone import run_milestone_template_mining

    run_milestone_template_mining(ctx)


def _run_sample_bindings(ctx):
    from .sampling import run_sampling

    run_sampling(ctx)


def _run_postgres_baseline(ctx):
    from .benchmarking import run_postgres_baseline

    run_postgres_baseline(ctx)


def _run_neo4j_baseline(ctx):
    from .benchmarking import run_neo4j_baseline

    run_neo4j_baseline(ctx)


def _run_compare_engines(ctx):
    from .comparison import run_engine_comparison

    run_engine_comparison(ctx)


def _run_join_order(ctx):
    from .benchmarking import run_join_order

    run_join_order(ctx)


def _run_compute_theory(ctx):
    from .theory import run_theory

    run_theory(ctx)


def _run_postprocess(ctx):
    from .analysis import run_postprocess

    run_postprocess(ctx)


def _run_make_figures(ctx):
    from .figures import run_figures

    run_figures(ctx)


def _run_build_final_package(ctx):
    from .final_package import build_final_package

    build_final_package(ctx)


def _run_verify_results(ctx):
    from .verify import verify_results

    verify_results(ctx)


def _run_write_milestone_report(ctx):
    from .milestone import write_milestone_report

    write_milestone_report(ctx)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    ctx = load_context(args.config)
    commands = {
        "check-env": _run_check_env,
        "preprocess": _run_preprocess,
        "load-postgres": _run_load_postgres,
        "load-neo4j": _run_load_neo4j,
        "mine-templates": _run_mine_templates,
        "mine-milestone-templates": _run_mine_milestone_templates,
        "sample-bindings": _run_sample_bindings,
        "run-postgres-baseline": _run_postgres_baseline,
        "run-neo4j-baseline": _run_neo4j_baseline,
        "compare-engines": _run_compare_engines,
        "run-join-order": _run_join_order,
        "compute-theory": _run_compute_theory,
        "postprocess": _run_postprocess,
        "make-figures": _run_make_figures,
        "build-final-package": _run_build_final_package,
        "verify-results": _run_verify_results,
        "write-milestone-report": _run_write_milestone_report,
    }
    commands[args.command](ctx)


if __name__ == "__main__":
    main()
