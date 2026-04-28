# DRKG Query Benchmark

This project compares PostgreSQL, Neo4j, and DuckDB on DRKG query workloads. It studies fixed conjunctive queries over paths, triangles, and a 4-cycle, plus bounded directional reachability. The analysis uses database systems behavior and CS 784 ideas: skew, join order, acyclic versus cyclic structure, and AGM-style output bounds.

## Outline

1. **Problem and motivation:** compare graph, row-store SQL, and columnar SQL execution on matched DRKG workloads.
2. **Data preparation:** clean DRKG, drop malformed endpoints, deduplicate directed triples, and keep a common graph for all engines.
3. **Workload construction:** mine typed templates, select `P2`, `P3`, `T1`, `T2`, and `C4`, then sample hub-anchored and uniform-random bindings.
4. **Systems and protocol:** load PostgreSQL, Neo4j, and DuckDB; run matched fixed-query benchmarks; run PostgreSQL join-order experiments; run bounded reachability.
5. **Theory lens:** classify templates by acyclic/cyclic structure and compare runtime against output size, intermediate work, skew, and AGM-style bounds.
6. **Results:** show that skew and intermediate expansion dominate the simple acyclic/cyclic story.

## Run Commands

Set up the environment:

```bash
bash scripts/00_setup_env.sh --config config.yaml
```

Run the full benchmark:

```bash
bash scripts/run_all.sh --config config.yaml
```

Run individual phases:

```bash
bash scripts/01_phase_setup.sh --config config.yaml
bash scripts/02_phase_prepare.sh --config config.yaml
bash scripts/03_phase_experiments.sh --config config.yaml
bash scripts/04_phase_analysis.sh --config config.yaml
bash scripts/05_phase_finalize.sh --config config.yaml
```

Run a quick PostgreSQL-only milestone check:

```bash
bash scripts/run_milestone.sh --config config_milestone.yaml
```

Verify an existing full run:

```bash
bash scripts/_run_cli.sh verify-results --config config.yaml
```

## Key Results

- The saved full run uses `5,874,229` unique DRKG edges, `97,237` nodes, and `107` relations.
- The fixed-query benchmark has `600` matched instances across five templates, two sampling regimes, and three engines.
- DuckDB is fastest on most fixed-query slices, showing the strength of a modern in-process columnar SQL engine on these joins.
- PostgreSQL is faster than Neo4j on most fixed-query slices, but Neo4j has lower medians on a few slices such as `C4 / hub`.
- Hub-anchored queries are consistently harder than uniform-random queries, confirming that skew and anchor choice dominate runtime.
- The acyclic `P3` path is the main stress case. It is much slower and less stable than the triangle and 4-cycle templates, so acyclic structure alone does not predict easy execution.
- PostgreSQL join order matters: cross-product-inducing orders for `P3` time out on every attempted binding, while connected/default orders often complete.
- Bounded reachability is anchor-dominated. Hub anchors expand to tens of thousands of reachable nodes, while uniform anchors usually remain tiny.

The resulting story is practical: runtime is driven by skew, output size, and intermediate expansion more than by the acyclic-versus-cyclic label alone.
