# DRKG Query Benchmark

`DRKG-Query` benchmarks matched conjunctive-query workloads derived from the DRKG knowledge graph on PostgreSQL and Neo4j. The pipeline cleans and deduplicates DRKG edges, mines typed query templates, samples anchor bindings, runs paired SQL and Cypher workloads, analyzes the results with a theory lens, and packages tables and figures for reporting.

## What This Project Does

- preprocess `data/drkg.tsv` into a deduplicated directed graph plus node and relation summaries
- load the graph into PostgreSQL and Neo4j
- mine benchmark templates: 2/3/4-edge paths, triangles, and an optional 4-cycle
- sample both hub-anchored and uniform-random bindings
- run baseline benchmarks on both engines and a PostgreSQL join-order study
- record runtime summaries, output cardinalities, and engine-specific plan/work proxies
- build a final results package and verify that expected artifacts were produced

## Repository Layout

- `src/drkg_bench/`: Python code for preprocessing, loading, template mining, benchmarking, analysis, plotting, and verification
- `scripts/`: shell entrypoints for setup and the end-to-end pipeline
- `data/`: DRKG inputs and metadata tables
- `config.yaml`: full benchmark configuration
- `config_milestone.yaml`: lightweight PostgreSQL-only configuration
- `results_milestone/`: example milestone outputs already generated in this repo

## Requirements

- Python 3 with `venv`
- Docker, if using the default service mode from the configs
- local DRKG files under `data/`
- enough RAM and disk for PostgreSQL, Neo4j, and intermediate result files

`bash scripts/00_setup_env.sh --config ...` creates the virtual environment, installs `requirements.txt`, and records environment details in the configured setup directory.

## Quick Start

### Milestone Run

This is the fastest way to validate the pipeline. It uses PostgreSQL only, scans the first `50000` DRKG rows, mines a reduced template set, and writes a compact summary.

```bash
bash scripts/run_milestone.sh --config config_milestone.yaml
```

Key outputs:

- `results_milestone/milestone.md`
- `results_milestone/postgres_runtime.csv`
- `results_milestone/selected_templates.csv`
- `results_milestone/figures/`

### Full Benchmark

This runs the full workflow: setup, preprocessing, both database loads, template mining, binding sampling, baseline benchmarks, PostgreSQL join-order experiments, analysis, figure generation, packaging, and verification.

```bash
bash scripts/run_all.sh --config config.yaml
```

Primary output:

- `results/`

## Pipeline Phases

`scripts/run_all.sh` runs these phases in order:

1. `setup`: create the environment and validate tools, config wiring, and secrets
2. `prepare`: preprocess DRKG, load PostgreSQL, mine templates, sample bindings, and load Neo4j
3. `experiments`: run PostgreSQL baseline, Neo4j baseline, engine comparison, and PostgreSQL join-order experiments
4. `analysis`: compute theory artifacts, summarize results, and render figures
5. `finalize`: build the final package and verify required outputs

## Running Individual Stages

You can run any stage through the CLI wrapper:

```bash
bash scripts/_run_cli.sh check-env --config config.yaml
bash scripts/_run_cli.sh preprocess --config config.yaml
bash scripts/_run_cli.sh mine-templates --config config.yaml
bash scripts/_run_cli.sh run-postgres-baseline --config config.yaml
bash scripts/_run_cli.sh run-neo4j-baseline --config config.yaml
bash scripts/_run_cli.sh run-join-order --config config.yaml
bash scripts/_run_cli.sh compute-theory --config config.yaml
bash scripts/_run_cli.sh verify-results --config config.yaml
```

The wrapper activates the configured virtual environment when available, sets `PYTHONPATH`, and auto-starts Docker services for commands that need a database.

## Configs

- `config.yaml`: full two-engine benchmark with Docker-managed PostgreSQL and Neo4j, fixed benchmark settings, and outputs under `results/`
- `config_milestone.yaml`: reduced PostgreSQL-only run with smaller memory settings, a `50000`-row DRKG subset, and outputs under `results_milestone/`

Important config sections include result paths, database connection settings, Docker container names and ports, template-selection thresholds, sampling budgets, and benchmark timeouts.

## Output Layout

The full run writes a structured result tree:

- `results/01_setup/`: environment report and dependency snapshot
- `results/02_prepare/`: preprocess summaries, load summaries, mined templates, sampled bindings, and preparation figures
- `results/03_experiments/`: PostgreSQL and Neo4j benchmark CSVs, comparison tables, join-order outputs, and experiment figures
- `results/04_analysis/`: theory outputs, summary tables, and analysis figures
- `results/05_final/`: `final_tables/`, `final_figures/`, `config_snapshot.yaml`, and `final_manifest.json`

`verify-results` checks that this layout and the core artifacts were produced consistently.

## Data Notes

- `data/drkg.tsv` is the authoritative edge list
- `data/relation_glossary.tsv` and `data/entity2src.tsv` are treated as metadata inputs
- `node_type` is derived from the prefix before the first `::` in `node_id`
- exact `(src_id, rel_type, dst_id)` duplicates are dropped during preprocessing
