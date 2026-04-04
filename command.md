# Run Commands

Full pipeline:

```bash
bash scripts/run_all.sh --config config.yaml
```

Approximate wall time on this machine:

- setup: `3-8 min` first run, `10-30 sec` on reruns
- prepare: `20-60+ min`
- experiments: `3.5-10+ hours`
- analysis: `3-10 min`
- finalize: `<1 min`

Resume from a specific phase if needed:

```bash
bash scripts/01_phase_setup.sh --config config.yaml
bash scripts/02_phase_prepare.sh --config config.yaml
bash scripts/03_phase_experiments.sh --config config.yaml
bash scripts/04_phase_analysis.sh --config config.yaml
bash scripts/05_phase_finalize.sh --config config.yaml
```

Outputs land in:

- `results/01_setup/`
- `results/02_prepare/`
- `results/03_experiments/`
- `results/04_analysis/`
- `results/05_final/`

Notes:

- If `services.mode: docker` and `services.auto_start: true`, DB-backed phases auto-start local PostgreSQL and Neo4j containers.
- For the final benchmark, only preprocessing uses tmpfs. The PostgreSQL and Neo4j containers must stay on persistent storage because phase 3 restarts the DBMS for every query instance.
- If the Docker containers already exist from an older tmpfs-backed config, recreate them once before the final full run:

```bash
docker rm -f drkg-bench-postgres drkg-bench-neo4j
bash scripts/dev_start_services_docker.sh --config config.yaml
```

- All figures are PNG, `dpi=600`, `font_size=16`, and have no titles.
