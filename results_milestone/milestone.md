# Lightweight Milestone Run

This is a PostgreSQL-only milestone run intended to produce quick, representative outputs rather than the full final benchmark.

Scope of this run:
- DRKG subset: first `50000` raw rows from `data/drkg.tsv`
- Engine coverage: PostgreSQL only
- Template mining: `path_2`
- Skipped for milestone: Neo4j baseline, 4-edge path search, 4-cycle search, join-order study, final report package
- Benchmark simplifications: no per-instance DB restart, `0` warmup runs, `1` measured run, `1` instrumented run, `2` bindings per template/regime

Saved outputs in this folder:
- `selected_templates.csv`
- `postgres_runtime.csv`
- `figures/milestone_process.png`
- `figures/selected_template_metrics.png`
- `figures/postgres_runtime.png`
- `milestone.md`

Run summary:
- Selected templates: `1`
- Baseline bindings: `4`
- PostgreSQL benchmark instances: `4`
- Successful PostgreSQL instances: `4`

Milestone process:
- Raw rows scanned: `50000`
- Unique edges kept: `50000`
- Unique nodes kept: `2366`
- Duplicate rows dropped: `0`

Selected templates:
- `T1`: family=`path`, pattern=`HumGenHumGen -> HumGenHumGen`, types=`Gene:Gene -> Gene:Gene`, grounded=`5163435`, anchors=`2022`

PostgreSQL runtime summary:
- `T1` / `hub`: median=`6.909595` ms, q1=`5.586906` ms, q3=`8.232284` ms
- `T1` / `uniform`: median=`4.778351` ms, q1=`4.354949` ms, q3=`5.201752` ms

File guide:
- `selected_templates.csv`: short template catalog used in the milestone run. `pattern` is a concise relation sequence and `types` shows endpoint types on each edge.
- `postgres_runtime.csv`: runtime summary for the PostgreSQL milestone queries. `n` is the number of benchmark instances per regime.
- `figures/milestone_process.png`: compact workflow diagram from DRKG subset to final PostgreSQL runs.
- `figures/selected_template_metrics.png`: grounded matches and valid anchors for the chosen template.
- `figures/postgres_runtime.png`: PostgreSQL runtime comparison between `uniform` and `hub` anchor regimes, with IQR bars and individual run points.

Interpretation:
- This milestone run is useful for checking data flow, query generation, PostgreSQL execution, logging, and figure generation.
- It is not a substitute for the final full benchmark because it uses a DRKG subset and omits cross-engine and join-order evaluations.
- Intermediate step folders were removed intentionally so `results_milestone/` stays concise.
