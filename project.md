# Neo4j vs PostgreSQL vs DuckDB on DRKG Conjunctive-Query and Bounded-Reachability Workloads

## 1. Objective

Compare Neo4j, PostgreSQL, and DuckDB on matched conjunctive-query workloads derived from DRKG, plus a bounded-reachability workload, and explain the results using CS 784 ideas: acyclic vs cyclic structure, AGM-style output bounds, join ordering, and skew. The fixed-query workload covers paths, triangles, and a 4-cycle; the recursive workload covers depth-2 and depth-3 directional reachability from a small anchor sample.

## 2. Hypothesis

The executed results support the parts of the hypothesis about skew and join-order sensitivity, but not the simple structural claim that acyclic templates are more stable than cyclic templates.

- Hub-anchored queries are consistently slower than uniform-random queries across all engines and templates.
- The selected 3-edge path `P3` is the hardest template by a wide margin and is much less stable than the triangle and 4-cycle templates because of large output size and intermediate expansion.
- DuckDB is the fastest engine on most summarized template-regime pairs; PostgreSQL is faster than Neo4j on most pairs. Neo4j is fastest on `C4 / hub` and narrowly faster than PostgreSQL on the timed `P3 / hub` median, though the `P3 / hub` comparison has failure caveats.
- PostgreSQL forced join orders show that disconnected, cross-product-inducing orders for `P3` time out on every attempted binding, while connected and default orders succeed on most bindings.
- Bounded reachability is anchor-dominated: hub anchors expand to tens of thousands of reachable nodes and run hundreds of milliseconds to seconds, while uniform anchors usually finish in single-digit milliseconds.
- Yannakakis and worst-case optimal joins are used only as analytic reference points, not as claims about any system's implementation.

## 3. Systems and Reproducibility

Run all three systems on one machine through Docker-managed local services for PostgreSQL and Neo4j and through an in-process DuckDB database. Report the fixed DBMS configuration captured in the result package.

The executed project uses:

- PostgreSQL 16 with parallel query disabled (`max_parallel_workers_per_gather=0`)
- Neo4j 5.26.0 Community Edition with `CYPHER runtime=slotted` for every benchmark query
- A separate Neo4j `PROFILE` pass after successful timing runs; in the saved results, these passes failed runtime verification and Neo4j plan-work metrics are unavailable
- DuckDB as an in-process columnar SQL engine, using a persisted `.duckdb` database
- One dedicated PostgreSQL role and project database
- One authenticated local Neo4j account for the project instance
- Both server systems bound to `localhost`
- Fixed PostgreSQL settings: `shared_buffers=4GB`, `work_mem=256MB`, `maintenance_work_mem=2GB`, `effective_cache_size=16GB`, and `max_parallel_workers_per_gather=0`
- Fixed Neo4j settings: heap size `8g`, page-cache size `8g`, and configured runtime `slotted`
- Fixed DuckDB settings: `memory_limit=8GB`, `threads=4`
- Programmatic DBMS restart before each PostgreSQL or Neo4j query instance using the Docker restart scripts in `scripts/`
- OS page-cache flush was attempted only when configured; the saved full-run config has no flush command configured, so `flush_ok=false` is recorded

## 4. Data Model

**Neo4j:**

- Entities as `(:Entity {node_id, node_type})`
- Directed relationships loaded from `drkg.tsv`
- One deterministic mapping from each raw DRKG relation ID to a legal Neo4j relationship-type token; the raw relation ID remains the logical relation name used in the benchmark specification and report
- Uniqueness constraint or index on node ID

**PostgreSQL:**

- `nodes(node_id, node_type)`
- `edges(src_id, dst_id, rel_type)`
- Uniqueness on `(src_id, rel_type, dst_id)` to enforce the Section 4 deduplication rule
- Indexes on `(rel_type, src_id)`, `(rel_type, dst_id)`, and `(src_id, rel_type)`
- The `(src_id, rel_type)` index is intentional for anchor-first probes that bind `src_id` before filtering by relation type

**DuckDB:**

- Same `nodes` and `edges` schema as PostgreSQL, persisted in a single `.duckdb` file
- Loads the same pre-deduplicated `nodes.csv` and `edges.csv` produced by preprocessing, then builds query-support indexes and `ANALYZE`s the database so DuckDB sees the same edge set as PostgreSQL and Neo4j

**Canonical data semantics:**

- `drkg.tsv` is the authoritative source of edge direction and endpoint types
- `node_type` is the substring before the first `::` in `node_id`
- Drop rows where either endpoint is of the form `<type>::` with an empty local identifier
- Deduplicate exact `(src_id, rel_type, dst_id)` triples on load; all benchmark queries use set semantics over this deduplicated graph
- Keep self-loops in storage, but benchmark templates enforce pairwise-distinct node variables, so self-loops cannot satisfy benchmark edges
- Use `relation_glossary.tsv` and `entity2src.tsv` only as metadata tables; do not infer edge direction from `relation_glossary.tsv`
- The `Connected entity-types` column in `relation_glossary.tsv` may disagree with the actual `(src_id, dst_id)` direction in `drkg.tsv` for some relations, so any endpoint-type checks must be derived from `drkg.tsv`
- The executed preprocessing kept 5,874,229 unique edges, 97,237 unique nodes, and 107 unique relations; it dropped 29 rows with empty endpoint identifiers and 3 duplicate triples, and kept 3,499 self-loops in storage

For theory and query specification, each relation type is treated logically as a filtered binary relation:

```
R_t(src_id, dst_id) := SELECT src_id, dst_id FROM edges WHERE rel_type = t
```

Here `t` is the raw relation ID from `drkg.tsv`; the benchmark harness maps that raw ID to the corresponding Neo4j relationship-type token.

## 5. Workload

Use paired SQL and Cypher templates with identical semantics. SQL templates run on both PostgreSQL and DuckDB without modification; Cypher templates run on Neo4j.

**Acyclic templates:**

- 2-edge path (`P2`)
- 3-edge path (`P3`)

**Cyclic templates:**

- 2 triangle templates (`T1`, `T2`)
- 1 4-cycle template (`C4`)

The saved full-run configuration disables 4-edge paths (`select_path4_if_available=false`) and enables the 4-cycle (`select_four_cycle_if_available=true`), so the executed fixed-query workload contains five templates: `P2`, `P3`, `T1`, `T2`, and `C4`.

**Bounded reachability:**

A separate recursive workload runs depth-2 and depth-3 directional reachability from a small anchor sample (5 anchors per regime). PostgreSQL and DuckDB use a recursive CTE that expands a frontier and counts distinct reachable endpoints; Neo4j uses a variable-length Cypher pattern and counts distinct endpoint nodes.

**Parameter regimes:**

- Hub-anchored
- Uniform-random

Each template designates one anchor variable, chosen to be the first node variable in the written pattern. A valid anchor is a binding of that variable that participates in at least one full grounded match of the template after all filters.

- **Hub-anchored:** sample anchors from the top 10% of valid anchors by degree on the first edge of the template, measured on the anchor side of that edge; use out-degree when the anchor is the source of the first edge and in-degree otherwise; break ties by `node_id`
- **Uniform-random:** sample anchors uniformly without replacement from all valid anchors

**Fixed semantics:**

- Directed edges
- Pairwise-distinct node variables
- Distinct edge aliases; if the same relation type appears multiple times in one template, each edge position uses a separate alias and may not bind the same stored edge tuple twice
- In SQL, when the same relation type appears multiple times, use separate aliases and add pairwise tuple-inequality predicates of the form `NOT (e_i.src_id = e_j.src_id AND e_i.dst_id = e_j.dst_id)` between those same-type aliases; in Cypher, use separate relationship variables
- Benchmark `COUNT(*)` / `count(*)` over the deduplicated graph, with output cardinality logged separately; do not add `COUNT(DISTINCT ...)` because ingest already enforces set semantics

## 6. Template-Mining and Selection Step

Run this first.

Mine candidate typed paths of lengths 2 and 3, candidate typed triangles, and candidate typed 4-cycles directly from the cleaned, deduplicated DRKG load defined in Section 4. A candidate template is identified by its ordered relation-type pattern together with its ordered endpoint-type pattern under the benchmark distinctness rules.

The implementation has configuration fields for length-4 paths and 4-cycles. In the saved full run, length-4 paths are disabled (`select_path4_if_available=false`) and 4-cycles are enabled (`select_four_cycle_if_available=true`).

Ordering convention for candidate patterns:

- For a path, edge order is the written path order from the anchor variable through the path
- For a cycle, the anchor variable is the first node variable in the written pattern; edge order starts at the anchor, follows the first written edge, continues around the cycle in that traversal direction, and ends with the edge that returns to the anchor
- Endpoint-type patterns follow that same ordered edge sequence

For each candidate pattern, report:

- Endpoint type pattern, derived from the actual endpoint IDs in `drkg.tsv`
- Relation-type pattern
- Grounded match count under the benchmark distinctness rules
- Valid-anchor count under the Section 5 anchor convention
- Anchor-node degree statistics for the designated anchor variable: min, median, p95, and max of the first-edge degree among supporting anchors

Qualification rules:

- A path, triangle, or 4-cycle candidate qualifies only if it has at least 100 grounded matches and at least 20 valid anchors after the Section 4 cleaning and deduplication rules

Selection rules:

- For each enabled path length, select the highest-ranked qualifying path candidate as the benchmark path template of that length
- Select the two highest-ranked qualifying triangle candidates as the benchmark triangle templates
- If 4-cycle selection is enabled, select the highest-ranked qualifying 4-cycle candidate as the benchmark 4-cycle template
- Ranking is by grounded match count descending, then valid-anchor count descending, then relation-type pattern under element-wise lexicographic tuple comparison, then endpoint-type pattern under the same element-wise tuple comparison

Freeze the final template set before benchmarking.

The executed mining step selected:

- `P2`: 2-edge path, 503,732,205 grounded matches and 3,978 valid anchors
- `P3`: 3-edge path, 140,435,650,562 grounded matches and 3,974 valid anchors
- `T1`: triangle, 50,701,812 grounded matches and 5,070 valid anchors
- `T2`: triangle, 42,863,115 grounded matches and 4,556 valid anchors
- `C4`: 4-cycle, 113,628 grounded matches and 568 valid anchors

## 7. Metrics

**Core cross-engine metrics:**

- Median wall-clock time over 10 plain measured executions for PostgreSQL and Neo4j, and over 3 plain measured executions for DuckDB, on the fixed-query templates (Neo4j `C4` uses reduced measured timing without `PROFILE` to avoid the profile pass dominating the extension)
- IQR over the same configured plain measured executions
- Output cardinality

**Execution-behavior metrics reported per engine, not as numerically identical cross-system quantities:**

- PostgreSQL buffer hits from `EXPLAIN (ANALYZE, BUFFERS)`
- PostgreSQL intermediate-work proxy: sum of `Actual Rows × Actual Loops` over plan nodes
- Neo4j total DB hits from `PROFILE`, when the profile pass succeeds
- Neo4j intermediate-work proxy: sum of operator row counts from `PROFILE`, when the profile pass succeeds
- DuckDB plain timing only; no instrumented work proxy is required for the cross-engine comparison

In the saved results, Neo4j timing and output-cardinality results are available for successful plain executions, but Neo4j `PROFILE` instrumentation failed runtime verification for successful timed rows. Therefore Neo4j DB-hit and operator-work columns are empty in the final CSVs. PostgreSQL is the only engine with a populated work-proxy column in the AGM and work plots.

## 8. Measurement Protocol

A query instance is one fully instantiated run defined by:

- One template
- One regime
- One sampled anchor binding
- One engine
- And, for PostgreSQL forced-order experiments, one specific join order
- And, for reachability, one depth and one engine

All bindings are sampled once with a fixed recorded random seed before any benchmarking begins.

For each fixed-query instance:

1. Ensure the engine is in the fixed benchmark configuration for that system
2. If privileged OS page-cache flush is available, flush the OS page cache; otherwise skip this step and record that cold-start control is limited to DB restart only
3. Restart the DBMS (PostgreSQL and Neo4j only; DuckDB reopens its in-process database file)
4. Run 1 untimed warmup execution of the plain query, without `EXPLAIN` or `PROFILE`
5. Apply a hard timeout of 60 seconds to the warmup and to each plain measured execution
6. Run the configured measured plain executions and report median and IQR; in the saved full-run config this is 10 runs for PostgreSQL and Neo4j, and 3 runs for DuckDB
7. Run 1 separate untimed instrumented execution immediately afterward to collect plan metrics: `EXPLAIN (ANALYZE, BUFFERS)` for PostgreSQL and `PROFILE` with `Runtime SLOTTED` for Neo4j; DuckDB does not run a separate instrumented pass in this package
8. Apply a hard timeout of 180 seconds to the instrumented execution
9. If the warmup or any plain measured run times out, exhausts memory, or fails with an execution error, abort the remaining runs for that query instance and log status, failure stage, failure type, timeout value, and completed measured-run count; record median, IQR, output cardinality, and instrumentation metrics as `NA` for that instance
10. If the warmup and all configured plain measured runs succeed but the instrumented execution times out, exhausts memory, or fails with an execution error, keep median, IQR, and output cardinality from the successful plain runs, record instrumentation metrics as `NA`, and log the instrumented failure separately
11. Log timing and instrumentation metrics to CSV

The instrumented execution is excluded from wall-clock timing because it perturbs execution. The configured measured executions all occur within the same post-restart instance, so the warmup is the only intended cache primer for that query instance.

For each reachability instance, the engine runs a single timed execution per (engine, regime, anchor, depth) combination with a 30-second timeout, since reachability is anchor-dominated and the per-anchor result is the unit of evidence rather than a 10-run distribution.

**Binding counts:**

- Baseline benchmarking: 20 sampled bindings per template-regime, without replacement when enough valid anchors exist; otherwise use all valid anchors and report the shortfall
- Join-order study: 5 sampled bindings per template-regime, with the same rule
- Reachability: 5 anchors per regime per engine, evaluated at depths 2 and 3

**Executed experiment budget:**

- Baseline: 600 query instances (5 templates × 2 regimes × 20 bindings × 3 engines)
- PostgreSQL join-order study: 210 query instances
- Reachability: 60 query instances (3 engines × 2 regimes × 5 anchors × 2 depths)

The default-plan comparator instances use the same 5 bindings as the forced-order study, so the default-vs-forced comparison is matched.

## 9. Theory Lens

For each fixed-query template:

- Define variables and relations
- Define the join hypergraph
- Classify it as acyclic or cyclic
- Compute the tightest AGM-style upper bound after the same relation filters and anchor binding by solving for the optimal fractional edge cover of the filtered join hypergraph; note separately that pairwise-distinctness predicates can only reduce the true result size
- Compare runtime and intermediate expansion against that bound

The executed project compares the 3-edge path against the two selected triangle templates and the 4-cycle. In these results, the acyclic 3-edge path is much slower and less stable than the triangle and 4-cycle templates, so the observed behavior is better explained by skew, output size, and intermediate work than by acyclic/cyclic structure alone. PostgreSQL is the only engine with a populated work-proxy column, so the AGM-vs-runtime and work-vs-runtime plots use PostgreSQL work as the structural reference.

## 10. Join-Order Study in PostgreSQL

Compare:

- PostgreSQL default plan
- Forced join orders

Force join order with fully parenthesized left-deep SQL plus `join_collapse_limit=1` and `from_collapse_limit=1`.

- Connected-prefix orders use standard `INNER JOIN ... ON ...`
- Cross-product-inducing orders intentionally use `CROSS JOIN` or `JOIN ... ON TRUE` at the disconnected step, with the deferred join predicates placed in the final `WHERE` clause so the requested left-deep order is actually realized
- Every forced-order and default-plan comparator instance uses the full Section 8 measurement protocol, including restart, warmup, 10 measured runs, timeout policy, and separate instrumented pass

**Templates included:**

- 2-edge path is excluded because it has only one join and therefore no nontrivial join-order choice
- 4-cycle is excluded from the join-order study; only `P3`, `T1`, and `T2` are forced
- 3-edge path: all 6 left-deep orders, classified as connected-prefix or cross-product-inducing
- Each triangle: all 6 left-deep orders (all connected, since every triangle suffix shares a variable with its prefix)

For the 3-edge path, disconnected relation orders are classified as:

- **Connected-prefix orders**, where each newly joined relation shares at least one variable with the current intermediate result
- **Cross-product-inducing orders**, where some step joins a relation sharing no variable with the current intermediate result

These cross-product-inducing orders are intentional and quantify how badly poor join orders can blow up intermediate work.

In the saved results, the join-order study has 210 rows. The successful summary includes connected/default rows for `P3`, `T1`, and `T2`; the `P3` cross-product class timed out for all 20 attempted instances.

## 11. Deliverables

- Deterministic preprocessing script that cleans and deduplicates `drkg.tsv` and emits the Neo4j relation-type mapping
- Neo4j ingestion and indexing scripts
- PostgreSQL ingestion and indexing scripts
- DuckDB ingestion and indexing scripts
- Paired SQL and Cypher templates for the selected path, triangle, 4-cycle, and reachability workloads
- Template-mining and selection script
- Parameter samplers
- Benchmark harness with CSV logging
- Final result package under `results/05_final/` with manifest, config snapshot, slide-ready tables, story-ordered figures, and `results_and_conclusion.md`

The final package contains:

- `final_tables/` (4 story tables, numbered in story order):
  - `1_selected_templates.csv`: chosen workload templates with grounded match and anchor counts
  - `2_engine_summary.csv`: per template-regime cross-engine medians and PG/Neo4j and PG/DuckDB speedups
  - `3_join_order_summary.csv`: PostgreSQL forced-order vs default plan summary for `P3`, `T1`, `T2`
  - `4_reachability_runtime.csv`: bounded-reachability summary aggregating the 5 anchors per (regime, depth), with median anchor degree, median reachable count, and per-engine median runtime
- `final_figures/` (6 story figures, numbered in story order):
  - `1_template_profile.png`: mining space, selected queries, and binding support for the five templates
  - `2_engine_runtime.png`: PostgreSQL vs Neo4j vs DuckDB medians per template-regime, with engine/pg ratio labels above each non-pg bar to carry the speedup story
  - `3_structure_runtime.png`: acyclic vs cyclic medians per engine and regime
  - `4_agm_runtime.png`: PostgreSQL runtime against the AGM upper bound
  - `5_join_order_effect.png`: PostgreSQL forced-order vs default plans for `P3`, `T1`, `T2`
  - `6_reachability_runtime.png`: bounded-reachability runtime by engine, regime, and depth

## 12. Expected Contribution

A controlled comparison of Neo4j, PostgreSQL, and DuckDB on the executed DRKG path, triangle, 4-cycle, and bounded-reachability workloads. The main findings are that hub anchors and poor PostgreSQL join orders substantially increase runtime, the selected 3-edge path is harder than the triangle and 4-cycle templates despite being acyclic, DuckDB is the fastest engine on most fixed-query slices, PostgreSQL is faster than Neo4j on most fixed-query slices, and bounded reachability is anchor-dominated rather than engine-dominated. Neo4j timing results are included, including a small number of slices where Neo4j has the lower median, while Neo4j plan-work metrics are unavailable because the saved `PROFILE` passes failed runtime verification.
